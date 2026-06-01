from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Awaitable

from loguru import logger

from .exchange.base import ExchangeBase
from .models import Order, OrderStatus, Position, StateSnapshot

OrderCallback    = Callable[[Order,    str], Awaitable[None]]
PositionCallback = Callable[[Position, str], Awaitable[None]]


class ChangeMonitor:
    def __init__(self, exchange: ExchangeBase, reconcile_interval: int = 120) -> None:
        self._exchange = exchange
        self._reconcile_interval = reconcile_interval
        self._state = StateSnapshot()
        self._running = False
        self._order_cbs:    list[OrderCallback]    = []
        self._position_cbs: list[PositionCallback] = []
        # Task handles — set in start(), used in stop()
        self._stream_task:  asyncio.Task | None = None
        self._recon_task:   asyncio.Task | None = None

    # ── Subscription ──────────────────────────────────────────────────────────

    def on_order_event(self, cb: OrderCallback) -> None:
        self._order_cbs.append(cb)

    def on_position_event(self, cb: PositionCallback) -> None:
        self._position_cbs.append(cb)

    def current_state(self) -> StateSnapshot:
        """Return a deep copy so callers cannot accidentally mutate live state."""
        return self._state.copy()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True

        # ── Cold start (REST) ─────────────────────────────────────────────────
        try:
            orders    = await self._exchange.fetch_open_orders()
            positions = await self._exchange.fetch_open_positions()
            for o in orders:
                self._state.orders[o.order_id] = o
            for p in positions:
                self._state.positions[p.symbol] = p
            logger.info(
                "Cold start complete: {} orders, {} positions",
                len(orders), len(positions),
            )
        except Exception as exc:
            logger.error("Cold start failed: {}", exc)

        # ── Launch background tasks ───────────────────────────────────────────
        self._stream_task = asyncio.create_task(
            self._stream_consumer(), name="monitor-stream"
        )
        self._recon_task = asyncio.create_task(
            self._reconcile_loop(), name="monitor-reconcile"
        )

        try:
            await asyncio.gather(self._stream_task, self._recon_task)
        except asyncio.CancelledError:
            # stop() cancelled our tasks — do not propagate to main.py's gather
            pass

    def stop(self) -> None:
        """Cancel background tasks. Plain def — no await needed."""
        self._running = False
        if self._stream_task:
            self._stream_task.cancel()
        if self._recon_task:
            self._recon_task.cancel()

    # ── WebSocket consumer ─────────────────────────────────────────────────────

    async def _stream_consumer(self) -> None:
        try:
            async for event_type, payload in self._exchange.stream_events():
                if not self._running:
                    break
                if event_type == "ORDER_UPDATE":
                    await self._handle_order_event(payload)
                elif event_type == "POSITION_UPDATE":
                    await self._handle_position_event(payload)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("Stream consumer error: {}", exc)

    # ── REST reconciliation ───────────────────────────────────────────────────

    async def _reconcile_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._reconcile_interval)
                if not self._running:
                    break
                await self._reconcile_once()
                logger.info("Reconciliation complete")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Reconcile pass failed: {}", exc)

    async def _reconcile_once(self) -> None:
        orders    = await self._exchange.fetch_open_orders()
        positions = await self._exchange.fetch_open_positions()
        await self._diff_orders(orders)
        await self._diff_positions(positions)

    # ── Diff helpers (used by reconciliation) ─────────────────────────────────

    async def _diff_orders(self, rest_orders: list[Order]) -> None:
        """
        Compare REST snapshot against local state and fire events for gaps.

        Three cases:
          A) Order in REST but not in local state → truly new, fire OPENED.
          B) Order in both but status changed → delegate to _handle_order_event.
          C) Order in local state but absent from REST while still open locally
             → it closed between WebSocket events; infer the final status and fire.
        """
        rest_map = {o.order_id: o for o in rest_orders}

        # Cases A and B
        for oid, order in rest_map.items():
            if oid not in self._state.orders:
                # Genuinely new — fire OPENED
                await self._handle_order_event(order)
            else:
                existing = self._state.orders[oid]
                if order.status != existing.status:
                    await self._handle_order_event(order)

        # Case C — order was open locally but has vanished from REST
        for oid, existing in list(self._state.orders.items()):
            if oid not in rest_map and existing.is_open:
                # Build a synthetic closed order with the most likely terminal status
                inferred_status = OrderStatus.FILLED
                closed = copy.replace(existing, status=inferred_status)
                logger.debug(
                    "Order {} absent from REST while open locally — inferring {}",
                    oid, inferred_status.value,
                )
                await self._handle_order_event(closed)

    async def _diff_positions(self, rest_positions: list[Position]) -> None:
        """
        Compare REST positions against local state.

        Cases:
          A) Symbol in REST but not local, and open   → OPENED
          B) Symbol in local but not REST, and open   → CLOSED (missed WebSocket event)
          C) Symbol in both but side flipped          → CLOSED old, OPENED new
        """
        rest_map = {p.symbol: p for p in rest_positions}

        # Case A and C
        for sym, pos in rest_map.items():
            if sym not in self._state.positions:
                # New position
                await self._handle_position_event(pos)
            else:
                existing = self._state.positions[sym]
                # Detect side flip: e.g. LONG → SHORT
                if (
                    existing.position_amt != pos.position_amt
                    and existing.direction != pos.direction
                ):
                    # Close the old side first, then open the new one
                    closed = copy.replace(existing, position_amt=0.0)
                    await self._handle_position_event(closed)
                    await self._handle_position_event(pos)

        # Case B — position was open locally but absent from REST
        for sym, existing in list(self._state.positions.items()):
            if sym not in rest_map and existing.is_open:
                closed = copy.replace(existing, position_amt=0.0)
                logger.debug(
                    "Position {} absent from REST while open locally — inferring CLOSED", sym
                )
                await self._handle_position_event(closed)

    # ── Event handlers (used by both WebSocket and reconciliation) ────────────

    async def _handle_order_event(self, order: Order) -> None:
        oid = order.order_id
        event: str

        if oid not in self._state.orders:
            event = "OPENED"
        else:
            existing = self._state.orders[oid]
            if order.status == existing.status:
                if order != existing:
                    event = "UPDATED"
                else:
                    # Exact duplicate event — update state and do nothing.
                    self._state.orders[oid] = order
                    return
            elif order.status == OrderStatus.FILLED:
                event = "FILLED"
            elif order.status in (
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            ):
                event = order.status.value
            else:
                event = f"STATUS_CHANGED:{order.status.value}"

        self._state.orders[oid] = order
        await self._fire_order(order, event)

    async def _handle_position_event(self, position: Position) -> None:
        sym = position.symbol

        if sym not in self._state.positions:
            if not position.is_open:
                # Closed position we never tracked — ignore
                return
            event = "OPENED"
            self._state.positions[sym] = position
        elif not position.is_open:
            event = "CLOSED"
            del self._state.positions[sym]
        else:
            event = "UPDATED"
            self._state.positions[sym] = position

        if event != "UPDATED":
            await self._fire_position(position, event)

    # ── Callback dispatch ─────────────────────────────────────────────────────

    async def _fire_order(self, order: Order, event: str) -> None:
        for cb in self._order_cbs:
            try:
                await cb(order, event)
            except Exception as exc:
                logger.error("Order callback raised: {}", exc)

    async def _fire_position(self, position: Position, event: str) -> None:
        for cb in self._position_cbs:
            try:
                await cb(position, event)
            except Exception as exc:
                logger.error("Position callback raised: {}", exc)