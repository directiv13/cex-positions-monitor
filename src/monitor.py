from __future__ import annotations

import asyncio
from collections.abc import Callable, Awaitable
from dataclasses import replace as dc_replace

from loguru import logger

from .exchange.base import ExchangeBase
from .models import Order, OrderStatus, Position, StateSnapshot


def _pos_key(p: Position) -> str:
    return f"{p.symbol}_{p.position_side.value}"

OrderCallback      = Callable[[Order,    str], Awaitable[None]]
PositionCallback   = Callable[[Position, str], Awaitable[None]]
ConnectionCallback = Callable[[dict], Awaitable[None]]


class ChangeMonitor:
    def __init__(self, exchange: ExchangeBase, reconcile_interval: int = 120) -> None:
        self._exchange = exchange
        self._reconcile_interval = reconcile_interval
        self._state = StateSnapshot()
        self._running = False
        self._order_cbs:      list[OrderCallback]      = []
        self._position_cbs:   list[PositionCallback]   = []
        self._connection_cbs: list[ConnectionCallback] = []
        # Task handles — set in start(), used in stop()
        self._stream_task:  asyncio.Task | None = None
        self._recon_task:   asyncio.Task | None = None

    # ── Subscription ──────────────────────────────────────────────────────────

    def on_order_event(self, cb: OrderCallback) -> None:
        self._order_cbs.append(cb)

    def on_position_event(self, cb: PositionCallback) -> None:
        self._position_cbs.append(cb)

    def on_connection_event(self, cb: ConnectionCallback) -> None:
        self._connection_cbs.append(cb)

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
                self._state.positions[_pos_key(p)] = p
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
                elif event_type == "CONNECTION" and isinstance(payload, dict):
                    await self._handle_connection_event(payload)
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
        Reconcile the REST open-orders snapshot into local state **silently**.

        Reconciliation never notifies — its only job is to keep the in-memory
        state (and therefore the pinned dashboard) consistent with reality,
        catching anything the WebSocket stream missed. Notifications come solely
        from live WebSocket events, which carry the true order status.

        This matters because `fetch_open_orders` returns only *open* orders: when
        an order vanishes from it we know it closed but not *how* (filled vs.
        canceled vs. expired). Guessing here previously produced false "FILLED"
        messages, so we no longer emit anything from reconciliation at all.

        Cases:
          A) In REST, not in local state  → add.
          B) In both, status changed      → update.
          C) Open locally, gone from REST → closed; drop from state.
        """
        rest_map = {o.order_id: o for o in rest_orders}

        # Cases A and B — add new / update changed, state only
        for oid, order in rest_map.items():
            existing = self._state.orders.get(oid)
            if existing is None or order.status != existing.status:
                self._state.orders[oid] = order

        # Case C — order was open locally but has vanished from REST
        for oid, existing in list(self._state.orders.items()):
            if oid not in rest_map and existing.is_open:
                logger.debug(
                    "Order {} vanished from REST during reconciliation — "
                    "removing from state (no notification)", oid,
                )
                del self._state.orders[oid]

    async def _diff_positions(self, rest_positions: list[Position]) -> None:
        """
        Reconcile REST positions into local state **silently** (see _diff_orders).

        Cases:
          A) In REST, not in local state  → add.
          B) Open locally, gone from REST → closed; drop from state.
          C) In both but side flipped     → replace with the new side.
          D) In both, same side           → refresh REST-only fields.
        """
        rest_map = {_pos_key(p): p for p in rest_positions}

        # Cases A, C and D
        for sym, pos in rest_map.items():
            existing = self._state.positions.get(sym)
            if existing is None:
                # New position
                self._state.positions[sym] = pos
            elif (
                existing.position_amt != pos.position_amt
                and existing.direction != pos.direction
            ):
                # Side flip (e.g. LONG → SHORT): replace with the current side.
                self._state.positions[sym] = pos
            else:
                # Refresh REST-only fields not available in WS events.
                self._state.positions[sym] = dc_replace(
                    existing,
                    leverage=pos.leverage,
                    liquidation_price=pos.liquidation_price,
                    mark_price=pos.mark_price,
                )

        # Case B — position was open locally but absent from REST
        for sym, existing in list(self._state.positions.items()):
            if sym not in rest_map and existing.is_open:
                logger.debug(
                    "Position {} vanished from REST during reconciliation — "
                    "removing from state (no notification)", sym,
                )
                del self._state.positions[sym]

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
        key = _pos_key(position)

        # Merge REST-only fields (leverage, liquidation_price) from existing state
        # when the incoming position came from a WebSocket event that lacks them.
        existing = self._state.positions.get(key)
        if existing and position.leverage is None:
            position = dc_replace(
                position,
                leverage=existing.leverage,
                liquidation_price=existing.liquidation_price or position.liquidation_price,
            )

        if key not in self._state.positions:
            if not position.is_open:
                # Closed position we never tracked — ignore
                return
            event = "OPENED"
            self._state.positions[key] = position

        elif not position.is_open:
            event = "CLOSED"
            del self._state.positions[key]
        else:
            event = "UPDATED"
            self._state.positions[key] = position

        if event != "UPDATED":
            await self._fire_position(position, event)

    async def _handle_connection_event(self, info: dict) -> None:
        """Forward WebSocket connectivity changes (lost/restored) to callbacks."""
        logger.warning(
            "WebSocket connection {}: stream={} {}",
            info.get("status"), info.get("stream"), info.get("error") or "",
        )
        await self._fire_connection(info)

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

    async def _fire_connection(self, info: dict) -> None:
        for cb in self._connection_cbs:
            try:
                await cb(info)
            except Exception as exc:
                logger.error("Connection callback raised: {}", exc)