from __future__ import annotations

import asyncio
import websockets
import json
import time
from dataclasses import replace as dc_replace

from collections.abc import AsyncIterator
from datetime import datetime


from loguru import logger

from ..models import Order, OrderStatus, Position, PositionSide
from .base import ExchangeBase


# ── Futures user-data socket tuning ───────────────────────────────────────────
# The client sends a ping every PING_INTERVAL and drops the connection if no
# pong arrives within PING_TIMEOUT. The library default (20/20) is aggressive
# for a link that occasionally stalls; 20/60 keeps dead-connection detection
# without killing healthy-but-slow ones.
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 60
WS_OPEN_TIMEOUT = 10
WS_CLOSE_TIMEOUT = 5
# Frames buffered before websockets pauses reading from the transport. While
# paused, *no* frames are read — including pongs — which self-inflicts a
# "keepalive ping timeout". The read loop below does no I/O, so this headroom
# only ever matters during a burst.
WS_MAX_QUEUE = 256
WS_MAX_BACKOFF = 60
# How long a session must hold before it counts as healthy. Below this the
# connection is flapping: the backoff keeps escalating and no "restored" alert
# is sent, so an unstable link produces one ⚠️ instead of a ⚠️/✅ every few
# seconds.
WS_STABLE_SESSION_SECONDS = 30
# Futures listenKeys expire 60 minutes after creation; refresh at half that.
LISTEN_KEY_REFRESH_SECONDS = 30 * 60
# Hard ceiling on every REST call. Without it aiohttp allows 5 minutes, long
# enough for a single hung request to stall the reconnect loop.
REST_TIMEOUT_SECONDS = 10


class BinanceExchange(ExchangeBase):
    name = "BINANCE"

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._testnet = testnet
        self._client = None
        self._running = False
        # Single stop-event owned here; stream_events watches it.
        self._stop_event: asyncio.Event = asyncio.Event()
        # Current futures listenKey, published by the listener for the keepalive
        # task; None whenever no socket is established.
        self._futures_listen_key: str | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            from aiohttp import ClientTimeout
            from binance import AsyncClient
        except ImportError:
            logger.error(
                "python-binance is not installed. "
                "Run: pip install python-binance"
            )
            return

        self._client = await AsyncClient.create(
            self._api_key,
            self._api_secret,
            testnet=self._testnet,
            session_params={"timeout": ClientTimeout(total=REST_TIMEOUT_SECONDS)},
        )
        self._running = True
        self._stop_event.clear()
        logger.info("BinanceExchange started (testnet={})", self._testnet)

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()          # signals stream_events to exit cleanly
        if self._client is not None:
            try:
                await self._client.close_connection()
            except Exception as exc:
                logger.debug("Error closing Binance connection: {}", exc)
        logger.info("BinanceExchange stopped")

    # ── REST cold-start & reconciliation ──────────────────────────────────────

    async def fetch_open_orders(self) -> list[Order]:
        if self._client is None:
            return []
        orders: list[Order] = []
        # Spot tracking is intentionally disabled — futures only.
        try:
            for raw in await self._client.futures_get_open_orders():
                orders.append(self._parse_order_from_rest(raw, is_futures=True))
        except Exception as exc:
            logger.error("fetch futures open orders failed: {}", exc)
        return orders

    async def fetch_open_positions(self) -> list[Position]:
        if self._client is None:
            return []
        positions: list[Position] = []
        try:
            for raw in await self._client.futures_position_information():
                try:
                    amt = float(raw.get("positionAmt") or 0)
                except (TypeError, ValueError):
                    continue
                if amt == 0.0:
                    continue
                positions.append(self._parse_position_from_rest(raw))
        except Exception as exc:
            logger.error("fetch open positions failed: {}", exc)
        return positions

    # ── Real-time WebSocket stream ─────────────────────────────────────────────

    async def stream_events(self) -> AsyncIterator[tuple[str, Order | Position | dict]]:
        """
        Async generator that yields (event_type, payload) tuples indefinitely.

        A futures listener task pushes events into a shared queue (spot tracking
        is disabled). This method drains the queue and yields each item, stopping
        cleanly when stop() is called.

        Reconnection uses exponential backoff: wait = min(2^attempt, 60) seconds.
        The counter resets after any session that stayed up for
        WS_STABLE_SESSION_SECONDS, so a normal drop retries in 2s while genuine
        flapping backs off.

        CONNECTION events track the *socket*, not the message flow. A futures
        user-data stream is silent while the account is idle, so gating them on
        an incoming payload would report outages that lasted seconds as if they
        had lasted until the next trade. "restored" is therefore tied to the
        handshake — held back only long enough to confirm the session holds, and
        reporting the downtime measured to the moment the socket came back.

        The read loop performs no I/O. Any await in here stalls frame processing,
        which fills the receive buffer, pauses the transport and makes websockets
        kill the connection with a "keepalive ping timeout".
        """
        if self._client is None:
            logger.error(
                "BinanceExchange.stream_events called before start() or after a "
                "failed start(). No events will be produced."
            )
            return

        queue: asyncio.Queue[tuple[str, Order | Position | dict]] = asyncio.Queue()
        # Accumulates per-trade realized PnL (rp) from ORDER_TRADE_UPDATE so that
        # ACCOUNT_UPDATE position-close events show trade PnL rather than the
        # cumulative "cr" (Accumulated Realized Income) field.
        _last_rp: dict[str, float] = {}

        # Spot listener intentionally removed — spot tracking is disabled.

        # ── Futures listener ───────────────────────────────────────────────────
        async def _futures_listener() -> None:

            assert self._client is not None, "AsyncClient not initialized"

            attempt = 0
            lost_emitted = False
            lost_at = 0.0
            restore_task: asyncio.Task | None = None

            async def _announce_restore(reconnected_at: float) -> None:
                """Report the reconnect only once the socket has proven it holds."""
                nonlocal lost_emitted
                await asyncio.sleep(WS_STABLE_SESSION_SECONDS)
                lost_emitted = False
                await queue.put((
                    "CONNECTION",
                    {
                        "stream": "FUTURES",
                        "status": "restored",
                        "downtime": reconnected_at - lost_at,
                    },
                ))

            while self._running:
                connected_at = 0.0
                try:
                    # Get a fresh listenKey
                    listen_key = await self._client.futures_stream_get_listen_key()
                    self._futures_listen_key = listen_key
                    url = f"wss://fstream.binance.com/private/ws/{listen_key}"
                    logger.info("Futures WS connecting to: {}", url)

                    async with websockets.connect(
                        url,
                        ping_interval=WS_PING_INTERVAL,
                        ping_timeout=WS_PING_TIMEOUT,
                        open_timeout=WS_OPEN_TIMEOUT,
                        close_timeout=WS_CLOSE_TIMEOUT,
                        max_queue=WS_MAX_QUEUE,
                    ) as socket:
                        connected_at = time.monotonic()
                        logger.info("Futures WS connected")
                        if lost_emitted and restore_task is None:
                            restore_task = asyncio.create_task(
                                _announce_restore(connected_at),
                                name="binance-futures-restore-announce",
                            )

                        async for msg in socket:
                            try:
                                payload = json.loads(msg) if isinstance(msg, (str, bytes)) else msg
                            except Exception:
                                continue
                            if not isinstance(payload, dict):
                                continue
                            et = payload.get("e")
                            if et == "ORDER_TRADE_UPDATE":
                                inner = payload.get("o") or {}
                                try:
                                    rp_val = float(inner.get("rp") or 0)
                                    rp_sym = str(inner.get("s") or "")
                                    rp_ps  = str(inner.get("ps") or "BOTH")
                                    if rp_sym:
                                        rp_key = f"{rp_sym}_{rp_ps}"
                                        _last_rp[rp_key] = _last_rp.get(rp_key, 0.0) + rp_val
                                except (TypeError, ValueError):
                                    pass
                                order = self._parse_order_from_ws(
                                    inner,
                                    is_futures=True,
                                    event_time=int(payload.get("E") or 0),
                                )
                                await queue.put(("ORDER_UPDATE", order))
                            elif et == "ACCOUNT_UPDATE":
                                acc = payload.get("a") or {}
                                for p in acc.get("P") or []:
                                    try:
                                        float(p.get("pa") or 0)
                                    except (TypeError, ValueError):
                                        continue
                                    pos = self._parse_position_from_ws(p)
                                    if not pos.is_open:
                                        rp_key = f"{pos.symbol}_{pos.position_side.value}"
                                        if rp_key in _last_rp:
                                            pos = dc_replace(pos, realised_pnl=_last_rp.pop(rp_key))
                                    await queue.put(("POSITION_UPDATE", pos))
                            elif et == "listenKeyExpired":
                                logger.warning("Futures listenKey expired, reconnecting")
                                break

                except asyncio.CancelledError:
                    if restore_task is not None:
                        restore_task.cancel()
                    return
                except Exception as exc:
                    # One alert per outage; cleared once a reconnect is confirmed.
                    if not lost_emitted:
                        lost_emitted = True
                        lost_at = time.monotonic()
                        await queue.put(("CONNECTION", {"stream": "FUTURES", "status": "lost", "error": str(exc)}))
                    logger.error("Futures WS error: {}", exc)
                finally:
                    self._futures_listen_key = None
                    # Died before the reconnect was confirmed — stay in the
                    # "lost" state rather than announcing a recovery.
                    if restore_task is not None:
                        if not restore_task.done():
                            restore_task.cancel()
                        restore_task = None

                if not self._running:
                    break

                if connected_at and time.monotonic() - connected_at >= WS_STABLE_SESSION_SECONDS:
                    attempt = 0
                attempt += 1
                wait = min(2 ** attempt, WS_MAX_BACKOFF)
                logger.info("Futures WS reconnecting in {}s (attempt {})", wait, attempt)
                await asyncio.sleep(wait)

        # ── listenKey keepalive ────────────────────────────────────────────────
        # Extends the key the listener is currently using. Skipped while no
        # socket is up — the listener fetches a fresh key on every reconnect.
        async def _futures_keepalive() -> None:

            assert self._client is not None, "AsyncClient not initialized"

            while self._running:
                await asyncio.sleep(LISTEN_KEY_REFRESH_SECONDS)
                listen_key = self._futures_listen_key
                if listen_key is None:
                    continue
                try:
                    await self._client.futures_stream_keepalive(listen_key)
                    logger.debug("Futures listenKey refreshed")
                except Exception as exc:
                    logger.error("Futures listenKey keepalive failed: {}", exc)

        # ── Run listeners + drain queue ────────────────────────────────────────
        tasks = [
            asyncio.create_task(_futures_listener(),   name="binance-futures-listener"),
            asyncio.create_task(_futures_keepalive(),  name="binance-futures-keepalive"),
        ]
        try:
            while self._running:
                # Use wait_for so we can check _running / _stop_event periodically
                # even when the queue is quiet.
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield item
                except asyncio.TimeoutError:
                    continue
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Parsing helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _ts(ms: int | str) -> datetime:
        return datetime.utcfromtimestamp(int(ms) / 1_000)

    def _parse_order_from_ws(
        self,
        raw: dict,
        is_futures: bool,
        event_time: int = 0,
    ) -> Order:
        """
        Parse a WebSocket order payload.

        Spot executionReport field map:
          i=orderId, c=clientOrderId, s=symbol, S=side,
          o=orderType (NOT a nested dict here),
          X=status, p=price, q=origQty, z=executedQty,
          T=transactionTime, E=eventTime

        Futures ORDER_TRADE_UPDATE inner "o" field map:
          i=orderId, c=clientOrderId, s=symbol, S=side,
          o=orderType, X=status, p=price, q=origQty, z=filledAccumulatedQty,
          T=tradeTime, (E is on the outer envelope, passed as event_time)
        """
        try:
            status_enum = OrderStatus(raw.get("X") or raw.get("status") or "NEW")
        except ValueError:
            status_enum = OrderStatus.NEW

        transaction_ms = int(raw.get("T") or raw.get("time") or 0)
        event_ms = event_time or int(raw.get("E") or 0)

        time_dt = self._ts(transaction_ms) if transaction_ms else datetime.utcnow()
        # update_time comes from the event envelope timestamp, not the transaction time
        update_dt = self._ts(event_ms) if event_ms else time_dt

        update_type = str(raw.get("x") or raw.get("X") or "").upper() or None

        ps_raw = raw.get("ps") if is_futures else None
        order_position_side: PositionSide | None = None
        if ps_raw:
            try:
                order_position_side = PositionSide(ps_raw)
            except ValueError:
                pass

        return Order(
            order_id=int(raw.get("i") or raw.get("orderId") or 0),
            client_order_id=str(raw.get("c") or raw.get("clientOrderId") or ""),
            symbol=str(raw.get("s") or raw.get("symbol") or ""),
            side=str(raw.get("S") or raw.get("side") or ""),
            order_type=str(raw.get("o") or raw.get("type") or ""),  # "o" = orderType in WS
            status=status_enum,
            price=float(raw.get("p") or raw.get("price") or 0),
            asks_notional=float(raw.get("a") or raw.get("bidNotional") or 0),
            bids_notional=float(raw.get("b") or raw.get("askNotional") or 0),
            orig_qty=float(raw.get("q") or raw.get("origQty") or 0),
            executed_qty=float(raw.get("z") or raw.get("executedQty") or 0),
            is_reduce_only=bool(raw.get("R") or raw.get("reduceOnly") or False),
            time=time_dt,
            update_time=update_dt,
            exchange=self.name,
            is_futures=is_futures,
            update_type=update_type,
            position_side=order_position_side,
        )

    def _parse_position_from_ws(self, raw: dict) -> Position:
        """
        Parse a position entry from a futures ACCOUNT_UPDATE payload.

        ACCOUNT_UPDATE position fields:
          s=symbol, pa=positionAmt, ep=entryPrice, up=unrealizedPnL,
          mt=marginType, iw=isolatedWallet, ps=positionSide

        NOTE: liquidationPrice is NOT present in ACCOUNT_UPDATE; it defaults to
        0 and callers should treat that as unavailable rather than meaningful.
        """
        try:
            amt = float(raw.get("pa") or 0)
        except (TypeError, ValueError):
            amt = 0.0

        try:
            entry = float(raw.get("ep") or 0)
        except (TypeError, ValueError):
            entry = 0.0

        try:
            pnl = float(raw.get("cr") or 0)
        except (TypeError, ValueError):
            pnl = 0.0

        position_side_raw = raw.get("ps") or ("LONG" if amt >= 0 else "SHORT")
        try:
            position_side = PositionSide(position_side_raw)
        except ValueError:
            position_side = PositionSide.LONG if amt >= 0 else PositionSide.SHORT

        return Position(
            symbol=str(raw.get("s") or ""),
            position_side=position_side,
            entry_price=entry,
            mark_price=0.0,           # not available in ACCOUNT_UPDATE
            position_amt=amt,
            realised_pnl=pnl,
            unrealised_pnl=0.0,      # not available in ACCOUNT_UPDATE
            margin_type=str(raw.get("mt") or "cross"),
            liquidation_price=0.0,    # not available in ACCOUNT_UPDATE
            exchange=self.name,
            update_time=datetime.utcnow(),
        )

    def _parse_order_from_rest(self, raw: dict, is_futures: bool) -> Order:
        try:
            status_enum = OrderStatus(raw.get("status") or "NEW")
        except ValueError:
            status_enum = OrderStatus.NEW

        time_ms = int(raw.get("time") or 0)
        update_ms = int(raw.get("updateTime") or 0)
        time_dt = self._ts(time_ms) if time_ms else datetime.utcnow()
        update_dt = self._ts(update_ms) if update_ms else time_dt

        rest_ps_raw = raw.get("positionSide") if is_futures else None
        rest_order_position_side: PositionSide | None = None
        if rest_ps_raw:
            try:
                rest_order_position_side = PositionSide(rest_ps_raw)
            except ValueError:
                pass

        return Order(
            order_id=int(raw.get("orderId") or 0),
            client_order_id=str(raw.get("clientOrderId") or ""),
            symbol=str(raw.get("symbol") or ""),
            side=str(raw.get("side") or ""),
            order_type=str(raw.get("type") or ""),
            status=status_enum,
            price=float(raw.get("price") or 0),
            asks_notional=0,
            bids_notional=0,
            orig_qty=float(raw.get("origQty") or 0),
            executed_qty=float(raw.get("executedQty") or 0),
            time=time_dt,
            update_time=update_dt,
            exchange=self.name,
            is_futures=is_futures,
            position_side=rest_order_position_side,
        )

    def _parse_position_from_rest(self, raw: dict) -> Position:
        try:
            amt = float(raw.get("positionAmt") or 0)
        except (TypeError, ValueError):
            amt = 0.0

        # Guard against Binance returning the string "0" for entryPrice on
        # positions that exist but haven't been filled yet. float("0") is 0.0,
        # which is valid here — the `or` short-circuit would wrongly replace it
        # with markPrice, so we use explicit None checks instead.
        entry_raw = raw.get("entryPrice")
        try:
            entry = float(entry_raw) if entry_raw is not None else 0.0
        except (TypeError, ValueError):
            entry = 0.0

        try:
            mark = float(raw.get("markPrice") or 0)
        except (TypeError, ValueError):
            mark = 0.0

        try:
            pnl = float(raw.get("unRealizedProfit") or 0)
        except (TypeError, ValueError):
            pnl = 0.0

        try:
            liq = float(raw.get("liquidationPrice") or 0)
        except (TypeError, ValueError):
            liq = 0.0

        position_side_raw = raw.get("positionSide") or ("LONG" if amt >= 0 else "SHORT")
        try:
            position_side = PositionSide(position_side_raw)
        except ValueError:
            position_side = PositionSide.LONG if amt >= 0 else PositionSide.SHORT

        return Position(
            symbol=str(raw.get("symbol") or ""),
            position_side=position_side,
            entry_price=entry,
            mark_price=mark,
            position_amt=amt,
            realised_pnl=0.0,  # not available in REST endpoint, only ACCOUNT_UPDATE WS events
            unrealised_pnl=pnl,
            margin_type=str(raw.get("marginType") or "cross"),
            liquidation_price=liq,
            exchange=self.name,
            update_time=datetime.utcnow(),
        )