from __future__ import annotations

import asyncio
import websockets
import json

from collections.abc import AsyncIterator
from datetime import datetime


from loguru import logger

from ..models import Order, OrderStatus, Position, PositionSide
from .base import ExchangeBase


class BinanceExchange(ExchangeBase):
    name = "BINANCE"

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._testnet = testnet
        self._client = None
        self._bsm = None
        self._running = False
        # Single stop-event owned here; stream_events watches it.
        self._stop_event: asyncio.Event = asyncio.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            from binance import AsyncClient, BinanceSocketManager
        except ImportError:
            logger.error(
                "python-binance is not installed. "
                "Run: pip install python-binance"
            )
            return

        self._client = await AsyncClient.create(
            self._api_key, self._api_secret, testnet=self._testnet
        )
        self._bsm = BinanceSocketManager(self._client)
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
        try:
            for raw in await self._client.get_open_orders():
                orders.append(self._parse_order_from_rest(raw, is_futures=False))
        except Exception as exc:
            logger.error("fetch spot open orders failed: {}", exc)
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

    async def stream_events(self) -> AsyncIterator[tuple[str, Order | Position]]:
        """
        Async generator that yields (event_type, payload) tuples indefinitely.

        Two background tasks (spot + futures listeners) push events into a shared
        queue. This method drains the queue and yields each item, stopping cleanly
        when stop() is called.

        Reconnection uses exponential backoff: wait = min(2^attempt, 60) seconds.
        The backoff counter resets only after the first successful message.

        listenKey keepalive is done correctly by passing the key that the socket
        context manager exposes, rather than calling the no-arg variant that raises
        TypeError.
        """
        if self._client is None or self._bsm is None:
            logger.error(
                "BinanceExchange.stream_events called before start() or after a "
                "failed start(). No events will be produced."
            )
            return

        queue: asyncio.Queue[tuple[str, Order | Position]] = asyncio.Queue()

        # ── Spot listener ──────────────────────────────────────────────────────
        async def _spot_listener() -> None:

            assert self._bsm is not None, "BinanceSocketManager not initialized"

            attempt = 0

            while self._running:
                try:
                    async with self._bsm.user_socket() as socket:
                        got_first = False
                        while self._running:
                            msg = await socket.recv()
                            if not got_first:
                                attempt = 0
                                got_first = True
                            payload = msg if isinstance(msg, dict) else {}
                            et = payload.get("e")
                            if et == "executionReport":
                                order = self._parse_order_from_ws(payload, is_futures=False)
                                await queue.put(("ORDER_UPDATE", order))
                            elif et == "listenKeyExpired":
                                # Key expired — break inner loop to force reconnect
                                logger.warning("Spot listenKey expired, reconnecting")
                                break
                            elif et == "error":
                                logger.error("Spot WS error event: {}", payload)
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    attempt += 1
                    wait = min(2 ** attempt, 60)
                    logger.debug("Spot WS disconnected (attempt {}), retrying in {}s: {}", attempt, wait, exc)
                    await asyncio.sleep(wait)

        # ── Futures listener ───────────────────────────────────────────────────
        async def _futures_listener() -> None:

            assert self._client is not None, "AsyncClient not initialized"

            attempt = 0
            while self._running:
                try:
                    # Get a fresh listenKey
                    listen_key = await self._client.futures_stream_get_listen_key()
                    url = f"wss://fstream.binance.com/private/ws/{listen_key}"
                    logger.info("Futures WS connecting to: {}", url)

                    async with websockets.connect(url) as socket:
                        got_first = False
                        while self._running:
                            try:
                                msg = await asyncio.wait_for(socket.recv(), timeout=30.0)
                            except asyncio.TimeoutError:
                                # Send a ping to keep the connection alive
                                await socket.ping()
                                continue
                            if not got_first:
                                attempt = 0
                                got_first = True
                            try:
                                payload = json.loads(msg) if isinstance(msg, str) else msg
                            except Exception:
                                continue
                            et = payload.get("e")
                            if et == "ORDER_TRADE_UPDATE":
                                inner = payload.get("o") or {}
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
                                        amt = float(p.get("pa") or 0)
                                    except (TypeError, ValueError):
                                        continue
                                    pos = self._parse_position_from_ws(p)
                                    await queue.put(("POSITION_UPDATE", pos))
                            elif et == "listenKeyExpired":
                                logger.warning("Futures listenKey expired, reconnecting")
                                break

                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    attempt += 1
                    wait = min(2 ** attempt, 60)
                    logger.error(
                        "Futures WS error (attempt {}), retrying in {}s: {}",
                        attempt, wait, exc,
                    )
                    await asyncio.sleep(wait)

        # ── listenKey keepalive ────────────────────────────────────────────────
        # python-binance does not expose the listenKey after socket creation, but
        # it provides dedicated keepalive endpoints we can call with a fresh fetch.
        async def _spot_keepalive() -> None:

            assert self._client is not None, "AsyncClient not initialized"

            while self._running:
                await asyncio.sleep(30 * 60)
                try:
                    # Correct method name on AsyncClient
                    await self._client.stream_get_listen_key()
                    logger.debug("Spot listenKey refreshed")
                except Exception as exc:
                    logger.error("Spot listenKey keepalive failed: {}", exc)

        async def _futures_keepalive() -> None:

            assert self._client is not None, "AsyncClient not initialized"

            while self._running:
                await asyncio.sleep(30 * 60)
                try:
                    # Correct method name on AsyncClient
                    await self._client.futures_stream_get_listen_key()
                    logger.debug("Futures listenKey refreshed")
                except Exception as exc:
                    logger.error("Futures listenKey keepalive failed: {}", exc)

        # ── Run listeners + drain queue ────────────────────────────────────────
        tasks = [
            asyncio.create_task(_spot_listener(),      name="binance-spot-listener"),
            asyncio.create_task(_futures_listener(),   name="binance-futures-listener"),
            asyncio.create_task(_spot_keepalive(),     name="binance-spot-keepalive"),
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

        return Order(
            order_id=int(raw.get("i") or raw.get("orderId") or 0),
            client_order_id=str(raw.get("c") or raw.get("clientOrderId") or ""),
            symbol=str(raw.get("s") or raw.get("symbol") or ""),
            side=str(raw.get("S") or raw.get("side") or ""),
            order_type=str(raw.get("o") or raw.get("type") or ""),  # "o" = orderType in WS
            status=status_enum,
            price=float(raw.get("p") or raw.get("price") or 0),
            orig_qty=float(raw.get("q") or raw.get("origQty") or 0),
            executed_qty=float(raw.get("z") or raw.get("executedQty") or 0),
            time=time_dt,
            update_time=update_dt,
            exchange=self.name,
            is_futures=is_futures,
            update_type=update_type,
        )

    def _parse_position_from_ws(self, raw: dict) -> Position:
        """
        Parse a position entry from a futures ACCOUNT_UPDATE payload.

        ACCOUNT_UPDATE position fields:
          s=symbol, pa=positionAmt, ep=entryPrice, up=unrealizedPnL,
          mt=marginType, iw=isolatedWallet, ps=positionSide

        NOTE: leverage and liquidationPrice are NOT present in ACCOUNT_UPDATE.
        They default to 1 and 0 respectively; callers should treat them as
        unavailable rather than meaningful values.
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
            leverage=1,               # not available in ACCOUNT_UPDATE
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

        return Order(
            order_id=int(raw.get("orderId") or 0),
            client_order_id=str(raw.get("clientOrderId") or ""),
            symbol=str(raw.get("symbol") or ""),
            side=str(raw.get("side") or ""),
            order_type=str(raw.get("type") or ""),
            status=status_enum,
            price=float(raw.get("price") or 0),
            orig_qty=float(raw.get("origQty") or 0),
            executed_qty=float(raw.get("executedQty") or 0),
            time=time_dt,
            update_time=update_dt,
            exchange=self.name,
            is_futures=is_futures,
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
            lev = int(float(raw.get("leverage") or 1))
        except (TypeError, ValueError):
            lev = 1

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
            leverage=lev,
            margin_type=str(raw.get("marginType") or "cross"),
            liquidation_price=liq,
            exchange=self.name,
            update_time=datetime.utcnow(),
        )