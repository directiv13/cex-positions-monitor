from __future__ import annotations

import asyncio
import signal
from loguru import logger

from config import settings
from src.logger import setup_logger
from src.exchange.binance import BinanceExchange
from src.models import OrderStatus
from src.monitor import ChangeMonitor
from src.pushover import PushoverNotifier
from src.telegram_bot import TelegramBot


async def main() -> None:
    setup_logger(settings.LOG_LEVEL, settings.LOG_FILE)
    logger.info("Starting bot")

    exchange = BinanceExchange(settings.BINANCE_API_KEY, settings.BINANCE_API_SECRET, settings.BINANCE_TESTNET)
    await exchange.start()

    pushover = PushoverNotifier(settings.PUSHOVER_APP_TOKEN, settings.PUSHOVER_USER_KEY)

    monitor = ChangeMonitor(exchange, reconcile_interval=settings.REST_RECONCILE_INTERVAL)

    telegram = TelegramBot(
        settings.TELEGRAM_BOT_TOKEN,
        settings.allowed_users(),
        settings.TELEGRAM_CHANNEL_ID,
        settings.TELEGRAM_CHANNEL_THREAD_ID,
        exchange,
        monitor,
        settings.TELEGRAM_PINNED_MESSAGE_ID,
    )

    # Event callback wiring
    async def order_cb(order, event):
        if order.status == OrderStatus.PARTIALLY_FILLED:
            return

        txt = order.short_repr() + " | event=" + event
        await telegram.broadcast_to_channel(txt)

        if event in (
            "OPENED",
            "FILLED",
            "CANCELED",
            "REJECTED",
            "EXPIRED",
            "UPDATED",
            "STATUS_CHANGED:NEW",
            "STATUS_CHANGED:CANCELED",
            "STATUS_CHANGED:REJECTED",
            "STATUS_CHANGED:EXPIRED",
        ):
            snapshot = monitor.current_state()
            open_orders = [o for o in snapshot.orders.values() if o.is_open]
            await telegram.update_dashboard(open_orders, list(snapshot.positions.values()))

    async def position_cb(position, event):
        txt = position.short_repr() + " | event=" + event
        await telegram.broadcast_to_channel(txt)

        if event in ("OPENED", "CLOSED"):
            snapshot = monitor.current_state()
            open_orders = [o for o in snapshot.orders.values() if o.is_open]
            await telegram.update_dashboard(open_orders, list(snapshot.positions.values()))

    monitor.on_order_event(order_cb)
    monitor.on_position_event(position_cb)

    loop = asyncio.get_event_loop()

    async def _shutdown():
        logger.info("Shutting down")
        await monitor.stop()
        await telegram.shutdown()
        await exchange.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))
        except NotImplementedError:
            # Windows sometimes raises
            pass

    await asyncio.gather(monitor.start(), telegram.run_polling())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
