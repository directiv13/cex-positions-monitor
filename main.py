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

    if settings.DASHBOARD_POLL_INTERVAL < 10:
        logger.warning(
            "DASHBOARD_POLL_INTERVAL=%s is below the recommended minimum of 10s. Telegram may rate-limit editMessageText calls.",
            settings.DASHBOARD_POLL_INTERVAL,
        )

    telegram = TelegramBot(
        settings.TELEGRAM_BOT_TOKEN,
        settings.allowed_users(),
        settings.TELEGRAM_CHANNEL_ID,
        settings.TELEGRAM_CHANNEL_THREAD_ID,
        exchange,
        monitor,
        settings.DASHBOARD_POLL_INTERVAL,
        settings.TELEGRAM_PINNED_MESSAGE_ID,
    )

    def make_order_callback(telegram_bot: TelegramBot):
        async def _cb(order, event):
            if order.status == OrderStatus.PARTIALLY_FILLED:
                return
            plain = order.short_repr() + " | event=" + event

            # Send message to Telegram channel
            await telegram_bot.broadcast_message(plain)
        return _cb

    def make_position_callback(telegram_bot: TelegramBot, pushover_notifier: PushoverNotifier):
        async def _cb(position, event):
            # Send message to Telegram channel
            await telegram_bot.broadcast_message(position)

            # Send Pushover notification for position events
            if event == "OPENED":
                await pushover_notifier.notify_position_opened(position.short_repr())
            elif event == "CLOSED":
                await pushover_notifier.notify_position_closed(position.short_repr())
        return _cb

    monitor.on_order_event(make_order_callback(telegram))
    monitor.on_position_event(make_position_callback(telegram,pushover))

    async def _dashboard_refresh_loop() -> None:
        while True:
            try:
                await asyncio.sleep(settings.DASHBOARD_POLL_INTERVAL)
                if telegram._paused:
                    continue
                snapshot = monitor.current_state()
                open_orders = [o for o in snapshot.orders.values() if o.is_open]
                await telegram.update_dashboard(open_orders, list(snapshot.positions.values()))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Dashboard refresh loop error: {}", exc)

    loop = asyncio.get_event_loop()

    async def _shutdown():
        logger.info("Shutting down")
        monitor.stop()
        await telegram.shutdown()
        await exchange.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))
        except NotImplementedError:
            # Windows sometimes raises
            pass

    await asyncio.gather(monitor.start(), telegram.run_polling(), _dashboard_refresh_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
