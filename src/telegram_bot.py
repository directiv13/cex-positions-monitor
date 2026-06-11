from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable

from loguru import logger

from .formatter import dashboard_message, orders_list_message, positions_list_message
from .models import Order, Position


def _auth(allowed_users: list[int]):
    """
    Decorator that rejects Telegram commands from users not in allowed_users.
    An empty allowed_users list means unrestricted access (useful for dev/testnet).
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(update, context):
            if allowed_users:
                uid = update.effective_user.id if update.effective_user else None
                if uid not in allowed_users:
                    await update.message.reply_text("⛔ Unauthorized.")
                    return
            return await func(update, context)
        return wrapper
    return decorator


class TelegramBot:
    def __init__(
        self,
        token: str,
        allowed_users: list[int],
        channel_id: str,
        channel_thread_id: int,
        exchange,
        change_monitor,
        dashboard_poll_interval: int = 30,
        pinned_message_id: int = 0,
    ) -> None:
        self._token = token
        self._allowed_users = allowed_users
        self._channel_id = channel_id
        self._channel_thread_id = channel_thread_id or None  # 0 → None (general channel)
        self._exchange = exchange
        self._monitor = change_monitor
        self._dashboard_poll_interval = dashboard_poll_interval
        # Use configured pinned id (0 == not set)
        self._pinned_message_id: int | None = pinned_message_id or None
        self._paused = False
        self._app = None

    # ── Setup ──────────────────────────────────────────────────────────────────

    async def _build_app(self) -> None:
        """Build the PTB Application and register all command handlers."""
        try:
            from telegram.ext import Application, CommandHandler
        except ImportError:
            logger.error(
                "python-telegram-bot is not installed. "
                "Run: pip install python-telegram-bot"
            )
            return

        self._app = Application.builder().token(self._token).build()
        auth = _auth(self._allowed_users)

        @auth
        async def cmd_start(update, context):
            await update.message.reply_html(
                "<b>👋 Binance Monitor Bot</b>\n\n"
                "Monitoring your exchange account and posting updates to the channel.\n\n"
                "Use /help to see available commands."
            )

        @auth
        async def cmd_help(update, context):
            await update.message.reply_html(
                "<b>Available Commands</b>\n\n"
                "/status    – Monitor and connection status\n"
                "/orders    – Current open orders\n"
                "/positions – Current open futures positions\n"
                "/dashboard – Force-refresh the pinned dashboard\n"
                "/pause     – Pause event notifications\n"
                "/resume    – Resume event notifications\n"
                "/help      – This message"
            )

        @auth
        async def cmd_status(update, context):
            state = "⏸ Paused" if self._paused else "▶️ Running"
            pinned = (
                f"message ID <code>{self._pinned_message_id}</code>"
                if self._pinned_message_id
                else "not set yet"
            )
            await update.message.reply_html(
                f"<b>Status</b>\n\n"
                f"Monitor    : {state}\n"
                f"Exchange   : <code>{self._exchange.name}</code>\n"
                f"Channel    : <code>{self._channel_id}</code>\n"
                f"Dashboard  : {pinned}\n"
                f"Poll every : <b>{self._dashboard_poll_interval}s</b>\n"
                f"Paused     : {'Yes' if self._paused else 'No'}"
            )

        @auth
        async def cmd_orders(update, context):
            snap = self._monitor.current_state()
            text = orders_list_message(list(snap.orders.values()))
            await update.message.reply_html(text)

        @auth
        async def cmd_positions(update, context):
            snap = self._monitor.current_state()
            text = positions_list_message(list(snap.positions.values()))
            await update.message.reply_html(text)

        @auth
        async def cmd_dashboard(update, context):
            snap = self._monitor.current_state()
            open_orders = [o for o in snap.orders.values() if o.is_open]  # property, not method
            await self.update_dashboard(open_orders, list(snap.positions.values()))
            await update.message.reply_text("✅ Dashboard refreshed.")

        @auth
        async def cmd_pause(update, context):
            self._paused = True
            await update.message.reply_text("⏸ Dashboard updates paused.")

        @auth
        async def cmd_resume(update, context):
            self._paused = False
            await update.message.reply_text("▶️ Dashboard updates resumed.")

        for name, handler in [
            ("start",     cmd_start),
            ("help",      cmd_help),
            ("status",    cmd_status),
            ("orders",    cmd_orders),
            ("positions", cmd_positions),
            ("dashboard", cmd_dashboard),
            ("pause",     cmd_pause),
            ("resume",    cmd_resume),
        ]:
            self._app.add_handler(CommandHandler(name, handler))

    # ── Outbound: pinned dashboard ─────────────────────────────────────────────

    async def update_dashboard(
        self, orders: list[Order], positions: list[Position]
    ) -> None:
        """
        Edit the pinned dashboard message in place.

        Flow:
          - First call: send message → pin it → store message_id.
          - Subsequent calls: edit the existing message.
          - MessageNotModified (content unchanged): swallow silently.
          - Any other edit error: reset pin ID → fall through to send a fresh message.
        """
        if self._app is None:
            logger.debug("Telegram app not ready; skipping dashboard update")
            return

        html = dashboard_message(orders, positions)
        bot = self._app.bot

        if self._pinned_message_id is None:
            await self._send_and_pin(bot, html)
            return

        # Try editing in place
        try:
            await bot.edit_message_text(
                text=html,
                chat_id=self._channel_id,
                message_id=self._pinned_message_id,
                parse_mode="HTML",
            )
            return
        except Exception as exc:
            if "Message is not modified" in str(exc):
                # Content identical — not an error, nothing to do
                return
            # Message was deleted, bot lost channel access, or other real error
            logger.warning(
                "Could not edit pinned message ({}); sending a new one.", exc
            )
            self._pinned_message_id = None

        # Fallback: send fresh and pin
        await self._send_and_pin(bot, html)

    async def _send_and_pin(self, bot, html: str) -> None:
        """Send a new dashboard message and pin it."""
        try:
            msg = await bot.send_message(
                self._channel_id,
                html,
                parse_mode="HTML",
                message_thread_id=self._channel_thread_id,
            )
            self._pinned_message_id = msg.message_id
        except Exception as exc:
            logger.error("Failed to send dashboard message: {}", exc)
            return
        try:
            await bot.pin_chat_message(
                self._channel_id,
                self._pinned_message_id,
                disable_notification=True,
            )
        except Exception as exc:
            # Pinning failing is non-fatal; the message was still sent
            logger.warning("Failed to pin dashboard message: {}", exc)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run_polling(self) -> None:
        """Build the app, initialize PTB, and start polling. Blocks until cancelled."""
        await self._build_app()
        if self._app is None:
            logger.error("TelegramBot: app failed to build, polling not started")
            return
        
        assert self._app.updater is not None, "PTB app is in an unexpected state (updater missing)"

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram polling started")
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            return
        
    async def broadcast_message(self, text: str) -> None:
        """Send a plain text message to the channel."""
        if self._paused:
            logger.debug("Bot is paused; skipping broadcast message")
            return
        
        if self._app is None:
            logger.debug("Telegram app not ready; skipping broadcast")
            return
        
        try:
            await self._app.bot.send_message(
                self._channel_id,
                text,
                parse_mode="HTML",
                message_thread_id=self._channel_thread_id,
            )
        except Exception as exc:
            logger.error("Failed to send broadcast message: {}", exc)

    async def shutdown(self) -> None:
        if self._app is None:
            return
        
        assert self._app.updater is not None, "PTB app is in an unexpected state (updater missing)"

        try:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info("Telegram bot shut down")
        except Exception as exc:
            logger.error("Telegram shutdown error: {}", exc)