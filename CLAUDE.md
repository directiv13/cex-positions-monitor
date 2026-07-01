# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the bot

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
python main.py
```

Copy `.env.example` to `.env` and fill in credentials before running. There are no automated tests.

## Docker

```bash
docker compose up -d --build    # start
docker compose logs -f bot      # tail logs
docker compose down             # stop
```

Deployment: pushes to `main` trigger a GitHub Actions workflow (`.github/workflows/deploy.yml`) that SCPs the repo to a VPS and runs `docker compose up -d --build`.

## Architecture

The bot is a single-process, fully async Python application (`asyncio`). `main.py` wires together all components and runs `asyncio.gather(monitor.start(), telegram.run_polling(), _dashboard_refresh_loop())`.

### Component overview

| Module | Role |
|---|---|
| `config/settings.py` | Pydantic `BaseSettings`; all config is read from `.env`. Exposes `settings` singleton. |
| `src/models.py` | Pure dataclasses: `Order`, `Position`, `StateSnapshot`. No persistence. |
| `src/exchange/base.py` | `ExchangeBase` ABC. Implement `start/stop/fetch_open_orders/fetch_open_positions/stream_events` to add a new exchange. |
| `src/exchange/binance.py` | Concrete Binance implementation. Spot stream uses `python-binance`; futures stream uses raw `websockets` with manual listenKey management. Both reconnect with exponential backoff (`min(2^n, 60)s`). Events are funnelled into a shared `asyncio.Queue` and yielded from `stream_events()`. |
| `src/monitor.py` | `ChangeMonitor` owns the live `StateSnapshot`. It consumes `stream_events()` in one task and runs a REST reconciliation loop in another (default every 120 s). Reconciliation diffs the REST snapshot against in-memory state to catch missed WebSocket events, then fires `on_order_event`/`on_position_event` callbacks. |
| `src/telegram_bot.py` | PTB (`python-telegram-bot`) application. Broadcasts event messages to the channel and keeps a pinned dashboard message in sync. The `_auth` decorator restricts commands to `TELEGRAM_ALLOWED_USERS`; empty list = unrestricted. |
| `src/pushover.py` | `PushoverNotifier` sends push notifications to one or more user keys via `httpx`. |
| `src/formatter.py` | Formats `Order`/`Position` objects into Telegram HTML strings. All outbound HTML is produced here. |
| `src/logger.py` | Configures loguru: stderr + rotating file (`logs/bot.log`), daily rotation, 14-day retention, gzip compression. |

### Data flow

```
BinanceExchange.stream_events()  ──►  ChangeMonitor._stream_consumer()
                                             │
                               ┌─────────────┴──────────────┐
                               ▼                            ▼
                    _handle_order_event()        _handle_position_event()
                               │                            │
                          callbacks                    callbacks
                               │                            │
                    TelegramBot.broadcast_message()   PushoverNotifier.notify_*()

REST reconciliation (every 120s):
  fetch_open_orders/positions  ──►  _diff_orders/_diff_positions  ──►  same callbacks
```

The `StateSnapshot` is the single source of truth for in-memory state; `ChangeMonitor.current_state()` returns a deep copy so callers cannot mutate it.

### Adding a new exchange

1. Create `src/exchange/<name>.py` implementing `ExchangeBase`.
2. Instantiate it in `main.py` in place of (or alongside) `BinanceExchange`.
3. `ChangeMonitor` is exchange-agnostic; no changes needed there.

### Key env vars

| Variable | Default | Notes |
|---|---|---|
| `DASHBOARD_POLL_INTERVAL` | `30` | Seconds between dashboard edits. Minimum recommended: 10 (Telegram rate limit). |
| `REST_RECONCILE_INTERVAL` | `120` | Seconds between REST reconciliation passes. |
| `TELEGRAM_PINNED_MESSAGE_ID` | `0` | Pre-set the pinned message ID to avoid sending a fresh one on restart. |
| `TELEGRAM_CHANNEL_THREAD_ID` | `0` | Set to a topic thread ID for forum-style channels; `0` = general. |
| `PUSHOVER_USER_KEYS` | `""` | Comma-separated list; all keys receive every notification. |
| `BINANCE_TESTNET` | `False` | Routes all Binance calls to the testnet endpoints. |
