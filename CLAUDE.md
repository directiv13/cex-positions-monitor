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
| `src/exchange/binance.py` | Concrete Binance implementation, **futures-only** (spot orders/positions tracking is intentionally disabled). The futures stream uses raw `websockets` with manual listenKey management. Events are funnelled into a shared `asyncio.Queue` and yielded from `stream_events()`; connection drops/restores are emitted as `("CONNECTION", {...})` items on the same queue. See [Futures WebSocket connection handling](#futures-websocket-connection-handling) before touching the read loop. |
| `src/monitor.py` | `ChangeMonitor` owns the live `StateSnapshot`. It consumes `stream_events()` in one task and runs a REST reconciliation loop in another (default every 120 s). **Only live WebSocket events fire notifications** (`on_order_event`/`on_position_event`); reconciliation updates in-memory state **silently** (no messages/pushes) so the pinned dashboard stays accurate without emitting misleading events. Connection events fire `on_connection_event`. |
| `src/telegram_bot.py` | PTB (`python-telegram-bot`) application. Broadcasts event messages to the channel and keeps a pinned dashboard message in sync. The `_auth` decorator restricts commands to `TELEGRAM_ALLOWED_USERS`; empty list = unrestricted. |
| `src/pushover.py` | `PushoverNotifier` sends push notifications to one or more user keys via `httpx`. |
| `src/formatter.py` | Formats `Order`/`Position` objects into Telegram HTML strings. All outbound HTML is produced here. |
| `src/logger.py` | Configures loguru: stderr + rotating file (`logs/bot.log`), daily rotation, 14-day retention, gzip compression. |

### Data flow

```
BinanceExchange.stream_events()  ──►  ChangeMonitor._stream_consumer()
                                             │
                     ┌───────────────────────┼───────────────────────┐
                     ▼                        ▼                       ▼
          _handle_order_event()   _handle_position_event()  _handle_connection_event()
                     │                        │                       │
                 callbacks                callbacks               callbacks
                     │                        │                       │
      TelegramBot.broadcast_message()  PushoverNotifier.notify_*()  TelegramBot.broadcast_message()
                                                                    (WS lost/restored alert)

REST reconciliation (every 120s):
  fetch_open_orders/positions  ──►  _diff_orders/_diff_positions  ──►  update StateSnapshot only
                                                                       (NO notifications — dashboard only)
```

Reconciliation is deliberately silent: `fetch_open_orders` returns only *open* orders, so an order that vanishes from it could have been filled **or** canceled/expired — indistinguishable at that layer. Rather than guess (which produced false "FILLED" messages), reconciliation just syncs state; all notifications come from live WebSocket events that carry the true status.

The `StateSnapshot` is the single source of truth for in-memory state; `ChangeMonitor.current_state()` returns a deep copy so callers cannot mutate it.

### REST-only position fields

`ACCOUNT_UPDATE` carries no `mark_price`, `liquidation_price` or `unrealised_pnl` — the WS parser leaves them at `0.0`. Reconciliation is what fills them in (`_diff_positions`, case D), and `_handle_position_event` carries `liquidation_price` over from existing state so a WS update doesn't blank it. **Any new REST-only field must be added to both places**, or the dashboard will show zeros from the first WS event onwards.

Leverage is deliberately *not* tracked. It isn't in `ACCOUNT_UPDATE`, and the only way to get it per event was a REST call from inside the WS read loop — which is what caused the disconnects described below. Don't reintroduce it without a cache the read loop can consult synchronously.

### Futures WebSocket connection handling

Three invariants, all learned from production failures:

1. **The read loop must never await I/O.** `websockets` buffers `WS_MAX_QUEUE` frames, then calls `transport.pause_reading()` — which stops *all* frames being read, including pongs, so its own keepalive kills the connection with `sent 1011 (internal error) keepalive ping timeout`. A single slow REST call in the loop is enough. Enrich events downstream (in `ChangeMonitor` or the reconcile pass) instead.
2. **Connection events track the socket, not message flow.** A futures user-data stream sends nothing while the account is idle, so "have we received a payload?" is not a liveness signal — gating the ✅ on one reported a 4-second outage as a 36-minute one. `restored` is tied to the handshake and carries `downtime` measured to the moment the socket came back.
3. **`restored` is held back `WS_STABLE_SESSION_SECONDS` to confirm the session holds.** Without it, a flapping link alternates ⚠️/✅ every few seconds. The same threshold gates the backoff reset: a session that held that long retries in 2s, anything shorter keeps escalating `min(2^n, 60)s`. One ⚠️ per outage, no ✅ until it is genuinely back.

Tuning lives in module constants at the top of `binance.py` (`WS_PING_INTERVAL`/`WS_PING_TIMEOUT` 20/60, `WS_MAX_QUEUE`, `WS_STABLE_SESSION_SECONDS`, `REST_TIMEOUT_SECONDS`). `REST_TIMEOUT_SECONDS` is passed to `AsyncClient.create` as an aiohttp `ClientTimeout`; without it aiohttp allows 5 minutes per request, long enough for one hung call to stall the reconnect loop.

The listener publishes its current key to `self._futures_listen_key`; `_futures_keepalive` `PUT`s that key every `LISTEN_KEY_REFRESH_SECONDS` (keys expire after 60 min) and skips while no socket is up.

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
