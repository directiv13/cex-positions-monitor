# Binance Monitor Telegram Bot

Lightweight async bot that monitors Binance **Futures** for order and position changes, posts event notifications to a Telegram channel, maintains a pinned dashboard message, and forwards push notifications via Pushover.

> Spot tracking is intentionally disabled — the bot watches futures orders and positions only.

Requirements

- Python 3.12+
- Install pinned dependencies:

```bash
python -m pip install -r requirements.txt
```

Setup

1. Copy `.env.example` to `.env` and fill in credentials and IDs.
2. Ensure the bot has permission to post and pin messages in the target channel.

Running

```bash
# set up virtualenv (recommended)
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Notes

- All I/O is async; the `BinanceExchange` uses `python-binance` for REST and a raw `websockets` futures user-data stream.
- Notifications are sent only from live WebSocket events. The periodic REST reconciliation pass updates in-memory state (and therefore the pinned dashboard) **silently** — it never posts messages or pushes, avoiding misleading "filled vs. canceled" guesses.
- Dashboard PnL, mark price and liquidation price come from that reconciliation pass; Binance does not include them in live position events. They refresh on the `REST_RECONCILE_INTERVAL` cadence, not per event. Position leverage is not tracked.
- This repository is scaffolded to allow adding new exchanges under `src/exchange/` by implementing `ExchangeBase`.

Connection health

- If the futures WebSocket drops, the bot posts a ⚠️ disconnect alert to the channel with the underlying error, and a ✅ alert once it is back. Health alerts are sent even while notifications are paused.
- The ✅ alert reports how long the socket was actually down (`Down for 4s`). It is held back ~30s to confirm the connection holds, so an unstable link produces a single ⚠️ rather than a stream of alternating alerts — expect the ✅ to arrive a little after recovery, with an accurate downtime figure.
- Reconnects use exponential backoff (2s → 60s). A connection that stayed up resets the backoff, so an ordinary drop is retried within seconds.
- Alerts track the socket itself, not incoming trade activity: the stream is silent while the account is idle, so an outage is reported by its real duration rather than by the gap until your next trade.