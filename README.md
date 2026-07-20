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
- If the futures WebSocket connection drops, the bot posts a disconnect alert to the channel (and a follow-up when it reconnects). These health alerts are sent even while notifications are paused.
- This repository is scaffolded to allow adding new exchanges under `src/exchange/` by implementing `ExchangeBase`.