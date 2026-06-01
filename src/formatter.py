from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from .models import Order, Position


def _utcnow_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_price(p: float) -> str:
    if p == 0:
        return "MARKET"
    s = f"{p:.8f}".rstrip("0").rstrip(".")
    return s


def order_event_message(order: Order, event: str) -> str:
    header = "📬 Order Opened"
    if event in ("FILLED", "STATUS_CHANGED:FILLED"):
        header = "✅ Order Filled"
    elif event in ("CANCELED", "STATUS_CHANGED:CANCELED"):
        header = "❌ Order Cancelled"
    elif event == "REJECTED":
        header = "🚫 Order Rejected"
    elif event == "EXPIRED":
        header = "⏱ Order Expired"    
    elif event == "UPDATED":
        header = "🔄 Order Updated"    
    elif event in ("PARTIALLY_FILLED", "STATUS_CHANGED:PARTIALLY_FILLED"):
        header = "🔄 Partially Filled"

    side_emoji = "🟢" if order.side.upper().startswith("BUY") else "🔴"
    price = _fmt_price(order.price)
    txt = (
        f"<b>{header}</b>\n"
        f"<i>Exchange:</i> <b>{order.exchange}</b> — <i>Market:</i> <b>{'FUT' if order.is_futures else 'SPOT'}</b>\n"
        f"{side_emoji} <b>{order.side}</b> <code>{order.orig_qty}</code> <code>{order.symbol}</code> @ <code>{price}</code>\n"
        f"Status: <b>{order.status}</b> — <code>{order.order_id}</code>\n"
        f"<i>{_utcnow_str()}</i>"
    )
    return txt


def position_event_message(position: Position, event: str) -> str:
    header = "📈 Position Opened" if event == "OPENED" else "📉 Position Closed"
    dir_emoji = "📈" if position.position_amt > 0 else "📉"
    txt = (
        f"<b>{header}</b>\n"
        f"{dir_emoji} <b>{position.direction}</b> <code>{position.position_amt}</code> <code>{position.symbol}</code>\n"
        f"entry=<b>{_fmt_price(position.entry_price)}</b> mark=<b>{_fmt_price(position.mark_price)}</b> liq=<b>{_fmt_price(position.liquidation_price)}</b>\n"
        f"PnL: <b>{position.unrealised_pnl:+.2f} USDT</b> leverage=<b>{position.leverage}</b> margin=<b>{position.margin_type}</b>\n"
        f"<i>{_utcnow_str()}</i>"
    )
    return txt


def dashboard_message(orders: List[Order], positions: List[Position]) -> str:
    if not orders and not positions:
        return "✅ No open positions or orders."

    parts = ["<b>📊 Live Dashboard</b>", f"<i>Updated: {_utcnow_str()}</i>", ""]
    if positions:
        parts.append("<b>📈 Open Positions</b>")
        for p in positions:
            parts.append(f"• [{p.exchange}] {p.direction} <code>{p.position_amt}</code> <code>{p.symbol}</code> entry={_fmt_price(p.entry_price)}  PnL={p.unrealised_pnl:+.2f} USDT")
        parts.append("")

    if orders:
        parts.append("<b>📋 Open Orders</b>")
        for o in orders:
            market = "FUT" if o.is_futures else "SPOT"
            parts.append(f"• [{o.exchange}/{market}] {"🔴 SHORT" if o.side == 'SELL' else "🟢 LONG"} <code>{o.orig_qty}</code> <code>{o.symbol}</code> @ { _fmt_price(o.price) }  {o.status} {o.fill_pct:.0f}%")

    return "\n".join(parts)


def orders_list_message(orders: List[Order]) -> str:
    if not orders:
        return "No open orders."
    parts = ["<b>Open Orders</b>"]
    for o in orders:
        parts.append(f"• {o.short_repr()}")
    return "\n".join(parts)


def positions_list_message(positions: List[Position]) -> str:
    if not positions:
        return "No open positions."
    parts = ["<b>Open Positions</b>"]
    for p in positions:
        parts.append(f"• {p.short_repr()}")
    return "\n".join(parts)
