from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from .models import Order, Position, OrderStatus


def _utcnow_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_price(p: float) -> str:
    if p == 0:
        return "MARKET"
    s = f"{p:.8f}".rstrip("0").rstrip(".")
    return s


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

def position_message(position: Position) -> str:
    return position.short_repr()

def order_message(order: Order) -> str:
    emoji = ""
    position_side = ""
    order_type = ""
    size = 0.0

    if order.side == "SELL":
        size = order.asks_notional
        if order.is_reduce_only:
            emoji = "🟢"
            position_side = "LONG"
            order_type = "CLOSE"
        else:
            emoji = "🔴"
            position_side = "SHORT"
            order_type = "OPEN"
    elif order.side == "BUY":
        size = order.bids_notional
        if order.is_reduce_only:
            emoji = "🔴"
            position_side = "SHORT"
            order_type = "CLOSE"
        else:
            emoji = "🟢"
            position_side = "LONG"
            order_type = "OPEN"

    status_label = order.status.value
    if order.update_type == "AMENDMENT" and order.status == OrderStatus.NEW:
            status_label = "UPDATED"

    msg = (f"<b>{emoji} {order_type} {position_side} ORDER</b>\n"
           "────────────\n"
           f"<b>Status: {status_label}</b>\n"
           "────────────\n"
           f"<code>{order.symbol}</code>\n\n"
           f"{order.order_type}\n\n"
           f"💲 <b>Price</b>:       ${order.price}\n"
           f"🤑 <b>Amount</b>:      {order.orig_qty}\n"
           f"💵 <b>Size</b>:        {size} USDT\n")

    return msg