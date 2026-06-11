from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    BOTH = "BOTH"


@dataclass
class Order:
    order_id: int
    client_order_id: str
    symbol: str
    side: str           # BUY / SELL
    order_type: str     # LIMIT / MARKET / STOP …
    status: OrderStatus
    price: float
    orig_qty: float
    executed_qty: float
    time: datetime
    update_time: datetime
    exchange: str = "UNKNOWN"
    is_futures: bool = False
    update_type: str | None = None

    @property
    def is_open(self) -> bool:
        return self.status in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED)

    @property
    def fill_pct(self) -> float:
        if self.orig_qty == 0:
            return 0.0
        return round(self.executed_qty / self.orig_qty * 100, 2)

    def short_repr(self) -> str:
        market = "FUTURES" if self.is_futures else "SPOT"
        status_label = self.status.value
        if self.update_type == "AMENDMENT" and self.status == OrderStatus.NEW:
            status_label = "UPDATED"
        return (
            f"[{self.exchange}/{market}]"
            f"\n\n<b>{f"🔴 {status_label} SELL" if self.side == 'SELL' else f"🟢 {status_label} BUY"}</b>\n"
            f"\n<b>Amount:</b> {self.orig_qty}"
            f"\n<b>Symbol:</b> <code>{self.symbol}</code> "
            f"\n<b>Price:</b> {self.price or 'MARKET'}"
            f"\n<b>Status:</b> {status_label}"
        )


@dataclass
class Position:
    symbol: str
    position_side: PositionSide
    entry_price: float
    mark_price: float
    position_amt: float     # positive = LONG, negative = SHORT
    realised_pnl: float
    unrealised_pnl: float
    leverage: int
    margin_type: str        # isolated / cross
    liquidation_price: float
    exchange: str = "UNKNOWN"
    update_time: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_open(self) -> bool:
        return self.position_amt != 0.0

    @property
    def direction(self) -> str:
        return "LONG" if self.position_amt > 0 else "SHORT"

    def short_repr(self) -> str:
        state = " OPEN" if self.is_open else " CLOSED"
        emodji = "❌"
        direction = self.direction
        msg = (f"[{self.exchange}/FUTURES]"
                f"\n\n<b>{emodji}{state}{direction} <code>{self.symbol}</code></b>\n")
        
        if self.is_open:
            emodji = "🟢" if self.position_amt > 0 else "🔴"
            direction = f" {direction}"  # add space for better formatting when open
            msg += (
                f"\n<b>Amount:</b> {abs(self.position_amt)}"
                f"\n<b>Entry Price:</b> {self.entry_price}"
            )
        else:
            sign = "+" if self.realised_pnl >= 0 else ""
            msg += f"\n<b>Realised PnL:</b> {sign}{self.realised_pnl:.4f} USDT"

        return msg


@dataclass
class StateSnapshot:
    orders: dict[int, Order] = field(default_factory=dict)       # order_id → Order
    positions: dict[str, Position] = field(default_factory=dict)  # symbol   → Position

    def copy(self) -> StateSnapshot:
        """Return a deep copy so callers cannot mutate live state."""
        return copy.deepcopy(self)