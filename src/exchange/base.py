from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Tuple

from ..models import Order, Position, StateSnapshot


class ExchangeBase(ABC):
    name: str = "BASE"

    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def stop(self) -> None:
        ...

    @abstractmethod
    async def fetch_open_orders(self) -> list[Order]:
        ...

    @abstractmethod
    async def fetch_open_positions(self) -> list[Position]:
        ...

    @abstractmethod
    def stream_events(self) -> AsyncIterator[Tuple[str, object]]:
        """
        Yield (event_type, payload) tuples as they arrive from the exchange.
        event_type values: "ORDER_UPDATE", "POSITION_UPDATE"
        The generator must handle reconnection and listenKey keepalive internally.
        """
        ...
