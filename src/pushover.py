from __future__ import annotations

import httpx
from loguru import logger

from src.models import Position


PRIORITY_LOWEST = -2
PRIORITY_LOW = -1
PRIORITY_NORMAL = 0
PRIORITY_HIGH = 1
PRIORITY_EMERGENCY = 2


class PushoverNotifier:
    def __init__(self, app_token: str, user_key: str):
        self.app_token = app_token
        self.user_key = user_key
        self._client = httpx.AsyncClient(timeout=10.0)

    async def send(self, title: str, message: str, priority: int = 0, sound: str = "pushover", url: str | None = None, url_title: str | None = None) -> bool:
        data = {
            "token": self.app_token,
            "user": self.user_key,
            "title": title,
            "message": message,
            "priority": str(priority),
            "sound": sound,
        }
        if url:
            data["url"] = url
        if url_title:
            data["url_title"] = url_title

        if priority == PRIORITY_EMERGENCY:
            data["retry"] = 60
            data["expire"] = 3600

        try:
            r = await self._client.post("https://api.pushover.net/1/messages.json", data=data)
            return r.status_code == 200
        except Exception:
            logger.exception("Pushover send failed")
            return False

    async def notify_position_opened(self, position: Position) -> bool:
        side = "🟢 LONG" if position.direction == "LONG" else "🔴 SHORT"
        return await self.send("Position Opened", (f"{side}\nSymbol: {position.symbol}"), priority=PRIORITY_HIGH, sound="cashregister")

    async def notify_position_closed(self, position: Position) -> bool:
        return await self.send("Position Closed", f"Symbol: {position.symbol}", priority=PRIORITY_HIGH, sound="magic")
