from __future__ import annotations

import httpx
from loguru import logger


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

    async def notify_order_opened(self, order_repr: str) -> bool:
        return await self.send("Order Opened", order_repr, priority=PRIORITY_NORMAL, sound="cashregister")

    async def notify_order_closed(self, order_repr: str, status: str) -> bool:
        sound = "magic" if status == "FILLED" else "falling"
        pr = PRIORITY_HIGH
        return await self.send("Order Closed", order_repr, priority=pr, sound=sound)

    async def notify_position_opened(self, position_repr: str) -> bool:
        return await self.send("Position Opened", position_repr, priority=PRIORITY_NORMAL, sound="cashregister")

    async def notify_position_closed(self, position_repr: str) -> bool:
        return await self.send("Position Closed", position_repr, priority=PRIORITY_HIGH, sound="magic")
