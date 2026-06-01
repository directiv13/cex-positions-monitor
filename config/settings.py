from __future__ import annotations

from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str = Field(...)
    TELEGRAM_ALLOWED_USERS: str = Field("", description="Comma-separated admin user IDs")
    TELEGRAM_CHANNEL_ID: str = Field(...)
    TELEGRAM_CHANNEL_THREAD_ID: int = Field(0, description="Topic thread ID; 0 = general channel")

    BINANCE_API_KEY: str = Field("", description="Binance API key")
    BINANCE_API_SECRET: str = Field("", description="Binance API secret")
    BINANCE_TESTNET: bool = Field(False)

    PUSHOVER_APP_TOKEN: str = Field("", description="Pushover app token")
    PUSHOVER_USER_KEY: str = Field("", description="Pushover user key")

    REST_RECONCILE_INTERVAL: int = Field(120, description="Seconds between REST reconciliation")

    LOG_LEVEL: str = Field("INFO")
    LOG_FILE: str = Field("logs/bot.log")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }

    def allowed_users(self) -> List[int]:
        raw = (self.TELEGRAM_ALLOWED_USERS or "").strip()
        if not raw:
            return []
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        out: List[int] = []
        for p in parts:
            try:
                out.append(int(p))
            except ValueError:
                continue
        return out


settings = Settings()
