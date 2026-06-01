from __future__ import annotations

from loguru import logger
from pathlib import Path
import sys


def setup_logger(level: str = "INFO", log_file: str = "logs/bot.log") -> None:
    logger.remove()
    # stderr
    logger.add(sys.stderr, level=level)

    # file with rotation
    p = Path(log_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(p),
        rotation="00:00",
        retention="14 days",
        compression="gz",
        level=level,
        enqueue=True,
    )


__all__ = ["setup_logger", "logger"]
