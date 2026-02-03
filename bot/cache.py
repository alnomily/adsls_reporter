import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from bot.utils import utcnow

logger = logging.getLogger(__name__)

# default freshness and TTL
FRESHNESS = timedelta(minutes=1)
CACHE_TTL = timedelta(minutes=5)

# Simple in-memory cache
CACHE: Dict[str, Dict[str, Any]] = {}


def set_freshness(delta: timedelta) -> None:
    global FRESHNESS
    FRESHNESS = delta


class CacheManager:
    @staticmethod
    def get(key: str) -> Optional[Any]:
        entry = CACHE.get(key)
        if not entry:
            return None
        if utcnow() - entry["ts"] > CACHE_TTL:
            CACHE.pop(key, None)
            return None
        return entry["data"]

    @staticmethod
    def set(key: str, value: Any) -> None:
        CACHE[key] = {"data": value, "ts": utcnow()}

    @staticmethod
    def clear(key: Optional[str] = None) -> None:
        if key:
            CACHE.pop(key, None)
        else:
            CACHE.clear()
