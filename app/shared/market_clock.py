from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def to_shanghai_wall_clock(value: datetime) -> datetime:
    """Normalize an instant to the application's naive Shanghai DB clock."""

    if value.tzinfo is None:
        return value
    return value.astimezone(SHANGHAI_TZ).replace(tzinfo=None)
