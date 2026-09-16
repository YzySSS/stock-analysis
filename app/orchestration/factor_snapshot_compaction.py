from __future__ import annotations

from typing import Any


TABLE_NAME = "strategy_factor_snapshot"


def reset_strategy_factor_snapshot(cursor: Any, *, auto_increment_floor: int) -> None:
    """Recreate the online factor detail table before restoring retained rows."""

    next_id = int(auto_increment_floor)
    if next_id < 1:
        raise ValueError("auto_increment_floor must be positive")
    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} AUTO_INCREMENT = {next_id}")
