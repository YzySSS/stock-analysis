from __future__ import annotations

from typing import Any


ALL_A_UNIVERSE_CODE = "ALL_A"

INDEX_UNIVERSE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "000016.SH": {"name": "上证50", "expected_members": 50},
    "000300.SH": {"name": "沪深300", "expected_members": 300},
    "000905.SH": {"name": "中证500", "expected_members": 500},
    "000852.SH": {"name": "中证1000", "expected_members": 1000},
}

SUPPORTED_BACKTEST_UNIVERSE_CODES = (
    ALL_A_UNIVERSE_CODE,
    *INDEX_UNIVERSE_DEFINITIONS.keys(),
)


def normalize_backtest_universe(value: Any) -> str:
    code = str(value or ALL_A_UNIVERSE_CODE).strip().upper()
    if code not in SUPPORTED_BACKTEST_UNIVERSE_CODES:
        supported = ", ".join(SUPPORTED_BACKTEST_UNIVERSE_CODES)
        raise ValueError(f"unsupported backtest universe {code!r}; supported: {supported}")
    return code


def universe_label(code: Any) -> str:
    normalized = normalize_backtest_universe(code)
    if normalized == ALL_A_UNIVERSE_CODE:
        return "历史全A"
    return str(INDEX_UNIVERSE_DEFINITIONS[normalized]["name"])


def expected_index_members(code: Any) -> int:
    normalized = normalize_backtest_universe(code)
    if normalized == ALL_A_UNIVERSE_CODE:
        raise ValueError("ALL_A does not have a fixed index member count")
    return int(INDEX_UNIVERSE_DEFINITIONS[normalized]["expected_members"])


def index_member_guard_range(expected_members: Any) -> tuple[int, int]:
    expected = int(expected_members)
    if expected <= 0:
        raise ValueError("expected index members must be positive")
    minimum = max(1, (expected * 95 + 99) // 100)
    maximum = expected * 105 // 100
    return minimum, maximum
