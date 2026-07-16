from __future__ import annotations

from typing import Iterable


STOCK_INSTRUMENT_TYPE = "stock"
STOCK_DAILY_COMPLETENESS_RATIO = 0.95
STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS = 45
SUPPORTED_SELECTION_INSTRUMENT_TYPES = frozenset({STOCK_INSTRUMENT_TYPE})
SUPPORTED_BACKTEST_INSTRUMENT_TYPES = frozenset({STOCK_INSTRUMENT_TYPE})

_INSTRUMENT_LABELS = {
    "stock": "股票",
    "etf": "ETF",
    "index": "指数",
}

_OPERATION_LABELS = {
    "selection": "选股",
    "selection_results": "选股结果",
    "backtest": "回测",
}


def normalize_instrument_type(value: object) -> str:
    return str(value or STOCK_INSTRUMENT_TYPE).strip().lower()


class UnsupportedInstrumentError(ValueError):
    code = "unsupported_instrument"

    def __init__(self, instrument_type: object, operation: str, supported: Iterable[str]) -> None:
        self.instrument_type = normalize_instrument_type(instrument_type)
        self.operation = operation
        self.supported_instrument_types = tuple(sorted(set(supported)))
        instrument_label = _INSTRUMENT_LABELS.get(self.instrument_type, self.instrument_type or "未知标的")
        operation_label = _OPERATION_LABELS.get(operation, operation)
        supported_labels = "、".join(
            _INSTRUMENT_LABELS.get(item, item) for item in self.supported_instrument_types
        )
        self.message = (
            f"{instrument_label}{operation_label}数据与策略链尚未建设完成，"
            f"当前仅支持{supported_labels}。"
        )
        super().__init__(self.message)

    def as_detail(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "operation": self.operation,
            "instrument_type": self.instrument_type,
            "supported_instrument_types": list(self.supported_instrument_types),
        }


def require_supported_instrument(
    instrument_type: object,
    *,
    operation: str,
    supported: Iterable[str],
) -> str:
    normalized = normalize_instrument_type(instrument_type)
    supported_set = frozenset(supported)
    if normalized not in supported_set:
        raise UnsupportedInstrumentError(normalized, operation, supported_set)
    return normalized
