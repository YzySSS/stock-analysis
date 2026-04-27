from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SelectionTrackingRecord:
    code: str
    name: str
    selection_date: str
    strategy_id: str
    strategy_display_name: Optional[str]
    strategy_version: Optional[str]
    score: Optional[float]
    factor_scores: Dict[str, Any]
    selected_open_price: Optional[float]
    current_price: Optional[float]
    price_change_pct: Optional[float]
    run_id: Optional[str] = None
    rank_no: Optional[int] = None
    selected_close_price: Optional[float] = None
    latest_trade_date: Optional[str] = None
    reason_summary: Optional[list[str]] = None
    risk_summary: Optional[list[str]] = None
