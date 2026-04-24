from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SelectionTrackingRecord:
    code: str
    name: str
    selection_date: str
    strategy_id: str
    score: Optional[float]
    factor_scores: Dict[str, Any]
    selected_open_price: Optional[float]
    current_price: Optional[float]
    price_change_pct: Optional[float]
