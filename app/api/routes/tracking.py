from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.error_learning.tracker import SelectionResultTracker

router = APIRouter(tags=["tracking"])


def _build_tracking_summary(items: list[dict]) -> dict:
    pct_values = [item.get("price_change_pct") for item in items if item.get("price_change_pct") is not None]
    avg_return = round(sum(pct_values) / len(pct_values), 2) if pct_values else None
    win_count = len([value for value in pct_values if value >= 0])
    win_rate = round((win_count / len(pct_values)) * 100, 2) if pct_values else None
    max_gain = max((item.get("max_gain_pct") for item in items if item.get("max_gain_pct") is not None), default=None)
    max_drawdown = min((item.get("max_drawdown_pct") for item in items if item.get("max_drawdown_pct") is not None), default=None)
    return {
        "count": len(items),
        "avg_return_pct": avg_return,
        "win_rate_pct": win_rate,
        "max_gain_pct": max_gain,
        "max_drawdown_pct": max_drawdown,
    }


@router.get("/tracking/latest")
def get_latest_tracking(
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type)
    items = tracker.to_dict_list(records)
    return {
        "summary": _build_tracking_summary(items),
        "items": items,
    }


@router.get("/tracking")
def get_tracking_by_run(
    run_id: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type, run_id=run_id)
    items = tracker.to_dict_list(records)
    return {
        "run_id": run_id,
        "summary": _build_tracking_summary(items),
        "items": items,
    }
