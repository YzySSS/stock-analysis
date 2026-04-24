from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.error_learning.tracker import SelectionResultTracker

router = APIRouter(tags=["tracking"])


@router.get("/tracking/latest")
def get_latest_tracking(
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type)
    return {
        "items": tracker.to_dict_list(records),
    }


@router.get("/tracking")
def get_tracking_by_run(
    run_id: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type, run_id=run_id)
    return {
        "run_id": run_id,
        "items": tracker.to_dict_list(records),
    }
