from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.strategies.service import StrategyService

router = APIRouter(tags=["selection"])


class SelectionRunRequest(BaseModel):
    strategy_id: Optional[str] = None
    limit: int = 20
    instrument_type: str = "stock"
    save: bool = True


@router.post("/selection/run")
def run_selection(payload: SelectionRunRequest) -> dict:
    service = StrategyService()
    return service.run_strategy(
        strategy_id=payload.strategy_id,
        limit=payload.limit,
        instrument_type=payload.instrument_type,
        save=payload.save,
    )


@router.get("/selection/results")
def get_selection_results(
    run_id: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    from app.error_learning.tracker import SelectionResultTracker

    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type, run_id=run_id)
    return {
        "run_id": run_id,
        "items": tracker.to_dict_list(records),
    }
