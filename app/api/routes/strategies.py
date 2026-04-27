from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.strategies.service import StrategyService

router = APIRouter(tags=["strategies"])


@router.get("/strategies")
def list_strategies() -> dict:
    service = StrategyService()
    return {
        "default_strategy": service.get_default_strategy_id(),
        "strategies": service.list_strategies(),
    }


@router.get("/strategies/detail")
def get_strategy_detail(
    strategy_id: Optional[str] = Query(default=None),
    instrument_type: str = Query(default="stock"),
    sample_limit: int = Query(default=200, ge=20, le=1000),
) -> dict:
    service = StrategyService()
    return {
        "strategy": service.get_strategy_detail(
            strategy_id=strategy_id,
            instrument_type=instrument_type,
            sample_limit=sample_limit,
        ),
    }
