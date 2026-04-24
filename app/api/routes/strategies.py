from __future__ import annotations

from fastapi import APIRouter

from app.strategies.service import StrategyService

router = APIRouter(tags=["strategies"])


@router.get("/strategies")
def list_strategies() -> dict:
    service = StrategyService()
    return {
        "default_strategy": service.get_default_strategy_id(),
        "strategies": service.list_strategies(),
    }
