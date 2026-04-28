from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.strategies.service import StrategyService

router = APIRouter(tags=["strategies"])


@router.get("/strategies")
def list_strategies() -> dict:
    service = StrategyService()
    strategies = service.list_strategies()
    return {
        "default_strategy": service.get_default_strategy_id(),
        "summary": {
            "count": len(strategies),
            "current_count": len([item for item in strategies if item.get("mode") == "current"]),
            "legacy_count": len([item for item in strategies if item.get("mode") == "legacy"]),
            "runtime_ready_count": len([item for item in strategies if item.get("runtime_ready")]),
            "experimental_count": len([item for item in strategies if item.get("availability") == "experimental"]),
            "display_only_count": len([item for item in strategies if item.get("availability") == "display_only"]),
        },
        "strategies": strategies,
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
