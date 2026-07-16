from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.strategies.service import StrategyService

router = APIRouter(tags=["strategies"])


@router.get("/strategies")
def list_strategies(instrument_type: str = Query(default="stock")) -> dict:
    service = StrategyService()
    strategies = service.list_strategies(instrument_type=instrument_type)
    return {
        "default_strategy": service.get_default_strategy_id(),
        "instrument_type": instrument_type,
        "summary": {
            "count": len(strategies),
            "current_count": len([item for item in strategies if item.get("mode") == "current"]),
            "legacy_count": len([item for item in strategies if item.get("mode") == "legacy"]),
            "loadable_count": len([item for item in strategies if item.get("loadable")]),
            "data_ready_count": len([item for item in strategies if item.get("data_ready")]),
            "runtime_ready_count": len([item for item in strategies if item.get("runtime_ready")]),
            "backtest_ready_count": len([item for item in strategies if item.get("backtest_ready")]),
            "validated_count": len([item for item in strategies if item.get("validated")]),
            "prototype_count": len([item for item in strategies if item.get("runtime_status") == "prototype"]),
            "experimental_count": len([item for item in strategies if item.get("status") == "experimental"]),
            "display_only_count": len([item for item in strategies if not item.get("runtime_ready")]),
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
