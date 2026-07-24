from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.stock_selection.forward_observation import ForwardObservationRepository, ForwardObservationService
from app.stock_selection.factor_evaluation_v2 import (
    StrategyFactorEvaluationRepository,
)
from app.strategies.service import StrategyService

router = APIRouter(tags=["strategies"])


class ForwardActionRequest(BaseModel):
    observation_id: str = Field(..., min_length=1, max_length=96)
    code: str = Field(..., min_length=1, max_length=16)
    action_type: str = Field(..., min_length=1, max_length=32)
    action_price: float | None = Field(default=None, gt=0)
    note: str | None = Field(default=None, max_length=500)


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
    strategy_id: str = Query(min_length=1, max_length=64),
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


@router.get("/strategies/forward-evidence")
def get_forward_evidence(
    strategy_id: str = Query(min_length=1, max_length=64),
) -> dict:
    return {
        "forward_evidence": ForwardObservationService().evidence_summary(strategy_id),
    }


@router.get("/strategies/factor-evaluation")
def get_factor_evaluation(
    strategy_id: str = Query(min_length=1, max_length=64),
    strategy_version: str | None = Query(default=None, max_length=32),
    horizon_days: int = Query(default=5, ge=1, le=60),
    scope_name: str = Query(
        default="eligible_pool",
        pattern="^(eligible_pool|selected_top_k)$",
    ),
) -> dict:
    return {
        "factor_evaluation": StrategyFactorEvaluationRepository().latest_summary(
            strategy_id,
            strategy_version=strategy_version,
            horizon_days=horizon_days,
            scope_name=scope_name,
        )
    }


@router.post("/strategies/forward-actions")
def record_forward_action(payload: ForwardActionRequest) -> dict:
    try:
        action = ForwardObservationRepository().record_action(**payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"action": action}
