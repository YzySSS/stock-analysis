from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.portfolio.service import PortfolioService

router = APIRouter(tags=["portfolio"])


class PortfolioPositionRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=16)
    cost_price: float = Field(..., gt=0)
    quantity: int = Field(..., gt=0)
    strategy_id: str = Field(default="short_term", max_length=64)
    buy_datetime: datetime | None = None
    target_style: str = Field(default="short_swing", max_length=32)
    max_loss_pct: float | None = Field(default=None, gt=0, le=30)
    note: str | None = Field(default=None, max_length=500)


class PortfolioPositionUpdateRequest(BaseModel):
    cost_price: float | None = Field(default=None, gt=0)
    quantity: int | None = Field(default=None, gt=0)
    strategy_id: str | None = Field(default=None, max_length=64)
    buy_datetime: datetime | None = None
    target_style: str | None = Field(default=None, max_length=32)
    max_loss_pct: float | None = Field(default=None, gt=0, le=30)
    note: str | None = Field(default=None, max_length=500)
    is_active: bool | None = None


@router.get("/portfolio")
def list_portfolio(include_inactive: bool = Query(default=False)) -> dict:
    return PortfolioService().list_positions(include_inactive=include_inactive)


@router.post("/portfolio")
def create_portfolio_position(payload: PortfolioPositionRequest) -> dict:
    data = payload.model_dump()
    if data.get("buy_datetime"):
        data["buy_datetime"] = data["buy_datetime"].replace(microsecond=0).isoformat(sep=" ")
    try:
        return {"position": PortfolioService().create_position(data)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/portfolio/{position_id}")
def update_portfolio_position(position_id: int, payload: PortfolioPositionUpdateRequest) -> dict:
    data: dict[str, Any] = payload.model_dump(exclude_unset=True)
    if data.get("buy_datetime"):
        data["buy_datetime"] = data["buy_datetime"].replace(microsecond=0).isoformat(sep=" ")
    try:
        return {"position": PortfolioService().update_position(position_id, data)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/portfolio/{position_id}")
def delete_portfolio_position(position_id: int) -> dict:
    try:
        return PortfolioService().delete_position(position_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/portfolio/{position_id}/advice/refresh")
def refresh_portfolio_advice(
    position_id: int,
    force: bool = Query(default=True),
) -> dict:
    service = PortfolioService()
    try:
        run = service.create_advice_refresh_run(position_id, force=force)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"advice_run": run}


@router.get("/portfolio/advice/runs/{run_id}")
def get_portfolio_advice_run(run_id: int) -> dict:
    try:
        return {"advice_run": PortfolioService().get_advice_run(run_id)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/portfolio/advice/runs/{run_id}/cancel")
def cancel_portfolio_advice_run(run_id: int) -> dict:
    try:
        return {"advice_run": PortfolioService().request_cancel_advice_run(run_id)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/portfolio/advice/outcomes/evaluate")
def evaluate_portfolio_advice_outcomes(
    limit: int = Query(default=100, ge=1, le=1000),
    force: bool = Query(default=False),
) -> dict:
    return {"outcome_update": PortfolioService().evaluate_advice_outcomes(limit=limit, force=force)}
