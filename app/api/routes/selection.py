from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.routes.tracking import _invalidate_tracking_summary_cache
from app.shared.db import mysql_conn
from app.shared.instrument_policy import (
    SUPPORTED_SELECTION_INSTRUMENT_TYPES,
    UnsupportedInstrumentError,
    require_supported_instrument,
)
from app.stock_selection.run_tasks import SelectionRunService
from app.strategies.service import StrategyService

router = APIRouter(tags=["selection"])


class SelectionRunRequest(BaseModel):
    strategy_id: Optional[str] = None
    limit: int = Field(default=3, ge=1, le=200)
    max_picks: Optional[int] = Field(default=None, ge=1, le=200)
    score_threshold: Optional[float] = Field(default=None, ge=0, le=100)
    instrument_type: str = "stock"
    market_board: Optional[str] = None
    save: bool = False
    async_run: bool = True


class SelectionSaveItemRequest(BaseModel):
    run_id: str
    strategy_id: str
    score_threshold: Optional[float] = None
    item: dict


def _require_selection_instrument(instrument_type: str, operation: str = "selection") -> str:
    try:
        return require_supported_instrument(
            instrument_type,
            operation=operation,
            supported=SUPPORTED_SELECTION_INSTRUMENT_TYPES,
        )
    except UnsupportedInstrumentError as exc:
        raise HTTPException(status_code=422, detail=exc.as_detail()) from exc


@router.post("/selection/run", status_code=202)
def run_selection(payload: SelectionRunRequest) -> dict:
    instrument_type = _require_selection_instrument(payload.instrument_type)
    effective_limit = payload.max_picks if payload.max_picks is not None else payload.limit
    if not payload.async_run:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "synchronous_selection_disabled",
                "message": "选股已统一迁移到可恢复队列，请使用 async_run=true 并通过 run_id 查询进度。",
            },
        )

    service = SelectionRunService()
    try:
        run = service.submit(
            {
                "strategy_id": payload.strategy_id,
                "limit": effective_limit,
                "max_picks": effective_limit,
                "score_threshold": payload.score_threshold,
                "instrument_type": instrument_type,
                "market_board": payload.market_board,
                "save": payload.save,
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return run


@router.get("/selection/runs/{run_id}")
def get_selection_run(run_id: str) -> dict:
    try:
        return SelectionRunService().get_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/selection/runs")
def get_selection_runs(limit: int = Query(default=20, ge=1, le=100)) -> dict:
    return {"items": SelectionRunService().list_runs(limit=limit)}


@router.post("/selection/runs/{run_id}/cancel")
def cancel_selection_run(run_id: str) -> dict:
    try:
        return SelectionRunService().request_cancel(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/selection/save-item")
def save_selection_item(payload: SelectionSaveItemRequest) -> dict:
    service = StrategyService()
    result = service.save_strategy_result(
        strategy_id=payload.strategy_id,
        item=payload.item,
        run_id=payload.run_id,
        score_threshold=payload.score_threshold,
    )
    _invalidate_tracking_summary_cache()
    return result


def _sample_size(instrument_type: str) -> int:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM stock_basic WHERE instrument_type = %s", (instrument_type,))
            row = cursor.fetchone() or {}
            return int(row.get("count") or 0)


def _latest_run_meta(instrument_type: str, run_id: Optional[str] = None, strategy_id: Optional[str] = None) -> dict:
    sql = """
    SELECT
        sr.run_id,
        sr.trade_date,
        sr.strategy_id,
        MAX(sr.created_at) AS created_at
    FROM selection_result sr
    INNER JOIN stock_basic sb ON sr.code = sb.code
    WHERE sb.instrument_type = %s
    """
    params = [instrument_type]
    if run_id:
        sql += " AND sr.run_id = %s"
        params.append(run_id)
    elif strategy_id:
        sql += """
        AND sr.run_id = (
            SELECT sr2.run_id
            FROM selection_result sr2
            INNER JOIN stock_basic sb2 ON sr2.code = sb2.code
            WHERE sb2.instrument_type = %s
              AND sr2.strategy_id = %s
            ORDER BY sr2.created_at DESC, sr2.id DESC
            LIMIT 1
        )
        """
        params.extend([instrument_type, strategy_id])
    else:
        sql += """
        AND sr.run_id = (
            SELECT sr2.run_id
            FROM selection_result sr2
            INNER JOIN stock_basic sb2 ON sr2.code = sb2.code
            WHERE sb2.instrument_type = %s
            ORDER BY sr2.created_at DESC, sr2.id DESC
            LIMIT 1
        )
        """
        params.append(instrument_type)
    sql += " GROUP BY sr.run_id, sr.trade_date, sr.strategy_id ORDER BY created_at DESC LIMIT 1"

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() or {}


@router.get("/selection/results")
def get_selection_results(
    run_id: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    limit: int = Query(default=3, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    from app.error_learning.tracker import SelectionResultTracker

    instrument_type = _require_selection_instrument(instrument_type, operation="selection_results")
    tracker = SelectionResultTracker()
    resolved_run_id = run_id
    run_meta = _latest_run_meta(instrument_type=instrument_type, run_id=run_id, strategy_id=strategy_id)
    if not resolved_run_id and run_meta.get("run_id"):
        resolved_run_id = run_meta.get("run_id")

    resolved_strategy_id = strategy_id or run_meta.get("strategy_id")
    records = tracker.build_latest_selection_snapshot(
        limit=limit,
        instrument_type=instrument_type,
        run_id=resolved_run_id,
        strategy_id=resolved_strategy_id,
    )
    items = tracker.to_dict_list(records)

    if not resolved_strategy_id and items:
        resolved_strategy_id = items[0].get("strategy_id")

    service = StrategyService()
    strategy = service.get_strategy_detail(strategy_id=resolved_strategy_id) if resolved_strategy_id else None

    return {
        "status": "success" if items else "no_history",
        "reason_code": None if items else "no_history",
        "message": None if items else "当前条件下暂无真实历史选股结果。",
        "run_id": resolved_run_id or (items[0].get("run_id") if items else None),
        "requested_strategy_id": strategy_id,
        "strategy": strategy,
        "summary": {
            "selected_trade_date": items[0].get("selection_date") if items else str(run_meta.get("trade_date") or "") or None,
            "run_created_at": str(run_meta.get("created_at")) if run_meta.get("created_at") else None,
            "latest_trade_date": items[0].get("latest_trade_date") if items else None,
            "total_count": len(items),
            "sample_size": _sample_size(instrument_type),
            "instrument_type": instrument_type,
            "updated_at": items[0].get("latest_trade_date") if items else None,
            "result_strategy_id": resolved_strategy_id,
        },
        "items": items,
    }
