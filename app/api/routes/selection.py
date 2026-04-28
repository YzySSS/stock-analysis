from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.shared.db import mysql_conn
from app.strategies.service import StrategyService

router = APIRouter(tags=["selection"])


class SelectionRunRequest(BaseModel):
    strategy_id: Optional[str] = None
    limit: int = 3
    score_threshold: Optional[float] = None
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
        score_threshold=payload.score_threshold,
    )


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
        sql += " AND sr.run_id = (SELECT run_id FROM selection_result WHERE strategy_id = %s ORDER BY created_at DESC LIMIT 1)"
        params.append(strategy_id)
    else:
        sql += " AND sr.run_id = (SELECT run_id FROM selection_result ORDER BY created_at DESC LIMIT 1)"
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
