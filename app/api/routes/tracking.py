from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from app.shared.db import mysql_conn

from app.error_learning.tracker import SelectionResultTracker

router = APIRouter(tags=["tracking"])


def _build_tracking_summary(items: list[dict]) -> dict:
    pct_values = [item.get("price_change_pct") for item in items if item.get("price_change_pct") is not None]
    avg_return = round(sum(pct_values) / len(pct_values), 2) if pct_values else None
    benchmark_pct = 0.0
    excess_return_pct = round(avg_return - benchmark_pct, 2) if avg_return is not None else None
    win_count = len([value for value in pct_values if value >= 0])
    win_rate = round((win_count / len(pct_values)) * 100, 2) if pct_values else None
    max_gain = max((item.get("max_gain_pct") for item in items if item.get("max_gain_pct") is not None), default=None)
    max_drawdown = min((item.get("max_drawdown_pct") for item in items if item.get("max_drawdown_pct") is not None), default=None)
    tracking_count = len([item for item in items if item.get("review_status") == "tracking"])
    best_item = max(
        (item for item in items if item.get("price_change_pct") is not None),
        key=lambda item: item.get("price_change_pct") or 0,
        default=None,
    )
    worst_item = min(
        (item for item in items if item.get("price_change_pct") is not None),
        key=lambda item: item.get("price_change_pct") or 0,
        default=None,
    )
    return {
        "count": len(items),
        "tracking_count": tracking_count,
        "avg_return_pct": avg_return,
        "benchmark_return_pct": benchmark_pct,
        "excess_return_pct": excess_return_pct,
        "win_rate_pct": win_rate,
        "max_gain_pct": max_gain,
        "max_drawdown_pct": max_drawdown,
        "best_item": {
            "code": best_item.get("code"),
            "name": best_item.get("name"),
            "price_change_pct": best_item.get("price_change_pct"),
        } if best_item else None,
        "worst_item": {
            "code": worst_item.get("code"),
            "name": worst_item.get("name"),
            "price_change_pct": worst_item.get("price_change_pct"),
        } if worst_item else None,
    }


def _list_tracking_runs(
    instrument_type: str,
    strategy_id: Optional[str] = None,
    selection_date: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        sr.run_id,
        sr.trade_date,
        sr.strategy_id,
        MAX(sr.created_at) AS created_at,
        COUNT(*) AS item_count
    FROM selection_result sr
    INNER JOIN stock_basic sb ON sr.code = sb.code
    WHERE sb.instrument_type = %s
    """
    params: list[Any] = [instrument_type]
    if strategy_id:
        sql += " AND sr.strategy_id = %s"
        params.append(strategy_id)
    if selection_date:
        sql += " AND sr.trade_date = %s"
        params.append(selection_date)
    sql += " GROUP BY sr.run_id, sr.trade_date, sr.strategy_id ORDER BY sr.trade_date DESC, created_at DESC LIMIT %s"
    params.append(limit)

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()


def _tracking_payload(
    *,
    run_id: Optional[str] = None,
    strategy_id: Optional[str] = None,
    selection_date: Optional[str] = None,
    limit: int = 20,
    instrument_type: str = "stock",
    latest_only: bool = False,
) -> dict:
    tracker = SelectionResultTracker()
    resolved_run_id = run_id
    if not resolved_run_id and selection_date:
        runs = _list_tracking_runs(
            instrument_type=instrument_type,
            strategy_id=strategy_id,
            selection_date=selection_date,
            limit=1,
        )
        resolved_run_id = runs[0].get("run_id") if runs else None

    if latest_only and not resolved_run_id and not selection_date:
        records = tracker.build_latest_selection_snapshot(
            limit=limit,
            instrument_type=instrument_type,
            strategy_id=strategy_id,
        )
    else:
        records = tracker.build_latest_selection_snapshot(
            limit=limit,
            instrument_type=instrument_type,
            run_id=resolved_run_id,
            strategy_id=strategy_id,
            selection_date=None if resolved_run_id else selection_date,
        )
    items = tracker.to_dict_list(records)
    return {
        "run_id": resolved_run_id,
        "strategy_id": strategy_id,
        "selection_date": selection_date,
        "summary": _build_tracking_summary(items),
        "items": items,
        "available_runs": _list_tracking_runs(instrument_type=instrument_type, strategy_id=strategy_id),
    }


@router.get("/tracking/latest")
def get_latest_tracking(
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
    strategy_id: Optional[str] = Query(default=None),
    selection_date: Optional[str] = Query(default=None),
) -> dict:
    return _tracking_payload(
        strategy_id=strategy_id,
        selection_date=selection_date,
        limit=limit,
        instrument_type=instrument_type,
        latest_only=True,
    )


@router.get("/tracking")
def get_tracking_by_run(
    run_id: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    selection_date: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    return _tracking_payload(
        run_id=run_id,
        strategy_id=strategy_id,
        selection_date=selection_date,
        limit=limit,
        instrument_type=instrument_type,
    )


@router.delete("/tracking/item")
def delete_tracking_item(
    code: str = Query(...),
    selection_date: str = Query(...),
    strategy_id: str = Query(...),
    instrument_type: str = Query(default="stock"),
) -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM selection_result sr
                INNER JOIN stock_basic sb ON sr.code = sb.code
                WHERE sr.code = %s
                  AND sr.trade_date = %s
                  AND sr.strategy_id = %s
                  AND sb.instrument_type = %s
                """,
                (code, selection_date, strategy_id, instrument_type),
            )
            matched_count = int((cursor.fetchone() or [0])[0] or 0)
            if matched_count <= 0:
                raise HTTPException(status_code=404, detail="未找到可删除的复盘记录")

            cursor.execute(
                """
                DELETE sr
                FROM selection_result sr
                INNER JOIN stock_basic sb ON sr.code = sb.code
                WHERE sr.code = %s
                  AND sr.trade_date = %s
                  AND sr.strategy_id = %s
                  AND sb.instrument_type = %s
                """,
                (code, selection_date, strategy_id, instrument_type),
            )

    return {
        "code": code,
        "selection_date": selection_date,
        "strategy_id": strategy_id,
        "instrument_type": instrument_type,
        "deleted_count": matched_count,
    }


@router.get("/tracking/filters")
def get_tracking_filters(
    instrument_type: str = Query(default="stock"),
    strategy_id: Optional[str] = Query(default=None),
) -> dict:
    runs = _list_tracking_runs(instrument_type=instrument_type, strategy_id=strategy_id, limit=200)
    seen_dates = []
    seen = set()
    for item in runs:
        value = str(item.get("trade_date") or "")
        if value and value not in seen:
            seen.add(value)
            seen_dates.append(value)
    return {
        "strategy_id": strategy_id,
        "selection_dates": seen_dates,
        "available_runs": runs,
    }
