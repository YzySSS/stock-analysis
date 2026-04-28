from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.error_learning.tracker import SelectionResultTracker
from app.shared.db import mysql_conn

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


@router.get("/tracking/latest")
def get_latest_tracking(
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type)
    items = tracker.to_dict_list(records)
    return {
        "summary": _build_tracking_summary(items),
        "items": items,
    }


@router.get("/tracking")
def get_tracking_by_run(
    run_id: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    instrument_type: str = Query(default="stock"),
) -> dict:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type=instrument_type, run_id=run_id)
    items = tracker.to_dict_list(records)
    return {
        "run_id": run_id,
        "summary": _build_tracking_summary(items),
        "items": items,
    }


@router.delete("/tracking/run")
def delete_tracking_run(run_id: str = Query(...), instrument_type: str = Query(default="stock")) -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM selection_result sr
                INNER JOIN stock_basic sb ON sr.code = sb.code
                WHERE sr.run_id = %s AND sb.instrument_type = %s
                """,
                (run_id, instrument_type),
            )
            matched_count = int((cursor.fetchone() or [0])[0] or 0)
            if matched_count <= 0:
                raise HTTPException(status_code=404, detail="未找到可删除的选股结果")

            cursor.execute(
                """
                DELETE sr
                FROM selection_result sr
                INNER JOIN stock_basic sb ON sr.code = sb.code
                WHERE sr.run_id = %s AND sb.instrument_type = %s
                """,
                (run_id, instrument_type),
            )

    return {
        "run_id": run_id,
        "instrument_type": instrument_type,
        "deleted_count": matched_count,
    }
