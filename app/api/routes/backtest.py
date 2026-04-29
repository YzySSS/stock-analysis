from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.shared.db import mysql_conn

router = APIRouter(tags=["backtest"])


class BacktestRunRequest(BaseModel):
    strategy_id: str = "lowvol_reversal"
    start_date: str
    end_date: str
    return_mode: str = "1d"
    instrument_type: str = "stock"
    use_adjusted_price: bool = False
    save: bool = True


@router.post("/backtest/run")
def run_backtest(payload: BacktestRunRequest) -> dict:
    return {
        "status": "pending",
        "message": "V2 backtest engine not implemented yet",
        "request": payload.model_dump(),
    }


@router.get("/backtest/results")
def get_backtest_results(run_id: Optional[str] = Query(default=None)) -> dict:
    return {
        "run_id": run_id,
        "status": "pending",
        "message": "V2 backtest result service not implemented yet",
        "summary": None,
        "curve": [],
    }


@router.get("/backtest/trades")
def get_backtest_trades(
    run_id: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
    trade_date: Optional[str] = Query(default=None),
    code: Optional[str] = Query(default=None),
    return_mode: str = Query(default="1d"),
) -> dict:
    return {
        "run_id": run_id,
        "limit": limit,
        "trade_date": trade_date,
        "code": code,
        "return_mode": return_mode,
        "items": [],
        "message": "V2 backtest trade service not implemented yet",
    }


@router.get("/backtest/runs")
def get_backtest_runs(limit: int = Query(default=20, ge=1, le=100)) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id, strategy_id, start_date, end_date, return_mode, status, started_at, finished_at
                FROM backtest_run
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall() or []
    return {
        "items": [
            {
                **row,
                "start_date": str(row.get("start_date")) if row.get("start_date") else None,
                "end_date": str(row.get("end_date")) if row.get("end_date") else None,
                "started_at": str(row.get("started_at")) if row.get("started_at") else None,
                "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
            }
            for row in rows
        ]
    }


@router.get("/factor-input/status")
def get_factor_input_status() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total_rows, COUNT(DISTINCT code) AS covered_codes, MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date FROM factor_input_daily")
            summary = cursor.fetchone() or {}

            cursor.execute(
                """
                SELECT
                    SUM(CASE WHEN pe_tushare IS NOT NULL THEN 1 ELSE 0 END) AS pe_tushare_filled,
                    SUM(CASE WHEN pb_tushare IS NOT NULL THEN 1 ELSE 0 END) AS pb_tushare_filled,
                    SUM(CASE WHEN turnover_rate IS NOT NULL THEN 1 ELSE 0 END) AS turnover_rate_filled,
                    SUM(CASE WHEN roe IS NOT NULL THEN 1 ELSE 0 END) AS roe_filled,
                    SUM(CASE WHEN revenue_yoy IS NOT NULL THEN 1 ELSE 0 END) AS revenue_yoy_filled,
                    COUNT(*) AS total_rows
                FROM factor_input_daily
                """
            )
            field_row = cursor.fetchone() or {}

            cursor.execute(
                """
                SELECT task_name, run_id, status, started_at, finished_at, message
                FROM task_run_log
                WHERE task_name = 'factor_input_history_backfill'
                ORDER BY id DESC
                LIMIT 1
                """
            )
            latest_task = cursor.fetchone() or {}

    total_rows = int(summary.get("total_rows") or 0)

    def pct(filled: object) -> float | None:
        if not total_rows:
            return None
        return round((int(filled or 0) / total_rows) * 100, 2)

    return {
        "coverage": {
            "trade_date_start": str(summary.get("min_trade_date")) if summary.get("min_trade_date") else None,
            "trade_date_end": str(summary.get("max_trade_date")) if summary.get("max_trade_date") else None,
            "covered_stock_codes": int(summary.get("covered_codes") or 0),
            "covered_rows": total_rows,
            "fields": [
                {"field": "pe_tushare", "coverage_pct": pct(field_row.get("pe_tushare_filled"))},
                {"field": "pb_tushare", "coverage_pct": pct(field_row.get("pb_tushare_filled"))},
                {"field": "turnover_rate", "coverage_pct": pct(field_row.get("turnover_rate_filled"))},
                {"field": "roe", "coverage_pct": pct(field_row.get("roe_filled"))},
                {"field": "revenue_yoy", "coverage_pct": pct(field_row.get("revenue_yoy_filled"))},
            ],
        },
        "latest_task": {
            "task_name": latest_task.get("task_name"),
            "run_id": latest_task.get("run_id"),
            "status": latest_task.get("status"),
            "started_at": str(latest_task.get("started_at")) if latest_task.get("started_at") else None,
            "finished_at": str(latest_task.get("finished_at")) if latest_task.get("finished_at") else None,
            "message": latest_task.get("message"),
        },
    }
