from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.backtest.service import BacktestRequest, BacktestService
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
    max_picks: Optional[int] = Field(default=None, ge=1, le=50)
    score_threshold: Optional[float] = Field(default=None, ge=0, le=100)


@router.post("/backtest/run")
def run_backtest(payload: BacktestRunRequest) -> dict:
    try:
        request = BacktestRequest(**payload.model_dump(exclude={"save"}))
        if not payload.save:
            return BacktestService().run(request, save=False)
        service = BacktestService()
        run = service.submit(request)
        return normalize_run_row(run)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backtest/runs/{run_id}/cancel")
def cancel_backtest_run(run_id: str) -> dict:
    try:
        return normalize_run_row(BacktestService().request_cancel(run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def normalize_run_row(row: dict) -> dict:
    summary = row.get("summary_json")
    if isinstance(summary, str):
        summary = json.loads(summary)
    request = row.get("request_json")
    if isinstance(request, str):
        request = json.loads(request)
    return {
        "run_id": row.get("run_id"),
        "strategy_id": row.get("strategy_id"),
        "start_date": str(row.get("start_date")) if row.get("start_date") else None,
        "end_date": str(row.get("end_date")) if row.get("end_date") else None,
        "return_mode": row.get("return_mode"),
        "status": row.get("status"),
        "worker_id": row.get("worker_id"),
        "locked_at": str(row.get("locked_at")) if row.get("locked_at") else None,
        "worker_heartbeat_at": str(row.get("worker_heartbeat_at")) if row.get("worker_heartbeat_at") else None,
        "cancel_requested": bool(row.get("cancel_requested")),
        "is_system_test": bool(row.get("is_system_test")),
        "sample_days": int(row.get("sample_days") or 0),
        "total_picks": int(row.get("total_picks") or 0),
        "total_trades": int(row.get("total_trades") or 0),
        "progress_total_days": int(row.get("progress_total_days") or 0),
        "progress_done_days": int(row.get("progress_done_days") or 0),
        "progress_pct": float(row.get("progress_pct") or 0),
        "current_trade_date": str(row.get("current_trade_date")) if row.get("current_trade_date") else None,
        "estimated_seconds_left": int(row.get("estimated_seconds_left")) if row.get("estimated_seconds_left") is not None else None,
        "total_return_pct": float(row.get("total_return_pct")) if row.get("total_return_pct") is not None else None,
        "avg_return_pct": float(row.get("avg_return_pct")) if row.get("avg_return_pct") is not None else None,
        "max_drawdown_pct": float(row.get("max_drawdown_pct")) if row.get("max_drawdown_pct") is not None else None,
        "win_rate_pct": float(row.get("win_rate_pct")) if row.get("win_rate_pct") is not None else None,
        "summary": summary,
        "request": request,
        "started_at": str(row.get("started_at")) if row.get("started_at") else None,
        "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
        "error_message": row.get("error_message"),
    }


@router.get("/backtest/results")
def get_backtest_results(run_id: Optional[str] = Query(default=None)) -> dict:
    if not run_id:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT run_id FROM backtest_run WHERE COALESCE(is_system_test, 0) = 0 ORDER BY id DESC LIMIT 1")
                latest = cursor.fetchone() or {}
                run_id = latest.get("run_id")
    if not run_id:
        return {"run_id": None, "summary": None, "curve": []}

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM backtest_run WHERE run_id = %s", (run_id,))
            run = cursor.fetchone()
            if not run:
                raise HTTPException(status_code=404, detail="backtest run not found")

            cursor.execute(
                """
                SELECT trade_date, pick_count, avg_return_1d_pct, avg_return_3d_pct,
                       win_rate_1d_pct, win_rate_3d_pct,
                       benchmark_return_1d_pct, benchmark_return_3d_pct
                FROM backtest_summary_daily
                WHERE run_id = %s
                ORDER BY trade_date
                """,
                (run_id,),
            )
            curve = cursor.fetchall() or []

    summary_json = run.get("summary_json")
    summary = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    normalized = normalize_run_row({**run, "summary_json": summary})
    return {
        **normalized,
        "curve": [
            {
                **row,
                "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            }
            for row in curve
        ],
    }


@router.get("/backtest/trades")
def get_backtest_trades(
    run_id: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
    trade_date: Optional[str] = Query(default=None),
    code: Optional[str] = Query(default=None),
    return_mode: str = Query(default="1d"),
) -> dict:
    conditions = ["run_id = %s"]
    params: list[object] = [run_id]
    if trade_date:
        conditions.append("trade_date = %s")
        params.append(trade_date)
    if code:
        conditions.append("code = %s")
        params.append(code)
    params.append(limit)
    sql = f"""
    SELECT run_id, strategy_id, trade_date, code, entry_date, entry_price,
           exit_date_1d, exit_price_1d, return_1d_pct,
           exit_date_3d, exit_price_3d, return_3d_pct,
           max_gain_pct, max_drawdown_pct
    FROM backtest_trade
    WHERE {' AND '.join(conditions)}
    ORDER BY trade_date DESC, return_{'1d' if return_mode == '1d' else '3d'}_pct DESC
    LIMIT %s
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall() or []
    return {
        "run_id": run_id,
        "limit": limit,
        "trade_date": trade_date,
        "code": code,
        "return_mode": return_mode,
        "items": [
            {
                **row,
                "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
                "entry_date": str(row.get("entry_date")) if row.get("entry_date") else None,
                "exit_date_1d": str(row.get("exit_date_1d")) if row.get("exit_date_1d") else None,
                "exit_date_3d": str(row.get("exit_date_3d")) if row.get("exit_date_3d") else None,
            }
            for row in rows
        ],
    }


@router.get("/backtest/runs")
def get_backtest_runs(
    limit: int = Query(default=20, ge=1, le=100),
    include_system_tests: bool = Query(default=False),
) -> dict:
    where_sql = "" if include_system_tests else "WHERE COALESCE(is_system_test, 0) = 0"
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM backtest_run
                {where_sql}
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall() or []
    return {
        "items": [
            normalize_run_row(row)
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
