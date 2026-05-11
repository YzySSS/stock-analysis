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
    commission_bps: float = Field(default=0, ge=0, le=100)
    slippage_bps: float = Field(default=0, ge=0, le=100)
    apply_execution_constraints: bool = False
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


def strategy_display_name_for_run(strategy_id: str | None, strategy_version: str | None) -> str:
    """Build a run display name from the historical version stored on the run.

    Important: this must not read the current strategy registry version to label
    old runs. A backtest run should always display the strategy version that was
    recorded when the run was created, even after later strategy iterations.
    """
    base_names = {
        "lowvol_reversal": "低波反转策略",
        "v13_three_factor": "三因子策略",
        "v12_legacy": "多因子策略",
    }
    base = base_names.get(strategy_id or "", strategy_id or "-")
    if not strategy_version:
        return base
    version_label = str(strategy_version).split("-", 1)[0].strip()
    if not version_label or version_label == "v1":
        return base
    if version_label[0].isdigit():
        version_label = f"v{version_label}"
    return f"{base} {version_label}"


def normalize_run_row(row: dict) -> dict:
    summary = row.get("summary_json")
    if isinstance(summary, str):
        summary = json.loads(summary)
    request = row.get("request_json")
    if isinstance(request, str):
        request = json.loads(request)
    strategy_id = row.get("strategy_id")
    strategy_version = row.get("strategy_version")
    return {
        "run_id": row.get("run_id"),
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "strategy_display_name": strategy_display_name_for_run(strategy_id, strategy_version),
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
    limit: int = Query(default=10, ge=1, le=100),
    page: int = Query(default=1, ge=1),
    trade_date: Optional[str] = Query(default=None),
    code: Optional[str] = Query(default=None),
    return_mode: str = Query(default="1d"),
) -> dict:
    conditions = ["run_id = %s"]
    base_params: list[object] = [run_id]
    if trade_date:
        conditions.append("trade_date = %s")
        base_params.append(trade_date)
    if code:
        conditions.append("code = %s")
        base_params.append(code)
    offset = (page - 1) * limit
    prefixed_conditions = [f"t.{condition}" if condition.startswith(("run_id", "trade_date", "code")) else condition for condition in conditions]
    where_sql = " AND ".join(prefixed_conditions)
    sql = f"""
    SELECT t.run_id, t.strategy_id, t.trade_date, t.code, sb.name,
           p.score AS entry_score, p.factor_json,
           t.entry_date, t.entry_price,
           t.exit_date_1d, t.exit_price_1d, t.return_1d_pct,
           t.exit_date_3d, t.exit_price_3d, t.return_3d_pct,
           t.max_gain_pct, t.max_drawdown_pct
    FROM backtest_trade t
    LEFT JOIN stock_basic sb ON sb.code = t.code
    LEFT JOIN backtest_pick p ON p.run_id = t.run_id AND p.trade_date = t.trade_date AND p.code = t.code
    WHERE {where_sql}
    ORDER BY t.trade_date DESC, t.return_{'1d' if return_mode == '1d' else '3d'}_pct DESC
    LIMIT %s OFFSET %s
    """
    count_sql = f"""
    SELECT COUNT(*) AS total
    FROM backtest_trade t
    WHERE {where_sql}
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_sql, base_params)
            total = int((cursor.fetchone() or {}).get("total") or 0)
            cursor.execute(sql, base_params + [limit, offset])
            rows = cursor.fetchall() or []
    return {
        "run_id": run_id,
        "limit": limit,
        "page": page,
        "total": total,
        "total_pages": (total + limit - 1) // limit if limit else 0,
        "trade_date": trade_date,
        "code": code,
        "return_mode": return_mode,
        "items": [
            {
                **row,
                "factor_json": json.loads(row.get("factor_json")) if isinstance(row.get("factor_json"), str) else row.get("factor_json"),
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
            cursor.execute("SELECT MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date FROM factor_input_daily")
            summary = cursor.fetchone() or {}
            cursor.execute("SELECT TABLE_ROWS AS total_rows FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'factor_input_daily'")
            table_stats = cursor.fetchone() or {}

            latest_trade_date = summary.get("max_trade_date")
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
                WHERE trade_date = %s
                """,
                (latest_trade_date,),
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

    latest_rows = int(field_row.get("total_rows") or 0)
    total_rows = int(table_stats.get("total_rows") or latest_rows)

    def pct(filled: object) -> float | None:
        if not latest_rows:
            return None
        return round((int(filled or 0) / latest_rows) * 100, 2)

    return {
        "coverage": {
            "trade_date_start": str(summary.get("min_trade_date")) if summary.get("min_trade_date") else None,
            "trade_date_end": str(summary.get("max_trade_date")) if summary.get("max_trade_date") else None,
            "covered_stock_codes": latest_rows,
            "covered_rows": total_rows,
            "latest_trade_date_rows": latest_rows,
            "field_coverage_scope": "latest_trade_date",
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
