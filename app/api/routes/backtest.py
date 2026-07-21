from __future__ import annotations

import json
from typing import Optional
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.backtest.policy import research_disclosure
from app.backtest.repository import BacktestRepository
from app.backtest.service import BacktestRequest, BacktestService
from app.shared.instrument_policy import UnsupportedInstrumentError
from app.shared.index_universe import ALL_A_UNIVERSE_CODE, universe_label

router = APIRouter(tags=["backtest"])
backtest_repository = BacktestRepository()


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(entry: float | None, price: float | None) -> float | None:
    if entry is None or price is None or entry <= 0:
        return None
    return round((price - entry) / entry * 100, 4)


def _adjust_price(price: float | None, factor: float | None, entry_factor: float | None) -> float | None:
    if price is None:
        return None
    if factor is None or entry_factor is None or entry_factor <= 0:
        return price
    return price * factor / entry_factor


class BacktestRunRequest(BaseModel):
    strategy_id: str
    start_date: str
    end_date: str
    return_mode: str = "1d"
    trade_strategy_id: Optional[str] = None
    evaluation_mode: str = "research"
    instrument_type: str = "stock"
    universe_code: str = ALL_A_UNIVERSE_CODE
    use_adjusted_price: bool = False
    commission_bps: float = Field(default=0, ge=0, le=100)
    stamp_tax_bps: float = Field(default=0, ge=0, le=100)
    slippage_bps: float = Field(default=0, ge=0, le=100)
    apply_execution_constraints: bool = False
    save: bool = True
    max_picks: Optional[int] = Field(default=None, ge=1, le=50)
    score_threshold: Optional[float] = Field(default=None, ge=0, le=100)


@router.post("/backtest/run", status_code=202)
def run_backtest(payload: BacktestRunRequest) -> dict:
    if not payload.save:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "synchronous_backtest_disabled",
                "message": "回测已统一迁移到可恢复队列，请提交任务并通过 run_id 查询进度。",
            },
        )
    try:
        request = BacktestRequest(**payload.model_dump(exclude={"save"}))
        service = BacktestService()
        run = service.submit(request)
        return normalize_run_row(run)
    except UnsupportedInstrumentError as exc:
        raise HTTPException(status_code=422, detail=exc.as_detail()) from exc
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
        "a_share_sentiment": "A股舆情选股",
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


def normalize_run_row(row: dict, *, include_details: bool = True) -> dict:
    summary = row.get("summary_json")
    if include_details and isinstance(summary, str):
        summary = json.loads(summary)
    request = row.get("request_json")
    if isinstance(request, str):
        request = json.loads(request)
    methodology = row.get("methodology_json")
    if isinstance(methodology, str):
        methodology = json.loads(methodology)
    strategy_id = row.get("strategy_id")
    strategy_version = row.get("strategy_version")
    universe_code = (request or {}).get("universe_code") or ALL_A_UNIVERSE_CODE
    result = {
        **research_disclosure(row.get("methodology_version")),
        "run_id": row.get("run_id"),
        "strategy_id": strategy_id,
        "trade_strategy_id": row.get("trade_strategy_id"),
        "strategy_version": strategy_version,
        "strategy_display_name": strategy_display_name_for_run(strategy_id, strategy_version),
        "start_date": str(row.get("start_date")) if row.get("start_date") else None,
        "end_date": str(row.get("end_date")) if row.get("end_date") else None,
        "return_mode": row.get("return_mode"),
        "evaluation_mode": row.get("evaluation_mode"),
        "data_cutoff_date": str(row.get("data_cutoff_date")) if row.get("data_cutoff_date") else None,
        "strategy_config_hash": row.get("strategy_config_hash"),
        "instrument_type": row.get("instrument_type") or (request or {}).get("instrument_type") or "stock",
        "universe_code": universe_code,
        "universe_label": universe_label(universe_code),
        "status": row.get("status"),
        "phase": row.get("phase"),
        "deduplicated": bool(row.get("deduplicated")),
        "worker_id": row.get("worker_id"),
        "locked_at": str(row.get("locked_at")) if row.get("locked_at") else None,
        "worker_heartbeat_at": str(row.get("worker_heartbeat_at")) if row.get("worker_heartbeat_at") else None,
        "cancel_requested": bool(row.get("cancel_requested")),
        "attempt_count": int(row.get("attempt_count") or 0),
        "max_attempts": int(row.get("max_attempts") or 0),
        "is_system_test": bool(row.get("is_system_test")),
        "validation_baseline_id": row.get("validation_baseline_id"),
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
        "request": request,
        "started_at": str(row.get("started_at")) if row.get("started_at") else None,
        "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
        "error_code": row.get("error_code"),
        "error_message": row.get("error_message"),
    }
    if include_details:
        result["summary"] = summary
        result["methodology"] = methodology
    return result


@router.get("/backtest/results")
def get_backtest_results(run_id: Optional[str] = Query(default=None)) -> dict:
    if not run_id:
        run_id = backtest_repository.latest_official_run_id()
    if not run_id:
        return {**research_disclosure(), "run_id": None, "summary": None, "curve": []}

    run, curve = backtest_repository.load_run_results(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="backtest run not found")

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
    offset = (page - 1) * limit
    page_data = backtest_repository.load_trade_page(
        run_id=run_id,
        limit=limit,
        offset=offset,
        trade_date=trade_date,
        code=code,
        return_mode=return_mode,
    )
    total = page_data["total"]
    rows = page_data["rows"]
    horizon_by_key = build_trade_horizon_rows(
        rows,
        page_data["horizon_bars"],
        page_data["use_adjusted_price"],
    )
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
                "horizon_days": horizon_by_key.get((row.get("trade_date"), row.get("code")), []),
                "factor_json": json.loads(row.get("factor_json")) if isinstance(row.get("factor_json"), str) else row.get("factor_json"),
                "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
                "entry_date": str(row.get("entry_date")) if row.get("entry_date") else None,
                "exit_date_1d": str(row.get("exit_date_1d")) if row.get("exit_date_1d") else None,
                "exit_date_3d": str(row.get("exit_date_3d")) if row.get("exit_date_3d") else None,
            }
            for row in rows
        ],
    }


def build_trade_horizon_rows(
    trades: list[dict],
    bars_by_key: dict[tuple[object, object], list[dict]],
    use_adjusted_price: bool = False,
) -> dict[tuple[object, object], list[dict]]:
    """Build T/T+1/T+2/T+3/T+4 close and intraday risk snapshots for trade rows.

    Values are relative to the recorded entry price. When the run used adjusted
    prices, future close/high/low are compared on the same adjusted basis as the
    backtest return calculation.
    """
    result: dict[tuple[object, object], list[dict]] = {}
    if not trades:
        return result
    for trade in trades:
        code = trade.get("code")
        trade_date = trade.get("trade_date")
        entry_date = trade.get("entry_date") or trade_date
        entry_price = _to_float(trade.get("entry_price"))
        if not code or not trade_date or not entry_date or not entry_price:
            continue
        bars = bars_by_key.get((trade_date, code), [])
        entry_factor = _to_float(bars[0].get("adj_factor")) if bars else None
        items: list[dict] = []
        for day_no, bar in enumerate(bars[:5], start=0):
            factor = _to_float(bar.get("adj_factor")) if use_adjusted_price else None
            base_factor = entry_factor if use_adjusted_price else None
            close = _adjust_price(_to_float(bar.get("close")), factor, base_factor)
            high = _adjust_price(_to_float(bar.get("high")), factor, base_factor)
            low = _adjust_price(_to_float(bar.get("low")), factor, base_factor)
            items.append(
                {
                    "day_no": day_no,
                    "label": "入场日" if day_no == 0 else f"入场+{day_no}",
                    "trade_date": str(bar.get("trade_date")) if bar.get("trade_date") else None,
                    "close_price": round(close, 4) if close is not None else None,
                    "close_return_pct": _pct(entry_price, close),
                    "max_gain_pct": _pct(entry_price, high),
                    "max_drawdown_pct": _pct(entry_price, low),
                }
            )
        result[(trade_date, code)] = items
    return result


@router.get("/backtest/runs")
def get_backtest_runs(
    limit: int = Query(default=20, ge=1, le=100),
    include_system_tests: bool = Query(default=False),
    compact: bool = Query(default=False),
) -> dict:
    rows = backtest_repository.list_runs(
        limit=limit,
        include_system_tests=include_system_tests,
    )
    return {
        "items": [
            normalize_run_row(row, include_details=not compact)
            for row in rows
        ]
    }


@router.get("/factor-input/status")
def get_factor_input_status() -> dict:
    status_rows = backtest_repository.load_factor_input_status()
    summary = status_rows["summary"]
    table_stats = status_rows["table_stats"]
    field_row = status_rows["field_row"]
    latest_task = status_rows["latest_task"]

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
