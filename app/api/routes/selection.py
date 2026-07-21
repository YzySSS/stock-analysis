from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.api.routes.tracking import _invalidate_tracking_summary_cache
from app.jobs.durable_tasks import DurableTaskService
from app.shared.cache import get_cache_backend
from app.shared.instrument_policy import (
    SUPPORTED_SELECTION_INSTRUMENT_TYPES,
    UnsupportedInstrumentError,
    require_supported_instrument,
)
from app.stock_selection.repository import SelectionRepository
from app.stock_selection.run_tasks import SelectionRunService
from app.strategies.service import StrategyService

router = APIRouter(tags=["selection"])
selection_repository = SelectionRepository()
logger = logging.getLogger(__name__)

_SELECTION_RUN_TERMINAL_STATUSES = frozenset({"success", "failed", "cancelled", "no_data"})
_SELECTION_SSE_TIMEOUT_SECONDS = 10 * 60.0
_SELECTION_SSE_POLL_SECONDS = 1.0
_SELECTION_SSE_HEARTBEAT_SECONDS = 15.0
_SELECTION_SSE_DB_RECONCILE_SECONDS = 10.0


class SelectionRunRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
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


def _retired_strategy_detail(strategy_id: str) -> dict:
    return {
        "code": "STRATEGY_RETIRED",
        "strategy_id": strategy_id,
        "message": "该策略已退役，不能创建新的选股任务；历史结果仍可查询。",
    }


def _historical_strategy_summary(strategy_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    """Describe a retired strategy without requiring its implementation/config.

    Historical rows remain readable after cleanup; registry membership only
    controls whether a strategy can create new work.
    """

    first = items[0] if items else {}
    return {
        "id": strategy_id,
        "display_name": first.get("strategy_display_name") or strategy_id,
        "version": first.get("strategy_version"),
        "status": "retired",
        "mode": "historical_only",
        "executable": False,
        "runtime_ready": False,
        "runtime_status": "retired",
        "availability": "retired",
        "availability_label": "仅历史结果",
        "availability_note": "策略实现已清理；历史结果与证据继续只读保留。",
        "factors": [],
    }


def _selection_sse_cache_diagnostics() -> dict[str, Any]:
    """Return diagnostics after triggering the lazy Redis connectivity probe.

    The shared Redis backend intentionally connects lazily.  A read against a
    reserved key makes the diagnostics authoritative for this request without
    writing business data or requiring Redis for normal polling APIs.
    """

    backend = get_cache_backend()
    diagnostics = backend.diagnostics()
    if diagnostics.get("backend") == "redis" and diagnostics.get("status") == "uninitialized":
        backend.get("system:selection-sse-readiness")
        diagnostics = backend.diagnostics()
    return diagnostics


def _selection_sse_is_ready(diagnostics: dict[str, Any]) -> bool:
    return (
        diagnostics.get("backend") == "redis"
        and diagnostics.get("status") == "ready"
        and not bool(diagnostics.get("fallback_active"))
    )


def _selection_sse_cache_read(cache_backend: Any, key: str) -> tuple[Any, dict[str, Any]]:
    """Read Redis-backed state outside the async event loop.

    The shared cache API is intentionally synchronous because normal FastAPI
    routes run it in the thread pool.  SSE is an async route, so both the Redis
    read (including its connectivity probe) and the resulting diagnostics must
    be collected in a worker thread.
    """

    return cache_backend.get(key), cache_backend.diagnostics()


def _sse_event(event: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    return f"event: {event}\ndata: {encoded}\n\n"


async def _selection_run_event_stream(
    *,
    request: Request,
    run_id: str,
    service: SelectionRunService,
    initial_status: dict[str, Any],
    cache_backend: Any,
    timeout_seconds: float = _SELECTION_SSE_TIMEOUT_SECONDS,
    poll_seconds: float = _SELECTION_SSE_POLL_SECONDS,
    heartbeat_seconds: float = _SELECTION_SSE_HEARTBEAT_SECONDS,
    db_reconcile_seconds: float = _SELECTION_SSE_DB_RECONCILE_SECONDS,
) -> AsyncIterator[str]:
    """Stream Redis-shared state with periodic MySQL source-of-truth checks."""

    started_at = time.monotonic()
    last_heartbeat_at = started_at
    last_db_reconcile_at = started_at
    last_payload: str | None = None
    status = initial_status

    while True:
        if await request.is_disconnected():
            return

        serialized = json.dumps(status, ensure_ascii=False, sort_keys=True, default=str)
        if serialized != last_payload:
            yield _sse_event("status", status)
            last_payload = serialized
            last_heartbeat_at = time.monotonic()

        if str(status.get("status") or "") in _SELECTION_RUN_TERMINAL_STATUSES:
            return

        elapsed = time.monotonic() - started_at
        if elapsed >= timeout_seconds:
            yield _sse_event(
                "timeout",
                {
                    "code": "SSE_TIMEOUT",
                    "run_id": run_id,
                    "message": "选股任务状态流已到达最长连接时间，请继续轮询任务状态。",
                },
            )
            return

        await asyncio.sleep(max(0.0, min(poll_seconds, timeout_seconds - elapsed)))
        if await request.is_disconnected():
            return

        try:
            cached_status, cache_diagnostics = await asyncio.to_thread(
                _selection_sse_cache_read,
                cache_backend,
                SelectionRunService.status_cache_key(run_id),
            )
        except Exception as exc:
            logger.warning("selection SSE cache read failed run_id=%s error=%s", run_id, exc)
            yield _sse_event(
                "error",
                {
                    "code": "SSE_CACHE_UNAVAILABLE",
                    "run_id": run_id,
                    "message": "Redis 状态流中断，请继续轮询任务状态。",
                },
            )
            return
        if not _selection_sse_is_ready(cache_diagnostics):
            yield _sse_event(
                "error",
                {
                    "code": "SSE_CACHE_UNAVAILABLE",
                    "run_id": run_id,
                    "message": "Redis 状态流中断，请继续轮询任务状态。",
                },
            )
            return
        if isinstance(cached_status, dict):
            status = cached_status

        now = time.monotonic()
        if now - last_db_reconcile_at >= max(0.0, db_reconcile_seconds):
            try:
                status = await asyncio.to_thread(service.get_run, run_id, False)
                last_db_reconcile_at = now
            except Exception as exc:
                logger.warning("selection SSE status reconciliation failed run_id=%s error=%s", run_id, exc)
                if not isinstance(cached_status, dict):
                    yield _sse_event(
                        "error",
                        {
                            "code": "SSE_STATUS_READ_FAILED",
                            "run_id": run_id,
                            "message": "任务状态流暂时不可用，请继续轮询任务状态。",
                        },
                    )
                    return

        now = time.monotonic()
        if now - last_heartbeat_at >= heartbeat_seconds:
            yield ": keep-alive\n\n"
            last_heartbeat_at = now


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

    if not StrategyService().is_registered_strategy(payload.strategy_id):
        raise HTTPException(status_code=410, detail=_retired_strategy_detail(payload.strategy_id))

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


@router.get("/selection/runs/{run_id}/events")
async def get_selection_run_events(run_id: str, request: Request) -> StreamingResponse:
    diagnostics = await asyncio.to_thread(_selection_sse_cache_diagnostics)
    if not _selection_sse_is_ready(diagnostics):
        redis_status = (
            diagnostics.get("status")
            if diagnostics.get("backend") == "redis"
            else "disabled"
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SSE_UNAVAILABLE",
                "message": "Redis 状态流当前不可用，请使用任务状态轮询接口。",
                "fallback": "polling",
                "cache_mode": diagnostics.get("backend") or "unknown",
                "redis_status": redis_status or "unknown",
            },
        )

    service = SelectionRunService()
    try:
        initial_status = await asyncio.to_thread(service.get_run, run_id, False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return StreamingResponse(
        _selection_run_event_stream(
            request=request,
            run_id=run_id,
            service=service,
            initial_status=initial_status,
            cache_backend=get_cache_backend(),
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
    if not service.is_registered_strategy(payload.strategy_id):
        raise HTTPException(status_code=410, detail=_retired_strategy_detail(payload.strategy_id))
    try:
        result = service.save_strategy_result(
            strategy_id=payload.strategy_id,
            item=payload.item,
            run_id=payload.run_id,
            score_threshold=payload.score_threshold,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    intraday_cache = result.get("intraday_cache") or {}
    if intraday_cache.get("status") == "queued":
        code = result.get("code")
        trade_date = intraday_cache.get("trade_date")
        try:
            if not code or not trade_date:
                raise ValueError("saved selection enrichment requires code and trade_date")
            queued = DurableTaskService().enqueue_selection_enrichment(
                str(code),
                str(trade_date),
            )
            intraday_cache["job_id"] = queued["task_id"]
        except Exception as exc:
            logger.exception("failed to persist saved selection enrichment task")
            intraday_cache.update(
                {
                    "status": "degraded",
                    "error_code": "durable_task_enqueue_failed",
                    "message": "选股结果已保存，但分钟线补全暂未进入任务队列。",
                }
            )
        result["intraday_cache"] = intraday_cache
    _invalidate_tracking_summary_cache()
    return result


def _sample_size(instrument_type: str) -> int:
    return selection_repository.count_instruments(instrument_type)


def _latest_run_meta(instrument_type: str, run_id: Optional[str] = None, strategy_id: Optional[str] = None) -> dict:
    return selection_repository.latest_result_run_meta(
        instrument_type,
        run_id=run_id,
        strategy_id=strategy_id,
    )


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
    strategy = None
    if resolved_strategy_id:
        if service.is_registered_strategy(resolved_strategy_id):
            strategy = service.get_strategy_detail(strategy_id=resolved_strategy_id)
        else:
            strategy = _historical_strategy_summary(resolved_strategy_id, items)

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
