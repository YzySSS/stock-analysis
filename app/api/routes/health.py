from __future__ import annotations

from fastapi import APIRouter, Response

from app.jobs.readiness import build_operational_readiness
from app.shared.cache import cache_diagnostics, get_cache_backend
from app.shared.db import mysql_pool_diagnostics
from app.shared.observability import database_metrics, request_metrics

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    cache = cache_diagnostics()
    cache_mode = str(cache.get("backend") or "memory")
    if cache_mode != "redis":
        redis_status = "disabled"
    elif cache.get("status") == "ready":
        redis_status = "ready"
    else:
        redis_status = "degraded"
    backend = get_cache_backend()
    snapshot_pointers = {}
    # Keep liveness cheap: do not make the first Redis connection from /health.
    if cache_mode == "memory" or cache.get("status") in {"ready", "fallback"}:
        for strategy_id in ("a_share_sentiment", "a_share_sentiment_v05"):
            pointer = backend.get(f"sentiment:snapshot:latest:{strategy_id}")
            if isinstance(pointer, dict):
                snapshot_pointers[strategy_id] = pointer
    stable_pointer = snapshot_pointers.get("a_share_sentiment") or {}
    return {
        "status": "ok",
        "cache_mode": cache_mode,
        "redis_status": redis_status,
        "data_snapshot_id": stable_pointer.get("snapshot_id"),
        "data_freshness": snapshot_pointers,
    }


@router.get("/health/performance")
def health_performance() -> dict:
    return {
        "requests": request_metrics.snapshot(),
        "database": database_metrics.snapshot(),
        "connection_pool": mysql_pool_diagnostics(),
        "cache": cache_diagnostics(),
    }


@router.get("/readiness")
def readiness(response: Response) -> dict:
    payload = build_operational_readiness()
    if payload.get("status") == "not_ready":
        response.status_code = 503
    return payload
