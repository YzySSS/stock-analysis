from __future__ import annotations

from fastapi import APIRouter, Response

from app.jobs.readiness import build_operational_readiness

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.get("/readiness")
def readiness(response: Response) -> dict:
    payload = build_operational_readiness()
    if payload.get("status") == "not_ready":
        response.status_code = 503
    return payload
