from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.etf_rotation.service import EtfRotationService
from app.etf_rotation.spec import etf_rotation_spec_hash, load_etf_rotation_spec


router = APIRouter(tags=["etf-rotation"])


def _public_spec() -> dict:
    spec = load_etf_rotation_spec()
    return {
        "model_id": spec["model_id"],
        "model_name": spec["model_name"],
        "version": spec["version"],
        "status": spec["status"],
        "spec_hash": etf_rotation_spec_hash(),
        "instrument_type": spec["instrument_type"],
        "decision_timing": spec["decision_timing"],
        "earliest_execution": spec["earliest_execution"],
        "allow_cash": spec["allow_cash"],
        "maximum_selections": spec["maximum_selections"],
        "data_contract": spec["data_contract"],
        "scoring": spec["scoring"],
        "risk_overlay": spec["risk_overlay"],
        "forward_observation": spec["forward_observation"],
        "guardrails": spec["guardrails"],
        "sectors": spec["sectors"],
    }


@router.get("/etf-rotation/spec")
def etf_rotation_spec() -> dict:
    return _public_spec()


@router.get("/etf-rotation/latest")
def latest_etf_rotation() -> dict:
    return {
        "item": EtfRotationService().latest(),
        "spec": _public_spec(),
    }


@router.get("/etf-rotation/runs/{run_id}")
def etf_rotation_run(run_id: str) -> dict:
    try:
        item = EtfRotationService().get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"item": item, "spec": _public_spec()}
