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
def etf_rotation_spec(spec_hash: str | None = None) -> dict:
    spec = _public_spec()
    if spec_hash is not None and spec_hash != spec['spec_hash']:
        raise HTTPException(status_code=409, detail="ETF specification changed; reload the latest signal")
    return spec


@router.get("/etf-rotation/latest")
def latest_etf_rotation(compact: bool = False) -> dict:
    if compact:
        from app.etf_rotation.display import latest_compact
        spec = _public_spec()
        summary = {key: spec[key] for key in ('model_id', 'model_name', 'version', 'spec_hash')}
        summary['sector_count'] = len(spec['sectors'])
        summary['data_contract'] = {key: spec['data_contract'][key] for key in ('minimum_sector_history_days', 'minimum_etf_history_days')}
        return {'item': latest_compact(spec), 'spec': summary}
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


@router.get("/etf-rotation/runs/{run_id}/candidates")
def etf_rotation_candidates(run_id: str) -> dict:
    from app.etf_rotation.display import candidates
    return {"run_id": run_id, "items": candidates(run_id, details=True)}


@router.get("/etf-rotation/runs/{run_id}/outcomes")
def etf_rotation_outcomes(run_id: str) -> dict:
    from app.etf_rotation.display import outcomes
    return {"run_id": run_id, "items": outcomes(run_id)}
