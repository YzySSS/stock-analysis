from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any


SPEC_PATH = Path(__file__).resolve().parent / "specs" / "industry_etf_rotation_v1.json"
V1_1_OVERLAY_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "industry_etf_rotation_v1_1_overlay.json"
)
V1_2_OVERLAY_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "industry_etf_rotation_v1_2_overlay.json"
)
V1_1_SPEC_HASH = "8e9457d495c4852fc05f9c5eeb93428e0bcd0f6a40107c85c278e276852c997a"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _validate_weights(label: str, weights: dict[str, Any]) -> None:
    values = [float(value) for value in weights.values()]
    if not values or any(value < 0 for value in values):
        raise ValueError(f"{label} weights must be non-negative")
    if abs(sum(values) - 1.0) > 1e-9:
        raise ValueError(f"{label} weights must sum to 1")


def _validate_spec(spec: dict[str, Any]) -> None:
    if spec.get("model_id") != "industry_etf_rotation_v1":
        raise ValueError("unexpected ETF rotation model_id")
    if spec.get("status") != "research_only_shadow":
        raise ValueError("ETF rotation V1 must remain research_only_shadow")
    if spec.get("instrument_type") != "etf":
        raise ValueError("ETF rotation instrument_type must be etf")
    if spec.get("allow_cash") is not True:
        raise ValueError("ETF rotation V1 must allow an empty selection")
    if spec.get("decision_timing") != "post_close":
        raise ValueError("ETF rotation V1 decision timing must be post_close")
    if spec.get("earliest_execution") != "next_trade_day_open":
        raise ValueError("ETF rotation V1 execution timing must be next_trade_day_open")

    scoring = spec.get("scoring") or {}
    _validate_weights("sector", scoring.get("sector_weights") or {})
    _validate_weights("etf", scoring.get("etf_weights") or {})
    _validate_weights("combined", scoring.get("combined_weights") or {})

    sectors = spec.get("sectors") or []
    if not sectors:
        raise ValueError("ETF rotation V1 requires an explicit sector universe")
    sector_ids = [str(item.get("sector_id") or "") for item in sectors]
    ts_codes = [str(item.get("etf", {}).get("ts_code") or "") for item in sectors]
    if any(not value for value in sector_ids + ts_codes):
        raise ValueError("sector_id and etf.ts_code are required")
    if len(sector_ids) != len(set(sector_ids)):
        raise ValueError("sector_id must be unique")
    if len(ts_codes) != len(set(ts_codes)):
        raise ValueError("each ETF may appear in only one V1 sector")
    for item in sectors:
        if not item.get("fund_flow_industries"):
            raise ValueError(f"{item['sector_id']} requires exact fund-flow aliases")
        if not item.get("opinion_industries"):
            raise ValueError(f"{item['sector_id']} requires exact opinion aliases")

    timing_caps = (spec.get("risk_overlay") or {}).get("timing_state_max_selections") or {}
    expected_states = {
        "cash",
        "defensive",
        "cautious",
        "neutral",
        "risk_on",
        "strong_risk_on",
        "missing",
    }
    if set(timing_caps) != expected_states:
        raise ValueError("timing state caps must cover the complete V2.0 state set")
    if int(timing_caps["cash"]) != 0 or int(timing_caps["missing"]) != 0:
        raise ValueError("cash and missing timing states must fail closed")
    allowed_cycle_states = set(
        (spec.get("risk_overlay") or {}).get(
            "sector_cycle_allowed_states",
            [],
        )
    )
    if allowed_cycle_states != {"first_impulse", "main_up", "pullback"}:
        raise ValueError(
            "ETF rotation cycle gate must only allow first impulse, "
            "main uptrend, and primary-uptrend pullback"
        )
    if spec.get("version") == "1.2.0":
        thresholds = spec.get("startup_confirmation_thresholds") or {}
        required_thresholds = {
            "minimum_return_5d_pct",
            "minimum_return_10d_pct",
            "minimum_return_20d_pct",
            "minimum_ma20_slope_10_pct",
            "strong_return_10d_override_pct",
            "minimum_above_ma20_days_10",
            "minimum_consecutive_above_ma20_days_10",
        }
        if set(thresholds) != required_thresholds:
            raise ValueError(
                "ETF rotation V1.2 startup thresholds are incomplete"
            )


@lru_cache(maxsize=1)
def load_etf_rotation_v1_1_spec() -> dict[str, Any]:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    overlay = json.loads(V1_1_OVERLAY_PATH.read_text(encoding="utf-8"))
    spec["version"] = overlay["version"]
    spec["data_contract"].update(overlay.get("data_contract") or {})
    spec["scoring"]["formula_contract"].update(
        (overlay.get("scoring") or {}).get("formula_contract") or {}
    )
    spec["risk_overlay"].update(overlay.get("risk_overlay") or {})
    spec["guardrails"] = [
        *spec.get("guardrails", []),
        *(overlay.get("guardrails_append") or []),
    ]
    _validate_spec(spec)
    return spec


def etf_rotation_v1_1_spec_hash() -> str:
    return hashlib.sha256(
        _canonical_json(load_etf_rotation_v1_1_spec()).encode("utf-8")
    ).hexdigest()


@lru_cache(maxsize=1)
def load_etf_rotation_spec() -> dict[str, Any]:
    base = load_etf_rotation_v1_1_spec()
    if etf_rotation_v1_1_spec_hash() != V1_1_SPEC_HASH:
        raise ValueError("ETF rotation V1.2 base spec mismatch")
    overlay = json.loads(V1_2_OVERLAY_PATH.read_text(encoding="utf-8"))
    if overlay.get("version") != "1.2.0":
        raise ValueError("unexpected ETF rotation V1.2 version")
    if overlay.get("base_spec_hash") != V1_1_SPEC_HASH:
        raise ValueError("ETF rotation V1.2 base_spec_hash mismatch")

    spec = deepcopy(base)
    spec["version"] = overlay["version"]
    spec["startup_confirmation_thresholds"] = overlay[
        "startup_confirmation_thresholds"
    ]
    spec["guardrails"] = [
        *spec.get("guardrails", []),
        *(overlay.get("guardrails_append") or []),
    ]
    _validate_spec(spec)
    return spec


def etf_rotation_spec_hash() -> str:
    return hashlib.sha256(_canonical_json(load_etf_rotation_spec()).encode("utf-8")).hexdigest()
