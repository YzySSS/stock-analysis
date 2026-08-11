from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from app.market_timing.leadership_cycle import _number, _sha256
from app.market_timing.leadership_cycle_v4 import (
    LeadershipCycleBuilder as V4LeadershipCycleBuilder,
    classify_cycle as classify_v4_cycle,
    leadership_cycle_spec_hash as v4_spec_hash,
    load_leadership_cycle_spec as load_v4_spec,
)


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "market_leadership_cycle_v5.json"
)
MODEL_ID = "market_leadership_cycle_v5"
MODEL_VERSION = "5.0.0"
BASE_MODEL_ID = "market_leadership_cycle_v4"
BASE_SPEC_HASH = "ba716c57768fbb0260ff17781b6fd4096fb640a1fcc1e3501b0598d0eed545e8"

STRONG_STATES = frozenset({"confirmed", "core", "crowded"})
STRENGTH_LABELS = {
    "watch": "观察",
    "confirmed": "强度达标",
    "core": "核心强势",
    "crowded": "高位拥挤",
    "fading": "退潮",
}
CYCLE_LABELS = {
    "impulse_watch": "短线转强·等待延续",
    "first_impulse": "多周期转强",
}


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list, tuple)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def load_leadership_cycle_spec() -> dict[str, Any]:
    base = load_v4_spec()
    if base.get("model_id") != BASE_MODEL_ID or v4_spec_hash() != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V5 base spec mismatch")
    overlay = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    if overlay.get("model_id") != MODEL_ID:
        raise RuntimeError("leadership cycle V5 spec model_id mismatch")
    if overlay.get("version") != MODEL_VERSION:
        raise RuntimeError("leadership cycle V5 spec version mismatch")
    if overlay.get("base_spec_hash") != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V5 base_spec_hash mismatch")

    spec = deepcopy(base)
    spec.update(overlay)
    thresholds = spec.get("strength_confirmation_thresholds") or {}
    required_thresholds = {
        "minimum_leadership_score",
        "minimum_heat_score",
        "minimum_capital_score",
        "minimum_breadth_score",
        "minimum_price_score",
        "minimum_source_count",
        "minimum_confidence",
        "minimum_capital_component_coverage",
        "maximum_market_branches",
        "constructive_cycle_states",
    }
    if set(thresholds) != required_thresholds:
        raise RuntimeError("leadership cycle V5 strength thresholds are incomplete")
    return spec


def leadership_cycle_spec_hash() -> str:
    return _sha256(load_leadership_cycle_spec())


def _cycle_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return (
        value.replace("短线转强·启动待确认", CYCLE_LABELS["impulse_watch"])
        .replace("多周期启动确认", CYCLE_LABELS["first_impulse"])
        .replace("启动确认", "多周期转强标准")
    )


def classify_cycle(
    metrics: Mapping[str, Any],
    breadth: Mapping[str, Any] | None = None,
    *,
    evidence_alignment: Mapping[str, Any] | None = None,
    spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cycle = dict(
        classify_v4_cycle(
            metrics,
            breadth,
            evidence_alignment=evidence_alignment,
            spec=spec or load_leadership_cycle_spec(),
        )
    )
    state = str(cycle.get("cycle_state") or "")
    if state in CYCLE_LABELS:
        cycle["cycle_label"] = CYCLE_LABELS[state]
    for field in ("reasons", "upgrade_triggers", "downgrade_triggers"):
        cycle[field] = [_cycle_text(item) for item in (cycle.get(field) or [])]
    return cycle


def resolve_capital_evidence(
    *,
    sector_type: str,
    sector_name: str,
    price_industries: Sequence[str],
    flow_latest: Mapping[tuple[str, str], Mapping[str, Any]],
    minimum_component_coverage: float,
) -> dict[str, Any]:
    exact = flow_latest.get((sector_type, sector_name))
    exact_amount = _number(exact.get("net_amount")) if exact else None
    if exact_amount is not None:
        return {
            "status": "observed",
            "mode": "exact_sector",
            "net_amount": round(exact_amount, 4),
            "expected_count": 1,
            "covered_count": 1,
            "coverage": 1.0,
            "members": [f"{sector_type}:{sector_name}"],
            "quote_time_start": str(exact.get("quote_time") or "") or None,
            "quote_time_end": str(exact.get("quote_time") or "") or None,
        }

    expected = tuple(
        dict.fromkeys(
            str(item).strip() for item in price_industries if str(item).strip()
        )
    )
    if sector_type != "theme" or not expected:
        return {
            "status": "missing",
            "mode": "none",
            "net_amount": None,
            "expected_count": len(expected),
            "covered_count": 0,
            "coverage": 0.0,
            "members": [],
            "quote_time_start": None,
            "quote_time_end": None,
        }

    members: list[tuple[str, Mapping[str, Any], float]] = []
    for industry in expected:
        member = flow_latest.get(("industry", industry))
        amount = _number(member.get("net_amount")) if member else None
        if member is not None and amount is not None:
            members.append((industry, member, amount))
    coverage = len(members) / len(expected)
    status = (
        "observed"
        if members and coverage >= float(minimum_component_coverage)
        else ("incomplete" if members else "missing")
    )
    quote_times = sorted(
        str(member.get("quote_time"))
        for _, member, _ in members
        if member.get("quote_time") is not None
    )
    return {
        "status": status,
        "mode": "mapped_industry_sum" if members else "none",
        "net_amount": (
            round(sum(amount for _, _, amount in members), 4)
            if status == "observed"
            else None
        ),
        "expected_count": len(expected),
        "covered_count": len(members),
        "coverage": round(coverage, 4),
        "members": [f"industry:{industry}" for industry, _, _ in members],
        "quote_time_start": quote_times[0] if quote_times else None,
        "quote_time_end": quote_times[-1] if quote_times else None,
    }


def build_strength_confirmation_checks(
    row: Mapping[str, Any],
    *,
    capital_evidence: Mapping[str, Any],
    source_count: int | None,
    positive_news_count: int | None,
    negative_news_count: int | None,
    base_opinion_confirmed: bool,
    thresholds: Mapping[str, Any],
) -> dict[str, bool]:
    price_metrics = row.get("price_metrics") or {}
    breadth_metrics = row.get("breadth_metrics") or {}
    constructive_states = set(thresholds["constructive_cycle_states"])
    source_ready = (
        int(source_count) >= int(thresholds["minimum_source_count"])
        if source_count is not None
        else bool(base_opinion_confirmed)
    )
    news_balance_ready = (
        int(positive_news_count or 0) >= int(negative_news_count or 0)
        if positive_news_count is not None and negative_news_count is not None
        else bool(base_opinion_confirmed)
    )
    return {
        "综合强度达到确认线": (_number(row.get("leadership_score")) or 0.0)
        >= float(thresholds["minimum_leadership_score"]),
        "舆情热度达到确认线": (_number(row.get("heat_score")) or 0.0)
        >= float(thresholds["minimum_heat_score"]),
        "资金证据真实可用": capital_evidence.get("status") == "observed",
        "资金不为净流出": (_number(row.get("capital_score")) or 0.0)
        >= float(thresholds["minimum_capital_score"]),
        "真实宽度达到确认线": (_number(row.get("breadth_score")) or 0.0)
        >= float(thresholds["minimum_breadth_score"]),
        "价格结构达到确认线": (_number(row.get("price_score")) or 0.0)
        >= float(thresholds["minimum_price_score"]),
        "舆情来源达到确认线": source_ready,
        "正负舆情未出现倒挂": news_balance_ready,
        "价格证据已对齐": row.get("price_evidence_status") == "ready",
        "真实宽度已就绪": breadth_metrics.get("status") == "ready",
        "行情阶段允许确认": row.get("cycle_state") in constructive_states,
        "价格站上MA60": (_number(price_metrics.get("distance_ma60_pct")) or 0.0)
        >= 0.0,
        "综合置信度达到确认线": (_number(row.get("confidence")) or 0.0)
        >= float(thresholds["minimum_confidence"]),
    }


def classify_strength(
    row: Mapping[str, Any],
    *,
    checks: Mapping[str, bool],
) -> str:
    if row.get("leadership_state") == "fading":
        return "fading"
    if not all(checks.values()):
        return "watch"
    leadership_score = _number(row.get("leadership_score")) or 0.0
    heat_score = _number(row.get("heat_score")) or 0.0
    capital_score = _number(row.get("capital_score")) or 0.0
    breadth_score = _number(row.get("breadth_score")) or 0.0
    price_score = _number(row.get("price_score")) or 0.0
    crowding_score = _number(row.get("crowding_score")) or 0.0
    if (
        crowding_score >= 65
        and leadership_score >= 65
        and row.get("cycle_state") in {"main_up", "late_acceleration"}
    ):
        return "crowded"
    if (
        leadership_score >= 72
        and heat_score >= 55
        and capital_score >= 55
        and breadth_score >= 55
        and price_score >= 55
    ):
        return "core"
    return "confirmed"


class LeadershipCycleBuilder(V4LeadershipCycleBuilder):
    model_id = MODEL_ID
    model_version = MODEL_VERSION

    def _load_spec(self) -> dict[str, Any]:
        return load_leadership_cycle_spec()

    def _spec_hash(self) -> str:
        return leadership_cycle_spec_hash()

    def _classify_cycle(
        self,
        price_metrics: Mapping[str, Any],
        breadth: Mapping[str, Any],
        *,
        through: date,
        technical_date: Any,
    ) -> dict[str, Any]:
        return classify_cycle(
            price_metrics,
            breadth,
            evidence_alignment=self._alignment(
                price_metrics,
                through=through,
                technical_date=technical_date,
            ),
            spec=self.spec,
        )

    def _load_base_snapshot(self, trade_date: date | str) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT model_id, version, spec_hash, trade_date,
                           as_of_datetime, data_cutoff_datetime,
                           sector_type, sector_name, leadership_state,
                           state_label, cycle_state, cycle_label,
                           leadership_score, confidence,
                           heat_score, capital_score, breadth_score,
                           persistence_score, crowding_score, price_score,
                           price_evidence_status, price_metrics_json,
                           breadth_metrics_json, evidence_json,
                           contradiction_json, upgrade_triggers_json,
                           downgrade_triggers_json, source_lineage_json,
                           data_quality_json, payload_hash
                    FROM market_leadership_state_daily
                    WHERE model_id=%s AND trade_date=%s
                    ORDER BY leadership_score DESC, sector_type, sector_name
                    """,
                    (BASE_MODEL_ID, trade_date),
                )
                stored = list(cursor.fetchall() or [])
        result = []
        for row in stored:
            result.append(
                {
                    "model_id": MODEL_ID,
                    "version": MODEL_VERSION,
                    "spec_hash": self.spec_hash,
                    "trade_date": str(row.get("trade_date")),
                    "as_of": str(row.get("as_of_datetime")),
                    "data_cutoff": str(row.get("data_cutoff_datetime")),
                    "sector_type": row.get("sector_type"),
                    "sector_name": row.get("sector_name"),
                    "leadership_state": row.get("leadership_state"),
                    "state_label": row.get("state_label"),
                    "cycle_state": row.get("cycle_state"),
                    "cycle_label": row.get("cycle_label"),
                    "leadership_score": _number(row.get("leadership_score")),
                    "confidence": _number(row.get("confidence")),
                    "heat_score": _number(row.get("heat_score")),
                    "capital_score": _number(row.get("capital_score")),
                    "breadth_score": _number(row.get("breadth_score")),
                    "persistence_score": _number(row.get("persistence_score")),
                    "crowding_score": _number(row.get("crowding_score")),
                    "price_score": _number(row.get("price_score")),
                    "price_evidence_status": row.get("price_evidence_status"),
                    "price_metrics": _json_value(row.get("price_metrics_json"), {}),
                    "breadth_metrics": _json_value(row.get("breadth_metrics_json"), {}),
                    "evidence": _json_value(row.get("evidence_json"), []),
                    "contradictions": _json_value(row.get("contradiction_json"), []),
                    "upgrade_triggers": _json_value(row.get("upgrade_triggers_json"), []),
                    "downgrade_triggers": _json_value(row.get("downgrade_triggers_json"), []),
                    "source_lineage": _json_value(row.get("source_lineage_json"), {}),
                    "data_quality": _json_value(row.get("data_quality_json"), {}),
                    "base_payload_hash": row.get("payload_hash"),
                    "research_only": True,
                }
            )
        return result

    def _load_confirmation_context(
        self,
        trade_date: date | str,
    ) -> tuple[
        dict[tuple[str, str], dict[str, Any]],
        dict[tuple[str, str], dict[str, Any]],
    ]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, source_count,
                           positive_news_count, negative_news_count,
                           as_of_datetime
                    FROM sector_opinion_daily
                    WHERE trade_date=%s
                    ORDER BY as_of_datetime
                    """,
                    (trade_date,),
                )
                opinion_rows = list(cursor.fetchall() or [])
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, net_amount, pct_chg,
                           company_count, quote_time
                    FROM market_sector_fund_flow_intraday
                    WHERE trade_date=%s
                    ORDER BY quote_time
                    """,
                    (trade_date,),
                )
                flow_rows = list(cursor.fetchall() or [])
        opinion_latest = {
            (str(row["sector_type"]), str(row["sector_name"])): dict(row)
            for row in opinion_rows
        }
        flow_latest = {
            (str(row["sector_type"]), str(row["sector_name"])): dict(row)
            for row in flow_rows
        }
        return opinion_latest, flow_latest

    @staticmethod
    def _stored_capital_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
        lineage = row.get("source_lineage") or {}
        source = lineage.get("capital")
        if isinstance(source, Mapping):
            status = str(source.get("status") or "missing")
        else:
            status = "observed" if source == "market_sector_fund_flow_intraday" else "missing"
        capital_score = _number(row.get("capital_score"))
        return {
            "status": status,
            "mode": "immutable_base_exact" if status == "observed" else "none",
            "net_amount": (
                round((capital_score - 50.0) * 5.0, 4)
                if status == "observed" and capital_score is not None
                else None
            ),
            "expected_count": 1 if status == "observed" else 0,
            "covered_count": 1 if status == "observed" else 0,
            "coverage": 1.0 if status == "observed" else 0.0,
            "members": [],
            "quote_time_start": None,
            "quote_time_end": None,
        }

    def _finalize_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        base_snapshot: bool,
        opinion_latest: Mapping[tuple[str, str], Mapping[str, Any]],
        flow_latest: Mapping[tuple[str, str], Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        thresholds = self.spec["strength_confirmation_thresholds"]
        hierarchy_groups = self.spec.get("hierarchy_groups") or {}
        result = []
        for source_row in rows:
            row = deepcopy(dict(source_row))
            row["model_id"] = MODEL_ID
            row["version"] = MODEL_VERSION
            row["spec_hash"] = self.spec_hash
            base_state = str(row.get("leadership_state") or "watch")
            sector_type = str(row.get("sector_type") or "")
            sector_name = str(row.get("sector_name") or "")
            sector_key = (sector_type, sector_name)

            cycle_state = str(row.get("cycle_state") or "")
            if cycle_state in CYCLE_LABELS:
                row["cycle_label"] = CYCLE_LABELS[cycle_state]
            for field in ("evidence", "contradictions", "upgrade_triggers", "downgrade_triggers"):
                row[field] = [_cycle_text(item) for item in (row.get(field) or [])]

            source_lineage = deepcopy(dict(row.get("source_lineage") or {}))
            price_industries = list(
                ((source_lineage.get("price") or {}).get("industries") or [])
            )
            previous_capital_source = source_lineage.get("capital")
            previous_capital_observed = (
                previous_capital_source == "market_sector_fund_flow_intraday"
                or (
                    isinstance(previous_capital_source, Mapping)
                    and previous_capital_source.get("status") == "observed"
                )
            )
            if base_snapshot:
                capital_evidence = self._stored_capital_evidence(row)
            else:
                capital_evidence = resolve_capital_evidence(
                    sector_type=sector_type,
                    sector_name=sector_name,
                    price_industries=price_industries,
                    flow_latest=flow_latest,
                    minimum_component_coverage=float(
                        thresholds["minimum_capital_component_coverage"]
                    ),
                )
                amount = _number(capital_evidence.get("net_amount"))
                row["capital_score"] = round(
                    50 + max(-45, min(45, amount / 5))
                    if amount is not None
                    else 50.0,
                    2,
                )
                weights = self.spec["strength_weights"]
                row["leadership_score"] = round(
                    (_number(row.get("heat_score")) or 0.0) * float(weights["heat"])
                    + (_number(row.get("capital_score")) or 0.0) * float(weights["capital"])
                    + (_number(row.get("breadth_score")) or 0.0) * float(weights["breadth"])
                    + (_number(row.get("persistence_score")) or 0.0) * float(weights["persistence"])
                    + (_number(row.get("price_score")) or 0.0) * float(weights["price"]),
                    2,
                )
                capital_now_observed = capital_evidence.get("status") == "observed"
                confidence = _number(row.get("confidence")) or 0.0
                if capital_now_observed and not previous_capital_observed:
                    confidence += 0.15
                elif previous_capital_observed and not capital_now_observed:
                    confidence -= 0.15
                row["confidence"] = round(max(0.0, min(1.0, confidence)), 4)

            evidence = list(row.get("evidence") or [])
            if evidence:
                evidence[0] = str(evidence[0]).replace("主线强度", "行业综合强度")
            amount = _number(capital_evidence.get("net_amount"))
            if capital_evidence.get("status") == "observed":
                if capital_evidence.get("mode") == "mapped_industry_sum":
                    capital_text = (
                        f"主题行业资金合计 {amount:+.2f}（"
                        f"{capital_evidence.get('covered_count')}/"
                        f"{capital_evidence.get('expected_count')}）"
                    )
                else:
                    capital_text = f"当日资金 {amount:+.2f}"
            elif capital_evidence.get("status") == "incomplete":
                capital_text = (
                    "主题资金覆盖不足 "
                    f"{capital_evidence.get('covered_count')}/"
                    f"{capital_evidence.get('expected_count')}，仅作中性评分"
                )
            else:
                capital_text = "资金证据缺失，仅作中性评分"
            if len(evidence) >= 2:
                evidence[1] = capital_text
            elif evidence:
                evidence.append(capital_text)
            else:
                evidence = [
                    f"行业综合强度 {(_number(row.get('leadership_score')) or 0.0):.1f}",
                    capital_text,
                ]
            row["evidence"] = evidence

            opinion = opinion_latest.get(sector_key)
            source_count = int(opinion.get("source_count") or 0) if opinion else None
            positive = int(opinion.get("positive_news_count") or 0) if opinion else None
            negative = int(opinion.get("negative_news_count") or 0) if opinion else None
            base_opinion_confirmed = (
                base_snapshot
                and base_state in STRONG_STATES
                and (_number(row.get("confidence")) or 0.0)
                >= float(thresholds["minimum_confidence"])
                and not any("负面新闻多于正面新闻" in str(item) for item in row.get("contradictions") or [])
            )
            checks = build_strength_confirmation_checks(
                row,
                capital_evidence=capital_evidence,
                source_count=source_count,
                positive_news_count=positive,
                negative_news_count=negative,
                base_opinion_confirmed=base_opinion_confirmed,
                thresholds=thresholds,
            )
            strength_state = classify_strength(row, checks=checks)
            row["leadership_state"] = strength_state
            row["state_label"] = STRENGTH_LABELS[strength_state]

            contradictions = list(row.get("contradictions") or [])
            if capital_evidence.get("status") != "observed":
                contradictions.append("资金证据缺失或覆盖不足·不得确认")
            elif (_number(row.get("capital_score")) or 0.0) < float(
                thresholds["minimum_capital_score"]
            ):
                contradictions.append("当日资金净流出·不得确认")
            if (_number((row.get("price_metrics") or {}).get("distance_ma60_pct")) or 0.0) < 0:
                contradictions.append("价格仍在MA60下方·不得确认")
            if row.get("cycle_state") not in set(thresholds["constructive_cycle_states"]):
                contradictions.append(f"{row.get('cycle_label') or '行情阶段待确认'}·仅作雷达观察")
            if strength_state == "watch":
                contradictions.append("主线证据未闭环，仅作行业观察")
            row["contradictions"] = list(dict.fromkeys(str(item) for item in contradictions if item))

            source_lineage["capital"] = {
                "source": (
                    "market_sector_fund_flow_intraday"
                    if capital_evidence.get("status") == "observed"
                    else None
                ),
                **dict(capital_evidence),
            }
            if base_snapshot:
                source_lineage["base_snapshot"] = {
                    "model_id": BASE_MODEL_ID,
                    "payload_hash": row.pop("base_payload_hash", None),
                    "decision_time_evidence_reused": True,
                }
            row["source_lineage"] = source_lineage

            failed_checks = [name for name, passed in checks.items() if not passed]
            hierarchy_key = f"{sector_type}:{sector_name}"
            data_quality = deepcopy(dict(row.get("data_quality") or {}))
            data_quality.update(
                {
                    "strength_confirmation_rule": "observed_capital_constructive_cycle_v1",
                    "capital_evidence_status": capital_evidence.get("status"),
                    "capital_flow_mode": capital_evidence.get("mode"),
                    "capital_component_coverage": capital_evidence.get("coverage"),
                    "strength_confirmation_checks": checks,
                    "strength_confirmation_failed_checks": failed_checks,
                    "market_confirmation_eligible": (
                        strength_state in STRONG_STATES and all(checks.values())
                    ),
                    "hierarchy_group": hierarchy_groups.get(hierarchy_key, hierarchy_key),
                    "market_mainline_maximum": 1,
                    "market_branch_maximum": int(thresholds["maximum_market_branches"]),
                    "display_confirmation_reserved_for_market_mainline": True,
                    "base_snapshot_reused": base_snapshot,
                }
            )
            row["data_quality"] = data_quality
            row["research_only"] = True
            row.pop("payload_hash", None)
            row["payload_hash"] = _sha256(row)
            result.append(row)
        return result

    def build_rows(self, trade_date: date | str) -> list[dict[str, Any]]:
        base_rows = self._load_base_snapshot(trade_date)
        if base_rows:
            return self._finalize_rows(
                base_rows,
                base_snapshot=True,
                opinion_latest={},
                flow_latest={},
            )
        raw_rows = super().build_rows(trade_date)
        opinion_latest, flow_latest = self._load_confirmation_context(trade_date)
        return self._finalize_rows(
            raw_rows,
            base_snapshot=False,
            opinion_latest=opinion_latest,
            flow_latest=flow_latest,
        )
