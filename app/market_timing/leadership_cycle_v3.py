from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from app.market_timing.leadership_cycle import (
    CYCLE_LABELS as V2_CYCLE_LABELS,
    LeadershipCycleBuilder as V2LeadershipCycleBuilder,
    _number,
    _sha256,
    leadership_cycle_spec_hash as v2_spec_hash,
    load_leadership_cycle_spec as load_v2_spec,
)


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "market_leadership_cycle_v3.json"
)
MODEL_ID = "market_leadership_cycle_v3"
MODEL_VERSION = "3.0.0"
BASE_MODEL_ID = "market_leadership_cycle_v2"
BASE_SPEC_HASH = "80d11f297cc6012e2756e788c0ce9a1afd9c46d7f995fa296171e6f251f65786"

CYCLE_LABELS = {
    **V2_CYCLE_LABELS,
    "stale_data": "数据待对齐",
    "rebound_candidate": "持续修复·B浪候选",
    "oversold_rebound": "超跌反弹·趋势未确认",
}


def load_leadership_cycle_spec() -> dict[str, Any]:
    base = load_v2_spec()
    if base.get("model_id") != BASE_MODEL_ID or v2_spec_hash() != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V3 base spec mismatch")
    overlay = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    if overlay.get("model_id") != MODEL_ID:
        raise RuntimeError("leadership cycle V3 spec model_id mismatch")
    if overlay.get("version") != MODEL_VERSION:
        raise RuntimeError("leadership cycle V3 spec version mismatch")
    if overlay.get("base_spec_hash") != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V3 base_spec_hash mismatch")

    spec = deepcopy(base)
    spec.update(overlay)
    thresholds = spec.get("cycle_thresholds") or {}
    required_thresholds = {
        "completed_impulse_min_runup_pct",
        "completed_impulse_max_drawdown_pct",
        "rebound_observation_min_pct",
        "secondary_decline_max_return_5d_pct",
        "b_wave_min_rebound_pct",
        "b_wave_min_return_5d_pct",
        "b_wave_min_distance_ma20_pct",
        "b_wave_min_ma20_slope_5_pct",
        "b_wave_min_days_since_low",
        "b_wave_min_breadth_score",
        "b_wave_min_above_ma20_pct",
    }
    if set(thresholds) != required_thresholds:
        raise RuntimeError("leadership cycle V3 thresholds are incomplete")
    weights = spec.get("strength_weights") or {}
    if set(weights) != {"heat", "capital", "breadth", "persistence", "price"}:
        raise RuntimeError("leadership cycle V3 strength weights are incomplete")
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-9:
        raise RuntimeError("leadership cycle V3 strength weights must sum to one")
    return spec


def leadership_cycle_spec_hash() -> str:
    return _sha256(load_leadership_cycle_spec())


def build_evidence_alignment(
    trade_date: date | str,
    price_metrics: Mapping[str, Any],
    technical_date: Any,
) -> dict[str, Any]:
    requested = str(trade_date)[:10]
    price_trade_date = str(price_metrics.get("trade_date") or "")[:10] or None
    technical_trade_date = str(technical_date or "")[:10] or None
    return {
        "requested_trade_date": requested,
        "price_trade_date": price_trade_date,
        "technical_trade_date": technical_trade_date,
        "evidence_aligned": (
            price_trade_date == requested and technical_trade_date == requested
        ),
    }


def classify_cycle(
    metrics: Mapping[str, Any],
    breadth: Mapping[str, Any] | None = None,
    *,
    evidence_alignment: Mapping[str, Any] | None = None,
    spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if metrics.get("status") != "ready":
        state = "insufficient_data"
        return {
            "cycle_state": state,
            "cycle_label": CYCLE_LABELS[state],
            "reasons": ["行业价格历史不足60个交易日"],
            "upgrade_triggers": ["补齐行业价格历史后再识别波段位置"],
            "downgrade_triggers": [],
        }
    if evidence_alignment is not None and not evidence_alignment.get(
        "evidence_aligned"
    ):
        state = "stale_data"
        return {
            "cycle_state": state,
            "cycle_label": CYCLE_LABELS[state],
            "reasons": [
                "判定日、行业价格日与技术特征日尚未对齐",
                (
                    f"判定日 {evidence_alignment.get('requested_trade_date') or '-'}，"
                    f"价格 {evidence_alignment.get('price_trade_date') or '-'}，"
                    f"技术特征 {evidence_alignment.get('technical_trade_date') or '-'}"
                ),
            ],
            "upgrade_triggers": ["等待同一交易日的行业价格与技术特征全部就绪"],
            "downgrade_triggers": [],
        }

    active_spec = dict(spec or load_leadership_cycle_spec())
    thresholds = active_spec["cycle_thresholds"]
    breadth_metrics = breadth or {}
    distance20 = _number(metrics.get("distance_ma20_pct")) or 0.0
    distance60 = _number(metrics.get("distance_ma60_pct")) or 0.0
    slope20 = _number(metrics.get("ma20_slope_5_pct")) or 0.0
    slope60 = _number(metrics.get("ma60_slope_5_pct")) or 0.0
    return5 = _number(metrics.get("return_5d_pct")) or 0.0
    return20 = _number(metrics.get("return_20d_pct")) or 0.0
    drawdown = _number(metrics.get("drawdown_from_high_60_pct")) or 0.0
    prior_runup = _number(metrics.get("prior_runup_to_high_pct")) or 0.0
    post_drawdown = _number(metrics.get("post_high_drawdown_pct")) or 0.0
    rebound = _number(metrics.get("rebound_from_post_high_low_pct")) or 0.0
    days_since_high = int(metrics.get("days_since_high_60") or 0)
    days_since_low = int(metrics.get("days_since_post_high_low") or 0)
    breadth_score = _number(breadth_metrics.get("score")) or 0.0
    above_ma20_pct = _number(breadth_metrics.get("above_ma20_pct")) or 0.0

    completed_impulse = (
        prior_runup >= float(thresholds["completed_impulse_min_runup_pct"])
        and post_drawdown
        <= float(thresholds["completed_impulse_max_drawdown_pct"])
    )
    b_wave_checks = {
        "低点后反弹幅度": rebound
        >= float(thresholds["b_wave_min_rebound_pct"]),
        "近5日收益": return5 >= float(thresholds["b_wave_min_return_5d_pct"]),
        "接近或站回MA20": distance20
        >= float(thresholds["b_wave_min_distance_ma20_pct"]),
        "MA20斜率转正": slope20
        > float(thresholds["b_wave_min_ma20_slope_5_pct"]),
        "低点后持续时间": days_since_low
        >= int(thresholds["b_wave_min_days_since_low"]),
        "真实宽度就绪": breadth_metrics.get("status") == "ready",
        "真实宽度扩散": breadth_score
        >= float(thresholds["b_wave_min_breadth_score"]),
        "站上MA20成分占比": above_ma20_pct
        >= float(thresholds["b_wave_min_above_ma20_pct"]),
    }
    b_wave_confirmed = (
        completed_impulse
        and distance60 < 0
        and all(b_wave_checks.values())
    )
    secondary_decline = (
        completed_impulse
        and distance20 < 0
        and distance60 < 0
        and (
            rebound < float(thresholds["rebound_observation_min_pct"])
            or return5
            <= float(thresholds["secondary_decline_max_return_5d_pct"])
        )
    )

    if b_wave_confirmed:
        state = "rebound_candidate"
    elif secondary_decline:
        state = "secondary_decline_risk"
    elif (
        completed_impulse
        and rebound >= float(thresholds["rebound_observation_min_pct"])
        and distance60 < 0
    ):
        state = "oversold_rebound"
    elif (
        distance20 >= 0
        and distance60 >= 0
        and slope20 > 0
        and slope60 >= 0
        and return20 >= 15
        and drawdown >= -5
    ):
        state = "late_acceleration"
    elif (
        distance20 >= 0
        and distance60 >= 0
        and slope20 > 0
        and slope60 >= 0
    ):
        state = "main_up"
    elif (
        prior_runup >= 15
        and distance60 >= 0
        and drawdown <= -5
        and slope60 >= 0
    ):
        state = "pullback"
    elif distance20 >= 0 and slope20 > 0 and distance60 >= -3:
        state = "first_impulse"
    elif distance20 < 0 and distance60 < 0 and slope20 < 0 and slope60 < 0:
        state = "downtrend"
    elif abs(distance60) <= 5 and abs(return20) <= 8 and days_since_high >= 10:
        state = "base"
    else:
        state = "range"

    reasons = [
        f"相对MA20 {distance20:+.1f}%、MA60 {distance60:+.1f}%",
        f"MA20五日斜率 {slope20:+.1f}%、近5日收益 {return5:+.1f}%",
        f"距60日高点 {drawdown:.1f}%",
    ]
    if completed_impulse:
        reasons.extend(
            [
                f"前段上涨 {prior_runup:.1f}%、高点后最大回撤 {post_drawdown:.1f}%",
                f"低点后修复 {rebound:.1f}%、已持续 {days_since_low} 个交易日",
            ]
        )
    if breadth_metrics.get("status") == "ready":
        reasons.append(
            f"真实宽度 {breadth_score:.1f}、MA20上方成分 {above_ma20_pct:.1f}%"
        )
    if state == "oversold_rebound":
        failed = [name for name, passed in b_wave_checks.items() if not passed]
        reasons.append("尚未满足B浪确认：" + "、".join(failed))

    return {
        "cycle_state": state,
        "cycle_label": CYCLE_LABELS[state],
        "reasons": reasons,
        "upgrade_triggers": [
            "低点后反弹至少8%，且近5日收益达到2%",
            "价格接近或站回MA20、MA20五日斜率转正",
            "真实宽度达到40且至少25%成分站上MA20",
        ],
        "downgrade_triggers": [
            "近5日收益跌至-4%以下或反弹重新失守阶段低点",
            "MA20继续下行且成分股宽度再度收缩",
        ],
    }


class LeadershipCycleBuilder(V2LeadershipCycleBuilder):
    model_id = MODEL_ID
    model_version = MODEL_VERSION

    def _load_spec(self) -> dict[str, Any]:
        return load_leadership_cycle_spec()

    def _spec_hash(self) -> str:
        return leadership_cycle_spec_hash()

    @staticmethod
    def _alignment(
        price_metrics: Mapping[str, Any],
        *,
        through: date,
        technical_date: Any,
    ) -> dict[str, Any]:
        return build_evidence_alignment(through, price_metrics, technical_date)

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

    def _price_evidence_status(
        self,
        price_metrics: Mapping[str, Any],
        *,
        through: date,
        technical_date: Any,
    ) -> str:
        status = str(price_metrics.get("status") or "missing")
        if status != "ready":
            return status
        alignment = self._alignment(
            price_metrics,
            through=through,
            technical_date=technical_date,
        )
        return "ready" if alignment["evidence_aligned"] else "stale_data"

    def _extra_data_quality(
        self,
        price_metrics: Mapping[str, Any],
        breadth: Mapping[str, Any],
        *,
        through: date,
        technical_date: Any,
    ) -> dict[str, Any]:
        del breadth
        return {
            **self._alignment(
                price_metrics,
                through=through,
                technical_date=technical_date,
            ),
            "b_wave_confirmation_rule": "sustained_repair_v1",
            "breadth_confirmation_required_for_b_wave": True,
        }
