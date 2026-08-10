from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from app.market_timing.leadership_cycle import (
    _number,
    _sha256,
    compute_price_metrics as compute_v2_price_metrics,
)
from app.market_timing.leadership_cycle_v3 import (
    CYCLE_LABELS as V3_CYCLE_LABELS,
    LeadershipCycleBuilder as V3LeadershipCycleBuilder,
    classify_cycle as classify_v3_cycle,
    leadership_cycle_spec_hash as v3_spec_hash,
    load_leadership_cycle_spec as load_v3_spec,
)


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "market_leadership_cycle_v4.json"
)
MODEL_ID = "market_leadership_cycle_v4"
MODEL_VERSION = "4.0.0"
BASE_MODEL_ID = "market_leadership_cycle_v3"
BASE_SPEC_HASH = "2d503ba55a8ee533f9323103e75e4b6a9f0436a17decbd2d1cf48823529936b4"

CYCLE_LABELS = {
    **V3_CYCLE_LABELS,
    "impulse_watch": "短线转强·启动待确认",
    "first_impulse": "多周期启动确认",
}


def load_leadership_cycle_spec() -> dict[str, Any]:
    base = load_v3_spec()
    if base.get("model_id") != BASE_MODEL_ID or v3_spec_hash() != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V4 base spec mismatch")
    overlay = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    if overlay.get("model_id") != MODEL_ID:
        raise RuntimeError("leadership cycle V4 spec model_id mismatch")
    if overlay.get("version") != MODEL_VERSION:
        raise RuntimeError("leadership cycle V4 spec version mismatch")
    if overlay.get("base_spec_hash") != BASE_SPEC_HASH:
        raise RuntimeError("leadership cycle V4 base_spec_hash mismatch")

    spec = deepcopy(base)
    spec.update(overlay)
    thresholds = spec.get("startup_confirmation_thresholds") or {}
    required_thresholds = {
        "minimum_return_5d_pct",
        "minimum_return_10d_pct",
        "minimum_return_20d_pct",
        "minimum_ma20_slope_10_pct",
        "strong_return_10d_override_pct",
        "minimum_above_ma20_days_10",
        "minimum_consecutive_above_ma20_days_10",
        "minimum_breadth_score",
        "minimum_above_ma20_pct",
    }
    if set(thresholds) != required_thresholds:
        raise RuntimeError("leadership cycle V4 startup thresholds are incomplete")
    return spec


def leadership_cycle_spec_hash() -> str:
    return _sha256(load_leadership_cycle_spec())


def compute_price_metrics(
    series: Sequence[Mapping[str, Any]],
    *,
    minimum_days: int = 60,
) -> dict[str, Any]:
    metrics = compute_v2_price_metrics(series, minimum_days=minimum_days)
    if metrics.get("status") != "ready":
        return metrics

    values = [
        float(value)
        for row in series
        if (
            (value := _number(row.get("value"))) is not None
            and value > 0
            and str(row.get("trade_date") or "")[:10]
        )
    ]
    current = values[-1]
    ma20 = mean(values[-20:])
    prior_ma20_10 = mean(values[-30:-10])
    above_ma20_flags = []
    for index in range(len(values) - 10, len(values)):
        rolling_ma20 = mean(values[index - 19 : index + 1])
        above_ma20_flags.append(values[index] > rolling_ma20)
    consecutive_above_ma20 = 0
    for is_above in reversed(above_ma20_flags):
        if not is_above:
            break
        consecutive_above_ma20 += 1

    metrics.update(
        {
            "return_10d_pct": round((current / values[-11] - 1) * 100, 4),
            "return_20d_exact_pct": round(
                (current / values[-21] - 1) * 100,
                4,
            ),
            "ma20_slope_10_pct": round(
                (ma20 / prior_ma20_10 - 1) * 100,
                4,
            ),
            "above_ma20_days_10": sum(above_ma20_flags),
            "consecutive_above_ma20_days_10": consecutive_above_ma20,
        }
    )
    return metrics


def build_startup_price_checks(
    metrics: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> dict[str, bool]:
    return5 = _number(metrics.get("return_5d_pct")) or 0.0
    return10 = _number(metrics.get("return_10d_pct")) or 0.0
    return20 = _number(metrics.get("return_20d_exact_pct"))
    if return20 is None:
        return20 = _number(metrics.get("return_20d_pct")) or 0.0
    slope20_10 = _number(metrics.get("ma20_slope_10_pct")) or 0.0
    above_ma20_days = int(metrics.get("above_ma20_days_10") or 0)
    consecutive_above_ma20 = int(
        metrics.get("consecutive_above_ma20_days_10") or 0
    )
    return {
        "近5日未明显走弱": return5
        >= float(thresholds["minimum_return_5d_pct"]),
        "近10日收益达到确认线": return10
        >= float(thresholds["minimum_return_10d_pct"]),
        "近20日收益达到确认线": return20
        >= float(thresholds["minimum_return_20d_pct"]),
        "中期趋势或强动量确认": (
            slope20_10 > float(thresholds["minimum_ma20_slope_10_pct"])
            or return10
            >= float(thresholds["strong_return_10d_override_pct"])
        ),
        "近10日至少6日站上MA20": above_ma20_days
        >= int(thresholds["minimum_above_ma20_days_10"]),
        "连续站上MA20至少3日": consecutive_above_ma20
        >= int(thresholds["minimum_consecutive_above_ma20_days_10"]),
    }


def classify_cycle(
    metrics: Mapping[str, Any],
    breadth: Mapping[str, Any] | None = None,
    *,
    evidence_alignment: Mapping[str, Any] | None = None,
    spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    active_spec = dict(spec or load_leadership_cycle_spec())
    cycle = classify_v3_cycle(
        metrics,
        breadth,
        evidence_alignment=evidence_alignment,
        spec=active_spec,
    )
    if cycle["cycle_state"] != "first_impulse":
        return cycle

    thresholds = active_spec["startup_confirmation_thresholds"]
    breadth_metrics = breadth or {}
    return5 = _number(metrics.get("return_5d_pct")) or 0.0
    return10 = _number(metrics.get("return_10d_pct")) or 0.0
    return20 = _number(metrics.get("return_20d_exact_pct"))
    if return20 is None:
        return20 = _number(metrics.get("return_20d_pct")) or 0.0
    slope20_10 = _number(metrics.get("ma20_slope_10_pct")) or 0.0
    above_ma20_days = int(metrics.get("above_ma20_days_10") or 0)
    consecutive_above_ma20 = int(
        metrics.get("consecutive_above_ma20_days_10") or 0
    )
    breadth_score = _number(breadth_metrics.get("score")) or 0.0
    above_ma20_pct = _number(breadth_metrics.get("above_ma20_pct")) or 0.0

    checks = {
        **build_startup_price_checks(metrics, thresholds),
        "真实宽度就绪": breadth_metrics.get("status") == "ready",
        "真实宽度达到确认线": breadth_score
        >= float(thresholds["minimum_breadth_score"]),
        "站上MA20成分达到确认线": above_ma20_pct
        >= float(thresholds["minimum_above_ma20_pct"]),
    }
    reasons = [
        *cycle["reasons"],
        f"近10日收益 {return10:+.1f}%、近20日收益 {return20:+.1f}%",
        (
            f"MA20十日斜率 {slope20_10:+.1f}%，近10日有 "
            f"{above_ma20_days} 日站上MA20、连续 {consecutive_above_ma20} 日"
        ),
    ]

    if all(checks.values()):
        return {
            **cycle,
            "cycle_label": CYCLE_LABELS["first_impulse"],
            "reasons": reasons,
            "upgrade_triggers": [
                "MA60斜率转正且价格继续站稳MA20、MA60",
                "10日与20日趋势、真实宽度继续同步扩散",
            ],
            "downgrade_triggers": [
                "近10日收益跌破3%或近5日收益跌至-3%以下",
                "近10日站上MA20不足6日或真实宽度跌破55",
            ],
        }

    failed = [name for name, passed in checks.items() if not passed]
    return {
        **cycle,
        "cycle_state": "impulse_watch",
        "cycle_label": CYCLE_LABELS["impulse_watch"],
        "reasons": [
            *reasons,
            "尚未满足多周期启动确认：" + "、".join(failed),
        ],
        "upgrade_triggers": [
            "近10日收益至少3%、近20日收益至少3%，且近5日不低于-3%",
            "近10日至少6日、最近连续3日站上MA20",
            "真实宽度至少55且至少60%成分站上MA20",
        ],
        "downgrade_triggers": [
            "重新跌回MA20下方或距MA60低于-3%",
            "近5日明显走弱且真实宽度继续收缩",
        ],
    }


class LeadershipCycleBuilder(V3LeadershipCycleBuilder):
    model_id = MODEL_ID
    model_version = MODEL_VERSION

    def _load_spec(self) -> dict[str, Any]:
        return load_leadership_cycle_spec()

    def _spec_hash(self) -> str:
        return leadership_cycle_spec_hash()

    def _compute_price_metrics(
        self,
        price_series: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return compute_price_metrics(
            price_series,
            minimum_days=int(self.spec["minimum_price_history_days"]),
        )

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

    def _extra_data_quality(
        self,
        price_metrics: Mapping[str, Any],
        breadth: Mapping[str, Any],
        *,
        through: date,
        technical_date: Any,
    ) -> dict[str, Any]:
        return {
            **super()._extra_data_quality(
                price_metrics,
                breadth,
                through=through,
                technical_date=technical_date,
            ),
            "startup_confirmation_rule": "multi_horizon_5_10_20_v1",
            "startup_stability_window_days": 10,
            "startup_return_windows_are_exact_intervals": True,
            "breadth_confirmation_required_for_startup": True,
        }
