from __future__ import annotations

from datetime import date
from typing import Any, Iterable


MODEL_ID = "huatai_multidim_v19"
MODEL_NAME = "多维市场择时 V1.9"
MODEL_VERSION = "v1.9"

DAILY_WEIGHTS: dict[str, float] = {
    "index_bollinger": 0.08,
    "multi_index_trend": 0.12,
    "index_pe_percentile": 0.10,
    "erp": 0.10,
    "margin_buy_ratio": 0.15,
    "option_pcr": 0.06,
    "qvix_volatility": 0.06,
    "iv_skew": 0.06,
    "futures_holding_net": 0.07,
    "up_down_amount_pressure": 0.20,
}

REALTIME_WEIGHTS: dict[str, float] = {
    "index_bollinger": 0.06,
    "multi_index_trend": 0.10,
    "index_pe_percentile": 0.09,
    "erp": 0.09,
    "margin_buy_ratio": 0.12,
    "option_pcr": 0.05,
    "qvix_volatility": 0.05,
    "iv_skew": 0.05,
    "futures_holding_net": 0.05,
    "up_down_amount_pressure": 0.08,
    "intraday_market_strength": 0.10,
    "intraday_breadth": 0.10,
    "intraday_limit_emotion": 0.06,
}

INDICATOR_DIMENSIONS: dict[str, str] = {
    "index_bollinger": "technical",
    "multi_index_trend": "technical",
    "index_pe_percentile": "valuation",
    "erp": "valuation",
    "margin_buy_ratio": "capital",
    "option_pcr": "derivatives",
    "qvix_volatility": "derivatives",
    "iv_skew": "derivatives",
    "futures_holding_net": "derivatives",
    "up_down_amount_pressure": "breadth",
    "intraday_market_strength": "breadth",
    "intraday_breadth": "breadth",
    "intraday_limit_emotion": "breadth",
}

DIMENSION_LABELS = {
    "technical": "趋势",
    "valuation": "估值",
    "capital": "资金",
    "derivatives": "衍生品情绪",
    "breadth": "市场宽度",
}

STATE_LABELS = {
    "strong_risk_on": "积极进攻",
    "risk_on": "正常开仓",
    "cautious": "谨慎试探",
    "defensive": "防守观望",
}

POSITION_UPPER = {
    "strong_risk_on": 1.0,
    "risk_on": 0.8,
    "cautious": 0.45,
    "defensive": 0.15,
}

ACTION_LABELS = {
    "strong_risk_on": "趋势、资金与市场宽度共振，可按仓位上限执行并保留个股风控",
    "risk_on": "选股可正常执行，回测可按标准仓位观察",
    "cautious": "可小仓验证，等待市场扩散或回踩确认",
    "defensive": "不建议新增重仓，选股结果以观察为主",
}


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def score_signal(score: float | None, *, bullish: float = 65.0, bearish: float = 40.0) -> int:
    if score is None:
        return 0
    if score >= bullish:
        return 1
    if score <= bearish:
        return -1
    return 0


def signal_label(signal: int) -> str:
    return {1: "偏多", 0: "中性", -1: "偏空"}.get(signal, "未知")


def percentile_rank(values: Iterable[float], value: float | None) -> float | None:
    clean = sorted(float(item) for item in values if item is not None)
    if value is None or not clean:
        return None
    below = sum(item < value for item in clean)
    equal = sum(item == value for item in clean)
    return (below + equal * 0.5) / len(clean)


def _source_age_days(source_trade_date: str | None, target_trade_date: str | None) -> int | None:
    if not source_trade_date or not target_trade_date:
        return None
    try:
        return max(0, (date.fromisoformat(target_trade_date) - date.fromisoformat(source_trade_date)).days)
    except ValueError:
        return None


def freshness_multiplier(
    *,
    source_status: str | None,
    source_trade_date: str | None,
    target_trade_date: str | None,
) -> float:
    age_days = _source_age_days(source_trade_date, target_trade_date)
    if age_days is None:
        return 0.70 if source_status == "沿用最近收盘" else 1.0
    if age_days <= 0:
        return 1.0
    if age_days == 1:
        return 0.70
    if age_days <= 3:
        return 0.40
    return 0.0


def calibrate_indicator_score(
    indicator_id: str,
    raw_score: float | None,
    raw_value: float | None,
    *,
    history_values: Iterable[float] = (),
    source_status: str | None = None,
    source_trade_date: str | None = None,
    target_trade_date: str | None = None,
) -> tuple[float | None, dict[str, Any]]:
    if raw_score is None:
        return None, {
            "calibration_method": "missing",
            "freshness_multiplier": 0.0,
        }

    calibrated = clamp(float(raw_score))
    history = [float(item) for item in history_values if item is not None]
    details: dict[str, Any] = {
        "raw_score": round(float(raw_score), 4),
        "history_count": len(history),
        "calibration_method": "identity",
    }

    if indicator_id in {"iv_skew", "futures_holding_net"} and raw_value is not None:
        if len(history) >= 10:
            rank = percentile_rank(history[-120:], float(raw_value))
            if rank is not None:
                rank_score = 100.0 * (1.0 - rank) if indicator_id == "iv_skew" else 100.0 * rank
                calibration_confidence = min(1.0, len(history) / 40.0)
                calibrated = 50.0 + (rank_score - 50.0) * 0.75 * calibration_confidence
                details.update(
                    {
                        "calibration_method": "rolling_percentile",
                        "percentile_rank": round(rank, 6),
                        "rank_score": round(rank_score, 4),
                        "calibration_confidence": round(calibration_confidence, 4),
                    }
                )
        else:
            calibrated = 50.0 + (calibrated - 50.0) * 0.25
            details.update(
                {
                    "calibration_method": "insufficient_history_shrink",
                    "calibration_confidence": round(len(history) / 40.0, 4),
                }
            )
    elif indicator_id == "qvix_volatility":
        # QVIX and IV skew represent the same broad volatility-risk family.
        # Keep QVIX informative without letting a moderate percentile become
        # an independent hard veto beside IV skew.
        calibrated = 50.0 + (calibrated - 50.0) * 0.65
        details["calibration_method"] = "correlation_shrink"

    freshness = freshness_multiplier(
        source_status=source_status,
        source_trade_date=source_trade_date,
        target_trade_date=target_trade_date,
    )
    calibrated = 50.0 + (calibrated - 50.0) * freshness
    details.update(
        {
            "freshness_multiplier": round(freshness, 4),
            "source_trade_date": source_trade_date,
            "target_trade_date": target_trade_date,
            "effective_score": round(clamp(calibrated), 4),
        }
    )
    return clamp(calibrated), details


def compose_timing_state(
    indicators: list[dict[str, Any]],
    *,
    weights: dict[str, float],
) -> dict[str, Any]:
    valid: list[tuple[dict[str, Any], float, float]] = []
    for item in indicators:
        indicator_id = str(item.get("indicator_id") or "")
        weight = float(weights.get(indicator_id, 0.0))
        score = item.get("score")
        if weight <= 0 or score is None:
            continue
        valid.append((item, float(score), weight))

    total_weight = sum(weight for _, _, weight in valid)
    configured_weight = sum(weights.values())
    timing_score = (
        sum(score * weight for _, score, weight in valid) / total_weight
        if total_weight
        else 50.0
    )

    dimension_numerator: dict[str, float] = {}
    dimension_denominator: dict[str, float] = {}
    for item, score, weight in valid:
        indicator_id = str(item.get("indicator_id") or "")
        dimension = INDICATOR_DIMENSIONS.get(indicator_id)
        if not dimension:
            continue
        dimension_numerator[dimension] = dimension_numerator.get(dimension, 0.0) + score * weight
        dimension_denominator[dimension] = dimension_denominator.get(dimension, 0.0) + weight

    dimension_scores = {
        dimension: dimension_numerator[dimension] / denominator
        for dimension, denominator in dimension_denominator.items()
        if denominator > 0
    }
    dimension_signals = {
        dimension: score_signal(score, bullish=60.0, bearish=40.0)
        for dimension, score in dimension_scores.items()
    }
    vote_sum = sum(dimension_signals.values())
    technical_score = dimension_scores.get("technical")
    breadth_score = dimension_scores.get("breadth")
    market_confirmation = any(
        score is not None and score >= 60.0
        for score in (technical_score, breadth_score)
    )
    market_weak = any(
        score is not None and score <= 40.0
        for score in (technical_score, breadth_score)
    )
    has_extreme_bearish_dimension = any(score <= 30.0 for score in dimension_scores.values())

    if (
        timing_score >= 65.0
        and vote_sum >= 3
        and technical_score is not None
        and technical_score >= 60.0
        and breadth_score is not None
        and breadth_score >= 60.0
        and not has_extreme_bearish_dimension
    ):
        state = "strong_risk_on"
    elif timing_score >= 55.0 and vote_sum >= 2 and market_confirmation:
        state = "risk_on"
    elif timing_score <= 42.0 or (vote_sum <= -2 and market_weak):
        state = "defensive"
    else:
        state = "cautious"

    dimensions = [
        {
            "dimension": dimension,
            "dimension_label": DIMENSION_LABELS.get(dimension, dimension),
            "score": round(score, 2),
            "signal": dimension_signals.get(dimension, 0),
            "signal_label": signal_label(dimension_signals.get(dimension, 0)),
        }
        for dimension, score in dimension_scores.items()
    ]

    return {
        "timing_score": round(clamp(timing_score), 2),
        "state": state,
        "state_label": STATE_LABELS[state],
        "position_upper": POSITION_UPPER[state],
        "position_upper_pct": round(POSITION_UPPER[state] * 100),
        "combined_signal": 1 if state in {"strong_risk_on", "risk_on"} else -1 if state == "defensive" else 0,
        "action_label": ACTION_LABELS[state],
        "dimension_vote_sum": vote_sum,
        "dimension_scores": {key: round(value, 2) for key, value in dimension_scores.items()},
        "dimension_signals": dimension_signals,
        "dimensions": dimensions,
        "confidence": round(min(1.0, total_weight / configured_weight), 4) if configured_weight else 0.0,
    }
