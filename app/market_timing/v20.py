from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from app.shared.db import mysql_conn, mysql_read_conn


SPEC_PATH = Path(__file__).resolve().parent / "specs" / "market_timing_v20.json"
MODEL_ID = "market_timing_v20"
MODEL_NAME = "市场择时 V2.0"
MODEL_VERSION = "v2.0"
INDEX_CODE = "000300.SH"

DIMENSION_LABELS = {
    "trend": "趋势",
    "breadth": "市场宽度",
    "capital": "资金量",
    "tail_risk": "尾部风险",
    "leadership": "主线结构",
}

DIMENSION_WEIGHTS = {
    "trend": 0.25,
    "breadth": 0.25,
    "capital": 0.20,
    "tail_risk": 0.15,
    "leadership": 0.15,
}

STATE_ORDER = (
    "cash",
    "defensive",
    "cautious",
    "neutral",
    "risk_on",
    "strong_risk_on",
)

STATE_LABELS = {
    "cash": "空仓防守",
    "defensive": "低仓防守",
    "cautious": "谨慎试探",
    "neutral": "中性均衡",
    "risk_on": "积极参与",
    "strong_risk_on": "强势进攻",
}

POSITION_RANGES = {
    "cash": (0.00, 0.10),
    "defensive": (0.10, 0.25),
    "cautious": (0.25, 0.45),
    "neutral": (0.45, 0.65),
    "risk_on": (0.65, 0.85),
    "strong_risk_on": (0.85, 1.00),
}

ACTION_LABELS = {
    "cash": "暂停新增仓位，以现金和风险处置为主",
    "defensive": "仅保留高确定性小仓位，反弹优先降风险",
    "cautious": "可轻仓试错，必须等待个股触发点和止损位",
    "neutral": "分批参与，不追高，保留至少三成机动现金",
    "risk_on": "可按计划提高仓位，但仍受行业集中度和海龟风控约束",
    "strong_risk_on": "趋势与扩散共振，可积极参与并用移动止损保护利润",
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


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


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _mean(values: Sequence[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def load_market_timing_v20_spec() -> dict[str, Any]:
    with SPEC_PATH.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    if spec.get("model_id") != MODEL_ID:
        raise RuntimeError("market timing V2.0 spec model_id mismatch")
    return spec


def market_timing_v20_spec_hash() -> str:
    return _sha256(load_market_timing_v20_spec())


def state_from_score(score: float) -> str:
    if score < 20:
        return "cash"
    if score < 35:
        return "defensive"
    if score < 48:
        return "cautious"
    if score < 60:
        return "neutral"
    if score < 75:
        return "risk_on"
    return "strong_risk_on"


def _signal_from_score(score: float) -> int:
    return 1 if score >= 60 else -1 if score < 35 else 0


def _target_for_state(state: str) -> float:
    low, high = POSITION_RANGES[state]
    return round((low + high) / 2, 4)


def compose_market_timing_v20(
    dimensions: Mapping[str, Mapping[str, Any]],
    *,
    overlay_points: float = 0.0,
    previous_signal: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compose fixed-weight dimensions without missing-weight redistribution."""

    normalized: dict[str, dict[str, Any]] = {}
    weighted_score = 0.0
    available_weight = 0.0
    for dimension, weight in DIMENSION_WEIGHTS.items():
        raw = dict(dimensions.get(dimension) or {})
        raw_score = _to_float(raw.get("score"))
        available = raw_score is not None
        score = clamp(raw_score if raw_score is not None else 50.0)
        inputs = raw.get("inputs") if isinstance(raw.get("inputs"), Mapping) else {}
        available_components = _to_float(inputs.get("available_components"))
        expected_components = _to_float(inputs.get("expected_components"))
        source_coverage = (
            max(0.0, min(1.0, available_components / expected_components))
            if (
                available
                and available_components is not None
                and expected_components is not None
                and expected_components > 0
            )
            else 1.0 if available else 0.0
        )
        normalized[dimension] = {
            **raw,
            "dimension": dimension,
            "dimension_label": DIMENSION_LABELS[dimension],
            "score": round(score, 4),
            "available": available,
            "source_coverage": round(source_coverage, 4),
            "weight": weight,
            "signal": _signal_from_score(score),
        }
        weighted_score += score * weight
        if available:
            available_weight += weight * source_coverage

    bounded_overlay = max(-10.0, min(10.0, float(overlay_points)))
    raw_score = clamp(weighted_score + bounded_overlay)
    raw_state = state_from_score(raw_score)
    emergency_reasons: list[str] = []
    tail_score = normalized["tail_risk"]["score"]
    breadth_score = normalized["breadth"]["score"]
    trend_score = normalized["trend"]["score"]
    if tail_score <= 20:
        emergency_reasons.append("tail_risk_at_or_below_20")
    if breadth_score <= 25:
        emergency_reasons.append("breadth_at_or_below_25")
    if breadth_score <= 35 and trend_score <= 35:
        emergency_reasons.append("trend_and_breadth_joint_breakdown")
    emergency = bool(emergency_reasons)
    if emergency:
        raw_state = "cash" if tail_score <= 15 or breadth_score <= 18 else "defensive"

    previous_signal = dict(previous_signal or {})
    previous_state = str(previous_signal.get("state") or raw_state)
    if previous_state not in STATE_ORDER:
        previous_state = raw_state
    previous_index = STATE_ORDER.index(previous_state)
    raw_index = STATE_ORDER.index(raw_state)
    previous_details = _json_value(
        previous_signal.get("coverage_json")
        or previous_signal.get("details")
        or {},
        {},
    )
    prior_candidate = str(previous_details.get("upgrade_candidate_state") or "")
    prior_streak = int(previous_details.get("upgrade_streak") or 0)

    upgrade_candidate_state = None
    upgrade_streak = 0
    hysteresis_action = "hold"
    if emergency or raw_index < previous_index:
        effective_state = raw_state
        hysteresis_action = "emergency_downgrade" if emergency else "same_day_downgrade"
    elif raw_index > previous_index:
        upgrade_candidate_state = raw_state
        upgrade_streak = prior_streak + 1 if prior_candidate == raw_state else 1
        if upgrade_streak >= 2:
            effective_state = STATE_ORDER[min(previous_index + 1, raw_index)]
            hysteresis_action = "confirmed_upgrade_one_step"
            if effective_state != raw_state:
                upgrade_streak = 1
                upgrade_candidate_state = raw_state
            else:
                upgrade_streak = 0
                upgrade_candidate_state = None
        else:
            effective_state = previous_state
            hysteresis_action = "upgrade_waiting_confirmation"
    else:
        effective_state = raw_state

    position_low, position_high = POSITION_RANGES[effective_state]
    target = _target_for_state(effective_state)
    previous_target = _to_float(
        previous_details.get("position_target")
        or previous_signal.get("position_target")
    )
    if (
        previous_target is not None
        and target > previous_target
        and not emergency
    ):
        target = min(target, previous_target + 0.20)
        target = max(position_low, min(position_high, target))
    confidence = round(available_weight / sum(DIMENSION_WEIGHTS.values()), 4)

    reasons = [
        f"{DIMENSION_LABELS[key]} {item['score']:.1f}分"
        + ("" if item["available"] else "（缺失按中性）")
        for key, item in normalized.items()
    ]
    if bounded_overlay:
        reasons.append(f"日内覆盖层 {bounded_overlay:+.1f}分")
    if hysteresis_action == "upgrade_waiting_confirmation":
        reasons.append(f"{STATE_LABELS[raw_state]}需连续两日确认，暂维持{STATE_LABELS[effective_state]}")
    risk_notes = []
    if confidence < 1:
        risk_notes.append(
            f"核心维度覆盖 {confidence * 100:.0f}%，缺失维度按50分且不重分权重"
        )
    if emergency_reasons:
        risk_notes.append("紧急降仓触发：" + " / ".join(emergency_reasons))

    return {
        "model_id": MODEL_ID,
        "model_name": MODEL_NAME,
        "version": MODEL_VERSION,
        "spec_hash": market_timing_v20_spec_hash(),
        "timing_score": round(raw_score, 2),
        "raw_state": raw_state,
        "state": effective_state,
        "state_label": STATE_LABELS[effective_state],
        "position_range": {
            "low": round(position_low, 4),
            "high": round(position_high, 4),
            "low_pct": round(position_low * 100),
            "high_pct": round(position_high * 100),
        },
        "position_target": round(target, 4),
        "position_target_pct": round(target * 100),
        "position_upper": round(position_high, 4),
        "position_upper_pct": round(position_high * 100),
        "combined_signal": (
            1
            if effective_state in {"risk_on", "strong_risk_on"}
            else -1
            if effective_state in {"cash", "defensive"}
            else 0
        ),
        "confidence": confidence,
        "overlay_points": round(bounded_overlay, 2),
        "dimensions": normalized,
        "reasons": reasons,
        "risk_notes": risk_notes,
        "action_label": ACTION_LABELS[effective_state],
        "emergency": emergency,
        "emergency_reasons": emergency_reasons,
        "hysteresis_action": hysteresis_action,
        "upgrade_candidate_state": upgrade_candidate_state,
        "upgrade_streak": upgrade_streak,
        "research_only": True,
    }


class MarketTimingV20Repository:
    def __init__(
        self,
        *,
        connection_factory=None,
        read_connection_factory=None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        self._read_connection_factory = read_connection_factory or mysql_read_conn

    def _previous_market_date(self, trade_date: date | str) -> date | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(trade_date) AS trade_date
                    FROM daily_kline
                    WHERE trade_date < %s
                    """,
                    (trade_date,),
                )
                return (cursor.fetchone() or {}).get("trade_date")

    def _market_breadth(self, trade_date: date | str) -> dict[str, Any]:
        previous_date = self._previous_market_date(trade_date)
        if previous_date is None:
            return {"score": None, "source_status": "missing_previous_trade_date"}
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS sample_size,
                        AVG(CASE WHEN d.close>p.close THEN 1.0 ELSE 0.0 END) AS up_ratio,
                        AVG((d.close/p.close-1)*100) AS equal_weight_return_pct,
                        SUM(CASE WHEN d.close>p.close THEN COALESCE(d.amount,0) ELSE 0 END) AS up_amount,
                        SUM(CASE WHEN d.close<p.close THEN COALESCE(d.amount,0) ELSE 0 END) AS down_amount,
                        SUM(CASE WHEN d.close/p.close-1>=0.095 THEN 1 ELSE 0 END) AS limit_up_like,
                        SUM(CASE WHEN d.close/p.close-1<=-0.095 THEN 1 ELSE 0 END) AS limit_down_like
                    FROM daily_kline d
                    INNER JOIN daily_kline p
                        ON p.code=d.code AND p.trade_date=%s
                    INNER JOIN stock_instrument_lifecycle l
                        ON l.code=d.code
                       AND l.instrument_type='stock'
                       AND l.listing_date<=d.trade_date
                       AND (l.delisting_date IS NULL OR l.delisting_date>=d.trade_date)
                    WHERE d.trade_date=%s
                      AND d.close>0 AND p.close>0
                      AND NOT EXISTS (
                          SELECT 1
                          FROM stock_name_history n
                          WHERE n.code=d.code
                            AND n.start_date<=d.trade_date
                            AND (n.end_date IS NULL OR n.end_date>=d.trade_date)
                            AND (n.is_st=1 OR n.is_delisting_period=1)
                      )
                    """,
                    (previous_date, trade_date),
                )
                row = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT AVG(
                        CASE WHEN latest_close>ma20 THEN 1.0 ELSE 0.0 END
                    ) AS above_ma20_ratio,
                    COUNT(*) AS feature_sample_size
                    FROM stock_technical_feature_daily
                    WHERE trade_date=%s
                      AND source_trade_date=%s
                      AND latest_close IS NOT NULL
                      AND ma20 IS NOT NULL
                    """,
                    (trade_date, trade_date),
                )
                technical = cursor.fetchone() or {}
        up_ratio = _to_float(row.get("up_ratio"))
        equal_return = _to_float(row.get("equal_weight_return_pct"))
        up_amount = _to_float(row.get("up_amount")) or 0.0
        down_amount = _to_float(row.get("down_amount")) or 0.0
        amount_total = up_amount + down_amount
        amount_pressure = (
            (up_amount - down_amount) / amount_total if amount_total > 0 else None
        )
        above_ma20 = _to_float(technical.get("above_ma20_ratio"))
        components = [
            None if up_ratio is None else clamp(up_ratio * 100),
            None if amount_pressure is None else clamp(50 + amount_pressure * 55),
            None if above_ma20 is None else clamp(above_ma20 * 100),
            None if equal_return is None else clamp(50 + equal_return * 8),
        ]
        score = _mean(components)
        return {
            "score": score,
            "source_status": "ready" if score is not None else "missing",
            "source": "daily_kline+stock_technical_feature_daily+PIT_universe",
            "inputs": {
                "sample_size": int(row.get("sample_size") or 0),
                "up_ratio": up_ratio,
                "amount_pressure": amount_pressure,
                "above_ma20_ratio": above_ma20,
                "equal_weight_return_pct": equal_return,
                "limit_up_like": int(row.get("limit_up_like") or 0),
                "limit_down_like": int(row.get("limit_down_like") or 0),
                "previous_trade_date": previous_date,
                "available_components": sum(value is not None for value in components),
                "expected_components": len(components),
            },
        }

    def _trend(self, trade_date: date | str) -> dict[str, Any]:
        index_codes = ("000300.SH", "000852.SH", "000688.SH")
        rows_by_code: dict[str, list[Mapping[str, Any]]] = {}
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                for code in index_codes:
                    cursor.execute(
                        """
                        SELECT trade_date, close
                        FROM market_index_daily
                        WHERE index_code=%s AND trade_date<=%s
                          AND close IS NOT NULL
                        ORDER BY trade_date DESC
                        LIMIT 60
                        """,
                        (code, trade_date),
                    )
                    rows_by_code[code] = list(reversed(cursor.fetchall() or []))
                cursor.execute(
                    """
                    SELECT indicator_id, score, source_status
                    FROM market_timing_indicator_daily
                    WHERE trade_date=%s
                      AND index_code=%s
                      AND model_id='huatai_multidim_v19'
                      AND indicator_id IN ('index_bollinger','multi_index_trend')
                    """,
                    (trade_date, INDEX_CODE),
                )
                v19 = cursor.fetchall() or []
        components: list[float | None] = []
        index_details: list[dict[str, Any]] = []
        for code, rows in rows_by_code.items():
            closes = [_to_float(row.get("close")) for row in rows]
            closes = [value for value in closes if value is not None]
            if len(closes) < 20:
                index_details.append({"index_code": code, "status": "insufficient_history"})
                continue
            current = closes[-1]
            ma20 = sum(closes[-20:]) / 20
            ma60 = sum(closes[-60:]) / len(closes[-60:])
            return20 = (current / closes[-20] - 1) * 100
            score = clamp(
                50
                + (8 if current > ma20 else -8)
                + (8 if current > ma60 else -8)
                + max(-18, min(18, return20 * 2.2))
            )
            components.append(score)
            index_details.append(
                {
                    "index_code": code,
                    "close": current,
                    "ma20": ma20,
                    "ma60": ma60,
                    "return20_pct": return20,
                    "score": score,
                }
            )
        components.extend(_to_float(row.get("score")) for row in v19)
        score = _mean(components)
        return {
            "score": score,
            "source_status": "ready" if score is not None else "missing",
            "source": "market_index_daily+market_timing_v19",
            "inputs": {
                "indices": index_details,
                "v19_components": [dict(row) for row in v19],
                "available_components": sum(value is not None for value in components),
                "expected_components": len(index_codes) + 2,
            },
        }

    def _capital(self, trade_date: date | str) -> dict[str, Any]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT SUM(amount) AS total_amount
                    FROM daily_kline d
                    INNER JOIN stock_instrument_lifecycle l
                        ON l.code=d.code
                       AND l.instrument_type='stock'
                       AND l.listing_date<=d.trade_date
                       AND (l.delisting_date IS NULL OR l.delisting_date>=d.trade_date)
                    WHERE d.trade_date=%s
                    """,
                    (trade_date,),
                )
                total_amount = _to_float((cursor.fetchone() or {}).get("total_amount"))
                cursor.execute(
                    """
                    SELECT d.trade_date, SUM(d.amount) AS total_amount
                    FROM daily_kline d
                    INNER JOIN (
                        SELECT DISTINCT trade_date
                        FROM daily_kline
                        WHERE trade_date<=%s
                        ORDER BY trade_date DESC
                        LIMIT 20
                    ) recent_dates
                        ON recent_dates.trade_date=d.trade_date
                    INNER JOIN stock_instrument_lifecycle l
                        ON l.code=d.code
                       AND l.instrument_type='stock'
                       AND l.listing_date<=d.trade_date
                       AND (l.delisting_date IS NULL OR l.delisting_date>=d.trade_date)
                    GROUP BY d.trade_date
                    ORDER BY d.trade_date DESC
                    """,
                    (trade_date,),
                )
                amount_history = [
                    _to_float(row.get("total_amount"))
                    for row in (cursor.fetchall() or [])
                ]
                cursor.execute(
                    """
                    SELECT indicator_id, score, value, source_status
                    FROM market_timing_indicator_daily
                    WHERE trade_date=%s
                      AND index_code=%s
                      AND model_id='huatai_multidim_v19'
                      AND indicator_id='margin_buy_ratio'
                    """,
                    (trade_date, INDEX_CODE),
                )
                margin = cursor.fetchone()
        amount_history = [value for value in amount_history if value is not None]
        amount_score = None
        amount_ratio = None
        if total_amount is not None and amount_history:
            average = sum(amount_history) / len(amount_history)
            if average > 0:
                amount_ratio = total_amount / average
                amount_score = clamp(50 + (amount_ratio - 1) * 55)
        margin_score = _to_float(margin.get("score")) if margin else None
        score = _mean([amount_score, margin_score])
        return {
            "score": score,
            "source_status": "ready" if score is not None else "missing",
            "source": "daily_kline+market_margin_daily",
            "inputs": {
                "total_amount": total_amount,
                "amount_ratio_20d": amount_ratio,
                "amount_score": amount_score,
                "margin_score": margin_score,
                "available_components": sum(
                    value is not None for value in (amount_score, margin_score)
                ),
                "expected_components": 2,
            },
        }

    def _tail_risk(
        self,
        trade_date: date | str,
        breadth: Mapping[str, Any],
    ) -> dict[str, Any]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT indicator_id, score, source_status
                    FROM market_timing_indicator_daily
                    WHERE trade_date=%s
                      AND index_code=%s
                      AND model_id='huatai_multidim_v19'
                      AND indicator_id IN (
                          'option_pcr','qvix_volatility','iv_skew',
                          'futures_holding_net'
                      )
                    """,
                    (trade_date, INDEX_CODE),
                )
                rows = cursor.fetchall() or []
        components = [_to_float(row.get("score")) for row in rows]
        breadth_inputs = breadth.get("inputs") or {}
        limit_up = int(breadth_inputs.get("limit_up_like") or 0)
        limit_down = int(breadth_inputs.get("limit_down_like") or 0)
        limit_total = limit_up + limit_down
        limit_score = (
            clamp(50 + (limit_up - limit_down) / limit_total * 45)
            if limit_total
            else None
        )
        components.append(limit_score)
        score = _mean(components)
        return {
            "score": score,
            "source_status": "ready" if score is not None else "missing",
            "source": "market_derivatives+PIT_limit_structure",
            "inputs": {
                "derivative_components": [dict(row) for row in rows],
                "limit_up_like": limit_up,
                "limit_down_like": limit_down,
                "limit_structure_score": limit_score,
                "available_components": sum(value is not None for value in components),
                "expected_components": 5,
            },
        }

    def _leadership(self, trade_date: date | str) -> dict[str, Any]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, sector_score,
                           weighted_impact_score, source_count, stock_count
                    FROM sector_opinion_daily
                    WHERE trade_date=%s
                    ORDER BY sector_score DESC, source_count DESC
                    LIMIT 20
                    """,
                    (trade_date,),
                )
                rows = cursor.fetchall() or []
        scores = [_to_float(row.get("sector_score")) for row in rows]
        scores = [value for value in scores if value is not None]
        if not scores:
            return {
                "score": None,
                "source_status": "missing",
                "source": "sector_opinion_daily",
                "inputs": {"strong_sector_count": 0, "top_sectors": []},
            }
        top5 = scores[:5]
        strong_count = sum(value >= 65 for value in scores)
        source_breadth = _mean(
            [
                min(100.0, 35 + int(row.get("source_count") or 0) * 8)
                for row in rows[:5]
            ]
        )
        leadership_score = clamp(
            (_mean(top5) or 50) * 0.65
            + min(100.0, 35 + strong_count * 7) * 0.20
            + (source_breadth or 50) * 0.15
        )
        return {
            "score": leadership_score,
            "source_status": "ready",
            "source": "sector_opinion_daily",
            "inputs": {
                "strong_sector_count": strong_count,
                "top5_average_score": _mean(top5),
                "source_breadth_score": source_breadth,
                "top_sectors": [
                    {
                        "sector_type": row.get("sector_type"),
                        "sector_name": row.get("sector_name"),
                        "sector_score": _to_float(row.get("sector_score")),
                        "source_count": int(row.get("source_count") or 0),
                    }
                    for row in rows[:5]
                ],
            },
        }

    def build_dimensions(self, trade_date: date | str) -> dict[str, dict[str, Any]]:
        breadth = self._market_breadth(trade_date)
        return {
            "trend": self._trend(trade_date),
            "breadth": breadth,
            "capital": self._capital(trade_date),
            "tail_risk": self._tail_risk(trade_date, breadth),
            "leadership": self._leadership(trade_date),
        }

    def previous_signal(
        self,
        trade_date: date | str,
        *,
        index_code: str = INDEX_CODE,
    ) -> dict[str, Any] | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM market_timing_signal_daily
                    WHERE model_id=%s
                      AND index_code=%s
                      AND trade_date<%s
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (MODEL_ID, index_code, trade_date),
                )
                row = cursor.fetchone()
        if not row:
            return None
        result = dict(row)
        result["coverage_json"] = _json_value(result.get("coverage_json"), {})
        return result

    def materialize(
        self,
        trade_date: date | str,
        *,
        index_code: str = INDEX_CODE,
        overlay_points: float = 0.0,
    ) -> dict[str, Any]:
        dimensions = self.build_dimensions(trade_date)
        previous = self.previous_signal(trade_date, index_code=index_code)
        result = compose_market_timing_v20(
            dimensions,
            overlay_points=overlay_points,
            previous_signal=previous,
        )
        result["trade_date"] = str(trade_date)
        result["index_code"] = index_code
        result["as_of"] = str(trade_date)
        result["source"] = "PIT_market_data+market_timing_v19_shadow_inputs"
        details = {
            "spec_hash": result["spec_hash"],
            "raw_state": result["raw_state"],
            "position_range": result["position_range"],
            "position_target": result["position_target"],
            "position_target_pct": result["position_target_pct"],
            "overlay_points": result["overlay_points"],
            "emergency": result["emergency"],
            "emergency_reasons": result["emergency_reasons"],
            "hysteresis_action": result["hysteresis_action"],
            "upgrade_candidate_state": result["upgrade_candidate_state"],
            "upgrade_streak": result["upgrade_streak"],
            "dimensions": result["dimensions"],
            "missing_weight_redistributed": False,
            "research_only": True,
        }
        indicator_rows = []
        for dimension, item in result["dimensions"].items():
            indicator_rows.append(
                (
                    trade_date,
                    index_code,
                    MODEL_ID,
                    MODEL_VERSION,
                    dimension,
                    f"v20_{dimension}",
                    DIMENSION_LABELS[dimension],
                    item["score"],
                    f"{item['score']:.1f}",
                    item["score"],
                    item["signal"],
                    {1: "偏多", 0: "中性", -1: "偏空"}[item["signal"]],
                    item.get("source_status") or "missing",
                    item.get("source"),
                    _canonical_json(
                        {
                            "spec_hash": result["spec_hash"],
                            "available": item["available"],
                            "weight": item["weight"],
                            "inputs": item.get("inputs") or {},
                        }
                    ),
                )
            )
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO market_timing_indicator_daily (
                        trade_date, index_code, model_id, version, dimension,
                        indicator_id, indicator_name, value, value_label,
                        score, signal_value, signal_label, source_status,
                        source, metadata_json
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s
                    )
                    ON DUPLICATE KEY UPDATE
                        version=VALUES(version),
                        dimension=VALUES(dimension),
                        indicator_name=VALUES(indicator_name),
                        value=VALUES(value),
                        value_label=VALUES(value_label),
                        score=VALUES(score),
                        signal_value=VALUES(signal_value),
                        signal_label=VALUES(signal_label),
                        source_status=VALUES(source_status),
                        source=VALUES(source),
                        metadata_json=VALUES(metadata_json)
                    """,
                    indicator_rows,
                )
                cursor.execute(
                    """
                    INSERT INTO market_timing_signal_daily (
                        trade_date, index_code, model_id, model_name, version,
                        combined_signal, timing_score, state, state_label,
                        position_upper, confidence, reasons_json,
                        risk_notes_json, coverage_json, source
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s
                    )
                    ON DUPLICATE KEY UPDATE
                        model_name=VALUES(model_name),
                        version=VALUES(version),
                        combined_signal=VALUES(combined_signal),
                        timing_score=VALUES(timing_score),
                        state=VALUES(state),
                        state_label=VALUES(state_label),
                        position_upper=VALUES(position_upper),
                        confidence=VALUES(confidence),
                        reasons_json=VALUES(reasons_json),
                        risk_notes_json=VALUES(risk_notes_json),
                        coverage_json=VALUES(coverage_json),
                        source=VALUES(source)
                    """,
                    (
                        trade_date,
                        index_code,
                        MODEL_ID,
                        MODEL_NAME,
                        MODEL_VERSION,
                        result["combined_signal"],
                        result["timing_score"],
                        result["state"],
                        result["state_label"],
                        result["position_upper"],
                        result["confidence"],
                        _canonical_json(result["reasons"]),
                        _canonical_json(result["risk_notes"]),
                        _canonical_json(details),
                        result["source"],
                    ),
                )
        return result

    def latest(self, *, index_code: str = INDEX_CODE) -> dict[str, Any] | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM market_timing_signal_daily
                    WHERE model_id=%s AND index_code=%s
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (MODEL_ID, index_code),
                )
                row = cursor.fetchone()
        if not row:
            return None
        coverage = _json_value(row.get("coverage_json"), {})
        result = {
            "model_id": row.get("model_id"),
            "model_name": row.get("model_name"),
            "version": row.get("version"),
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "as_of": str(row.get("trade_date")) if row.get("trade_date") else None,
            "timing_score": _to_float(row.get("timing_score")),
            "state": row.get("state"),
            "state_label": row.get("state_label"),
            "position_upper": _to_float(row.get("position_upper")),
            "position_upper_pct": (
                round(float(row["position_upper"]) * 100)
                if row.get("position_upper") is not None
                else None
            ),
            "confidence": _to_float(row.get("confidence")),
            "combined_signal": int(row.get("combined_signal") or 0),
            "reasons": _json_value(row.get("reasons_json"), []),
            "risk_notes": _json_value(row.get("risk_notes_json"), []),
            "source": row.get("source"),
            "research_only": True,
            **coverage,
        }
        result["action_label"] = ACTION_LABELS.get(
            str(result.get("state")),
            "等待更多证据",
        )
        return result
