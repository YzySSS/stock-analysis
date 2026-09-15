from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from app.data_ingestion.market_opinion_events import (
    ACTIONABLE_CONTENT_ROLES,
    CONFIRMED_EVENT_STATES,
    CONFIRMED_RELATION_STATES,
    EVIDENCE_RULE_VERSION,
    RELATION_RULE_VERSION,
)
from app.stock_selection.base import BaseSelectionStrategy
from app.stock_selection.sentiment_v06_evaluation import (
    sentiment_v06_evaluation_spec_hash,
)
from app.stock_selection.sentiment_v06_intraday import INTRADAY_FEATURE_VERSION
from app.shared.market_clock import to_shanghai_wall_clock


FACTOR_SCHEMA_VERSION = "sentiment-v06-factors-v1"
ENTRY_RULE_VERSION = "sentiment-v06-entry-v1"
EVALUATION_METHOD_VERSION = "sentiment-v06-eval-v1"
EXECUTION_RULE_VERSION = "sentiment-v06-execution-v1"


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return round(max(low, min(float(value), high)), 4)


def _datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return to_shanghai_wall_clock(parsed)


def _text_datetime(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else None


def _latest_at_or_before(value: Any, decision: datetime | None) -> bool | None:
    parsed = _datetime(value)
    if parsed is None or decision is None:
        return None
    return parsed <= decision


def _age_seconds(value: Any, decision: datetime | None) -> float | None:
    parsed = _datetime(value)
    if parsed is None or decision is None or parsed > decision:
        return None
    return max((decision - parsed).total_seconds(), 0.0)


def _event_evidence(event: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in event.get("evidence") or [] if isinstance(row, Mapping)]


class AShareSentimentV06Strategy(BaseSelectionStrategy):
    """Point-in-time intraday sentiment research strategy.

    The opportunity score ranks evidence. ``entry_eligibility`` independently
    decides whether current, non-compensable conditions are met. This class is
    intentionally isolated from the frozen 0.4.4/0.5.1 implementation.
    """

    strategy_id = "a_share_sentiment_v06"
    FACTOR_KEYS = (
        "catalyst_quality",
        "persistence",
        "relation_recognition",
        "fund_confirmation",
        "price_volume_confirmation",
        "chip_liquidity_capacity",
    )
    RISK_ON_STATES = frozenset({"risk_on", "bull", "strong", "supportive"})
    CAUTIOUS_STATES = frozenset({"cautious", "neutral", "pressured"})

    @staticmethod
    def _events(item: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in (
                item.get("sentiment_v06_events")
                or item.get("canonical_events")
                or item.get("events")
                or []
            )
            if isinstance(row, Mapping)
        ]

    @staticmethod
    def _relations(item: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in (
                item.get("sentiment_v06_relations")
                or item.get("event_relations")
                or item.get("relations")
                or []
            )
            if isinstance(row, Mapping)
        ]

    @staticmethod
    def _market_regime(item: Mapping[str, Any]) -> str:
        raw = str(item.get("market_regime") or item.get("market_state") or "unknown").strip().lower()
        if raw in AShareSentimentV06Strategy.RISK_ON_STATES:
            return "risk_on"
        if raw in AShareSentimentV06Strategy.CAUTIOUS_STATES:
            return "cautious"
        return "defensive"

    @classmethod
    def _direct_event_ids(cls, item: Mapping[str, Any]) -> set[str]:
        explicit = {
            str(value)
            for value in item.get("direct_event_ids") or []
            if str(value)
        }
        confirmed_relation_events = {
            str(row.get("canonical_event_id") or "")
            for row in cls._relations(item)
            if row.get("relation_status") in CONFIRMED_RELATION_STATES
        }
        eligible = {
            str(event.get("canonical_event_id"))
            for event in cls._events(item)
            if event.get("canonical_event_id")
            and event.get("content_role") in ACTIONABLE_CONTENT_ROLES
            and event.get("direction") == "positive"
            and event.get("confirmation_status") in CONFIRMED_EVENT_STATES
            and str(event.get("canonical_event_id")) in confirmed_relation_events
        }
        return eligible & explicit if explicit else eligible

    @classmethod
    def _candidate_lanes(cls, item: Mapping[str, Any]) -> list[str]:
        explicit = [
            str(value).strip()
            for value in item.get("candidate_lanes") or []
            if str(value).strip() in {"direct_catalyst", "theme_leader"}
        ]
        lanes: list[str] = []
        for value in explicit:
            if value not in lanes:
                lanes.append(value)
        if not lanes and cls._direct_event_ids(item):
            lanes.append("direct_catalyst")
        if item.get("opinion_sector_name") and "theme_leader" not in lanes:
            lanes.append("theme_leader")
        return lanes

    def prepare_context(self, data_bundle: dict[str, Any]) -> dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        minimum_listed_days = int(filters.get("min_listed_days", 60))
        minimum_latest_amount = float(filters.get("min_latest_amount", 50_000_000))
        minimum_median_amount = float(filters.get("min_median_amount_20", 80_000_000))
        accepted: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        lane_counts = {"direct_catalyst": 0, "theme_leader": 0, "both": 0}

        for raw in data_bundle.get("candidates") or []:
            item = dict(raw)
            reasons: list[str] = []
            if bool(item.get("is_st")):
                reasons.append("st_stock")
            if item.get("lifecycle_known") is False:
                reasons.append("lifecycle_unknown")
            if bool(item.get("is_suspended") or item.get("suspended")):
                reasons.append("suspended")
            if bool(item.get("is_delisting")) or str(item.get("list_status") or "").upper() in {
                "D",
                "DELISTING",
            }:
                reasons.append("delisting")
            listed_days = _number(item.get("listed_trade_days"))
            if listed_days is None:
                reasons.append("listed_trade_days_unknown")
            elif listed_days < minimum_listed_days:
                reasons.append("new_listing")
            if item.get("required_data_complete") is False:
                reasons.append("required_data_incomplete")

            clock_mode = str(item.get("decision_clock_mode") or "").strip().lower()
            latest_amount = (
                _number(item.get("realtime_amount"))
                if clock_mode == "intraday"
                else _number(item.get("technical_latest_amount"))
            )
            if latest_amount is None and clock_mode != "intraday":
                latest_amount = _number(item.get("amount"))
            median_amount = _number(item.get("median_amount_20"))
            if latest_amount is None or latest_amount < minimum_latest_amount:
                reasons.append("latest_liquidity_below_floor")
            if median_amount is None or median_amount < minimum_median_amount:
                reasons.append("twenty_day_liquidity_below_floor")

            lanes = self._candidate_lanes(item)
            if not lanes:
                reasons.append("no_v06_candidate_lane")
            if reasons:
                rejections.append(
                    {
                        "code": item.get("code"),
                        "candidate_lanes": lanes,
                        "reasons": sorted(set(reasons)),
                    }
                )
                continue

            if len(lanes) > 1:
                lane_counts["both"] += 1
            for lane in lanes:
                lane_counts[lane] += 1
            direct_ids = self._direct_event_ids(item)
            adverse_ids = {
                str(value)
                for value in item.get("adverse_event_ids") or []
                if str(value)
            }
            accepted.append(
                {
                    **item,
                    "candidate_lanes": lanes,
                    "direct_event_ids": sorted(direct_ids),
                    "adverse_event_ids": sorted(adverse_ids),
                    "adverse_event_veto": bool(adverse_ids),
                    "market_regime": self._market_regime(item),
                    "hard_gate_pass": not bool(adverse_ids),
                    "hard_gate_reasons": ["high_confidence_adverse_event"] if adverse_ids else [],
                    "watch_gate_reasons": [],
                    "latest_liquidity_amount": latest_amount,
                    "factor_schema_version": FACTOR_SCHEMA_VERSION,
                    "evidence_rule_version": EVIDENCE_RULE_VERSION,
                    "relation_rule_version": RELATION_RULE_VERSION,
                    "entry_rule_version": ENTRY_RULE_VERSION,
                    "evaluation_method_version": EVALUATION_METHOD_VERSION,
                    "evaluation_spec_hash": sentiment_v06_evaluation_spec_hash(),
                    "execution_rule_version": EXECUTION_RULE_VERSION,
                }
            )

        return {
            **data_bundle,
            "candidates": accepted,
            "sentiment_v06_filter_summary": {
                "before": len(data_bundle.get("candidates") or []),
                "after": len(accepted),
                "rejected": len(rejections),
                "lane_counts": lane_counts,
                "rejections": rejections,
            },
        }

    @staticmethod
    def _event_impact(event: Mapping[str, Any]) -> float | None:
        values = [
            value
            for row in _event_evidence(event)
            if (value := _number(row.get("impact_score"))) is not None
        ]
        return max(values) if values else None

    @classmethod
    def _lane_events(cls, item: Mapping[str, Any], lane: str) -> list[dict[str, Any]]:
        events = cls._events(item)
        if lane == "direct_catalyst":
            direct_ids = cls._direct_event_ids(item)
            return [
                event
                for event in events
                if str(event.get("canonical_event_id") or "") in direct_ids
            ]
        theme_ids = {
            str(relation.get("canonical_event_id") or "")
            for relation in cls._relations(item)
            if relation.get("relation_type") == "theme_mapping"
        }
        return [
            event
            for event in events
            if str(event.get("canonical_event_id") or "") in theme_ids
            if event.get("content_role") in ACTIONABLE_CONTENT_ROLES
            and event.get("direction") == "positive"
            and int(event.get("available_evidence_count") or 0) > 0
        ]

    @classmethod
    def _catalyst_factors(
        cls,
        item: Mapping[str, Any],
        lane: str,
    ) -> tuple[float | None, float | None, dict[str, Any]]:
        events = cls._lane_events(item, lane)
        if not events:
            return None, None, {
                "status": "unknown",
                "event_count": 0,
                "reason": "no_actionable_canonical_event",
            }
        event_scores: list[float] = []
        persistence_values: list[float] = []
        for event in events:
            impact = cls._event_impact(event)
            confidence = _number(event.get("event_confidence"))
            if impact is None or confidence is None:
                continue
            score = impact * 0.58 + _clamp(confidence, 0.0, 1.0) * 100.0 * 0.42
            if event.get("confirmation_status") not in CONFIRMED_EVENT_STATES:
                score = min(score, 49.0)
            if lane == "theme_leader":
                # A theme event is not automatically a company-specific event.
                score = min(score, 72.0)
            event_scores.append(_clamp(score))
            persistence = _number(event.get("persistence_score"))
            if persistence is not None:
                persistence_values.append(_clamp(persistence))
        if not event_scores:
            return None, None, {
                "status": "unknown",
                "event_count": len(events),
                "reason": "event_importance_or_confidence_missing",
            }
        ordered = sorted(event_scores, reverse=True)
        catalyst = ordered[0]
        if len(ordered) > 1:
            catalyst = min(100.0, ordered[0] * 0.85 + ordered[1] * 0.15)
        persistence = max(persistence_values) if persistence_values else None
        return _clamp(catalyst), persistence, {
            "status": "known",
            "event_count": len(events),
            "canonical_event_ids": [
                str(event.get("canonical_event_id")) for event in events
            ],
            "aggregation": "best_event_plus_15pct_second_event_cap",
        }

    @classmethod
    def _relation_factor(
        cls,
        item: Mapping[str, Any],
        lane: str,
    ) -> tuple[float | None, dict[str, Any]]:
        relations = cls._relations(item)
        if lane == "direct_catalyst":
            direct_ids = cls._direct_event_ids(item)
            eligible = [
                relation
                for relation in relations
                if str(relation.get("canonical_event_id") or "") in direct_ids
                and relation.get("relation_status") in CONFIRMED_RELATION_STATES
            ]
        else:
            eligible = [
                relation
                for relation in relations
                if relation.get("relation_type") == "theme_mapping"
                or relation.get("relation_status") == "background_only"
            ]
        values = [
            value
            for relation in eligible
            if (value := _number(relation.get("relation_score"))) is not None
        ]
        if not values:
            return None, {"status": "unknown", "relation_count": len(eligible)}
        value = max(values)
        if lane == "theme_leader":
            value = min(value, 70.0)
        return _clamp(value), {
            "status": "known",
            "relation_count": len(eligible),
            "relation_ids": [str(row.get("relation_id")) for row in eligible if row.get("relation_id")],
        }

    @staticmethod
    def _theme_fund(item: Mapping[str, Any]) -> tuple[float | None, dict[str, Any]]:
        raw = item.get("market_theme_fund_flow")
        if not isinstance(raw, Mapping):
            return None, {"status": "unknown", "reason": "theme_fund_flow_missing"}
        raw_net = _number(raw.get("net_amount"))
        source_unit = str(raw.get("source_unit") or "").strip().lower()
        if source_unit in {"亿元", "亿", "100m_cny"}:
            net = raw_net
            unit_compatible = True
        elif source_unit in {"元", "人民币元", "yuan", "cny", "rmb"}:
            net = raw_net / 100_000_000.0 if raw_net is not None else None
            unit_compatible = True
        elif source_unit in {"万元", "万", "10k_cny"}:
            net = raw_net / 10_000.0 if raw_net is not None else None
            unit_compatible = True
        else:
            net = None
            unit_compatible = False
        quote_time = raw.get("quote_time")
        if net is None:
            return None, {
                "status": "unknown",
                "reason": (
                    "theme_net_amount_missing"
                    if raw_net is None
                    else "theme_source_unit_unsupported"
                ),
                "source_unit": source_unit or None,
                "unit_compatible": unit_compatible,
            }
        pct_change = _number(raw.get("pct_chg")) or 0.0
        score = _clamp(
            50.0
            + max(-80.0, min(net, 80.0)) * 0.5
            + max(-5.0, min(pct_change, 5.0)) * 6.0
        )
        return _clamp(score), {
            "status": "known",
            "net_amount_yi": net,
            "quote_time": str(quote_time) if quote_time else None,
            "received_at": str(raw.get("received_at"))
            if raw.get("received_at")
            else None,
            "source_unit": source_unit,
            "unit_compatible": unit_compatible,
        }

    @classmethod
    def _fund_factor(
        cls,
        item: Mapping[str, Any],
        lane: str,
    ) -> tuple[float | None, dict[str, Any]]:
        clock_mode = str(item.get("decision_clock_mode") or "").strip().lower()
        realtime_net = _number(item.get("realtime_mf_net"))
        realtime_denominator = _number(item.get("realtime_mf_amount"))
        denominator_source = "stock_realtime_moneyflow_snapshot.amount"
        source_unit = str(item.get("realtime_mf_source_unit") or "").strip().lower()
        unit_compatible = source_unit in {"元", "人民币元", "yuan", "cny", "rmb"}
        realtime_trade_date = str(item.get("realtime_trade_date") or "")[:10]
        moneyflow_trade_date = str(item.get("realtime_mf_trade_date") or "")[:10]
        same_trade_date = bool(
            realtime_trade_date
            and moneyflow_trade_date
            and realtime_trade_date == moneyflow_trade_date
        )
        current_known = bool(
            clock_mode == "intraday"
            and realtime_net is not None
            and realtime_denominator is not None
            and realtime_denominator > 0
            and same_trade_date
            and unit_compatible
        )
        if current_known:
            intensity = realtime_net / realtime_denominator * 100.0
            stock_score = _clamp(50.0 + max(-20.0, min(intensity, 20.0)) * 2.5)
            stock_clock = "realtime_same_clock"
        else:
            daily_net = _number(item.get("net_mf_amount"))
            daily_amount = _number(item.get("amount"))
            if daily_net is not None and daily_amount is not None and daily_amount > 0:
                intensity = daily_net * 10_000.0 / daily_amount * 100.0
                stock_score = _clamp(50.0 + max(-20.0, min(intensity, 20.0)) * 2.0)
                stock_clock = "daily_background"
            else:
                intensity = None
                stock_score = None
                stock_clock = "unknown"
        theme_score, theme_metrics = cls._theme_fund(item)
        values: list[tuple[float, float]] = []
        if stock_score is not None:
            values.append((stock_score, 0.75))
        if lane == "theme_leader" and theme_score is not None:
            values.append((theme_score, 0.25))
        if not values:
            final = None
        else:
            weight = sum(item_weight for _, item_weight in values)
            final = _clamp(sum(value * item_weight for value, item_weight in values) / weight)
        return final, {
            "status": "known" if final is not None else "unknown",
            "stock_clock": stock_clock,
            "current_clock_complete": current_known,
            "net_flow_intensity_pct": round(intensity, 4) if intensity is not None else None,
            "net_amount": realtime_net if current_known else None,
            "denominator": realtime_denominator if current_known else None,
            "denominator_source": denominator_source if current_known else None,
            "same_trade_date": same_trade_date,
            "source_unit": source_unit or None,
            "unit_compatible": unit_compatible,
            "quote_time": item.get("realtime_mf_quote_time"),
            "received_at": item.get("realtime_mf_received_at"),
            "theme": theme_metrics,
        }

    @staticmethod
    def _price_volume_factor(
        item: Mapping[str, Any],
        lane: str,
    ) -> tuple[float | None, dict[str, Any]]:
        price_change = _number(item.get("realtime_pct_chg"))
        if price_change is None:
            price_change = _number(item.get("pct_chg_1d"))
            price_clock = "daily_background"
        else:
            price_clock = "realtime"
        if price_change is None:
            return None, {"status": "unknown", "reason": "price_change_missing"}
        base = 50.0 + max(-10.0, min(price_change, 10.0)) * 3.5
        base -= max(price_change - 7.0, 0.0) * 8.0
        volume_ratio = _number(item.get("volume_ratio"))
        if volume_ratio is not None:
            base += max(-8.0, min((volume_ratio - 1.0) * 10.0, 10.0))
        path = item.get("intraday_path") if isinstance(item.get("intraday_path"), Mapping) else {}
        path_state = str(path.get("path_state") or "unknown")
        base += {
            "sustained_support": 12.0,
            "repair": 4.0,
            "spike_fade": -28.0,
            "weak_or_unconfirmed": -8.0,
        }.get(path_state, 0.0)
        vwap_deviation = _number(path.get("vwap_deviation_pct"))
        event_response = _number(path.get("event_response_pct"))
        if event_response is not None:
            base -= max(event_response - 7.0, 0.0) * 4.0
        if vwap_deviation is not None:
            base += max(-8.0, min(vwap_deviation * 2.0, 6.0))
        current_rank = _number(item.get("theme_current_rank"))
        current_pool = _number(item.get("theme_current_pool_size"))
        if lane == "theme_leader" and current_rank is not None and current_pool and current_pool > 0:
            percentile = 1.0 - (current_rank - 1.0) / max(current_pool, 1.0)
            base += max(0.0, percentile) * 8.0
        return _clamp(base), {
            "status": "known",
            "price_clock": price_clock,
            "price_change_pct": price_change,
            "volume_ratio": volume_ratio,
            "path_state": path_state,
            "intraday_feature_version": path.get("feature_version") or INTRADAY_FEATURE_VERSION,
            "theme_current_rank": current_rank,
            "theme_current_pool_size": current_pool,
            "event_response_pct": event_response,
        }

    @staticmethod
    def _chip_capacity_factor(item: Mapping[str, Any]) -> tuple[float | None, dict[str, Any]]:
        median_amount = _number(item.get("median_amount_20"))
        if median_amount is None or median_amount <= 0:
            return None, {"status": "unknown", "reason": "median_amount_20_missing"}
        liquidity_ratio = median_amount / 80_000_000.0
        capacity = _clamp(50.0 + max(-1.0, min(math.log10(max(liquidity_ratio, 0.01)), 1.5)) * 22.0)
        winner_rate = _number(item.get("chip_winner_rate"))
        winner_rate_pct = (
            winner_rate * 100.0
            if winner_rate is not None and 0.0 <= winner_rate <= 1.0
            else winner_rate
        )
        chip_center = _number(item.get("chip_weight_avg"))
        price = _number(item.get("realtime_price"))
        if price is None:
            price = _number(item.get("close"))
        chip_penalty = 0.0
        if winner_rate_pct is not None and winner_rate_pct >= 90.0:
            chip_penalty += 10.0
        if chip_center is not None and price is not None and chip_center > 0:
            distance = (price / chip_center - 1.0) * 100.0
            chip_penalty += max(distance - 12.0, 0.0) * 0.8
        return _clamp(capacity - chip_penalty), {
            "status": "known",
            "median_amount_20": median_amount,
            "liquidity_floor": 80_000_000.0,
            "chip_winner_rate": winner_rate,
            "chip_winner_rate_pct": winner_rate_pct,
            "chip_penalty": round(chip_penalty, 4),
        }

    def _lane_factor_set(self, item: Mapping[str, Any], lane: str) -> tuple[dict[str, float | None], dict[str, Any]]:
        catalyst, persistence, catalyst_quality = self._catalyst_factors(item, lane)
        relation, relation_quality = self._relation_factor(item, lane)
        fund, fund_quality = self._fund_factor(item, lane)
        price_volume, price_quality = self._price_volume_factor(item, lane)
        chip, chip_quality = self._chip_capacity_factor(item)
        factors = {
            "catalyst_quality": catalyst,
            "persistence": persistence,
            "relation_recognition": relation,
            "fund_confirmation": fund,
            "price_volume_confirmation": price_volume,
            "chip_liquidity_capacity": chip,
        }
        quality = {
            "catalyst_quality": catalyst_quality,
            "persistence": {
                "status": "known" if persistence is not None else "unknown",
                "producer": "event_driver_contract",
            },
            "relation_recognition": relation_quality,
            "fund_confirmation": fund_quality,
            "price_volume_confirmation": price_quality,
            "chip_liquidity_capacity": chip_quality,
        }
        return factors, quality

    def compute_factors(self, data_bundle: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in data_bundle.get("candidates") or []:
            lane_factor_scores: dict[str, dict[str, float | None]] = {}
            lane_factor_quality: dict[str, dict[str, Any]] = {}
            for lane in self._candidate_lanes(item):
                factors, quality = self._lane_factor_set(item, lane)
                lane_factor_scores[lane] = factors
                lane_factor_quality[lane] = quality
            rows.append(
                {
                    **item,
                    "lane_factor_scores": lane_factor_scores,
                    "lane_factor_quality": lane_factor_quality,
                    "strategy_notes": [
                        "v0.6 机会评分与当前入场条件分离",
                        "缺失因子不以中性分补齐；AI 不改变正式分数、排序或条件状态",
                    ],
                }
            )
        return rows

    def _score_lane(
        self,
        factors: Mapping[str, Any],
    ) -> tuple[float, dict[str, float], float, list[str]]:
        configured = self.config.get("weights", {}) or {}
        weights = {key: float(configured.get(key, 0.0)) for key in self.FACTOR_KEYS}
        total = sum(weights.values())
        if total <= 0:
            raise ValueError("a_share_sentiment_v06 requires positive deterministic factor weights")
        weights = {key: value / total for key, value in weights.items()}
        contributions: dict[str, float] = {}
        known_weight = 0.0
        missing: list[str] = []
        for key, weight in weights.items():
            value = _number(factors.get(key))
            if value is None:
                contributions[key] = 0.0
                missing.append(key)
                continue
            known_weight += weight
            contributions[key] = round(_clamp(value) * weight, 4)
        return (
            _clamp(sum(contributions.values())),
            contributions,
            round(known_weight, 6),
            missing,
        )

    def _valid_until(self, item: Mapping[str, Any]) -> str | None:
        decision = _datetime(item.get("decision_as_of") or item.get("opinion_as_of_datetime"))
        if decision is None:
            return None
        entry = self.config.get("entry_gates", {}) or {}
        candidate = decision + timedelta(minutes=int(entry.get("valid_for_minutes", 5)))
        expirations = [
            value
            for event in self._lane_events(item, str(item.get("primary_lane") or ""))
            if (value := _datetime(event.get("effective_until"))) is not None
        ]
        if expirations:
            candidate = min(candidate, min(expirations))
        return _text_datetime(candidate)

    def _evaluate_entry(self, item: Mapping[str, Any]) -> dict[str, Any]:
        config = self.config.get("entry_gates", {}) or {}
        lane = str(item.get("primary_lane") or "")
        score = _number(item.get("final_score")) or 0.0
        decision = _datetime(item.get("decision_as_of") or item.get("opinion_as_of_datetime"))
        coverage = _number(item.get("market_coverage_ratio", item.get("snapshot_coverage")))
        realtime_market_coverage = _number(
            item.get("realtime_market_coverage_ratio")
        )
        minimum_coverage = float(config.get("minimum_coverage", 0.98))
        path = item.get("intraday_path") if isinstance(item.get("intraday_path"), Mapping) else {}
        fund = (
            (item.get("lane_factor_quality") or {}).get(lane, {}).get("fund_confirmation")
            if lane
            else {}
        ) or {}
        path_data_status = str(path.get("data_status") or "missing")
        path_state = str(path.get("path_state") or "unknown")
        high_drawdown = _number(path.get("high_drawdown_pct"))
        short_return = _number(path.get("short_return_pct"))
        vwap_deviation = _number(path.get("vwap_deviation_pct"))
        event_response = _number(path.get("event_response_pct"))
        price_change = _number(item.get("realtime_pct_chg"))
        theme_breadth = _number(item.get("theme_current_breadth_ratio"))
        lane_events = self._lane_events(item, lane)
        event_ok = bool(self._direct_event_ids(item)) if lane == "direct_catalyst" else bool(
            any(
                event.get("confirmation_status") in CONFIRMED_EVENT_STATES
                and not bool(event.get("is_terminal"))
                for event in lane_events
            )
        )
        relation_ok = (
            any(
                relation.get("relation_status") in CONFIRMED_RELATION_STATES
                and str(relation.get("canonical_event_id") or "") in self._direct_event_ids(item)
                for relation in self._relations(item)
            )
            if lane == "direct_catalyst"
            else bool(
                any(
                    relation.get("relation_type") == "theme_mapping"
                    and relation.get("relation_status") in CONFIRMED_RELATION_STATES
                    for relation in self._relations(item)
                )
            )
        )
        fund_intensity = _number(fund.get("net_flow_intensity_pct"))
        fund_ok = (
            bool(fund.get("current_clock_complete"))
            and fund.get("unit_compatible") is True
            and fund_intensity is not None
            and fund_intensity >= float(config.get("minimum_net_flow_intensity_pct", 0.0))
        )
        quote_source_ok = _latest_at_or_before(item.get("realtime_quote_time"), decision)
        quote_received_ok = _latest_at_or_before(item.get("realtime_received_at"), decision)
        moneyflow_source_ok = _latest_at_or_before(item.get("realtime_mf_quote_time"), decision)
        moneyflow_received_ok = _latest_at_or_before(item.get("realtime_mf_received_at"), decision)
        quote_source_age = _age_seconds(item.get("realtime_quote_time"), decision)
        quote_received_age = _age_seconds(item.get("realtime_received_at"), decision)
        moneyflow_source_age = _age_seconds(item.get("realtime_mf_quote_time"), decision)
        moneyflow_received_age = _age_seconds(item.get("realtime_mf_received_at"), decision)
        quote_ttl = float(config.get("quote_ttl_seconds", 180))
        maximum_clock_skew = float(config.get("maximum_clock_skew_seconds", 180))
        quote_time = _datetime(item.get("realtime_quote_time"))
        quote_received_time = _datetime(item.get("realtime_received_at"))
        moneyflow_time = _datetime(item.get("realtime_mf_quote_time"))
        moneyflow_received_time = _datetime(
            item.get("realtime_mf_received_at")
        )
        quote_receive_order_ok = bool(
            quote_time is not None
            and quote_received_time is not None
            and quote_received_time >= quote_time
        )
        moneyflow_receive_order_ok = bool(
            moneyflow_time is not None
            and moneyflow_received_time is not None
            and moneyflow_received_time >= moneyflow_time
        )
        clock_skew = (
            abs((quote_time - moneyflow_time).total_seconds())
            if quote_time is not None and moneyflow_time is not None
            else None
        )
        gates: dict[str, dict[str, Any]] = {
            "score": {
                "passed": score >= float(config.get("conditions_score", 68.0)),
                "value": score,
            },
            "snapshot_coverage": {
                "passed": coverage is not None and coverage >= minimum_coverage,
                "value": coverage,
            },
            "realtime_market_coverage": {
                "passed": realtime_market_coverage is not None
                and realtime_market_coverage >= minimum_coverage,
                "value": realtime_market_coverage,
            },
            "event_relation": {
                "passed": event_ok and relation_ok,
                "event_passed": event_ok,
                "relation_passed": relation_ok,
                "lane": lane,
            },
            "event_ledger": {
                "passed": str(item.get("event_evidence_source") or "")
                in {
                    "append_only_ledger",
                    "append_only_ledger_and_snapshot_bridge",
                },
                "value": item.get("event_evidence_source"),
            },
            "market_regime": {
                "passed": self._market_regime(item) != "defensive",
                "value": self._market_regime(item),
            },
            "fund_same_clock": {
                "passed": fund_ok,
                "value": fund_intensity,
                "clock": fund.get("stock_clock"),
                "unit_compatible": fund.get("unit_compatible"),
            },
            "source_and_receive_cutoff": {
                "passed": all(
                    value is True
                    for value in (
                        quote_source_ok,
                        quote_received_ok,
                        moneyflow_source_ok,
                        moneyflow_received_ok,
                    )
                )
                and quote_receive_order_ok
                and moneyflow_receive_order_ok,
                "quote_source_ok": quote_source_ok,
                "quote_received_ok": quote_received_ok,
                "moneyflow_source_ok": moneyflow_source_ok,
                "moneyflow_received_ok": moneyflow_received_ok,
                "quote_receive_order_ok": quote_receive_order_ok,
                "moneyflow_receive_order_ok": moneyflow_receive_order_ok,
            },
            "same_clock_freshness": {
                "passed": (
                    all(
                        value is not None and value <= quote_ttl
                        for value in (
                            quote_source_age,
                            quote_received_age,
                            moneyflow_source_age,
                            moneyflow_received_age,
                        )
                    )
                    and clock_skew is not None
                    and clock_skew <= maximum_clock_skew
                ),
                "quote_source_age_seconds": quote_source_age,
                "quote_received_age_seconds": quote_received_age,
                "moneyflow_source_age_seconds": moneyflow_source_age,
                "moneyflow_received_age_seconds": moneyflow_received_age,
                "clock_skew_seconds": clock_skew,
                "quote_ttl_seconds": quote_ttl,
                "maximum_clock_skew_seconds": maximum_clock_skew,
            },
            "intraday_path": {
                "passed": path_data_status == "complete"
                and path_state in {"sustained_support", "repair"},
                "data_status": path_data_status,
                "path_state": path_state,
            },
            "price_acceptance": {
                "passed": (
                    path_data_status == "complete"
                    and path_state != "spike_fade"
                    and high_drawdown is not None
                    and high_drawdown >= float(config.get("minimum_high_drawdown_pct", -2.5))
                    and short_return is not None
                    and short_return >= float(config.get("minimum_short_return_pct", -0.8))
                    and vwap_deviation is not None
                    and vwap_deviation >= float(config.get("minimum_vwap_deviation_pct", -0.5))
                    and price_change is not None
                    and price_change <= float(config.get("maximum_chase_pct", 7.0))
                ),
                "high_drawdown_pct": high_drawdown,
                "short_return_pct": short_return,
                "vwap_deviation_pct": vwap_deviation,
                "price_change_pct": price_change,
            },
            "event_price_response": {
                "passed": (
                    True
                    if lane != "direct_catalyst"
                    else event_response is not None
                    and event_response
                    >= float(config.get("minimum_event_response_pct", -0.5))
                    and event_response
                    <= float(config.get("maximum_event_response_pct", 7.0))
                ),
                "value": event_response,
                "required_for_lane": lane == "direct_catalyst",
                "pre_event_price": path.get("pre_event_price"),
                "event_available_at": path.get("event_available_at"),
            },
            "theme_breadth": {
                "passed": (
                    True
                    if lane != "theme_leader"
                    else theme_breadth is not None
                    and theme_breadth >= float(config.get("minimum_theme_breadth_ratio", 0.50))
                ),
                "value": theme_breadth,
                "required_for_lane": lane == "theme_leader",
            },
            "tradeability": {
                "passed": not bool(item.get("is_suspended"))
                and not bool(item.get("limit_up_untradable", item.get("is_limit_up")))
                and not bool(item.get("is_delisting")),
            },
        }
        definitive_invalid = list(item.get("hard_gate_reasons") or [])
        if bool(item.get("adverse_event_veto")) and "high_confidence_adverse_event" not in definitive_invalid:
            definitive_invalid.append("high_confidence_adverse_event")
        valid_until = self._valid_until(item)
        valid_until_dt = _datetime(valid_until)
        if decision is None:
            definitive_invalid.append("decision_as_of_missing")
        elif valid_until_dt is not None and valid_until_dt <= decision:
            definitive_invalid.append("event_or_snapshot_expired")
        if gates["tradeability"]["passed"] is False:
            definitive_invalid.append("untradable_security_state")

        block_reasons = list(dict.fromkeys(definitive_invalid))
        for name, result in gates.items():
            if result.get("passed") is not True:
                block_reasons.append(name)
        block_reasons = list(dict.fromkeys(block_reasons))
        if definitive_invalid:
            eligibility = "invalid"
        elif all(result.get("passed") is True for result in gates.values()):
            eligibility = "conditions_met"
        else:
            eligibility = "observe"
        return {
            "entry_eligibility": eligibility,
            "entry_block_reasons": block_reasons,
            "entry_gate_results": gates,
            "decision_as_of": _text_datetime(decision),
            "valid_until": valid_until,
        }

    def score(self, stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        scored: list[dict[str, Any]] = []
        for raw in stocks:
            item = dict(raw)
            lane_results: dict[str, dict[str, Any]] = {}
            for lane, factors in (item.get("lane_factor_scores") or {}).items():
                score, contributions, known_weight, missing = self._score_lane(factors)
                lane_name = str(lane)
                lane_candidate = {
                    **item,
                    "primary_lane": lane_name,
                    "candidate_lane": lane_name,
                    "factors": dict(factors),
                    "factor_contributions": contributions,
                    "factor_known_weight": known_weight,
                    "missing_factor_keys": missing,
                    "score": score,
                    "local_score": score,
                    "final_score": score,
                }
                entry = self._evaluate_entry(lane_candidate)
                lane_results[lane_name] = {
                    "score": score,
                    "factor_scores": dict(factors),
                    "factor_contributions": contributions,
                    "known_weight": known_weight,
                    "missing_factors": missing,
                    "scale": "sentiment-v06-common-0-100-v1",
                    "entry_eligibility": entry["entry_eligibility"],
                    "entry_block_reasons": entry["entry_block_reasons"],
                    "entry_gate_results": entry["entry_gate_results"],
                    "decision_as_of": entry["decision_as_of"],
                    "valid_until": entry["valid_until"],
                }
            if lane_results:
                primary_lane = min(
                    lane_results,
                    key=lambda lane: (
                        {"conditions_met": 0, "observe": 1, "invalid": 2}.get(
                            str(lane_results[lane].get("entry_eligibility")), 3
                        ),
                        -float(lane_results[lane]["score"]),
                        0 if lane == "direct_catalyst" else 1,
                        lane,
                    ),
                )
                primary = lane_results[primary_lane]
            else:
                primary_lane = "unknown"
                primary = {
                    "score": 0.0,
                    "factor_scores": {key: None for key in self.FACTOR_KEYS},
                    "factor_contributions": {key: 0.0 for key in self.FACTOR_KEYS},
                    "known_weight": 0.0,
                    "missing_factors": list(self.FACTOR_KEYS),
                    "entry_eligibility": "invalid",
                    "entry_block_reasons": ["no_v06_candidate_lane"],
                    "entry_gate_results": {},
                    "decision_as_of": None,
                    "valid_until": None,
                }
            enriched = {
                **item,
                "primary_lane": primary_lane,
                "candidate_lane": primary_lane,
                "lane_scores": lane_results,
                "factors": dict(primary["factor_scores"]),
                "factor_contributions": dict(primary["factor_contributions"]),
                "factor_known_weight": primary["known_weight"],
                "missing_factor_keys": list(primary["missing_factors"]),
                "score": primary["score"],
                "local_score": primary["score"],
                "final_score": primary["score"],
                "price_preference_delta_applied": 0.0,
                "ai_applied_adjustment": 0.0,
                "ai_status": "not_requested",
                "entry_eligibility": primary["entry_eligibility"],
                "entry_block_reasons": list(primary["entry_block_reasons"]),
                "entry_gate_results": dict(primary["entry_gate_results"]),
                "decision_as_of": primary["decision_as_of"],
                "valid_until": primary["valid_until"],
            }
            scored.append(enriched)
        return sorted(
            scored,
            key=lambda row: (
                {"conditions_met": 0, "observe": 1, "invalid": 2}.get(
                    str(row.get("entry_eligibility")), 3
                ),
                -float(row.get("final_score") or 0.0),
                str(row.get("code") or ""),
            ),
        )

    def select(self, scored_stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        max_picks = int(self.config.get("max_picks", 3))
        grading = self.config.get("grading", {}) or {}
        reject_below = float(grading.get("reject_below", 60.0))
        max_per_theme = int(grading.get("max_per_theme", 1))
        max_per_industry = int(grading.get("max_per_industry", 2))
        selected: list[dict[str, Any]] = []
        theme_counts: dict[str, int] = {}
        industry_counts: dict[str, int] = {}
        for item in scored_stocks:
            if float(item.get("final_score") or 0.0) < reject_below:
                continue
            if item.get("entry_eligibility") == "invalid":
                continue
            theme = str(item.get("opinion_sector_name") or "").strip()
            theme_keys = sorted(
                {
                    str(value).strip()
                    for value in (
                        item.get("sentiment_v06_concentration_themes")
                        or ([theme] if theme else [])
                    )
                    if str(value).strip()
                }
            )
            industry = str(item.get("industry") or "").strip()
            if any(theme_counts.get(value, 0) >= max_per_theme for value in theme_keys):
                continue
            if industry and industry_counts.get(industry, 0) >= max_per_industry:
                continue
            for value in theme_keys:
                theme_counts[value] = theme_counts.get(value, 0) + 1
            if industry:
                industry_counts[industry] = industry_counts.get(industry, 0) + 1
            conditions_met = item.get("entry_eligibility") == "conditions_met"
            grade_reason = (
                "shadow_conditions_met_research_only"
                if conditions_met
                else "entry_conditions_incomplete_or_failed"
            )
            gate_results = {
                "hard_gate_pass": bool(item.get("hard_gate_pass")),
                "hard_gate_reasons": list(item.get("hard_gate_reasons") or []),
                "watch_gate_reasons": list(item.get("entry_block_reasons") or []),
                "grade_reason": grade_reason,
                "entry": dict(item.get("entry_gate_results") or {}),
            }
            selected.append(
                {
                    **item,
                    "grade_state": "watch",
                    "signal_grade": "watch",
                    "grade_reason": grade_reason,
                    "trade_grade_state": "watch",
                    "trade_grade_label": "研究候选" if conditions_met else "仅观察",
                    "trade_grade_reason": (
                        "盘中必要条件均满足，但 0.6.0 仍为未验证影子策略"
                        if conditions_met
                        else "至少一项盘中必要条件不满足或证据不足"
                    ),
                    "validation_status": "shadow_only",
                    "score_breakdown": dict(item.get("factor_contributions") or {}),
                    "gate_results": gate_results,
                    "evidence_ids": sorted(
                        {
                            str(value)
                            for value in item.get("evidence_ids") or []
                            if str(value)
                        }
                    ),
                }
            )
            if len(selected) >= max_picks:
                break
        return selected

    def explain(self, stock: dict[str, Any]) -> dict[str, Any]:
        return {
            "code": stock.get("code"),
            "score": stock.get("final_score", stock.get("score")),
            "local_score": stock.get("local_score"),
            "strategy": self.strategy_id,
            "candidate_lane": stock.get("primary_lane"),
            "candidate_lanes": list(stock.get("candidate_lanes") or []),
            "lane_scores": stock.get("lane_scores") or {},
            "factors": stock.get("factors") or {},
            "factor_contributions": stock.get("factor_contributions") or {},
            "factor_quality": (stock.get("lane_factor_quality") or {}).get(
                stock.get("primary_lane"), {}
            ),
            "missing_factor_keys": list(stock.get("missing_factor_keys") or []),
            "factor_schema_version": FACTOR_SCHEMA_VERSION,
            "evidence_rule_version": EVIDENCE_RULE_VERSION,
            "relation_rule_version": RELATION_RULE_VERSION,
            "entry_rule_version": ENTRY_RULE_VERSION,
            "evaluation_method_version": EVALUATION_METHOD_VERSION,
            "evaluation_spec_hash": stock.get("evaluation_spec_hash")
            or sentiment_v06_evaluation_spec_hash(),
            "execution_rule_version": EXECUTION_RULE_VERSION,
            "entry_eligibility": stock.get("entry_eligibility"),
            "entry_block_reasons": list(stock.get("entry_block_reasons") or []),
            "entry_gate_results": stock.get("entry_gate_results") or {},
            "decision_as_of": stock.get("decision_as_of"),
            "valid_until": stock.get("valid_until"),
            "evidence_quality": stock.get("evidence_quality") or {},
            "canonical_events": self._events(stock),
            "event_relations": self._relations(stock),
            "intraday_path": stock.get("intraday_path") or {},
            "signal_grade": stock.get("signal_grade"),
            "grade_reason": stock.get("grade_reason"),
            "validation_status": "shadow_only",
            "gate_results": stock.get("gate_results") or {},
            "evidence_ids": list(stock.get("evidence_ids") or []),
            "ai_status": stock.get("ai_status") or "not_requested",
            "notes": list(stock.get("strategy_notes") or []),
            "raw_metrics": {
                **dict(stock.get("strategy_raw_metrics") or {}),
                "primary_lane": stock.get("primary_lane"),
                "candidate_lanes": list(stock.get("candidate_lanes") or []),
                "entry_eligibility": stock.get("entry_eligibility"),
                "entry_block_reasons": list(stock.get("entry_block_reasons") or []),
                "factor_schema_version": FACTOR_SCHEMA_VERSION,
                "evidence_rule_version": EVIDENCE_RULE_VERSION,
                "entry_rule_version": ENTRY_RULE_VERSION,
                "evaluation_method_version": EVALUATION_METHOD_VERSION,
                "evaluation_spec_hash": stock.get("evaluation_spec_hash")
                or sentiment_v06_evaluation_spec_hash(),
                "decision_as_of": stock.get("decision_as_of"),
                "valid_until": stock.get("valid_until"),
            },
        }
