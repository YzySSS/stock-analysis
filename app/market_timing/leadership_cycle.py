from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from app.shared.db import mysql_read_conn


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "market_leadership_cycle_v2.json"
)
MODEL_ID = "market_leadership_cycle_v2"
MODEL_VERSION = "2.0.0"

STRENGTH_LABELS = {
    "watch": "观察",
    "confirmed": "确认",
    "core": "核心",
    "crowded": "拥挤",
    "fading": "退潮",
}

CYCLE_LABELS = {
    "insufficient_data": "周期待补证",
    "base": "筑底观察",
    "first_impulse": "启动确认",
    "main_up": "主升阶段",
    "late_acceleration": "加速末段",
    "pullback": "主升回踩",
    "rebound_candidate": "反弹修复·B浪候选",
    "secondary_decline_risk": "二次下探·C浪风险",
    "downtrend": "下降趋势",
    "range": "震荡整理",
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


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clip(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None else None


def load_leadership_cycle_spec() -> dict[str, Any]:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    if spec.get("model_id") != MODEL_ID:
        raise RuntimeError("leadership cycle spec model_id mismatch")
    if spec.get("version") != MODEL_VERSION:
        raise RuntimeError("leadership cycle spec version mismatch")
    weights = spec.get("strength_weights") or {}
    if set(weights) != {"heat", "capital", "breadth", "persistence", "price"}:
        raise RuntimeError("leadership cycle strength weights are incomplete")
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-9:
        raise RuntimeError("leadership cycle strength weights must sum to one")
    return spec


def leadership_cycle_spec_hash() -> str:
    return _sha256(load_leadership_cycle_spec())


def build_sector_price_series(
    rows: Sequence[Mapping[str, Any]],
    industries: Sequence[str],
    *,
    maximum_days: int = 90,
    minimum_component_coverage: float = 0.6,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build a stable sector proxy without using raw ETF prices.

    A single THS industry keeps its published index close. Composite themes
    use an equal-weight return index built from provider pct_change fields, so
    unrelated index point scales never get averaged together.
    """

    expected = tuple(dict.fromkeys(str(item).strip() for item in industries if str(item).strip()))
    if not expected:
        return [], {
            "status": "unmapped",
            "proxy_type": None,
            "industries": [],
        }
    allowed = set(expected)
    grouped: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        industry = str(row.get("industry_name") or "").strip()
        trade_date = str(row.get("trade_date") or "")[:10]
        if industry in allowed and trade_date:
            grouped[trade_date][industry] = row

    if len(expected) == 1:
        industry = expected[0]
        series = [
            {
                "trade_date": trade_date,
                "value": value,
                "component_coverage": 1.0,
            }
            for trade_date, members in sorted(grouped.items())
            if (
                (member := members.get(industry)) is not None
                and (value := _number(member.get("close"))) is not None
                and value > 0
            )
        ][-maximum_days:]
        return series, {
            "status": "ready" if series else "missing",
            "proxy_type": "ths_industry_index_close",
            "industries": list(expected),
        }

    level = 100.0
    series: list[dict[str, Any]] = []
    for trade_date, members in sorted(grouped.items()):
        changes = [
            value
            for industry in expected
            if (
                (member := members.get(industry)) is not None
                and (
                    value := _number(
                        member.get("pct_change", member.get("pct_chg"))
                    )
                )
                is not None
            )
        ]
        coverage = len(changes) / len(expected)
        if coverage < minimum_component_coverage or not changes:
            continue
        level *= 1 + mean(changes) / 100
        series.append(
            {
                "trade_date": trade_date,
                "value": level,
                "component_coverage": coverage,
            }
        )
    return series[-maximum_days:], {
        "status": "ready" if series else "missing",
        "proxy_type": "equal_weight_ths_industry_return_index",
        "industries": list(expected),
    }


def compute_price_metrics(
    series: Sequence[Mapping[str, Any]],
    *,
    minimum_days: int = 60,
) -> dict[str, Any]:
    clean = [
        {
            "trade_date": str(row.get("trade_date") or "")[:10],
            "value": value,
        }
        for row in series
        if (
            (value := _number(row.get("value"))) is not None
            and value > 0
            and str(row.get("trade_date") or "")[:10]
        )
    ]
    if len(clean) < minimum_days:
        return {
            "status": "insufficient_history",
            "history_days": len(clean),
            "minimum_history_days": minimum_days,
        }

    values = [float(row["value"]) for row in clean]
    current = values[-1]
    ma20 = mean(values[-20:])
    ma60 = mean(values[-60:])
    prior_ma20 = mean(values[-25:-5]) if len(values) >= 25 else None
    prior_ma60 = mean(values[-65:-5]) if len(values) >= 65 else None
    window = clean[-60:]
    window_values = [float(row["value"]) for row in window]
    high_index = max(range(len(window_values)), key=window_values.__getitem__)
    high_value = window_values[high_index]
    low_before_high = min(window_values[: high_index + 1])
    post_high_values = window_values[high_index:]
    post_high_low = min(post_high_values)
    post_high_low_index = high_index + post_high_values.index(post_high_low)
    return20_base = values[-20]
    return60_base = values[-60]
    metrics = {
        "status": "ready",
        "history_days": len(clean),
        "trade_date": clean[-1]["trade_date"],
        "current": current,
        "ma20": ma20,
        "ma60": ma60,
        "distance_ma20_pct": (current / ma20 - 1) * 100,
        "distance_ma60_pct": (current / ma60 - 1) * 100,
        "ma20_slope_5_pct": (
            (ma20 / prior_ma20 - 1) * 100
            if prior_ma20 not in {None, 0}
            else None
        ),
        "ma60_slope_5_pct": (
            (ma60 / prior_ma60 - 1) * 100
            if prior_ma60 not in {None, 0}
            else None
        ),
        "return_5d_pct": (
            (current / values[-6] - 1) * 100 if len(values) >= 6 else None
        ),
        "return_20d_pct": (current / return20_base - 1) * 100,
        "return_60d_pct": (current / return60_base - 1) * 100,
        "high_60": high_value,
        "high_60_date": window[high_index]["trade_date"],
        "drawdown_from_high_60_pct": (current / high_value - 1) * 100,
        "prior_runup_to_high_pct": (
            (high_value / low_before_high - 1) * 100
            if low_before_high > 0
            else None
        ),
        "post_high_low": post_high_low,
        "post_high_low_date": window[post_high_low_index]["trade_date"],
        "post_high_drawdown_pct": (post_high_low / high_value - 1) * 100,
        "rebound_from_post_high_low_pct": (
            (current / post_high_low - 1) * 100
            if post_high_low > 0
            else None
        ),
        "days_since_high_60": len(window) - 1 - high_index,
        "days_since_post_high_low": len(window) - 1 - post_high_low_index,
    }
    return {
        key: _round(value) if isinstance(value, float) else value
        for key, value in metrics.items()
    }


def price_structure_score(metrics: Mapping[str, Any]) -> float:
    if metrics.get("status") != "ready":
        return 50.0
    distance20 = _number(metrics.get("distance_ma20_pct")) or 0.0
    distance60 = _number(metrics.get("distance_ma60_pct")) or 0.0
    return20 = _number(metrics.get("return_20d_pct")) or 0.0
    slope20 = _number(metrics.get("ma20_slope_5_pct")) or 0.0
    score = (
        50
        + (12 if distance20 >= 0 else -12)
        + (12 if distance60 >= 0 else -12)
        + max(-18, min(18, return20 * 1.2))
        + max(-8, min(8, slope20 * 2.0))
    )
    return round(_clip(score), 4)


def classify_cycle(metrics: Mapping[str, Any]) -> dict[str, Any]:
    if metrics.get("status") != "ready":
        state = "insufficient_data"
        return {
            "cycle_state": state,
            "cycle_label": CYCLE_LABELS[state],
            "reasons": ["行业价格历史不足60个交易日"],
            "upgrade_triggers": ["补齐行业价格历史后再识别波段位置"],
            "downgrade_triggers": [],
        }

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

    completed_impulse = prior_runup >= 20 and post_drawdown <= -15
    if completed_impulse and rebound >= 3 and distance60 < 0:
        state = "rebound_candidate"
    elif (
        completed_impulse
        and distance20 < 0
        and distance60 < 0
        and (rebound < 3 or return5 <= -4)
    ):
        state = "secondary_decline_risk"
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
        f"距60日高点 {drawdown:.1f}%",
    ]
    if completed_impulse:
        reasons.append(
            f"前段上涨 {prior_runup:.1f}%、高点后最大回撤 {post_drawdown:.1f}%"
        )
        reasons.append(f"低点后修复 {rebound:.1f}%")
    upgrade = [
        "价格重新站稳MA60与MA20",
        "MA20、MA60斜率转正且真实宽度同步扩散",
    ]
    downgrade = [
        "反弹失败并跌破高点后的阶段低点",
        "MA20继续下行且成分股宽度再度收缩",
    ]
    return {
        "cycle_state": state,
        "cycle_label": CYCLE_LABELS[state],
        "reasons": reasons,
        "upgrade_triggers": upgrade,
        "downgrade_triggers": downgrade,
    }


def compute_breadth_metrics(
    rows: Sequence[Mapping[str, Any]],
    industries: Sequence[str],
    *,
    minimum_members: int = 5,
    minimum_coverage: float = 0.8,
) -> dict[str, Any]:
    expected = {str(item).strip() for item in industries if str(item).strip()}
    if not expected:
        return {
            "status": "unmapped",
            "score": 50.0,
            "member_count": 0,
            "covered_count": 0,
        }
    members = [
        row
        for row in rows
        if str(row.get("industry") or "").strip() in expected
    ]
    covered = [
        row
        for row in members
        if int(row.get("kline_count_60") or 0) >= 60
        and _number(row.get("latest_close")) is not None
        and _number(row.get("ma20")) is not None
        and _number(row.get("ma60")) is not None
    ]
    coverage = len(covered) / len(members) if members else 0.0
    if len(members) < minimum_members or coverage < minimum_coverage:
        return {
            "status": "insufficient_coverage",
            "score": 50.0,
            "member_count": len(members),
            "covered_count": len(covered),
            "coverage": round(coverage, 4),
            "industries": sorted(expected),
        }
    above20 = sum(
        1
        for row in covered
        if float(row["latest_close"]) > float(row["ma20"])
    )
    above60 = sum(
        1
        for row in covered
        if float(row["latest_close"]) > float(row["ma60"])
    )
    advancers = sum(
        1 for row in covered if (_number(row.get("pct_chg_1d")) or 0) > 0
    )
    positive20 = sum(
        1 for row in covered if (_number(row.get("return_20d_pct")) or 0) > 0
    )
    denominator = len(covered)
    above20_pct = above20 / denominator * 100
    above60_pct = above60 / denominator * 100
    advancer_pct = advancers / denominator * 100
    positive20_pct = positive20 / denominator * 100
    score = (
        above20_pct * 0.35
        + above60_pct * 0.35
        + advancer_pct * 0.20
        + positive20_pct * 0.10
    )
    return {
        "status": "ready",
        "score": round(score, 4),
        "member_count": len(members),
        "covered_count": denominator,
        "coverage": round(coverage, 4),
        "above_ma20_pct": round(above20_pct, 4),
        "above_ma60_pct": round(above60_pct, 4),
        "advancer_pct": round(advancer_pct, 4),
        "positive_return_20d_pct": round(positive20_pct, 4),
        "industries": sorted(expected),
        "source_note": "按stock_basic行业映射的价格宽度；主题为显式行业代理",
    }


class LeadershipCycleBuilder:
    def __init__(self, *, read_connection_factory=None) -> None:
        self._read_connection_factory = read_connection_factory or mysql_read_conn
        self.spec = load_leadership_cycle_spec()
        self.spec_hash = leadership_cycle_spec_hash()

    def _mapping(
        self,
        sector_type: str,
        sector_name: str,
        available_price_industries: set[str],
    ) -> tuple[list[str], list[str], str]:
        key = f"{sector_type}:{sector_name}"
        configured = (self.spec.get("sector_mappings") or {}).get(key) or {}
        price_industries = list(configured.get("price_industries") or [])
        stock_industries = list(configured.get("stock_industries") or [])
        mapping_source = "explicit"
        if not price_industries and sector_name in available_price_industries:
            price_industries = [sector_name]
            mapping_source = "exact_name"
        if not stock_industries and sector_type == "industry":
            stock_industries = [sector_name]
        return price_industries, stock_industries, mapping_source

    def build_rows(self, trade_date: date | str) -> list[dict[str, Any]]:
        through = date.fromisoformat(str(trade_date))
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, as_of_datetime, sector_type, sector_name,
                           sector_score, weighted_impact_score, news_count,
                           source_count, stock_count, positive_news_count,
                           negative_news_count
                    FROM sector_opinion_daily
                    WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 14 DAY) AND %s
                    ORDER BY trade_date, as_of_datetime
                    """,
                    (through, through),
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
                    (through,),
                )
                flow_rows = list(cursor.fetchall() or [])
                cursor.execute(
                    """
                    SELECT trade_date, industry_name, close, pct_change
                    FROM etf_rotation_sector_daily
                    WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 180 DAY) AND %s
                    ORDER BY trade_date, industry_name
                    """,
                    (through, through),
                )
                price_rows = list(cursor.fetchall() or [])
                cursor.execute(
                    """
                    SELECT MAX(trade_date) AS trade_date
                    FROM stock_technical_feature_daily
                    WHERE trade_date<=%s
                    """,
                    (through,),
                )
                technical_date = (cursor.fetchone() or {}).get("trade_date")
                technical_rows: list[dict[str, Any]] = []
                if technical_date == through:
                    cursor.execute(
                        """
                        SELECT s.code, s.industry, t.latest_close, t.ma20, t.ma60,
                               t.pct_chg_1d, t.return_20d_pct,
                               t.kline_count_60
                        FROM stock_basic s
                        JOIN stock_technical_feature_daily t
                          ON t.code=s.code AND t.trade_date=%s
                        WHERE s.instrument_type='stock'
                          AND COALESCE(s.is_delisted, 0)=0
                        """,
                        (through,),
                    )
                    technical_rows = list(cursor.fetchall() or [])

        latest_opinion: dict[tuple[Any, str, str], Mapping[str, Any]] = {}
        for row in opinion_rows:
            key = (
                row["trade_date"],
                str(row["sector_type"]),
                str(row["sector_name"]),
            )
            latest_opinion[key] = row
        current_rows = [
            row
            for (row_date, _, _), row in latest_opinion.items()
            if row_date == through
        ]
        history_by_sector: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for (_, sector_type, sector_name), row in latest_opinion.items():
            history_by_sector[(sector_type, sector_name)].append(row)
        flow_latest: dict[tuple[str, str], Mapping[str, Any]] = {}
        for row in flow_rows:
            flow_latest[(str(row["sector_type"]), str(row["sector_name"]))] = row
        available_price_industries = {
            str(row.get("industry_name") or "") for row in price_rows
        }

        results = []
        for row in sorted(
            current_rows,
            key=lambda item: _number(item.get("sector_score")) or 0,
            reverse=True,
        )[: int(self.spec["maximum_sectors"])]:
            sector_type = str(row["sector_type"])
            sector_name = str(row["sector_name"])
            sector_key = (sector_type, sector_name)
            history = sorted(
                history_by_sector[sector_key],
                key=lambda item: item["trade_date"],
            )
            current_score = _number(row.get("sector_score")) or 0.0
            prior_score = (
                _number(history[-2].get("sector_score"))
                if len(history) >= 2
                else None
            )
            score_change = current_score - prior_score if prior_score is not None else 0.0
            source_count = int(row.get("source_count") or 0)
            stock_count = int(row.get("stock_count") or 0)
            positive = int(row.get("positive_news_count") or 0)
            negative = int(row.get("negative_news_count") or 0)
            flow = flow_latest.get(sector_key)
            net_amount = _number(flow.get("net_amount")) if flow else None

            price_industries, stock_industries, mapping_source = self._mapping(
                sector_type,
                sector_name,
                available_price_industries,
            )
            price_series, price_lineage = build_sector_price_series(
                price_rows,
                price_industries,
                maximum_days=int(self.spec["price_trade_days"]),
                minimum_component_coverage=float(
                    self.spec["minimum_component_coverage"]
                ),
            )
            price_metrics = compute_price_metrics(
                price_series,
                minimum_days=int(self.spec["minimum_price_history_days"]),
            )
            cycle = classify_cycle(price_metrics)
            price_score = price_structure_score(price_metrics)
            breadth = compute_breadth_metrics(
                technical_rows,
                stock_industries,
                minimum_members=int(self.spec["minimum_breadth_members"]),
                minimum_coverage=float(self.spec["minimum_breadth_coverage"]),
            )

            heat_score = _clip(current_score)
            capital_score = (
                50 + max(-45, min(45, net_amount / 5))
                if net_amount is not None
                else 50.0
            )
            history_scores = [
                _number(item.get("sector_score"))
                for item in history
                if _number(item.get("sector_score")) is not None
            ]
            persistence_score = (
                min(30.0, len(history_scores) / 5 * 30)
                + (
                    sum(value >= 55 for value in history_scores)
                    / len(history_scores)
                    * 40
                    if history_scores
                    else 0
                )
                + _clip(50 + score_change * 3) * 0.30
            )
            breadth_score = float(breadth["score"])
            weights = self.spec["strength_weights"]
            leadership_score = (
                heat_score * float(weights["heat"])
                + capital_score * float(weights["capital"])
                + breadth_score * float(weights["breadth"])
                + persistence_score * float(weights["persistence"])
                + price_score * float(weights["price"])
            )
            price_near_high = (
                _clip(
                    100
                    + (_number(price_metrics.get("drawdown_from_high_60_pct")) or -25)
                    * 3
                )
                if price_metrics.get("status") == "ready"
                else 50.0
            )
            crowding_score = (
                _clip((heat_score - 60) * 2.5) * 0.40
                + price_near_high * 0.35
                + breadth_score * 0.25
            )

            fading = (
                score_change <= -8
                or (
                    negative > positive
                    and net_amount is not None
                    and net_amount < 0
                )
                or leadership_score < 35
            )
            if fading:
                strength_state = "fading"
            elif (
                crowding_score >= 65
                and leadership_score >= 65
                and cycle["cycle_state"] in {"main_up", "late_acceleration"}
            ):
                strength_state = "crowded"
            elif (
                leadership_score >= 72
                and heat_score >= 55
                and capital_score >= 55
                and breadth_score >= 50
                and price_score >= 50
            ):
                strength_state = "core"
            elif (
                leadership_score >= 60
                and source_count >= 2
                and positive >= negative
                and capital_score >= 45
                and breadth_score >= 40
                and price_score >= 35
            ):
                strength_state = "confirmed"
            else:
                strength_state = "watch"

            contradictions = []
            if negative > positive:
                contradictions.append("负面新闻多于正面新闻")
            if net_amount is not None and net_amount < 0:
                contradictions.append("当日行业资金净流出")
            if breadth.get("status") == "ready" and breadth_score < 35:
                contradictions.append("真实成分股宽度偏弱")
            if (
                price_metrics.get("status") == "ready"
                and (_number(price_metrics.get("distance_ma60_pct")) or 0) < 0
            ):
                contradictions.append("行业价格仍在MA60下方")
            if cycle["cycle_state"] in {
                "rebound_candidate",
                "secondary_decline_risk",
                "downtrend",
            }:
                contradictions.append(cycle["cycle_label"])

            evidence = [
                f"主线强度 {leadership_score:.1f}，舆情 {heat_score:.1f}",
                (
                    f"当日资金 {net_amount:+.2f}"
                    if net_amount is not None
                    else "当日资金缺失，按中性处理"
                ),
            ]
            if breadth.get("status") == "ready":
                evidence.append(
                    "真实宽度 "
                    f"{breadth_score:.1f}（MA20上方 {breadth['above_ma20_pct']:.1f}%、"
                    f"MA60上方 {breadth['above_ma60_pct']:.1f}%）"
                )
            else:
                evidence.append("真实宽度待补证，不再用新闻覆盖数冒充")
            evidence.extend(cycle["reasons"])

            confidence = (
                min(0.30, source_count * 0.05)
                + min(0.15, len(history) / 5 * 0.15)
                + (0.15 if net_amount is not None else 0)
                + (0.20 if price_metrics.get("status") == "ready" else 0)
                + (0.20 if breadth.get("status") == "ready" else 0)
            )
            as_of = row.get("as_of_datetime")
            payload = {
                "model_id": MODEL_ID,
                "version": MODEL_VERSION,
                "spec_hash": self.spec_hash,
                "trade_date": str(through),
                "as_of": str(as_of),
                "data_cutoff": str(as_of),
                "sector_type": sector_type,
                "sector_name": sector_name,
                "leadership_state": strength_state,
                "state_label": STRENGTH_LABELS[strength_state],
                "cycle_state": cycle["cycle_state"],
                "cycle_label": cycle["cycle_label"],
                "leadership_score": round(leadership_score, 2),
                "confidence": round(min(1.0, confidence), 4),
                "heat_score": round(heat_score, 2),
                "capital_score": round(capital_score, 2),
                "breadth_score": round(breadth_score, 2),
                "persistence_score": round(persistence_score, 2),
                "crowding_score": round(crowding_score, 2),
                "price_score": round(price_score, 2),
                "price_evidence_status": price_metrics.get("status"),
                "price_metrics": price_metrics,
                "breadth_metrics": breadth,
                "evidence": evidence,
                "contradictions": contradictions,
                "upgrade_triggers": cycle["upgrade_triggers"],
                "downgrade_triggers": cycle["downgrade_triggers"],
                "source_lineage": {
                    "opinion": "sector_opinion_daily",
                    "capital": (
                        "market_sector_fund_flow_intraday"
                        if flow
                        else "missing_neutral"
                    ),
                    "price": {
                        **price_lineage,
                        "source": "etf_rotation_sector_daily",
                        "mapping_source": mapping_source,
                        "raw_etf_price_used": False,
                    },
                    "breadth": {
                        "source": "stock_technical_feature_daily+stock_basic",
                        "technical_trade_date": str(technical_date)
                        if technical_date
                        else None,
                    },
                },
                "data_quality": {
                    "price_status": price_metrics.get("status"),
                    "breadth_status": breadth.get("status"),
                    "news_coverage_stock_count": stock_count,
                    "news_coverage_not_used_as_breadth": True,
                    "wave_labels_are_hypotheses": True,
                },
                "research_only": True,
            }
            payload["payload_hash"] = _sha256(payload)
            results.append(payload)
        return results
