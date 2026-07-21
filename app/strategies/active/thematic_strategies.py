from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any, Dict, List

from app.shared.sentiment_scoring import score_sources
from app.stock_selection.base import BaseSelectionStrategy


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return round(max(low, min(value, high)), 2)


def _score_peak(value: float | None, center: float, tolerance: float, floor: float = 0.0) -> float:
    if value is None:
        return 50.0
    return _clamp(100 - abs(value - center) / max(tolerance, 0.0001) * (100 - floor))


def _sentiment_0_100(value: Any) -> float:
    number = _to_float(value, None)
    if number is None:
        return 50.0
    if -1 <= number <= 1:
        number = 50 + number * 50
    return _clamp(number)


def _has_strong_stock_news(item: Dict[str, Any], min_impact: float = 75.0, min_timeliness: float = 80.0) -> bool:
    negative_terms = ("跌停", "跌超", "下挫", "跳水", "杀跌", "大跌", "冲高回落", "风险", "减持", "处罚")
    for news in item.get("opinion_stock_news") or []:
        title = str(news.get("title") or "")
        if any(term in title for term in negative_terms):
            continue
        if news.get("direction") != "positive":
            continue
        impact = _to_float(news.get("impact_score"), 0) or 0
        timeliness = _to_float(news.get("timeliness_score"), 0) or 0
        if impact >= min_impact and timeliness >= min_timeliness:
            return True
    return False


def _has_actionable_stock_news(item: Dict[str, Any], min_timeliness: float = 70.0) -> bool:
    negative_terms = ("跌停", "跌超", "下挫", "跳水", "杀跌", "大跌", "冲高回落", "风险", "减持", "处罚")
    for news in item.get("opinion_stock_news") or []:
        title = str(news.get("title") or "")
        if news.get("direction") != "positive" or any(term in title for term in negative_terms):
            continue
        timeliness = _to_float(news.get("timeliness_score"), 0) or 0
        signed_score = _to_float(news.get("signed_score"), 0) or 0
        if (_to_float(news.get("impact_score"), 0) or 0) > 0 and signed_score > 0 and timeliness >= min_timeliness:
            return True
    return False


def _trade_signal_state(
    price_signal: float,
    high_drawdown: float | None,
    open_drawdown: float | None,
    intraday_low_pct: float | None,
    intraday_amplitude_pct: float | None,
    net_flow_intensity: float,
    match_type: str | None,
    deep_v_low_pct: float = -8.0,
    max_tradable_amplitude_pct: float = 10.0,
) -> Dict[str, str]:
    if intraday_low_pct is not None and intraday_low_pct <= deep_v_low_pct:
        return {"state": "watch", "label": "深V高波动观察", "reason": "盘中曾深跌，反抽不等于风险解除"}
    if intraday_amplitude_pct is not None and intraday_amplitude_pct >= max_tradable_amplitude_pct:
        return {"state": "watch", "label": "高振幅观察", "reason": "日内振幅过大，不按普通强势股追价"}
    if price_signal >= 2 and (high_drawdown is None or high_drawdown >= -2.5):
        return {"state": "tradable", "label": "强势可交易", "reason": "盘中价格确认较强"}
    if price_signal >= 0 and net_flow_intensity >= 1:
        return {"state": "tradable", "label": "强势可交易", "reason": "红盘且实时资金净流入"}
    if price_signal < -5 and net_flow_intensity <= 0:
        return {"state": "weak", "label": "走势未确认", "reason": "盘中跌幅较深且资金未确认"}
    if price_signal < -3:
        return {"state": "watch", "label": "水下观察", "reason": "舆情较强但盘中仍在水下"}
    if price_signal < 0:
        return {"state": "watch", "label": "水下观察", "reason": "盘中未翻红，等待承接确认"}
    if match_type == "sector_candidate":
        return {"state": "watch", "label": "舆情观察", "reason": "板块候选，缺少个股直接确认"}
    if open_drawdown is not None and open_drawdown < -2:
        return {"state": "watch", "label": "舆情观察", "reason": "开盘后承接一般"}
    return {"state": "tradable", "label": "强势可交易", "reason": "舆情与交易确认匹配"}


def _pct_distance(value: float | None, anchor: float | None) -> float | None:
    if value is None or anchor is None or anchor <= 0:
        return None
    return (value - anchor) / anchor * 100


def _sentiment_market_structure(item: Dict[str, Any], price_signal: float) -> Dict[str, Any]:
    """Score daily trend, volume and chip structure without filtering candidates."""

    reference_price = _to_float(item.get("realtime_price"), None) or _to_float(item.get("close"), None)
    ma5 = _to_float(item.get("ma5"), None)
    ma10 = _to_float(item.get("ma10"), None)
    ma20 = _to_float(item.get("ma20"), None)
    ma30 = _to_float(item.get("ma30"), None)

    ma5_distance = _pct_distance(reference_price, ma5)
    ma20_distance = _pct_distance(reference_price, ma20)
    ma30_distance = _pct_distance(reference_price, ma30)
    ma5_ma10_spread = _pct_distance(ma5, ma10)
    ma20_ma30_spread = _pct_distance(ma20, ma30)

    daily_trend = 50.0
    if ma20_distance is not None:
        daily_trend += max(-22.0, min(ma20_distance * 2.0, 18.0))
    if ma5_ma10_spread is not None:
        daily_trend += max(-15.0, min(ma5_ma10_spread * 4.0, 15.0))
    if ma20_ma30_spread is not None:
        daily_trend += max(-10.0, min(ma20_ma30_spread * 2.0, 10.0))
    if ma5_distance is not None:
        if -1.0 <= ma5_distance <= 5.0:
            daily_trend += 8.0
        elif ma5_distance < -1.0:
            daily_trend += max(ma5_distance * 2.0, -12.0)
        elif ma5_distance > 8.0:
            daily_trend -= min((ma5_distance - 8.0) * 2.0, 15.0)
    daily_trend = _clamp(daily_trend)

    if ma5_distance is not None and ma5_distance > 8.0:
        daily_state = "extended"
        daily_label = "日线偏离过大"
    elif (
        reference_price is not None
        and ma20 is not None
        and reference_price >= ma20
        and ma5 is not None
        and ma10 is not None
        and ma5 >= ma10
        and (ma30 is None or ma20 >= ma30)
    ):
        daily_state = "confirmed"
        daily_label = "日线趋势确认"
    elif reference_price is not None and ma20 is not None and ma5 is not None and reference_price >= ma20 and reference_price >= ma5:
        daily_state = "breakout"
        daily_label = "日线突破修复"
    elif reference_price is not None and ma20 is not None and ma5 is not None and ma10 is not None and reference_price < ma20 and ma5 < ma10:
        daily_state = "weak"
        daily_label = "日线趋势偏弱"
    else:
        daily_state = "neutral"
        daily_label = "日线结构中性"

    amount = _to_float(item.get("amount"), None)
    avg_amount_5 = _to_float(item.get("avg_amount_5"), None)
    avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
    amount_ratio_5d = amount / avg_amount_5 if amount is not None and avg_amount_5 and avg_amount_5 > 0 else None
    amount_ratio_20d = amount / avg_amount_20 if amount is not None and avg_amount_20 and avg_amount_20 > 0 else None
    volume_ratio = _to_float(item.get("volume_ratio"), None)
    realtime_amount_ratio = _to_float(item.get("realtime_amount_ratio"), None)
    volume_confirm = _clamp(
        _score_peak(volume_ratio, 1.5, 1.4, 30) * 0.50
        + _score_peak(amount_ratio_20d, 1.35, 1.2, 35) * 0.30
        + _score_peak(amount_ratio_5d, 1.20, 1.0, 40) * 0.20
    )
    if amount_ratio_20d is not None and amount_ratio_20d >= 1.1 and price_signal >= 1.0:
        volume_confirm = _clamp(volume_confirm + 5.0)
    if realtime_amount_ratio is not None and realtime_amount_ratio >= 1.5 and price_signal <= 2.0:
        volume_confirm = _clamp(volume_confirm - min((realtime_amount_ratio - 1.5) * 22 + (2.0 - price_signal) * 8, 45))

    cost50 = _to_float(item.get("chip_cost_50pct"), None)
    weight_avg = _to_float(item.get("chip_weight_avg"), None)
    chip_center = cost50 or weight_avg
    cost15 = _to_float(item.get("chip_cost_15pct"), None)
    cost85 = _to_float(item.get("chip_cost_85pct"), None)
    winner_rate = _to_float(item.get("chip_winner_rate"), None)
    chip_center_distance = _pct_distance(reference_price, chip_center)
    chip_upper_distance = _pct_distance(reference_price, cost85)
    chip_band_width = (
        (cost85 - cost15) / chip_center * 100
        if cost85 is not None and cost15 is not None and chip_center is not None and chip_center > 0 and cost85 >= cost15
        else None
    )
    chip_structure = _clamp(
        _score_peak(chip_center_distance, 3.0, 12.0, 20) * 0.40
        + _score_peak(winner_rate, 55.0, 45.0, 30) * 0.25
        + _score_peak(chip_band_width, 18.0, 22.0, 30) * 0.20
        + _score_peak(chip_upper_distance, 0.0, 12.0, 35) * 0.15
    )
    if chip_center is None and winner_rate is None and chip_band_width is None:
        chip_state = "unavailable"
        chip_label = "筹码数据待补"
    elif chip_center_distance is not None and chip_center_distance < -8.0:
        chip_state = "below_cost"
        chip_label = "筹码中枢下方"
    elif (chip_center_distance is not None and chip_center_distance > 15.0) or (winner_rate is not None and winner_rate >= 85.0):
        chip_state = "overheated"
        chip_label = "获利盘压力偏高"
    elif (
        chip_center_distance is not None
        and -3.0 <= chip_center_distance <= 8.0
        and (chip_band_width is None or chip_band_width <= 30.0)
    ):
        chip_state = "supportive"
        chip_label = "筹码结构有支撑"
    else:
        chip_state = "neutral"
        chip_label = "筹码结构中性"

    reasons: List[str] = []
    risks: List[str] = []
    if daily_state in {"confirmed", "breakout"}:
        reasons.append(f"{daily_label}，日线结构对舆情形成价格确认")
    elif daily_state == "weak":
        risks.append("日线趋势偏弱，本次仅在综合评分中降权，不作硬过滤")
    elif daily_state == "extended":
        risks.append("价格偏离 MA5 较远，综合评分计入追高风险")
    if volume_confirm >= 65:
        reasons.append("量能相对近期均值形成确认")
    elif volume_confirm <= 35:
        risks.append("量能确认偏弱，综合评分相应降低")
    if chip_state == "supportive":
        reasons.append("现价与筹码成本区匹配，筹码结构提供一定支撑")
    elif chip_state in {"below_cost", "overheated"}:
        risks.append(f"{chip_label}，筹码结构在综合评分中降权")

    return {
        "factors": {
            "daily_trend": daily_trend,
            "volume_confirm": volume_confirm,
            "chip_structure": chip_structure,
        },
        "raw_metrics": {
            "daily_trend_state": daily_state,
            "daily_trend_label": daily_label,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma30": ma30,
            "ma5_distance_pct": round(ma5_distance, 4) if ma5_distance is not None else None,
            "ma20_distance_pct": round(ma20_distance, 4) if ma20_distance is not None else None,
            "ma30_distance_pct": round(ma30_distance, 4) if ma30_distance is not None else None,
            "ma5_ma10_spread_pct": round(ma5_ma10_spread, 4) if ma5_ma10_spread is not None else None,
            "ma20_ma30_spread_pct": round(ma20_ma30_spread, 4) if ma20_ma30_spread is not None else None,
            "amount_ratio_5d": round(amount_ratio_5d, 4) if amount_ratio_5d is not None else None,
            "amount_ratio_20d": round(amount_ratio_20d, 4) if amount_ratio_20d is not None else None,
            "chip_structure_state": chip_state,
            "chip_structure_label": chip_label,
            "chip_center": chip_center,
            "chip_center_distance_pct": round(chip_center_distance, 4) if chip_center_distance is not None else None,
            "chip_upper_distance_pct": round(chip_upper_distance, 4) if chip_upper_distance is not None else None,
            "chip_band_width_pct": round(chip_band_width, 4) if chip_band_width is not None else None,
            "chip_winner_rate": winner_rate,
        },
        "reasons": reasons,
        "risks": risks,
    }


def _sentiment_market_context(item: Dict[str, Any]) -> Dict[str, Any]:
    """Build a soft market score from broad-index and whole-market signals."""

    components = [
        ("index_trend", _to_float(item.get("market_index_trend_score"), None), 0.40),
        ("index_day", _to_float(item.get("market_index_day_score"), None), 0.20),
        ("breadth", _to_float(item.get("market_breadth_score"), None), 0.25),
        ("volume", _to_float(item.get("market_volume_score"), None), 0.15),
    ]
    available = [(name, value, weight) for name, value, weight in components if value is not None]
    if available:
        available_weight = sum(weight for _, _, weight in available) or 1.0
        score = _clamp(sum(float(value) * weight for _, value, weight in available) / available_weight)
    else:
        score = _clamp(_to_float(item.get("market_strength"), 50.0) or 50.0)

    index_count = int(_to_float(item.get("market_index_count"), 0) or 0)
    index_codes = str(item.get("market_index_codes") or "")
    if score >= 65:
        state = "strong"
        label = "大盘指数偏强"
    elif score >= 55:
        state = "supportive"
        label = "大盘环境有支撑"
    elif score <= 35:
        state = "weak"
        label = "大盘指数偏弱"
    elif score <= 45:
        state = "pressured"
        label = "大盘环境承压"
    else:
        state = "neutral"
        label = "大盘环境中性"

    index_pct = _to_float(item.get("market_index_pct_chg"), None)
    if index_count >= 3:
        coverage_text = "沪深300、中证500和中证1000"
    elif index_count:
        coverage_text = f"{index_count}个宽基指数"
    else:
        coverage_text = "现有市场环境数据"
    reason = f"{coverage_text}与全A涨跌宽度共同评估，市场环境分 {score:.1f}"
    if index_pct is not None:
        reason += f"，宽基指数平均涨跌 {index_pct:+.2f}%"

    reasons: List[str] = []
    risks: List[str] = []
    if state in {"strong", "supportive"}:
        reasons.append(f"{label}，题材交易的市场背景较好")
    elif state in {"weak", "pressured"}:
        risks.append(f"{label}，宽基指数与市场宽度对短线题材形成压制")

    return {
        "score": score,
        "state": state,
        "label": label,
        "reason": reason,
        "reasons": reasons,
        "risks": risks,
        "raw_metrics": {
            "market_strength": item.get("market_strength"),
            "market_state": item.get("market_state"),
            "market_context_state": state,
            "market_context_label": label,
            "market_context_reason": reason,
            "market_index_trend_score": item.get("market_index_trend_score"),
            "market_index_day_score": item.get("market_index_day_score"),
            "market_index_pct_chg": item.get("market_index_pct_chg"),
            "market_breadth_score": item.get("market_breadth_score"),
            "market_volume_score": item.get("market_volume_score"),
            "market_index_count": index_count,
            "market_index_codes": index_codes or None,
            "csi300_pct_chg": item.get("csi300_pct_chg"),
            "csi500_pct_chg": item.get("csi500_pct_chg"),
            "csi1000_pct_chg": item.get("csi1000_pct_chg"),
        },
    }


class _ZScoreMixin:
    @staticmethod
    def _zscore_by_factor(stocks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        factor_keys = sorted({key for item in stocks for key in (item.get("factors") or {}).keys()})
        result: Dict[str, Dict[str, float]] = {key: {} for key in factor_keys}
        for key in factor_keys:
            values = [_to_float((item.get("factors") or {}).get(key), None) for item in stocks]
            valid = [v for v in values if v is not None]
            avg = mean(valid) if valid else 0
            std = pstdev(valid) if len(valid) > 1 else 1
            if not std:
                std = 1
            for item in stocks:
                code = str(item.get("code"))
                value = _to_float((item.get("factors") or {}).get(key), avg) or avg
                result[key][code] = max(-3.0, min(3.0, (value - avg) / std))
        return result

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        weights = self.config.get("weights", {}) or {}
        total_weight = sum(float(v or 0) for v in weights.values()) or 1.0
        normalized = {key: float(value or 0) / total_weight for key, value in weights.items()}
        zscores = self._zscore_by_factor(stocks)
        scored: List[Dict[str, Any]] = []
        for item in stocks:
            code = str(item.get("code"))
            weighted_z = sum(zscores.get(key, {}).get(code, 0) * weight for key, weight in normalized.items())
            # Keep the score readable and compatible with existing 60-point threshold.
            score = _clamp(62 + weighted_z * 12)
            scored.append({**item, "score": score, "weighted_z": round(weighted_z, 4)})
        return sorted(scored, key=lambda row: row.get("score", 0), reverse=True)

    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        threshold = float(self.config.get("score_threshold", 60) or 60)
        max_picks = int(self.config.get("max_picks", self.config.get("max_positions", 3)) or 3)
        return [item for item in scored_stocks if float(item.get("score", 0) or 0) >= threshold][:max_picks]

    def explain(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "code": stock.get("code"),
            "score": stock.get("score"),
            "factors": stock.get("factors", {}),
            "strategy": self.strategy_id,
            "notes": stock.get("strategy_notes", []),
            "raw_metrics": stock.get("strategy_raw_metrics", {}),
        }



class AShareSentimentStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """A股舆情选股 V2：先找热点板块/主题，再在板块内用交易确认选股。"""

    strategy_id = "a_share_sentiment"

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        weights = self.config.get("weights", {}) or {}
        total_weight = sum(float(v or 0) for v in weights.values()) or 1.0
        scored: List[Dict[str, Any]] = []
        for item in stocks:
            factors = item.get("factors") or {}
            base_score = sum(
                (50.0 if _to_float(factors.get(key), None) is None else (_to_float(factors.get(key), None) or 0.0)) * float(weight or 0)
                for key, weight in weights.items()
            ) / total_weight

            price_confirm = _to_float(factors.get("price_confirm"), 50.0)
            if price_confirm is None:
                price_confirm = 50.0
            intraday_confirm = _to_float(factors.get("intraday_confirm"), 50.0)
            if intraday_confirm is None:
                intraday_confirm = 50.0
            volume_confirm = _to_float(factors.get("volume_confirm"), 50.0)
            if volume_confirm is None:
                volume_confirm = 50.0
            penalty_multiplier = 1.0
            penalty_reasons: List[str] = []
            if price_confirm < 20 and intraday_confirm < 20:
                penalty_multiplier *= 0.85
                penalty_reasons.append("price_intraday_weak")
            if intraday_confirm < 25 and volume_confirm < 45:
                penalty_multiplier *= 0.90
                penalty_reasons.append("intraday_volume_weak")
            trade_signal_state = (item.get("strategy_raw_metrics") or {}).get("trade_signal_state")
            if trade_signal_state == "watch":
                penalty_multiplier *= 0.94
                penalty_reasons.append("watch_candidate")
            elif trade_signal_state == "weak":
                penalty_multiplier *= 0.86
                penalty_reasons.append("weak_trade_signal")

            raw_metrics = item.get("strategy_raw_metrics") or {}
            theme_delta = _to_float(raw_metrics.get("market_theme_score_delta"), 0.0) or 0.0
            score = base_score * penalty_multiplier + theme_delta
            scored.append({
                **item,
                "score": _clamp(score),
                "base_score": round(base_score, 4),
                "technical_penalty_multiplier": round(penalty_multiplier, 4),
                "technical_penalty_reasons": penalty_reasons,
                "market_theme_score_delta_applied": round(theme_delta, 4),
            })
        return sorted(scored, key=lambda row: row.get("score", 0), reverse=True)

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_news_count = int(filters.get("min_news_count") or 1)
        min_amount = _to_float(filters.get("min_amount"), 50_000_000) or 50_000_000
        min_sector_score = _to_float(filters.get("min_sector_score"), 45) or 45
        min_price = _to_float(filters.get("min_price"), 3) or 3
        max_price = _to_float(filters.get("max_price"), None)
        max_total_mv = _to_float(filters.get("max_total_mv"), 30_000_000) or 30_000_000  # 万元，约3000亿
        require_direct_stock_news = bool(filters.get("require_direct_stock_news", True))
        hard_realtime_loss_pct = _to_float(filters.get("hard_realtime_loss_pct"), -5.0)
        soft_realtime_loss_pct = _to_float(filters.get("soft_realtime_loss_pct"), -3.0)
        strong_news_min_impact = _to_float(filters.get("strong_news_min_impact"), 75.0) or 75.0
        min_stock_recognition = _to_float(filters.get("min_stock_recognition"), None)
        min_direct_stock_signed_score = _to_float(filters.get("min_direct_stock_signed_score"), 1.0) or 1.0
        min_actionable_news_timeliness = _to_float(filters.get("min_actionable_news_timeliness"), 70.0) or 70.0
        min_roe = _to_float(filters.get("min_roe"), None)
        use_market_opinion = any(item.get("opinion_sector_score") is not None for item in data_bundle.get("candidates", []))
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            if close is None or close < min_price:
                continue
            if max_price is not None and close > max_price:
                continue
            total_mv = _to_float(item.get("total_mv"), None)
            if total_mv is not None and total_mv > max_total_mv:
                continue
            realtime_pct = _to_float(item.get("realtime_pct_chg"), None)
            if hard_realtime_loss_pct is not None and realtime_pct is not None and realtime_pct <= hard_realtime_loss_pct:
                continue
            if (
                soft_realtime_loss_pct is not None
                and realtime_pct is not None
                and realtime_pct <= soft_realtime_loss_pct
                and not _has_strong_stock_news(item, min_impact=strong_news_min_impact)
            ):
                continue
            roe = _to_float(item.get("roe"), None)
            if min_roe is not None and roe is not None and roe < min_roe:
                continue
            amount = _to_float(item.get("amount"), None)
            if amount is None or amount < min_amount:
                continue
            if use_market_opinion:
                sector_score = _to_float(item.get("opinion_sector_score"), None)
                if sector_score is None or sector_score < min_sector_score:
                    continue
                recognition_score = _to_float(item.get("opinion_stock_recognition_score"), None)
                if (
                    min_stock_recognition is not None
                    and item.get("opinion_match_type") == "sector_candidate"
                    and recognition_score is not None
                    and recognition_score < min_stock_recognition
                ):
                    continue
                if item.get("opinion_match_type") == "direct_news_match" and not _has_actionable_stock_news(
                    item,
                    min_timeliness=min_actionable_news_timeliness,
                ):
                    continue
                if (
                    item.get("opinion_match_type") == "direct_news_match"
                    and (_to_float(item.get("opinion_stock_score"), 0.0) or 0.0) < min_direct_stock_signed_score
                ):
                    continue
                if require_direct_stock_news and (
                    item.get("opinion_match_type") != "direct_news_match"
                    or not item.get("opinion_stock_news")
                ):
                    continue
                if require_direct_stock_news and not _has_actionable_stock_news(
                    item,
                    min_timeliness=min_actionable_news_timeliness,
                ):
                    continue
            else:
                news_count = int(item.get("news_count") or 0)
                if news_count < min_news_count and item.get("sentiment_score") is None:
                    continue
            filtered.append(item)
        return {
            **data_bundle,
            "candidates": filtered,
            "sentiment_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(filtered),
                "mode": "market_opinion_v2" if use_market_opinion else "stock_sentiment_fallback",
                "min_sector_score": min_sector_score if use_market_opinion else None,
                "min_price": min_price,
                "max_price": max_price,
                "max_total_mv": max_total_mv,
                "require_direct_stock_news": require_direct_stock_news if use_market_opinion else None,
                "hard_realtime_loss_pct": hard_realtime_loss_pct,
                "soft_realtime_loss_pct": soft_realtime_loss_pct,
                "strong_news_min_impact": strong_news_min_impact,
                "min_stock_recognition": min_stock_recognition if use_market_opinion else None,
                "min_direct_stock_signed_score": min_direct_stock_signed_score if use_market_opinion else None,
                "min_actionable_news_timeliness": min_actionable_news_timeliness if use_market_opinion else None,
                "min_roe": min_roe,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters = self.config.get("hard_filters", {}) or {}
        deep_v_low_pct = _to_float(filters.get("deep_v_low_pct"), -8.0)
        max_tradable_amplitude_pct = _to_float(filters.get("max_tradable_intraday_amplitude_pct"), 10.0)
        if deep_v_low_pct is None:
            deep_v_low_pct = -8.0
        if max_tradable_amplitude_pct is None:
            max_tradable_amplitude_pct = 10.0
        rows = []
        for item in data_bundle.get("candidates", []):
            amount = _to_float(item.get("amount"), 0) or 0
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0
            buy_lg = _to_float(item.get("buy_lg_amount"), 0) or 0
            sell_lg = _to_float(item.get("sell_lg_amount"), 0) or 0
            buy_elg = _to_float(item.get("buy_elg_amount"), 0) or 0
            sell_elg = _to_float(item.get("sell_elg_amount"), 0) or 0
            pct1 = _to_float(item.get("pct_chg_1d"), 0) or 0
            realtime_pct = _to_float(item.get("realtime_pct_chg"), None)
            high_drawdown = _to_float(item.get("intraday_high_drawdown_pct"), None)
            open_drawdown = _to_float(item.get("intraday_open_drawdown_pct"), None)
            intraday_low_pct = _to_float(item.get("intraday_low_pct"), None)
            intraday_amplitude_pct = _to_float(item.get("intraday_amplitude_pct"), None)
            intraday_repair_pct = _to_float(item.get("intraday_repair_pct"), None)
            amount_ratio = _to_float(item.get("realtime_amount_ratio"), None)
            price_signal = realtime_pct if realtime_pct is not None else pct1
            market_structure = _sentiment_market_structure(item, price_signal)
            daily_trend = market_structure["factors"]["daily_trend"]
            volume_confirm = market_structure["factors"]["volume_confirm"]
            chip_structure = market_structure["factors"]["chip_structure"]
            market_context = _sentiment_market_context(item)
            realtime_mf_net = _to_float(item.get("realtime_mf_net"), None)
            realtime_mf_amount = _to_float(item.get("realtime_mf_amount"), None)
            realtime_mf_quote_time = item.get("realtime_mf_quote_time")
            popularity_score = _to_float(item.get("popularity_score"), None)
            popularity_rank = item.get("popularity_rank")
            attention_amount = _to_float(item.get("realtime_amount"), None) or amount
            amount_attention = _clamp(45 + min(math.log10(attention_amount / 100_000_000 + 1) * 18, 45)) if attention_amount and attention_amount > 0 else 50.0
            if popularity_score is not None:
                popularity_heat = _clamp(popularity_score * 0.68 + amount_attention * 0.32)
            else:
                popularity_heat = _clamp(amount_attention)
            if realtime_mf_net is not None:
                flow_amount = realtime_mf_amount or _to_float(item.get("realtime_amount"), None) or amount
                net_flow_intensity = (realtime_mf_net / flow_amount * 100) if flow_amount and flow_amount > 0 else 0
                fund_flow_source = "ths_realtime"
            else:
                net_flow_intensity = (net_mf * 10000 / amount * 100) if amount > 0 else 0
                fund_flow_source = "tushare_daily"
            large_net_amount = (buy_lg + buy_elg) - (sell_lg + sell_elg)
            large_flow_intensity = (large_net_amount * 10000 / amount * 100) if amount > 0 else 0
            fund_flow_score = _clamp(50 + net_flow_intensity * 1.6 + large_flow_intensity * 0.6)
            large_flow_signal = (
                "large_inflow"
                if large_flow_intensity >= 2
                else "large_outflow"
                if large_flow_intensity <= -2
                else "neutral"
            )

            sector_score = _to_float(item.get("opinion_sector_score"), None)
            if sector_score is not None:
                news_count = int(item.get("opinion_news_count") or 0)
                source_count = int(item.get("opinion_source_count") or 0)
                positive_count = int(item.get("opinion_positive_news_count") or 0)
                negative_count = int(item.get("opinion_negative_news_count") or 0)
                weighted_impact = _to_float(item.get("opinion_weighted_impact_score"), 50) or 50
                stock_score = _to_float(item.get("opinion_stock_score"), 50) or 50
                match_type = item.get("opinion_match_type")
                if match_type == "sector_candidate":
                    stock_score = min(stock_score, 72.0)
                recognition_score = _to_float(item.get("opinion_stock_recognition_score"), None)
                if recognition_score is None:
                    recognition_score = stock_score
                if match_type == "direct_news_match":
                    recognition_score = min(100.0, recognition_score + 4.0)
                elif match_type == "sector_candidate":
                    recognition_score = min(recognition_score, 78.0)
                source_rating = score_sources(item.get("opinion_sources") or [])
                price_confirm = 52 + price_signal * 4 - max(price_signal - 7, 0) * 7
                if high_drawdown is not None and high_drawdown < 0:
                    price_confirm -= min(abs(high_drawdown) * 4.5, 55)
                if open_drawdown is not None and open_drawdown < 0:
                    price_confirm -= min(abs(open_drawdown) * 2.5, 25)
                if intraday_low_pct is not None and intraday_low_pct <= -5:
                    price_confirm -= min(abs(intraday_low_pct) * 2.5, 30)
                if intraday_amplitude_pct is not None and intraday_amplitude_pct >= 8:
                    price_confirm -= min((intraday_amplitude_pct - 8) * 4.0 + 8, 30)
                intraday_confirm = 72.0
                if high_drawdown is not None and high_drawdown < 0:
                    intraday_confirm -= min(abs(high_drawdown) * 4.0, 50)
                if open_drawdown is not None and open_drawdown < 0:
                    intraday_confirm -= min(abs(open_drawdown) * 3.0, 30)
                if amount_ratio is not None and amount_ratio >= 1.5 and price_signal <= 2:
                    intraday_confirm -= min((amount_ratio - 1.5) * 20 + (2 - price_signal) * 8, 35)
                if intraday_low_pct is not None and intraday_low_pct <= -5:
                    intraday_confirm -= min(abs(intraday_low_pct) * 3.0, 35)
                if intraday_amplitude_pct is not None and intraday_amplitude_pct >= 8:
                    intraday_confirm -= min((intraday_amplitude_pct - 8) * 5.0 + 10, 35)
                trade_signal = _trade_signal_state(
                    price_signal=price_signal,
                    high_drawdown=high_drawdown,
                    open_drawdown=open_drawdown,
                    intraday_low_pct=intraday_low_pct,
                    intraday_amplitude_pct=intraday_amplitude_pct,
                    net_flow_intensity=net_flow_intensity,
                    match_type=match_type,
                    deep_v_low_pct=deep_v_low_pct,
                    max_tradable_amplitude_pct=max_tradable_amplitude_pct,
                )
                theme_tier = item.get("market_theme_tier") or "unknown"
                theme_label = item.get("market_theme_label") or "未分层"
                theme_delta = _to_float(item.get("market_theme_score_delta"), 0.0) or 0.0
                theme_alignment = {
                    "mainline": 92.0,
                    "strong_side": 74.0,
                    "side": 58.0,
                    "broad_related": 40.0,
                    "unknown": 50.0,
                }.get(theme_tier, 50.0)
                if match_type == "sector_candidate":
                    theme_alignment = max(0.0, theme_alignment - 8.0)
                if trade_signal["state"] == "watch":
                    price_confirm -= 8
                    intraday_confirm -= 7
                elif trade_signal["state"] == "weak":
                    price_confirm -= 16
                    intraday_confirm -= 14
                factors = {
                    "sector_heat": _clamp(sector_score),
                    "source_credibility": _clamp(source_rating["credibility_score"] * 100 + min(source_count, 8) * 2 + positive_count - negative_count * 3),
                    "info_importance": _clamp(weighted_impact),
                    "amplification": _clamp(35 + min(news_count, 12) * 5),
                    "stock_match": _clamp(stock_score),
                    "stock_recognition": _clamp(recognition_score),
                    "popularity_heat": popularity_heat,
                    "fund_flow": fund_flow_score,
                    "daily_trend": daily_trend,
                    "chip_structure": chip_structure,
                    "price_confirm": _clamp(price_confirm),
                    "volume_confirm": _clamp(volume_confirm),
                    "intraday_confirm": _clamp(intraday_confirm),
                    "market_theme": _clamp(theme_alignment),
                    "market_context": market_context["score"],
                }
                notes = [
                    "先按 NewsNow/RSS/AkShare 热点聚合识别板块/主题，再在热点内选股",
                    "资金、价格、成交量只做交易确认，避免旧版资金/技术因子反客为主",
                ]
                raw_metrics = {
                    "sentiment_mode": "market_opinion_v2",
                    "opinion_sector_type": item.get("opinion_sector_type"),
                    "opinion_sector_name": item.get("opinion_sector_name"),
                    "opinion_as_of_datetime": item.get("opinion_as_of_datetime"),
                    "opinion_sector_score": sector_score,
                    "opinion_weighted_impact_score": weighted_impact,
                    "opinion_news_count": news_count,
                    "opinion_source_count": source_count,
                    "opinion_positive_news_count": positive_count,
                    "opinion_negative_news_count": negative_count,
                    "opinion_stock_score": stock_score,
                    "opinion_stock_rank": item.get("opinion_stock_rank"),
                    "opinion_stock_pool_size": item.get("opinion_stock_pool_size"),
                    "opinion_stock_recognition_score": _clamp(recognition_score),
                    "opinion_stock_recognition_label": item.get("opinion_stock_recognition_label"),
                    "opinion_stock_recognition_reason": item.get("opinion_stock_recognition_reason"),
                    "opinion_stock_pct_chg": item.get("opinion_stock_pct_chg"),
                    "opinion_stock_amount": item.get("opinion_stock_amount"),
                    "opinion_match_type": match_type,
                    "opinion_match_reason": item.get("opinion_match_reason"),
                    "opinion_stock_news": item.get("opinion_stock_news") or [],
                    "opinion_top_news": item.get("opinion_top_news") or [],
                    "opinion_sector_top_news": item.get("opinion_sector_top_news") or [],
                    "opinion_sources": item.get("opinion_sources") or [],
                    "source_credibility_level": source_rating.get("credibility_level"),
                    "source_credibility_score": source_rating.get("credibility_score"),
                    "source_credibility_reason": source_rating.get("credibility_reason"),
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "large_net_amount": round(large_net_amount, 4),
                    "large_flow_intensity_pct": round(large_flow_intensity, 4),
                    "large_flow_signal": large_flow_signal,
                    "fund_flow_source": fund_flow_source,
                    "realtime_mf_net": realtime_mf_net,
                    "realtime_mf_amount": realtime_mf_amount,
                    "realtime_mf_quote_time": realtime_mf_quote_time,
                    "popularity_source": item.get("popularity_source"),
                    "popularity_rank": popularity_rank,
                    "popularity_source_score": item.get("popularity_source_score"),
                    "popularity_score": popularity_score,
                    "popularity_heat": popularity_heat,
                    "popularity_quote_time": item.get("popularity_quote_time"),
                    "amount_attention_score": amount_attention,
                    "pct_chg_1d": pct1,
                    "realtime_pct_chg": realtime_pct,
                    "intraday_high_drawdown_pct": high_drawdown,
                    "intraday_open_drawdown_pct": open_drawdown,
                    "intraday_low_pct": intraday_low_pct,
                    "intraday_amplitude_pct": intraday_amplitude_pct,
                    "intraday_repair_pct": intraday_repair_pct,
                    "realtime_amount_ratio": amount_ratio,
                    "price_signal_pct": price_signal,
                    "trade_signal_state": trade_signal["state"],
                    "trade_signal_label": trade_signal["label"],
                    "trade_signal_reason": trade_signal["reason"],
                    "market_theme_tier": theme_tier,
                    "market_theme_label": theme_label,
                    "market_theme_trend_score": item.get("market_theme_trend_score"),
                    "market_theme_rank": item.get("market_theme_rank"),
                    "market_theme_score_delta": round(theme_delta, 4),
                    "market_theme_match_adjustment": item.get("market_theme_match_adjustment"),
                    "market_theme_reason": item.get("market_theme_reason"),
                    "market_theme_fund_flow": item.get("market_theme_fund_flow"),
                    **market_context["raw_metrics"],
                    **market_structure["raw_metrics"],
                }
                notes.extend(market_context["reasons"])
                notes.append(f"主题层级：{theme_label}，{item.get('market_theme_reason') or '暂无主线分层原因'}")
                if item.get("opinion_stock_recognition_label"):
                    notes.append(f"板块辨识度：{item.get('opinion_stock_recognition_label')}，{item.get('opinion_stock_recognition_reason')}")
                if popularity_rank:
                    notes.append(f"个股人气：{item.get('popularity_source') or '热度榜'} 第 {popularity_rank} 名")
                if large_flow_signal == "large_inflow":
                    notes.append("大单/超大单资金小幅确认")
                if trade_signal["state"] == "tradable":
                    notes.append(f"交易状态：{trade_signal['label']}，{trade_signal['reason']}")
                else:
                    notes.append(f"交易状态：{trade_signal['label']}，建议先观察确认")
            else:
                sentiment = _sentiment_0_100(item.get("sentiment_score"))
                news_count = int(item.get("news_count") or 0)
                news_heat = _clamp(35 + min(news_count, 8) * 8)
                factors = {
                    "sector_heat": _clamp(sentiment * 0.65 + news_heat * 0.35),
                    "source_credibility": _clamp(45 + min(news_count, 6) * 5),
                    "info_importance": sentiment,
                    "amplification": news_heat,
                    "stock_match": sentiment,
                    "stock_recognition": 50.0,
                    "popularity_heat": popularity_heat,
                    "fund_flow": fund_flow_score,
                    "daily_trend": daily_trend,
                    "chip_structure": chip_structure,
                    "price_confirm": _clamp(52 + pct1 * 4 - max(pct1 - 7, 0) * 7),
                    "volume_confirm": volume_confirm,
                    "market_context": market_context["score"],
                }
                notes = ["未取到有效板块舆情聚合时，回退到旧版个股舆情缓存，但仍使用新版舆情主导权重口径"]
                raw_metrics = {
                    "sentiment_mode": "stock_sentiment_fallback",
                    "sentiment_score": item.get("sentiment_score"),
                    "news_count": news_count,
                    "fallback_sector_heat": factors["sector_heat"],
                    "fallback_amplification": factors["amplification"],
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "large_net_amount": round(large_net_amount, 4),
                    "large_flow_intensity_pct": round(large_flow_intensity, 4),
                    "large_flow_signal": large_flow_signal,
                    "fund_flow_source": fund_flow_source,
                    "realtime_mf_net": realtime_mf_net,
                    "realtime_mf_amount": realtime_mf_amount,
                    "realtime_mf_quote_time": realtime_mf_quote_time,
                    "popularity_source": item.get("popularity_source"),
                    "popularity_rank": popularity_rank,
                    "popularity_source_score": item.get("popularity_source_score"),
                    "popularity_score": popularity_score,
                    "popularity_heat": popularity_heat,
                    "popularity_quote_time": item.get("popularity_quote_time"),
                    "amount_attention_score": amount_attention,
                    **market_context["raw_metrics"],
                    **market_structure["raw_metrics"],
                }

            candidate_risks = list(item.get("candidate_risks") or [])
            if raw_metrics.get("large_flow_signal") == "large_outflow":
                candidate_risks.append("舆情较强但大单/超大单资金偏流出")
            if raw_metrics.get("trade_signal_state") == "watch":
                candidate_risks.append(raw_metrics.get("trade_signal_reason") or "盘中交易确认不足")
            elif raw_metrics.get("trade_signal_state") == "weak":
                candidate_risks.append(raw_metrics.get("trade_signal_reason") or "走势未确认")
            candidate_risks.extend(market_structure["risks"])
            candidate_risks.extend(market_context["risks"])
            rows.append({
                **item,
                "factors": factors,
                "strategy_notes": notes,
                "strategy_raw_metrics": raw_metrics,
                "candidate_reasons": (item.get("candidate_reasons") or [])
                + [item.get("opinion_match_reason") or "舆情热度与交易确认共同筛选"]
                + market_structure["reasons"]
                + market_context["reasons"],
                "candidate_risks": candidate_risks,
            })
        return rows
