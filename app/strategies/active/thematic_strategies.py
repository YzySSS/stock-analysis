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
        if news.get("direction") == "negative":
            continue
        impact = _to_float(news.get("impact_score"), 0) or 0
        timeliness = _to_float(news.get("timeliness_score"), 0) or 0
        if impact >= min_impact and timeliness >= min_timeliness:
            return True
    return False


def _has_actionable_stock_news(item: Dict[str, Any]) -> bool:
    negative_terms = ("跌停", "跌超", "下挫", "跳水", "杀跌", "大跌", "冲高回落", "风险", "减持", "处罚")
    for news in item.get("opinion_stock_news") or []:
        title = str(news.get("title") or "")
        if news.get("direction") == "negative" or any(term in title for term in negative_terms):
            continue
        if (_to_float(news.get("impact_score"), 0) or 0) > 0:
            return True
    return False


def _trade_signal_state(
    price_signal: float,
    high_drawdown: float | None,
    open_drawdown: float | None,
    net_flow_intensity: float,
    match_type: str | None,
) -> Dict[str, str]:
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


class FundChipRepairStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """资金筹码修复选股：资金回流 + 筹码压力改善 + 不追高。"""

    strategy_id = "fund_chip_repair"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 3) or 3
        max_price = _to_float(filters.get("max_price"), 120) or 120
        min_amount = _to_float(filters.get("min_amount"), 40_000_000) or 40_000_000
        filtered = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            if item.get("is_st"):
                continue
            if close is None or close < min_price or close > max_price:
                continue
            if amount is None or amount < min_amount:
                continue
            if item.get("net_mf_amount") is None or item.get("chip_winner_rate") is None:
                continue
            filtered.append(item)
        return {**data_bundle, "candidates": filtered, "fund_chip_filter_summary": {"before": len(data_bundle.get("candidates", [])), "after": len(filtered)}}

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            close_5d = _to_float(item.get("close_5d"), None)
            ma20 = _to_float(item.get("ma20"), None)
            amount = _to_float(item.get("amount"), None)
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0  # Tushare moneyflow amount: 万元
            buy_lg = _to_float(item.get("buy_lg_amount"), 0) or 0
            sell_lg = _to_float(item.get("sell_lg_amount"), 0) or 0
            buy_elg = _to_float(item.get("buy_elg_amount"), 0) or 0
            sell_elg = _to_float(item.get("sell_elg_amount"), 0) or 0
            winner_rate = _to_float(item.get("chip_winner_rate"), None)
            cost50 = _to_float(item.get("chip_cost_50pct"), None)
            cost85 = _to_float(item.get("chip_cost_85pct"), None)
            turnover = _to_float(item.get("turnover_rate"), None)
            std20 = _to_float(item.get("std_return_20"), None)

            net_flow_intensity = (net_mf * 10000 / amount * 100) if amount and amount > 0 else 0
            large_net = (buy_lg + buy_elg) - (sell_lg + sell_elg)
            large_flow_intensity = (large_net * 10000 / amount * 100) if amount and amount > 0 else 0
            ret5 = ((close - close_5d) / close_5d * 100) if close and close_5d else 0
            cost50_distance = ((close - cost50) / cost50 * 100) if close and cost50 else 0
            cost85_distance = ((close - cost85) / cost85 * 100) if close and cost85 else 0
            ma20_distance = ((close - ma20) / ma20 * 100) if close and ma20 else 0

            factors = {
                "fund_flow": _clamp(50 + net_flow_intensity * 2.4 + large_flow_intensity * 1.2),
                "chip_repair": _clamp(_score_peak(winner_rate, 38, 32, 25) * 0.45 + _score_peak(cost50_distance, 2, 12, 20) * 0.35 + _score_peak(cost85_distance, -5, 16, 20) * 0.20),
                "pullback": _clamp(_score_peak(ret5, -2.5, 9, 25) * 0.65 + _score_peak(ma20_distance, 0, 10, 25) * 0.35),
                "liquidity": _score_peak(turnover, 2.2, 2.4, 30),
                "lowvol": _clamp(100 - (std20 or 0.02) * 2200),
            }
            rows.append({
                **item,
                "factors": factors,
                "strategy_notes": ["资金流与大单净流入改善", "筹码获利比例不过热，关注成本区修复", "偏好温和回撤后的资金回补"],
                "strategy_raw_metrics": {
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "large_flow_intensity_pct": round(large_flow_intensity, 4),
                    "return_5d_pct": round(ret5, 4),
                    "cost50_distance_pct": round(cost50_distance, 4),
                    "cost85_distance_pct": round(cost85_distance, 4),
                },
                "candidate_reasons": (item.get("candidate_reasons") or []) + ["资金/筹码修复特征较好"],
            })
        return rows


class QualityLowVolStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """质量低波选股：基本面质量 + 合理估值 + 低波动。"""

    strategy_id = "quality_lowvol"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_amount = _to_float(filters.get("min_amount"), 35_000_000) or 35_000_000
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            if close is None or close <= 0 or amount is None or amount < min_amount:
                continue
            if item.get("roe") is None or item.get("pb_tushare") is None:
                continue
            filtered.append(item)
        return {**data_bundle, "candidates": filtered, "quality_lowvol_filter_summary": {"before": len(data_bundle.get("candidates", [])), "after": len(filtered)}}

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            roe = _to_float(item.get("roe"), None)
            roa = _to_float(item.get("roa"), None)
            profit_yoy = _to_float(item.get("profit_yoy"), None)
            revenue_yoy = _to_float(item.get("revenue_yoy"), None)
            eps = _to_float(item.get("eps"), None)
            pe = _to_float(item.get("pe_tushare"), None)
            pb = _to_float(item.get("pb_tushare"), None)
            std20 = _to_float(item.get("std_return_20"), None)
            turnover = _to_float(item.get("turnover_rate"), None)
            total_mv = _to_float(item.get("total_mv"), None)  # 万元

            quality = _clamp((roe or 0) * 3.2 + (roa or 0) * 2.0 + max(profit_yoy or 0, 0) * 0.22 + max(revenue_yoy or 0, 0) * 0.12 + (12 if eps and eps > 0 else -8))
            value = 50.0
            if pe is not None:
                value += 25 if 0 < pe <= 22 else 12 if pe <= 40 else -20 if pe > 80 or pe <= 0 else 0
            if pb is not None:
                value += 20 if 0 < pb <= 2.2 else 8 if pb <= 4 else -15 if pb > 7 or pb <= 0 else 0
            lowvol = _clamp(100 - (std20 or 0.025) * 2400)
            liquidity = _score_peak(turnover, 1.8, 2.5, 35)
            size_stability = _clamp(35 + min((total_mv or 0) / 1_000_000, 35))

            rows.append({
                **item,
                "factors": {
                    "quality": quality,
                    "value": _clamp(value),
                    "lowvol": lowvol,
                    "liquidity": liquidity,
                    "size_stability": size_stability,
                },
                "strategy_notes": ["盈利质量与合理估值优先", "低波动和中等流动性作为防守约束"],
                "strategy_raw_metrics": {"roe": roe, "roa": roa, "profit_yoy": profit_yoy, "pe": pe, "pb": pb, "std_return_20": std20},
            })
        return rows


class LeaderTacticsStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """龙头战法选股：行业/题材强势、放量、资金流、规模与辨识度。"""

    strategy_id = "leader_tactics"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_amount = _to_float(filters.get("min_amount"), 120_000_000) or 120_000_000
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            if close is None or close < 3 or close > 180:
                continue
            if amount is None or amount < min_amount:
                continue
            filtered.append(item)
        return {**data_bundle, "candidates": filtered, "leader_filter_summary": {"before": len(data_bundle.get("candidates", [])), "after": len(filtered)}}

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            pct1 = _to_float(item.get("pct_chg_1d"), 0) or 0
            vol_ratio = _to_float(item.get("volume_ratio"), 1) or 1
            turnover = _to_float(item.get("turnover_rate"), None)
            amount = _to_float(item.get("amount"), 0) or 0
            total_mv = _to_float(item.get("total_mv"), 0) or 0
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0
            market_strength = _to_float(item.get("market_strength"), 50) or 50
            sentiment = _sentiment_0_100(item.get("sentiment_score"))
            close = _to_float(item.get("close"), None)
            ma20 = _to_float(item.get("ma20"), None)
            ma20_distance = ((close - ma20) / ma20 * 100) if close and ma20 else 0
            net_flow_intensity = (net_mf * 10000 / amount * 100) if amount > 0 else 0

            rows.append({
                **item,
                "factors": {
                    "strength": _clamp(50 + pct1 * 5 + ma20_distance * 1.2),
                    "volume_confirm": _score_peak(vol_ratio, 1.8, 1.6, 35),
                    "fund_flow": _clamp(50 + net_flow_intensity * 2.2),
                    "leadership": _clamp(40 + min(amount / 100_000_000, 12) * 4 + min(total_mv / 1_000_000, 8) * 2.5),
                    "sentiment": sentiment,
                    "market_heat": _clamp(market_strength),
                    "turnover": _score_peak(turnover, 3.0, 3.2, 25),
                },
                "strategy_notes": ["偏好市场关注度高、成交额足、资金确认的阶段性龙头", "避免无量上涨和极端过热换手"],
                "strategy_raw_metrics": {"pct_chg_1d": pct1, "volume_ratio": vol_ratio, "net_flow_intensity_pct": round(net_flow_intensity, 4), "industry": item.get("industry")},
            })
        return rows


class LimitUpReversalStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """连板分歧反包/情绪龙观察：只做观察池，不替代低波或舆情主策略。"""

    strategy_id = "limitup_reversal"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 2.0) or 2.0
        max_price = _to_float(filters.get("max_price"), 80.0) or 80.0
        min_realtime_pct = _to_float(filters.get("min_realtime_pct"), 5.0) or 5.0
        min_realtime_amount = _to_float(filters.get("min_realtime_amount"), 200_000_000) or 200_000_000
        min_prev_amount = _to_float(filters.get("min_prev_amount"), 150_000_000) or 150_000_000
        min_prev_divergence_pct = _to_float(filters.get("min_prev_divergence_pct"), 4.0) or 4.0
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            realtime_price = _to_float(item.get("realtime_price"), None)
            realtime_pct = _to_float(item.get("realtime_pct_chg"), None)
            realtime_amount = _to_float(item.get("realtime_amount"), None)
            prev_amount = _to_float(item.get("amount"), None)
            high = _to_float(item.get("high"), None)
            prev_close = close
            if close is None or close < min_price or close > max_price:
                continue
            if realtime_price is None or realtime_pct is None:
                continue
            if realtime_pct < min_realtime_pct:
                continue
            if realtime_amount is None or realtime_amount < min_realtime_amount:
                continue
            if prev_amount is None or prev_amount < min_prev_amount:
                continue
            prev_divergence = ((high - prev_close) / high * 100) if high and prev_close and high > 0 else 0.0
            if prev_divergence < min_prev_divergence_pct and not item.get("is_limit_up"):
                continue
            filtered.append(item)
        return {
            **data_bundle,
            "candidates": filtered,
            "limitup_reversal_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(filtered),
                "hard_filters": filters,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), 0) or 0
            open_price = _to_float(item.get("open"), None)
            high = _to_float(item.get("high"), None)
            low = _to_float(item.get("low"), None)
            prev_amount = _to_float(item.get("amount"), 0) or 0
            avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
            realtime_price = _to_float(item.get("realtime_price"), close) or close
            realtime_pct = _to_float(item.get("realtime_pct_chg"), 0) or 0
            realtime_open = _to_float(item.get("realtime_open"), None)
            realtime_low = _to_float(item.get("realtime_low"), None)
            realtime_amount = _to_float(item.get("realtime_amount"), prev_amount) or prev_amount
            turnover = _to_float(item.get("turnover_rate"), None)
            volume_ratio = _to_float(item.get("volume_ratio"), None)
            total_mv = _to_float(item.get("total_mv"), 0) or 0
            pb = _to_float(item.get("pb_tushare"), None)
            roe = _to_float(item.get("roe"), None)
            profit_yoy = _to_float(item.get("profit_yoy"), None)
            popularity = _to_float(item.get("popularity_score"), None)
            market_strength = _to_float(item.get("market_strength"), 50) or 50

            prev_divergence_pct = ((high - close) / high * 100) if high and close and high > 0 else 0.0
            prev_amplitude_pct = ((high - low) / close * 100) if high and low and close and close > 0 else 0.0
            amount_ratio_prev = (prev_amount / avg_amount_20) if avg_amount_20 and avg_amount_20 > 0 else None
            intraday_repair_pct = ((realtime_price - realtime_low) / realtime_low * 100) if realtime_price and realtime_low and realtime_low > 0 else 0.0
            open_gap_pct = ((realtime_open - close) / close * 100) if realtime_open and close and close > 0 else None

            divergence = _clamp(prev_divergence_pct * 5.0 + prev_amplitude_pct * 2.2 + min((amount_ratio_prev or 1), 8) * 7)
            reversal = _clamp(45 + realtime_pct * 4.2 + min(intraday_repair_pct, 12) * 2.8 + (8 if item.get("is_limit_up") else 0))
            identification = _clamp(
                35
                + min(realtime_amount / 100_000_000, 30) * 1.8
                + min(prev_amount / 100_000_000, 40) * 1.0
                + min(total_mv / 1_000_000, 15) * 1.2
            )
            turnover_heat = _score_peak(turnover, 28.0, 24.0, 35)
            volume_heat = _score_peak(volume_ratio, 3.0, 3.0, 35)
            sentiment_heat = _clamp((popularity if popularity is not None else 50) * 0.55 + market_strength * 0.45)
            risk_control = 72.0
            if roe is not None and roe < 3:
                risk_control -= 8
            if profit_yoy is not None and profit_yoy < -50:
                risk_control -= 8
            if pb is not None and pb > 8:
                risk_control -= 6
            if open_gap_pct is not None and open_gap_pct < -6:
                risk_control -= 6
            risk_control = _clamp(risk_control)

            reasons = [
                "情绪龙观察：优先看分歧后承接与反包强度",
                f"昨日分歧回落约 {prev_divergence_pct:.1f}%，今日涨幅 {realtime_pct:.1f}%",
            ]
            if item.get("is_limit_up"):
                reasons.append("当前触及涨停，符合反包观察条件")
            risks = []
            if turnover is not None and turnover >= 35:
                risks.append(f"换手 {turnover:.1f}% 偏高，情绪波动大")
            if profit_yoy is not None and profit_yoy < 0:
                risks.append(f"利润同比 {profit_yoy:.1f}%，基本面承接弱")
            rows.append({
                **item,
                "factors": {
                    "divergence": divergence,
                    "reversal_strength": reversal,
                    "recognition": identification,
                    "turnover_heat": _clamp(turnover_heat * 0.6 + volume_heat * 0.4),
                    "sentiment_heat": sentiment_heat,
                    "risk_control": risk_control,
                },
                "strategy_notes": ["仅作为情绪龙观察池，不代表低风险买点", "涨停票在本策略中允许保留"],
                "candidate_reasons": (item.get("candidate_reasons") or []) + reasons,
                "candidate_risks": (item.get("candidate_risks") or []) + risks,
                "strategy_raw_metrics": {
                    "prev_open": open_price,
                    "prev_high": high,
                    "prev_low": low,
                    "prev_close": close,
                    "prev_amount": prev_amount,
                    "prev_divergence_pct": round(prev_divergence_pct, 4),
                    "prev_amplitude_pct": round(prev_amplitude_pct, 4),
                    "amount_ratio_prev": round(amount_ratio_prev, 4) if amount_ratio_prev is not None else None,
                    "realtime_price": realtime_price,
                    "realtime_pct_chg": realtime_pct,
                    "realtime_amount": realtime_amount,
                    "intraday_repair_pct": round(intraday_repair_pct, 4),
                    "open_gap_pct": round(open_gap_pct, 4) if open_gap_pct is not None else None,
                    "turnover_rate": turnover,
                    "volume_ratio": volume_ratio,
                    "pe_status": item.get("pe_status"),
                    "pe_status_reason": item.get("pe_status_reason"),
                },
            })
        return rows

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        weights = self.config.get("weights", {}) or {}
        total_weight = sum(float(v or 0) for v in weights.values()) or 1.0
        scored = []
        for item in stocks:
            factors = item.get("factors") or {}
            score = sum(float(factors.get(key, 50) or 50) * float(weight or 0) for key, weight in weights.items()) / total_weight
            scored.append({**item, "score": _clamp(score)})
        return sorted(scored, key=lambda row: row.get("score", 0), reverse=True)


class LowPositionResonanceStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """低位多因子共振：低位、超卖修复、温和放量和资金回流同时出现。"""

    strategy_id = "low_position_resonance"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 3) or 3
        max_price = _to_float(filters.get("max_price"), 120) or 120
        min_amount = _to_float(filters.get("min_amount"), 40_000_000) or 40_000_000
        max_20d_position = _to_float(filters.get("max_20d_position"), 42) or 42
        min_kline_count_20 = int(filters.get("min_kline_count_20") or 15)
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            min20 = _to_float(item.get("min_close_20"), None)
            max20 = _to_float(item.get("max_close_20"), None)
            kline_count_20 = int(_to_float(item.get("kline_count_20"), 0) or 0)
            if close is None or close < min_price or close > max_price:
                continue
            if amount is None or amount < min_amount:
                continue
            if min20 is None or max20 is None or max20 <= min20:
                continue
            if kline_count_20 < min_kline_count_20:
                continue
            position_20d = (close - min20) / (max20 - min20) * 100
            if position_20d > max_20d_position:
                continue
            filtered.append(item)
        return {
            **data_bundle,
            "candidates": filtered,
            "low_position_resonance_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(filtered),
                "hard_filters": filters,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            close_5d = _to_float(item.get("close_5d"), None)
            close_20d = _to_float(item.get("close_20d"), None)
            min20 = _to_float(item.get("min_close_20"), None)
            max20 = _to_float(item.get("max_close_20"), None)
            ma20 = _to_float(item.get("ma20"), None)
            amount = _to_float(item.get("amount"), 0) or 0
            avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
            volume_ratio = _to_float(item.get("volume_ratio"), None)
            turnover = _to_float(item.get("turnover_rate"), None)
            pct1 = _to_float(item.get("pct_chg_1d"), 0) or 0
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0
            realtime_net = _to_float(item.get("realtime_mf_net"), None)

            position_20d = ((close - min20) / (max20 - min20) * 100) if close and min20 is not None and max20 and max20 > min20 else 50
            ret5 = ((close - close_5d) / close_5d * 100) if close and close_5d else 0
            ret20 = ((close - close_20d) / close_20d * 100) if close and close_20d else 0
            ma20_distance = ((close - ma20) / ma20 * 100) if close and ma20 else 0
            amount_ratio = (amount / avg_amount_20) if amount and avg_amount_20 and avg_amount_20 > 0 else None
            net_flow_intensity = (net_mf * 10000 / amount * 100) if amount > 0 else 0
            realtime_flow_intensity = (realtime_net / amount * 100) if realtime_net is not None and amount > 0 else 0
            combined_flow = net_flow_intensity * 0.65 + realtime_flow_intensity * 0.35

            low_position = _clamp(100 - position_20d * 1.7)
            oversold_repair = _clamp(
                _score_peak(ret5, -2.0, 8.0, 28) * 0.45
                + _score_peak(ret20, -8.0, 18.0, 22) * 0.25
                + _score_peak(ma20_distance, -3.0, 9.0, 25) * 0.30
            )
            momentum_repair = _clamp(45 + pct1 * 5.0 + max(-ret5, 0) * 1.5 + max(ma20_distance, -8) * 1.0)
            band_reversion = _clamp(_score_peak(position_20d, 22, 24, 25) * 0.65 + _score_peak(ma20_distance, -4, 8, 25) * 0.35)
            fund_flow = _clamp(50 + combined_flow * 2.4)
            volume_confirm = _clamp(
                _score_peak(volume_ratio, 1.45, 1.25, 35) * 0.55
                + _score_peak(amount_ratio, 1.25, 1.0, 35) * 0.25
                + _score_peak(turnover, 2.2, 2.6, 30) * 0.20
            )

            reasons = [
                f"20日区间位置约 {position_20d:.1f}%，处在相对低位",
                "低位、超卖修复、温和放量和资金回流多因子共振",
            ]
            risks = []
            if pct1 < -3 and position_20d < 12:
                risks.append("价格仍贴近20日低点且当日偏弱，可能不是反转而是继续下探")
            if combined_flow < -1:
                risks.append("资金净流出尚未扭转")
            if volume_ratio is not None and volume_ratio > 3.5:
                risks.append("量比过高，可能是情绪化放量")

            rows.append({
                **item,
                "factors": {
                    "low_position": low_position,
                    "oversold_repair": oversold_repair,
                    "momentum_repair": momentum_repair,
                    "band_reversion": band_reversion,
                    "fund_flow": fund_flow,
                    "volume_confirm": volume_confirm,
                },
                "strategy_notes": ["借鉴低位多指标共振思想，但使用本地日线/资金字段重构", "适合观察低位修复，不适合追高"],
                "candidate_reasons": (item.get("candidate_reasons") or []) + reasons,
                "candidate_risks": (item.get("candidate_risks") or []) + risks,
                "strategy_raw_metrics": {
                    "position_20d_pct": round(position_20d, 4),
                    "return_5d_pct": round(ret5, 4),
                    "return_20d_pct": round(ret20, 4),
                    "ma20_distance_pct": round(ma20_distance, 4),
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "realtime_flow_intensity_pct": round(realtime_flow_intensity, 4),
                    "volume_ratio": volume_ratio,
                    "amount_ratio_20d": round(amount_ratio, 4) if amount_ratio is not None else None,
                },
            })
        return rows


class MultiTimeframeResonanceStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """多周期共振：用日线趋势、近20日、近5日和当日触发代理多周期确认。"""

    strategy_id = "multi_timeframe_resonance"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 3) or 3
        max_price = _to_float(filters.get("max_price"), 180) or 180
        min_amount = _to_float(filters.get("min_amount"), 80_000_000) or 80_000_000
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            if close is None or close < min_price or close > max_price:
                continue
            if amount is None or amount < min_amount:
                continue
            if item.get("ma20") is None:
                continue
            filtered.append(item)
        return {
            **data_bundle,
            "candidates": filtered,
            "multi_timeframe_resonance_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(filtered),
                "hard_filters": filters,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            close_5d = _to_float(item.get("close_5d"), None)
            close_20d = _to_float(item.get("close_20d"), None)
            ma20 = _to_float(item.get("ma20"), None)
            ma60 = _to_float(item.get("ma60"), None)
            amount = _to_float(item.get("amount"), 0) or 0
            avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
            volume_ratio = _to_float(item.get("volume_ratio"), None)
            turnover = _to_float(item.get("turnover_rate"), None)
            std20 = _to_float(item.get("std_return_20"), None)
            pct1 = _to_float(item.get("pct_chg_1d"), 0) or 0
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0
            realtime_net = _to_float(item.get("realtime_mf_net"), None)
            market_strength = _to_float(item.get("market_strength"), 50) or 50

            ret5 = ((close - close_5d) / close_5d * 100) if close and close_5d else 0
            ret20 = ((close - close_20d) / close_20d * 100) if close and close_20d else 0
            ma20_distance = ((close - ma20) / ma20 * 100) if close and ma20 else 0
            ma60_distance = ((close - ma60) / ma60 * 100) if close and ma60 else 0
            ma_spread = ((ma20 - ma60) / ma60 * 100) if ma20 and ma60 else 0
            amount_ratio = (amount / avg_amount_20) if amount and avg_amount_20 and avg_amount_20 > 0 else None
            net_flow_intensity = (net_mf * 10000 / amount * 100) if amount > 0 else 0
            realtime_flow_intensity = (realtime_net / amount * 100) if realtime_net is not None and amount > 0 else 0
            combined_flow = net_flow_intensity * 0.65 + realtime_flow_intensity * 0.35

            daily_trend = _clamp(52 + ma20_distance * 2.0 + ma60_distance * 1.0 + ma_spread * 1.6)
            medium_cycle = _clamp(_score_peak(ret20, 8, 18, 25) * 0.55 + _score_peak(ma20_distance, 3, 10, 25) * 0.45)
            short_cycle_trigger = _clamp(
                _score_peak(ret5, 3, 9, 30) * 0.35
                + _score_peak(pct1, 1.5, 4.5, 30) * 0.35
                + _score_peak(volume_ratio, 1.6, 1.4, 35) * 0.30
            )
            fund_flow = _clamp(50 + combined_flow * 2.2)
            market_context = _clamp(market_strength)
            risk_control = _clamp(
                72
                - abs(ma20_distance) * 1.0
                - max((std20 or 0.025) - 0.025, 0) * 900
                + _score_peak(turnover, 3.0, 3.5, 35) * 0.18
                + _score_peak(amount_ratio, 1.3, 1.0, 35) * 0.12
            )

            reasons = [
                "日线方向、近20日趋势、近5日触发和资金确认同步评分",
                f"5日涨跌 {ret5:.1f}%，20日涨跌 {ret20:.1f}%，MA20偏离 {ma20_distance:.1f}%",
            ]
            risks = []
            if ma20_distance > 18:
                risks.append("价格偏离20日均线较远，追高风险上升")
            if combined_flow < -1:
                risks.append("资金确认偏弱")
            if std20 is not None and std20 > 0.05:
                risks.append("近20日波动较高，需降低仓位或等待回落")

            rows.append({
                **item,
                "factors": {
                    "daily_trend": daily_trend,
                    "medium_cycle": medium_cycle,
                    "short_cycle_trigger": short_cycle_trigger,
                    "fund_flow": fund_flow,
                    "market_context": market_context,
                    "risk_control": risk_control,
                },
                "strategy_notes": ["当前用日线、近5日、近20日代理多周期共振", "后续可接入分钟线后再升级为真正多周期"],
                "candidate_reasons": (item.get("candidate_reasons") or []) + reasons,
                "candidate_risks": (item.get("candidate_risks") or []) + risks,
                "strategy_raw_metrics": {
                    "return_5d_pct": round(ret5, 4),
                    "return_20d_pct": round(ret20, 4),
                    "ma20_distance_pct": round(ma20_distance, 4),
                    "ma60_distance_pct": round(ma60_distance, 4),
                    "ma20_ma60_spread_pct": round(ma_spread, 4),
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "realtime_flow_intensity_pct": round(realtime_flow_intensity, 4),
                    "volume_ratio": volume_ratio,
                    "amount_ratio_20d": round(amount_ratio, 4) if amount_ratio is not None else None,
                },
            })
        return rows


class ChanStructureWatchStrategy(_ZScoreMixin, BaseSelectionStrategy):
    """缠论结构观察：日线级别的分型/中枢/背驰代理信号。

    V1 不声称完整自动化缠论，只把可复盘的日线结构拆成因子：
    低位底分型代理、下降笔转折、中枢支撑、背驰修复、离开确认和风险约束。
    """

    strategy_id = "chan_structure_watch"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 3) or 3
        max_price = _to_float(filters.get("max_price"), 160) or 160
        min_amount = _to_float(filters.get("min_amount"), 50_000_000) or 50_000_000
        min_kline_count_20 = int(filters.get("min_kline_count_20") or 18)
        max_position_20d = _to_float(filters.get("max_position_20d"), 78) or 78
        filtered = []
        for item in data_bundle.get("candidates", []):
            if item.get("is_st"):
                continue
            close = _to_float(item.get("close"), None)
            amount = _to_float(item.get("amount"), None)
            min20 = _to_float(item.get("min_close_20"), None)
            max20 = _to_float(item.get("max_close_20"), None)
            kline_count_20 = int(_to_float(item.get("kline_count_20"), 0) or 0)
            if close is None or close < min_price or close > max_price:
                continue
            if amount is None or amount < min_amount:
                continue
            if min20 is None or max20 is None or max20 <= min20:
                continue
            if kline_count_20 < min_kline_count_20:
                continue
            position_20d = (close - min20) / (max20 - min20) * 100
            if position_20d > max_position_20d:
                continue
            filtered.append(item)
        return {
            **data_bundle,
            "candidates": filtered,
            "chan_structure_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(filtered),
                "hard_filters": filters,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            open_price = _to_float(item.get("open"), None)
            high = _to_float(item.get("high"), None)
            low = _to_float(item.get("low"), None)
            close_5d = _to_float(item.get("close_5d"), None)
            close_20d = _to_float(item.get("close_20d"), None)
            min20 = _to_float(item.get("min_close_20"), None)
            max20 = _to_float(item.get("max_close_20"), None)
            ma20 = _to_float(item.get("ma20"), None)
            ma60 = _to_float(item.get("ma60"), None)
            amount = _to_float(item.get("amount"), 0) or 0
            avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
            volume_ratio = _to_float(item.get("volume_ratio"), None)
            turnover = _to_float(item.get("turnover_rate"), None)
            std20 = _to_float(item.get("std_return_20"), None)
            pct1 = _to_float(item.get("pct_chg_1d"), 0) or 0
            net_mf = _to_float(item.get("net_mf_amount"), 0) or 0
            realtime_net = _to_float(item.get("realtime_mf_net"), None)

            position_20d = ((close - min20) / (max20 - min20) * 100) if close and min20 is not None and max20 and max20 > min20 else 50
            ret5 = ((close - close_5d) / close_5d * 100) if close and close_5d else 0
            ret20 = ((close - close_20d) / close_20d * 100) if close and close_20d else 0
            day_range = (high - low) if high is not None and low is not None else None
            close_from_low = ((close - low) / day_range * 100) if close is not None and low is not None and day_range and day_range > 0 else 50
            upper_shadow = ((high - close) / day_range * 100) if close is not None and high is not None and day_range and day_range > 0 else 0
            body_strength = ((close - open_price) / open_price * 100) if close and open_price and open_price > 0 else pct1
            ma20_distance = ((close - ma20) / ma20 * 100) if close and ma20 else 0
            ma60_distance = ((close - ma60) / ma60 * 100) if close and ma60 else 0
            amount_ratio = (amount / avg_amount_20) if amount and avg_amount_20 and avg_amount_20 > 0 else None
            net_flow_intensity = (net_mf * 10000 / amount * 100) if amount > 0 else 0
            realtime_flow_intensity = (realtime_net / amount * 100) if realtime_net is not None and amount > 0 else 0
            combined_flow = net_flow_intensity * 0.65 + realtime_flow_intensity * 0.35

            # Chan V1 proxies:
            # - bottom_fractal: near the lower 20d range, closes away from the day low, and avoids a weak full-day close.
            # - stroke_turn: 20d weakness starts to flatten while 5d/1d improves.
            # - center_support: price sits around a 20d overlap area instead of chasing the upper extreme.
            # - divergence_proxy: price remains low but selling momentum, volatility, and volume pressure fade.
            bottom_fractal = _clamp(
                (100 - position_20d * 1.45) * 0.45
                + _score_peak(close_from_low, 72, 36, 30) * 0.35
                + _score_peak(body_strength, 1.2, 5.5, 30) * 0.20
            )
            stroke_turn = _clamp(
                _score_peak(ret20, -8, 18, 25) * 0.30
                + _score_peak(ret5, 1.5, 8.5, 25) * 0.35
                + _score_peak(pct1, 1.2, 4.2, 30) * 0.20
                + _score_peak(ma20_distance, -1.5, 8.0, 25) * 0.15
            )
            center_support = _clamp(
                _score_peak(position_20d, 42, 30, 25) * 0.40
                + _score_peak(ma20_distance, 0, 7, 25) * 0.35
                + _score_peak(amount_ratio, 1.15, 0.9, 35) * 0.25
            )
            divergence_proxy = _clamp(
                _score_peak(position_20d, 26, 24, 25) * 0.28
                + _score_peak(ret20, -10, 18, 25) * 0.22
                + _score_peak(ret5, -1, 7, 25) * 0.22
                + _clamp(100 - (std20 or 0.026) * 1900) * 0.16
                + _score_peak(volume_ratio, 1.05, 1.2, 35) * 0.12
            )
            breakout_confirm = _clamp(
                _score_peak(ma20_distance, 2.0, 8.0, 25) * 0.28
                + _score_peak(pct1, 2.0, 4.5, 30) * 0.28
                + _score_peak(volume_ratio, 1.35, 1.2, 35) * 0.24
                + _clamp(50 + combined_flow * 2.0) * 0.20
            )
            risk_control = 76.0
            risk_control -= max(position_20d - 70, 0) * 1.2
            risk_control -= max(upper_shadow - 34, 0) * 0.55
            risk_control -= max((std20 or 0.025) - 0.04, 0) * 900
            risk_control -= max((turnover or 0) - 18, 0) * 0.9
            if ma60_distance < -18:
                risk_control -= 8
            if combined_flow < -1.2:
                risk_control -= 6
            risk_control = _clamp(risk_control)

            buy_point = "二买观察"
            if breakout_confirm >= 72 and center_support >= 58:
                buy_point = "三买/离开确认观察"
            elif bottom_fractal >= 72 and divergence_proxy >= 62:
                buy_point = "一买背驰观察"

            reasons = [
                f"缠论结构代理：{buy_point}，20日区间位置约 {position_20d:.1f}%",
                f"5日涨跌 {ret5:.1f}%，20日涨跌 {ret20:.1f}%，MA20偏离 {ma20_distance:.1f}%",
            ]
            if divergence_proxy >= 65:
                reasons.append("价格仍处低位但短期跌势收敛，具备背驰修复观察价值")
            if center_support >= 65:
                reasons.append("价格靠近20日重叠区/均线附近，近似中枢支撑")
            if breakout_confirm >= 68:
                reasons.append("出现向上离开中枢的初步确认信号")

            risks = []
            if upper_shadow > 38:
                risks.append(f"上影线占日内波动约 {upper_shadow:.1f}%，冲高回落风险")
            if combined_flow < -1:
                risks.append("资金确认偏弱，结构信号可能只是弱反弹")
            if ma60_distance < -18:
                risks.append("仍明显低于MA60，大级别趋势未修复")
            if turnover is not None and turnover > 22:
                risks.append(f"换手 {turnover:.1f}% 偏高，短线波动可能放大")

            rows.append({
                **item,
                "factors": {
                    "bottom_fractal": bottom_fractal,
                    "stroke_turn": stroke_turn,
                    "center_support": center_support,
                    "divergence_proxy": divergence_proxy,
                    "breakout_confirm": breakout_confirm,
                    "risk_control": risk_control,
                },
                "strategy_notes": ["V1 使用日线聚合字段近似缠论结构，先作为观察池验证", "高分代表结构值得跟踪，不代表立即买入"],
                "candidate_reasons": (item.get("candidate_reasons") or []) + reasons,
                "candidate_risks": (item.get("candidate_risks") or []) + risks,
                "strategy_raw_metrics": {
                    "chan_buy_point_proxy": buy_point,
                    "position_20d_pct": round(position_20d, 4),
                    "return_5d_pct": round(ret5, 4),
                    "return_20d_pct": round(ret20, 4),
                    "close_from_low_pct": round(close_from_low, 4),
                    "upper_shadow_pct": round(upper_shadow, 4),
                    "body_strength_pct": round(body_strength, 4),
                    "ma20_distance_pct": round(ma20_distance, 4),
                    "ma60_distance_pct": round(ma60_distance, 4),
                    "amount_ratio_20d": round(amount_ratio, 4) if amount_ratio is not None else None,
                    "volume_ratio": volume_ratio,
                    "turnover_rate": turnover,
                    "std_return_20": std20,
                    "net_flow_intensity_pct": round(net_flow_intensity, 4),
                    "realtime_flow_intensity_pct": round(realtime_flow_intensity, 4),
                },
            })
        return rows

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        weights = self.config.get("weights", {}) or {}
        total_weight = sum(float(v or 0) for v in weights.values()) or 1.0
        scored = []
        for item in stocks:
            factors = item.get("factors") or {}
            score = sum(float(factors.get(key, 50) or 50) * float(weight or 0) for key, weight in weights.items()) / total_weight
            scored.append({**item, "score": _clamp(score)})
        return sorted(
            scored,
            key=lambda row: (
                -float(row.get("score") or 0),
                -float((row.get("factors") or {}).get("divergence_proxy") or 0),
                -float((row.get("factors") or {}).get("center_support") or 0),
                -float(row.get("amount") or 0),
                str(row.get("code") or ""),
            ),
        )

    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        threshold = float(self.config.get("score_threshold", 60) or 60)
        max_picks = int(self.config.get("max_picks", self.config.get("max_positions", 5)) or 5)
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
                if item.get("opinion_match_type") == "direct_news_match" and not _has_actionable_stock_news(item):
                    continue
                if require_direct_stock_news and (
                    item.get("opinion_match_type") != "direct_news_match"
                    or not item.get("opinion_stock_news")
                ):
                    continue
                if require_direct_stock_news and not _has_actionable_stock_news(item):
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
                "min_roe": min_roe,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
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
            amount_ratio = _to_float(item.get("realtime_amount_ratio"), None)
            price_signal = realtime_pct if realtime_pct is not None else pct1
            vol_ratio = _to_float(item.get("volume_ratio"), 1) or 1
            market_strength = _to_float(item.get("market_strength"), 50) or 50
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
                volume_confirm = _score_peak(vol_ratio, 1.5, 1.5, 35)
                if amount_ratio is not None and amount_ratio >= 1.5 and price_signal <= 2:
                    volume_confirm -= min((amount_ratio - 1.5) * 22 + (2 - price_signal) * 8, 45)
                intraday_confirm = 72.0
                if high_drawdown is not None and high_drawdown < 0:
                    intraday_confirm -= min(abs(high_drawdown) * 4.0, 50)
                if open_drawdown is not None and open_drawdown < 0:
                    intraday_confirm -= min(abs(open_drawdown) * 3.0, 30)
                if amount_ratio is not None and amount_ratio >= 1.5 and price_signal <= 2:
                    intraday_confirm -= min((amount_ratio - 1.5) * 20 + (2 - price_signal) * 8, 35)
                trade_signal = _trade_signal_state(
                    price_signal=price_signal,
                    high_drawdown=high_drawdown,
                    open_drawdown=open_drawdown,
                    net_flow_intensity=net_flow_intensity,
                    match_type=match_type,
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
                    "price_confirm": _clamp(price_confirm),
                    "volume_confirm": _clamp(volume_confirm),
                    "intraday_confirm": _clamp(intraday_confirm),
                    "market_theme": _clamp(theme_alignment),
                    "market_context": _clamp(market_strength),
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
                }
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
                    "price_confirm": _clamp(52 + pct1 * 4 - max(pct1 - 7, 0) * 7),
                    "volume_confirm": _score_peak(vol_ratio, 1.5, 1.5, 35),
                    "market_context": _clamp(market_strength),
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
                }

            candidate_risks = list(item.get("candidate_risks") or [])
            if raw_metrics.get("large_flow_signal") == "large_outflow":
                candidate_risks.append("舆情较强但大单/超大单资金偏流出")
            if raw_metrics.get("trade_signal_state") == "watch":
                candidate_risks.append(raw_metrics.get("trade_signal_reason") or "盘中交易确认不足")
            elif raw_metrics.get("trade_signal_state") == "weak":
                candidate_risks.append(raw_metrics.get("trade_signal_reason") or "走势未确认")
            rows.append({
                **item,
                "factors": factors,
                "strategy_notes": notes,
                "strategy_raw_metrics": raw_metrics,
                "candidate_reasons": (item.get("candidate_reasons") or []) + [item.get("opinion_match_reason") or "舆情热度与交易确认共同筛选"],
                "candidate_risks": candidate_risks,
            })
        return rows
