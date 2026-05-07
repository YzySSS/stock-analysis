from __future__ import annotations

from statistics import mean, pstdev
from typing import Any, Dict, List

from app.stock_selection.base import BaseSelectionStrategy


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _score_0_100(value: Any, default: float = 50.0) -> float:
    number = _to_float(value, default)
    if number is None:
        return default
    if 0 <= number <= 1:
        number *= 100
    return round(max(0.0, min(number, 100.0)), 2)


class V12LegacyStrategy(BaseSelectionStrategy):
    """V12 多因子策略的新架构可执行版。

    目标不是照搬旧脚本，而是按 V12 白皮书语义在当前 MySQL 主链路上重建：
    trend / momentum / quality / sentiment / value / liquidity 六因子，支持市场强度
    动态权重和真实舆情快照 fallback。
    """

    strategy_id = "v12_legacy"

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        return data_bundle

    def _market_adjusted_weights(self, item: Dict[str, Any]) -> Dict[str, float]:
        base = {"trend": 0.20, "momentum": 0.15, "quality": 0.20, "sentiment": 0.15, "value": 0.20, "liquidity": 0.10}
        state = item.get("market_state") or "neutral"
        strength = _to_float(item.get("market_strength"), 50) or 50
        if state == "bull" or strength >= 60:
            return {"trend": 0.22, "momentum": 0.17, "quality": 0.21, "sentiment": 0.16, "value": 0.18, "liquidity": 0.06}
        if state == "bear" or strength <= 40:
            return {"trend": 0.18, "momentum": 0.13, "quality": 0.19, "sentiment": 0.14, "value": 0.22, "liquidity": 0.14}
        return base

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), 0) or 0
            amount = _to_float(item.get("amount"), None)
            pe = _to_float(item.get("pe_tushare"), None)
            pb = _to_float(item.get("pb_tushare"), None)
            roe = _to_float(item.get("roe"), None)
            sentiment_raw = _to_float(item.get("sentiment_score"), None)

            # 当前候选输入还没有完整历史序列时，先复用主链路已计算的三类技术近似分。
            trend = round(_score_0_100(item.get("lowvol_score"), 50) * 0.45 + _score_0_100(item.get("reversal_score"), 50) * 0.35 + _score_0_100(item.get("stability_score"), 20) * 0.20, 2)
            momentum = _score_0_100(item.get("reversal_score"), 50)
            quality = _score_0_100(item.get("quality_score"), 20)

            # 真实舆情为 -1~+1 时映射到 0~100；没有新闻快照则 fallback 到量价/数据质量情绪。
            if sentiment_raw is not None:
                sentiment = round(max(0, min(100, 50 + sentiment_raw * 50)), 2)
            else:
                sentiment = round(_score_0_100(item.get("turnover_score"), 50) * 0.70 + _score_0_100(item.get("data_quality_score"), 0) * 0.30, 2)

            value = 50.0
            if pe is not None and pe > 0:
                value += 25 if pe <= 20 else 12 if pe <= 40 else -15 if pe >= 100 else 0
            if pb is not None and pb > 0:
                value += 15 if pb <= 2 else 6 if pb <= 4 else -10 if pb >= 8 else 0
            if roe is not None and roe >= 10:
                value += 5
            value = round(max(0, min(100, value)), 2)

            if amount is not None and amount > 0:
                liquidity = max(0, min(100, 30 + min(amount / 100_000_000, 10) * 7))
            else:
                liquidity = _score_0_100(item.get("turnover_score"), 50)

            rows.append({
                **item,
                "factors": {
                    "trend": trend,
                    "momentum": momentum,
                    "quality": quality,
                    "sentiment": sentiment,
                    "value": value,
                    "liquidity": round(liquidity, 2),
                },
                "v12_weights": self._market_adjusted_weights(item),
            })
        return rows

    def _zscore_by_factor(self, stocks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
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
        zscores = self._zscore_by_factor(stocks)
        scored: List[Dict[str, Any]] = []
        for item in stocks:
            code = str(item.get("code"))
            weights = item.get("v12_weights") or self._market_adjusted_weights(item)
            weighted_z = sum(zscores.get(key, {}).get(code, 0) * weight for key, weight in weights.items())
            total_score = max(0.0, min(100.0, 50 + weighted_z * 15))
            scored.append({**item, "score": round(total_score, 4), "v12_weighted_z": round(weighted_z, 4)})
        return sorted(scored, key=lambda row: row.get("score", 0), reverse=True)

    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        threshold = float(self.config.get("score_threshold", 0))
        max_picks = int(self.config.get("max_picks", 5))
        selected = [item for item in scored_stocks if float(item.get("score", 0) or 0) >= threshold]
        return selected[:max_picks]

    def explain(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "code": stock.get("code"),
            "score": stock.get("score"),
            "factors": stock.get("factors", {}),
            "strategy": self.strategy_id,
            "market_state": stock.get("market_state"),
            "market_strength": stock.get("market_strength"),
            "weights": stock.get("v12_weights", {}),
            "sentiment_source": "stock_sentiment_daily" if stock.get("sentiment_score") is not None else "fallback_price_volume",
        }
