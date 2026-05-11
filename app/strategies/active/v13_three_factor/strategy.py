"""三因子策略模板。

这是新模块化架构下的第一个可插拔策略骨架。
当前阶段先定义统一接口和配置读取方式，后续再把旧仓库里的
真实因子计算与选股逻辑逐步迁入。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import sqrt
from typing import Any, Dict, List


@dataclass
class StrategyInput:
    """策略输入。

    后续可逐步补充为标准化数据对象，例如：
    - 股票基础信息
    - 历史K线
    - 基本面因子
    - 实时行情快照
    """

    trade_date: str
    universe: List[str] = field(default_factory=list)
    market_context: Dict[str, Any] = field(default_factory=dict)
    features: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class StockScore:
    """单只股票评分结果。"""

    code: str
    total_score: float
    factor_scores: Dict[str, float] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass
class StrategyOutput:
    """策略输出。"""

    strategy_id: str
    trade_date: str
    selected: List[StockScore] = field(default_factory=list)
    rejected: List[StockScore] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


def _stddev(values: List[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return sqrt(variance)


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _clamp_score(value: float) -> float:
    return round(max(0.0, min(value, 100.0)), 2)


class V13ThreeFactorStrategy:
    """三因子策略。

    核心因子：
    - turnover
    - lowvol
    - reversal
    """

    strategy_id = "v13_three_factor"
    strategy_name = "三因子策略"
    version = "0.1.0"

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or self.default_config()

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        filters = self.config.get("hard_filters", {}) or {}
        min_price = _to_float(filters.get("min_price"), 5) or 5
        max_price = _to_float(filters.get("max_price"), 150) or 150
        min_avg_amount_20 = _to_float(filters.get("min_avg_amount_20"), 50_000_000) or 50_000_000
        min_history_days = int(filters.get("min_history_days") or 20)
        require_factor_input = bool(filters.get("require_factor_input", True))

        candidates = []
        for item in data_bundle.get("candidates", []):
            close = _to_float(item.get("close"), None)
            avg_amount_20 = _to_float(item.get("avg_amount_20"), None)
            kline_count_20 = int(item.get("kline_count_20") or 0)
            if item.get("is_st"):
                continue
            if close is None or close < min_price or close > max_price:
                continue
            if kline_count_20 < min_history_days:
                continue
            if avg_amount_20 is None or avg_amount_20 < min_avg_amount_20:
                continue
            if require_factor_input and item.get("turnover_rate") is None:
                continue
            candidates.append(item)

        return {
            **data_bundle,
            "candidates": candidates,
            "v13_filter_summary": {
                "before": len(data_bundle.get("candidates", [])),
                "after": len(candidates),
                "removed": len(data_bundle.get("candidates", [])) - len(candidates),
                "hard_filters": filters,
            },
        }

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for item in data_bundle.get("candidates", []):
            score = self.score_stock(str(item.get("code")), item)
            reasons = [
                f"换手 {score.factor_scores.get('turnover', 0):.2f}：温和活跃优先",
                f"低波 {score.factor_scores.get('lowvol', 0):.2f}：20日区间宽度越窄越稳",
                f"反转 {score.factor_scores.get('reversal', 0):.2f}：偏好温和回撤后的修复",
            ]
            risks = []
            volume_ratio = _to_float(item.get("volume_ratio"), None)
            turnover_rate = _to_float(item.get("turnover_rate"), None)
            if volume_ratio is not None and volume_ratio > 3:
                risks.append("量比偏高，短线可能过热")
            if turnover_rate is not None and turnover_rate < 0.5:
                risks.append("换手偏低，流动性需继续观察")
            rows.append(
                {
                    **item,
                    "factors": score.factor_scores,
                    "v13_notes": score.notes,
                    "candidate_reasons": (item.get("candidate_reasons") or []) + reasons,
                    "candidate_risks": (item.get("candidate_risks") or []) + risks,
                }
            )
        return rows

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        scored: List[Dict[str, Any]] = []
        for item in stocks:
            factors = item.get("factors", {})
            weights = self.config.get("weights", self.default_config()["weights"])
            total_score = (
                float(factors.get("turnover", 0) or 0) * float(weights.get("turnover", 0) or 0)
                + float(factors.get("lowvol", 0) or 0) * float(weights.get("lowvol", 0) or 0)
                + float(factors.get("reversal", 0) or 0) * float(weights.get("reversal", 0) or 0)
            )
            scored.append({**item, "score": round(total_score, 4)})
        return sorted(scored, key=lambda row: row.get("score", 0), reverse=True)

    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        threshold = float(self.config.get("score_threshold", 0))
        max_picks = int(self.config.get("max_picks", self.config.get("max_positions", 5)))
        selected = [item for item in scored_stocks if float(item.get("score", 0) or 0) >= threshold]
        return selected[:max_picks]

    def explain(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "code": stock.get("code"),
            "score": stock.get("score"),
            "factors": stock.get("factors", {}),
            "strategy": self.strategy_id,
            "notes": stock.get("v13_notes", []),
        }

    @classmethod
    def default_config(cls) -> Dict[str, Any]:
        return {
            "weights": {
                "turnover": 0.35,
                "lowvol": 0.35,
                "reversal": 0.30,
            },
            "hard_filters": {
                "min_price": 5,
                "max_price": 150,
                "min_avg_amount_20": 50_000_000,
                "min_history_days": 20,
                "require_factor_input": True,
            },
            "holding_period": 3,
            "score_threshold": 60,
            "max_positions": 5,
            "min_turnover_amount": 50_000_000,
            "selection_time": "14:30",
        }

    def score_stock(self, code: str, feature_row: Dict[str, Any]) -> StockScore:
        """对单只股票打分。

        当前优先采用旧仓库中已经出现过的 V13 三因子近似逻辑：
        - Turnover: 20日平均换手率映射到 0-100
        - LowVol: 波动率越低分越高
        - Reversal: 近20日跌得越多分越高

        如果上游已经给出 *_score，则优先直接使用。
        """

        turnover_rate = _to_float(feature_row.get("turnover_rate"), None)
        volume_ratio = _to_float(feature_row.get("volume_ratio"), None)
        close = _to_float(feature_row.get("close"), None)
        close_20d = _to_float(feature_row.get("close_20d"), None)
        ma20 = _to_float(feature_row.get("ma20"), None)
        max_close_20 = _to_float(feature_row.get("max_close_20"), None)
        min_close_20 = _to_float(feature_row.get("min_close_20"), None)

        if turnover_rate is not None and close is not None and close_20d and ma20 and max_close_20 and min_close_20:
            # 换手：偏好 0.8%~3% 的温和活跃区间，避免极低流动性和过热换手。
            turnover_score = _clamp_score(100 - abs(turnover_rate - 1.8) * 18)
            if volume_ratio is not None:
                if 0.8 <= volume_ratio <= 1.8:
                    turnover_score = _clamp_score(turnover_score + 5)
                elif volume_ratio > 3:
                    turnover_score = _clamp_score(turnover_score - min((volume_ratio - 3) * 8, 20))

            # 低波：用 20 日收盘价区间宽度近似波动，区间越窄越稳。
            range_20 = (max_close_20 - min_close_20) / ma20 if ma20 else 0
            lowvol_score = _clamp_score(100 - range_20 * 180)

            # 反转：偏好温和回撤后的修复，明显上涨或深跌都降分。
            ret_20d = (close - close_20d) / close_20d if close_20d else 0
            if ret_20d < -0.25:
                reversal_score = _clamp_score(35 + (ret_20d + 0.25) * 120)
            elif ret_20d < 0:
                reversal_score = _clamp_score(55 + min(abs(ret_20d) * 220, 35))
            else:
                reversal_score = _clamp_score(55 - ret_20d * 180)
        elif all(key in feature_row for key in ["turnover_score", "lowvol_score", "reversal_score"]):
            turnover_score = float(feature_row.get("turnover_score", 0))
            lowvol_score = float(feature_row.get("lowvol_score", 0))
            reversal_score = float(feature_row.get("reversal_score", 0))
        else:
            closes = [float(x) for x in feature_row.get("closes", []) if x is not None]
            turnovers = [float(x) for x in feature_row.get("turnovers", []) if x is not None]

            if len(closes) < 20:
                return StockScore(
                    code=code,
                    total_score=0.0,
                    factor_scores={"turnover": 0.0, "lowvol": 0.0, "reversal": 0.0},
                    notes=["历史收盘价不足20日"],
                )

            recent_turnovers = turnovers[-20:] if len(turnovers) >= 20 else turnovers
            avg_turnover = sum(recent_turnovers) / len(recent_turnovers) if recent_turnovers else 0.0
            turnover_score = max(0.0, min(100.0, 100.0 - (avg_turnover - 2.0) * 5.0))

            returns = []
            for prev, curr in zip(closes[:-1], closes[1:]):
                if prev:
                    returns.append((curr - prev) / prev)
            recent_returns = returns[-60:] if len(returns) >= 60 else returns
            volatility = _stddev(recent_returns) * sqrt(252) if recent_returns else 0.0
            lowvol_score = max(0.0, min(100.0, 100.0 - volatility * 200.0)) if recent_returns else 50.0

            price_now = closes[-1]
            price_20d = closes[-20] if len(closes) >= 20 else closes[0]
            ret_20d = ((price_now - price_20d) / price_20d) if price_20d else 0.0
            reversal_score = max(0.0, min(100.0, 50.0 - ret_20d * 150.0))

        weights = self.config["weights"]
        total_score = (
            turnover_score * weights["turnover"]
            + lowvol_score * weights["lowvol"]
            + reversal_score * weights["reversal"]
        )

        return StockScore(
            code=code,
            total_score=round(total_score, 2),
            factor_scores={
                "turnover": round(turnover_score, 2),
                "lowvol": round(lowvol_score, 2),
                "reversal": round(reversal_score, 2),
            },
            notes=["使用 factor_input_daily + 20日行情窗口计算"] if turnover_rate is not None else [],
        )

    def run(self, payload: StrategyInput) -> StrategyOutput:
        """执行策略。

        当前模板逻辑：
        1. 遍历股票池
        2. 读取已标准化的 feature 输入
        3. 执行三因子加权打分
        4. 按阈值和最大持仓数筛选
        """

        scored: List[StockScore] = []
        for code in payload.universe:
            feature_row = payload.features.get(code, {})
            scored.append(self.score_stock(code, feature_row))

        scored.sort(key=lambda item: item.total_score, reverse=True)

        threshold = self.config["score_threshold"]
        max_positions = self.config["max_positions"]

        selected = [item for item in scored if item.total_score >= threshold][:max_positions]
        rejected = [item for item in scored if item.code not in {x.code for x in selected}]

        return StrategyOutput(
            strategy_id=self.strategy_id,
            trade_date=payload.trade_date,
            selected=selected,
            rejected=rejected,
            metadata={
                "strategy_name": self.strategy_name,
                "version": self.version,
                "config": self.config,
                "universe_size": len(payload.universe),
                "selected_count": len(selected),
            },
        )
