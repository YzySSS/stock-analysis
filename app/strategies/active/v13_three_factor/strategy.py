"""V13 三因子策略模板。

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


class V13ThreeFactorStrategy:
    """V13 三因子策略。

    核心因子：
    - turnover
    - lowvol
    - reversal
    """

    strategy_id = "v13_three_factor"
    strategy_name = "V13 三因子策略"
    version = "0.1.0"

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or self.default_config()

    @classmethod
    def default_config(cls) -> Dict[str, Any]:
        return {
            "weights": {
                "turnover": 0.35,
                "lowvol": 0.35,
                "reversal": 0.30,
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

        if all(key in feature_row for key in ["turnover_score", "lowvol_score", "reversal_score"]):
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
