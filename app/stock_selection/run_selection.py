"""选股运行入口。

第一版目标：
1. 从策略注册表加载默认策略或指定策略
2. 构造标准化输入
3. 执行策略
4. 输出标准化 JSON

后续再逐步接入真实数据层、数据库和定时调度。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from typing import Dict

from app.shared.strategy_loader import StrategyLoader
from app.strategies.active.v13_three_factor.strategy import StrategyInput


def build_demo_features(universe: list[str]) -> Dict[str, Dict]:
    """构造演示用特征。

    当前是占位实现，后续由 data_ingestion 模块提供真实因子输入。
    """

    demo_scores = {}
    base = [88, 76, 64, 59, 52]
    for i, code in enumerate(universe):
        seed = base[i % len(base)]
        demo_scores[code] = {
            "turnover_score": max(seed - 5, 0),
            "lowvol_score": max(seed, 0),
            "reversal_score": max(seed - 10, 0),
        }
    return demo_scores


def main() -> None:
    parser = argparse.ArgumentParser(description="运行选股策略")
    parser.add_argument("--strategy", help="策略 ID，不传则使用默认策略")
    parser.add_argument("--date", help="交易日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--universe",
        help="逗号分隔的股票池，例如 000001.SZ,000002.SZ,600519.SH",
    )
    args = parser.parse_args()

    trade_date = args.date or datetime.now().strftime("%Y-%m-%d")
    universe = (
        [item.strip() for item in args.universe.split(",") if item.strip()]
        if args.universe
        else ["000001.SZ", "000002.SZ", "600519.SH", "300750.SZ", "601318.SH"]
    )

    loader = StrategyLoader()
    strategy = loader.load_strategy(args.strategy)

    payload = StrategyInput(
        trade_date=trade_date,
        universe=universe,
        market_context={"mode": "demo"},
        features=build_demo_features(universe),
    )

    result = strategy.run(payload)
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
