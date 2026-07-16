"""Submit a selection task to the recoverable MySQL worker queue."""

from __future__ import annotations

import argparse
import json

from app.stock_selection.run_tasks import SelectionRunService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="提交异步选股任务；结果通过 run_id 在选股页或 API 查询。"
    )
    parser.add_argument("--strategy", dest="strategy_id", help="策略 ID；默认使用当前默认策略")
    parser.add_argument("--limit", type=int, default=3, help="最大入选数量，默认 3")
    parser.add_argument("--score-threshold", type=float, default=None, help="可选分数底线")
    parser.add_argument("--instrument-type", default="stock", help="当前仅支持 stock")
    parser.add_argument("--market-board", default=None, help="可选市场板块白名单")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run = SelectionRunService().submit(
        {
            "strategy_id": args.strategy_id,
            "limit": args.limit,
            "max_picks": args.limit,
            "score_threshold": args.score_threshold,
            "instrument_type": args.instrument_type,
            "market_board": args.market_board,
            "save": False,
        }
    )
    print(json.dumps(run, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
