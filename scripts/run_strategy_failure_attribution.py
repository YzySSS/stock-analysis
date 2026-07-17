from __future__ import annotations

import argparse
import json

from app.backtest.failure_attribution import StrategyFailureAttributionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic, read-only failure-attribution report for successful frozen backtest runs."
    )
    parser.add_argument("--run-id", action="append", required=True, help="Successful backtest run id; repeat for multiple runs")
    parser.add_argument("--benchmark-index-code", default="000300.SH")
    parser.add_argument("--compact", action="store_true", help="Print compact JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = StrategyFailureAttributionService()
    reports = [service.build_report(run_id, args.benchmark_index_code) for run_id in args.run_id]
    payload = {"report_version": "strategy_failure_attribution_batch_v1", "reports": reports}
    print(json.dumps(payload, ensure_ascii=False, indent=None if args.compact else 2, default=str))


if __name__ == "__main__":
    main()
