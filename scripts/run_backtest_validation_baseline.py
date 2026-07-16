from __future__ import annotations

import argparse
import json
from datetime import datetime

from app.backtest.validation_baseline import ALLOWED_BASELINE_STRATEGIES, BacktestValidationBaseline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a small serial backtest validation baseline. Dry-run unless --execute is supplied."
    )
    parser.add_argument("--baseline-id", help="Stable identifier used to group system-test runs")
    parser.add_argument("--report-only", help="Print an existing baseline report without creating runs")
    parser.add_argument("--strategies", nargs="+", default=list(ALLOWED_BASELINE_STRATEGIES))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--return-mode", choices=("1d", "3d"), default="1d")
    parser.add_argument("--max-trade-days", type=int, default=10)
    parser.add_argument("--max-picks", type=int, default=3)
    parser.add_argument("--min-available-mb", type=int, default=1024)
    parser.add_argument("--max-swap-used-mb", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--execute", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    baseline = BacktestValidationBaseline()
    if args.report_only:
        print(json.dumps(baseline.report(args.report_only), ensure_ascii=False, indent=2, default=str))
        return
    if not args.start_date or not args.end_date:
        parser.error("--start-date and --end-date are required unless --report-only is used")
    baseline_id = args.baseline_id or (
        f"baseline_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{args.start_date}_{args.end_date}_{args.return_mode}"
    )
    kwargs = {
        "baseline_id": baseline_id,
        "strategies": args.strategies,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "return_mode": args.return_mode,
        "max_trade_days": args.max_trade_days,
        "max_picks": args.max_picks,
        "min_available_mb": args.min_available_mb,
        "max_swap_used_mb": args.max_swap_used_mb,
    }
    if args.execute:
        result = baseline.execute(
            **kwargs,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    else:
        result = baseline.plan(**kwargs)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
