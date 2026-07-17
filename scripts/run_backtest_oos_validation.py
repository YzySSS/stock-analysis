from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.backtest.strategy_validation import (
    ALLOWED_VALIDATION_MODES,
    ALLOWED_VALIDATION_STRATEGIES,
    HISTORICAL_HOLDOUT,
    StrategyValidationRequest,
    StrategyValidationService,
)


DEFAULT_THRESHOLDS = {
    "lowvol_reversal": 60.0,
    "v13_three_factor": 65.0,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze and execute a controlled strategy validation protocol. "
            "New protocols are dry-run plans unless --freeze is supplied."
        )
    )
    parser.add_argument("--protocol-id")
    parser.add_argument("--batch-id")
    parser.add_argument("--strategy-id", choices=sorted(ALLOWED_VALIDATION_STRATEGIES))
    parser.add_argument("--validation-mode", choices=sorted(ALLOWED_VALIDATION_MODES), default=HISTORICAL_HOLDOUT)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--universe-code", default="ALL_A")
    parser.add_argument("--benchmark-index-code", default="000300.SH")
    parser.add_argument("--max-picks", type=int, default=3)
    parser.add_argument("--score-threshold", type=float)
    parser.add_argument("--commission-bps", type=float, default=3.0)
    parser.add_argument("--stamp-tax-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--minimum-trade-days", type=int, default=120)
    parser.add_argument("--minimum-trades", type=int, default=120)
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Execute the protocol after freezing it")
    parser.add_argument("--execute-protocol", help="Execute an existing frozen protocol")
    parser.add_argument("--reconcile-protocol", help="Finalize a running protocol after coordinator interruption")
    parser.add_argument("--report-only", help="Print one existing protocol and its report")
    parser.add_argument("--rebuild-report", help="Deterministically rebuild a successful protocol report")
    parser.add_argument("--supersede-protocol", help="Mark a frozen/failed protocol as replaced")
    parser.add_argument("--replacement-protocol")
    parser.add_argument("--supersede-reason", default="replaced by a stricter frozen protocol")
    parser.add_argument("--list", action="store_true", help="List recent frozen validation protocols")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min-available-mb", type=int, default=1024)
    parser.add_argument("--max-swap-used-mb", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    service = StrategyValidationService()
    if args.report_only:
        result = service.get(args.report_only)
    elif args.rebuild_report:
        result = service.rebuild_report(args.rebuild_report)
    elif args.supersede_protocol:
        if not args.replacement_protocol:
            parser.error("--supersede-protocol requires --replacement-protocol")
        result = service.supersede(
            args.supersede_protocol,
            replacement_protocol_id=args.replacement_protocol,
            reason=args.supersede_reason,
        )
    elif args.list:
        result = {"items": service.list(limit=max(1, min(args.limit, 100)))}
    elif args.execute_protocol:
        result = service.execute(
            args.execute_protocol,
            min_available_mb=args.min_available_mb,
            max_swap_used_mb=args.max_swap_used_mb,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    elif args.reconcile_protocol:
        result = service.reconcile(args.reconcile_protocol)
    else:
        if not args.strategy_id or not args.start_date or not args.end_date:
            parser.error("--strategy-id, --start-date and --end-date are required for a new protocol")
        protocol_id = args.protocol_id or (
            f"oos_{args.validation_mode}_{args.strategy_id}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        request = StrategyValidationRequest(
            protocol_id=protocol_id,
            batch_id=args.batch_id,
            strategy_id=args.strategy_id,
            validation_mode=args.validation_mode,
            start_date=args.start_date,
            end_date=args.end_date,
            universe_code=args.universe_code,
            benchmark_index_code=args.benchmark_index_code,
            max_picks=args.max_picks,
            score_threshold=(
                args.score_threshold
                if args.score_threshold is not None
                else DEFAULT_THRESHOLDS[args.strategy_id]
            ),
            commission_bps=args.commission_bps,
            stamp_tax_bps=args.stamp_tax_bps,
            slippage_bps=args.slippage_bps,
            minimum_trade_days=args.minimum_trade_days,
            minimum_trades=args.minimum_trades,
        )
        if args.execute and not args.freeze:
            parser.error("--execute requires --freeze for a new protocol")
        result = service.freeze(request) if args.freeze else service.plan(request)
        if args.execute:
            result = service.execute(
                protocol_id,
                min_available_mb=args.min_available_mb,
                max_swap_used_mb=args.max_swap_used_mb,
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
