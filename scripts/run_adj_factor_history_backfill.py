#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.adj_factor_history import (  # noqa: E402
    DEFAULT_MINIMUM_COVERAGE_RATIO,
    AdjFactorHistoryBackfill,
)
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "adj_factor_history_backfill"
LOCK_NAME = "stock_analysis_adj_factor_sync"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resumable Tushare adjustment-factor history backfill and coverage audit"
    )
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--force", action="store_true", help="refresh dates that already meet coverage")
    parser.add_argument("--pause-seconds", type=float, default=0.5)
    parser.add_argument(
        "--minimum-coverage-ratio",
        type=float,
        default=DEFAULT_MINIMUM_COVERAGE_RATIO,
    )
    parser.add_argument("--max-days", type=int)
    parser.add_argument("--max-failures", type=int, default=10)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--corporate-action-samples", type=int, default=0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    service = AdjFactorHistoryBackfill()
    default_start, default_end = service.date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    if args.audit_only:
        payload = service.audit(
            start_date,
            end_date,
            minimum_coverage_ratio=args.minimum_coverage_ratio,
        )
        if args.corporate_action_samples > 0:
            payload["corporate_action_samples"] = service.corporate_action_samples(
                start_date,
                end_date,
                limit=args.corporate_action_samples,
            )
        print(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
        return 0 if payload["ready"] else 1

    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "adjustment factor sync already running"}))
        return 0

    run_id = f"adj_factor_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "start_date": start_date,
        "end_date": end_date,
        "pending_only": not args.force,
        "pause_seconds": max(args.pause_seconds, 0.0),
        "minimum_coverage_ratio": args.minimum_coverage_ratio,
        "max_days": args.max_days,
        "max_failures": args.max_failures,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata)
    try:
        payload = service.run(
            run_id,
            start_date,
            end_date,
            pending_only=not args.force,
            pause_seconds=max(args.pause_seconds, 0.0),
            minimum_coverage_ratio=args.minimum_coverage_ratio,
            max_days=args.max_days,
            max_failures=args.max_failures,
        )
        payload["audit"] = service.audit(
            start_date,
            end_date,
            minimum_coverage_ratio=args.minimum_coverage_ratio,
        )
        if args.corporate_action_samples > 0:
            payload["corporate_action_samples"] = service.corporate_action_samples(
                start_date,
                end_date,
                limit=args.corporate_action_samples,
            )
        logger.finish(
            TASK_NAME,
            run_id,
            payload["status"],
            (
                "adjustment factor history backfill completed; "
                f"processed={payload['processed_trade_days']}, "
                f"success={payload['success_trade_days']}, "
                f"partial={payload['partial_trade_days']}, "
                f"empty={payload['empty_trade_days']}, "
                f"failed={payload['failed_trade_days']}"
            ),
            payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
        return 0
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], metadata)
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock)
        if release_error:
            print(json.dumps({"lock_release_warning": release_error}), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
