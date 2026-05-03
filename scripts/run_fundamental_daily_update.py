from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.fundamental_sync import FundamentalSync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batched fundamental sync jobs until coverage target or exhaustion.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--pause-between-batches", type=float, default=2.0)
    parser.add_argument("--max-batches", type=int, default=0, help="0 means no explicit batch cap")
    parser.add_argument("--stale-after-days", type=int, default=30)
    parser.add_argument("--all", action="store_true", help="Sync stale records instead of only missing ones")
    parser.add_argument("--profit-yoy-only", action="store_true", help="Only refill rows where profit_yoy is still null")
    parser.add_argument("--prioritize-missing-pe", action="store_true", help="Prioritize stocks where PE is missing and EPS/fundamentals are still incomplete")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    sync = FundamentalSync(sleep_seconds=args.sleep_seconds)

    batch_no = 0
    summaries = []
    total_scanned = 0
    total_updated = 0
    total_no_data = 0
    total_failed = 0
    total_throttled = 0

    while True:
        if args.max_batches and batch_no >= args.max_batches:
            break

        result = sync.run(
            limit=args.batch_size,
            only_missing=not args.all and not args.profit_yoy_only and not args.prioritize_missing_pe,
            stale_after_days=args.stale_after_days,
            only_missing_profit_yoy=args.profit_yoy_only,
            prioritize_missing_pe=args.prioritize_missing_pe,
        )
        batch_no += 1
        summaries.append(result.to_dict())
        total_scanned += result.scanned
        total_updated += result.updated
        total_no_data += result.no_data
        total_failed += result.failed
        total_throttled += result.throttled

        print(json.dumps({
            "batch_no": batch_no,
            **result.to_dict(),
        }, ensure_ascii=False), flush=True)

        if result.scanned < args.batch_size:
            break
        if result.updated == 0 and result.no_data == 0:
            break

        time.sleep(args.pause_between_batches)

    print(json.dumps({
        "finished": True,
        "batch_size": args.batch_size,
        "batches": batch_no,
        "total_scanned": total_scanned,
        "total_updated": total_updated,
        "total_no_data": total_no_data,
        "total_failed": total_failed,
        "total_throttled": total_throttled,
        "runs": summaries,
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
