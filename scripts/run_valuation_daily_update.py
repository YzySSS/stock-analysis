from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.valuation_sync import ValuationSync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batched valuation sync jobs until coverage target or exhaustion.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--pause-between-batches", type=float, default=2.0)
    parser.add_argument("--max-batches", type=int, default=0, help="0 means no explicit batch cap")
    parser.add_argument("--stale-after-days", type=int, default=7)
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--all", action="store_true", help="Sync stale records instead of only missing ones")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    sync = ValuationSync()

    batch_no = 0
    summaries = []
    total_scanned = 0
    total_updated = 0
    total_missing_source = 0

    while True:
        if args.max_batches and batch_no >= args.max_batches:
            break

        result = sync.run(
            limit=args.batch_size,
            instrument_type=args.instrument_type,
            only_missing=not args.all,
            stale_after_days=args.stale_after_days,
        )
        batch_no += 1
        summaries.append(result.to_dict())
        total_scanned += result.scanned
        total_updated += result.updated
        total_missing_source += result.missing_source

        print(json.dumps({
            "batch_no": batch_no,
            **result.to_dict(),
        }, ensure_ascii=False), flush=True)

        if result.scanned < args.batch_size:
            break
        if result.updated == 0 and result.missing_source == 0:
            break

        time.sleep(args.pause_between_batches)

    print(json.dumps({
        "finished": True,
        "batch_size": args.batch_size,
        "batches": batch_no,
        "total_scanned": total_scanned,
        "total_updated": total_updated,
        "total_missing_source": total_missing_source,
        "runs": summaries,
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
