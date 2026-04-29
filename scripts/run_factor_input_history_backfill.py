from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.factor_input_history_sync import FactorInputHistorySync
from app.shared.task_log import TaskRunLogger


def build_run_id() -> str:
    return f"factor_input_history_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill factor input history for selection-related fields")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--limit-per-day", type=int, default=200)
    parser.add_argument("--offset", type=int, default=0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logger = TaskRunLogger()
    run_id = build_run_id()
    metadata = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "limit_per_day": args.limit_per_day,
        "offset": args.offset,
    }
    logger.start(task_name="factor_input_history_backfill", run_id=run_id, metadata=metadata)
    try:
        sync = FactorInputHistorySync()
        result = sync.run(
            start_date=args.start_date,
            end_date=args.end_date,
            limit_per_day=args.limit_per_day,
            offset=args.offset,
        )
        payload = {**metadata, **result}
        logger.finish(
            task_name="factor_input_history_backfill",
            run_id=run_id,
            status="success",
            message=f"factor input history backfill completed, rows_synced={result.get('rows_synced', 0)}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name="factor_input_history_backfill",
            run_id=run_id,
            status="failed",
            message=str(exc)[:500],
            metadata=metadata,
        )
        raise


if __name__ == "__main__":
    main()
