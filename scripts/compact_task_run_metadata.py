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

from app.jobs.task_log_compaction import TaskRunMetadataCompactionService  # noqa: E402
from app.shared.task_log import TASK_RUN_METADATA_MAX_BYTES, TaskRunLogger  # noqa: E402


TASK_NAME = "task_run_metadata_compaction"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compact duplicated market-opinion detail from task_run_log metadata"
    )
    parser.add_argument("--apply", action="store_true", help="apply updates; default is dry-run")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    service = TaskRunMetadataCompactionService(
        max_bytes=TASK_RUN_METADATA_MAX_BYTES,
        batch_size=args.batch_size,
    )
    if not args.apply:
        print(
            json.dumps(
                {"status": "inspection_only", "preview": service.preview()},
                ensure_ascii=False,
                default=str,
                indent=2,
            )
        )
        return

    logger = TaskRunLogger()
    run_id = f"task_log_compaction_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.start(
        TASK_NAME,
        run_id,
        {
            "operation": "compact_market_opinion_task_metadata",
            "batch_size": args.batch_size,
            "max_bytes": TASK_RUN_METADATA_MAX_BYTES,
        },
    )
    try:
        result = service.apply()
        status = "success" if result.get("status") == "success" else "skipped"
        logger.finish(
            TASK_NAME,
            run_id,
            status,
            f"task-run metadata compaction finished: {status}",
            result,
        )
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        payload = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            payload["error"],
            payload,
            error_code="task_run_metadata_compaction_failed",
        )
        print(json.dumps({"status": "failed", **payload}, ensure_ascii=False), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
