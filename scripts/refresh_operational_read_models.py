from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.read_models.materialization import (
    DEFAULT_RANK_LIMIT,
    SUPPORTED_MODELS,
    LocalReadModelMaterializer,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "operational_read_models_refresh"


def _parse_models(value: str) -> list[str]:
    models = [item.strip().lower() for item in str(value).split(",") if item.strip()]
    if not models:
        raise argparse.ArgumentTypeError("at least one read model is required")
    invalid = [item for item in models if item != "all" and item not in SUPPORTED_MODELS]
    if invalid:
        raise argparse.ArgumentTypeError(f"unsupported read model(s): {', '.join(invalid)}")
    return models


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh realtime rank, tracking summary and operational status read "
            "models using local MySQL tables only"
        )
    )
    parser.add_argument(
        "--models",
        type=_parse_models,
        default=["all"],
        help="all or comma-separated: realtime-rank,tracking-summary,operational-status",
    )
    parser.add_argument("--rank-limit", type=int, default=DEFAULT_RANK_LIMIT)
    parser.add_argument("--summary-date", help="tracking summary date (YYYY-MM-DD)")
    parser.add_argument("--captured-at", help="operational snapshot time (ISO-8601)")
    args = parser.parse_args(argv)

    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    metadata = {
        "models": args.models,
        "rank_limit": args.rank_limit,
        "summary_date": args.summary_date,
        "captured_at": args.captured_at,
        "input_source": "local_mysql",
        "external_provider_calls": False,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata)
    try:
        result = LocalReadModelMaterializer().refresh(
            args.models,
            rank_limit=args.rank_limit,
            summary_date=args.summary_date,
            captured_at=args.captured_at,
        )
        payload = {**metadata, **result}
        logger.finish(
            TASK_NAME,
            run_id,
            "success",
            "local MySQL read models refreshed",
            payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        payload = {
            **metadata,
            "status": "failed",
            "error_code": type(exc).__name__,
            "error": str(exc)[:1000],
        }
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            str(exc)[:500],
            payload,
            error_code=type(exc).__name__,
        )
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
