from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.task_log import TaskRunLogger
from app.stock_selection.sentiment_snapshot_materializer import (
    SentimentSnapshotInputQualityError,
    SentimentSnapshotMaterializationService,
)


TASK_NAME = "sentiment_candidate_snapshot_materialize"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize the latest sentiment selection read model from one "
            "repeatable-read MySQL snapshot; no external provider is called"
        )
    )
    parser.add_argument(
        "--strategy-id",
        default="a_share_sentiment",
        choices=("a_share_sentiment", "a_share_sentiment_v05"),
    )
    parser.add_argument(
        "--allow-shadow",
        action="store_true",
        help="allow an explicitly shadow_only strategy; never enables it for API traffic",
    )
    parser.add_argument(
        "--dual-run",
        action="store_true",
        help=(
            "materialize stable 0.4.4 and v0.5 shadow from the same MySQL "
            "repeatable-read snapshot"
        ),
    )
    parser.add_argument(
        "--max-picks",
        type=int,
        help="optional formal selection cap; defaults to the strategy configuration",
    )
    args = parser.parse_args(argv)

    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    metadata = {
        "strategy_id": args.strategy_id,
        "allow_shadow": bool(args.allow_shadow),
        "dual_run": bool(args.dual_run),
        "max_picks": args.max_picks,
        "input_source": "mysql_repeatable_read",
        "external_provider_calls": False,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata=metadata)
    try:
        service = SentimentSnapshotMaterializationService()
        if args.dual_run:
            result = service.materialize_dual(max_picks=args.max_picks)
        else:
            result = service.materialize(
                strategy_id=args.strategy_id,
                allow_shadow=bool(args.allow_shadow),
                max_picks=args.max_picks,
            )
        payload = {**metadata, **result}
        logger.finish(
            TASK_NAME,
            run_id,
            "success",
            message=(
                f"snapshot={result.get('snapshot_id') or result.get('dual_input_hash')} "
                f"coverage={result.get('coverage_ratio')}"
            ),
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 0
    except SentimentSnapshotInputQualityError as exc:
        payload = {
            **metadata,
            "status": "rejected",
            "error_code": "SENTIMENT_SNAPSHOT_INPUT_QUALITY",
            "error": str(exc),
            "input_audit": exc.audit.as_dict(),
        }
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            message=str(exc)[:500],
            error_code="SENTIMENT_SNAPSHOT_INPUT_QUALITY",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr)
        return 2
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
            message=str(exc)[:500],
            error_code=type(exc).__name__,
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
