from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from pymysql.err import OperationalError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.stock_selection.technical_feature_daily import TechnicalFeatureDailyRefreshService
from app.shared.task_log import TaskRunLogger


TASK_NAME = "stock_technical_feature_daily_refresh"
RETRYABLE_MYSQL_ERROR_CODES = {1205, 1213, 2006, 2013}


def is_retryable_refresh_error(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) and bool(exc.args) and int(exc.args[0]) in RETRYABLE_MYSQL_ERROR_CODES


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently refresh stock_technical_feature_daily from local daily_kline rows"
    )
    parser.add_argument("--trade-date", help="as-of trade date (YYYY-MM-DD); defaults to latest daily_kline date")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    if args.attempts < 1:
        parser.error("--attempts must be positive")
    if args.retry_seconds < 0:
        parser.error("--retry-seconds cannot be negative")
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    logger = TaskRunLogger()
    metadata = {
        "requested_trade_date": args.trade_date,
        "source": "daily_kline",
        "batch_size": args.batch_size,
        "max_attempts": args.attempts,
    }
    logger.start(TASK_NAME, run_id, metadata=metadata)
    try:
        result = None
        for attempt in range(1, args.attempts + 1):
            try:
                result = TechnicalFeatureDailyRefreshService(batch_size=args.batch_size).refresh(
                    args.trade_date
                )
                break
            except Exception as exc:
                if attempt >= args.attempts or not is_retryable_refresh_error(exc):
                    raise
                metadata["last_retryable_error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
                metadata["attempts_used"] = attempt
                time.sleep(args.retry_seconds * attempt)
        if result is None:
            raise RuntimeError("technical feature refresh produced no result")
        payload = {**metadata, **result}
        payload["attempts_used"] = int(metadata.get("attempts_used") or 0) + 1
        logger.finish(
            TASK_NAME,
            run_id,
            "success",
            message=f"technical feature snapshot rows={result.get('published_rows', 0)}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        failure_metadata = {
            **metadata,
            "attempts_used": int(metadata.get("attempts_used") or 0) + 1,
        }
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            message=str(exc)[:500],
            metadata=failure_metadata,
        )
        print(
            json.dumps(
                {"status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:1000]}"},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
