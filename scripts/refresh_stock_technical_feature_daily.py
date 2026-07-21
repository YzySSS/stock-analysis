from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.stock_selection.technical_feature_daily import TechnicalFeatureDailyRefreshService
from app.shared.task_log import TaskRunLogger


TASK_NAME = "stock_technical_feature_daily_refresh"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently refresh stock_technical_feature_daily from local daily_kline rows"
    )
    parser.add_argument("--trade-date", help="as-of trade date (YYYY-MM-DD); defaults to latest daily_kline date")
    args = parser.parse_args(argv)
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    logger = TaskRunLogger()
    metadata = {"requested_trade_date": args.trade_date, "source": "daily_kline"}
    logger.start(TASK_NAME, run_id, metadata=metadata)
    try:
        result = TechnicalFeatureDailyRefreshService().refresh(args.trade_date)
        payload = {**metadata, **result}
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
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            message=str(exc)[:500],
            metadata=metadata,
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
