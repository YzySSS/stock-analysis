from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.etf_rotation.data_sync import latest_market_trade_date
from app.etf_rotation.service import EtfRotationService
from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "etf_rotation_signal_materialize"
LOCK_NAME = "stock_analysis_etf_rotation_signal_materialize"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize the research-only ETF rotation signal"
    )
    parser.add_argument("--trade-date", help="YYYY-MM-DD")
    args = parser.parse_args()
    trade_date = (
        date.fromisoformat(args.trade_date)
        if args.trade_date
        else latest_market_trade_date()
    )

    task_run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "trade_date": trade_date.isoformat(),
        "research_only": True,
        "automatic_trading": False,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, task_run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another ETF rotation materializer owns the lock",
            "run_id": task_run_id,
        }
        logger.finish(TASK_NAME, task_run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    try:
        signal = EtfRotationService().materialize(trade_date)
        result = {
            "status": "success",
            "run_id": task_run_id,
            "signal_run_id": signal["run_id"],
            "signal_status": signal["status"],
            "trade_date": str(signal["trade_date"]),
            "candidate_count": signal["candidate_count"],
            "eligible_count": signal["eligible_count"],
            "selected_count": signal["selected_count"],
            "timing_state": signal["timing_state"],
            "idempotent_reuse": signal.get("idempotent_reuse", False),
            "research_only": True,
        }
        logger.finish(TASK_NAME, task_run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:1000]}"
        logger.finish(TASK_NAME, task_run_id, "failed", error, metadata)
        print(
            json.dumps(
                {"status": "failed", "run_id": task_run_id, "error": error},
                ensure_ascii=False,
            )
        )
        return 1
    finally:
        release_mysql_advisory_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())
