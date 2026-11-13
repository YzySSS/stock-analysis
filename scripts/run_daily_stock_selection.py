"""Submit the user's 09:35 daily preview without changing frozen research code."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.jobs.readiness import build_operational_readiness
from app.shared.db import mysql_read_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger
from app.stock_selection.run_tasks import SelectionRunService
from app.stock_selection.sentiment_snapshot_materializer import SentimentSnapshotMaterializationService
from app.strategies.service import StrategyService

TASK_NAME = "daily_stock_selection"
LOCK_NAME = "daily_stock_selection_lock"
TIMEZONE = ZoneInfo("Asia/Shanghai")
STRATEGY_ID = "a_share_sentiment"
STRATEGY_VERSION = "0.4.4"
MAX_PICKS = 3
EXECUTION_TIME = time(9, 35)
DEADLINE = time(9, 40)


def local_now() -> datetime:
    return datetime.now(TIMEZONE)


def daily_key(signal_date: date) -> str:
    material = f"daily_stock_selection_v1:{signal_date}:{STRATEGY_ID}:{STRATEGY_VERSION}:{MAX_PICKS}:09:35"
    return hashlib.sha256(material.encode()).hexdigest()


class DailySelectionRunService(SelectionRunService):
    """Use the normal submission pipeline, with a persistent scheduled identity.

    Manual tasks only deduplicate while active. This separate cron namespace
    also reuses terminal rows, including zero candidates, failures and cancels.
    The caller holds the advisory lock across lookup/materialization/submission.
    No changes to frozen selector/worker/research-source fingerprints are needed.
    """

    def __init__(self, signal_date: date):
        super().__init__()
        self.signal_date = signal_date
        self.submission_snapshot: dict[str, Any] | None = None

    def _idempotency_key(self, request: dict[str, Any], trade_date: date) -> str:
        return daily_key(self.signal_date)

    def _get_active_by_idempotency(self, idempotency_key: str) -> dict[str, Any] | None:
        with mysql_read_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM selection_run WHERE idempotency_key=%s ORDER BY id DESC LIMIT 1",
                    (idempotency_key,),
                )
                return cursor.fetchone()

    def existing(self) -> dict[str, Any] | None:
        row = self._get_active_by_idempotency(daily_key(self.signal_date))
        return self._normalize_row(row, include_result=False) if row else None

    def _resolve_submission_snapshot_meta(self, **kwargs: Any) -> dict[str, Any]:
        if self.submission_snapshot is None:
            raise RuntimeError("daily selection must pin its freshly materialized snapshot")
        return self.submission_snapshot


def policy() -> dict[str, Any]:
    return {
        "timezone": "Asia/Shanghai",
        "execution_time": "09:35:00",
        "submission_deadline": "09:40:00",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "max_picks": MAX_PICKS,
        "save": False,
        "automatic_trading": False,
        "calendar_source": "tushare.trade_cal",
    }


def preflight(now: datetime, readiness: dict[str, Any]) -> tuple[str, str | None]:
    now = now.astimezone(TIMEZONE) if now.tzinfo else now.replace(tzinfo=TIMEZONE)
    calendar = (readiness.get("data") or {}).get("independent_calendar") or {}
    if (
        calendar.get("health") != "healthy"
        or calendar.get("current_date") != now.date().isoformat()
        or calendar.get("current_date_is_open") not in (True, False)
    ):
        return "blocked", "independent_calendar_not_ready"
    if calendar["current_date_is_open"] is False:
        return "skipped", "market_closed"
    if not EXECUTION_TIME <= now.time().replace(tzinfo=None) < DEADLINE:
        return "skipped", "outside_0935_submission_window"
    if not readiness.get("accepting_jobs"):
        return "blocked", "operational_readiness_not_ready"
    return "ready", None


def next_scheduled_at(now: datetime) -> str | None:
    # Bounded, indexed calendar lookup; never infer holidays from weekdays.
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT cal_date FROM etf_rotation_trade_calendar "
                "WHERE exchange_code='SSE' AND is_open=1 AND source='tushare.trade_cal' "
                "AND cal_date>=%s ORDER BY cal_date LIMIT 8",
                (now.date(),),
            )
            rows = cursor.fetchall() or []
    for row in rows:
        day = row["cal_date"]
        if isinstance(day, datetime):
            day = day.date()
        if not isinstance(day, date):
            day = date.fromisoformat(str(day)[:10])
        scheduled = datetime.combine(day, EXECUTION_TIME, tzinfo=TIMEZONE)
        if scheduled > now:
            return scheduled.isoformat()
    return None


def run(*, dry_run: bool = False) -> dict[str, Any]:
    now = local_now()
    service = DailySelectionRunService(now.date())
    result = {**policy(), "signal_date": now.date().isoformat(), "checked_at": now.isoformat()}
    lock = None
    if not dry_run:
        lock = acquire_mysql_advisory_lock(LOCK_NAME)
        if lock is None:
            return {**result, "status": "skipped", "reason": "already_running"}
    try:
        existing = service.existing()
        if existing:
            return {**result, "status": "deduplicated", "run_id": existing["run_id"],
                    "run_status": existing["status"], "dry_run": dry_run}
        readiness = build_operational_readiness()
        status, reason = preflight(now, readiness)
        calendar = (readiness.get("data") or {}).get("independent_calendar") or {}
        result.update({"status": status, "reason": reason,
                       "readiness_status": readiness.get("status"),
                       "calendar": calendar, "dry_run": dry_run})
        if dry_run:
            meta = StrategyService().get_strategy_meta(STRATEGY_ID)
            result.update({"registered_strategy_version": meta.get("version"),
                           "next_scheduled_at": next_scheduled_at(now),
                           "selection_task_submitted": False,
                           "snapshot_materialized": False})
            if str(meta.get("version")) != STRATEGY_VERSION:
                result.update(status="blocked", reason="strategy_version_drift")
            return result
        if status != "ready":
            return result
        strategy = StrategyService()
        if str(strategy.get_strategy_meta(STRATEGY_ID).get("version")) != STRATEGY_VERSION:
            raise RuntimeError("daily selection requires frozen strategy version 0.4.4")
        # At 09:35, the normal 09:22 snapshot is too old (next regular run 09:37).
        # Build current local inputs first; never silently reuse yesterday's data.
        if not strategy._sentiment_read_model_enabled():
            raise RuntimeError("daily selection requires USE_SENTIMENT_READ_MODEL=true")
        snapshot = SentimentSnapshotMaterializationService().materialize(
            strategy_id=STRATEGY_ID, max_picks=MAX_PICKS,
        )
        decision = datetime.fromisoformat(str(snapshot.get("decision_as_of")))
        if decision.tzinfo is None:
            decision = decision.replace(tzinfo=TIMEZONE)
        decision = decision.astimezone(TIMEZONE)
        if (snapshot.get("snapshot_status") != "ready"
                or snapshot.get("quality_status") != "passed"
                or snapshot.get("strategy_id") != STRATEGY_ID
                or str(snapshot.get("strategy_version")) != STRATEGY_VERSION
                or decision.date() != now.date()
                or not EXECUTION_TIME <= decision.time().replace(tzinfo=None) < DEADLINE):
            raise RuntimeError("daily selection snapshot is not ready/current/in the 09:35 window")
        if preflight(local_now(), readiness)[0] != "ready":
            return {**result, "status": "blocked", "reason": "submission_deadline_elapsed",
                    "snapshot_id": snapshot.get("snapshot_id")}
        service.submission_snapshot = snapshot
        submitted = service.submit({
            "strategy_id": STRATEGY_ID, "instrument_type": "stock", "market_board": "all",
            "limit": MAX_PICKS, "max_picks": MAX_PICKS, "save": False,
        })
        return {**result, "status": "submitted", "reason": None,
                "run_id": submitted["run_id"], "run_status": submitted["status"],
                "input_snapshot_id": submitted.get("input_snapshot_id"),
                "selection_task_submitted": True}
    finally:
        if lock is not None:
            error = release_mysql_advisory_lock(lock)
            if error:
                raise RuntimeError(f"daily selection lock release failed: {error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="交易日09:35提交A股舆情0.4.4，最多3只；不自动保存或交易")
    parser.add_argument("--dry-run", action="store_true", help="只读检查；不创建快照、选股任务或任务日志")
    args = parser.parse_args(argv)
    logger = TaskRunLogger()
    log_id = f"{TASK_NAME}_{local_now().strftime('%Y%m%d_%H%M%S_%f')}"
    if not args.dry_run:
        logger.start(TASK_NAME, log_id, metadata=policy())
    try:
        result = run(dry_run=args.dry_run)
        if not args.dry_run:
            logger.finish(TASK_NAME, log_id,
                          "failed" if result["status"] == "blocked" else "success",
                          message=f"{result['status']}: {result.get('reason') or result.get('run_id') or ''}",
                          metadata=result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 2 if result["status"] == "blocked" else 0
    except Exception as exc:
        if not args.dry_run:
            logger.finish(TASK_NAME, log_id, "failed", message=str(exc)[:500],
                          metadata=policy(), error_code=type(exc).__name__)
        print(json.dumps({**policy(), "status": "failed", "error": str(exc)[:500]},
                         ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
