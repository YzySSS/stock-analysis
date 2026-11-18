"""Submit the user's 09:35 selection and save its results to tracking.

Keep the queue/selector preview-only and frozen research code unchanged. This
scheduled caller owns the separately authorized, idempotent save-item step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time as clock
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
WAIT_SECONDS = 300
POLL_SECONDS = 5


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
        "save_to_tracking": True,
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


def save_tracking_item(*, run_id: str, item: dict[str, Any], score_threshold: Any) -> dict[str, Any]:
    # Reuse the manual save contract, including durable minute enrichment and
    # tracking/display cache invalidation; do not run the selector a second time.
    from app.api.routes.selection import SelectionSaveItemRequest, save_selection_item

    return save_selection_item(SelectionSaveItemRequest(
        run_id=run_id, strategy_id=STRATEGY_ID, item=tracking_item(item), score_threshold=score_threshold,
    ))


def tracking_item(item: dict[str, Any]) -> dict[str, Any]:
    """Pin the existing manual intraday-save semantics to the recorded quote.

    Published snapshot rows keep realtime fields inside explain.raw_metrics and
    may expose yesterday's close as selected_price. The manual writer chooses
    that recorded realtime quote during market hours, but otherwise chooses a
    close based on *save time*. Pin the former explicitly so delayed saves are
    identical, minute enrichment receives the correct day, and no current quote
    or newly computed selector result is substituted.
    """
    raw = {**((item.get("explain") or {}).get("raw_metrics") or {}),
           **(item.get("strategy_raw_metrics") or {}), **(item.get("factor_scores") or {})}
    preserved = (item.get("selected_price_type") or raw.get("selected_price_type")) == "realtime"
    price = (item.get("selected_price") or raw.get("selected_price")) if preserved else None
    price = price or item.get("realtime_price") or raw.get("realtime_price")
    trade_date = ((item.get("selected_price_trade_date") or raw.get("selected_price_trade_date"))
                  if preserved else None)
    trade_date = trade_date or item.get("realtime_trade_date") or raw.get("realtime_trade_date")
    quote_time = ((item.get("selected_price_quote_time") or raw.get("selected_price_quote_time"))
                  if preserved else None)
    quote_time = quote_time or item.get("realtime_quote_time") or raw.get("realtime_quote_time")
    if price is None or not trade_date or not quote_time:
        raise ValueError("daily tracking requires its recorded realtime price/date/time")
    return {**item, "trade_date": str(trade_date)[:10], "selected_price": price,
            "selected_price_type": "realtime",
            "selected_price_source": (item.get("selected_price_source") if preserved else None)
                                     or "stock_realtime_snapshot",
            "selected_price_trade_date": str(trade_date)[:10],
            "selected_price_quote_time": quote_time}


def save_completed_run(service: DailySelectionRunService, run_id: str,
                       *, wait_seconds: float = WAIT_SECONDS) -> dict[str, Any]:
    deadline = clock.monotonic() + max(0, wait_seconds)
    while True:
        run = service.get_run(run_id)
        status = run.get("status")
        if status not in {"queued", "running"}:
            break
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            return {"status": "pending", "run_status": status,
                    "tracking_status": "pending", "reason": "selection_not_finished"}
        clock.sleep(min(POLL_SECONDS, remaining))
    if status not in {"success", "no_data"}:
        return {"status": "failed", "run_status": status, "tracking_status": "not_saved",
                "reason": "selection_not_successful", "error": run.get("error_message")}

    request = run.get("request") or {}
    if (run.get("strategy_id") != STRATEGY_ID
            or str(request.get("strategy_version")) != STRATEGY_VERSION
            or request.get("forward_protocol_id") or request.get("forward_observation_id")):
        raise ValueError("tracking save requires the pinned daily 0.4.4 task, not a research task")
    result = run.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("results"), list):
        raise ValueError("daily selection result is missing; refusing to report an empty selection")
    items = result["results"]
    codes = [item.get("code") for item in items if isinstance(item, dict)]
    if (len(items) > MAX_PICKS or len(codes) != len(items) or any(not code for code in codes)
            or len(set(codes)) != len(codes)
            or int(result.get("count", len(items))) != len(items)):
        raise ValueError("daily selection result count/codes are invalid")
    if status == "no_data" and items:
        raise ValueError("no_data task cannot contain tracking candidates")

    saved, errors = [], []
    for item in items:
        try:
            response = save_tracking_item(run_id=run_id, item=item,
                                          score_threshold=result.get("score_threshold"))
            saved.append(item["code"])
            if (response.get("intraday_cache") or {}).get("status") == "degraded":
                errors.append({"code": item["code"], "error": "intraday_enrichment_enqueue_failed"})
        except Exception as exc:
            errors.append({"code": item["code"], "error": str(exc)[:500]})
    return {"status": "failed" if errors else "completed", "run_status": status,
            "tracking_status": "failed" if errors else "saved" if items else "empty",
            "saved_count": len(saved), "saved_codes": saved, "tracking_errors": errors,
            "reason": "tracking_save_failed" if errors else None}


def run(*, dry_run: bool = False, save_only: bool = False,
        wait_seconds: float = WAIT_SECONDS) -> dict[str, Any]:
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
            result.update(status="deduplicated", run_id=existing["run_id"],
                          run_status=existing["status"], dry_run=dry_run,
                          selection_task_submitted=False)
            if not dry_run:
                result.update(save_completed_run(service, existing["run_id"], wait_seconds=wait_seconds))
            return result
        if save_only:
            return {**result, "status": "skipped", "reason": "no_daily_selection_run",
                    "dry_run": dry_run, "selection_task_submitted": False}
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
        result.update(status="submitted", reason=None, run_id=submitted["run_id"],
                      run_status=submitted["status"],
                      input_snapshot_id=submitted.get("input_snapshot_id"),
                      selection_task_submitted=True)
        result.update(save_completed_run(service, submitted["run_id"], wait_seconds=wait_seconds))
        return result
    finally:
        if lock is not None:
            error = release_mysql_advisory_lock(lock)
            if error:
                raise RuntimeError(f"daily selection lock release failed: {error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="交易日09:35提交A股舆情0.4.4，最多3只；结果保存跟踪复盘，不交易")
    parser.add_argument("--dry-run", action="store_true", help="只读检查；不创建快照、选股任务或任务日志")
    parser.add_argument("--save-only", action="store_true", help="只补存当日已有自动选股结果，绝不新建快照或选股任务")
    parser.add_argument("--wait-seconds", type=float, default=WAIT_SECONDS,
                        help="等待已有任务完成的秒数，0～300，超时退出并由既定补存调度重验")
    args = parser.parse_args(argv)
    if not 0 <= args.wait_seconds <= WAIT_SECONDS:
        parser.error("--wait-seconds must be between 0 and 300")
    logger = TaskRunLogger()
    log_id = f"{TASK_NAME}_{local_now().strftime('%Y%m%d_%H%M%S_%f')}"
    if not args.dry_run:
        logger.start(TASK_NAME, log_id, metadata=policy())
    try:
        result = run(dry_run=args.dry_run, save_only=args.save_only, wait_seconds=args.wait_seconds)
        if not args.dry_run:
            logger.finish(TASK_NAME, log_id,
                          "failed" if result["status"] in {"blocked", "failed", "pending"} else "success",
                          message=f"{result['status']}: {result.get('reason') or result.get('run_id') or ''}",
                          metadata=result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 2 if result["status"] in {"blocked", "failed", "pending"} else 0
    except Exception as exc:
        if not args.dry_run:
            logger.finish(TASK_NAME, log_id, "failed", message=str(exc)[:500],
                          metadata=policy(), error_code=type(exc).__name__)
        print(json.dumps({**policy(), "status": "failed", "error": str(exc)[:500]},
                         ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
