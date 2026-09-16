from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any

from app.jobs.errors import sanitize_error_message
from app.orchestration.migrate import migration_plan
from app.shared.db import mysql_read_conn, ping_mysql
from app.shared.instrument_policy import (
    STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS,
    STOCK_DAILY_COMPLETENESS_RATIO,
    STOCK_INSTRUMENT_TYPE,
)
from app.shared.market_clock import SHANGHAI_TZ, to_shanghai_wall_clock


WORKER_STALE_SECONDS = 45
QUEUE_WARNING_SECONDS = 5 * 60
TASK_RUNNING_STALE_SECONDS = 60 * 60
DATA_FRESHNESS_CUTOFF_TIME = time(18, 45)
INDEPENDENT_CALENDAR_EXCHANGE = "SSE"
INDEPENDENT_CALENDAR_SOURCE = "tushare.trade_cal"
INDEPENDENT_CALENDAR_RECENT_DAYS = 46


@dataclass(frozen=True)
class WorkerDefinition:
    worker_type: str
    label: str
    table: str
    id_column: str = "run_id"
    heartbeat_column: str = "worker_heartbeat_at"
    stale_seconds: int = 15 * 60


WORKER_DEFINITIONS = (
    WorkerDefinition("backtest", "回测 Worker", "backtest_run", stale_seconds=30 * 60),
    WorkerDefinition("selection", "选股 Worker", "selection_run"),
    WorkerDefinition(
        "durable_task",
        "API 持久异步任务 Worker",
        "durable_task",
        id_column="task_id",
        stale_seconds=5 * 60,
    ),
    WorkerDefinition(
        "portfolio_advice",
        "持仓建议 Worker",
        "portfolio_advice_run",
        id_column="id",
        stale_seconds=5 * 60,
    ),
)


CRITICAL_TASKS = (
    ("stock_basic_sync", "股票基础信息同步"),
    ("daily_kline_increment", "日线增量更新"),
    ("factor_input_daily_update", "历史输入层日更"),
)


DATA_SNAPSHOT_SQL = f"""
SELECT
    (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_latest_available_trade_date,
    (
        SELECT k.trade_date
        FROM daily_kline k
        WHERE k.trade_date >= DATE_SUB(
            (SELECT MAX(trade_date) FROM daily_kline),
            INTERVAL {STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS} DAY
        )
        GROUP BY k.trade_date
        HAVING COUNT(*) >= (
            SELECT COUNT(*) * {STOCK_DAILY_COMPLETENESS_RATIO}
            FROM stock_basic
            WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}'
        )
        ORDER BY k.trade_date DESC
        LIMIT 1
    ) AS daily_kline_latest_complete_trade_date,
    (SELECT MAX(trade_date) FROM factor_input_daily) AS factor_input_latest_trade_date,
    (SELECT MAX(updated_at) FROM stock_basic) AS stock_basic_latest_updated_at
"""


INDEPENDENT_CALENDAR_SNAPSHOT_SQL = f"""
SELECT
    COUNT(*) AS calendar_row_count,
    MIN(cal_date) AS calendar_coverage_start_date,
    MAX(cal_date) AS calendar_coverage_end_date,
    MAX(updated_at) AS calendar_latest_updated_at,
    COALESCE(SUM(CASE WHEN cal_date=%s THEN 1 ELSE 0 END), 0) AS current_date_row_count,
    MAX(CASE WHEN cal_date=%s THEN is_open ELSE NULL END) AS current_date_is_open,
    COALESCE(SUM(CASE WHEN source<>%s THEN 1 ELSE 0 END), 0) AS unexpected_source_count,
    COALESCE(SUM(
        CASE WHEN cal_date BETWEEN DATE_SUB(
            %s, INTERVAL {INDEPENDENT_CALENDAR_RECENT_DAYS - 1} DAY
        ) AND %s THEN 1 ELSE 0 END
    ), 0) AS recent_calendar_day_count,
    MAX(
        CASE WHEN is_open=1 AND cal_date<=%s THEN cal_date ELSE NULL END
    ) AS expected_latest_trade_date,
    COALESCE(SUM(
        CASE WHEN is_open=1
                  AND cal_date>COALESCE(%s, '1000-01-01')
                  AND cal_date<=%s
             THEN 1 ELSE 0 END
    ), 0) AS daily_kline_missing_trade_days,
    COALESCE(SUM(
        CASE WHEN is_open=1
                  AND cal_date>COALESCE(%s, '1000-01-01')
                  AND cal_date<=%s
             THEN 1 ELSE 0 END
    ), 0) AS factor_input_missing_trade_days
FROM etf_rotation_trade_calendar
WHERE exchange_code=%s
"""


def classify_worker_snapshot(row: dict[str, Any] | None, stale_seconds: int = WORKER_STALE_SECONDS) -> str:
    if not row:
        return "missing"
    if row.get("status") in {"stopped", "replaced"}:
        return "stopped"
    age = row.get("heartbeat_age_seconds")
    if age is None or int(age) > stale_seconds:
        return "stale"
    return "healthy"


def _worker_snapshots() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            for definition in WORKER_DEFINITIONS:
                cursor.execute(
                    """
                    SELECT worker_type, worker_id, status, current_job_id,
                           started_at, heartbeat_at, last_job_started_at,
                           last_job_finished_at, stopped_at,
                           TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS heartbeat_age_seconds
                    FROM worker_runtime_heartbeat
                    WHERE worker_type=%s
                    ORDER BY heartbeat_at DESC
                    LIMIT 1
                    """,
                    (definition.worker_type,),
                )
                row = cursor.fetchone()
                health = classify_worker_snapshot(row)
                items.append(
                    {
                        "worker_type": definition.worker_type,
                        "label": definition.label,
                        "health": health,
                        "worker_id": row.get("worker_id") if row else None,
                        "process_status": row.get("status") if row else None,
                        "current_job_id": str(row.get("current_job_id")) if row and row.get("current_job_id") is not None else None,
                        "started_at": str(row.get("started_at")) if row and row.get("started_at") else None,
                        "heartbeat_at": str(row.get("heartbeat_at")) if row and row.get("heartbeat_at") else None,
                        "heartbeat_age_seconds": int(row.get("heartbeat_age_seconds")) if row and row.get("heartbeat_age_seconds") is not None else None,
                        "last_job_started_at": str(row.get("last_job_started_at")) if row and row.get("last_job_started_at") else None,
                        "last_job_finished_at": str(row.get("last_job_finished_at")) if row and row.get("last_job_finished_at") else None,
                        "stale_after_seconds": WORKER_STALE_SECONDS,
                    }
                )
    return items


def _queue_snapshots() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            for definition in WORKER_DEFINITIONS:
                cursor.execute(
                    f"""
                    SELECT
                        COALESCE(SUM(status='queued'), 0) AS queued_count,
                        COALESCE(SUM(status='running'), 0) AS running_count,
                        COALESCE(SUM(status='failed' AND finished_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)), 0) AS failed_24h_count,
                        COALESCE(SUM(
                            status='running'
                            AND COALESCE({definition.heartbeat_column}, started_at, locked_at)
                                < DATE_SUB(NOW(), INTERVAL %s SECOND)
                        ), 0) AS stale_running_count,
                        TIMESTAMPDIFF(
                            SECOND,
                            MIN(CASE WHEN status='queued' THEN created_at END),
                            NOW()
                        ) AS oldest_queued_age_seconds
                    FROM {definition.table}
                    """,
                    (definition.stale_seconds,),
                )
                row = cursor.fetchone() or {}
                queued_count = int(row.get("queued_count") or 0)
                stale_running_count = int(row.get("stale_running_count") or 0)
                oldest_age = int(row.get("oldest_queued_age_seconds")) if row.get("oldest_queued_age_seconds") is not None else None
                health = "error" if stale_running_count else "warning" if oldest_age is not None and oldest_age > QUEUE_WARNING_SECONDS else "healthy"
                items.append(
                    {
                        "job_type": definition.worker_type,
                        "label": definition.label,
                        "health": health,
                        "queued_count": queued_count,
                        "running_count": int(row.get("running_count") or 0),
                        "failed_24h_count": int(row.get("failed_24h_count") or 0),
                        "stale_running_count": stale_running_count,
                        "oldest_queued_age_seconds": oldest_age,
                        "stale_after_seconds": definition.stale_seconds,
                    }
                )
    return items


def _critical_task_snapshots() -> list[dict[str, Any]]:
    names = [item[0] for item in CRITICAL_TASKS]
    labels = dict(CRITICAL_TASKS)
    placeholders = ", ".join(["%s"] * len(names))
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT t.task_name, t.run_id, t.status, t.started_at, t.finished_at,
                       t.error_code, t.message,
                       CASE WHEN t.status='running'
                            THEN TIMESTAMPDIFF(SECOND, t.started_at, NOW())
                            ELSE NULL END AS running_age_seconds
                FROM task_run_log t
                INNER JOIN (
                    SELECT task_name, MAX(id) AS max_id
                    FROM task_run_log
                    WHERE task_name IN ({placeholders})
                    GROUP BY task_name
                ) latest ON latest.max_id=t.id
                """,
                names,
            )
            rows = {row["task_name"]: row for row in (cursor.fetchall() or [])}

    items: list[dict[str, Any]] = []
    for task_name in names:
        row = rows.get(task_name)
        recorded_status = row.get("status") if row else None
        running_age = int(row.get("running_age_seconds")) if row and row.get("running_age_seconds") is not None else None
        stale = recorded_status == "running" and (running_age or 0) > TASK_RUNNING_STALE_SECONDS
        status = "stale" if stale else recorded_status or "missing"
        health = "healthy" if status == "success" else "warning" if status == "partial_success" else "error" if status in {"failed", "killed", "stale"} else "unknown"
        items.append(
            {
                "task_name": task_name,
                "label": labels[task_name],
                "health": health,
                "status": status,
                "recorded_status": recorded_status,
                "run_id": row.get("run_id") if row else None,
                "started_at": str(row.get("started_at")) if row and row.get("started_at") else None,
                "finished_at": str(row.get("finished_at")) if row and row.get("finished_at") else None,
                "running_age_seconds": running_age,
                "error_code": row.get("error_code") if row else None,
            }
        )
    return items


def _serialize_data_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    available_date = (
        str(row.get("daily_kline_latest_available_trade_date"))
        if row.get("daily_kline_latest_available_trade_date")
        else None
    )
    daily_date = (
        str(row.get("daily_kline_latest_complete_trade_date"))
        if row.get("daily_kline_latest_complete_trade_date")
        else None
    )
    factor_date = str(row.get("factor_input_latest_trade_date")) if row.get("factor_input_latest_trade_date") else None
    missing = [
        key
        for key, value in (
            ("daily_kline", daily_date),
            ("factor_input_daily", factor_date),
            ("stock_basic", row.get("stock_basic_latest_updated_at")),
        )
        if value is None
    ]
    return {
        "health": "error" if missing else "warning" if daily_date and factor_date and factor_date < daily_date else "healthy",
        "daily_kline_latest_trade_date": daily_date,
        "daily_kline_latest_available_trade_date": available_date,
        "daily_kline_latest_is_partial": bool(available_date and daily_date and available_date != daily_date),
        "factor_input_latest_trade_date": factor_date,
        "stock_basic_latest_updated_at": str(row.get("stock_basic_latest_updated_at")) if row.get("stock_basic_latest_updated_at") else None,
        "factor_input_lags_daily_kline": bool(daily_date and factor_date and factor_date < daily_date),
        "missing": missing,
    }


def _freshness_reference(now: datetime | None = None) -> dict[str, Any]:
    wall_clock = to_shanghai_wall_clock(now or datetime.now(SHANGHAI_TZ))
    current_date = wall_clock.date()
    completed_through_date = (
        current_date
        if wall_clock.time() >= DATA_FRESHNESS_CUTOFF_TIME
        else current_date - timedelta(days=1)
    )
    return {
        "current_date": current_date,
        "completed_through_date": completed_through_date,
        "cutoff_time": DATA_FRESHNESS_CUTOFF_TIME.strftime("%H:%M:%S"),
        "timezone": "Asia/Shanghai",
    }


def _date_text(value: Any) -> str | None:
    return str(value) if value is not None else None


def _serialize_independent_calendar_snapshot(
    row: dict[str, Any],
    *,
    reference: dict[str, Any],
) -> dict[str, Any]:
    current_date = reference["current_date"]
    calendar_rows = int(row.get("calendar_row_count") or 0)
    current_date_rows = int(row.get("current_date_row_count") or 0)
    recent_days = int(row.get("recent_calendar_day_count") or 0)
    unexpected_sources = int(row.get("unexpected_source_count") or 0)
    expected_trade_date = _date_text(row.get("expected_latest_trade_date"))
    issues: list[str] = []

    if calendar_rows == 0:
        issues.append("独立交易日历无可用数据")
    if current_date_rows != 1:
        issues.append(f"独立交易日历未完整覆盖当前日期 {current_date}")
    if recent_days != INDEPENDENT_CALENDAR_RECENT_DAYS:
        issues.append(
            "独立交易日历最近 "
            f"{INDEPENDENT_CALENDAR_RECENT_DAYS} 个自然日不连续（实际 {recent_days} 日）"
        )
    if unexpected_sources:
        issues.append(
            "独立交易日历包含 "
            f"{unexpected_sources} 条非 {INDEPENDENT_CALENDAR_SOURCE} 来源记录"
        )
    if expected_trade_date is None:
        issues.append(
            "独立交易日历无法确定截至 "
            f"{reference['completed_through_date']} 的最近交易日"
        )

    current_date_is_open = row.get("current_date_is_open")
    return {
        "health": "error" if issues else "healthy",
        "exchange_code": INDEPENDENT_CALENDAR_EXCHANGE,
        "source": INDEPENDENT_CALENDAR_SOURCE,
        "timezone": reference["timezone"],
        "cutoff_time": reference["cutoff_time"],
        "current_date": str(current_date),
        "current_date_is_open": (
            bool(int(current_date_is_open))
            if current_date_is_open is not None
            else None
        ),
        "completed_through_date": str(reference["completed_through_date"]),
        "expected_latest_trade_date": expected_trade_date,
        "coverage_start_date": _date_text(row.get("calendar_coverage_start_date")),
        "coverage_end_date": _date_text(row.get("calendar_coverage_end_date")),
        "latest_updated_at": _date_text(row.get("calendar_latest_updated_at")),
        "recent_expected_calendar_days": INDEPENDENT_CALENDAR_RECENT_DAYS,
        "recent_actual_calendar_days": recent_days,
        "daily_kline_missing_trade_days": int(
            row.get("daily_kline_missing_trade_days") or 0
        ),
        "factor_input_missing_trade_days": int(
            row.get("factor_input_missing_trade_days") or 0
        ),
        "issues": issues,
    }


def _merge_independent_freshness(
    data: dict[str, Any],
    calendar: dict[str, Any],
) -> dict[str, Any]:
    merged = {
        **data,
        "independent_calendar": calendar,
        "expected_latest_trade_date": calendar.get("expected_latest_trade_date"),
        "daily_kline_missing_trade_days": int(
            calendar.get("daily_kline_missing_trade_days") or 0
        ),
        "factor_input_missing_trade_days": int(
            calendar.get("factor_input_missing_trade_days") or 0
        ),
    }
    stale = (
        merged["daily_kline_missing_trade_days"] > 0
        or merged["factor_input_missing_trade_days"] > 0
    )
    if calendar.get("health") != "healthy" or stale:
        merged["health"] = "error"
    return merged


def _data_hard_reasons(data: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if data.get("missing"):
        reasons.append("关键数据表尚无可用数据")

    calendar = data.get("independent_calendar") or {}
    if calendar.get("health") != "healthy":
        reasons.extend(str(item) for item in (calendar.get("issues") or []))
        return reasons

    expected_date = data.get("expected_latest_trade_date")
    daily_missing = int(data.get("daily_kline_missing_trade_days") or 0)
    factor_missing = int(data.get("factor_input_missing_trade_days") or 0)
    if daily_missing:
        reasons.append(
            "日线落后独立交易日历 "
            f"{daily_missing} 个交易日（最新 {data.get('daily_kline_latest_trade_date')}，"
            f"应到 {expected_date}）"
        )
    if factor_missing:
        reasons.append(
            "历史输入层落后独立交易日历 "
            f"{factor_missing} 个交易日（最新 {data.get('factor_input_latest_trade_date')}，"
            f"应到 {expected_date}）"
        )
    return reasons


def _data_snapshots(now: datetime | None = None) -> dict[str, Any]:
    reference = _freshness_reference(now)
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(DATA_SNAPSHOT_SQL)
            row = cursor.fetchone() or {}
            cursor.execute(
                INDEPENDENT_CALENDAR_SNAPSHOT_SQL,
                (
                    reference["current_date"],
                    reference["current_date"],
                    INDEPENDENT_CALENDAR_SOURCE,
                    reference["current_date"],
                    reference["current_date"],
                    reference["completed_through_date"],
                    row.get("daily_kline_latest_complete_trade_date"),
                    reference["completed_through_date"],
                    row.get("factor_input_latest_trade_date"),
                    reference["completed_through_date"],
                    INDEPENDENT_CALENDAR_EXCHANGE,
                ),
            )
            calendar_row = cursor.fetchone() or {}
    data = _serialize_data_snapshot(row)
    calendar = _serialize_independent_calendar_snapshot(
        calendar_row,
        reference=reference,
    )
    return _merge_independent_freshness(data, calendar)


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except ValueError:
        return None


def _classify_error_recovery(
    error_summary: dict[str, Any],
    latest_run: dict[str, Any] | None,
) -> dict[str, Any]:
    latest_status = str((latest_run or {}).get("status") or "")
    latest_at_value = (latest_run or {}).get("finished_at") or (latest_run or {}).get("started_at")
    latest_at = _as_datetime(latest_at_value)
    latest_success_at_value = (latest_run or {}).get("latest_success_at")
    latest_partial_at_value = (latest_run or {}).get("latest_partial_success_at")
    latest_success_at = _as_datetime(latest_success_at_value)
    latest_partial_at = _as_datetime(latest_partial_at_value)
    error_at = _as_datetime(error_summary.get("last_seen_at"))
    base = {
        "latest_run_status": latest_status or None,
        "latest_run_at": str(latest_at_value) if latest_at_value else None,
        "recovery_run_status": None,
        "recovery_run_at": None,
    }

    if error_summary.get("source_kind") != "scheduled_task":
        return {
            **base,
            "recovery_status": "historical",
            "recovery_label": "历史记录",
        }
    if latest_run is None:
        return {
            **base,
            "recovery_status": "unresolved",
            "recovery_label": "未见后续运行",
        }
    if error_at is None:
        return {
            **base,
            "recovery_status": "unresolved",
            "recovery_label": "未见后续成功",
        }

    successful_recovery = latest_success_at is not None and latest_success_at >= error_at
    partial_recovery = latest_partial_at is not None and latest_partial_at >= error_at
    if successful_recovery and (
        not partial_recovery or latest_success_at >= latest_partial_at
    ):
        return {
            **base,
            "recovery_run_status": "success",
            "recovery_run_at": str(latest_success_at_value),
            "recovery_status": "recovered",
            "recovery_label": "已恢复",
        }
    if partial_recovery:
        return {
            **base,
            "recovery_run_status": "partial_success",
            "recovery_run_at": str(latest_partial_at_value),
            "recovery_status": "partially_recovered",
            "recovery_label": "后续部分成功",
        }
    if latest_status == "running" and latest_at is not None and latest_at >= error_at:
        return {
            **base,
            "recovery_status": "running_after_error",
            "recovery_label": "后续运行中",
        }
    return {
        **base,
        "recovery_status": "unresolved",
        "recovery_label": "未恢复",
    }


def recent_error_summaries(days: int = 7, limit: int = 12) -> list[dict[str, Any]]:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_kind, job_type, error_code,
                       SUM(occurrence_count) AS occurrence_count,
                       MIN(first_seen_at) AS first_seen_at,
                       MAX(last_seen_at) AS last_seen_at,
                       MAX(last_message) AS last_message
                FROM job_error_daily_summary
                WHERE error_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                GROUP BY source_kind, job_type, error_code
                ORDER BY last_seen_at DESC, occurrence_count DESC
                LIMIT %s
                """,
                (max(days - 1, 0), limit),
            )
            rows = cursor.fetchall() or []
            task_names = sorted(
                {
                    str(row.get("job_type"))
                    for row in rows
                    if row.get("source_kind") == "scheduled_task" and row.get("job_type")
                }
            )
            latest_runs: dict[str, dict[str, Any]] = {}
            if task_names:
                placeholders = ", ".join(["%s"] * len(task_names))
                cursor.execute(
                    f"""
                    SELECT latest.task_name, latest.status, latest.started_at, latest.finished_at,
                           task_ids.latest_success_at, task_ids.latest_partial_success_at
                    FROM task_run_log latest
                    INNER JOIN (
                        SELECT task_name,
                               MAX(id) AS max_id,
                               MAX(
                                   CASE WHEN status='success'
                                   THEN COALESCE(finished_at, started_at)
                                   ELSE NULL END
                               ) AS latest_success_at,
                               MAX(
                                   CASE WHEN status='partial_success'
                                   THEN COALESCE(finished_at, started_at)
                                   ELSE NULL END
                               ) AS latest_partial_success_at
                        FROM task_run_log
                        WHERE task_name IN ({placeholders})
                        GROUP BY task_name
                    ) task_ids ON latest.id = task_ids.max_id
                    """,
                    task_names,
                )
                latest_runs = {
                    str(row.get("task_name")): row
                    for row in (cursor.fetchall() or [])
                    if row.get("task_name")
                }

    items = []
    for row in rows:
        item = {
            **row,
            "occurrence_count": int(row.get("occurrence_count") or 0),
            "first_seen_at": str(row.get("first_seen_at")) if row.get("first_seen_at") else None,
            "last_seen_at": str(row.get("last_seen_at")) if row.get("last_seen_at") else None,
            "last_message": sanitize_error_message(row.get("last_message")),
        }
        item.update(_classify_error_recovery(item, latest_runs.get(str(row.get("job_type")))))
        items.append(item)
    return items


def build_operational_readiness() -> dict[str, Any]:
    checked_at = to_shanghai_wall_clock(datetime.now(SHANGHAI_TZ)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    try:
        mysql_info = ping_mysql()
        workers = _worker_snapshots()
        queues = _queue_snapshots()
        critical_tasks = _critical_task_snapshots()
        data = _data_snapshots()
        schema_plan = migration_plan()
    except Exception as exc:
        return {
            "status": "not_ready",
            "accepting_jobs": False,
            "checked_at": checked_at,
            "database": {"health": "error", "error": sanitize_error_message(exc, limit=300)},
            "workers": [],
            "queues": [],
            "critical_tasks": [],
            "data": {"health": "unknown"},
            "schema_migrations": {"health": "unknown"},
            "reasons": ["数据库或任务健康查询失败"],
        }

    hard_reasons = [
        f"{item['label']} {item['health']}"
        for item in workers
        if item["health"] != "healthy"
    ]
    hard_reasons.extend(
        f"{item['label']} 存在 {item['stale_running_count']} 个失联任务"
        for item in queues
        if item["stale_running_count"]
    )
    hard_reasons.extend(_data_hard_reasons(data))
    if not schema_plan.get("ready"):
        hard_reasons.append(f"数据库存在 {schema_plan.get('pending', 0)} 个待执行 migration")

    warning_reasons = [
        f"{item['label']} 队列等待超过 {QUEUE_WARNING_SECONDS} 秒"
        for item in queues
        if item["health"] == "warning"
    ]
    warning_reasons.extend(
        f"{item['label']} 最近状态为 {item['status']}"
        for item in critical_tasks
        if item["health"] != "healthy"
    )
    if data.get("health") == "warning":
        warning_reasons.append("历史输入层落后于最新日线交易日")

    status = "not_ready" if hard_reasons else "degraded" if warning_reasons else "ready"
    return {
        "status": status,
        "accepting_jobs": status != "not_ready",
        "checked_at": checked_at,
        "database": {
            "health": "healthy",
            "database": mysql_info.get("db"),
            "version": mysql_info.get("version"),
        },
        "workers": workers,
        "queues": queues,
        "critical_tasks": critical_tasks,
        "data": data,
        "schema_migrations": {
            "health": "healthy" if schema_plan.get("ready") else "error",
            "target": schema_plan.get("target"),
            "total": schema_plan.get("total"),
            "applied": schema_plan.get("applied"),
            "pending": schema_plan.get("pending"),
            "pending_versions": [
                item.get("version")
                for item in schema_plan.get("items", [])
                if item.get("status") != "applied"
            ],
        },
        "reasons": [*hard_reasons, *warning_reasons],
    }
