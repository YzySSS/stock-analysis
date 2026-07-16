from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.jobs.errors import sanitize_error_message
from app.orchestration.migrate import migration_plan
from app.shared.db import mysql_conn, ping_mysql
from app.shared.instrument_policy import (
    STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS,
    STOCK_DAILY_COMPLETENESS_RATIO,
    STOCK_INSTRUMENT_TYPE,
)


WORKER_STALE_SECONDS = 45
QUEUE_WARNING_SECONDS = 5 * 60
TASK_RUNNING_STALE_SECONDS = 60 * 60


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
    with mysql_conn() as conn:
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
    with mysql_conn() as conn:
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
    with mysql_conn() as conn:
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


def _data_snapshots() -> dict[str, Any]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(DATA_SNAPSHOT_SQL)
            row = cursor.fetchone() or {}
    return _serialize_data_snapshot(row)


def recent_error_summaries(days: int = 7, limit: int = 12) -> list[dict[str, Any]]:
    with mysql_conn() as conn:
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
    return [
        {
            **row,
            "occurrence_count": int(row.get("occurrence_count") or 0),
            "first_seen_at": str(row.get("first_seen_at")) if row.get("first_seen_at") else None,
            "last_seen_at": str(row.get("last_seen_at")) if row.get("last_seen_at") else None,
            "last_message": sanitize_error_message(row.get("last_message")),
        }
        for row in rows
    ]


def build_operational_readiness() -> dict[str, Any]:
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    if data.get("health") == "error":
        hard_reasons.append("关键数据表尚无可用数据")
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
