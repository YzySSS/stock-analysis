from __future__ import annotations

import json
from fastapi import APIRouter

from app.shared.db import mysql_conn, ping_mysql

router = APIRouter(tags=["system"])


TRACKED_TASKS = [
    "daily_kline_increment",
    "daily_kline_backfill",
    "fundamental_sync",
    "valuation_sync",
]

TASK_NAME_LABELS = {
    "daily_kline_increment": "日线增量更新",
    "daily_kline_backfill": "历史日线补齐",
    "fundamental_sync": "基本面补齐",
    "valuation_sync": "估值补齐",
}


def _scalar(sql: str) -> int | None:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return None
            value = next(iter(row.values()))
            return int(value) if value is not None else None


def _coverage_stats() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock') AS total_stock_codes,
                    (SELECT COUNT(DISTINCT dk.code) FROM daily_kline dk INNER JOIN stock_basic sb ON dk.code = sb.code WHERE sb.instrument_type='stock') AS daily_kline_covered_codes,
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock' AND (roe IS NOT NULL OR roa IS NOT NULL OR grossprofit_margin IS NOT NULL OR revenue_yoy IS NOT NULL)) AS fundamental_filled_codes,
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock' AND (pe_tushare IS NOT NULL OR pb_tushare IS NOT NULL)) AS valuation_filled_codes
                """
            )
            row = cursor.fetchone() or {}
            total_codes = int(row.get("total_stock_codes") or 0)
            covered_codes = int(row.get("daily_kline_covered_codes") or 0)
            fundamental_filled = int(row.get("fundamental_filled_codes") or 0)
            valuation_filled = int(row.get("valuation_filled_codes") or 0)
            return {
                "total_stock_codes": total_codes,
                "daily_kline_covered_codes": covered_codes,
                "daily_kline_coverage_pct": round((covered_codes / total_codes) * 100, 2) if total_codes else None,
                "fundamental_filled_codes": fundamental_filled,
                "fundamental_coverage_pct": round((fundamental_filled / total_codes) * 100, 2) if total_codes else None,
                "valuation_filled_codes": valuation_filled,
                "valuation_coverage_pct": round((valuation_filled / total_codes) * 100, 2) if total_codes else None,
            }


def _latest_dates() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_latest_trade_date,
                    (SELECT MAX(updated_at) FROM stock_basic) AS stock_basic_latest_updated_at,
                    (SELECT MAX(fundamental_updated_at) FROM stock_basic) AS fundamental_latest_updated_at,
                    (SELECT MAX(valuation_updated_at) FROM stock_basic) AS valuation_latest_updated_at,
                    (SELECT MAX(created_at) FROM selection_result) AS selection_result_latest_created_at,
                    (SELECT MAX(trade_date) FROM selection_result) AS selection_result_latest_trade_date
                """
            )
            row = cursor.fetchone() or {}
            return {
                key: str(value) if value is not None else None
                for key, value in row.items()
            }


def _field_missing_stats() -> dict:
    tracked_fields = [
        "pe_tushare",
        "pb_tushare",
        "roe",
        "roa",
        "grossprofit_margin",
        "netprofit_margin",
        "revenue_yoy",
        "profit_yoy",
    ]
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            total_sql = "SELECT COUNT(*) AS total FROM stock_basic WHERE instrument_type='stock'"
            cursor.execute(total_sql)
            total_row = cursor.fetchone() or {}
            total = int(total_row.get("total") or 0)

            select_parts = [f"SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) AS {field}_missing" for field in tracked_fields]
            sql = f"SELECT {', '.join(select_parts)} FROM stock_basic WHERE instrument_type='stock'"
            cursor.execute(sql)
            row = cursor.fetchone() or {}

            items = []
            for field in tracked_fields:
                missing = int(row.get(f"{field}_missing") or 0)
                coverage = round(((total - missing) / total) * 100, 2) if total else None
                missing_rate = round((missing / total) * 100, 2) if total else None
                items.append(
                    {
                        "field": field,
                        "missing_count": missing,
                        "coverage_pct": coverage,
                        "missing_rate_pct": missing_rate,
                    }
                )

            items.sort(key=lambda item: item["missing_count"], reverse=True)
            return {
                "total_stock_codes": total,
                "items": items,
                "worst_fields": items[:3],
            }


def _decode_metadata(value: object) -> dict | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
    return {"raw": str(value)}


def _latest_task_runs() -> list[dict]:
    placeholders = ", ".join(["%s"] * len(TRACKED_TASKS))
    sql = f"""
    SELECT t1.task_name, t1.run_id, t1.status, t1.started_at, t1.finished_at, t1.message, t1.metadata_json
    FROM task_run_log t1
    INNER JOIN (
        SELECT task_name, MAX(id) AS max_id
        FROM task_run_log
        WHERE task_name IN ({placeholders})
        GROUP BY task_name
    ) t2 ON t1.id = t2.max_id
    ORDER BY t1.id DESC
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, TRACKED_TASKS)
            rows = cursor.fetchall() or []
    items = []
    for row in rows:
        items.append(
            {
                "task_name": row.get("task_name"),
                "task_label": TASK_NAME_LABELS.get(row.get("task_name"), row.get("task_name")),
                "run_id": row.get("run_id"),
                "status": row.get("status"),
                "started_at": str(row.get("started_at")) if row.get("started_at") else None,
                "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
                "message": row.get("message"),
                "metadata": _decode_metadata(row.get("metadata_json")),
            }
        )
    return items


@router.get("/system/status")
def system_status() -> dict:
    mysql_info = ping_mysql()
    table_counts = {
        "stock_basic": _scalar("SELECT COUNT(*) AS count FROM stock_basic"),
        "daily_kline": _scalar("SELECT COUNT(*) AS count FROM daily_kline"),
        "selection_result": _scalar("SELECT COUNT(*) AS count FROM selection_result"),
    }

    return {
        "status": "ok",
        "health": {
            "status": "ok",
            "database": mysql_info.get("db"),
            "version": mysql_info.get("version"),
        },
        "table_counts": table_counts,
        "coverage": _coverage_stats(),
        "latest": _latest_dates(),
        "task_runs": _latest_task_runs(),
        "field_missing": _field_missing_stats(),
    }
