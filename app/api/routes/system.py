from __future__ import annotations

from fastapi import APIRouter

from app.shared.db import mysql_conn, ping_mysql

router = APIRouter(tags=["system"])


def _scalar(sql: str) -> int | None:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return None
            value = next(iter(row.values()))
            return int(value) if value is not None else None


def _latest_dates() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_latest_trade_date,
                    (SELECT MAX(updated_at) FROM stock_basic) AS stock_basic_latest_updated_at,
                    (SELECT MAX(created_at) FROM selection_result) AS selection_result_latest_created_at,
                    (SELECT MAX(trade_date) FROM selection_result) AS selection_result_latest_trade_date
                """
            )
            row = cursor.fetchone() or {}
            return {
                key: str(value) if value is not None else None
                for key, value in row.items()
            }


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
        "latest": _latest_dates(),
    }
