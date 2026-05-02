from __future__ import annotations

import json
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from fastapi import APIRouter

import akshare as ak

from app.shared.db import mysql_conn, ping_mysql

router = APIRouter(tags=["system"])


TRACKED_TASKS = [
    "daily_kline_increment",
    "daily_kline_backfill",
    "fundamental_sync",
    "valuation_sync",
]

KLINE_LATEST_SAMPLE_LIMIT = 20

SUSPENSION_LOOKBACK_DAYS = 30
STATUS_CACHE_TTL_SECONDS = 1800
STATUS_FILE_CACHE_PATH = Path(__file__).resolve().parents[2] / "logs" / "system_status_external_cache.json"

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


def _kline_latest_shortfall() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            latest_trade_date = row.get("latest_trade_date")
            if latest_trade_date is None:
                return {
                    "latest_trade_date": None,
                    "missing_count": 0,
                    "sample_codes": [],
                    "items": [],
                }

            cursor.execute(
                """
                SELECT sb.code, sb.name, sb.is_st, sb.is_delisted,
                       (
                         SELECT MAX(dk2.trade_date)
                         FROM daily_kline dk2
                         WHERE dk2.code = sb.code
                       ) AS last_trade_date
                FROM stock_basic sb
                LEFT JOIN daily_kline dk
                  ON dk.code = sb.code AND dk.trade_date = %s
                WHERE sb.instrument_type = 'stock'
                  AND dk.code IS NULL
                ORDER BY sb.code
                LIMIT %s
                """,
                (latest_trade_date, KLINE_LATEST_SAMPLE_LIMIT),
            )
            sample_rows = cursor.fetchall() or []

            cursor.execute(
                """
                SELECT COUNT(*) AS missing_count
                FROM stock_basic sb
                LEFT JOIN daily_kline dk
                  ON dk.code = sb.code AND dk.trade_date = %s
                WHERE sb.instrument_type = 'stock'
                  AND dk.code IS NULL
                """,
                (latest_trade_date,),
            )
            count_row = cursor.fetchone() or {}
            missing_count = int(count_row.get("missing_count") or 0)

            cache_key = _status_cache_key()
            suspension_map, paused_map = _get_external_status_maps(cache_key, str(latest_trade_date))

            items = []
            for row in sample_rows:
                code = row.get("code")
                plain_code = str(code or "").split(".")[-1]
                suspension = suspension_map.get(plain_code)
                paused_listing = paused_map.get(plain_code)
                reason_tags = []
                status_label = "source_missing"
                if paused_listing:
                    status_label = "paused_listing"
                    reason_tags.append("暂停上市")
                if suspension:
                    if status_label == "source_missing":
                        status_label = "suspended"
                    reason_tags.append("停牌")
                items.append(
                    {
                        "code": code,
                        "name": row.get("name"),
                        "is_st": bool(row.get("is_st")),
                        "is_delisted": bool(row.get("is_delisted")),
                        "last_trade_date": str(row.get("last_trade_date")) if row.get("last_trade_date") else None,
                        "gap_days": None,
                        "status_label": status_label,
                        "reason_tags": reason_tags,
                        "paused_listing_date": paused_listing,
                        "suspension": suspension,
                    }
                )

            return {
                "latest_trade_date": str(latest_trade_date),
                "missing_count": missing_count,
                "sample_codes": [item["code"] for item in items],
                "items": items,
            }


def _status_cache_key(now: datetime | None = None) -> int:
    current = now or datetime.now()
    return int(current.timestamp()) // STATUS_CACHE_TTL_SECONDS


def _load_external_status_file_cache(cache_key: int) -> dict | None:
    if not STATUS_FILE_CACHE_PATH.exists():
        return None
    try:
        payload = json.loads(STATUS_FILE_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if payload.get("cache_key") != cache_key:
        return None
    return payload


def _save_external_status_file_cache(cache_key: int, payload: dict) -> None:
    STATUS_FILE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE_CACHE_PATH.write_text(
        json.dumps(
            {
                "cache_key": cache_key,
                "cached_at": datetime.now().isoformat(timespec="seconds"),
                **payload,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _get_external_status_maps(cache_key: int, latest_trade_date_str: str) -> tuple[dict[str, dict], dict[str, str]]:
    cached = _load_external_status_file_cache(cache_key)
    if cached and cached.get("latest_trade_date") == latest_trade_date_str:
        return cached.get("suspension_map") or {}, cached.get("paused_map") or {}

    suspension_map = _fetch_recent_suspension_map_cached(cache_key, latest_trade_date_str)
    paused_map = _fetch_paused_listing_map_cached(cache_key)
    _save_external_status_file_cache(
        cache_key,
        {
            "latest_trade_date": latest_trade_date_str,
            "suspension_map": suspension_map,
            "paused_map": paused_map,
        },
    )
    return suspension_map, paused_map


@lru_cache(maxsize=4)
def _fetch_paused_listing_map_cached(_cache_key: int) -> dict[str, str]:
    try:
        df = ak.stock_info_sh_delist(symbol="全部")
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    result: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row.get("公司代码") or "").strip()
        paused_date = row.get("暂停上市日期")
        if not code or paused_date is None:
            continue
        result[code] = str(paused_date)
    return result


@lru_cache(maxsize=8)
def _fetch_recent_suspension_map_cached(_cache_key: int, latest_trade_date_str: str) -> dict[str, dict]:
    latest_trade_date: object = latest_trade_date_str
    if latest_trade_date is None:
        return {}

    latest_dt = latest_trade_date if isinstance(latest_trade_date, datetime) else datetime.strptime(str(latest_trade_date), "%Y-%m-%d")
    result: dict[str, dict] = {}
    for days_back in range(SUSPENSION_LOOKBACK_DAYS + 1):
        date_str = (latest_dt - timedelta(days=days_back)).strftime("%Y%m%d")
        try:
            df = ak.stock_tfp_em(date=date_str)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            code = str(row.get("代码") or "").strip()
            if not code or code in result:
                continue
            result[code] = {
                "suspension_date": str(row.get("停牌时间")) if row.get("停牌时间") is not None else None,
                "resume_date": str(row.get("停牌截止时间")) if row.get("停牌截止时间") is not None else None,
                "reason": row.get("停牌原因"),
                "market": row.get("所属市场"),
                "expected_resume_date": str(row.get("预计复牌时间")) if row.get("预计复牌时间") is not None else None,
            }
    return result


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


def _valuation_gap_breakdown() -> dict:
    kline_shortfall = _kline_latest_shortfall()
    shortfall_map = {
        item.get("code"): item
        for item in (kline_shortfall.get("items") or [])
        if item.get("code")
    }

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code, name, is_st, is_delisted, pe_tushare, pb_tushare, valuation_updated_at
                FROM stock_basic
                WHERE instrument_type = 'stock' AND (pe_tushare IS NULL OR pb_tushare IS NULL)
                ORDER BY code
                """
            )
            rows = cursor.fetchall() or []

    pb = {
        "missing_total": 0,
        "non_fault_missing": 0,
        "source_missing": 0,
        "actionable_missing": 0,
        "never_updated": 0,
        "sample_non_fault": [],
        "sample_source_missing": [],
        "sample_actionable": [],
    }
    pe = {
        "missing_total": 0,
        "non_fault_missing": 0,
        "source_missing": 0,
        "actionable_missing": 0,
        "never_updated": 0,
        "sample_non_fault": [],
        "sample_source_missing": [],
        "sample_actionable": [],
        "not_applicable_hint": None,
    }

    def push_sample(bucket: list, item: dict, limit: int = 10) -> None:
        if len(bucket) < limit:
            bucket.append(item)

    for row in rows:
        code = row.get("code")
        status = shortfall_map.get(code, {})
        status_label = status.get("status_label")
        is_non_fault = bool(row.get("is_delisted")) or status_label in {"paused_listing", "suspended"}
        valuation_updated_at = row.get("valuation_updated_at")
        has_pb_gap = row.get("pb_tushare") is None
        has_pe_gap = row.get("pe_tushare") is None
        item = {
            "code": code,
            "name": row.get("name"),
            "is_st": bool(row.get("is_st")),
            "status_label": status_label,
            "valuation_updated_at": str(valuation_updated_at) if valuation_updated_at else None,
        }

        if has_pb_gap:
            pb["missing_total"] += 1
            if valuation_updated_at is None:
                pb["never_updated"] += 1
            if is_non_fault:
                pb["non_fault_missing"] += 1
                push_sample(pb["sample_non_fault"], item)
            elif valuation_updated_at is None:
                pb["actionable_missing"] += 1
                push_sample(pb["sample_actionable"], item)
            else:
                pb["source_missing"] += 1
                push_sample(pb["sample_source_missing"], item)

        if has_pe_gap:
            pe["missing_total"] += 1
            if valuation_updated_at is None:
                pe["never_updated"] += 1
            if is_non_fault:
                pe["non_fault_missing"] += 1
                push_sample(pe["sample_non_fault"], item)
            elif valuation_updated_at is None:
                pe["actionable_missing"] += 1
                push_sample(pe["sample_actionable"], item)
            else:
                pe["source_missing"] += 1
                push_sample(pe["sample_source_missing"], item)

    pe["not_applicable_hint"] = "PE 缺口中可能混有亏损股票导致的口径不适用，当前先未单独拆出。"
    return {
        "pb": pb,
        "pe": pe,
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
        "kline_latest_shortfall": _kline_latest_shortfall(),
        "latest": _latest_dates(),
        "task_runs": _latest_task_runs(),
        "field_missing": _field_missing_stats(),
        "valuation_gap_breakdown": _valuation_gap_breakdown(),
    }
