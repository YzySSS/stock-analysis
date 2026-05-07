from __future__ import annotations

import json
import time
from fastapi import APIRouter

from app.shared.db import mysql_conn, ping_mysql

router = APIRouter(tags=["system"])

SYSTEM_STATUS_CACHE_TTL_SECONDS = 60
_SYSTEM_STATUS_CACHE: dict | None = None
_SYSTEM_STATUS_CACHE_AT = 0.0


TRACKED_TASKS = [
    "daily_kline_increment",
    "daily_kline_backfill",
    "fundamental_sync",
    "valuation_sync",
    "stock_status_snapshot_refresh",
    "factor_input_history_backfill",
    "factor_input_daily_update",
    "market_context_daily_update",
    "stock_sentiment_daily_update",
    "stock_realtime_snapshot_update",
    "market_fund_flow_update",
]

KLINE_LATEST_SAMPLE_LIMIT = 20

TASK_NAME_LABELS = {
    "daily_kline_increment": "日线增量更新",
    "daily_kline_backfill": "历史日线补齐",
    "fundamental_sync": "基本面补齐",
    "valuation_sync": "估值补齐",
    "stock_status_snapshot_refresh": "状态快照刷新",
    "factor_input_history_backfill": "历史输入层回填",
    "factor_input_daily_update": "历史输入层日更",
    "market_context_daily_update": "市场强度日更",
    "stock_sentiment_daily_update": "真实舆情日更",
    "stock_realtime_snapshot_update": "实时行情分钟快照",
    "market_fund_flow_update": "板块资金流快照",
}


def _basic_data_ranges() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS rows_count,
                    COUNT(DISTINCT code) AS covered_stock_codes,
                    COUNT(DISTINCT trade_date) AS covered_trade_days,
                    MIN(trade_date) AS earliest_trade_date,
                    MAX(trade_date) AS latest_trade_date,
                    MAX(updated_at) AS last_updated_at
                FROM daily_kline
                """
            )
            daily = cursor.fetchone() or {}
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS rows_count,
                    COUNT(DISTINCT code) AS covered_stock_codes,
                    COUNT(DISTINCT trade_date) AS covered_trade_days,
                    MIN(trade_date) AS earliest_trade_date,
                    MAX(trade_date) AS latest_trade_date,
                    MAX(updated_at) AS last_updated_at
                FROM factor_input_daily
                """
            )
            factor = cursor.fetchone() or {}
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS rows_count,
                    SUM(instrument_type='stock') AS covered_stock_codes,
                    MAX(updated_at) AS last_updated_at,
                    MAX(fundamental_updated_at) AS fundamental_last_updated_at,
                    MAX(valuation_updated_at) AS valuation_last_updated_at
                FROM stock_basic
                """
            )
            basic = cursor.fetchone() or {}

    def normalize(row: dict) -> dict:
        return {
            key: (str(value) if value is not None and key.endswith(("date", "at")) else int(value) if value is not None and key in {"rows_count", "covered_stock_codes", "covered_trade_days"} else value)
            for key, value in row.items()
        }

    return {
        "daily_kline": normalize(daily),
        "factor_input_daily": normalize(factor),
        "stock_basic": normalize(basic),
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

            status_map = _fetch_stock_status_snapshot_map(str(latest_trade_date))

            items = []
            for row in sample_rows:
                code = row.get("code")
                snapshot = status_map.get(code, {})
                status_label = snapshot.get("status_label") or "source_missing"
                reason_tags = []
                if status_label == "paused_listing":
                    reason_tags.append("暂停上市")
                if status_label == "suspended":
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
                        "paused_listing_date": snapshot.get("paused_listing_date"),
                        "suspension": {
                            "suspension_date": snapshot.get("suspension_date"),
                            "resume_date": snapshot.get("resume_date"),
                            "reason": snapshot.get("status_reason"),
                            "market": None,
                            "expected_resume_date": snapshot.get("expected_resume_date"),
                        } if snapshot else None,
                    }
                )

            return {
                "latest_trade_date": str(latest_trade_date),
                "missing_count": missing_count,
                "sample_codes": [item["code"] for item in items],
                "items": items,
            }


def _kline_history_completeness() -> dict:
    """Measure daily kline completeness by trade date.

    Code-level coverage can be 100% while many historical trade dates only contain
    a partial market slice.  Use current stock_basic listing_date as a pragmatic
    expected universe for each trade date, then compare actual daily_kline rows.
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date, COUNT(*) AS actual_count
                FROM daily_kline
                GROUP BY trade_date
                ORDER BY trade_date DESC
                """
            )
            daily_rows = cursor.fetchall() or []
            cursor.execute(
                """
                SELECT listing_date, COUNT(*) AS count
                FROM stock_basic
                WHERE instrument_type = 'stock' AND is_delisted = 0
                GROUP BY listing_date
                """
            )
            listing_rows = cursor.fetchall() or []

    null_listing_count = 0
    listing_counts: list[tuple[str, int]] = []
    for row in listing_rows:
        count = int(row.get("count") or 0)
        listing_date = row.get("listing_date")
        if listing_date is None:
            null_listing_count += count
        else:
            listing_counts.append((str(listing_date), count))
    listing_counts.sort(key=lambda item: item[0])

    expected_by_date: dict[str, int] = {}
    running_expected = null_listing_count
    idx = 0
    asc_trade_dates = sorted(str(row.get("trade_date")) for row in daily_rows if row.get("trade_date"))
    for trade_date in asc_trade_dates:
        while idx < len(listing_counts) and listing_counts[idx][0] <= trade_date:
            running_expected += listing_counts[idx][1]
            idx += 1
        expected_by_date[trade_date] = running_expected

    total_days = len(daily_rows)
    complete_99 = 0
    complete_95 = 0
    low_days = []
    recent_low_days = []
    min_pct = None
    for row in daily_rows:
        trade_date = str(row.get("trade_date")) if row.get("trade_date") else None
        actual_count = int(row.get("actual_count") or 0)
        expected_count = int(expected_by_date.get(trade_date or "", 0))
        pct = round((actual_count / expected_count) * 100, 2) if expected_count else 0
        min_pct = pct if min_pct is None else min(min_pct, pct)
        if pct >= 99:
            complete_99 += 1
        if pct >= 95:
            complete_95 += 1
        item = {
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "actual_count": actual_count,
            "expected_count": expected_count,
            "completeness_pct": pct,
            "missing_count": max(expected_count - actual_count, 0),
        }
        if pct < 95:
            low_days.append(item)
        if pct < 95 and len(recent_low_days) < 20:
            recent_low_days.append(item)

    return {
        "total_trade_days": total_days,
        "complete_days_99_pct": complete_99,
        "complete_days_95_pct": complete_95,
        "complete_days_99_ratio_pct": round(complete_99 / total_days * 100, 2) if total_days else None,
        "complete_days_95_ratio_pct": round(complete_95 / total_days * 100, 2) if total_days else None,
        "low_completeness_days": len(low_days),
        "min_completeness_pct": min_pct,
        "recent_low_days": recent_low_days,
    }


def _fetch_stock_status_snapshot_map(trade_date: str) -> dict[str, dict]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code, status_label, status_reason, suspension_date, resume_date,
                       paused_listing_date, expected_resume_date
                FROM stock_status_snapshot
                WHERE trade_date = %s
                """,
                (trade_date,),
            )
            rows = cursor.fetchall() or []
    result = {}
    for row in rows:
        result[row.get("code")] = {
            "status_label": row.get("status_label"),
            "status_reason": row.get("status_reason"),
            "suspension_date": str(row.get("suspension_date")) if row.get("suspension_date") else None,
            "resume_date": str(row.get("resume_date")) if row.get("resume_date") else None,
            "paused_listing_date": str(row.get("paused_listing_date")) if row.get("paused_listing_date") else None,
            "expected_resume_date": str(row.get("expected_resume_date")) if row.get("expected_resume_date") else None,
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


def _data_baseline_summary() -> dict:
    """Lightweight coverage cards for /system.

    Keep this bounded to simple aggregate queries and cache the whole endpoint.
    """
    latest = _latest_dates()
    latest_kline_date = latest.get("daily_kline_latest_trade_date")
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total FROM stock_basic WHERE instrument_type='stock'")
            total_stock = int((cursor.fetchone() or {}).get("total") or 0)

            cursor.execute(
                "SELECT COUNT(*) AS count FROM daily_kline WHERE trade_date = %s",
                (latest_kline_date,),
            )
            kline_count = int((cursor.fetchone() or {}).get("count") or 0) if latest_kline_date else 0

            cursor.execute(
                """
                SELECT
                  SUM(CASE WHEN roe IS NOT NULL OR roa IS NOT NULL OR grossprofit_margin IS NOT NULL OR revenue_yoy IS NOT NULL THEN 1 ELSE 0 END) AS fundamental_count,
                  SUM(CASE WHEN pe_tushare IS NOT NULL OR pb_tushare IS NOT NULL THEN 1 ELSE 0 END) AS valuation_count
                FROM stock_basic
                WHERE instrument_type='stock'
                """
            )
            stock_row = cursor.fetchone() or {}
            fundamental_count = int(stock_row.get("fundamental_count") or 0)
            valuation_count = int(stock_row.get("valuation_count") or 0)

            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM factor_input_daily")
            factor_date = (cursor.fetchone() or {}).get("latest_trade_date")
            cursor.execute(
                "SELECT COUNT(*) AS count FROM factor_input_daily WHERE trade_date = %s",
                (factor_date,),
            )
            factor_count = int((cursor.fetchone() or {}).get("count") or 0) if factor_date else 0

            cursor.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN latest_price IS NOT NULL AND latest_price > 0 THEN 1 ELSE 0 END) AS valid FROM stock_realtime_snapshot WHERE code REGEXP '^(sh|sz|bj)\\.'")
            realtime_row = cursor.fetchone() or {}
            realtime_total = int(realtime_row.get("total") or 0)
            realtime_valid = int(realtime_row.get("valid") or 0)

            cursor.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN net_amount IS NOT NULL THEN 1 ELSE 0 END) AS valid FROM market_sector_fund_flow_snapshot")
            fund_flow_row = cursor.fetchone() or {}
            fund_flow_total = int(fund_flow_row.get("total") or 0)
            fund_flow_valid = int(fund_flow_row.get("valid") or 0)

    sentiment = _sentiment_quality_stats()
    sentiment_effective = int(sentiment.get("effective_news_count") or 0)
    sentiment_raw = int(sentiment.get("raw_news_count") or 0)

    def pct(done: int, total: int) -> float | None:
        return round(done / total * 100, 2) if total else None

    return {
        "items": [
            {"key": "kline", "label": "日K线", "value": pct(kline_count, total_stock), "done": kline_count, "total": total_stock, "unit": "记录数"},
            {"key": "fundamental", "label": "基本面", "value": pct(fundamental_count, total_stock), "done": fundamental_count, "total": total_stock, "unit": "记录数"},
            {"key": "valuation", "label": "估值数据", "value": pct(valuation_count, total_stock), "done": valuation_count, "total": total_stock, "unit": "记录数"},
            {"key": "factor", "label": "因子输入", "value": pct(factor_count, total_stock), "done": factor_count, "total": total_stock, "unit": "记录数"},
            {"key": "realtime", "label": "实时快照", "value": pct(realtime_valid, realtime_total), "done": realtime_valid, "total": realtime_total, "unit": "记录数"},
            {"key": "fundflow", "label": "板块资金流", "value": pct(fund_flow_valid, fund_flow_total), "done": fund_flow_valid, "total": fund_flow_total, "unit": "记录数"},
            {"key": "sentiment", "label": "情绪质量", "value": sentiment.get("avg_quality"), "done": sentiment_effective, "total": sentiment_raw, "unit": "有效新闻 / 总新闻"},
        ]
    }


def _sentiment_quality_stats() -> dict:
    """Lightweight news quality snapshot for the system page.

    Keep this query bounded to stock_sentiment_daily/stock_news aggregates so
    /api/system/status stays fast and does not scan行情/因子大表。
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM stock_sentiment_daily")
            latest_row = cursor.fetchone() or {}
            latest_trade_date = latest_row.get("latest_trade_date")
            if not latest_trade_date:
                return {
                    "latest_trade_date": None,
                    "stock_count": 0,
                    "raw_news_count": 0,
                    "effective_news_count": 0,
                    "filtered_out_count": 0,
                    "filtered_out_pct": None,
                    "avg_credibility": None,
                    "avg_quality": None,
                    "quality_levels": [],
                    "credibility_levels": [],
                }

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS stock_count,
                    COALESCE(SUM(CASE WHEN raw_news_count > 0 THEN raw_news_count ELSE news_count END), 0) AS raw_news_count,
                    COALESCE(SUM(news_count), 0) AS effective_news_count,
                    AVG(credibility_avg) AS avg_credibility,
                    AVG(quality_avg) AS avg_quality
                FROM stock_sentiment_daily
                WHERE trade_date = %s
                """,
                (latest_trade_date,),
            )
            row = cursor.fetchone() or {}
            raw_news_count = int(row.get("raw_news_count") or 0)
            effective_news_count = int(row.get("effective_news_count") or 0)
            filtered_out_count = max(raw_news_count - effective_news_count, 0)

            cursor.execute(
                """
                SELECT quality_level, COUNT(*) AS count
                FROM stock_news
                WHERE quality_level IS NOT NULL
                  AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY quality_level
                ORDER BY FIELD(quality_level, 'high', 'medium', 'low', 'very_low'), quality_level
                """
            )
            quality_levels = cursor.fetchall() or []
            cursor.execute(
                """
                SELECT credibility_level, COUNT(*) AS count
                FROM stock_news
                WHERE credibility_level IS NOT NULL
                  AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY credibility_level
                ORDER BY FIELD(credibility_level, 'S', 'A', 'B', 'C', 'D'), credibility_level
                """
            )
            credibility_levels = cursor.fetchall() or []

    return {
        "latest_trade_date": str(latest_trade_date),
        "stock_count": int(row.get("stock_count") or 0),
        "raw_news_count": raw_news_count,
        "effective_news_count": effective_news_count,
        "filtered_out_count": filtered_out_count,
        "filtered_out_pct": round(filtered_out_count / raw_news_count * 100, 2) if raw_news_count else None,
        "avg_credibility": round(float(row.get("avg_credibility")), 4) if row.get("avg_credibility") is not None else None,
        "avg_quality": round(float(row.get("avg_quality")), 2) if row.get("avg_quality") is not None else None,
        "quality_levels": [{"level": item.get("quality_level"), "count": int(item.get("count") or 0)} for item in quality_levels],
        "credibility_levels": [{"level": item.get("credibility_level"), "count": int(item.get("count") or 0)} for item in credibility_levels],
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
    latest_trade_date = _latest_dates().get("daily_kline_latest_trade_date")
    status_map = _fetch_stock_status_snapshot_map(latest_trade_date) if latest_trade_date else {}

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code, name, is_st, is_delisted, pe_tushare, pb_tushare, valuation_updated_at, profit_yoy, eps
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
        "not_applicable_missing": 0,
        "source_missing": 0,
        "actionable_missing": 0,
        "never_updated": 0,
        "sample_non_fault": [],
        "sample_not_applicable": [],
        "sample_source_missing": [],
        "sample_actionable": [],
        "not_applicable_hint": None,
    }

    def push_sample(bucket: list, item: dict, limit: int = 10) -> None:
        if len(bucket) < limit:
            bucket.append(item)

    for row in rows:
        code = row.get("code")
        status = status_map.get(code, {})
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
            "status_reason": status.get("status_reason"),
            "valuation_updated_at": str(valuation_updated_at) if valuation_updated_at else None,
            "profit_yoy": float(row.get("profit_yoy")) if row.get("profit_yoy") is not None else None,
            "eps": float(row.get("eps")) if row.get("eps") is not None else None,
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

            eps = row.get("eps")
            profit_yoy = row.get("profit_yoy")
            is_not_applicable = eps is not None and float(eps) <= 0
            is_loss_like = profit_yoy is not None and float(profit_yoy) < 0

            if is_non_fault:
                pe["non_fault_missing"] += 1
                push_sample(pe["sample_non_fault"], item)
            elif is_not_applicable:
                pe["not_applicable_missing"] += 1
                push_sample(pe["sample_not_applicable"], item)
            elif is_loss_like:
                pe["not_applicable_missing"] += 1
                push_sample(pe["sample_not_applicable"], item)
            elif valuation_updated_at is None:
                pe["actionable_missing"] += 1
                push_sample(pe["sample_actionable"], item)
            else:
                pe["source_missing"] += 1
                push_sample(pe["sample_source_missing"], item)

    pe["not_applicable_hint"] = "当前优先按 eps <= 0 识别“PE 不适用”；若 eps 暂缺，再退化用 profit_yoy < 0 近似识别。"
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


def _scheduled_tasks() -> list[dict]:
    return [
        {"task_name": "daily_kline_increment", "task_label": "日线增量更新", "schedule": "每天 02:00"},
        {"task_name": "daily_kline_backfill", "task_label": "历史日线补齐", "schedule": "每天 02:15"},
        {"task_name": "fundamental_sync", "task_label": "基本面补齐", "schedule": "每天 02:40"},
        {"task_name": "valuation_sync", "task_label": "估值补齐", "schedule": "每天 02:50"},
        {"task_name": "stock_status_snapshot_refresh", "task_label": "状态快照刷新", "schedule": "每天 03:05"},
        {"task_name": "factor_input_history_backfill", "task_label": "历史输入层回填", "schedule": "按需 / 后台批次"},
        {"task_name": "factor_input_daily_update", "task_label": "历史输入层日更", "schedule": "每天 03:20"},
        {"task_name": "market_context_daily_update", "task_label": "市场强度日更", "schedule": "每天 03:35"},
        {"task_name": "stock_sentiment_daily_update", "task_label": "真实舆情日更", "schedule": "每天 03:50"},
        {"task_name": "stock_realtime_snapshot_update", "task_label": "实时行情分钟快照", "schedule": "交易日 09:00-15:59 每分钟，脚本内判断盘中时间/失败降级"},
        {"task_name": "market_fund_flow_update", "task_label": "板块资金流快照", "schedule": "交易日 09:00-15:59 每 3 分钟"},
    ]


@router.get("/system/status")
def system_status() -> dict:
    global _SYSTEM_STATUS_CACHE, _SYSTEM_STATUS_CACHE_AT
    now = time.monotonic()
    if _SYSTEM_STATUS_CACHE and now - _SYSTEM_STATUS_CACHE_AT < SYSTEM_STATUS_CACHE_TTL_SECONDS:
        cached = dict(_SYSTEM_STATUS_CACHE)
        cached["cache"] = {
            "hit": True,
            "ttl_seconds": SYSTEM_STATUS_CACHE_TTL_SECONDS,
            "age_seconds": round(now - _SYSTEM_STATUS_CACHE_AT, 2),
        }
        return cached

    mysql_info = ping_mysql()
    payload = {
        "status": "ok",
        "health": {
            "status": "ok",
            "database": mysql_info.get("db"),
            "version": mysql_info.get("version"),
        },
        "latest": _latest_dates(),
        "sentiment_quality": _sentiment_quality_stats(),
        "data_baseline": _data_baseline_summary(),
        "scheduled_tasks": _scheduled_tasks(),
        "task_runs": _latest_task_runs(),
    }
    _SYSTEM_STATUS_CACHE = dict(payload)
    _SYSTEM_STATUS_CACHE_AT = now
    payload["cache"] = {
        "hit": False,
        "ttl_seconds": SYSTEM_STATUS_CACHE_TTL_SECONDS,
        "age_seconds": 0,
    }
    return payload
