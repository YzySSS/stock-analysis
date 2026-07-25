from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, time as datetime_time
from fastapi import APIRouter

from app.data_ingestion.realtime_lifecycle import (
    FULL_MARKET_RAW_TRADE_DAYS,
    ROLLUP_INTERVALS,
    ROLLUP_TRADE_DAYS,
    TRACKED_RAW_TRADE_DAYS,
)
from app.jobs.readiness import build_operational_readiness, recent_error_summaries
from app.shared.cache import get_cache_backend
from app.shared.db import mysql_read_conn, ping_mysql
from app.shared.instrument_policy import STOCK_DAILY_COMPLETENESS_RATIO, STOCK_INSTRUMENT_TYPE

router = APIRouter(tags=["system"])

SYSTEM_STATUS_CACHE_TTL_SECONDS = 60
SYSTEM_STATUS_CACHE_KEY = "system:status:v4"


TASK_SCHEDULES = [
    {"task_name": "stock_basic_sync", "task_label": "股票基础信息同步", "schedule": "每天 01:30"},
    {"task_name": "daily_kline_increment", "task_label": "日线增量更新", "schedule": "每天 02:00"},
    {"task_name": "stock_technical_feature_daily_refresh", "task_label": "日技术特征读模型刷新", "schedule": "每天 02:05；交易日 18:40"},
    {"task_name": "adj_factor_daily_update", "task_label": "复权因子日更", "schedule": "每天 02:10"},
    {"task_name": "adj_factor_history_backfill", "task_label": "历史复权因子补齐", "schedule": "按需 / 后台批次"},
    {"task_name": "daily_kline_backfill", "task_label": "历史日线补齐", "schedule": "每天 02:15"},
    {"task_name": "moneyflow_daily_update", "task_label": "资金流日更", "schedule": "每天 02:20"},
    {"task_name": "chip_daily_update", "task_label": "筹码数据日更", "schedule": "每天 02:30"},
    {"task_name": "fundamental_sync", "task_label": "基本面补齐", "schedule": "每天 02:40"},
    {"task_name": "valuation_sync", "task_label": "估值补齐", "schedule": "每天 02:50"},
    {"task_name": "stock_status_snapshot_refresh", "task_label": "状态快照刷新", "schedule": "每天 03:05；交易日 15:25"},
    {"task_name": "pe_baidu_valuation_backfill", "task_label": "百度 PE 估值补齐", "schedule": "交易日 03:10"},
    {"task_name": "factor_input_daily_update", "task_label": "历史输入层日更", "schedule": "每天 03:20；交易日 18:30"},
    {"task_name": "market_context_daily_update", "task_label": "市场强度日更", "schedule": "每天 03:35"},
    {"task_name": "market_timing_daily_update", "task_label": "市场择时日更", "schedule": "每天 03:40；交易日 15:35"},
    {"task_name": "market_timing_v20_shadow_update", "task_label": "市场择时 V2.0 影子日更", "schedule": "每天 03:42；交易日 15:42"},
    {"task_name": "market_scenario_forecast_shadow_update", "task_label": "市场概率情景与主线状态", "schedule": "每天 03:47；交易日 15:47"},
    {"task_name": "strategy_factor_ci_daily_update", "task_label": "策略因子 CI 日更", "schedule": "每天 03:45；交易日 15:50"},
    {"task_name": "stock_sentiment_daily_update", "task_label": "真实舆情日更", "schedule": "每天 03:50"},
    {"task_name": "stock_status_pit_backfill", "task_label": "历史股票状态真相层", "schedule": "每天 04:35；交易日 18:35 补当日停牌；全量回填按需后台运行"},
    {"task_name": "fundamental_pit_backfill", "task_label": "公告日基本面真相层", "schedule": "每天 04:40；全量回填按需后台运行"},
    {"task_name": "index_constituent_pit_backfill", "task_label": "指数成分历史真相层", "schedule": "每天 04:45；全量回填按需后台运行"},
    {"task_name": "data_quality_audit", "task_label": "核心数据质量审计", "schedule": "每天 05:00；交易日 18:45"},
    {"task_name": "strategy_forward_outcome_update", "task_label": "策略前瞻收益更新", "schedule": "每天 05:10"},
    {"task_name": "strategy_factor_evaluation_v2", "task_label": "策略因子有效性 V2 评估", "schedule": "每天 05:20 / 05:30，按策略串行"},
    {"task_name": "etf_rotation_data_sync", "task_label": "行业 ETF 轮动数据同步", "schedule": "每天 05:40；交易日 16:20"},
    {"task_name": "etf_rotation_signal_materialize", "task_label": "行业 ETF 轮动影子信号", "schedule": "每天 05:55；交易日 16:35"},
    {"task_name": "etf_rotation_forward_outcomes", "task_label": "行业 ETF 轮动前瞻观察", "schedule": "每天 06:05"},
    {"task_name": "daily_kline_realtime_eod_backfill", "task_label": "日线收盘快照兜底", "schedule": "交易日 15:10"},
    {"task_name": "stock_realtime_lifecycle", "task_label": "实时行情分层与保留", "schedule": "交易日 15:20"},
    {"task_name": "market_opinion_update", "task_label": "热点舆情聚合", "schedule": "交易日 09:00-15:59 每 15 分钟"},
    {"task_name": "market_opinion_lifecycle", "task_label": "舆情快照去重与保留", "schedule": "交易日 16:05"},
    {"task_name": "sentiment_candidate_snapshot_materialize", "task_label": "舆情候选快照物化", "schedule": "交易时段约每 15 分钟；交易日 18:55"},
    {"task_name": "operational_read_models_refresh", "task_label": "实时榜单、首页缓存与运维读模型刷新", "schedule": "交易时段每 5 分钟；交易日 19:00"},
    {"task_name": "automatic_strategy_observation", "task_label": "新策略五交易日配对前瞻观察", "schedule": "交易日 09:25；每个新策略仅前 5 个成功配对交易日"},
    {"task_name": "stock_realtime_snapshot_update", "task_label": "实时行情分钟快照", "schedule": "交易日 09:00-15:59 每分钟，脚本内判断交易时段"},
    {"task_name": "portfolio_etf_quote_update", "task_label": "持仓 ETF 行情", "schedule": "交易日 09:00-15:59 每 5 分钟"},
    {"task_name": "market_fund_flow_update", "task_label": "板块资金流快照", "schedule": "交易日 09:00-15:59 每 3 分钟"},
    {"task_name": "ths_concept_hot_update", "task_label": "同花顺热点概念", "schedule": "交易日 09:00-15:59 每 30 分钟"},
    {"task_name": "stock_realtime_moneyflow_update", "task_label": "个股实时资金流", "schedule": "交易日 09:00-15:59 每 5 分钟"},
    {"task_name": "stock_popularity_update", "task_label": "股票热度榜", "schedule": "交易日 09:00-15:59 每 5 分钟"},
    {"task_name": "factor_input_history_backfill", "task_label": "历史输入层回填", "schedule": "按需 / 后台批次"},
    {"task_name": "job_retention", "task_label": "任务与错误保留治理", "schedule": "每天 04:15"},
]

TRACKED_TASKS = [item["task_name"] for item in TASK_SCHEDULES]
TASK_NAME_LABELS = {item["task_name"]: item["task_label"] for item in TASK_SCHEDULES}

KLINE_LATEST_SAMPLE_LIMIT = 20
TASK_RUNNING_STALE_SECONDS = 60 * 60

LATEST_DATES_SQL = f"""
SELECT
    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}') AS total_stock_codes,
    (SELECT MAX(updated_at) FROM stock_basic) AS stock_basic_latest_updated_at,
    (SELECT MAX(fundamental_updated_at) FROM stock_basic) AS fundamental_latest_updated_at,
    (SELECT MAX(valuation_updated_at) FROM stock_basic) AS valuation_latest_updated_at,
    (SELECT MAX(created_at) FROM selection_result) AS selection_result_latest_created_at,
    (SELECT MAX(trade_date) FROM selection_result) AS selection_result_latest_trade_date
"""

LATEST_KLINE_COUNTS_SQL = f"""
SELECT trade_date, COUNT(*) AS row_count
FROM daily_kline FORCE INDEX (idx_trade_date)
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT {KLINE_LATEST_SAMPLE_LIMIT}
"""


def _basic_data_ranges() -> dict:
    with mysql_read_conn() as conn:
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
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return None
            value = next(iter(row.values()))
            return int(value) if value is not None else None


def _coverage_stats() -> dict:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}') AS total_stock_codes,
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
    with mysql_read_conn() as conn:
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
    with mysql_read_conn() as conn:
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
    with mysql_read_conn() as conn:
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
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(LATEST_DATES_SQL)
            row = cursor.fetchone() or {}
            total_stock_codes = int(row.get("total_stock_codes") or 0)
            cursor.execute(LATEST_KLINE_COUNTS_SQL)
            recent_kline_counts = cursor.fetchall() or []
            latest_available_row = recent_kline_counts[0] if recent_kline_counts else {}
            latest_available = latest_available_row.get("trade_date")
            latest_available_count = int(latest_available_row.get("row_count") or 0)
            completeness_floor = total_stock_codes * STOCK_DAILY_COMPLETENESS_RATIO
            latest_complete_row = next(
                (
                    item
                    for item in recent_kline_counts
                    if int(item.get("row_count") or 0) >= completeness_floor
                ),
                {},
            )
            latest_complete = latest_complete_row.get("trade_date")
            latest_complete_count = int(latest_complete_row.get("row_count") or 0)

            result = {
                key: str(value) if value is not None else None
                for key, value in row.items()
                if key != "total_stock_codes"
            }
            result.update(
                {
                    "daily_kline_latest_trade_date": str(latest_complete) if latest_complete else (str(latest_available) if latest_available else None),
                    "daily_kline_latest_available_trade_date": str(latest_available) if latest_available else None,
                    "daily_kline_latest_available_count": latest_available_count,
                    "daily_kline_latest_complete_trade_date": str(latest_complete) if latest_complete else None,
                    "daily_kline_latest_complete_count": latest_complete_count,
                    "daily_kline_latest_is_partial": bool(latest_available and latest_complete and latest_available != latest_complete),
                    "total_stock_codes": total_stock_codes,
                }
            )
            return result


def _data_baseline_summary(
    *,
    latest: dict | None = None,
    sentiment: dict | None = None,
) -> dict:
    """Lightweight coverage cards for /system.

    Keep this bounded to simple aggregate queries and cache the whole endpoint.
    """
    latest = latest or _latest_dates()
    latest_kline_date = latest.get("daily_kline_latest_complete_trade_date") or latest.get("daily_kline_latest_trade_date")
    with mysql_read_conn() as conn:
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

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN net_amount IS NOT NULL THEN 1 ELSE 0 END) AS valid,
                    MAX(trade_date) AS latest_trade_date,
                    MAX(quote_time) AS latest_quote_time
                FROM market_sector_fund_flow_snapshot
                WHERE trade_date = (SELECT MAX(trade_date) FROM market_sector_fund_flow_snapshot)
                  AND quote_time >= DATE_SUB(
                      (SELECT MAX(quote_time) FROM market_sector_fund_flow_snapshot),
                      INTERVAL 20 MINUTE
                  )
                """
            )
            fund_flow_row = cursor.fetchone() or {}
            fund_flow_total = int(fund_flow_row.get("total") or 0)
            fund_flow_valid = int(fund_flow_row.get("valid") or 0)
            fund_flow_date = fund_flow_row.get("latest_trade_date")
            fund_flow_time = fund_flow_row.get("latest_quote_time")

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN net_amount IS NOT NULL THEN 1 ELSE 0 END) AS valid,
                    MAX(trade_date) AS latest_trade_date,
                    MAX(quote_time) AS latest_quote_time
                FROM stock_realtime_moneyflow_snapshot
                WHERE trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)
                  AND quote_time >= DATE_SUB(
                      (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot),
                      INTERVAL 20 MINUTE
                  )
                """
            )
            realtime_moneyflow_row = cursor.fetchone() or {}
            realtime_moneyflow_valid = int(realtime_moneyflow_row.get("valid") or 0)
            realtime_moneyflow_date = realtime_moneyflow_row.get("latest_trade_date")
            realtime_moneyflow_time = realtime_moneyflow_row.get("latest_quote_time")

            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM adj_factor_daily")
            adj_date = (cursor.fetchone() or {}).get("latest_trade_date")
            cursor.execute("SELECT COUNT(*) AS count FROM adj_factor_daily WHERE trade_date = %s", (adj_date,))
            adj_count = int((cursor.fetchone() or {}).get("count") or 0) if adj_date else 0

            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM stock_moneyflow_daily")
            moneyflow_date = (cursor.fetchone() or {}).get("latest_trade_date")
            cursor.execute("SELECT COUNT(*) AS count FROM stock_moneyflow_daily WHERE trade_date = %s", (moneyflow_date,))
            moneyflow_count = int((cursor.fetchone() or {}).get("count") or 0) if moneyflow_date else 0

            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM stock_chip_daily")
            chip_date = (cursor.fetchone() or {}).get("latest_trade_date")
            cursor.execute("SELECT COUNT(*) AS count FROM stock_chip_daily WHERE trade_date = %s", (chip_date,))
            chip_count = int((cursor.fetchone() or {}).get("count") or 0) if chip_date else 0

    sentiment = sentiment or _sentiment_quality_stats()
    sentiment_effective = int(sentiment.get("effective_news_count") or 0)
    sentiment_raw = int(sentiment.get("raw_news_count") or 0)

    def pct(done: int, total: int) -> float | None:
        return round(min(done / total * 100, 100), 2) if total else None

    return {
        "items": [
            {"key": "kline", "label": "日K线", "value": pct(kline_count, total_stock), "done": kline_count, "total": total_stock, "unit": "记录数"},
            {"key": "fundamental", "label": "基本面", "value": pct(fundamental_count, total_stock), "done": fundamental_count, "total": total_stock, "unit": "记录数"},
            {"key": "valuation", "label": "估值数据", "value": pct(valuation_count, total_stock), "done": valuation_count, "total": total_stock, "unit": "记录数"},
            {"key": "factor", "label": "因子输入", "value": pct(factor_count, total_stock), "done": factor_count, "total": total_stock, "unit": "记录数"},
            {"key": "realtime", "label": "实时快照", "value": pct(realtime_valid, realtime_total), "done": realtime_valid, "total": realtime_total, "unit": "记录数"},
            {"key": "adjfactor", "label": "复权因子", "value": pct(adj_count, total_stock), "done": adj_count, "total": total_stock, "unit": f"{adj_date or '-'} 记录数"},
            {"key": "moneyflow", "label": "个股资金流（日频）", "value": pct(moneyflow_count, total_stock), "done": moneyflow_count, "total": total_stock, "unit": f"{moneyflow_date or '-'} 完整交易日"},
            {"key": "moneyflow_realtime", "label": "个股资金流（实时）", "value": pct(realtime_moneyflow_valid, total_stock), "done": realtime_moneyflow_valid, "total": total_stock, "unit": f"{realtime_moneyflow_time or realtime_moneyflow_date or '-'} 报价"},
            {"key": "chip", "label": "筹码数据", "value": pct(chip_count, total_stock), "done": chip_count, "total": total_stock, "unit": f"{chip_date or '-'} 记录数"},
            {"key": "fundflow", "label": "板块资金流（实时）", "value": pct(fund_flow_valid, fund_flow_total), "done": fund_flow_valid, "total": fund_flow_total, "unit": f"{fund_flow_time or fund_flow_date or '-'} 报价"},
            {"key": "sentiment", "label": "情绪质量", "value": sentiment.get("avg_quality"), "done": sentiment_effective, "total": sentiment_raw, "unit": "有效新闻 / 总新闻"},
        ]
    }


def _sentiment_quality_stats() -> dict:
    """Lightweight news quality snapshot for the system page.

    Keep this query bounded to stock_sentiment_daily/stock_news aggregates so
    /api/system/status stays fast and does not scan行情/因子大表。
    """
    with mysql_read_conn() as conn:
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
    with mysql_read_conn() as conn:
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

    with mysql_read_conn() as conn:
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


def _sanitize_status_text(value: object) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)(token|secret|api[_-]?key|password|passwd)=([^&\s]+)", r"\1=***", text)
    text = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+", r"\1***", text)
    return text[:300]


def _market_opinion_update_status(task_runs: list[dict]) -> dict | None:
    run = next((item for item in task_runs if item.get("task_name") == "market_opinion_update"), None)
    if not run:
        return None
    meta = run.get("metadata") or {}
    errors = meta.get("errors") if isinstance(meta.get("errors"), dict) else {}
    failed_sources = [
        {"source_id": source_id, "error": _sanitize_status_text(error)}
        for source_id, error in list(errors.items())[:12]
    ]
    return {
        "task_name": "market_opinion_update",
        "task_label": run.get("task_label"),
        "run_id": run.get("run_id"),
        "status": run.get("status"),
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "as_of": meta.get("as_of"),
        "source_count": len(meta.get("sources") or []),
        "failed_source_count": int(meta.get("failed_sources") or len(failed_sources) or 0),
        "failed_sources": failed_sources,
        "fetched_items": meta.get("fetched_items"),
        "saved_items": meta.get("saved_items"),
        "sector_summary_count": meta.get("sector_summary_count"),
        "top_sectors": [
            {
                "sector_name": item.get("sector_name"),
                "sector_type": item.get("sector_type"),
                "sector_score": item.get("sector_score"),
                "news_count": item.get("news_count"),
                "source_count": item.get("source_count"),
            }
            for item in (meta.get("top_sectors") or [])[:5]
            if isinstance(item, dict)
        ],
        "message": _sanitize_status_text(run.get("message")),
    }


def _latest_task_runs() -> list[dict]:
    placeholders = ", ".join(["%s"] * len(TRACKED_TASKS))
    sql = f"""
    SELECT t1.task_name, t1.run_id, t1.status, t1.started_at, t1.finished_at, t1.message, t1.metadata_json,
           CASE
             WHEN t1.status = 'running' THEN TIMESTAMPDIFF(SECOND, t1.started_at, NOW())
             ELSE NULL
           END AS running_age_seconds
    FROM task_run_log t1
    INNER JOIN (
        SELECT task_name, MAX(id) AS max_id
        FROM task_run_log
        WHERE task_name IN ({placeholders})
        GROUP BY task_name
    ) t2 ON t1.id = t2.max_id
    ORDER BY t1.id DESC
    """
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, TRACKED_TASKS)
            rows = cursor.fetchall() or []
    items = []
    for row in rows:
        metadata = _decode_metadata(row.get("metadata_json"))
        running_age_seconds = int(row.get("running_age_seconds") or 0) if row.get("running_age_seconds") is not None else None
        stale = row.get("status") == "running" and (running_age_seconds or 0) > TASK_RUNNING_STALE_SECONDS
        if row.get("task_name") == "market_opinion_update" and isinstance(metadata, dict):
            metadata = dict(metadata)
            if isinstance(metadata.get("errors"), dict):
                metadata["errors"] = {
                    source_id: _sanitize_status_text(error)
                    for source_id, error in metadata["errors"].items()
                }
        items.append(
            {
                "task_name": row.get("task_name"),
                "task_label": TASK_NAME_LABELS.get(row.get("task_name"), row.get("task_name")),
                "run_id": row.get("run_id"),
                "status": "stale" if stale else row.get("status"),
                "recorded_status": row.get("status"),
                "stale": stale,
                "running_age_seconds": running_age_seconds,
                "started_at": str(row.get("started_at")) if row.get("started_at") else None,
                "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
                "message": _sanitize_status_text(row.get("message")) if row.get("task_name") == "market_opinion_update" else row.get("message"),
                "metadata": metadata,
            }
        )
    return items


def _scheduled_tasks() -> list[dict]:
    return [dict(item) for item in TASK_SCHEDULES]


def _data_quality_status(task_runs: list[dict]) -> dict:
    run = next((item for item in task_runs if item.get("task_name") == "data_quality_audit"), None)
    if not run:
        return {
            "health": "unknown",
            "status": "missing",
            "generated_at": None,
            "counts": {"pass": 0, "warn": 0, "fail": 0},
            "checks": [],
            "message": "尚未执行核心数据质量审计",
        }

    metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    payload = dict(metadata)
    if run.get("status") in {"failed", "stale"}:
        payload["health"] = "error"
        payload["status"] = "fail"
    payload.update(
        {
            "task_status": run.get("status"),
            "run_id": run.get("run_id"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
            "message": run.get("message"),
        }
    )
    payload.setdefault("counts", {"pass": 0, "warn": 0, "fail": 0})
    payload.setdefault("checks", [])
    return payload


def _realtime_lifecycle_summary() -> dict:
    table_names = (
        "stock_realtime_intraday",
        "stock_realtime_bar_rollup",
        "stock_realtime_intraday_tracked",
    )
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name, table_rows,
                       ROUND((data_length + index_length) / 1024 / 1024, 2) AS allocated_mb
                FROM information_schema.tables
                WHERE table_schema=DATABASE() AND table_name IN (%s,%s,%s)
                """,
                table_names,
            )
            table_rows = {
                (row.get("table_name") or row.get("TABLE_NAME")): {
                    "approx_rows": int(row.get("table_rows") or row.get("TABLE_ROWS") or 0),
                    "allocated_mb": float(row.get("allocated_mb") or row.get("ALLOCATED_MB") or 0),
                }
                for row in (cursor.fetchall() or [])
            }
            cursor.execute(
                """
                SELECT partition_name, table_rows, data_length, index_length
                FROM information_schema.partitions
                WHERE table_schema=DATABASE() AND table_name='stock_realtime_intraday'
                  AND partition_name IS NOT NULL
                ORDER BY partition_ordinal_position
                """
            )
            raw_partitions = cursor.fetchall() or []
            cursor.execute(
                """
                SELECT partition_name, table_rows
                FROM information_schema.partitions
                WHERE table_schema=DATABASE() AND table_name='stock_realtime_bar_rollup'
                  AND partition_name IS NOT NULL
                ORDER BY partition_ordinal_position
                """
            )
            rollup_partitions = cursor.fetchall() or []
            daily_partition_dates: set[date] = set()
            p_future_approx_rows = 0
            raw_partition_approx_rows = 0
            raw_partition_allocated_bytes = 0
            for row in raw_partitions:
                partition_name = str(row.get("partition_name") or row.get("PARTITION_NAME") or "")
                raw_partition_approx_rows += int(row.get("table_rows") or row.get("TABLE_ROWS") or 0)
                raw_partition_allocated_bytes += int(row.get("data_length") or row.get("DATA_LENGTH") or 0)
                raw_partition_allocated_bytes += int(row.get("index_length") or row.get("INDEX_LENGTH") or 0)
                if partition_name == "p_future":
                    p_future_approx_rows = int(row.get("table_rows") or row.get("TABLE_ROWS") or 0)
                    continue
                if len(partition_name) == 9 and partition_name.startswith("p") and partition_name[1:].isdigit():
                    try:
                        daily_partition_dates.add(datetime.strptime(partition_name[1:], "%Y%m%d").date())
                    except ValueError:
                        pass

            p_future_dates: set[date] = set()
            has_p_future = any(
                str(row.get("partition_name") or row.get("PARTITION_NAME") or "") == "p_future"
                for row in raw_partitions
            )
            first_future_date = None
            last_future_date = None
            if has_p_future:
                cursor.execute(
                    """
                    SELECT trade_date
                    FROM stock_realtime_intraday PARTITION (p_future)
                    ORDER BY trade_date ASC
                    LIMIT 1
                    """
                )
                first_future_date = (cursor.fetchone() or {}).get("trade_date")
                cursor.execute(
                    """
                    SELECT trade_date
                    FROM stock_realtime_intraday PARTITION (p_future)
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                )
                last_future_date = (cursor.fetchone() or {}).get("trade_date")
            for value in (first_future_date, last_future_date):
                if isinstance(value, datetime):
                    p_future_dates.add(value.date())
                elif isinstance(value, date):
                    p_future_dates.add(value)
                elif value:
                    p_future_dates.add(date.fromisoformat(str(value)))

            raw_dates = sorted(daily_partition_dates | p_future_dates, reverse=True)
            rollup_partition_dates: set[date] = set()
            rollup_p_future_approx_rows = 0
            for row in rollup_partitions:
                partition_name = str(row.get("partition_name") or row.get("PARTITION_NAME") or "")
                if partition_name == "p_future":
                    rollup_p_future_approx_rows = int(row.get("table_rows") or row.get("TABLE_ROWS") or 0)
                    continue
                if len(partition_name) == 9 and partition_name.startswith("p") and partition_name[1:].isdigit():
                    try:
                        rollup_partition_dates.add(datetime.strptime(partition_name[1:], "%Y%m%d").date())
                    except ValueError:
                        pass
            cursor.execute(
                """
                SELECT trade_date, interval_minutes, status, source_rows, source_codes,
                       rollup_rows, rollup_codes, first_quote_minute, last_quote_minute,
                       source_fingerprint, error_code, error_message, started_at, finished_at
                FROM stock_realtime_rollup_manifest
                ORDER BY trade_date DESC, interval_minutes ASC
                LIMIT 40
                """
            )
            manifest_rows = cursor.fetchall() or []

    def normalize_manifest(row: dict) -> dict:
        return {
            **row,
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "first_quote_minute": str(row.get("first_quote_minute")) if row.get("first_quote_minute") else None,
            "last_quote_minute": str(row.get("last_quote_minute")) if row.get("last_quote_minute") else None,
            "started_at": str(row.get("started_at")) if row.get("started_at") else None,
            "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
        }

    normalized_manifests = [normalize_manifest(row) for row in manifest_rows]
    manifest_map = {
        (item.get("trade_date"), int(item.get("interval_minutes") or 0)): item
        for item in normalized_manifests
    }
    latest_by_interval: dict[int, dict] = {}
    for item in normalized_manifests:
        interval = int(item.get("interval_minutes") or 0)
        if interval in {5, 15} and interval not in latest_by_interval:
            latest_by_interval[interval] = item

    now = datetime.now()
    lifecycle_due = datetime_time(15, 20)
    missing_manifests: list[dict] = []
    pending_manifests: list[dict] = []
    for raw_date in raw_dates:
        for interval in (5, 15):
            if (raw_date.isoformat(), interval) in manifest_map:
                continue
            entry = {"trade_date": raw_date.isoformat(), "interval_minutes": interval, "status": "missing"}
            if raw_date == now.date() and now.time() < lifecycle_due:
                entry["status"] = "pending"
                pending_manifests.append(entry)
            else:
                missing_manifests.append(entry)

    raw_date_strings = {item.isoformat() for item in raw_dates}
    raw_valid_rows_by_date: dict[str, int] = {}
    for item in normalized_manifests:
        trade_date = str(item.get("trade_date") or "")
        if trade_date in raw_date_strings:
            raw_valid_rows_by_date[trade_date] = max(
                raw_valid_rows_by_date.get(trade_date, 0),
                int(item.get("source_rows") or 0),
            )
    raw_valid_source_rows = sum(raw_valid_rows_by_date.values())
    raw_allocated_mb: float | None = max(
        float(table_rows.get("stock_realtime_intraday", {}).get("allocated_mb") or 0),
        round(raw_partition_allocated_bytes / 1024 / 1024, 2),
    )
    if raw_valid_source_rows > 0 and raw_allocated_mb < 1:
        # InnoDB can leave partition statistics at their empty-table defaults
        # after REORGANIZE PARTITION even while millions of rows remain present.
        raw_allocated_mb = None
    failed_manifests = [
        item
        for item in normalized_manifests
        if item.get("trade_date") in raw_date_strings and item.get("status") in {"failed", "running"}
    ]
    partial_manifests = [
        item
        for item in normalized_manifests
        if item.get("trade_date") in raw_date_strings and item.get("status") == "partial"
    ]
    if failed_manifests or missing_manifests or p_future_dates:
        health = "error"
        message = "分钟行情生命周期存在失败、缺失或未归档分区"
    elif partial_manifests or pending_manifests:
        health = "pending" if pending_manifests and not partial_manifests else "warn"
        message = "分钟行情生命周期尚未到收盘执行时间或数据不完整"
    else:
        health = "ok"
        message = "分钟行情分区与 5m/15m 汇总完整"
    return {
        "health": health,
        "message": message,
        "policy": {
            "full_market_raw_trade_days": FULL_MARKET_RAW_TRADE_DAYS,
            "rollup_trade_days": ROLLUP_TRADE_DAYS,
            "tracked_raw_trade_days": TRACKED_RAW_TRADE_DAYS,
            "rollup_intervals_minutes": list(ROLLUP_INTERVALS),
        },
        "raw": {
            **table_rows.get("stock_realtime_intraday", {}),
            "approx_rows": max(
                int(table_rows.get("stock_realtime_intraday", {}).get("approx_rows") or 0),
                raw_partition_approx_rows,
                raw_valid_source_rows,
            ),
            "valid_source_rows": raw_valid_source_rows,
            "allocated_mb": raw_allocated_mb,
            "trade_days": len(raw_dates),
            "min_trade_date": min(raw_dates).isoformat() if raw_dates else None,
            "max_trade_date": max(raw_dates).isoformat() if raw_dates else None,
            "daily_partitions": len(daily_partition_dates),
            "partitioned": bool(raw_partitions),
            "p_future_dates": [item.isoformat() for item in sorted(p_future_dates)],
            "p_future_approx_rows": p_future_approx_rows,
        },
        "rollup": {
            **table_rows.get("stock_realtime_bar_rollup", {}),
            "trade_days": len(rollup_partition_dates),
            "min_trade_date": min(rollup_partition_dates).isoformat() if rollup_partition_dates else None,
            "max_trade_date": max(rollup_partition_dates).isoformat() if rollup_partition_dates else None,
            "daily_partitions": len(rollup_partition_dates),
            "p_future_approx_rows": rollup_p_future_approx_rows,
        },
        "tracked": table_rows.get("stock_realtime_intraday_tracked", {}),
        "latest_manifests": [latest_by_interval[item] for item in sorted(latest_by_interval)],
        "failed_manifests": failed_manifests,
        "partial_manifests": partial_manifests,
        "missing_manifests": missing_manifests,
        "pending_manifests": pending_manifests,
    }


def _market_opinion_storage_summary() -> dict:
    table_names = (
        "sector_opinion_daily",
        "sector_opinion_stock",
        "sector_opinion_news_ref",
        "sector_opinion_source_ref",
    )
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name, table_rows,
                       ROUND((data_length + index_length) / 1024 / 1024, 2) AS allocated_mb
                FROM information_schema.tables
                WHERE table_schema=DATABASE() AND table_name IN (%s,%s,%s,%s)
                """,
                table_names,
            )
            tables = {}
            for row in cursor.fetchall() or []:
                name = row.get("table_name") or row.get("TABLE_NAME")
                tables[name] = {
                    "approx_rows": int(row.get("table_rows") or row.get("TABLE_ROWS") or 0),
                    "allocated_mb": float(row.get("allocated_mb") or row.get("ALLOCATED_MB") or 0),
                }
            cursor.execute("SELECT MAX(as_of_datetime) AS max_as_of FROM sector_opinion_daily")
            latest_as_of = (cursor.fetchone() or {}).get("max_as_of")
            if latest_as_of:
                cursor.execute(
                    """
                    SELECT payload_version, COUNT(*) AS rows_count
                    FROM sector_opinion_daily
                    WHERE as_of_datetime=%s
                    GROUP BY payload_version
                    """,
                    (latest_as_of,),
                )
                latest_versions = cursor.fetchall() or []
            else:
                latest_versions = []
    return {
        "policy": {
            "intraday_trade_days": 5,
            "daily_trade_days": 90,
            "payload_version": 2,
        },
        "latest_as_of": str(latest_as_of) if latest_as_of else None,
        "latest_payload_versions": [
            {
                "payload_version": int(row.get("payload_version") or 0),
                "rows": int(row.get("rows_count") or 0),
            }
            for row in latest_versions
        ],
        "tables": tables,
    }


@router.get("/system/status")
def system_status() -> dict:
    now = time.time()
    cached_entry = get_cache_backend().get(SYSTEM_STATUS_CACHE_KEY)
    if isinstance(cached_entry, dict) and isinstance(cached_entry.get("payload"), dict):
        cached = dict(cached_entry["payload"])
        cached_at = float(cached_entry.get("cached_at") or now)
        cached["cache"] = {
            "hit": True,
            "ttl_seconds": SYSTEM_STATUS_CACHE_TTL_SECONDS,
            "age_seconds": round(max(0.0, now - cached_at), 2),
        }
        return cached

    mysql_info = ping_mysql()
    task_runs = _latest_task_runs()
    readiness = build_operational_readiness()
    latest = _latest_dates()
    sentiment_quality = _sentiment_quality_stats()
    payload = {
        "status": "ok",
        "health": {
            "status": "ok",
            "database": mysql_info.get("db"),
            "version": mysql_info.get("version"),
        },
        "latest": latest,
        "sentiment_quality": sentiment_quality,
        "data_baseline": _data_baseline_summary(
            latest=latest,
            sentiment=sentiment_quality,
        ),
        "data_quality": _data_quality_status(task_runs),
        "scheduled_tasks": _scheduled_tasks(),
        "task_runs": task_runs,
        "readiness": readiness,
        "job_error_summary": recent_error_summaries(),
        "realtime_lifecycle": _realtime_lifecycle_summary(),
        "market_opinion_storage": _market_opinion_storage_summary(),
        "retention_policy": {
            "task_run_log_detail_days": 90,
            "selection_task_days": 90,
            "backtest_system_test_days": 90,
            "portfolio_raw_response_days": 30,
            "portfolio_snapshot_days": 90,
            "structured_error_summary_days": 365,
            "realtime_full_market_raw_trade_days": FULL_MARKET_RAW_TRADE_DAYS,
            "realtime_rollup_trade_days": ROLLUP_TRADE_DAYS,
            "realtime_tracked_raw_trade_days": TRACKED_RAW_TRADE_DAYS,
            "market_opinion_intraday_trade_days": 5,
            "market_opinion_daily_trade_days": 90,
            "tracking_stats_days": 14,
        },
        "market_opinion_update": _market_opinion_update_status(task_runs),
    }
    get_cache_backend().set(
        SYSTEM_STATUS_CACHE_KEY,
        {"payload": payload, "cached_at": now},
        ttl_seconds=SYSTEM_STATUS_CACHE_TTL_SECONDS,
    )
    payload["cache"] = {
        "hit": False,
        "ttl_seconds": SYSTEM_STATUS_CACHE_TTL_SECONDS,
        "age_seconds": 0,
    }
    return payload
