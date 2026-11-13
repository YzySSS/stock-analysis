"""Fast, read-only dataset projection for strategy page GETs.

Keep this outside the frozen research implementation: runtime submission and
cohort identity continue using the original capability service. Only query
access paths differ; coverage, dates and readiness inputs must remain equal.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict

from app.shared.db import mysql_read_conn
from app.shared.instrument_policy import (
    STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS,
    STOCK_DAILY_COMPLETENESS_RATIO,
    STOCK_INSTRUMENT_TYPE,
)
from app.stock_selection.dataset_scope import required_dataset_code_prefixes, sql_code_prefix_filter
from app.strategies.capability import StrategyCapabilityService

_DATASET_REFRESH_LOCK = threading.Lock()
_DATASET_CACHE: tuple[float, Dict[str, Any]] | None = None


def get_display_dataset_snapshot() -> Dict[str, Any]:
    global _DATASET_CACHE
    now = time.monotonic()
    cached = _DATASET_CACHE
    if cached and now - cached[0] < StrategyCapabilityService.DATASET_CACHE_SECONDS:
        return cached[1]
    # Page switches can request the same snapshot from multiple GET endpoints.
    with _DATASET_REFRESH_LOCK:
        cached = _DATASET_CACHE
        if cached and time.monotonic() - cached[0] < StrategyCapabilityService.DATASET_CACHE_SECONDS:
            return cached[1]
        snapshot = _query_display_dataset_snapshot()
        _DATASET_CACHE = (time.monotonic(), snapshot)
        return snapshot


def _query_display_dataset_snapshot() -> Dict[str, Any]:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock' AND COALESCE(is_delisted, 0)=0) AS stock_count,
                    (
                        SELECT k.trade_date
                        FROM daily_kline k FORCE INDEX (idx_trade_date)
                        STRAIGHT_JOIN stock_basic daily_sb ON daily_sb.code=k.code
                        WHERE daily_sb.instrument_type='{STOCK_INSTRUMENT_TYPE}'
                          AND COALESCE(daily_sb.is_delisted, 0)=0
                          AND k.trade_date >= DATE_SUB(
                              (SELECT MAX(trade_date) FROM daily_kline),
                              INTERVAL {STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS} DAY
                          )
                        GROUP BY k.trade_date
                        HAVING COUNT(DISTINCT k.code) >= (
                            SELECT COUNT(*) * {STOCK_DAILY_COMPLETENESS_RATIO}
                            FROM stock_basic
                            WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}'
                              AND COALESCE(is_delisted, 0)=0
                        )
                        ORDER BY k.trade_date DESC
                        LIMIT 1
                    ) AS reference_trade_date,
                    (SELECT MAX(trade_date) FROM factor_input_daily) AS factor_input_date,
                    (SELECT MAX(trade_date) FROM stock_moneyflow_daily) AS moneyflow_date,
                    (SELECT MAX(trade_date) FROM stock_chip_daily) AS chip_date,
                    (SELECT MAX(as_of_datetime) FROM sector_opinion_daily) AS sector_opinion_at,
                    NOW(6) AS snapshot_now
                """
            )
            row = cursor.fetchone() or {}
            counts: Dict[str, int] = {}
            dated_tables = {
                "daily_kline_codes": ("daily_kline", "idx_trade_date", row.get("reference_trade_date")),
                "factor_input_codes": ("factor_input_daily", "idx_factor_input_trade_code", row.get("factor_input_date")),
                "moneyflow_codes": ("stock_moneyflow_daily", "idx_moneyflow_trade_date", row.get("moneyflow_date")),
                "chip_codes": ("stock_chip_daily", "idx_chip_trade_date", row.get("chip_date")),
            }
            for key, (table_name, date_index, latest_date) in dated_tables.items():
                eligible_code_prefixes = required_dataset_code_prefixes((table_name,))
                universe_filter_sql = sql_code_prefix_filter(
                    "sb.code", eligible_code_prefixes
                )
                expected_key = f"{key}_expected"
                if eligible_code_prefixes:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*) AS count
                        FROM stock_basic sb
                        WHERE sb.instrument_type='stock'
                          AND COALESCE(sb.is_delisted, 0)=0
                          {universe_filter_sql}
                        """
                    )
                    counts[expected_key] = int(
                        (cursor.fetchone() or {}).get("count") or 0
                    )
                else:
                    counts[expected_key] = int(row.get("stock_count") or 0)
                if not latest_date:
                    counts[key] = 0
                    continue
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT source.code) AS count
                    FROM {table_name} source FORCE INDEX ({date_index})
                    STRAIGHT_JOIN stock_basic sb ON sb.code = source.code
                    WHERE source.trade_date = %s
                      AND sb.instrument_type = 'stock'
                      AND COALESCE(sb.is_delisted, 0) = 0
                      {universe_filter_sql}
                    """,
                    (latest_date,),
                )
                counts[key] = int((cursor.fetchone() or {}).get("count") or 0)
            if row.get("sector_opinion_at"):
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM sector_opinion_daily WHERE as_of_datetime=%s",
                    (row.get("sector_opinion_at"),),
                )
                counts["sector_opinion_rows"] = int((cursor.fetchone() or {}).get("count") or 0)
            else:
                counts["sector_opinion_rows"] = 0

            cursor.execute(
                """
                SELECT
                    manifest.snapshot_id,
                    manifest.strategy_id,
                    manifest.strategy_version,
                    manifest.trade_date,
                    manifest.decision_as_of,
                    manifest.candidate_count,
                    manifest.coverage_ratio,
                    manifest.published_at
                FROM sentiment_candidate_snapshot_manifest manifest
                INNER JOIN (
                    SELECT strategy_id, strategy_version, MAX(id) AS latest_id
                    FROM sentiment_candidate_snapshot_manifest
                    WHERE status='ready'
                      AND quality_status='passed'
                      AND published_at IS NOT NULL
                    GROUP BY strategy_id, strategy_version
                ) latest ON latest.latest_id=manifest.id
                """
            )
            strategy_snapshot_rows = cursor.fetchall() or []

    reference_trade_date = row.get("reference_trade_date")
    strategy_snapshots = {
        f"{item.get('strategy_id')}:{item.get('strategy_version')}": dict(item)
        for item in strategy_snapshot_rows
        if isinstance(item, dict)
        and item.get("strategy_id")
        and item.get("strategy_version")
    }
    return {
        "stock_count": int(row.get("stock_count") or 0),
        "reference_trade_date": reference_trade_date,
        "snapshot_now": row.get("snapshot_now"),
        "strategy_snapshots": strategy_snapshots,
        "datasets": {
            "daily_kline": {
                "latest_at": reference_trade_date,
                "covered_codes": counts["daily_kline_codes"],
                "expected_codes": counts["daily_kline_codes_expected"],
            },
            "factor_input_daily": {
                "latest_at": row.get("factor_input_date"),
                "covered_codes": counts["factor_input_codes"],
                "expected_codes": counts["factor_input_codes_expected"],
            },
            "stock_moneyflow_daily": {
                "latest_at": row.get("moneyflow_date"),
                "covered_codes": counts["moneyflow_codes"],
                "expected_codes": counts["moneyflow_codes_expected"],
                "eligible_code_prefixes": list(
                    required_dataset_code_prefixes(("stock_moneyflow_daily",))
                ),
            },
            "stock_chip_daily": {
                "latest_at": row.get("chip_date"),
                "covered_codes": counts["chip_codes"],
                "expected_codes": counts["chip_codes_expected"],
            },
            "sector_opinion_daily": {
                "latest_at": row.get("sector_opinion_at"),
                "row_count": counts["sector_opinion_rows"],
            },
        },
    }
