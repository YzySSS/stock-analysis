from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.shared.db import mysql_conn


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return str(value)
    return value


def _normalize_row(row: dict[str, Any] | None) -> dict[str, Any]:
    return {key: _normalize_value(value) for key, value in (row or {}).items()}


def _normalize_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [_normalize_row(row) for row in (rows or [])]


class DataQualityRepository:
    """Read only the latest bounded slices needed by the daily audit."""

    SAMPLE_LIMIT = 10

    def fetch_snapshot(self) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                dates = self._fetch_dates(cursor)
                stock_basic = self._fetch_stock_basic(cursor)
                daily_kline = self._fetch_daily_kline(cursor, dates)
                factor_input = self._fetch_factor_input(cursor, dates)
                future_rows = self._fetch_future_rows(cursor)

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dates": dates,
            "stock_basic": stock_basic,
            "daily_kline": daily_kline,
            "factor_input_daily": factor_input,
            "future_rows": future_rows,
        }

    @staticmethod
    def _fetch_dates(cursor: Any) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT
                (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_trade_date,
                (SELECT MAX(trade_date) FROM factor_input_daily) AS factor_input_trade_date,
                (
                    SELECT MAX(trade_date)
                    FROM stock_status_snapshot
                    WHERE trade_date <= (SELECT MAX(trade_date) FROM daily_kline)
                ) AS status_snapshot_trade_date,
                (
                    SELECT MAX(trade_date)
                    FROM stock_status_snapshot
                    WHERE trade_date <= (SELECT MAX(trade_date) FROM factor_input_daily)
                ) AS factor_status_snapshot_trade_date,
                (SELECT MAX(updated_at) FROM stock_basic) AS stock_basic_updated_at
            """
        )
        return _normalize_row(cursor.fetchone())

    @staticmethod
    def _fetch_stock_basic(cursor: Any) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(instrument_type='stock') AS stock_rows,
                SUM(instrument_type='stock' AND COALESCE(is_delisted, 0)=0) AS active_stock_rows,
                SUM(instrument_type IS NULL OR TRIM(instrument_type)='') AS missing_instrument_type,
                SUM(instrument_type='stock' AND code NOT REGEXP '^(sh|sz|bj)\\.[0-9]{6}$') AS invalid_code,
                SUM(instrument_type='stock' AND (name IS NULL OR TRIM(name)='')) AS missing_name,
                SUM(instrument_type='stock' AND (market IS NULL OR TRIM(market)='')) AS missing_market,
                SUM(
                    instrument_type='stock'
                    AND market IS NOT NULL
                    AND SUBSTRING_INDEX(code, '.', 1) <> market
                ) AS market_code_mismatch,
                SUM(
                    instrument_type='stock' AND COALESCE(is_delisted, 0)=0
                    AND listing_date IS NULL
                ) AS missing_listing_date,
                SUM(
                    instrument_type='stock' AND COALESCE(is_delisted, 0)=0
                    AND (
                        industry IS NULL OR TRIM(industry)=''
                        OR LOWER(TRIM(industry)) IN ('nan', 'none', 'null', 'unknown', '-')
                    )
                ) AS missing_industry,
                SUM(
                    instrument_type='stock' AND COALESCE(is_delisted, 0)=0
                    AND (name LIKE '%退市%' OR RIGHT(TRIM(name), 1)='退')
                    AND updated_at < DATE_SUB(
                        (SELECT MAX(updated_at) FROM stock_basic WHERE instrument_type='stock'),
                        INTERVAL 1 DAY
                    )
                ) AS suspected_delisted_active
            FROM stock_basic
            """
        )
        return _normalize_row(cursor.fetchone())

    def _fetch_daily_kline(self, cursor: Any, dates: dict[str, Any]) -> dict[str, Any]:
        trade_date = dates.get("daily_kline_trade_date")
        status_date = dates.get("status_snapshot_trade_date")
        if not trade_date:
            return {"metrics": {}, "gaps": {}, "samples": []}

        cursor.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT k.code) AS distinct_codes,
                COUNT(*) - COUNT(DISTINCT k.code) AS duplicate_rows,
                SUM(sb.code IS NULL) AS orphan_rows,
                SUM(sb.instrument_type='stock' AND COALESCE(sb.is_delisted, 0)=0) AS active_stock_rows,
                SUM(k.open IS NULL OR k.high IS NULL OR k.low IS NULL OR k.close IS NULL) AS null_ohlc,
                SUM(k.open <= 0 OR k.high <= 0 OR k.low <= 0 OR k.close <= 0) AS nonpositive_ohlc,
                SUM(
                    k.high < GREATEST(k.open, k.close, k.low)
                    OR k.low > LEAST(k.open, k.close, k.high)
                ) AS invalid_ohlc_order,
                SUM(k.volume IS NULL OR k.volume < 0) AS invalid_volume,
                SUM(k.amount IS NULL OR k.amount < 0) AS invalid_amount,
                SUM(k.source IS NULL OR TRIM(k.source)='') AS missing_source
            FROM daily_kline k
            LEFT JOIN stock_basic sb ON sb.code=k.code
            WHERE k.trade_date=%s
            """,
            (trade_date,),
        )
        metrics = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS missing_total,
                SUM(COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')) AS expected_non_trading,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND sb.listing_date=%s
                ) AS new_listing_pending,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND NOT (sb.listing_date IS NOT NULL AND sb.listing_date=%s)
                ) AS actionable_missing
            FROM stock_basic sb
            LEFT JOIN daily_kline k ON k.code=sb.code AND k.trade_date=%s
            LEFT JOIN stock_status_snapshot ss ON ss.code=sb.code AND ss.trade_date=%s
            WHERE sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (sb.listing_date IS NULL OR sb.listing_date <= %s)
              AND k.code IS NULL
            """,
            (trade_date, trade_date, trade_date, status_date, trade_date),
        )
        gaps = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                sb.code, sb.name, sb.listing_date,
                ss.status_label, ss.status_reason,
                (SELECT MAX(h.trade_date) FROM daily_kline h WHERE h.code=sb.code) AS last_trade_date,
                CASE
                    WHEN COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')
                        THEN 'expected_non_trading'
                    WHEN sb.listing_date=%s THEN 'new_listing_pending'
                    ELSE 'actionable_missing'
                END AS classification
            FROM stock_basic sb
            LEFT JOIN daily_kline k ON k.code=sb.code AND k.trade_date=%s
            LEFT JOIN stock_status_snapshot ss ON ss.code=sb.code AND ss.trade_date=%s
            WHERE sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (sb.listing_date IS NULL OR sb.listing_date <= %s)
              AND k.code IS NULL
            ORDER BY FIELD(classification, 'actionable_missing', 'new_listing_pending', 'expected_non_trading'), sb.code
            LIMIT %s
            """,
            (trade_date, trade_date, status_date, trade_date, self.SAMPLE_LIMIT),
        )
        return {"metrics": metrics, "gaps": gaps, "samples": _normalize_rows(cursor.fetchall())}

    def _fetch_factor_input(self, cursor: Any, dates: dict[str, Any]) -> dict[str, Any]:
        trade_date = dates.get("factor_input_trade_date")
        status_date = dates.get("factor_status_snapshot_trade_date")
        if not trade_date:
            return {
                "metrics": {},
                "coverage_gaps": {},
                "market_field_gaps": {},
                "samples": [],
            }

        cursor.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT f.code) AS distinct_codes,
                COUNT(*) - COUNT(DISTINCT f.code) AS duplicate_rows,
                SUM(sb.code IS NULL) AS orphan_rows,
                SUM(sb.instrument_type='stock' AND COALESCE(sb.is_delisted, 0)=0) AS active_stock_rows,
                SUM(k.code IS NULL) AS missing_same_day_kline,
                SUM(f.completeness_score IS NULL) AS null_completeness,
                MIN(f.completeness_score) AS min_completeness,
                AVG(f.completeness_score) AS avg_completeness,
                MAX(f.completeness_score) AS max_completeness,
                SUM(f.completeness_score < 0.8) AS low_completeness_rows,
                SUM(
                    f.turnover_rate IS NULL OR f.volume_ratio IS NULL
                    OR f.total_mv IS NULL OR f.circ_mv IS NULL
                ) AS missing_market_field_rows,
                SUM(
                    f.roe IS NULL AND f.roa IS NULL
                    AND f.grossprofit_margin IS NULL AND f.netprofit_margin IS NULL
                    AND f.revenue_yoy IS NULL AND f.profit_yoy IS NULL
                ) AS missing_all_fundamental_rows,
                SUM(f.pe_tushare IS NULL) AS missing_pe_rows,
                SUM(f.pb_tushare IS NULL) AS missing_pb_rows,
                SUM(
                    f.valuation_source IS NULL OR TRIM(f.valuation_source)=''
                    OR f.fundamental_source IS NULL OR TRIM(f.fundamental_source)=''
                    OR f.source IS NULL OR TRIM(f.source)=''
                ) AS missing_provenance_rows
            FROM factor_input_daily f
            LEFT JOIN stock_basic sb ON sb.code=f.code
            LEFT JOIN daily_kline k ON k.code=f.code AND k.trade_date=f.trade_date
            WHERE f.trade_date=%s
            """,
            (trade_date,),
        )
        metrics = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS missing_total,
                SUM(COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')) AS expected_non_trading,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND sb.listing_date=%s
                ) AS new_listing_pending,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND NOT (sb.listing_date IS NOT NULL AND sb.listing_date=%s)
                ) AS actionable_missing
            FROM stock_basic sb
            LEFT JOIN factor_input_daily f ON f.code=sb.code AND f.trade_date=%s
            LEFT JOIN stock_status_snapshot ss ON ss.code=sb.code AND ss.trade_date=%s
            WHERE sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (sb.listing_date IS NULL OR sb.listing_date <= %s)
              AND f.code IS NULL
            """,
            (trade_date, trade_date, trade_date, status_date, trade_date),
        )
        coverage_gaps = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS missing_total,
                SUM(COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')) AS expected_non_trading,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND sb.listing_date=%s
                ) AS new_listing_pending,
                SUM(
                    COALESCE(ss.status_label, '') NOT IN ('paused_listing', 'suspended')
                    AND NOT (sb.listing_date IS NOT NULL AND sb.listing_date=%s)
                ) AS actionable_missing
            FROM factor_input_daily f
            INNER JOIN stock_basic sb ON sb.code=f.code
            LEFT JOIN stock_status_snapshot ss ON ss.code=f.code AND ss.trade_date=%s
            WHERE f.trade_date=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (
                  f.turnover_rate IS NULL OR f.volume_ratio IS NULL
                  OR f.total_mv IS NULL OR f.circ_mv IS NULL
              )
            """,
            (trade_date, trade_date, status_date, trade_date),
        )
        market_field_gaps = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                f.code, sb.name, sb.listing_date, f.completeness_score,
                ss.status_label, ss.status_reason,
                (f.turnover_rate IS NULL) AS missing_turnover_rate,
                (f.volume_ratio IS NULL) AS missing_volume_ratio,
                (f.total_mv IS NULL) AS missing_total_mv,
                (f.circ_mv IS NULL) AS missing_circ_mv,
                CASE
                    WHEN COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')
                        THEN 'expected_non_trading'
                    WHEN sb.listing_date=%s THEN 'new_listing_pending'
                    ELSE 'actionable_missing'
                END AS classification
            FROM factor_input_daily f
            INNER JOIN stock_basic sb ON sb.code=f.code
            LEFT JOIN stock_status_snapshot ss ON ss.code=f.code AND ss.trade_date=%s
            WHERE f.trade_date=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (
                  f.turnover_rate IS NULL OR f.volume_ratio IS NULL
                  OR f.total_mv IS NULL OR f.circ_mv IS NULL
              )
            ORDER BY FIELD(classification, 'actionable_missing', 'new_listing_pending', 'expected_non_trading'), f.code
            LIMIT %s
            """,
            (trade_date, status_date, trade_date, self.SAMPLE_LIMIT),
        )
        samples = _normalize_rows(cursor.fetchall())
        return {
            "metrics": metrics,
            "coverage_gaps": coverage_gaps,
            "market_field_gaps": market_field_gaps,
            "samples": samples,
        }

    @staticmethod
    def _fetch_future_rows(cursor: Any) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM daily_kline WHERE trade_date > CURDATE()) AS daily_kline,
                (SELECT COUNT(*) FROM factor_input_daily WHERE trade_date > CURDATE()) AS factor_input_daily,
                (SELECT COUNT(*) FROM stock_status_snapshot WHERE trade_date > CURDATE()) AS stock_status_snapshot
            """
        )
        return _normalize_row(cursor.fetchone())
