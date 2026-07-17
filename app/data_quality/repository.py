from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.shared.db import mysql_conn
from app.shared.index_universe import INDEX_UNIVERSE_DEFINITIONS, index_member_guard_range


HISTORY_LOOKBACK_TRADE_DAYS = 60
UPSTREAM_TASKS = {
    "daily_kline": ("daily_kline_realtime_eod_backfill", "daily_kline_increment"),
    "factor_input_daily": ("factor_input_daily_update",),
    "stock_status_snapshot": ("stock_status_snapshot_refresh",),
    "stock_status_pit": ("stock_status_pit_backfill",),
    "fundamental_pit": ("fundamental_pit_backfill",),
    "index_constituent_pit": ("index_constituent_pit_backfill",),
}
UPSTREAM_SOURCE_BY_TASK = {
    "daily_kline_realtime_eod_backfill": "stock_realtime_snapshot",
    "daily_kline_increment": "tushare_daily",
    "factor_input_daily_update": "tushare_daily_basic",
    "stock_status_snapshot_refresh": "akshare",
    "stock_status_pit_backfill": "tushare_stock_basic+namechange+suspend_d",
    "fundamental_pit_backfill": "tushare_fina_indicator_vip",
    "index_constituent_pit_backfill": "tushare_index_weight",
}


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


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def gap_persistence(
    reference_trade_dates: list[Any],
    last_success_trade_date: Any,
    listing_date: Any = None,
) -> dict[str, Any]:
    """Describe a trailing gap using a bounded set of market trade dates."""

    dates = sorted({value for item in reference_trade_dates if (value := _as_date(item)) is not None})
    if not dates:
        return {
            "consecutive_missing_trade_days": None,
            "persistence_level": "unknown",
            "persistence_capped": False,
            "first_missing_trade_date": None,
        }

    last_success = _as_date(last_success_trade_date)
    listed = _as_date(listing_date)
    eligible_dates = [item for item in dates if listed is None or item >= listed]
    missing_dates = [item for item in eligible_dates if last_success is None or item > last_success]
    streak = len(missing_dates)
    if streak <= 0:
        level = "none"
    elif streak == 1:
        level = "single_day"
    elif streak < 5:
        level = "persistent"
    else:
        level = "long_running"

    earliest = eligible_dates[0] if eligible_dates else None
    capped = bool(
        missing_dates
        and earliest
        and (
            (last_success is not None and last_success < earliest)
            or (last_success is None and (listed is None or listed < earliest))
        )
    )
    return {
        "consecutive_missing_trade_days": streak,
        "persistence_level": level,
        "persistence_capped": capped,
        "first_missing_trade_date": str(missing_dates[0]) if missing_dates else None,
    }


class DataQualityRepository:
    """Read only the latest bounded slices needed by the daily audit."""

    SAMPLE_LIMIT = 10

    def fetch_snapshot(self) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                dates = self._fetch_dates(cursor)
                upstream_attempts = self._fetch_upstream_attempts(cursor)
                stock_basic = self._fetch_stock_basic(cursor)
                daily_kline = self._fetch_daily_kline(cursor, dates)
                factor_input = self._fetch_factor_input(cursor, dates)
                point_in_time = self._fetch_point_in_time_status(cursor, dates)
                fundamental_pit = self._fetch_point_in_time_fundamentals(cursor, dates)
                index_constituent_pit = self._fetch_point_in_time_index_constituents(cursor, dates)
                future_rows = self._fetch_future_rows(cursor)

                daily_reference_dates = self._fetch_recent_daily_dates(
                    cursor,
                    dates.get("daily_kline_trade_date"),
                )
                factor_reference_dates = self._fetch_recent_factor_dates(
                    cursor,
                    dates.get("factor_input_trade_date"),
                )
                daily_samples = daily_kline.get("samples") or []
                daily_success = self._fetch_latest_daily_success(
                    cursor,
                    [str(item.get("code")) for item in daily_samples if item.get("code")],
                    dates.get("daily_kline_trade_date"),
                )
                self._enrich_gap_samples(
                    daily_samples,
                    daily_success,
                    daily_reference_dates,
                    upstream_attempts.get("daily_kline") or {},
                )

                coverage_samples = factor_input.get("coverage_samples") or []
                coverage_success = self._fetch_latest_factor_success(
                    cursor,
                    [str(item.get("code")) for item in coverage_samples if item.get("code")],
                    dates.get("factor_input_trade_date"),
                    require_complete_market_fields=False,
                )
                self._enrich_gap_samples(
                    coverage_samples,
                    coverage_success,
                    factor_reference_dates,
                    upstream_attempts.get("factor_input_daily") or {},
                )

                market_field_samples = factor_input.get("samples") or []
                market_field_success = self._fetch_latest_factor_success(
                    cursor,
                    [str(item.get("code")) for item in market_field_samples if item.get("code")],
                    dates.get("factor_input_trade_date"),
                    require_complete_market_fields=True,
                )
                self._enrich_gap_samples(
                    market_field_samples,
                    market_field_success,
                    factor_reference_dates,
                    upstream_attempts.get("factor_input_daily") or {},
                )

        return {
            "audit_version": "dq5",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "history_lookback_trade_days": HISTORY_LOOKBACK_TRADE_DAYS,
            "dates": dates,
            "upstream_attempts": upstream_attempts,
            "stock_basic": stock_basic,
            "daily_kline": daily_kline,
            "factor_input_daily": factor_input,
            "point_in_time_status": point_in_time,
            "point_in_time_fundamentals": fundamental_pit,
            "point_in_time_index_constituents": index_constituent_pit,
            "future_rows": future_rows,
        }

    @staticmethod
    def _fetch_upstream_attempts(cursor: Any) -> dict[str, dict[str, Any]]:
        task_names = sorted({task for tasks in UPSTREAM_TASKS.values() for task in tasks})
        placeholders = ", ".join(["%s"] * len(task_names))
        cursor.execute(
            f"""
            SELECT id, task_name, run_id, status, started_at, finished_at, metadata_json
            FROM task_run_log
            WHERE task_name IN ({placeholders})
            ORDER BY id DESC
            LIMIT 30
            """,
            task_names,
        )
        rows = cursor.fetchall() or []
        attempts: dict[str, dict[str, Any]] = {}
        for dataset, candidate_tasks in UPSTREAM_TASKS.items():
            row = next((item for item in rows if item.get("task_name") in candidate_tasks), None)
            if not row:
                attempts[dataset] = {}
                continue
            raw_metadata = row.get("metadata_json")
            if isinstance(raw_metadata, dict):
                metadata = raw_metadata
            else:
                try:
                    metadata = json.loads(raw_metadata or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    metadata = {}
            target_trade_date = metadata.get("trade_date") or metadata.get("end_date")
            coverage = (metadata.get("daily_basic_coverage") or {}).get(str(target_trade_date), {})
            attempts[dataset] = {
                "task_name": row.get("task_name"),
                "run_id": row.get("run_id"),
                "status": row.get("status"),
                "last_attempt_at": str(row.get("finished_at") or row.get("started_at"))
                if row.get("finished_at") or row.get("started_at")
                else None,
                "started_at": str(row.get("started_at")) if row.get("started_at") else None,
                "finished_at": str(row.get("finished_at")) if row.get("finished_at") else None,
                "target_trade_date": str(target_trade_date) if target_trade_date else None,
                "source": metadata.get("source") or UPSTREAM_SOURCE_BY_TASK.get(str(row.get("task_name"))),
                "rows_synced": metadata.get("rows_synced"),
                "source_rows": coverage.get("rows") if isinstance(coverage, dict) else None,
                "source_coverage_ratio": coverage.get("coverage_ratio") if isinstance(coverage, dict) else None,
            }
        return attempts

    @staticmethod
    def _fetch_recent_daily_dates(cursor: Any, trade_date: Any) -> list[Any]:
        if not trade_date:
            return []
        cursor.execute(
            """
            SELECT DISTINCT trade_date
            FROM daily_kline
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (trade_date, HISTORY_LOOKBACK_TRADE_DAYS),
        )
        return [row.get("trade_date") for row in (cursor.fetchall() or []) if row.get("trade_date")]

    @staticmethod
    def _fetch_recent_factor_dates(cursor: Any, trade_date: Any) -> list[Any]:
        if not trade_date:
            return []
        cursor.execute(
            """
            SELECT DISTINCT trade_date
            FROM factor_input_daily
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (trade_date, HISTORY_LOOKBACK_TRADE_DAYS),
        )
        return [row.get("trade_date") for row in (cursor.fetchall() or []) if row.get("trade_date")]

    @staticmethod
    def _fetch_latest_daily_success(
        cursor: Any,
        codes: list[str],
        trade_date: Any,
    ) -> dict[str, dict[str, Any]]:
        if not codes or not trade_date:
            return {}
        placeholders = ", ".join(["%s"] * len(codes))
        cursor.execute(
            f"""
            SELECT h.code, h.trade_date, h.source, h.updated_at
            FROM daily_kline h
            INNER JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM daily_kline
                WHERE code IN ({placeholders}) AND trade_date <= %s
                GROUP BY code
            ) latest ON latest.code=h.code AND latest.trade_date=h.trade_date
            """,
            [*codes, trade_date],
        )
        return {str(row["code"]): _normalize_row(row) for row in (cursor.fetchall() or [])}

    @staticmethod
    def _fetch_latest_factor_success(
        cursor: Any,
        codes: list[str],
        trade_date: Any,
        *,
        require_complete_market_fields: bool,
    ) -> dict[str, dict[str, Any]]:
        if not codes or not trade_date:
            return {}
        placeholders = ", ".join(["%s"] * len(codes))
        completeness_sql = """
            AND turnover_rate IS NOT NULL
            AND volume_ratio IS NOT NULL
            AND total_mv IS NOT NULL
            AND circ_mv IS NOT NULL
        """ if require_complete_market_fields else ""
        cursor.execute(
            f"""
            SELECT h.code, h.trade_date, h.source, h.valuation_source,
                   h.fundamental_source, h.updated_at
            FROM factor_input_daily h
            INNER JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM factor_input_daily
                WHERE code IN ({placeholders}) AND trade_date <= %s
                {completeness_sql}
                GROUP BY code
            ) latest ON latest.code=h.code AND latest.trade_date=h.trade_date
            """,
            [*codes, trade_date],
        )
        return {str(row["code"]): _normalize_row(row) for row in (cursor.fetchall() or [])}

    @staticmethod
    def _enrich_gap_samples(
        samples: list[dict[str, Any]],
        success_by_code: dict[str, dict[str, Any]],
        reference_trade_dates: list[Any],
        upstream_attempt: dict[str, Any],
    ) -> None:
        for sample in samples:
            success = success_by_code.get(str(sample.get("code"))) or {}
            success_source = (
                success.get("valuation_source")
                or success.get("source")
                or success.get("fundamental_source")
            )
            sample.update(
                {
                    "last_success_trade_date": success.get("trade_date"),
                    "last_success_source": success_source,
                    "last_success_at": success.get("updated_at"),
                    "last_attempt_at": upstream_attempt.get("last_attempt_at"),
                    "last_attempt_status": upstream_attempt.get("status"),
                    "last_attempt_task": upstream_attempt.get("task_name"),
                    "last_attempt_target_trade_date": upstream_attempt.get("target_trade_date"),
                }
            )
            sample.update(
                gap_persistence(
                    reference_trade_dates,
                    success.get("trade_date"),
                    sample.get("listing_date"),
                )
            )

    @staticmethod
    def _fetch_dates(cursor: Any) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT
                (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_trade_date,
                (SELECT MAX(trade_date) FROM factor_input_daily) AS factor_input_trade_date,
                (SELECT MIN(trade_date) FROM factor_input_daily) AS factor_input_min_trade_date,
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

    def _fetch_point_in_time_status(self, cursor: Any, dates: dict[str, Any]) -> dict[str, Any]:
        start_date = dates.get("factor_input_min_trade_date")
        end_date = dates.get("factor_input_trade_date")
        if not start_date or not end_date:
            return {
                "history_start_date": start_date,
                "history_end_date": end_date,
                "lifecycle": {},
                "name_history": {},
                "suspension": {},
                "manifest": {},
                "historical_market_data": {},
                "samples": [],
            }

        cursor.execute(
            """
            SELECT
                COUNT(*) AS lifecycle_rows,
                SUM(list_status='L') AS active_rows,
                SUM(list_status='D') AS delisted_rows,
                SUM(list_status='P') AS paused_rows,
                SUM(listing_date IS NULL) AS missing_listing_date,
                SUM(delisting_date IS NOT NULL AND listing_date IS NOT NULL AND delisting_date <= listing_date)
                    AS invalid_lifecycle_rows
            FROM stock_instrument_lifecycle
            """
        )
        lifecycle = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS relevant_codes,
                SUM(
                    EXISTS(
                        SELECT 1
                        FROM stock_name_history nh
                        WHERE nh.code=l.code
                          AND nh.start_date <= %s
                          AND (nh.end_date IS NULL OR nh.end_date >= %s)
                    )
                ) AS name_covered_codes
            FROM stock_instrument_lifecycle l
            WHERE l.instrument_type='stock'
              AND (l.listing_date IS NULL OR l.listing_date <= %s)
              AND (l.delisting_date IS NULL OR l.delisting_date >= %s)
            """,
            (end_date, start_date, end_date, start_date),
        )
        name_coverage = _normalize_row(cursor.fetchone())
        cursor.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT code) AS codes,
                SUM(end_date IS NOT NULL AND end_date < start_date) AS invalid_intervals,
                SUM(is_st=1) AS st_intervals,
                SUM(is_delisting_period=1) AS delisting_intervals
            FROM stock_name_history
            """
        )
        name_history = _normalize_row(cursor.fetchone()) | name_coverage

        cursor.execute(
            """
            SELECT l.code, l.name, l.listing_date, l.delisting_date,
                   'name_history_unknown' AS classification
            FROM stock_instrument_lifecycle l
            WHERE l.instrument_type='stock'
              AND (l.listing_date IS NULL OR l.listing_date <= %s)
              AND (l.delisting_date IS NULL OR l.delisting_date >= %s)
              AND NOT EXISTS(
                  SELECT 1
                  FROM stock_name_history nh
                  WHERE nh.code=l.code
                    AND nh.start_date <= LEAST(COALESCE(l.delisting_date, %s), %s)
                    AND COALESCE(nh.end_date, '9999-12-31') >=
                        GREATEST(COALESCE(l.listing_date, %s), %s)
              )
            ORDER BY l.code
            LIMIT %s
            """,
            (end_date, start_date, end_date, end_date, start_date, start_date, self.SAMPLE_LIMIT),
        )
        name_samples = _normalize_rows(cursor.fetchall())

        cursor.execute(
            """
            SELECT
                MAX(CASE WHEN dataset='lifecycle' AND partition_key='full' THEN status END)
                    AS lifecycle_status,
                MAX(CASE WHEN dataset='name_history' AND partition_key='full' THEN status END)
                    AS name_history_status,
                MAX(CASE WHEN dataset='lifecycle' AND partition_key='full' THEN finished_at END)
                    AS lifecycle_finished_at,
                MAX(CASE WHEN dataset='name_history' AND partition_key='full' THEN finished_at END)
                    AS name_history_finished_at
            FROM stock_status_pit_manifest
            """
        )
        manifest = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT COUNT(DISTINCT trade_date) AS expected_trade_days
            FROM factor_input_daily
            WHERE trade_date BETWEEN %s AND %s
            """,
            (start_date, end_date),
        )
        expected_trade_days = int((cursor.fetchone() or {}).get("expected_trade_days") or 0)
        cursor.execute(
            """
            SELECT
                COUNT(*) AS manifest_days,
                SUM(status='success') AS successful_days,
                SUM(status='failed') AS failed_days,
                SUM(source_rows) AS source_rows,
                MAX(finished_at) AS latest_finished_at
            FROM stock_status_pit_manifest
            WHERE dataset='suspension_daily'
              AND partition_key BETWEEN %s AND %s
            """,
            (str(start_date), str(end_date)),
        )
        suspension = _normalize_row(cursor.fetchone()) | {"expected_trade_days": expected_trade_days}

        cursor.execute(
            """
            SELECT
                COUNT(*) AS historical_delisted_codes,
                SUM(
                    m.status='success'
                    AND JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$.classification')) =
                        'source_confirmed_no_market_activity'
                ) AS no_market_activity_codes,
                SUM(
                    EXISTS(
                        SELECT 1 FROM daily_kline k
                        WHERE k.code=l.code
                          AND k.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                               AND LEAST(%s, l.delisting_date)
                    )
                ) AS kline_covered_codes,
                SUM(
                    EXISTS(
                        SELECT 1 FROM factor_input_daily f
                        WHERE f.code=l.code
                          AND f.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                               AND LEAST(%s, l.delisting_date)
                    )
                ) AS factor_covered_codes,
                SUM(
                    NOT COALESCE((
                        m.status='success'
                        AND JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$.classification')) =
                            'source_confirmed_no_market_activity'
                    ), 0)
                    AND (
                    NOT EXISTS(
                        SELECT 1 FROM daily_kline k
                        WHERE k.code=l.code
                          AND k.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                               AND LEAST(%s, l.delisting_date)
                    )
                    OR NOT EXISTS(
                        SELECT 1 FROM factor_input_daily f
                        WHERE f.code=l.code
                          AND f.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                               AND LEAST(%s, l.delisting_date)
                    ))
                ) AS missing_market_data_codes
            FROM stock_instrument_lifecycle l
            LEFT JOIN stock_status_pit_manifest m
              ON m.dataset='historical_market_data' AND m.partition_key=l.code
            WHERE l.instrument_type='stock'
              AND l.list_status='D'
              AND l.listing_date <= %s
              AND l.delisting_date >= %s
            """,
            (
                start_date,
                end_date,
                start_date,
                end_date,
                start_date,
                end_date,
                start_date,
                end_date,
                end_date,
                start_date,
            ),
        )
        historical_market_data = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS historical_factor_rows,
                SUM(
                    f.turnover_rate IS NOT NULL
                    AND f.total_mv IS NOT NULL
                    AND f.circ_mv IS NOT NULL
                ) AS historical_market_field_rows
            FROM factor_input_daily f
            INNER JOIN stock_instrument_lifecycle l ON l.code=f.code
            WHERE l.instrument_type='stock'
              AND l.list_status='D'
              AND l.listing_date <= %s
              AND l.delisting_date >= %s
              AND f.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                   AND LEAST(%s, l.delisting_date)
            """,
            (end_date, start_date, start_date, end_date),
        )
        factor_market_fields = _normalize_row(cursor.fetchone())
        historical_factor_rows = int(factor_market_fields.get("historical_factor_rows") or 0)
        historical_market_field_rows = int(
            factor_market_fields.get("historical_market_field_rows") or 0
        )
        historical_market_data |= {
            **factor_market_fields,
            "market_field_coverage_ratio": round(
                historical_market_field_rows / historical_factor_rows,
                4,
            )
            if historical_factor_rows
            else 1.0,
        }

        cursor.execute(
            """
            SELECT
                l.code, l.name, l.listing_date, l.delisting_date,
                NOT EXISTS(
                    SELECT 1 FROM daily_kline k
                    WHERE k.code=l.code
                      AND k.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                           AND LEAST(%s, l.delisting_date)
                ) AS missing_kline,
                NOT EXISTS(
                    SELECT 1 FROM factor_input_daily f
                    WHERE f.code=l.code
                      AND f.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                           AND LEAST(%s, l.delisting_date)
                ) AS missing_factor_input,
                'historical_universe_missing' AS classification
            FROM stock_instrument_lifecycle l
            LEFT JOIN stock_status_pit_manifest m
              ON m.dataset='historical_market_data' AND m.partition_key=l.code
            WHERE l.instrument_type='stock'
              AND l.list_status='D'
              AND l.listing_date <= %s
              AND l.delisting_date >= %s
              AND (
                  NOT EXISTS(
                      SELECT 1 FROM daily_kline k
                      WHERE k.code=l.code
                        AND k.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                             AND LEAST(%s, l.delisting_date)
                  )
                  OR NOT EXISTS(
                      SELECT 1 FROM factor_input_daily f
                      WHERE f.code=l.code
                        AND f.trade_date BETWEEN GREATEST(%s, l.listing_date)
                                             AND LEAST(%s, l.delisting_date)
                  )
              )
              AND NOT COALESCE((
                  m.status='success'
                  AND JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$.classification')) =
                      'source_confirmed_no_market_activity'
              ), 0)
            ORDER BY l.delisting_date DESC, l.code
            LIMIT %s
            """,
            (
                start_date,
                end_date,
                start_date,
                end_date,
                end_date,
                start_date,
                start_date,
                end_date,
                start_date,
                end_date,
                self.SAMPLE_LIMIT,
            ),
        )
        samples = _normalize_rows(cursor.fetchall())
        return {
            "history_start_date": str(start_date),
            "history_end_date": str(end_date),
            "lifecycle": lifecycle,
            "name_history": name_history,
            "suspension": suspension,
            "manifest": manifest,
            "historical_market_data": historical_market_data,
            "name_samples": name_samples,
            "samples": samples,
        }

    def _fetch_point_in_time_fundamentals(
        self,
        cursor: Any,
        dates: dict[str, Any],
    ) -> dict[str, Any]:
        start_date = dates.get("factor_input_min_trade_date")
        end_date = dates.get("factor_input_trade_date")
        if not start_date or not end_date:
            return {
                "history_start_date": start_date,
                "history_end_date": end_date,
                "table": {},
                "manifest": {},
                "coverage": {},
                "coverage_by_date": [],
                "samples": [],
            }

        cursor.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT code) AS distinct_codes,
                MIN(announcement_date) AS min_announcement_date,
                MAX(announcement_date) AS max_announcement_date,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date,
                SUM(announcement_date < period_end_date) AS invalid_reporting_order_rows,
                SUM(announcement_date > CURDATE()) AS future_announcement_rows,
                SUM(period_end_date > CURDATE()) AS future_period_rows,
                SUM(
                    roe IS NULL AND roa IS NULL AND grossprofit_margin IS NULL
                    AND netprofit_margin IS NULL AND revenue_yoy IS NULL
                    AND profit_yoy IS NULL AND eps IS NULL
                ) AS empty_indicator_rows
            FROM stock_fundamental_pit
            """
        )
        table = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                COUNT(*) AS manifest_periods,
                SUM(status='success') AS successful_periods,
                SUM(status='partial_success') AS partial_periods,
                SUM(status='failed') AS failed_periods,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date,
                MAX(finished_at) AS latest_finished_at
            FROM fundamental_pit_manifest
            """
        )
        manifest = _normalize_row(cursor.fetchone())

        cursor.execute(
            """
            SELECT MAX(trade_date) AS trade_date
            FROM factor_input_daily
            WHERE trade_date BETWEEN %s AND %s
            GROUP BY YEAR(trade_date), QUARTER(trade_date)
            ORDER BY trade_date
            """,
            (start_date, end_date),
        )
        representative_dates = [
            str(row["trade_date"])
            for row in (cursor.fetchall() or [])
            if row.get("trade_date")
        ]
        representative_dates = sorted(
            set([str(start_date), str(end_date), *representative_dates])
        )
        if len(representative_dates) > 12:
            last_index = len(representative_dates) - 1
            representative_dates = sorted(
                {
                    representative_dates[round(index * last_index / 11)]
                    for index in range(12)
                }
            )

        coverage_by_date: list[dict[str, Any]] = []
        for trade_date in representative_dates:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS expected_rows,
                    SUM(
                        EXISTS(
                            SELECT 1
                            FROM stock_fundamental_pit p
                            WHERE p.code=f.code
                              AND p.announcement_date <= f.trade_date
                              AND p.period_end_date <= f.trade_date
                              AND (
                                  p.roe IS NOT NULL OR p.roa IS NOT NULL
                                  OR p.grossprofit_margin IS NOT NULL
                                  OR p.netprofit_margin IS NOT NULL
                                  OR p.revenue_yoy IS NOT NULL
                                  OR p.profit_yoy IS NOT NULL OR p.eps IS NOT NULL
                              )
                        )
                    ) AS covered_rows
                FROM factor_input_daily f
                INNER JOIN stock_instrument_lifecycle l ON l.code=f.code
                WHERE f.trade_date=%s
                  AND l.instrument_type='stock'
                  AND l.listing_date IS NOT NULL
                  AND l.listing_date <= DATE_SUB(f.trade_date, INTERVAL 120 DAY)
                  AND (l.delisting_date IS NULL OR l.delisting_date >= f.trade_date)
                """,
                (trade_date,),
            )
            coverage_row = _normalize_row(cursor.fetchone()) | {"trade_date": trade_date}
            expected_rows = int(coverage_row.get("expected_rows") or 0)
            covered_rows = int(coverage_row.get("covered_rows") or 0)
            coverage_row["coverage_ratio"] = round(
                covered_rows / expected_rows,
                6,
            ) if expected_rows else 0.0
            coverage_by_date.append(coverage_row)

        expected_rows = sum(int(row.get("expected_rows") or 0) for row in coverage_by_date)
        covered_rows = sum(int(row.get("covered_rows") or 0) for row in coverage_by_date)
        coverage_ratio = round(covered_rows / expected_rows, 6) if expected_rows else 0.0
        worst = min(
            coverage_by_date,
            key=lambda row: float(row.get("coverage_ratio") or 0),
            default={},
        )
        samples: list[dict[str, Any]] = []
        worst_trade_date = worst.get("trade_date")
        if worst_trade_date:
            cursor.execute(
                """
                SELECT f.code, l.name, l.listing_date, l.delisting_date,
                       %s AS trade_date,
                       'fundamental_asof_missing' AS classification
                FROM factor_input_daily f
                INNER JOIN stock_instrument_lifecycle l ON l.code=f.code
                WHERE f.trade_date=%s
                  AND l.instrument_type='stock'
                  AND l.listing_date IS NOT NULL
                  AND l.listing_date <= DATE_SUB(f.trade_date, INTERVAL 120 DAY)
                  AND (l.delisting_date IS NULL OR l.delisting_date >= f.trade_date)
                  AND NOT EXISTS(
                      SELECT 1
                      FROM stock_fundamental_pit p
                      WHERE p.code=f.code
                        AND p.announcement_date <= f.trade_date
                        AND p.period_end_date <= f.trade_date
                        AND (
                            p.roe IS NOT NULL OR p.roa IS NOT NULL
                            OR p.grossprofit_margin IS NOT NULL
                            OR p.netprofit_margin IS NOT NULL
                            OR p.revenue_yoy IS NOT NULL
                            OR p.profit_yoy IS NOT NULL OR p.eps IS NOT NULL
                        )
                  )
                ORDER BY f.code
                LIMIT %s
                """,
                (worst_trade_date, worst_trade_date, self.SAMPLE_LIMIT),
            )
            samples = _normalize_rows(cursor.fetchall())

        return {
            "history_start_date": str(start_date),
            "history_end_date": str(end_date),
            "table": table,
            "manifest": manifest,
            "coverage": {
                "sample_dates": len(coverage_by_date),
                "expected_rows": expected_rows,
                "covered_rows": covered_rows,
                "missing_rows": max(expected_rows - covered_rows, 0),
                "coverage_ratio": coverage_ratio,
                "worst_trade_date": worst_trade_date,
                "worst_coverage_ratio": worst.get("coverage_ratio"),
            },
            "coverage_by_date": coverage_by_date,
            "samples": samples,
        }

    def _fetch_point_in_time_index_constituents(
        self,
        cursor: Any,
        dates: dict[str, Any],
    ) -> dict[str, Any]:
        start_date = _as_date(dates.get("factor_input_min_trade_date"))
        end_date = _as_date(dates.get("factor_input_trade_date"))
        index_codes = list(INDEX_UNIVERSE_DEFINITIONS)
        if not start_date or not end_date:
            return {
                "history_start_date": str(start_date) if start_date else None,
                "history_end_date": str(end_date) if end_date else None,
                "table": {},
                "manifest": {},
                "coverage": {},
                "coverage_by_date": [],
                "samples": [],
            }

        def shift_month(value: date, offset: int) -> date:
            absolute = value.year * 12 + value.month - 1 + offset
            return date(absolute // 12, absolute % 12 + 1, 1)

        required_start_month = shift_month(start_date, -1)
        required_end_month = shift_month(end_date, -1)
        expected_months = max(
            (required_end_month.year - required_start_month.year) * 12
            + required_end_month.month
            - required_start_month.month
            + 1,
            0,
        )
        index_placeholders = ", ".join(["%s"] * len(index_codes))

        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT index_code) AS distinct_index_codes,
                COUNT(DISTINCT effective_date) AS distinct_snapshot_dates,
                MIN(effective_date) AS min_effective_date,
                MAX(effective_date) AS max_effective_date,
                SUM(l.code IS NULL) AS orphan_rows,
                SUM(weight IS NULL OR weight < 0) AS invalid_weight_rows,
                SUM(effective_date > CURDATE()) AS future_effective_rows,
                SUM(index_code NOT IN ({index_placeholders})) AS unsupported_index_rows
            FROM index_constituent_pit p
            LEFT JOIN stock_instrument_lifecycle l ON l.code=p.code
            """,
            index_codes,
        )
        table = _normalize_row(cursor.fetchone())

        guard_ranges = {
            code: index_member_guard_range(config["expected_members"])
            for code, config in INDEX_UNIVERSE_DEFINITIONS.items()
        }
        member_guard_sql = " OR ".join(
            f"(index_code='{code}' AND member_count NOT BETWEEN {bounds[0]} AND {bounds[1]})"
            for code, bounds in guard_ranges.items()
        )
        cursor.execute(
            f"""
            SELECT COUNT(*) AS total_snapshot_partitions,
                   COALESCE(SUM(
                       CASE
                           WHEN ({member_guard_sql})
                             OR weight_sum NOT BETWEEN 95 AND 105
                           THEN 1 ELSE 0
                       END
                   ), 0) AS invalid_snapshot_partitions
            FROM (
                SELECT index_code, effective_date,
                       COUNT(*) AS member_count,
                       SUM(weight) AS weight_sum
                FROM index_constituent_pit
                WHERE index_code IN ({index_placeholders})
                GROUP BY index_code, effective_date
            ) snapshots
            """,
            index_codes,
        )
        table.update(_normalize_row(cursor.fetchone()))

        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS manifest_partitions,
                COUNT(DISTINCT index_code) AS manifest_index_codes,
                SUM(status='success') AS successful_partitions,
                SUM(status='partial_success') AS partial_partitions,
                SUM(status='failed') AS failed_partitions,
                MIN(period_month) AS min_period_month,
                MAX(period_month) AS max_period_month,
                MAX(finished_at) AS latest_finished_at
            FROM index_constituent_pit_manifest
            WHERE index_code IN ({index_placeholders})
              AND period_month BETWEEN %s AND %s
            """,
            (*index_codes, required_start_month, required_end_month),
        )
        manifest = _normalize_row(cursor.fetchone())
        manifest["expected_months"] = expected_months
        manifest["expected_partitions"] = expected_months * len(index_codes)
        manifest["required_start_month"] = str(required_start_month)
        manifest["required_end_month"] = str(required_end_month)

        cursor.execute(
            """
            SELECT MAX(trade_date) AS trade_date
            FROM factor_input_daily
            WHERE trade_date BETWEEN %s AND %s
            GROUP BY YEAR(trade_date), QUARTER(trade_date)
            ORDER BY trade_date
            """,
            (start_date, end_date),
        )
        representative_dates = [
            str(row["trade_date"])
            for row in (cursor.fetchall() or [])
            if row.get("trade_date")
        ]
        representative_dates = sorted(
            set([str(start_date), str(end_date), *representative_dates])
        )
        if len(representative_dates) > 12:
            last_index = len(representative_dates) - 1
            representative_dates = sorted(
                {
                    representative_dates[round(index * last_index / 11)]
                    for index in range(12)
                }
            )

        coverage_by_date: list[dict[str, Any]] = []
        samples: list[dict[str, Any]] = []
        for trade_date in representative_dates:
            for index_code in index_codes:
                cursor.execute(
                    """
                    SELECT effective_date, COUNT(*) AS member_count,
                           SUM(weight) AS weight_sum,
                           DATEDIFF(%s, effective_date) AS staleness_days
                    FROM index_constituent_pit
                    WHERE index_code=%s
                      AND effective_date=(
                          SELECT MAX(effective_date)
                          FROM index_constituent_pit
                          WHERE index_code=%s AND effective_date <= %s
                      )
                    GROUP BY effective_date
                    """,
                    (trade_date, index_code, index_code, trade_date),
                )
                row = _normalize_row(cursor.fetchone())
                expected_members = int(
                    INDEX_UNIVERSE_DEFINITIONS[index_code]["expected_members"]
                )
                member_count = int(row.get("member_count") or 0)
                covered_members = min(member_count, expected_members)
                coverage_ratio = round(
                    covered_members / expected_members,
                    6,
                ) if expected_members else 0.0
                item = {
                    "trade_date": trade_date,
                    "index_code": index_code,
                    "index_name": INDEX_UNIVERSE_DEFINITIONS[index_code]["name"],
                    "effective_date": row.get("effective_date"),
                    "member_count": member_count,
                    "expected_members": expected_members,
                    "covered_members": covered_members,
                    "coverage_ratio": coverage_ratio,
                    "weight_sum": row.get("weight_sum"),
                    "staleness_days": row.get("staleness_days"),
                }
                coverage_by_date.append(item)
                if (
                    coverage_ratio < 0.95
                    or row.get("effective_date") is None
                    or int(row.get("staleness_days") or 0) > 45
                ) and len(samples) < self.SAMPLE_LIMIT:
                    samples.append(
                        {
                            **item,
                            "classification": "index_snapshot_gap",
                        }
                    )

        expected_members_total = sum(
            int(row.get("expected_members") or 0) for row in coverage_by_date
        )
        covered_members_total = sum(
            int(row.get("covered_members") or 0) for row in coverage_by_date
        )
        coverage_ratio = round(
            covered_members_total / expected_members_total,
            6,
        ) if expected_members_total else 0.0
        worst = min(
            coverage_by_date,
            key=lambda row: float(row.get("coverage_ratio") or 0),
            default={},
        )
        max_staleness_days = max(
            [int(row.get("staleness_days") or 0) for row in coverage_by_date],
            default=0,
        )
        return {
            "history_start_date": str(start_date),
            "history_end_date": str(end_date),
            "supported_indices": [
                {
                    "index_code": code,
                    "index_name": INDEX_UNIVERSE_DEFINITIONS[code]["name"],
                    "expected_members": INDEX_UNIVERSE_DEFINITIONS[code]["expected_members"],
                }
                for code in index_codes
            ],
            "table": table,
            "manifest": manifest,
            "coverage": {
                "sample_dates": len(representative_dates),
                "sample_index_snapshots": len(coverage_by_date),
                "expected_members": expected_members_total,
                "covered_members": covered_members_total,
                "missing_members": max(expected_members_total - covered_members_total, 0),
                "coverage_ratio": coverage_ratio,
                "worst_trade_date": worst.get("trade_date"),
                "worst_index_code": worst.get("index_code"),
                "worst_coverage_ratio": worst.get("coverage_ratio"),
                "max_staleness_days": max_staleness_days,
            },
            "coverage_by_date": coverage_by_date,
            "samples": samples,
        }

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
                "coverage_samples": [],
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
                    sb.instrument_type='stock'
                    AND COALESCE(sb.is_delisted, 0)=0
                    AND f.roe IS NULL AND f.roa IS NULL
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
                sb.code, sb.name, sb.listing_date,
                ss.status_label, ss.status_reason,
                CASE
                    WHEN COALESCE(ss.status_label, '') IN ('paused_listing', 'suspended')
                        THEN 'expected_non_trading'
                    WHEN sb.listing_date=%s THEN 'new_listing_pending'
                    ELSE 'actionable_missing'
                END AS classification
            FROM stock_basic sb
            LEFT JOIN factor_input_daily f ON f.code=sb.code AND f.trade_date=%s
            LEFT JOIN stock_status_snapshot ss ON ss.code=sb.code AND ss.trade_date=%s
            WHERE sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              AND (sb.listing_date IS NULL OR sb.listing_date <= %s)
              AND f.code IS NULL
            ORDER BY FIELD(classification, 'actionable_missing', 'new_listing_pending', 'expected_non_trading'), sb.code
            LIMIT %s
            """,
            (trade_date, trade_date, status_date, trade_date, self.SAMPLE_LIMIT),
        )
        coverage_samples = _normalize_rows(cursor.fetchall())

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
            "coverage_samples": coverage_samples,
            "samples": samples,
        }

    @staticmethod
    def _fetch_future_rows(cursor: Any) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM daily_kline WHERE trade_date > CURDATE()) AS daily_kline,
                (SELECT COUNT(*) FROM factor_input_daily WHERE trade_date > CURDATE()) AS factor_input_daily,
                (SELECT COUNT(*) FROM stock_status_snapshot WHERE trade_date > CURDATE()) AS stock_status_snapshot,
                (SELECT COUNT(*) FROM index_constituent_pit WHERE effective_date > CURDATE()) AS index_constituent_pit
            """
        )
        return _normalize_row(cursor.fetchone())
