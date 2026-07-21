from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from typing import Any, Callable

from app.shared.db import mysql_conn, mysql_read_conn


ConnectionFactory = Callable[..., AbstractContextManager]
TRACKING_STATS_MAX_AGE_DAYS = 14


class TrackingRepository:
    """Persistence boundary for saved-selection tracking and filters."""

    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
        read_connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        # Tests and custom repositories historically inject one factory; keep
        # that contract while using rollback-only pooled reads by default.
        self._read_connection_factory = (
            read_connection_factory
            or connection_factory
            or mysql_read_conn
        )

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def _read_connect(self, *, dict_cursor: bool = True):
        return self._read_connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _stats_cutoff(
        *,
        max_age_days: int = TRACKING_STATS_MAX_AGE_DAYS,
        as_of_datetime: datetime | None = None,
    ) -> datetime:
        normalized_days = max(1, int(max_age_days))
        current = as_of_datetime or datetime.now()
        return current.replace(tzinfo=None) - timedelta(days=normalized_days)

    @staticmethod
    def _latest_business_key_condition() -> str:
        return """
        sr.id IN (
            SELECT max_id
            FROM (
                SELECT MAX(id) AS max_id
                FROM selection_result
                GROUP BY code, trade_date, strategy_id
            ) latest_business_key
        )
        """

    def _selection_scope(
        self,
        *,
        instrument_type: str,
        run_id: str | None,
        strategy_id: str | None,
        selection_date: str | None,
        latest_only: bool,
        include_in_stats_only: bool = False,
    ) -> tuple[str, list[Any], str]:
        conditions = ["sb.instrument_type = %s"]
        params: list[Any] = [instrument_type]

        if run_id:
            conditions.append("sr.run_id = %s")
            params.append(run_id)
            order_by = "sr.rank_no ASC, sr.id ASC"
        elif selection_date:
            conditions.append("sr.trade_date = %s")
            params.append(selection_date)
            if strategy_id:
                conditions.append("sr.strategy_id = %s")
                params.append(strategy_id)
            conditions.append(self._latest_business_key_condition())
            order_by = "sr.trade_date DESC, sr.rank_no ASC, sr.id DESC"
        elif not latest_only:
            if strategy_id:
                conditions.append("sr.strategy_id = %s")
                params.append(strategy_id)
            conditions.append(self._latest_business_key_condition())
            order_by = "sr.trade_date DESC, sr.rank_no ASC, sr.id DESC"
        else:
            if strategy_id:
                conditions.append("sr.strategy_id = %s")
                params.append(strategy_id)
            latest_date_sql = """
            sr.trade_date = (
                SELECT MAX(sr2.trade_date)
                FROM selection_result sr2
                INNER JOIN stock_basic sb2 ON sr2.code = sb2.code
                WHERE sb2.instrument_type = %s
            """
            params.append(instrument_type)
            if strategy_id:
                latest_date_sql += " AND sr2.strategy_id = %s"
                params.append(strategy_id)
            latest_date_sql += ")"
            conditions.append(latest_date_sql)
            conditions.append(self._latest_business_key_condition())
            order_by = "sr.rank_no ASC, sr.id DESC"

        if include_in_stats_only:
            conditions.append("COALESCE(sr.include_in_stats, 1) = 1")
            conditions.append(
                f"sr.created_at >= DATE_SUB(NOW(), INTERVAL {TRACKING_STATS_MAX_AGE_DAYS} DAY)"
            )
        return " AND ".join(f"({condition.strip()})" for condition in conditions), params, order_by

    def list_selection_result_rows(
        self,
        *,
        limit: int,
        instrument_type: str,
        run_id: str | None = None,
        strategy_id: str | None = None,
        selection_date: str | None = None,
        offset: int = 0,
        latest_only: bool = True,
        include_in_stats_only: bool = False,
    ) -> list[dict[str, Any]]:
        where_sql, params, order_by = self._selection_scope(
            instrument_type=instrument_type,
            run_id=run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            latest_only=latest_only,
            include_in_stats_only=include_in_stats_only,
        )
        params.extend([max(1, int(limit)), max(0, int(offset))])
        sql = f"""
        WITH target_selection AS (
            SELECT sr.id
            FROM selection_result sr
            INNER JOIN stock_basic sb ON sr.code = sb.code
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        ),
        period_price AS (
            SELECT
                sr_inner.id AS selection_result_id,
                dk.trade_date,
                MAX(dk.high) AS high_price,
                MIN(dk.low) AS low_price
            FROM target_selection target
            INNER JOIN selection_result sr_inner ON sr_inner.id = target.id
            INNER JOIN daily_kline dk FORCE INDEX (uniq_code_date)
              ON dk.code = sr_inner.code
             AND dk.trade_date > sr_inner.trade_date
             AND dk.trade_date <= (SELECT MAX(trade_date) FROM daily_kline)
             AND dk.high > 0
             AND dk.low > 0
            GROUP BY sr_inner.id, dk.trade_date

            UNION ALL

            SELECT
                sr_inner.id AS selection_result_id,
                ri.trade_date,
                MAX(ri.latest_price) AS high_price,
                MIN(ri.latest_price) AS low_price
            FROM target_selection target
            INNER JOIN selection_result sr_inner ON sr_inner.id = target.id
            INNER JOIN stock_realtime_intraday_tracked ri
              ON ri.code = sr_inner.code
             AND ri.trade_date > sr_inner.trade_date
             AND ri.quote_minute >= TIMESTAMP(DATE_ADD(sr_inner.trade_date, INTERVAL 1 DAY))
             AND ri.latest_price IS NOT NULL
             AND ri.latest_price > 0
            GROUP BY sr_inner.id, ri.trade_date
        ),
        period_dk AS (
            SELECT
                selection_result_id,
                MAX(high_price) AS max_high,
                MIN(low_price) AS min_low,
                COUNT(DISTINCT trade_date) AS trade_day_count
            FROM period_price
            GROUP BY selection_result_id
        )
        SELECT
            sr.run_id,
            sr.run_id AS latest_run_id,
            sr.rank_no,
            sr.trade_date AS selection_date,
            sr.created_at AS selection_datetime,
            sr.strategy_id,
            sr.code,
            sr.score,
            COALESCE(sr.include_in_stats, 1) AS include_in_stats,
            sr.metadata_json,
            sb.name,
            sb.industry,
            sb.instrument_type,
            selected_dk.open AS selected_open_price,
            selected_dk.close AS selected_close_price,
            period_dk.max_high AS period_max_high,
            period_dk.min_low AS period_min_low,
            period_dk.trade_day_count AS trade_day_count,
            COALESCE(metadata_selected_dk.trade_date, latest_dk.trade_date) AS metric_trade_date,
            latest_dk.trade_date AS latest_trade_date,
            latest_dk.close AS daily_current_price,
            realtime.latest_price AS realtime_price,
            realtime.pct_chg AS realtime_pct_chg,
            realtime.quote_time AS realtime_quote_time,
            realtime.trade_date AS realtime_trade_date,
            realtime.high_price AS realtime_high_price,
            realtime.low_price AS realtime_low_price,
            COALESCE(realtime.latest_price, latest_dk.close) AS current_price
        FROM target_selection target
        INNER JOIN selection_result sr ON sr.id = target.id
        INNER JOIN stock_basic sb ON sr.code = sb.code
        LEFT JOIN daily_kline selected_dk
          ON sr.code = selected_dk.code
         AND sr.trade_date = selected_dk.trade_date
        LEFT JOIN daily_kline metadata_selected_dk
          ON sr.code = metadata_selected_dk.code
         AND metadata_selected_dk.trade_date = CASE
              WHEN JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')) IN ('', 'null') THEN NULL
              ELSE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')), '%%Y-%%m-%%d')
             END
        LEFT JOIN daily_kline latest_dk
          ON latest_dk.code = sr.code
         AND latest_dk.trade_date = (
              SELECT MAX(latest_for_code.trade_date)
              FROM daily_kline latest_for_code
              WHERE latest_for_code.code = sr.code
         )
        LEFT JOIN stock_realtime_snapshot realtime ON sr.code = realtime.code
        LEFT JOIN period_dk ON sr.id = period_dk.selection_result_id
        ORDER BY {order_by}
        """
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall() or []

    def count_items(
        self,
        *,
        instrument_type: str,
        strategy_id: str | None = None,
        selection_date: str | None = None,
        run_id: str | None = None,
        latest_only: bool = False,
        include_in_stats_only: bool = False,
    ) -> int:
        where_sql, params, _order_by = self._selection_scope(
            instrument_type=instrument_type,
            run_id=run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            latest_only=latest_only,
            include_in_stats_only=include_in_stats_only,
        )
        count_expression = "COUNT(*)" if run_id else "COUNT(DISTINCT sr.trade_date, sr.strategy_id, sr.code)"
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {count_expression} AS count
                    FROM selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    WHERE {where_sql}
                    """,
                    params,
                )
                row = cursor.fetchone() or {}
                return int(row.get("count") or 0)

    def list_runs(
        self,
        *,
        instrument_type: str,
        strategy_id: str | None = None,
        selection_date: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            sr.run_id,
            sr.trade_date,
            sr.strategy_id,
            MAX(sr.created_at) AS created_at,
            COUNT(*) AS item_count
        FROM selection_result sr
        INNER JOIN stock_basic sb ON sr.code = sb.code
        WHERE sb.instrument_type = %s
        """
        params: list[Any] = [instrument_type]
        if strategy_id:
            sql += " AND sr.strategy_id = %s"
            params.append(strategy_id)
        if selection_date:
            sql += " AND sr.trade_date = %s"
            params.append(selection_date)
        sql += " GROUP BY sr.run_id, sr.trade_date, sr.strategy_id ORDER BY sr.trade_date DESC, created_at DESC LIMIT %s"
        params.append(max(1, int(limit)))
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall() or []

    def set_include_in_stats(
        self,
        *,
        code: str,
        selection_date: str,
        strategy_id: str,
        instrument_type: str,
        include_in_stats: bool,
    ) -> int:
        cutoff = self._stats_cutoff()
        active_window_sql = "AND sr.created_at >= %s" if include_in_stats else ""
        params: list[Any] = [
            1 if include_in_stats else 0,
            code,
            selection_date,
            strategy_id,
            instrument_type,
        ]
        if include_in_stats:
            params.append(cutoff)
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    SET sr.include_in_stats = %s
                    WHERE sr.code = %s
                      AND sr.trade_date = %s
                      AND sr.strategy_id = %s
                      AND sb.instrument_type = %s
                      {active_window_sql}
                    """,
                    params,
                )
                return int(cursor.rowcount or 0)

    def exclude_expired_from_stats(
        self,
        *,
        instrument_type: str,
        max_age_days: int = TRACKING_STATS_MAX_AGE_DAYS,
        as_of_datetime: datetime | None = None,
    ) -> int:
        """Persistently exclude saved selections after their calendar-time window expires."""
        cutoff = self._stats_cutoff(max_age_days=max_age_days, as_of_datetime=as_of_datetime)
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    SET sr.include_in_stats = 0
                    WHERE COALESCE(sr.include_in_stats, 1) = 1
                      AND sr.created_at < %s
                      AND sb.instrument_type = %s
                    """,
                    (cutoff, instrument_type),
                )
                return int(cursor.rowcount or 0)

    def is_stats_window_expired(
        self,
        *,
        code: str,
        selection_date: str,
        strategy_id: str,
        instrument_type: str,
        max_age_days: int = TRACKING_STATS_MAX_AGE_DAYS,
        as_of_datetime: datetime | None = None,
    ) -> bool:
        """Return whether the latest saved row for a business key is outside the stats window."""
        cutoff = self._stats_cutoff(max_age_days=max_age_days, as_of_datetime=as_of_datetime)
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS row_count,
                        CASE WHEN MAX(sr.created_at) < %s THEN 1 ELSE 0 END AS is_expired
                    FROM selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    WHERE sr.code = %s
                      AND sr.trade_date = %s
                      AND sr.strategy_id = %s
                      AND sb.instrument_type = %s
                    """,
                    (cutoff, code, selection_date, strategy_id, instrument_type),
                )
                row = cursor.fetchone() or {}
                return bool(int(row.get("row_count") or 0) > 0 and int(row.get("is_expired") or 0) == 1)

    def delete_item(
        self,
        *,
        code: str,
        selection_date: str,
        strategy_id: str,
        instrument_type: str,
    ) -> int:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE sr
                    FROM selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    WHERE sr.code = %s
                      AND sr.trade_date = %s
                      AND sr.strategy_id = %s
                      AND sb.instrument_type = %s
                    """,
                    (code, selection_date, strategy_id, instrument_type),
                )
                return int(cursor.rowcount or 0)

    def list_strategy_options(self, instrument_type: str) -> list[dict[str, Any]]:
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        sr.strategy_id,
                        MAX(COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.strategy_display_name')), ''), sr.strategy_id)) AS strategy_display_name,
                        COUNT(*) AS item_count,
                        MAX(sr.created_at) AS last_created_at
                    FROM selection_result sr
                    INNER JOIN stock_basic sb ON sr.code = sb.code
                    WHERE sb.instrument_type = %s
                    GROUP BY sr.strategy_id
                    ORDER BY last_created_at DESC, sr.strategy_id ASC
                    """,
                    (instrument_type,),
                )
                return cursor.fetchall() or []
