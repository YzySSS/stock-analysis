from __future__ import annotations

import json
from contextlib import AbstractContextManager
from typing import Any, Callable, Sequence

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class BacktestRepository:
    """Persistence boundary for backtest inputs, jobs, results and read models."""

    HISTORY_TABLES = {"daily_kline", "factor_input_daily"}
    RESULT_TABLES = ("backtest_pick", "backtest_trade", "backtest_summary_daily")
    RUN_COLUMNS = (
        "run_id",
        "strategy_id",
        "trade_strategy_id",
        "strategy_version",
        "instrument_type",
        "start_date",
        "end_date",
        "return_mode",
        "evaluation_mode",
        "methodology_version",
        "data_cutoff_date",
        "strategy_config_hash",
        "methodology_json",
        "use_adjusted_price",
        "commission_bps",
        "stamp_tax_bps",
        "slippage_bps",
        "execution_constraints_enabled",
        "is_system_test",
        "validation_baseline_id",
        "status",
        "idempotency_key",
        "active_idempotency_key",
        "attempt_count",
        "max_attempts",
        "phase",
        "request_json",
        "started_at",
        "progress_total_days",
        "progress_done_days",
        "progress_pct",
    )

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _placeholders(items: Sequence[Any]) -> str:
        if not items:
            raise ValueError("at least one item is required")
        return ",".join(["%s"] * len(items))

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, default=str)

    def latest_official_run_id(self) -> str | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT run_id FROM backtest_run WHERE COALESCE(is_system_test, 0) = 0 ORDER BY id DESC LIMIT 1"
                )
                return (cursor.fetchone() or {}).get("run_id")

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM backtest_run WHERE run_id=%s", (run_id,))
                return cursor.fetchone()

    def load_run_results(
        self,
        run_id: str,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM backtest_run WHERE run_id = %s", (run_id,))
                run = cursor.fetchone()
                if not run:
                    return None, []
                cursor.execute(
                    """
                    SELECT trade_date, pick_count, avg_return_1d_pct, avg_return_3d_pct,
                           win_rate_1d_pct, win_rate_3d_pct,
                           benchmark_return_1d_pct, benchmark_return_3d_pct
                    FROM backtest_summary_daily
                    WHERE run_id = %s
                    ORDER BY trade_date
                    """,
                    (run_id,),
                )
                return run, cursor.fetchall() or []

    def list_runs(
        self,
        *,
        limit: int,
        include_system_tests: bool,
    ) -> list[dict[str, Any]]:
        where_sql = "" if include_system_tests else "WHERE COALESCE(is_system_test, 0) = 0"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT *
                    FROM backtest_run
                    {where_sql}
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cursor.fetchall() or []

    def load_trade_page(
        self,
        *,
        run_id: str,
        limit: int,
        offset: int,
        trade_date: str | None,
        code: str | None,
        return_mode: str,
    ) -> dict[str, Any]:
        conditions = ["t.run_id = %s"]
        base_params: list[Any] = [run_id]
        if trade_date:
            conditions.append("t.trade_date = %s")
            base_params.append(trade_date)
        if code:
            conditions.append("t.code = %s")
            base_params.append(code)
        where_sql = " AND ".join(conditions)
        return_column = "return_1d_pct" if return_mode == "1d" else "return_3d_pct"
        sql = f"""
        SELECT t.run_id, t.strategy_id, t.trade_date, t.code,
               COALESCE(sb.name, sil.name) AS name,
               p.score AS entry_score, p.factor_json,
               t.entry_date, t.entry_price,
               t.exit_date_1d, t.exit_price_1d, t.return_1d_pct,
               t.exit_date_3d, t.exit_price_3d, t.return_3d_pct,
               t.max_gain_pct, t.max_drawdown_pct
        FROM backtest_trade t
        LEFT JOIN stock_basic sb ON sb.code = t.code
        LEFT JOIN stock_instrument_lifecycle sil ON sil.code = t.code
        LEFT JOIN backtest_pick p ON p.run_id = t.run_id AND p.trade_date = t.trade_date AND p.code = t.code
        WHERE {where_sql}
        ORDER BY t.trade_date DESC, t.{return_column} DESC
        LIMIT %s OFFSET %s
        """
        count_sql = f"""
        SELECT COUNT(*) AS total
        FROM backtest_trade t
        WHERE {where_sql}
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT use_adjusted_price FROM backtest_run WHERE run_id=%s",
                    (run_id,),
                )
                run_row = cursor.fetchone() or {}
                cursor.execute(count_sql, base_params)
                total = int((cursor.fetchone() or {}).get("total") or 0)
                cursor.execute(sql, [*base_params, limit, offset])
                rows = cursor.fetchall() or []
                horizon_bars: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
                horizon_sql = """
                SELECT dk.trade_date, dk.close, dk.high, dk.low, af.adj_factor
                FROM daily_kline dk
                LEFT JOIN adj_factor_daily af ON af.code = dk.code AND af.trade_date = dk.trade_date
                WHERE dk.code=%s AND dk.trade_date >= %s
                ORDER BY dk.trade_date
                LIMIT 5
                """
                for trade in rows:
                    trade_code = trade.get("code")
                    signal_date = trade.get("trade_date")
                    entry_date = trade.get("entry_date") or signal_date
                    if not trade_code or not signal_date or not entry_date:
                        continue
                    cursor.execute(horizon_sql, (trade_code, entry_date))
                    horizon_bars[(signal_date, trade_code)] = cursor.fetchall() or []
        return {
            "use_adjusted_price": bool(run_row.get("use_adjusted_price")),
            "total": total,
            "rows": rows,
            "horizon_bars": horizon_bars,
        }

    def load_factor_input_status(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date FROM factor_input_daily"
                )
                summary = cursor.fetchone() or {}
                cursor.execute(
                    "SELECT TABLE_ROWS AS total_rows FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'factor_input_daily'"
                )
                table_stats = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN pe_tushare IS NOT NULL THEN 1 ELSE 0 END) AS pe_tushare_filled,
                        SUM(CASE WHEN pb_tushare IS NOT NULL THEN 1 ELSE 0 END) AS pb_tushare_filled,
                        SUM(CASE WHEN turnover_rate IS NOT NULL THEN 1 ELSE 0 END) AS turnover_rate_filled,
                        SUM(CASE WHEN roe IS NOT NULL THEN 1 ELSE 0 END) AS roe_filled,
                        SUM(CASE WHEN revenue_yoy IS NOT NULL THEN 1 ELSE 0 END) AS revenue_yoy_filled,
                        COUNT(*) AS total_rows
                    FROM factor_input_daily
                    WHERE trade_date = %s
                    """,
                    (summary.get("max_trade_date"),),
                )
                field_row = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT task_name, run_id, status, started_at, finished_at, message
                    FROM task_run_log
                    WHERE task_name = 'factor_input_history_backfill'
                    ORDER BY id DESC
                    LIMIT 1
                    """
                )
                latest_task = cursor.fetchone() or {}
        return {
            "summary": summary,
            "table_stats": table_stats,
            "field_row": field_row,
            "latest_task": latest_task,
        }

    def fetch_data_cutoff(self, end_date: str) -> Any:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(f.trade_date) AS data_cutoff_date
                    FROM factor_input_daily f
                    INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
                    WHERE f.trade_date <= %s
                    """,
                    (end_date,),
                )
                return (cursor.fetchone() or {}).get("data_cutoff_date")

    def get_active_run_by_idempotency(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM backtest_run
                    WHERE active_idempotency_key=%s
                      AND status IN ('queued','running')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (idempotency_key,),
                )
                return cursor.fetchone()

    def fetch_trade_dates(self, start_date: str, end_date: str) -> list[Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT f.trade_date
                    FROM factor_input_daily f
                    INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
                    WHERE f.trade_date BETWEEN %s AND %s
                    ORDER BY f.trade_date
                    """,
                    (start_date, end_date),
                )
                return cursor.fetchall() or []

    def lowvol_feature_cache_counts(self, trade_date: str) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM lowvol_reversal_feature_daily
                            WHERE trade_date = %s
                        ) AS cache_count,
                        (
                            SELECT COUNT(*)
                            FROM factor_input_daily f
                            INNER JOIN daily_kline dk
                              ON dk.code = f.code AND dk.trade_date = f.trade_date
                            INNER JOIN stock_instrument_lifecycle sil ON sil.code = f.code
                            WHERE f.trade_date = %s
                              AND sil.instrument_type = 'stock'
                              AND (sil.listing_date IS NULL OR sil.listing_date <= f.trade_date)
                              AND (sil.delisting_date IS NULL OR f.trade_date < sil.delisting_date)
                              AND dk.open IS NOT NULL
                              AND dk.open > 0
                        ) AS expected_count
                    """,
                    (trade_date, trade_date),
                )
                return cursor.fetchone() or {}

    def load_feature_candidate_rows(
        self,
        trade_date: str,
        instrument_type: str,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            sil.code,
            COALESCE(nh.name, sb.name, sil.name) AS name,
            COALESCE(sb.industry, sil.industry) AS industry,
            sil.instrument_type,
            CASE
                WHEN nh.id IS NULL THEN 0
                ELSE (nh.is_st=1 OR nh.is_delisting_period=1)
            END AS is_st,
            (nh.id IS NOT NULL) AS pit_status_available,
            COALESCE(nh.is_delisting_period, 0) AS is_delisting_period,
            f.pe_tushare,
            f.pb_tushare,
            fp.roe,
            fp.roa,
            fp.grossprofit_margin,
            fp.netprofit_margin,
            fp.revenue_yoy,
            fp.profit_yoy,
            fp.eps,
            fp.period_end_date AS fundamental_period,
            fp.announcement_date AS fundamental_publish_date,
            fp.source AS fundamental_source,
            (fp.id IS NOT NULL) AS pit_fundamental_available,
            f.turnover_rate,
            f.volume_ratio,
            f.total_mv,
            NULL AS completeness_score,
            dk.open,
            dk.close,
            dk.amount,
            dk.trade_date,
            lf.ma20,
            lf.ma60,
            lf.close_5d,
            lf.close_20d,
            lf.prev_close_1d,
            lf.max_close_20,
            lf.min_close_20,
            lf.avg_amount_20,
            lf.kline_count_20,
            lf.kline_count_60,
            lf.std_return_20,
            lf.pct_chg_1d,
            lf.turnover_rate_5d_avg,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            DATEDIFF(f.trade_date, sil.listing_date) AS listed_days,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state
        FROM factor_input_daily f
        INNER JOIN stock_instrument_lifecycle sil ON sil.code = f.code
        LEFT JOIN stock_basic sb ON sb.code = f.code
        LEFT JOIN stock_name_history nh ON nh.code = f.code
          AND nh.start_date <= f.trade_date
          AND (nh.end_date IS NULL OR nh.end_date >= f.trade_date)
        LEFT JOIN stock_fundamental_pit fp ON fp.id = (
            SELECT fp2.id
            FROM stock_fundamental_pit fp2
            WHERE fp2.code=f.code
              AND fp2.announcement_date <= f.trade_date
              AND fp2.period_end_date <= f.trade_date
            ORDER BY fp2.period_end_date DESC,
                     fp2.announcement_date DESC,
                     fp2.update_flag DESC,
                     fp2.id DESC
            LIMIT 1
        )
        INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
        INNER JOIN lowvol_reversal_feature_daily lf ON lf.code = f.code AND lf.trade_date = f.trade_date
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = f.code AND mf.trade_date = f.trade_date
        LEFT JOIN stock_chip_daily chip ON chip.code = f.code AND chip.trade_date = f.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON ssd.code = f.code
          AND ssd.trade_date = (
              SELECT MAX(s2.trade_date)
              FROM stock_sentiment_daily s2
              WHERE s2.code = f.code AND s2.trade_date <= f.trade_date
          )
        LEFT JOIN market_context_daily mcd ON mcd.trade_date = f.trade_date AND mcd.index_code = '000300.SH'
        WHERE f.trade_date = %s
          AND sil.instrument_type = %s
          AND (sil.listing_date IS NULL OR sil.listing_date <= f.trade_date)
          AND (sil.delisting_date IS NULL OR f.trade_date < sil.delisting_date)
          AND dk.open IS NOT NULL
          AND dk.open > 0
        ORDER BY sil.code
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (trade_date, instrument_type))
                return cursor.fetchall() or []

    def load_candidate_rows(
        self,
        *,
        trade_date: str,
        instrument_type: str,
        kline_window_start: str,
        factor_window_start: str,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            sil.code,
            COALESCE(nh.name, sb.name, sil.name) AS name,
            COALESCE(sb.industry, sil.industry) AS industry,
            sil.instrument_type,
            CASE
                WHEN nh.id IS NULL THEN 0
                ELSE (nh.is_st=1 OR nh.is_delisting_period=1)
            END AS is_st,
            (nh.id IS NOT NULL) AS pit_status_available,
            COALESCE(nh.is_delisting_period, 0) AS is_delisting_period,
            f.pe_tushare,
            f.pb_tushare,
            fp.roe,
            fp.roa,
            fp.grossprofit_margin,
            fp.netprofit_margin,
            fp.revenue_yoy,
            fp.profit_yoy,
            fp.eps,
            fp.period_end_date AS fundamental_period,
            fp.announcement_date AS fundamental_publish_date,
            fp.source AS fundamental_source,
            (fp.id IS NOT NULL) AS pit_fundamental_available,
            f.turnover_rate,
            f.volume_ratio,
            f.total_mv,
            NULL AS completeness_score,
            dk.open,
            dk.close,
            dk.amount,
            dk.trade_date,
            ma.ma20,
            ma.ma60,
            ma.close_5d,
            ma.close_20d,
            ma.prev_close_1d,
            ma.max_close_20,
            ma.min_close_20,
            ma.avg_amount_20,
            ma.kline_count_20,
            ma.kline_count_60,
            ma.std_return_20,
            ma.pct_chg_1d,
            tma.turnover_rate_5d_avg,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            DATEDIFF(f.trade_date, sil.listing_date) AS listed_days,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state
        FROM factor_input_daily f
        INNER JOIN stock_instrument_lifecycle sil ON sil.code = f.code
        LEFT JOIN stock_basic sb ON sb.code = f.code
        LEFT JOIN stock_name_history nh ON nh.code = f.code
          AND nh.start_date <= f.trade_date
          AND (nh.end_date IS NULL OR nh.end_date >= f.trade_date)
        LEFT JOIN stock_fundamental_pit fp ON fp.id = (
            SELECT fp2.id
            FROM stock_fundamental_pit fp2
            WHERE fp2.code=f.code
              AND fp2.announcement_date <= f.trade_date
              AND fp2.period_end_date <= f.trade_date
            ORDER BY fp2.period_end_date DESC,
                     fp2.announcement_date DESC,
                     fp2.update_flag DESC,
                     fp2.id DESC
            LIMIT 1
        )
        INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
        LEFT JOIN (
            SELECT
                code,
                AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
                AVG(CASE WHEN rn <= 60 THEN close END) AS ma60,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d,
                MAX(CASE WHEN rn = 20 THEN close END) AS close_20d,
                MAX(CASE WHEN rn = 2 THEN close END) AS prev_close_1d,
                MAX(CASE WHEN rn <= 20 THEN close END) AS max_close_20,
                MIN(CASE WHEN rn <= 20 THEN close END) AS min_close_20,
                AVG(CASE WHEN rn <= 20 THEN amount END) AS avg_amount_20,
                SUM(CASE WHEN rn <= 20 THEN 1 ELSE 0 END) AS kline_count_20,
                COUNT(*) AS kline_count_60,
                STDDEV_SAMP(CASE WHEN rn <= 20 AND prev_close IS NOT NULL AND prev_close > 0 THEN close / prev_close - 1 END) AS std_return_20,
                MAX(CASE WHEN rn = 1 AND prev_close IS NOT NULL AND prev_close > 0 THEN (close - prev_close) / prev_close * 100 END) AS pct_chg_1d
            FROM (
                SELECT
                    code,
                    trade_date,
                    close,
                    amount,
                    LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS prev_close,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM daily_kline
                WHERE trade_date BETWEEN %s AND %s
            ) ranked
            WHERE rn <= 60
            GROUP BY code
        ) ma ON sil.code = ma.code
        LEFT JOIN (
            SELECT code, AVG(turnover_rate) AS turnover_rate_5d_avg
            FROM (
                SELECT
                    code,
                    turnover_rate,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM factor_input_daily
                WHERE trade_date BETWEEN %s AND %s
                  AND turnover_rate IS NOT NULL
            ) ranked_turnover
            WHERE rn <= 5
            GROUP BY code
        ) tma ON sil.code = tma.code
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = f.code AND mf.trade_date = f.trade_date
        LEFT JOIN stock_chip_daily chip ON chip.code = f.code AND chip.trade_date = f.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON ssd.code = f.code
          AND ssd.trade_date = (
              SELECT MAX(s2.trade_date)
              FROM stock_sentiment_daily s2
              WHERE s2.code = f.code AND s2.trade_date <= f.trade_date
          )
        LEFT JOIN market_context_daily mcd ON mcd.trade_date = f.trade_date AND mcd.index_code = '000300.SH'
        WHERE f.trade_date = %s
          AND sil.instrument_type = %s
          AND (sil.listing_date IS NULL OR sil.listing_date <= f.trade_date)
          AND (sil.delisting_date IS NULL OR f.trade_date < sil.delisting_date)
          AND dk.open IS NOT NULL
          AND dk.open > 0
        ORDER BY sil.code
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        kline_window_start,
                        trade_date,
                        factor_window_start,
                        trade_date,
                        trade_date,
                        instrument_type,
                    ),
                )
                return cursor.fetchall() or []

    def fetch_window_start_date(self, table: str, trade_date: str, limit: int) -> Any:
        if table not in self.HISTORY_TABLES:
            raise ValueError("unsupported history table")
        sql = f"""
        SELECT MIN(trade_date) AS start_date
        FROM (
            SELECT DISTINCT trade_date
            FROM {table}
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
        ) recent_dates
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (trade_date, int(limit)))
                return (cursor.fetchone() or {}).get("start_date")

    def fetch_future_bar_rows(
        self,
        codes: Sequence[str],
        trade_date: str,
    ) -> list[dict[str, Any]]:
        if not codes:
            return []
        placeholders = self._placeholders(codes)
        sql = f"""
        SELECT dk.code, COALESCE(nh.name, sil.name) AS name,
               dk.trade_date, dk.open, dk.high, dk.low, dk.close,
               prev.close AS prev_close,
               af.adj_factor
        FROM daily_kline dk
        LEFT JOIN stock_instrument_lifecycle sil ON sil.code = dk.code
        LEFT JOIN stock_name_history nh ON nh.code = dk.code
          AND nh.start_date <= dk.trade_date
          AND (nh.end_date IS NULL OR nh.end_date >= dk.trade_date)
        LEFT JOIN adj_factor_daily af ON af.code = dk.code AND af.trade_date = dk.trade_date
        LEFT JOIN daily_kline prev ON prev.code = dk.code
          AND prev.trade_date = (
            SELECT MAX(p.trade_date)
            FROM daily_kline p
            WHERE p.code = dk.code AND p.trade_date < dk.trade_date
          )
        WHERE dk.code IN ({placeholders}) AND dk.trade_date > %s
        ORDER BY dk.code, dk.trade_date
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, [*codes, trade_date])
                return cursor.fetchall() or []

    def create_run(self, values: dict[str, Any]) -> None:
        missing = [column for column in self.RUN_COLUMNS if column not in values]
        if missing:
            raise ValueError(f"missing backtest run fields: {', '.join(missing)}")
        sql = (
            f"INSERT INTO backtest_run ({', '.join(self.RUN_COLUMNS)}) "
            f"VALUES ({', '.join(['%s'] * len(self.RUN_COLUMNS))})"
        )
        params = tuple(values[column] for column in self.RUN_COLUMNS)
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    def mark_running(self, run_id: str, progress_total_days: int, now: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET status='running', progress_total_days=%s, progress_done_days=0,
                        progress_pct=0, current_trade_date=NULL, estimated_seconds_left=NULL,
                        worker_heartbeat_at=%s, phase='回测执行中', error_code=NULL,
                        error_message=NULL, started_at=%s, finished_at=NULL
                    WHERE run_id=%s
                    """,
                    (progress_total_days, now, now, run_id),
                )

    def clear_run_results(self, run_id: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                for table in self.RESULT_TABLES:
                    cursor.execute(f"DELETE FROM {table} WHERE run_id=%s", (run_id,))

    def update_progress(
        self,
        *,
        run_id: str,
        done_days: int,
        total_days: int,
        progress_pct: float,
        current_trade_date: str,
        seconds_left: int | None,
        now: str,
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET progress_done_days=%s, progress_total_days=%s, progress_pct=%s,
                        current_trade_date=%s, estimated_seconds_left=%s, worker_heartbeat_at=%s,
                        phase='回测执行中'
                    WHERE run_id=%s
                    """,
                    (done_days, total_days, progress_pct, current_trade_date, seconds_left, now, run_id),
                )

    def save_results(
        self,
        *,
        run_id: str,
        strategy_id: str,
        picks: Sequence[dict[str, Any]],
        trades: Sequence[dict[str, Any]],
        daily: Sequence[dict[str, Any]],
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                if picks:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_pick (run_id, strategy_id, trade_date, code, rank_no, score, entry_price, entry_price_type, factor_json, explain_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE score=VALUES(score), rank_no=VALUES(rank_no), entry_price=VALUES(entry_price), factor_json=VALUES(factor_json), explain_json=VALUES(explain_json)
                        """,
                        [
                            (
                                run_id,
                                strategy_id,
                                item["trade_date"],
                                item["code"],
                                item.get("rank_no"),
                                item.get("score"),
                                item.get("entry_price"),
                                item.get("entry_price_type"),
                                self._json(item.get("factor_json")),
                                self._json(item.get("explain_json")),
                            )
                            for item in picks
                        ],
                    )
                if trades:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_trade (run_id, strategy_id, trade_date, code, entry_date, entry_price, exit_date_1d, exit_price_1d, return_1d_pct, exit_date_3d, exit_price_3d, return_3d_pct, max_gain_pct, max_drawdown_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE exit_date_1d=VALUES(exit_date_1d), exit_price_1d=VALUES(exit_price_1d), return_1d_pct=VALUES(return_1d_pct), exit_date_3d=VALUES(exit_date_3d), exit_price_3d=VALUES(exit_price_3d), return_3d_pct=VALUES(return_3d_pct), max_gain_pct=VALUES(max_gain_pct), max_drawdown_pct=VALUES(max_drawdown_pct)
                        """,
                        [
                            (
                                item["run_id"],
                                item["strategy_id"],
                                item["trade_date"],
                                item["code"],
                                item["entry_date"],
                                item["entry_price"],
                                item.get("exit_date_1d"),
                                item.get("exit_price_1d"),
                                item.get("return_1d_pct"),
                                item.get("exit_date_3d"),
                                item.get("exit_price_3d"),
                                item.get("return_3d_pct"),
                                item.get("max_gain_pct"),
                                item.get("max_drawdown_pct"),
                            )
                            for item in trades
                        ],
                    )
                if daily:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_summary_daily (run_id, strategy_id, trade_date, pick_count, avg_return_1d_pct, avg_return_3d_pct, win_rate_1d_pct, win_rate_3d_pct, benchmark_return_1d_pct, benchmark_return_3d_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE pick_count=VALUES(pick_count), avg_return_1d_pct=VALUES(avg_return_1d_pct), avg_return_3d_pct=VALUES(avg_return_3d_pct), win_rate_1d_pct=VALUES(win_rate_1d_pct), win_rate_3d_pct=VALUES(win_rate_3d_pct)
                        """,
                        [
                            (
                                item["run_id"],
                                item["strategy_id"],
                                item["trade_date"],
                                item["pick_count"],
                                item.get("avg_return_1d_pct"),
                                item.get("avg_return_3d_pct"),
                                item.get("win_rate_1d_pct"),
                                item.get("win_rate_3d_pct"),
                                item.get("benchmark_return_1d_pct"),
                                item.get("benchmark_return_3d_pct"),
                            )
                            for item in daily
                        ],
                    )

    def finish_run(self, values: dict[str, Any]) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET status=%s, phase=%s, sample_days=%s, total_picks=%s, total_trades=%s,
                        progress_done_days=CASE WHEN %s='success' THEN progress_total_days ELSE progress_done_days END,
                        progress_pct=CASE WHEN %s='success' THEN 100 ELSE progress_pct END,
                        estimated_seconds_left=CASE WHEN %s='success' THEN 0 ELSE estimated_seconds_left END,
                        total_return_pct=%s, avg_return_pct=%s, max_drawdown_pct=%s, win_rate_pct=%s,
                        worker_heartbeat_at=%s, estimated_seconds_left=CASE WHEN %s IN ('success','cancelled') THEN 0 ELSE estimated_seconds_left END,
                        summary_json=%s, error_code=%s, error_message=%s, finished_at=%s,
                        active_idempotency_key=NULL
                    WHERE run_id=%s
                    """,
                    (
                        values["status"],
                        values["phase"],
                        values["sample_days"],
                        values["total_picks"],
                        values["total_trades"],
                        values["status"],
                        values["status"],
                        values["status"],
                        values.get("total_return_pct"),
                        values.get("avg_return_pct"),
                        values.get("max_drawdown_pct"),
                        values.get("win_rate_pct"),
                        values["now"],
                        values["status"],
                        self._json(values.get("summary") or {}),
                        values.get("error_code"),
                        values.get("error_message"),
                        values["now"],
                        values["run_id"],
                    ),
                )
                return cursor.rowcount == 1

    def list_active_runs(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id, strategy_id, status, started_at
                    FROM backtest_run
                    WHERE status IN ('queued', 'running')
                    ORDER BY id
                    """
                )
                return cursor.fetchall() or []

    def list_baseline_runs(self, baseline_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM backtest_run
                    WHERE validation_baseline_id=%s AND is_system_test=1
                    ORDER BY id
                    """,
                    (baseline_id,),
                )
                return cursor.fetchall() or []

    def list_legacy_candidates(
        self,
        current: dict[str, Any],
        legacy_methodology_version: str,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM backtest_run
                    WHERE strategy_id=%s
                      AND start_date=%s
                      AND end_date=%s
                      AND return_mode=%s
                      AND status='success'
                      AND COALESCE(is_system_test, 0)=0
                      AND COALESCE(methodology_version, %s)=%s
                    ORDER BY id DESC
                    """,
                    (
                        current.get("strategy_id"),
                        current.get("start_date"),
                        current.get("end_date"),
                        current.get("return_mode"),
                        legacy_methodology_version,
                        legacy_methodology_version,
                    ),
                )
                return cursor.fetchall() or []
