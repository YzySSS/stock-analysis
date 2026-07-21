from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable, Sequence

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class SelectionRepository:
    """Persistence boundary for selection runs, candidates and saved results."""

    BOARD_FILTERS = {
        "all": "",
        "star": " AND sb.code LIKE 'sh.688%%' ",
        "chinext": " AND sb.code LIKE 'sz.300%%' ",
        "bse": " AND sb.code LIKE 'bj.%%' ",
        "main": " AND ((sb.code LIKE 'sh.6%%' AND sb.code NOT LIKE 'sh.688%%') OR sb.code REGEXP '^sz\\.(000|001|002)') ",
    }

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @classmethod
    def market_board_filter_sql(cls, market_board: str) -> str:
        try:
            return cls.BOARD_FILTERS[market_board]
        except KeyError as exc:
            raise ValueError(f"unsupported market board: {market_board}") from exc

    def count_instruments(self, instrument_type: str) -> int:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM stock_basic WHERE instrument_type = %s",
                    (instrument_type,),
                )
                return int((cursor.fetchone() or {}).get("count") or 0)

    def latest_result_run_meta(
        self,
        instrument_type: str,
        *,
        run_id: str | None = None,
        strategy_id: str | None = None,
    ) -> dict[str, Any]:
        sql = """
        SELECT
            sr.run_id,
            sr.trade_date,
            sr.strategy_id,
            MAX(sr.created_at) AS created_at
        FROM selection_result sr
        INNER JOIN stock_basic sb ON sr.code = sb.code
        WHERE sb.instrument_type = %s
        """
        params: list[Any] = [instrument_type]
        if run_id:
            sql += " AND sr.run_id = %s"
            params.append(run_id)
        elif strategy_id:
            sql += """
            AND sr.run_id = (
                SELECT sr2.run_id
                FROM selection_result sr2
                INNER JOIN stock_basic sb2 ON sr2.code = sb2.code
                WHERE sb2.instrument_type = %s
                  AND sr2.strategy_id = %s
                ORDER BY sr2.created_at DESC, sr2.id DESC
                LIMIT 1
            )
            """
            params.extend([instrument_type, strategy_id])
        else:
            sql += """
            AND sr.run_id = (
                SELECT sr2.run_id
                FROM selection_result sr2
                INNER JOIN stock_basic sb2 ON sr2.code = sb2.code
                WHERE sb2.instrument_type = %s
                ORDER BY sr2.created_at DESC, sr2.id DESC
                LIMIT 1
            )
            """
            params.append(instrument_type)
        sql += " GROUP BY sr.run_id, sr.trade_date, sr.strategy_id ORDER BY created_at DESC LIMIT 1"

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone() or {}

    def create_run(
        self,
        *,
        run_id: str,
        strategy_id: str,
        instrument_type: str,
        market_board: str | None,
        max_picks: int,
        score_threshold: float | None,
        idempotency_key: str,
        idempotency_date: Any,
        max_attempts: int,
        estimated_seconds_left: int,
        request_json: str,
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO selection_run (
                        run_id, strategy_id, instrument_type, market_board, max_picks, score_threshold,
                        save_requested, status, idempotency_key, active_idempotency_key, idempotency_date,
                        cancel_requested, attempt_count, max_attempts,
                        phase, progress_pct, estimated_seconds_left, request_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, 'queued', %s, %s, %s,
                        0, 0, %s,
                        '任务已提交', 0, %s, %s
                    )
                    """,
                    (
                        run_id,
                        strategy_id,
                        instrument_type,
                        market_board,
                        max_picks,
                        score_threshold,
                        idempotency_key,
                        idempotency_key,
                        idempotency_date,
                        max_attempts,
                        estimated_seconds_left,
                        request_json,
                    ),
                )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM selection_run WHERE run_id=%s", (run_id,))
                return cursor.fetchone()

    def list_runs(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM selection_run
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cursor.fetchall() or []

    def latest_data_trade_date(self) -> Any:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
                return (cursor.fetchone() or {}).get("trade_date")

    def get_active_run_by_idempotency(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM selection_run
                    WHERE active_idempotency_key=%s
                      AND status IN ('queued', 'running')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (idempotency_key,),
                )
                return cursor.fetchone()

    def average_recent_runtime(
        self,
        *,
        strategy_id: str,
        instrument_type: str,
        max_seconds: int,
    ) -> Any:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT AVG(TIMESTAMPDIFF(SECOND, started_at, finished_at)) AS avg_seconds
                    FROM (
                        SELECT started_at, finished_at
                        FROM selection_run
                        WHERE status='success'
                          AND strategy_id=%s
                          AND instrument_type=%s
                          AND started_at IS NOT NULL
                          AND finished_at IS NOT NULL
                          AND TIMESTAMPDIFF(SECOND, started_at, finished_at) BETWEEN 2 AND %s
                        ORDER BY id DESC
                        LIMIT 12
                    ) recent_runs
                    """,
                    (strategy_id, instrument_type, max_seconds),
                )
                return (cursor.fetchone() or {}).get("avg_seconds")

    def mark_execution_stage(self, run_id: str, worker_id: str) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_run
                    SET phase='策略计算中', progress_pct=10, worker_heartbeat_at=NOW()
                    WHERE run_id=%s AND status='running' AND worker_id=%s
                    """,
                    (run_id, worker_id),
                )
                return cursor.rowcount == 1

    def finish_success(
        self,
        *,
        run_id: str,
        worker_id: str,
        result_count: int,
        result_json: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_run
                    SET status='success', phase='运行完成', progress_pct=100, estimated_seconds_left=0,
                        result_count=%s, result_json=%s, error_code=NULL, error_message=NULL,
                        finished_at=NOW(), worker_heartbeat_at=NOW(), active_idempotency_key=NULL
                    WHERE run_id=%s AND status='running' AND worker_id=%s AND cancel_requested=0
                    """,
                    (result_count, result_json, run_id, worker_id),
                )
                return cursor.rowcount == 1

    def finish_failed(
        self,
        *,
        run_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_run
                    SET status='failed', phase='运行失败', estimated_seconds_left=0,
                        error_code=%s, error_message=%s, finished_at=NOW(),
                        worker_heartbeat_at=NOW(), active_idempotency_key=NULL
                    WHERE run_id=%s AND status='running' AND worker_id=%s
                    """,
                    (error_code, error_message[:2000], run_id, worker_id),
                )
                return cursor.rowcount == 1

    def load_market_opinion_rows(
        self,
        *,
        requested_as_of: str | None,
        latest_candidate_trade_date: str | None,
        allowed_sector_types: Sequence[str] | None = None,
        excluded_sector_names: Sequence[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        sector_filters: list[str] = []
        sector_filter_params: list[Any] = []
        normalized_types = sorted({str(value).strip() for value in (allowed_sector_types or []) if str(value).strip()})
        normalized_excluded = sorted({str(value).strip() for value in (excluded_sector_names or []) if str(value).strip()})
        if normalized_types:
            placeholders = ",".join(["%s"] * len(normalized_types))
            sector_filters.append(f"sector_type IN ({placeholders})")
            sector_filter_params.extend(normalized_types)
        if normalized_excluded:
            placeholders = ",".join(["%s"] * len(normalized_excluded))
            sector_filters.append(f"sector_name NOT IN ({placeholders})")
            sector_filter_params.extend(normalized_excluded)
        sector_filter_sql = "" if not sector_filters else "\n              AND " + "\n              AND ".join(sector_filters)
        if requested_as_of:
            sql = f"""
            SELECT id, payload_version, trade_date, sector_type, sector_name, as_of_datetime, sector_score, weighted_impact_score,
                   news_count, source_count, stock_count, positive_news_count, negative_news_count,
                   top_stocks_json, top_news_json, source_json
            FROM sector_opinion_daily
            WHERE as_of_datetime = (
                SELECT MAX(as_of_datetime)
                FROM sector_opinion_daily
                WHERE as_of_datetime <= %s
            )
            {sector_filter_sql}
            ORDER BY sector_score DESC
            LIMIT 30
            """
            params: Sequence[Any] = (requested_as_of, *sector_filter_params)
        else:
            sql = f"""
            SELECT id, payload_version, trade_date, sector_type, sector_name, as_of_datetime, sector_score, weighted_impact_score,
                   news_count, source_count, stock_count, positive_news_count, negative_news_count,
                   top_stocks_json, top_news_json, source_json
            FROM sector_opinion_daily
            WHERE as_of_datetime = (
                SELECT MAX(as_of_datetime)
                FROM sector_opinion_daily
                WHERE (%s IS NULL OR trade_date >= %s)
            )
            {sector_filter_sql}
            ORDER BY sector_score DESC
            LIMIT 30
            """
            params = (latest_candidate_trade_date, latest_candidate_trade_date, *sector_filter_params)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                sectors = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, net_amount, pct_chg, quote_time
                    FROM market_sector_fund_flow_snapshot
                    WHERE trade_date = (SELECT MAX(trade_date) FROM market_sector_fund_flow_snapshot)
                      AND quote_time >= DATE_SUB((SELECT MAX(quote_time) FROM market_sector_fund_flow_snapshot), INTERVAL 20 MINUTE)
                    """
                )
                fund_rows = cursor.fetchall() or []
        return sectors, fund_rows

    def load_candidate_rows(
        self,
        *,
        daily_kline_operator: str | None,
        cutoff_date: str | None,
        use_pit_fundamental: bool,
        fundamental_date_operator: str,
        fundamental_as_of_date: str,
        use_realtime: bool,
        use_current_popularity: bool,
        instrument_type: str,
        market_board: str,
        candidate_limit: int | None,
    ) -> list[dict[str, Any]]:
        daily_kline_latest_filter = ""
        daily_kline_recent_filter = ""
        daily_kline_window_filter = ""
        cutoff_params: list[Any] = []
        if daily_kline_operator:
            if daily_kline_operator not in {"<", "<="} or not cutoff_date:
                raise ValueError("invalid daily kline cutoff")
            daily_kline_latest_filter = f"WHERE trade_date {daily_kline_operator} %s"
            daily_kline_recent_filter = f"WHERE trade_date {daily_kline_operator} %s"
            daily_kline_window_filter = f"AND trade_date {daily_kline_operator} %s"
            cutoff_params = [cutoff_date, cutoff_date, cutoff_date]
        if fundamental_date_operator not in {"<", "<="} or not fundamental_as_of_date:
            raise ValueError("invalid fundamental PIT cutoff")
        if use_pit_fundamental:
            fundamental_join_sql = f"""
            LEFT JOIN stock_fundamental_pit fp ON fp.id = (
                SELECT fp2.id
                FROM stock_fundamental_pit fp2
                WHERE fp2.code = sb.code
                  AND fp2.announcement_date {fundamental_date_operator} %s
                  AND fp2.period_end_date <= %s
                ORDER BY fp2.period_end_date DESC,
                         fp2.announcement_date DESC,
                         fp2.update_flag DESC,
                         fp2.id DESC
                LIMIT 1
            )
            """
            fundamental_params: list[Any] = [fundamental_as_of_date, fundamental_as_of_date]
        else:
            fundamental_join_sql = "LEFT JOIN stock_fundamental_pit fp ON 1 = 0"
            fundamental_params = []
        market_board_filter = self.market_board_filter_sql(market_board)
        sql = """
        SELECT
            sb.code,
            sb.name,
            sb.industry,
            sb.instrument_type,
            sb.is_st,
            sb.pe_tushare,
            sb.pb_tushare,
            sb.roe,
            sb.roa,
            sb.grossprofit_margin,
            sb.netprofit_margin,
            sb.revenue_yoy,
            sb.profit_yoy,
            sb.eps,
            sb.fundamental_period AS legacy_fundamental_period,
            fp.roe AS pit_roe,
            fp.roa AS pit_roa,
            fp.grossprofit_margin AS pit_grossprofit_margin,
            fp.netprofit_margin AS pit_netprofit_margin,
            fp.revenue_yoy AS pit_revenue_yoy,
            fp.profit_yoy AS pit_profit_yoy,
            fp.eps AS pit_eps,
            fp.period_end_date AS pit_fundamental_period,
            fp.announcement_date AS pit_fundamental_publish_date,
            fp.source AS pit_fundamental_source,
            (fp.id IS NOT NULL) AS pit_fundamental_available,
            dk.open,
            dk.high,
            dk.low,
            dk.close,
            dk.amount,
            dk.trade_date,
            ma.ma5,
            ma.ma10,
            COALESCE(lf.ma20, ma.ma20) AS ma20,
            ma.ma30,
            lf.ma60,
            lf.close_5d,
            COALESCE(lf.close_20d, ma.close_20d) AS close_20d,
            lf.prev_close_1d,
            COALESCE(lf.max_close_20, ma.max_close_20) AS max_close_20,
            COALESCE(lf.min_close_20, ma.min_close_20) AS min_close_20,
            ma.avg_amount_5,
            COALESCE(lf.avg_amount_20, ma.avg_amount_20) AS avg_amount_20,
            COALESCE(lf.kline_count_20, ma.kline_count_20) AS kline_count_20,
            lf.kline_count_60,
            lf.std_return_20,
            lf.pct_chg_1d,
            fid.turnover_rate,
            lf.turnover_rate_5d_avg,
            DATEDIFF(dk.trade_date, sb.listing_date) AS listed_days,
            fid.volume_ratio,
            fid.total_mv,
            fid.completeness_score,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            realtime.latest_price AS realtime_price,
            realtime.pct_chg AS realtime_pct_chg,
            realtime.pre_close AS realtime_pre_close,
            realtime.open_price AS realtime_open,
            realtime.high_price AS realtime_high,
            realtime.low_price AS realtime_low,
            realtime.amount AS realtime_amount,
            realtime.quote_time AS realtime_quote_time,
            realtime.trade_date AS realtime_trade_date,
            realtime_mf.inflow_amount AS realtime_mf_inflow,
            realtime_mf.outflow_amount AS realtime_mf_outflow,
            realtime_mf.net_amount AS realtime_mf_net,
            realtime_mf.amount AS realtime_mf_amount,
            realtime_mf.turnover_rate AS realtime_mf_turnover_rate,
            realtime_mf.quote_time AS realtime_mf_quote_time,
            realtime_mf.trade_date AS realtime_mf_trade_date,
            pop.source AS popularity_source,
            pop.source_rank AS popularity_rank,
            pop.source_score AS popularity_source_score,
            pop.popularity_score,
            pop.quote_time AS popularity_quote_time,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state,
            mcd.market_index_trend_score,
            mcd.market_index_day_score,
            mcd.market_index_pct_chg,
            mcd.market_breadth_score,
            mcd.market_volume_score,
            mcd.market_index_count,
            mcd.market_index_codes,
            mcd.csi300_pct_chg,
            mcd.csi500_pct_chg,
            mcd.csi1000_pct_chg
        FROM stock_basic sb
        {fundamental_join_sql}
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.open, d1.high, d1.low, d1.close, d1.amount
            FROM daily_kline d1
            INNER JOIN (
                SELECT code, MAX(trade_date) AS max_date
                FROM daily_kline
                {daily_kline_latest_filter}
                GROUP BY code
            ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date
        ) dk ON sb.code = dk.code
        LEFT JOIN (
            SELECT
                code,
                AVG(CASE WHEN rn <= 5 THEN close END) AS ma5,
                AVG(CASE WHEN rn <= 10 THEN close END) AS ma10,
                AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
                AVG(CASE WHEN rn <= 30 THEN close END) AS ma30,
                MAX(CASE WHEN rn = 20 THEN close END) AS close_20d,
                MAX(CASE WHEN rn <= 20 THEN close END) AS max_close_20,
                MIN(CASE WHEN rn <= 20 THEN close END) AS min_close_20,
                AVG(CASE WHEN rn <= 5 THEN amount END) AS avg_amount_5,
                AVG(CASE WHEN rn <= 20 THEN amount END) AS avg_amount_20,
                SUM(CASE WHEN rn <= 20 THEN 1 ELSE 0 END) AS kline_count_20
            FROM (
                SELECT
                    code,
                    trade_date,
                    close,
                    amount,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM daily_kline
                WHERE trade_date >= (
                    SELECT MIN(trade_date)
                    FROM (
                        SELECT DISTINCT trade_date
                        FROM daily_kline
                        {daily_kline_recent_filter}
                        ORDER BY trade_date DESC
                        LIMIT 45
                    ) recent_trade_dates
                )
                {daily_kline_window_filter}
            ) ranked
            WHERE rn <= 30
            GROUP BY code
        ) ma ON sb.code = ma.code
        LEFT JOIN lowvol_reversal_feature_daily lf ON lf.code = sb.code AND lf.trade_date = dk.trade_date
        LEFT JOIN factor_input_daily fid ON fid.code = sb.code AND fid.trade_date = dk.trade_date
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = sb.code AND mf.trade_date = dk.trade_date
        LEFT JOIN stock_realtime_snapshot realtime ON %s = 1 AND realtime.code = sb.code
        LEFT JOIN stock_realtime_moneyflow_snapshot realtime_mf ON %s = 1
          AND realtime_mf.code = sb.code
          AND realtime_mf.trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)
          AND realtime_mf.quote_time >= DATE_SUB(
              (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot),
              INTERVAL 20 MINUTE
          )
        LEFT JOIN stock_popularity_snapshot pop ON %s = 1 AND pop.code = sb.code
        LEFT JOIN stock_chip_daily chip ON chip.code = sb.code AND chip.trade_date = dk.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON sb.code = ssd.code
          AND ssd.trade_date = (
              SELECT MAX(s2.trade_date)
              FROM stock_sentiment_daily s2
              WHERE s2.code = sb.code
                AND (dk.trade_date IS NULL OR s2.trade_date <= dk.trade_date)
          )
        LEFT JOIN (
            SELECT
                trade_date,
                AVG(market_strength) AS market_strength,
                CASE
                    WHEN AVG(market_strength) >= 60 THEN 'bull'
                    WHEN AVG(market_strength) <= 40 THEN 'bear'
                    ELSE 'neutral'
                END AS market_state,
                AVG(trend_score) AS market_index_trend_score,
                AVG(sentiment_score) AS market_index_day_score,
                AVG(index_pct_chg) AS market_index_pct_chg,
                AVG(breadth_score) AS market_breadth_score,
                AVG(volume_score) AS market_volume_score,
                COUNT(DISTINCT index_code) AS market_index_count,
                GROUP_CONCAT(DISTINCT index_code ORDER BY index_code SEPARATOR ',') AS market_index_codes,
                MAX(CASE WHEN index_code = '000300.SH' THEN index_pct_chg END) AS csi300_pct_chg,
                MAX(CASE WHEN index_code = '000905.SH' THEN index_pct_chg END) AS csi500_pct_chg,
                MAX(CASE WHEN index_code = '000852.SH' THEN index_pct_chg END) AS csi1000_pct_chg
            FROM market_context_daily
            WHERE index_code IN ('000300.SH', '000905.SH', '000852.SH')
            GROUP BY trade_date
        ) mcd ON dk.trade_date = mcd.trade_date
        WHERE sb.is_delisted = 0
          AND sb.instrument_type = %s
          {market_board_filter}
        ORDER BY (dk.trade_date IS NULL), dk.trade_date DESC, sb.code
        """.format(
            daily_kline_latest_filter=daily_kline_latest_filter,
            daily_kline_recent_filter=daily_kline_recent_filter,
            daily_kline_window_filter=daily_kline_window_filter,
            fundamental_join_sql=fundamental_join_sql,
            market_board_filter=market_board_filter,
        )
        params = [
            *fundamental_params,
            *cutoff_params,
            1 if use_realtime else 0,
            1 if use_realtime else 0,
            1 if use_current_popularity else 0,
            instrument_type,
        ]
        if candidate_limit:
            sql += " LIMIT %s"
            params.append(int(candidate_limit))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall() or []

    def save_result_rows(
        self,
        *,
        payload: list[tuple[Any, ...]],
        run_id: str,
    ) -> None:
        if not payload:
            return
        insert_sql = """
        INSERT INTO selection_result (
            run_id, trade_date, strategy_id, code, score, rank_no, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            run_id = VALUES(run_id),
            score = VALUES(score),
            rank_no = VALUES(rank_no),
            metadata_json = VALUES(metadata_json)
        """
        run_dedupe_sql = """
        DELETE newer
        FROM selection_result newer
        INNER JOIN selection_result older
          ON newer.run_id = older.run_id
         AND newer.code = older.code
         AND newer.id > older.id
        WHERE newer.run_id = %s
        """
        tracking_dedupe_sql = """
        DELETE older
        FROM selection_result older
        INNER JOIN selection_result newer
          ON older.trade_date = newer.trade_date
         AND older.strategy_id = newer.strategy_id
         AND older.code = newer.code
         AND older.id < newer.id
        WHERE newer.trade_date = %s
          AND newer.strategy_id = %s
          AND newer.code = %s
        """
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, payload)
                cursor.execute(run_dedupe_sql, (run_id,))
                for _, trade_date, strategy_id, code, *_ in payload:
                    cursor.execute(tracking_dedupe_sql, (trade_date, strategy_id, code))
