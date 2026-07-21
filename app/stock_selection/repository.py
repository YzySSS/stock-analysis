from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable, Sequence

from app.shared.db import mysql_conn, mysql_read_conn


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

    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
        read_connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        # A borrowed connection (the snapshot materializer) must keep reads in
        # the caller's REPEATABLE READ transaction. Normal reads use the
        # rollback-on-exit context and never issue meaningless commits.
        self._read_connection_factory = (
            read_connection_factory or connection_factory or mysql_read_conn
        )

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def _read_connect(self, *, dict_cursor: bool = True):
        return self._read_connection_factory(dict_cursor=dict_cursor)

    @classmethod
    def market_board_filter_sql(cls, market_board: str) -> str:
        try:
            return cls.BOARD_FILTERS[market_board]
        except KeyError as exc:
            raise ValueError(f"unsupported market board: {market_board}") from exc

    def count_instruments(self, instrument_type: str) -> int:
        with self._read_connect() as conn:
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

        with self._read_connect() as conn:
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
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM selection_run WHERE run_id=%s", (run_id,))
                return cursor.fetchone()

    def list_runs(self, limit: int) -> list[dict[str, Any]]:
        with self._read_connect() as conn:
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
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
                return (cursor.fetchone() or {}).get("trade_date")

    def get_active_run_by_idempotency(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._read_connect() as conn:
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
        with self._read_connect() as conn:
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
        decision_as_of: Any | None = None,
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
        point_in_time = decision_as_of or requested_as_of
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
                  AND created_at <= %s
                  AND updated_at <= %s
            )
            {sector_filter_sql}
            ORDER BY sector_score DESC
            LIMIT 30
            """
            params: Sequence[Any] = (
                requested_as_of,
                point_in_time,
                point_in_time,
                *sector_filter_params,
            )
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

        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                sectors = cursor.fetchall() or []
                if point_in_time:
                    cursor.execute(
                        """
                        SELECT sector_type, sector_name, net_amount, pct_chg, quote_time
                        FROM market_sector_fund_flow_snapshot
                        WHERE trade_date = (
                            SELECT MAX(trade_date)
                            FROM market_sector_fund_flow_snapshot
                            WHERE quote_time <= %s AND created_at <= %s AND updated_at <= %s
                        )
                          AND quote_time <= %s
                          AND created_at <= %s
                          AND updated_at <= %s
                          AND quote_time >= DATE_SUB((
                              SELECT MAX(quote_time)
                              FROM market_sector_fund_flow_snapshot
                              WHERE quote_time <= %s AND created_at <= %s AND updated_at <= %s
                          ), INTERVAL 20 MINUTE)
                        """,
                        (point_in_time,) * 9,
                    )
                else:
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
        decision_as_of: Any | None = None,
        expected_realtime_batch_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        daily_kline_latest_filter = ""
        daily_kline_params: list[Any] = []
        technical_feature_filters: list[str] = []
        technical_feature_params: list[Any] = []
        if daily_kline_operator:
            if daily_kline_operator not in {"<", "<="} or not cutoff_date:
                raise ValueError("invalid daily kline cutoff")
            daily_kline_latest_filter = f"WHERE trade_date {daily_kline_operator} %s"
            daily_kline_params.append(cutoff_date)
            technical_feature_filters.append(f"trade_date {daily_kline_operator} %s")
            technical_feature_params.append(cutoff_date)
        if fundamental_date_operator not in {"<", "<="} or not fundamental_as_of_date:
            raise ValueError("invalid fundamental PIT cutoff")
        # PIT fundamentals are intentionally outside the hot sentiment-wide
        # query. They remain available to stock detail/risk workflows, while
        # selection uses event, market, technical, money-flow and chip data.
        fundamental_join_sql = ""
        fundamental_params: list[Any] = []
        market_board_filter = self.market_board_filter_sql(market_board)
        if decision_as_of is not None:
            technical_feature_filters.append("computed_at <= %s")
            technical_feature_params.append(decision_as_of)
        technical_feature_date_filter = (
            "WHERE " + " AND ".join(technical_feature_filters)
            if technical_feature_filters
            else ""
        )
        decision_date = None
        if decision_as_of is not None:
            decision_date = (
                decision_as_of.date()
                if hasattr(decision_as_of, "date") and not isinstance(decision_as_of, str)
                else str(decision_as_of)[:10]
            )
        status_cutoff_sql = "%s" if decision_date is not None else "CURRENT_DATE"
        status_knowledge_sql = ""
        status_params: list[Any] = []
        if decision_date is not None:
            status_params = [decision_date, decision_as_of, decision_as_of]
            status_knowledge_sql = " AND ss2.created_at <= %s AND ss2.updated_at <= %s"

        normalized_batch_ids = sorted(
            {
                str(value).strip()
                for value in (expected_realtime_batch_ids or [])
                if str(value).strip()
            }
        )
        realtime_filter_sql = ""
        realtime_params: list[Any] = []
        if decision_as_of is not None:
            realtime_filter_sql = """
              AND realtime.trade_date = %s
              AND realtime.quote_time <= %s
              AND realtime.received_at <= %s
              AND realtime.updated_at <= %s
              AND COALESCE(realtime.is_stale, 0) = 0
            """
            realtime_params.extend(
                [decision_date, decision_as_of, decision_as_of, decision_as_of]
            )
        elif use_realtime:
            realtime_filter_sql = """
              AND realtime.trade_date = CURRENT_DATE
              AND COALESCE(realtime.is_stale, 0) = 0
            """
        if normalized_batch_ids:
            placeholders = ",".join(["%s"] * len(normalized_batch_ids))
            realtime_filter_sql += f" AND realtime.batch_id IN ({placeholders})"
            realtime_params.extend(normalized_batch_ids)

        realtime_moneyflow_filter_sql = """
          AND realtime_mf.trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)
          AND realtime_mf.quote_time >= DATE_SUB(
              (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot),
              INTERVAL 20 MINUTE
          )
        """
        realtime_moneyflow_params: list[Any] = []
        if decision_as_of is not None:
            realtime_moneyflow_filter_sql = """
              AND realtime_mf.trade_date = (
                  SELECT MAX(trade_date)
                  FROM stock_realtime_moneyflow_snapshot
                  WHERE quote_time <= %s AND created_at <= %s AND updated_at <= %s
              )
              AND realtime_mf.quote_time <= %s
              AND realtime_mf.created_at <= %s
              AND realtime_mf.updated_at <= %s
              AND realtime_mf.quote_time >= DATE_SUB((
                  SELECT MAX(quote_time)
                  FROM stock_realtime_moneyflow_snapshot
                  WHERE quote_time <= %s AND created_at <= %s AND updated_at <= %s
              ), INTERVAL 20 MINUTE)
            """
            realtime_moneyflow_params = [decision_as_of] * 9

        popularity_filter_sql = ""
        popularity_params: list[Any] = []
        if decision_as_of is not None:
            popularity_filter_sql = """
              AND pop.quote_time <= %s
              AND pop.created_at <= %s
              AND pop.updated_at <= %s
            """
            popularity_params = [decision_as_of] * 3
        technical_feature_projection = """
            ma.ma5,
            ma.ma10,
            ma.ma20,
            ma.ma30,
            ma.ma60,
            ma.close_5d,
            ma.close_20d,
            ma.prev_close_1d,
            ma.max_close_20,
            ma.min_close_20,
            ma.avg_amount_5,
            ma.avg_amount_20,
            ma.median_amount_20,
            ma.kline_count_20,
            ma.kline_count_60,
            ma.std_return_20,
            ma.pct_chg_1d,
            ma.return_5d_pct,
            ma.return_20d_pct,
            ma.latest_amount AS technical_latest_amount,
            ma.amount_ratio_5_20,
            ma.trade_date AS technical_feature_trade_date,
            ma.source_trade_date AS technical_source_trade_date,
        """
        technical_feature_join = f"""
        LEFT JOIN stock_technical_feature_daily ma
          ON ma.code = sb.code
         AND ma.trade_date = (
             SELECT MAX(trade_date)
             FROM stock_technical_feature_daily
             {technical_feature_date_filter}
         )
        """
        sql_template = """
        SELECT
            sb.code,
            sb.name,
            sb.industry,
            sb.instrument_type,
            sb.is_st,
            lifecycle.list_status,
            (lifecycle.code IS NOT NULL) AS lifecycle_known,
            COALESCE(name_state.is_delisting_period, 0) AS is_delisting,
            COALESCE(status_state.status_label, '') AS stock_status_label,
            COALESCE(status_state.status_label, '') IN ('suspended', 'paused_listing') AS is_suspended,
            (dk.code IS NOT NULL) AS daily_data_available,
            (ma.code IS NOT NULL) AS technical_data_available,
            (fid.code IS NOT NULL) AS factor_data_available,
            (mf.code IS NOT NULL) AS daily_moneyflow_data_available,
            (chip.code IS NOT NULL) AS chip_data_available,
            (realtime.code IS NOT NULL) AS realtime_data_available,
            NULL AS pe_tushare,
            NULL AS pb_tushare,
            NULL AS roe,
            NULL AS roa,
            NULL AS grossprofit_margin,
            NULL AS netprofit_margin,
            NULL AS revenue_yoy,
            NULL AS profit_yoy,
            NULL AS eps,
            NULL AS legacy_fundamental_period,
            NULL AS pit_roe,
            NULL AS pit_roa,
            NULL AS pit_grossprofit_margin,
            NULL AS pit_netprofit_margin,
            NULL AS pit_revenue_yoy,
            NULL AS pit_profit_yoy,
            NULL AS pit_eps,
            NULL AS pit_fundamental_period,
            NULL AS pit_fundamental_publish_date,
            NULL AS pit_fundamental_source,
            0 AS pit_fundamental_available,
            dk.open,
            dk.high,
            dk.low,
            dk.close,
            dk.amount,
            dk.trade_date,
            {technical_feature_projection}
            fid.turnover_rate,
            NULL AS turnover_rate_5d_avg,
            DATEDIFF(dk.trade_date, sb.listing_date) AS listed_days,
            ma.kline_count_60 AS listed_trade_days,
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
            realtime.batch_id AS realtime_batch_id,
            realtime.received_at AS realtime_received_at,
            realtime.is_stale AS realtime_is_stale,
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
        LEFT JOIN stock_instrument_lifecycle lifecycle ON lifecycle.code = sb.code
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
        LEFT JOIN stock_name_history name_state ON name_state.id = (
            SELECT nh2.id
            FROM stock_name_history nh2
            WHERE nh2.code = sb.code
              AND (dk.trade_date IS NULL OR nh2.start_date <= dk.trade_date)
              AND (nh2.end_date IS NULL OR dk.trade_date IS NULL OR nh2.end_date >= dk.trade_date)
            ORDER BY nh2.start_date DESC, nh2.id DESC
            LIMIT 1
        )
        LEFT JOIN stock_status_snapshot status_state
          ON status_state.id = (
              SELECT ss2.id
              FROM stock_status_snapshot ss2
              WHERE ss2.code = sb.code
                AND ss2.trade_date <= {status_cutoff_sql}
                {status_knowledge_sql}
              ORDER BY ss2.trade_date DESC, ss2.id DESC
              LIMIT 1
          )
        {technical_feature_join}
        LEFT JOIN factor_input_daily fid ON fid.code = sb.code AND fid.trade_date = dk.trade_date
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = sb.code AND mf.trade_date = dk.trade_date
        LEFT JOIN stock_realtime_snapshot realtime ON %s = 1 AND realtime.code = sb.code
          {realtime_filter_sql}
        LEFT JOIN stock_realtime_moneyflow_snapshot realtime_mf ON %s = 1
          AND realtime_mf.code = sb.code
          {realtime_moneyflow_filter_sql}
        LEFT JOIN stock_popularity_snapshot pop ON %s = 1 AND pop.code = sb.code
          {popularity_filter_sql}
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
        """

        def build_sql() -> str:
            return sql_template.format(
                daily_kline_latest_filter=daily_kline_latest_filter,
                fundamental_join_sql=fundamental_join_sql,
                market_board_filter=market_board_filter,
                technical_feature_projection=technical_feature_projection,
                technical_feature_join=technical_feature_join,
                status_cutoff_sql=status_cutoff_sql,
                status_knowledge_sql=status_knowledge_sql,
                realtime_filter_sql=realtime_filter_sql,
                realtime_moneyflow_filter_sql=realtime_moneyflow_filter_sql,
                popularity_filter_sql=popularity_filter_sql,
            )

        sql = build_sql()
        params = [
            *fundamental_params,
            *daily_kline_params,
            *status_params,
            *technical_feature_params,
            1 if use_realtime else 0,
            *realtime_params,
            1 if use_realtime else 0,
            *realtime_moneyflow_params,
            1 if use_current_popularity else 0,
            *popularity_params,
            instrument_type,
        ]
        if candidate_limit:
            sql += " LIMIT %s"
            params.append(int(candidate_limit))
        with self._read_connect() as conn:
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
