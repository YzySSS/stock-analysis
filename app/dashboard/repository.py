from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class DashboardRepository:
    """Persistence boundary for dashboard read models."""

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _placeholders(items: list[Any]) -> str:
        if not items:
            raise ValueError("at least one item is required")
        return ", ".join(["%s"] * len(items))

    def load_emotion_board_inputs(self, limit: int) -> dict[str, Any]:
        final_limit = max(1, int(limit))
        st_name_sql = "(COALESCE(r.name, sb.name) LIKE '*ST%%' OR COALESCE(r.name, sb.name) LIKE 'ST%%' OR COALESCE(r.name, sb.name) LIKE '退市%%')"
        limit_rate_sql = f"""
            CASE
                WHEN sb.code LIKE 'bj.%%' THEN 0.30
                WHEN sb.code LIKE 'sz.300%%' OR sb.code LIKE 'sz.301%%' OR sb.code LIKE 'sh.688%%' THEN 0.20
                WHEN {st_name_sql} THEN 0.05
                ELSE 0.10
            END
        """

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        r.code,
                        COALESCE(r.name, sb.name) AS name,
                        sb.industry,
                        r.latest_price,
                        r.pct_chg,
                        r.pre_close,
                        r.amount AS realtime_amount,
                        r.quote_time,
                        fid.turnover_rate,
                        fid.volume_ratio,
                        sb.pe_tushare,
                        sb.eps,
                        sb.roe,
                        sb.profit_yoy
                    FROM stock_realtime_snapshot r
                    LEFT JOIN stock_basic sb ON r.code = sb.code
                    LEFT JOIN factor_input_daily fid ON fid.code = r.code
                      AND fid.trade_date = (SELECT MAX(trade_date) FROM factor_input_daily)
                    WHERE sb.instrument_type = 'stock'
                      AND r.code NOT LIKE 'bj.%%'
                      AND r.code NOT LIKE 'sh.688%%'
                      AND r.code NOT LIKE 'sz.300%%'
                      AND r.code NOT LIKE 'sz.301%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE '*ST%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE 'ST%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE '退市%%'
                      AND r.pre_close > 0
                      AND r.latest_price > 0
                      AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2)
                    ORDER BY r.pct_chg DESC, r.amount DESC
                    LIMIT %s
                    """,
                    (max(final_limit * 3, 20),),
                )
                limit_rows = cursor.fetchall() or []

                cursor.execute(
                    """
                    SELECT id, payload_version, sector_type, sector_name,
                           as_of_datetime, sector_score, top_stocks_json,
                           top_news_json, source_json
                    FROM sector_opinion_daily
                    WHERE as_of_datetime = (SELECT MAX(as_of_datetime) FROM sector_opinion_daily)
                      AND sector_type = 'theme'
                    ORDER BY sector_score DESC
                    LIMIT 40
                    """
                )
                theme_rows = cursor.fetchall() or []

                cursor.execute(
                    f"""
                    SELECT
                        r.code,
                        COALESCE(r.name, sb.name) AS name,
                        sb.industry,
                        r.latest_price,
                        r.pct_chg,
                        r.pre_close,
                        r.amount AS realtime_amount,
                        r.quote_time,
                        fid.turnover_rate,
                        fid.volume_ratio,
                        sb.pe_tushare,
                        sb.eps,
                        sb.roe,
                        sb.profit_yoy,
                        mf.net_amount AS realtime_net_amount,
                        mf.inflow_amount AS realtime_inflow_amount,
                        mf.outflow_amount AS realtime_outflow_amount,
                        mf.source_unit AS realtime_moneyflow_unit,
                        pop.source_rank AS popularity_rank,
                        pop.popularity_score
                    FROM stock_realtime_snapshot r
                    LEFT JOIN stock_basic sb ON r.code = sb.code
                    LEFT JOIN factor_input_daily fid ON fid.code = r.code
                      AND fid.trade_date = (SELECT MAX(trade_date) FROM factor_input_daily)
                    LEFT JOIN stock_realtime_moneyflow_snapshot mf ON mf.code = r.code
                    LEFT JOIN stock_popularity_snapshot pop ON pop.code = r.code
                    WHERE sb.instrument_type = 'stock'
                      AND r.code NOT LIKE 'bj.%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE '*ST%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE 'ST%%'
                      AND COALESCE(r.name, sb.name) NOT LIKE '退市%%'
                      AND r.pre_close > 0
                      AND r.latest_price > 0
                      AND r.amount >= 80000000
                      AND r.pct_chg >= 2.5
                      AND r.latest_price < ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2)
                    ORDER BY r.pct_chg DESC, r.amount DESC
                    LIMIT %s
                    """,
                    (max(final_limit * 20, 120),),
                )
                hot_limit_rows = cursor.fetchall() or []

                cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM daily_kline")
                latest_kline_date = (cursor.fetchone() or {}).get("latest_trade_date")

                reversal_rows: list[dict[str, Any]] = []
                if latest_kline_date:
                    cursor.execute(
                        """
                        SELECT
                            r.code,
                            COALESCE(r.name, sb.name) AS name,
                            sb.industry,
                            r.latest_price,
                            r.pct_chg,
                            r.pre_close,
                            r.amount AS realtime_amount,
                            r.quote_time,
                            dk.open AS prev_open,
                            dk.high AS prev_high,
                            dk.low AS prev_low,
                            dk.close AS prev_close,
                            dk.amount AS prev_amount,
                            fid.turnover_rate,
                            fid.volume_ratio,
                            sb.pe_tushare,
                            sb.eps,
                            sb.roe,
                            sb.profit_yoy
                        FROM stock_realtime_snapshot r
                        INNER JOIN daily_kline dk ON dk.code = r.code AND dk.trade_date = %s
                        LEFT JOIN stock_basic sb ON r.code = sb.code
                        LEFT JOIN factor_input_daily fid ON fid.code = r.code
                          AND fid.trade_date = (SELECT MAX(trade_date) FROM factor_input_daily)
                        WHERE sb.instrument_type = 'stock'
                          AND r.code NOT LIKE 'bj.%%'
                          AND r.code NOT LIKE 'sh.688%%'
                          AND r.code NOT LIKE 'sz.300%%'
                          AND r.code NOT LIKE 'sz.301%%'
                          AND COALESCE(r.name, sb.name) NOT LIKE '*ST%%'
                          AND COALESCE(r.name, sb.name) NOT LIKE 'ST%%'
                          AND COALESCE(r.name, sb.name) NOT LIKE '退市%%'
                          AND r.latest_price > 0
                          AND r.pct_chg >= 3
                          AND r.amount >= 100000000
                          AND dk.high > 0
                          AND dk.close > 0
                          AND ((dk.high - dk.close) / dk.high * 100) >= 4
                        ORDER BY r.pct_chg DESC, r.amount DESC
                        LIMIT %s
                        """,
                        (latest_kline_date, max(final_limit * 20, 120)),
                    )
                    reversal_rows = cursor.fetchall() or []

                limit_codes = list(dict.fromkeys(str(row.get("code")) for row in limit_rows if row.get("code")))
                history_codes = list(
                    dict.fromkeys(
                        [*limit_codes, *[str(row.get("code")) for row in reversal_rows if row.get("code")]]
                    )
                )

                intraday_rows: list[dict[str, Any]] = []
                if limit_codes:
                    placeholders = self._placeholders(limit_codes)
                    cursor.execute(
                        f"""
                        SELECT code, name, trade_date, quote_minute, latest_price, pre_close
                        FROM stock_realtime_intraday
                        WHERE code IN ({placeholders})
                          AND trade_date = (SELECT MAX(trade_date) FROM stock_realtime_intraday)
                          AND latest_price IS NOT NULL
                          AND pre_close IS NOT NULL
                        ORDER BY code, quote_minute
                        """,
                        limit_codes,
                    )
                    intraday_rows = cursor.fetchall() or []

                history_by_code: dict[str, list[dict[str, Any]]] = {}
                if history_codes:
                    placeholders = self._placeholders(history_codes)
                    cursor.execute(
                        f"""
                        SELECT code, trade_date, close
                        FROM (
                            SELECT code, trade_date, close,
                                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS row_no
                            FROM daily_kline
                            WHERE code IN ({placeholders})
                        ) ranked
                        WHERE row_no <= 9
                        ORDER BY code, trade_date
                        """,
                        history_codes,
                    )
                    for row in cursor.fetchall() or []:
                        history_by_code.setdefault(str(row["code"]), []).append(row)

        return {
            "limit_rows": limit_rows,
            "theme_rows": theme_rows,
            "hot_limit_rows": hot_limit_rows,
            "latest_kline_date": latest_kline_date,
            "reversal_rows": reversal_rows,
            "intraday_rows": intraday_rows,
            "history_by_code": history_by_code,
        }

    def load_market_overview_inputs(self) -> dict[str, Any]:
        st_name_sql = "(COALESCE(r.name, sb.name) LIKE '*ST%' OR COALESCE(r.name, sb.name) LIKE 'ST%' OR COALESCE(r.name, sb.name) LIKE '退市%')"
        limit_rate_sql = f"""
            CASE
                WHEN sb.code LIKE 'bj.%%' THEN 0.30
                WHEN sb.code LIKE 'sz.300%%' OR sb.code LIKE 'sz.301%%' OR sb.code LIKE 'sh.688%%' THEN 0.20
                WHEN {st_name_sql} THEN 0.05
                ELSE 0.10
            END
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        SUM(r.pct_chg > 0) AS up_count,
                        SUM(r.pct_chg < 0) AS down_count,
                        SUM(r.pct_chg = 0) AS flat_count,
                        SUM(CASE WHEN r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS limit_up_count,
                        SUM(CASE WHEN r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS limit_down_count,
                        SUM(CASE WHEN {st_name_sql} AND NOT (r.code LIKE 'bj.%%' OR r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS st_limit_up_count,
                        SUM(CASE WHEN {st_name_sql} AND NOT (r.code LIKE 'bj.%%' OR r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS st_limit_down_count,
                        SUM(CASE WHEN (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board20_limit_up_count,
                        SUM(CASE WHEN (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board20_limit_down_count,
                        SUM(CASE WHEN r.code LIKE 'bj.%%' AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board30_limit_up_count,
                        SUM(CASE WHEN r.code LIKE 'bj.%%' AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board30_limit_down_count,
                        SUM(CASE WHEN NOT {st_name_sql} AND NOT (r.code LIKE 'bj.%%') AND NOT (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board10_limit_up_count,
                        SUM(CASE WHEN NOT {st_name_sql} AND NOT (r.code LIKE 'bj.%%') AND NOT (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board10_limit_down_count,
                        SUM(r.pct_chg >= 5) AS strong_up_count,
                        SUM(r.pct_chg <= -5) AS strong_down_count,
                        AVG(r.pct_chg) AS avg_pct_chg,
                        SUM(r.amount * r.pct_chg) / NULLIF(SUM(r.amount), 0) AS amount_weighted_pct_chg,
                        SUM(CASE WHEN r.pct_chg > 0 THEN r.amount ELSE 0 END) AS up_amount,
                        SUM(CASE WHEN r.pct_chg < 0 THEN r.amount ELSE 0 END) AS down_amount,
                        SUM(r.amount) AS total_amount,
                        MAX(r.quote_time) AS latest_quote_time,
                        MAX(r.trade_date) AS trade_date
                    FROM stock_realtime_snapshot r
                    LEFT JOIN stock_basic sb ON r.code = sb.code
                    WHERE r.pct_chg IS NOT NULL
                      AND (r.code LIKE 'sh.%%' OR r.code LIKE 'sz.%%' OR r.code LIKE 'bj.%%')
                    """
                )
                breadth = cursor.fetchone() or {}

                sector_columns = """
                    sector_type, sector_name, pct_chg, inflow_amount,
                    outflow_amount, net_amount, company_count, leading_stock,
                    leading_stock_pct_chg, leading_stock_price, quote_time, source_unit
                """
                cursor.execute(
                    f"""
                    SELECT {sector_columns}
                    FROM market_sector_fund_flow_snapshot
                    WHERE net_amount IS NOT NULL
                    ORDER BY net_amount DESC, pct_chg DESC
                    LIMIT 8
                    """
                )
                fund_strong_rows = cursor.fetchall() or []
                cursor.execute(
                    f"""
                    SELECT {sector_columns}
                    FROM market_sector_fund_flow_snapshot
                    WHERE net_amount IS NOT NULL
                    ORDER BY net_amount ASC, pct_chg ASC
                    LIMIT 8
                    """
                )
                fund_weak_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT MAX(quote_time) AS latest_fund_flow_time, COUNT(*) AS fund_flow_rows
                    FROM market_sector_fund_flow_snapshot
                    """
                )
                fund_flow_meta = cursor.fetchone() or {}

                industry_columns = """
                    sb.industry,
                    COUNT(*) AS stock_count,
                    AVG(r.pct_chg) AS avg_pct_chg,
                    SUM(r.amount * r.pct_chg) / NULLIF(SUM(r.amount), 0) AS amount_weighted_pct_chg,
                    SUM(r.pct_chg > 0) AS up_count,
                    SUM(r.pct_chg < 0) AS down_count,
                    SUM(CASE WHEN r.pct_chg > 0 THEN r.amount ELSE 0 END) AS up_amount,
                    SUM(CASE WHEN r.pct_chg < 0 THEN r.amount ELSE 0 END) AS down_amount,
                    SUM(r.amount) AS amount
                """
                industry_from = """
                    FROM stock_realtime_snapshot r
                    INNER JOIN stock_basic sb ON r.code = sb.code
                    WHERE sb.instrument_type = 'stock'
                      AND r.pct_chg IS NOT NULL
                      AND sb.industry IS NOT NULL
                      AND sb.industry <> ''
                    GROUP BY sb.industry
                    HAVING stock_count >= 5
                """
                cursor.execute(
                    f"SELECT {industry_columns} {industry_from} ORDER BY amount_weighted_pct_chg DESC, avg_pct_chg DESC LIMIT 8"
                )
                strong_rows = cursor.fetchall() or []
                cursor.execute(
                    f"SELECT {industry_columns} {industry_from} ORDER BY amount_weighted_pct_chg ASC, avg_pct_chg ASC LIMIT 8"
                )
                weak_rows = cursor.fetchall() or []

                current_trade_date = breadth.get("trade_date")
                if current_trade_date:
                    cursor.execute(
                        """
                        SELECT trade_date, market_strength
                        FROM market_context_daily
                        WHERE market_strength IS NOT NULL AND trade_date < %s
                        ORDER BY trade_date DESC
                        LIMIT 1
                        """,
                        (current_trade_date,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT trade_date, market_strength
                        FROM market_context_daily
                        WHERE market_strength IS NOT NULL
                        ORDER BY trade_date DESC
                        LIMIT 1
                        """
                    )
                previous_strength_row = cursor.fetchone() or {}

        return {
            "breadth": breadth,
            "fund_strong_rows": fund_strong_rows,
            "fund_weak_rows": fund_weak_rows,
            "fund_flow_meta": fund_flow_meta,
            "strong_rows": strong_rows,
            "weak_rows": weak_rows,
            "previous_strength": {
                "trade_date": str(previous_strength_row.get("trade_date")) if previous_strength_row.get("trade_date") else None,
                "market_strength": round(float(previous_strength_row.get("market_strength")), 2)
                if previous_strength_row.get("market_strength") is not None
                else None,
            },
        }

    def load_hot_theme_inputs(self) -> dict[str, list[dict[str, Any]]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id, payload_version, trade_date, sector_type, sector_name,
                        as_of_datetime, sector_score, weighted_impact_score,
                        news_count, source_count, stock_count, positive_news_count,
                        negative_news_count, top_stocks_json, top_news_json
                    FROM sector_opinion_daily
                    WHERE as_of_datetime = (SELECT MAX(as_of_datetime) FROM sector_opinion_daily)
                      AND sector_type = 'theme'
                    ORDER BY sector_score DESC, weighted_impact_score DESC
                    LIMIT 30
                    """
                )
                opinion_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, pct_chg, net_amount,
                           company_count, leading_stock, leading_stock_pct_chg, quote_time
                    FROM market_sector_fund_flow_snapshot
                    WHERE sector_type = 'concept'
                      AND net_amount IS NOT NULL
                      AND quote_time >= DATE_SUB((SELECT MAX(quote_time) FROM market_sector_fund_flow_snapshot), INTERVAL 20 MINUTE)
                    ORDER BY net_amount DESC, pct_chg DESC
                    LIMIT 40
                    """
                )
                fund_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT concept_name, concept_code, summary_date, quote_time,
                           driver_event, leading_stock, member_count, ths_score
                    FROM ths_concept_hot_snapshot
                    WHERE quote_time >= DATE_SUB((SELECT MAX(quote_time) FROM ths_concept_hot_snapshot), INTERVAL 120 MINUTE)
                    ORDER BY ths_score DESC, summary_date DESC
                    LIMIT 40
                    """
                )
                ths_rows = cursor.fetchall() or []
        return {
            "opinion_rows": opinion_rows,
            "fund_rows": fund_rows,
            "ths_rows": ths_rows,
        }
