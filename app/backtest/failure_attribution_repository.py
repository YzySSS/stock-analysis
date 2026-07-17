from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class StrategyFailureAttributionRepository:
    """Read-only persistence boundary for frozen-run failure attribution."""

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def load_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM backtest_run WHERE run_id=%s", (run_id,))
                return cursor.fetchone()

    def load_signal_dates(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, pick_count
                    FROM backtest_summary_daily
                    WHERE run_id=%s
                    ORDER BY trade_date
                    """,
                    (run_id,),
                )
                return cursor.fetchall() or []

    def load_trade_rows(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    WITH base AS (
                        SELECT p.run_id, p.trade_date, p.code, p.rank_no, p.score, p.factor_json,
                               t.entry_date, t.entry_price, t.exit_date_1d, t.exit_price_1d
                        FROM backtest_pick p
                        JOIN backtest_trade t
                          ON t.run_id=p.run_id
                         AND t.trade_date=p.trade_date
                         AND t.code=p.code
                        WHERE p.run_id=%s
                    ), future_bars AS (
                        SELECT b.*, k.trade_date AS bar_date, k.close AS bar_close,
                               ROW_NUMBER() OVER (
                                   PARTITION BY b.run_id, b.trade_date, b.code
                                   ORDER BY k.trade_date
                               ) AS bar_number
                        FROM base b
                        LEFT JOIN daily_kline k
                          ON k.code=b.code
                         AND k.trade_date >= b.entry_date
                         AND k.trade_date <= DATE_ADD(b.entry_date, INTERVAL 30 DAY)
                    )
                    SELECT run_id, trade_date, code, rank_no, score, factor_json,
                           entry_date, entry_price, exit_date_1d, exit_price_1d,
                           MAX(CASE WHEN bar_number=3 THEN bar_close END) AS exit_price_3d,
                           MAX(CASE WHEN bar_number=5 THEN bar_close END) AS exit_price_5d,
                           MAX(CASE WHEN bar_number=10 THEN bar_close END) AS exit_price_10d
                    FROM future_bars
                    GROUP BY run_id, trade_date, code, rank_no, score, factor_json,
                             entry_date, entry_price, exit_date_1d, exit_price_1d
                    ORDER BY trade_date, rank_no, code
                    """,
                    (run_id,),
                )
                return cursor.fetchall() or []

    def load_market_rows(self, index_code: str, end_date: Any) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, open, close
                    FROM market_index_daily
                    WHERE index_code=%s AND trade_date <= %s
                    ORDER BY trade_date
                    """,
                    (index_code, end_date),
                )
                return cursor.fetchall() or []
