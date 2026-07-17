from __future__ import annotations

import json
from contextlib import AbstractContextManager
from typing import Any, Callable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class StrategyValidationRepository:
    """Persistence boundary for frozen strategy validation protocols."""

    INSERT_FIELDS = (
        "protocol_id",
        "batch_id",
        "strategy_id",
        "strategy_version",
        "strategy_config_hash",
        "methodology_version",
        "protocol_version",
        "validation_mode",
        "eligible_for_validation",
        "frozen_at",
        "freeze_data_cutoff_date",
        "start_date",
        "end_date",
        "universe_code",
        "return_mode",
        "benchmark_index_code",
        "max_picks",
        "score_threshold",
        "use_adjusted_price",
        "commission_bps",
        "stamp_tax_bps",
        "slippage_bps",
        "execution_constraints_enabled",
        "minimum_trade_days",
        "minimum_trades",
        "strategy_snapshot_json",
        "request_json",
        "criteria_json",
        "status",
        "verdict",
        "validation_status",
    )

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, default=str)

    def current_data_cutoff(self) -> Any:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT LEAST(
                        (SELECT MAX(trade_date) FROM factor_input_daily),
                        (SELECT MAX(trade_date) FROM daily_kline)
                    ) AS data_cutoff_date
                    """
                )
                return (cursor.fetchone() or {}).get("data_cutoff_date")

    def create_protocol(self, values: dict[str, Any]) -> None:
        placeholders = ", ".join(["%s"] * len(self.INSERT_FIELDS))
        columns = ", ".join(self.INSERT_FIELDS)
        params = []
        for field in self.INSERT_FIELDS:
            value = values.get(field)
            if field.endswith("_json"):
                value = self._json(value)
            params.append(value)
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO strategy_validation_protocol ({columns}) VALUES ({placeholders})",
                    params,
                )

    def get_protocol(self, protocol_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM strategy_validation_protocol WHERE protocol_id=%s",
                    (protocol_id,),
                )
                return cursor.fetchone()

    def list_protocols(
        self,
        *,
        limit: int = 20,
        strategy_id: str | None = None,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if strategy_id:
            conditions.append("strategy_id=%s")
            params.append(strategy_id)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT *
                    FROM strategy_validation_protocol
                    {where_sql}
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    params,
                )
                return cursor.fetchall() or []

    def mark_running(self, protocol_id: str, executed_at: str) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET status='running', verdict='pending', error_message=NULL,
                        executed_at=%s, finished_at=NULL
                    WHERE protocol_id=%s AND status='frozen'
                    """,
                    (executed_at, protocol_id),
                )
                return cursor.rowcount == 1

    def attach_run(self, protocol_id: str, run_id: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET run_id=%s
                    WHERE protocol_id=%s AND status='running'
                    """,
                    (run_id, protocol_id),
                )

    def finish_protocol(
        self,
        *,
        protocol_id: str,
        verdict: str,
        validation_status: str,
        report: dict[str, Any],
        finished_at: str,
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET status='success', verdict=%s, validation_status=%s,
                        report_json=%s, error_message=NULL, finished_at=%s
                    WHERE protocol_id=%s AND status='running'
                    """,
                    (
                        verdict,
                        validation_status,
                        self._json(report),
                        finished_at,
                        protocol_id,
                    ),
                )

    def fail_protocol(self, protocol_id: str, error_message: str, finished_at: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET status='failed', verdict='inconclusive',
                        validation_status='validation_pending', error_message=%s,
                        finished_at=%s
                    WHERE protocol_id=%s AND status IN ('frozen', 'running')
                    """,
                    (error_message[:1000], finished_at, protocol_id),
                )

    def replace_report(
        self,
        *,
        protocol_id: str,
        verdict: str,
        validation_status: str,
        report: dict[str, Any],
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET verdict=%s, validation_status=%s, report_json=%s
                    WHERE protocol_id=%s AND status='success' AND run_id IS NOT NULL
                    """,
                    (verdict, validation_status, self._json(report), protocol_id),
                )

    def supersede_protocol(
        self,
        *,
        protocol_id: str,
        replacement_protocol_id: str,
        reason: str,
    ) -> bool:
        message = f"superseded_by={replacement_protocol_id}; {reason}"[:1000]
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_validation_protocol
                    SET status='superseded', verdict='superseded',
                        validation_status='validation_pending',
                        eligible_for_validation=0, error_message=%s,
                        finished_at=COALESCE(finished_at, NOW())
                    WHERE protocol_id=%s AND status IN ('frozen', 'failed')
                    """,
                    (message, protocol_id),
                )
                return cursor.rowcount == 1

    def load_run_daily(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, pick_count, avg_return_1d_pct,
                           win_rate_1d_pct
                    FROM backtest_summary_daily
                    WHERE run_id=%s
                    ORDER BY trade_date
                    """,
                    (run_id,),
                )
                return cursor.fetchall() or []

    def load_benchmark_rows(
        self,
        *,
        index_code: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, open
                    FROM market_index_daily
                    WHERE index_code=%s
                      AND trade_date >= %s
                      AND trade_date <= DATE_ADD(%s, INTERVAL 15 DAY)
                    ORDER BY trade_date
                    """,
                    (index_code, start_date, end_date),
                )
                return cursor.fetchall() or []
