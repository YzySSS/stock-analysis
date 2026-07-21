from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


class PortfolioRepository:
    """Persistence boundary for portfolio positions, advice runs and outcomes.

    Product rules stay in PortfolioService. This class owns SQL, short
    transactions and fixed-size batch loading for portfolio list pages.
    """

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _placeholders(items: list[Any]) -> str:
        if not items:
            raise ValueError("at least one item is required")
        return ", ".join(["%s"] * len(items))

    def list_positions(self, include_inactive: bool = False) -> list[dict[str, Any]]:
        where = "" if include_inactive else "WHERE p.is_active = 1"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT p.*
                    FROM portfolio_position p
                    {where}
                    ORDER BY p.is_active DESC, p.updated_at DESC, p.id DESC
                    """
                )
                return cursor.fetchall() or []

    def get_position(self, position_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM portfolio_position WHERE id = %s LIMIT 1",
                    (position_id,),
                )
                return cursor.fetchone()

    def create_position(
        self,
        *,
        code: str,
        strategy_id: str,
        cost_price: float,
        quantity: int,
        buy_datetime: Any,
        target_style: str,
        max_loss_pct: float | None,
        note: str | None,
    ) -> tuple[int | None, dict[str, Any] | None]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT code, name, industry, instrument_type, is_st, is_delisted
                    FROM stock_basic
                    WHERE code = %s
                    LIMIT 1
                    """,
                    (code,),
                )
                stock = cursor.fetchone()
                if not stock:
                    return None, None
                cursor.execute(
                    """
                    INSERT INTO portfolio_position
                        (code, name, strategy_id, cost_price, quantity, buy_datetime,
                         target_style, max_loss_pct, note)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        code,
                        stock.get("name"),
                        strategy_id,
                        cost_price,
                        quantity,
                        buy_datetime,
                        target_style,
                        max_loss_pct,
                        note,
                    ),
                )
                return int(cursor.lastrowid), stock

    def update_position(self, position_id: int, updates: dict[str, Any]) -> bool:
        if not updates:
            return False
        sets = ", ".join(f"{key} = %s" for key in updates)
        params = [*updates.values(), position_id]
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE portfolio_position SET {sets} WHERE id = %s",
                    params,
                )
                return cursor.rowcount == 1

    def deactivate_position(self, position_id: int) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE portfolio_position SET is_active = 0 WHERE id = %s",
                    (position_id,),
                )
                return cursor.rowcount == 1

    def load_market_contexts(
        self,
        codes: list[str],
        *,
        history_limit: int = 120,
    ) -> dict[str, dict[str, Any]]:
        final_codes = list(dict.fromkeys(str(code) for code in codes if code))
        if not final_codes:
            return {}
        placeholders = self._placeholders(final_codes)
        contexts = {
            code: {
                "basic": {},
                "quote": {},
                "history": [],
                "sentiment": {},
                "moneyflow": {},
                "chip": {},
            }
            for code in final_codes
        }

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT code, name, industry, instrument_type, is_st, is_delisted
                    FROM stock_basic
                    WHERE code IN ({placeholders})
                    """,
                    final_codes,
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["basic"] = row

                cursor.execute(
                    f"""
                    SELECT code, latest_price, pct_chg, change_amount, pre_close,
                           high_price, low_price, amount, trade_date, quote_time,
                           updated_at, source
                    FROM stock_realtime_snapshot
                    WHERE code IN ({placeholders})
                    """,
                    final_codes,
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["quote"] = row

                cursor.execute(
                    f"""
                    SELECT code, trade_date, open, high, low, close, volume, amount
                    FROM (
                        SELECT code, trade_date, open, high, low, close, volume, amount,
                               ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS row_no
                        FROM daily_kline
                        WHERE code IN ({placeholders})
                    ) ranked
                    WHERE row_no <= %s
                    ORDER BY code, trade_date
                    """,
                    [*final_codes, max(1, int(history_limit))],
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["history"].append(row)

                cursor.execute(
                    f"""
                    SELECT s.code, s.trade_date, s.sentiment_score, s.news_count,
                           s.filtered_news_count, s.credibility_avg, s.quality_avg
                    FROM stock_sentiment_daily s
                    INNER JOIN (
                        SELECT code, MAX(trade_date) AS trade_date
                        FROM stock_sentiment_daily
                        WHERE code IN ({placeholders})
                        GROUP BY code
                    ) latest ON latest.code=s.code AND latest.trade_date=s.trade_date
                    """,
                    final_codes,
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["sentiment"] = row

                cursor.execute(
                    f"""
                    SELECT code, trade_date, quote_time, net_amount, amount,
                           pct_chg, turnover_rate
                    FROM stock_realtime_moneyflow_snapshot
                    WHERE code IN ({placeholders})
                      AND trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)
                      AND quote_time >= DATE_SUB(
                          (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot),
                          INTERVAL 20 MINUTE
                      )
                    """,
                    final_codes,
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["moneyflow"] = row

                cursor.execute(
                    f"""
                    SELECT c.code, c.trade_date, c.his_low, c.his_high, c.cost_5pct,
                           c.cost_15pct, c.cost_50pct, c.cost_85pct, c.cost_95pct,
                           c.weight_avg, c.winner_rate
                    FROM stock_chip_daily c
                    INNER JOIN (
                        SELECT code, MAX(trade_date) AS trade_date
                        FROM stock_chip_daily
                        WHERE code IN ({placeholders})
                        GROUP BY code
                    ) latest ON latest.code=c.code AND latest.trade_date=c.trade_date
                    """,
                    final_codes,
                )
                for row in cursor.fetchall() or []:
                    code = str(row["code"])
                    if code in contexts:
                        contexts[code]["chip"] = row
        return contexts

    def create_advice_run(
        self,
        *,
        position_id: int,
        code: str,
        idempotency_key: str,
        active_idempotency_key: str,
        max_attempts: int,
        estimated_seconds_left: int,
        decision_level: str,
        prompt_version: str,
        input_snapshot_json: str,
    ) -> int:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO portfolio_advice_run (
                        position_id, code, status, idempotency_key, active_idempotency_key,
                        cancel_requested, attempt_count, max_attempts, phase, progress_pct,
                        estimated_seconds_left, decision_level, prompt_version, input_snapshot_json
                    ) VALUES (
                        %s, %s, 'queued', %s, %s,
                        0, 0, %s, '任务已提交', 0,
                        %s, %s, %s, %s
                    )
                    """,
                    (
                        position_id,
                        code,
                        idempotency_key,
                        active_idempotency_key,
                        max_attempts,
                        estimated_seconds_left,
                        decision_level,
                        prompt_version,
                        input_snapshot_json,
                    ),
                )
                return int(cursor.lastrowid)

    def get_advice_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM portfolio_advice_run WHERE id = %s LIMIT 1",
                    (run_id,),
                )
                return cursor.fetchone()

    def get_active_advice_run(self, active_idempotency_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM portfolio_advice_run
                    WHERE active_idempotency_key=%s
                      AND status IN ('queued', 'running')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (active_idempotency_key,),
                )
                return cursor.fetchone()

    def mark_advice_execution_stage(self, run_id: str, worker_id: str) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET phase='AI 建议生成中', progress_pct=10, worker_heartbeat_at=NOW()
                    WHERE id=%s AND status='running' AND worker_id=%s
                    """,
                    (run_id, worker_id),
                )
                return cursor.rowcount == 1

    def finish_advice_success(
        self,
        *,
        run_id: str,
        worker_id: str,
        decision_level: str,
        model_name: str,
        prompt_version: str,
        input_snapshot_json: str,
        raw_response: str,
        parsed_review_json: str,
        expires_at: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET status='succeeded', phase='运行完成', progress_pct=100,
                        estimated_seconds_left=0, decision_level=%s, model_name=%s,
                        prompt_version=%s, input_snapshot_json=%s, raw_response=%s,
                        parsed_review_json=%s, error_code=NULL, error_message=NULL,
                        expires_at=%s, finished_at=NOW(), worker_heartbeat_at=NOW(),
                        active_idempotency_key=NULL
                    WHERE id=%s AND status='running' AND worker_id=%s AND cancel_requested=0
                    """,
                    (
                        decision_level,
                        model_name,
                        prompt_version,
                        input_snapshot_json,
                        raw_response,
                        parsed_review_json,
                        expires_at,
                        run_id,
                        worker_id,
                    ),
                )
                return cursor.rowcount == 1

    def finish_advice_failed(
        self,
        run_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET status='failed', phase='运行失败', estimated_seconds_left=0,
                        error_code=%s, error_message=%s, finished_at=NOW(),
                        worker_heartbeat_at=NOW(), active_idempotency_key=NULL
                    WHERE id=%s AND status='running' AND worker_id=%s
                    """,
                    (error_code, error_message[:500], run_id, worker_id),
                )
                return cursor.rowcount == 1

    def list_successful_advice_runs(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM portfolio_advice_run
                    WHERE status='succeeded' AND finished_at IS NOT NULL
                    ORDER BY finished_at DESC, id DESC
                    LIMIT %s
                    """,
                    (max(1, int(limit)),),
                )
                return cursor.fetchall() or []

    def existing_outcome_horizons(self, advice_run_id: int) -> set[int]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT horizon_days
                    FROM portfolio_advice_outcome
                    WHERE advice_run_id=%s
                    """,
                    (advice_run_id,),
                )
                return {int(row["horizon_days"]) for row in (cursor.fetchall() or [])}

    def future_kline_rows(
        self,
        code: str,
        base_trade_date: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, open, high, low, close
                    FROM daily_kline
                    WHERE code=%s AND trade_date>%s
                    ORDER BY trade_date ASC
                    LIMIT %s
                    """,
                    (code, base_trade_date, int(limit)),
                )
                return cursor.fetchall() or []

    def upsert_advice_outcome(self, outcome: dict[str, Any]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO portfolio_advice_outcome
                        (advice_run_id, position_id, code, decision_level, base_price,
                         base_trade_date, evaluate_at, horizon_days, latest_price,
                         return_pct, max_gain_pct, max_drawdown_pct, stop_loss_touched,
                         take_profit_touched, support_broken, resistance_broken,
                         outcome_label, quality_score, evidence_json)
                    VALUES
                        (%(advice_run_id)s, %(position_id)s, %(code)s, %(decision_level)s,
                         %(base_price)s, %(base_trade_date)s, %(evaluate_at)s,
                         %(horizon_days)s, %(latest_price)s, %(return_pct)s,
                         %(max_gain_pct)s, %(max_drawdown_pct)s, %(stop_loss_touched)s,
                         %(take_profit_touched)s, %(support_broken)s,
                         %(resistance_broken)s, %(outcome_label)s, %(quality_score)s,
                         %(evidence_json)s)
                    ON DUPLICATE KEY UPDATE
                        decision_level=VALUES(decision_level),
                        base_price=VALUES(base_price),
                        base_trade_date=VALUES(base_trade_date),
                        evaluate_at=VALUES(evaluate_at),
                        latest_price=VALUES(latest_price),
                        return_pct=VALUES(return_pct),
                        max_gain_pct=VALUES(max_gain_pct),
                        max_drawdown_pct=VALUES(max_drawdown_pct),
                        stop_loss_touched=VALUES(stop_loss_touched),
                        take_profit_touched=VALUES(take_profit_touched),
                        support_broken=VALUES(support_broken),
                        resistance_broken=VALUES(resistance_broken),
                        outcome_label=VALUES(outcome_label),
                        quality_score=VALUES(quality_score),
                        evidence_json=VALUES(evidence_json)
                    """,
                    outcome,
                )

    def latest_valid_advice(self, position_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM portfolio_advice_run
                    WHERE position_id=%s
                      AND status='succeeded'
                      AND expires_at IS NOT NULL
                      AND expires_at>NOW()
                    ORDER BY expires_at DESC, id DESC
                    LIMIT 1
                    """,
                    (position_id,),
                )
                return cursor.fetchone()

    def expire_advice_run(self, run_id: int, reason: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET expires_at=LEAST(COALESCE(expires_at, NOW()), NOW()),
                        error_message=%s
                    WHERE id=%s
                    """,
                    (reason[:500], run_id),
                )

    def latest_advice_runs(self, position_ids: list[int]) -> dict[int, dict[str, Any]]:
        final_ids = list(dict.fromkeys(int(value) for value in position_ids))
        if not final_ids:
            return {}
        placeholders = self._placeholders(final_ids)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT r.*
                    FROM portfolio_advice_run r
                    INNER JOIN (
                        SELECT position_id, MAX(id) AS id
                        FROM portfolio_advice_run
                        WHERE position_id IN ({placeholders})
                        GROUP BY position_id
                    ) latest ON latest.id=r.id
                    """,
                    final_ids,
                )
                return {
                    int(row["position_id"]): row
                    for row in (cursor.fetchall() or [])
                }

    def advice_outcomes(self, run_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        final_ids = list(dict.fromkeys(int(value) for value in run_ids))
        if not final_ids:
            return {}
        placeholders = self._placeholders(final_ids)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT *
                    FROM portfolio_advice_outcome
                    WHERE advice_run_id IN ({placeholders})
                    ORDER BY advice_run_id, horizon_days
                    """,
                    final_ids,
                )
                rows = cursor.fetchall() or []
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(int(row["advice_run_id"]), []).append(row)
        return grouped

    def invalidate_advice(self, position_id: int, reason: str) -> list[int]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET expires_at=LEAST(COALESCE(expires_at, NOW()), NOW()),
                        error_message=COALESCE(error_message, %s)
                    WHERE position_id=%s
                      AND status='succeeded'
                      AND (expires_at IS NULL OR expires_at>NOW())
                    """,
                    (reason[:500], position_id),
                )
                cursor.execute(
                    """
                    SELECT id
                    FROM portfolio_advice_run
                    WHERE position_id=%s AND status IN ('queued', 'running')
                    """,
                    (position_id,),
                )
                return [int(row["id"]) for row in (cursor.fetchall() or [])]
