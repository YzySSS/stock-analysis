from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pymysql.err import IntegrityError

from app.jobs.errors import record_job_error
from app.jobs.mysql_state import MySQLJobStateRepository, MySQLJobTable, StaleRecoveryResult
from app.shared.db import mysql_conn
from app.shared.instrument_policy import SUPPORTED_SELECTION_INSTRUMENT_TYPES, require_supported_instrument
from app.strategies.service import StrategyService


logger = logging.getLogger(__name__)


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True, separators=(",", ":"))


def _from_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _dt(value: Any) -> str | None:
    return str(value) if value else None


class SelectionTaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_id: str = Field(min_length=1, max_length=64)
    instrument_type: str = "stock"
    market_board: str | None = None
    limit: int = Field(default=3, ge=1, le=200)
    max_picks: int = Field(default=3, ge=1, le=200)
    score_threshold: float | None = Field(default=None, ge=0, le=100)
    save: bool = False


class SelectionRunService:
    DEFAULT_ESTIMATE_SECONDS = 110
    FAST_ESTIMATE_SECONDS = 25
    MAX_ESTIMATE_SECONDS = 300
    HEARTBEAT_SECONDS = 3.0
    STALE_RUNNING_SECONDS = 15 * 60
    DEFAULT_MAX_ATTEMPTS = 2

    def __init__(self, job_states: MySQLJobStateRepository | None = None) -> None:
        self.job_states = job_states or MySQLJobStateRepository(
            MySQLJobTable(table="selection_run")
        )

    def submit(self, request: Dict[str, Any]) -> Dict[str, Any]:
        instrument_type = require_supported_instrument(
            request.get("instrument_type") or "stock",
            operation="selection",
            supported=SUPPORTED_SELECTION_INSTRUMENT_TYPES,
        )
        if request.get("save"):
            raise ValueError("选股任务只生成预览结果；请在结果页按条保存到跟踪复盘")

        strategy_service = StrategyService()
        strategy_id = request.get("strategy_id") or strategy_service.get_default_strategy_id()
        strategy_service.require_runtime_ready(strategy_id, instrument_type=instrument_type)
        effective_limit = request.get("max_picks") if request.get("max_picks") is not None else request.get("limit")
        request_payload = SelectionTaskPayload(
            **{
                **request,
                "strategy_id": strategy_id,
                "instrument_type": instrument_type,
                "limit": effective_limit,
                "max_picks": effective_limit,
                "save": False,
            }
        ).model_dump()
        idempotency_date = self._latest_data_trade_date()
        idempotency_key = self._idempotency_key(request_payload, idempotency_date)
        existing = self._get_active_by_idempotency(idempotency_key)
        if existing:
            payload = self._normalize_row(existing, include_result=False)
            payload["deduplicated"] = True
            return payload

        run_id = f"selection_task_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        estimate = self._estimate_seconds(strategy_id=strategy_id, instrument_type=instrument_type)
        try:
            with mysql_conn(dict_cursor=False) as conn:
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
                            request_payload.get("market_board"),
                            effective_limit,
                            request_payload.get("score_threshold"),
                            idempotency_key,
                            idempotency_key,
                            idempotency_date,
                            self.DEFAULT_MAX_ATTEMPTS,
                            estimate,
                            _to_json(request_payload),
                        ),
                    )
        except IntegrityError as exc:
            if exc.args and int(exc.args[0]) == 1062:
                existing = self._get_active_by_idempotency(idempotency_key)
                if existing:
                    payload = self._normalize_row(existing, include_result=False)
                    payload["deduplicated"] = True
                    return payload
            raise
        return self.get_run(run_id, include_result=False)

    def get_run(self, run_id: str, include_result: bool = True) -> Dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM selection_run WHERE run_id=%s", (run_id,))
                row = cursor.fetchone()
        if not row:
            raise ValueError("selection run not found")
        return self._normalize_row(row, include_result=include_result)

    def list_runs(self, limit: int = 20) -> list[Dict[str, Any]]:
        with mysql_conn() as conn:
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
                rows = cursor.fetchall() or []
        return [self._normalize_row(row, include_result=False) for row in rows]

    def claim_next_queued_run(self, worker_id: str) -> str | None:
        return self.job_states.claim_next(worker_id, running_phase="策略计算中")

    def recover_stale_running_runs(self, stale_seconds: int | None = None) -> StaleRecoveryResult:
        result = self.job_states.recover_stale(stale_seconds or self.STALE_RUNNING_SECONDS)
        if result.failed:
            record_job_error(
                "selection",
                "selection",
                "stale_retry_exhausted",
                "selection worker heartbeat stale and max attempts exhausted",
                count=result.failed,
            )
        return result

    def request_cancel(self, run_id: str) -> Dict[str, Any]:
        status = self.job_states.request_cancel(run_id)
        if status is None:
            raise ValueError("selection run not found")
        return self.get_run(run_id, include_result=False)

    def run_claimed(self, run_id: str, worker_id: str) -> None:
        stop_event = threading.Event()
        cancel_seen = threading.Event()
        heartbeat: threading.Thread | None = None
        try:
            if not self.job_states.owns_running_job(run_id, worker_id):
                raise RuntimeError("worker does not own this running selection task")
            run = self.get_run(run_id, include_result=False)
            request = SelectionTaskPayload.model_validate(run.get("request") or {})
            if self.job_states.finish_cancelled_if_requested(run_id, worker_id):
                return

            self._mark_execution_stage(run_id, worker_id)
            heartbeat = threading.Thread(
                target=self._worker_heartbeat,
                args=(run_id, worker_id, stop_event, cancel_seen),
                daemon=True,
            )
            heartbeat.start()
            result = StrategyService().run_strategy(
                strategy_id=request.strategy_id,
                limit=request.max_picks,
                instrument_type=request.instrument_type,
                market_board=request.market_board,
                save=False,
                score_threshold=request.score_threshold,
                run_id=run_id,
            )
            stop_event.set()
            heartbeat.join(timeout=1)
            if cancel_seen.is_set():
                self.job_states.finish_cancelled_if_requested(run_id, worker_id)
                return
            if self.job_states.finish_cancelled_if_requested(run_id, worker_id):
                return
            self._finish_success(run_id, worker_id, result)
        except Exception as exc:
            stop_event.set()
            if heartbeat:
                heartbeat.join(timeout=1)
            if self.job_states.finish_cancelled_if_requested(run_id, worker_id):
                return
            logger.exception("selection run failed run_id=%s", run_id)
            error_code = "invalid_request" if isinstance(exc, ValidationError) else "selection_failed"
            self._finish_failed(run_id, worker_id, error_code, str(exc))

    def _latest_data_trade_date(self) -> date:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
                row = cursor.fetchone() or {}
        value = row.get("trade_date")
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if value:
            return date.fromisoformat(str(value)[:10])
        return datetime.now().date()

    @staticmethod
    def _idempotency_key(request: Dict[str, Any], trade_date: date) -> str:
        material = _to_json(
            {
                "job_type": "selection",
                "trade_date": trade_date.isoformat(),
                "request": request,
            }
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def _get_active_by_idempotency(self, idempotency_key: str) -> Dict[str, Any] | None:
        with mysql_conn() as conn:
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

    def _estimate_seconds(self, strategy_id: str, instrument_type: str) -> int:
        with mysql_conn() as conn:
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
                    (strategy_id, instrument_type, self.MAX_ESTIMATE_SECONDS),
                )
                row = cursor.fetchone() or {}
        avg_seconds = _to_float(row.get("avg_seconds"))
        if avg_seconds:
            return int(max(8, min(self.MAX_ESTIMATE_SECONDS, avg_seconds * 1.25)))
        return self.DEFAULT_ESTIMATE_SECONDS if strategy_id == "a_share_sentiment" else self.FAST_ESTIMATE_SECONDS

    def _worker_heartbeat(
        self,
        run_id: str,
        worker_id: str,
        stop_event: threading.Event,
        cancel_seen: threading.Event,
    ) -> None:
        while not stop_event.wait(self.HEARTBEAT_SECONDS):
            if not self.job_states.heartbeat(run_id, worker_id):
                logger.warning("selection heartbeat lost ownership run_id=%s worker_id=%s", run_id, worker_id)
                return
            if self.job_states.is_cancel_requested(run_id):
                cancel_seen.set()

    def _mark_execution_stage(self, run_id: str, worker_id: str) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_run
                    SET phase='策略计算中', progress_pct=10, worker_heartbeat_at=NOW()
                    WHERE run_id=%s AND status='running' AND worker_id=%s
                    """,
                    (run_id, worker_id),
                )

    def _finish_success(self, run_id: str, worker_id: str, result: Dict[str, Any]) -> None:
        result_count = int(result.get("count") or len(result.get("results") or []))
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE selection_run
                    SET status='success', phase='运行完成', progress_pct=100, estimated_seconds_left=0,
                        result_count=%s, result_json=%s, error_code=NULL, error_message=NULL,
                        finished_at=NOW(), worker_heartbeat_at=NOW(), active_idempotency_key=NULL
                    WHERE run_id=%s AND status='running' AND worker_id=%s AND cancel_requested=0
                    """,
                    (result_count, _to_json(result), run_id, worker_id),
                )
                if cursor.rowcount != 1:
                    logger.warning("selection success ignored after ownership/status change run_id=%s", run_id)

    def _finish_failed(self, run_id: str, worker_id: str, error_code: str, error_message: str) -> None:
        updated = 0
        with mysql_conn(dict_cursor=False) as conn:
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
                updated = cursor.rowcount
        if updated:
            record_job_error("selection", "selection", error_code, error_message)

    def _normalize_row(self, row: Dict[str, Any], include_result: bool = True) -> Dict[str, Any]:
        request = _from_json(row.get("request_json")) or {}
        result = _from_json(row.get("result_json")) if include_result else None
        elapsed_seconds = None
        started_at = row.get("started_at")
        finished_at = row.get("finished_at")
        if started_at:
            end = finished_at or datetime.now()
            try:
                elapsed_seconds = max(0, int((end - started_at).total_seconds()))
            except TypeError:
                elapsed_seconds = None
        payload = {
            "run_id": row.get("run_id"),
            "strategy_id": row.get("strategy_id"),
            "instrument_type": row.get("instrument_type"),
            "market_board": row.get("market_board"),
            "max_picks": row.get("max_picks"),
            "score_threshold": _to_float(row.get("score_threshold")),
            "save_requested": bool(row.get("save_requested")),
            "status": row.get("status"),
            "phase": row.get("phase"),
            "progress_pct": _to_float(row.get("progress_pct")) or 0,
            "estimated_seconds_left": int(row.get("estimated_seconds_left")) if row.get("estimated_seconds_left") is not None else None,
            "elapsed_seconds": elapsed_seconds,
            "result_count": int(row.get("result_count") or 0),
            "request": request,
            "idempotency_date": _dt(row.get("idempotency_date")),
            "worker_id": row.get("worker_id"),
            "locked_at": _dt(row.get("locked_at")),
            "worker_heartbeat_at": _dt(row.get("worker_heartbeat_at")),
            "cancel_requested": bool(row.get("cancel_requested")),
            "attempt_count": int(row.get("attempt_count") or 0),
            "max_attempts": int(row.get("max_attempts") or self.DEFAULT_MAX_ATTEMPTS),
            "error_code": row.get("error_code"),
            "error_message": row.get("error_message"),
            "started_at": _dt(started_at),
            "finished_at": _dt(finished_at),
            "created_at": _dt(row.get("created_at")),
            "updated_at": _dt(row.get("updated_at")),
        }
        if include_result:
            payload["result"] = result
        return payload
