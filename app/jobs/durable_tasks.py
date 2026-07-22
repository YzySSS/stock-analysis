from __future__ import annotations

import hashlib
import json
import logging
import multiprocessing
import os
import threading
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.jobs.errors import infer_error_code, sanitize_error_message
from app.jobs.mysql_state import MySQLJobStateRepository, MySQLJobTable, StaleRecoveryResult
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


logger = logging.getLogger(__name__)

ConnectionFactory = Callable[..., AbstractContextManager]

INTRADAY_REFRESH_JOB = "intraday_refresh"
SELECTION_INTRADAY_ENRICHMENT_JOB = "selection_intraday_enrichment"
TRACKING_DEEP_REVIEW_JOB = "tracking_deep_review"
SUPPORTED_JOB_TYPES = frozenset(
    {
        INTRADAY_REFRESH_JOB,
        SELECTION_INTRADAY_ENRICHMENT_JOB,
        TRACKING_DEEP_REVIEW_JOB,
    }
)

DEFAULT_STALE_SECONDS = 5 * 60
JOB_HEARTBEAT_SECONDS = 10.0
DEFAULT_INTRADAY_TIMEOUT_SECONDS = 120.0
_RETRYABLE_DATABASE_ERROR_CODES = frozenset({1205, 1213, 2006, 2013})
_RETRYABLE_ERROR_CODES = frozenset({"upstream_timeout", "upstream_connection_error"})
_RETRYABLE_MESSAGE_TOKENS = (
    "temporarily unavailable",
    "service unavailable",
    "gateway timeout",
    "too many requests",
    "rate limit",
    "remote end closed",
    "connection closed",
    "empty response",
    "exited without a result",
)


class _IntradayTaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    code: str = Field(min_length=1, max_length=16)
    trade_date: str | None = Field(default=None, max_length=10)
    refresh: bool


class _DeepReviewTaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    review_job_id: str = Field(min_length=1, max_length=96)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _idempotency_digest(job_type: str, value: str) -> str:
    return hashlib.sha256(f"{job_type}:{value}".encode("utf-8")).hexdigest()


def _intraday_timeout_seconds() -> float:
    raw = os.getenv("DURABLE_INTRADAY_TIMEOUT_SECONDS", str(DEFAULT_INTRADAY_TIMEOUT_SECONDS))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = DEFAULT_INTRADAY_TIMEOUT_SECONDS
    return max(10.0, min(value, 10 * 60.0))


def _is_retryable_task_error(exc: Exception, error_code: str, message: str) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    if error_code in _RETRYABLE_ERROR_CODES:
        return True
    first_arg = exc.args[0] if getattr(exc, "args", ()) else None
    if isinstance(first_arg, int) and first_arg in _RETRYABLE_DATABASE_ERROR_CODES:
        return True
    normalized = str(message or "").lower()
    return any(token in normalized for token in _RETRYABLE_MESSAGE_TOKENS)


def _intraday_subprocess_entry(
    sender: Any,
    *,
    code: str,
    trade_date: str | None,
    refresh: bool,
) -> None:
    """Spawn target: no inherited SQLAlchemy pool or API process state."""

    try:
        from app.data_ingestion.intraday_bar_sync import get_or_fetch_intraday_bars

        output = get_or_fetch_intraday_bars(
            code=code,
            trade_date=trade_date,
            refresh=refresh,
        )
        sender.send(
            {
                "ok": True,
                "result": {
                    "code": output.get("code"),
                    "trade_date": output.get("trade_date"),
                    "source": output.get("source"),
                    "source_status": output.get("source_status"),
                    "count": int(output.get("count") or 0),
                    "saved_rows": int(output.get("saved_rows") or 0),
                },
            }
        )
    except BaseException as exc:
        sender.send(
            {
                "ok": False,
                "error_type": type(exc).__name__,
                "error_message": sanitize_error_message(exc),
            }
        )
    finally:
        sender.close()


def _run_intraday_isolated(
    *,
    code: str,
    trade_date: str | None,
    refresh: bool,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run AkShare work in a spawn child that can be terminated on timeout.

    ``spawn`` prevents the child from inheriting checked-out DB connections or
    the parent's SQLAlchemy pool. A timed-out provider cannot keep writing in a
    forgotten API/worker thread after the durable task has lost ownership.
    """

    timeout_seconds = timeout_seconds or _intraday_timeout_seconds()
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_intraday_subprocess_entry,
        kwargs={
            "sender": sender,
            "code": code,
            "trade_date": trade_date,
            "refresh": refresh,
        },
        name=f"intraday-provider-{code}",
    )
    started = False
    try:
        try:
            process.start()
            started = True
        finally:
            # The parent never sends. Closing this endpoint is also required
            # when spawn itself fails, otherwise the pipe handle leaks.
            sender.close()
        if not receiver.poll(timeout_seconds):
            raise TimeoutError(f"intraday provider exceeded {timeout_seconds:.0f} seconds")
        try:
            message = receiver.recv()
        except EOFError as exc:
            raise RuntimeError("intraday provider process exited without a result") from exc
    finally:
        receiver.close()
        if started:
            process.join(timeout=2.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5.0)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(timeout=2.0)
    if not isinstance(message, dict) or not message.get("ok"):
        error_type = str((message or {}).get("error_type") or "ProviderError")
        error_message = sanitize_error_message((message or {}).get("error_message") or "intraday provider failed")
        raise RuntimeError(f"{error_type}: {error_message}")
    return dict(message.get("result") or {})


@dataclass(frozen=True)
class DurableTaskSpec:
    task_id: str
    job_type: str
    related_entity_id: str | None
    idempotency_key: str
    payload: dict[str, Any]
    max_attempts: int = 3


def build_task_spec(
    job_type: str,
    payload: dict[str, Any],
    *,
    related_entity_id: str | None = None,
    idempotency_value: str | None = None,
    max_attempts: int = 3,
) -> DurableTaskSpec:
    if job_type not in SUPPORTED_JOB_TYPES:
        raise ValueError(f"unsupported durable job type: {job_type}")
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    task_id = f"task_{uuid.uuid4().hex}"
    identity = idempotency_value or task_id
    return DurableTaskSpec(
        task_id=task_id,
        job_type=job_type,
        related_entity_id=str(related_entity_id)[:96] if related_entity_id else None,
        idempotency_key=_idempotency_digest(job_type, str(identity)),
        payload=dict(payload),
        max_attempts=max_attempts,
    )


class DurableTaskRepository:
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def insert_with_cursor(cursor: Any, spec: DurableTaskSpec) -> str:
        cursor.execute(
            """
            INSERT INTO durable_task (
                task_id, job_type, related_entity_id, idempotency_key,
                active_idempotency_key, payload_json, status, max_attempts, phase
            ) VALUES (%s, %s, %s, %s, %s, %s, 'queued', %s, '等待执行')
            ON DUPLICATE KEY UPDATE updated_at=updated_at
            """,
            (
                spec.task_id,
                spec.job_type,
                spec.related_entity_id,
                spec.idempotency_key,
                spec.idempotency_key,
                json.dumps(spec.payload, ensure_ascii=False, default=str),
                spec.max_attempts,
            ),
        )
        cursor.execute(
            """
            SELECT task_id
            FROM durable_task
            WHERE active_idempotency_key=%s
            LIMIT 1
            """,
            (spec.idempotency_key,),
        )
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("durable task insert did not produce an active queue row")
        if isinstance(row, dict):
            return str(row["task_id"])
        return str(row[0])

    def enqueue_spec(self, spec: DurableTaskSpec) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                persisted_task_id = self.insert_with_cursor(cursor, spec)
        return {
            "task_id": persisted_task_id,
            "job_type": spec.job_type,
            "status": "queued",
            "deduplicated": persisted_task_id != spec.task_id,
        }

    def get_claimed(self, task_id: str, worker_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT task_id, job_type, related_entity_id, payload_json,
                           attempt_count, max_attempts
                    FROM durable_task
                    WHERE task_id=%s AND status='running' AND worker_id=%s
                    LIMIT 1
                    """,
                    (task_id, worker_id),
                )
                row = cursor.fetchone()
        if not row:
            return None
        normalized = dict(row)
        normalized["payload"] = _json_object(normalized.pop("payload_json", None))
        return normalized

    def finish_success(self, task_id: str, worker_id: str, result: dict[str, Any]) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE durable_task
                    SET status='success', result_json=%s, finished_at=NOW(),
                        worker_heartbeat_at=NOW(), phase='执行完成', progress_pct=100,
                        estimated_seconds_left=0, active_idempotency_key=NULL,
                        error_code=NULL, error_message=NULL
                    WHERE task_id=%s AND status='running' AND worker_id=%s
                    """,
                    (json.dumps(result, ensure_ascii=False, default=str), task_id, worker_id),
                )
                return cursor.rowcount == 1

    def finish_failed(
        self,
        task_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE durable_task
                    SET status='failed', finished_at=NOW(), worker_heartbeat_at=NOW(),
                        phase='执行失败', estimated_seconds_left=0,
                        active_idempotency_key=NULL, error_code=%s, error_message=%s
                    WHERE task_id=%s AND status='running' AND worker_id=%s
                    """,
                    (error_code[:64], error_message[:500], task_id, worker_id),
                )
                return cursor.rowcount == 1

    def requeue_retryable(
        self,
        task_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
    ) -> bool:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE durable_task
                    SET status='queued', worker_id=NULL, locked_at=NULL,
                        worker_heartbeat_at=NULL, started_at=NULL, finished_at=NULL,
                        phase='瞬态故障，等待重试', progress_pct=0,
                        estimated_seconds_left=NULL, error_code=%s, error_message=%s
                    WHERE task_id=%s AND status='running' AND worker_id=%s
                      AND cancel_requested=0 AND attempt_count < max_attempts
                    """,
                    ("transient_retry", error_message[:500], task_id, worker_id),
                )
                return cursor.rowcount == 1

    def reconcile_tracking_review_states(self) -> None:
        """Keep the deep-review projection aligned with stale queue recovery."""

        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ai_advice_snapshot advice
                    INNER JOIN durable_task task
                      ON task.related_entity_id=advice.advice_id
                     AND task.job_type='tracking_deep_review'
                    SET advice.status='queued', advice.completed_at=NULL,
                        advice.error_code=NULL, advice.error_message=NULL
                    WHERE task.status='queued'
                      AND task.error_code IN ('stale_worker_recovered', 'transient_retry')
                      AND advice.status<>'success'
                    """
                )
                cursor.execute(
                    """
                    UPDATE ai_advice_snapshot advice
                    INNER JOIN durable_task task
                      ON task.related_entity_id=advice.advice_id
                     AND task.job_type='tracking_deep_review'
                    SET advice.status='failed', advice.completed_at=NOW(),
                        advice.error_code='stale_retry_exhausted',
                        advice.error_message='durable worker heartbeat stale and max attempts exhausted'
                    WHERE task.status='failed'
                      AND task.error_code='stale_retry_exhausted'
                      AND advice.status NOT IN ('success', 'failed')
                    """
                )


class DurableTaskService:
    def __init__(
        self,
        repository: DurableTaskRepository | None = None,
        job_states: MySQLJobStateRepository | None = None,
        task_logger: TaskRunLogger | None = None,
    ) -> None:
        self.repository = repository or DurableTaskRepository()
        self.job_states = job_states or MySQLJobStateRepository(
            MySQLJobTable(
                table="durable_task",
                id_column="task_id",
                heartbeat_column="worker_heartbeat_at",
                phase_column="phase",
                active_idempotency_column="active_idempotency_key",
            )
        )
        self.task_logger = task_logger or TaskRunLogger()

    def enqueue_intraday_refresh(self, code: str, trade_date: str | None) -> dict[str, Any]:
        return self.repository.enqueue_spec(
            build_task_spec(
                INTRADAY_REFRESH_JOB,
                {"schema_version": 1, "code": code, "trade_date": trade_date, "refresh": True},
                related_entity_id=code,
                idempotency_value=f"{code}:{trade_date or 'latest'}",
            )
        )

    def enqueue_selection_enrichment(self, code: str, trade_date: str) -> dict[str, Any]:
        return self.repository.enqueue_spec(
            build_task_spec(
                SELECTION_INTRADAY_ENRICHMENT_JOB,
                {"schema_version": 1, "code": code, "trade_date": trade_date, "refresh": False},
                related_entity_id=code,
                idempotency_value=f"{code}:{trade_date}",
            )
        )

    def claim_next(self, worker_id: str) -> str | None:
        return self.job_states.claim_next(worker_id, running_phase="任务执行中")

    def recover_stale(self, stale_seconds: int = DEFAULT_STALE_SECONDS) -> StaleRecoveryResult:
        result = self.job_states.recover_stale(stale_seconds)
        self.repository.reconcile_tracking_review_states()
        return result

    def run_claimed(self, task_id: str, worker_id: str) -> str:
        if not self.job_states.owns_running_job(task_id, worker_id):
            raise RuntimeError("worker does not own this running durable task")
        task = self.repository.get_claimed(task_id, worker_id)
        if task is None:
            raise RuntimeError("claimed durable task payload is unavailable")

        job_type = str(task.get("job_type") or "")
        self._log_start(task_id, job_type, task.get("related_entity_id"))
        stop_event = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(task_id, worker_id, stop_event),
            name=f"durable-task-heartbeat-{task_id}",
            daemon=True,
        )
        heartbeat.start()
        try:
            result = self._dispatch(
                job_type,
                task.get("payload") or {},
                task_id=task_id,
                worker_id=worker_id,
            )
            if not self.repository.finish_success(task_id, worker_id, result):
                raise RuntimeError("durable task lost worker ownership before success persistence")
            self._log_finish(task_id, job_type, "success", metadata=result)
            return "success"
        except Exception as exc:
            safe_message = sanitize_error_message(exc)
            error_code = infer_error_code(safe_message, default=type(exc).__name__)
            attempt_count = int(task.get("attempt_count") or 1)
            max_attempts = int(task.get("max_attempts") or 1)
            if (
                attempt_count < max_attempts
                and _is_retryable_task_error(exc, error_code, safe_message)
            ):
                persisted = self.repository.requeue_retryable(
                    task_id,
                    worker_id,
                    error_code,
                    safe_message,
                )
                if persisted:
                    self.repository.reconcile_tracking_review_states()
                    self._log_finish(
                        task_id,
                        job_type,
                        "retrying",
                        message=safe_message,
                        metadata={
                            "attempt_count": attempt_count,
                            "max_attempts": max_attempts,
                            "original_error_code": error_code,
                        },
                        error_code="transient_retry",
                    )
                    return "requeued"
            persisted = self.repository.finish_failed(task_id, worker_id, error_code, safe_message)
            if persisted:
                self._log_finish(task_id, job_type, "failed", message=safe_message, error_code=error_code)
            else:
                self._log_finish(
                    task_id,
                    job_type,
                    "lost_ownership",
                    message="worker result discarded after ownership changed",
                )
            raise
        finally:
            stop_event.set()
            heartbeat.join(timeout=2.0)

    def _dispatch(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        task_id: str,
        worker_id: str,
    ) -> dict[str, Any]:
        try:
            if job_type in {INTRADAY_REFRESH_JOB, SELECTION_INTRADAY_ENRICHMENT_JOB}:
                validated: BaseModel = _IntradayTaskPayload.model_validate(payload)
            elif job_type == TRACKING_DEEP_REVIEW_JOB:
                validated = _DeepReviewTaskPayload.model_validate(payload)
            else:
                raise ValueError(f"unsupported durable job type: {job_type}")
        except ValidationError as exc:
            raise ValueError(f"invalid durable task payload: {exc}") from exc

        if job_type in {INTRADAY_REFRESH_JOB, SELECTION_INTRADAY_ENRICHMENT_JOB}:
            return _run_intraday_isolated(
                code=str(validated.code),
                trade_date=str(validated.trade_date) if validated.trade_date else None,
                refresh=bool(validated.refresh),
            )
        if job_type == TRACKING_DEEP_REVIEW_JOB:
            from app.tracking.deep_review import DeepReviewJobService, call_deepseek_review

            review_job_id = str(validated.review_job_id)
            DeepReviewJobService().execute_persisted_job(
                review_job_id=review_job_id,
                review_callable=call_deepseek_review,
                raise_on_failure=True,
                ownership_check=lambda: self.job_states.owns_running_job(task_id, worker_id),
            )
            return {"review_job_id": review_job_id}
        raise ValueError(f"unsupported durable job type: {job_type}")

    def _heartbeat_loop(self, task_id: str, worker_id: str, stop_event: threading.Event) -> None:
        while not stop_event.wait(JOB_HEARTBEAT_SECONDS):
            if not self.job_states.heartbeat(task_id, worker_id):
                logger.warning("durable task heartbeat lost ownership task_id=%s", task_id)
                return

    def _log_start(self, task_id: str, job_type: str, related_entity_id: Any) -> None:
        try:
            self.task_logger.start(
                f"durable_{job_type}"[:128],
                task_id,
                {"related_entity_id": related_entity_id},
            )
        except Exception:
            logger.warning("failed to start durable task audit log task_id=%s", task_id, exc_info=True)

    def _log_finish(
        self,
        task_id: str,
        job_type: str,
        status: str,
        *,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
        error_code: str | None = None,
    ) -> None:
        try:
            self.task_logger.finish(
                f"durable_{job_type}"[:128],
                task_id,
                status,
                message=message,
                metadata=metadata,
                error_code=error_code,
            )
        except Exception:
            logger.warning("failed to finish durable task audit log task_id=%s", task_id, exc_info=True)
