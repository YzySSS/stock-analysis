from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from app.shared.db import mysql_conn


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


@dataclass(frozen=True)
class MySQLJobTable:
    """Column mapping for a table that follows the shared job lifecycle contract."""

    table: str
    id_column: str = "run_id"
    heartbeat_column: str = "worker_heartbeat_at"
    phase_column: str = "phase"
    active_idempotency_column: str | None = "active_idempotency_key"

    def __post_init__(self) -> None:
        _safe_identifier(self.table)
        _safe_identifier(self.id_column)
        _safe_identifier(self.heartbeat_column)
        _safe_identifier(self.phase_column)
        if self.active_idempotency_column:
            _safe_identifier(self.active_idempotency_column)


@dataclass(frozen=True)
class StaleRecoveryResult:
    requeued: int = 0
    failed: int = 0
    cancelled: int = 0

    @property
    def total(self) -> int:
        return self.requeued + self.failed + self.cancelled


class MySQLJobStateRepository:
    """Short-transaction state operations shared by MySQL-backed workers.

    The repository deliberately owns only lifecycle transitions. Payload parsing,
    progress meaning, result persistence, and product-specific error semantics stay
    in the calling service.
    """

    def __init__(self, config: MySQLJobTable) -> None:
        self.config = config

    @property
    def _table(self) -> str:
        return self.config.table

    @property
    def _id(self) -> str:
        return self.config.id_column

    @property
    def _heartbeat(self) -> str:
        return self.config.heartbeat_column

    @property
    def _phase(self) -> str:
        return self.config.phase_column

    def _terminal_idempotency_sql(self) -> str:
        column = self.config.active_idempotency_column
        return f", {column}=NULL" if column else ""

    def claim_next(self, worker_id: str, running_phase: str = "任务执行中") -> str | None:
        """Atomically claim the oldest eligible queued row.

        A competing worker may select the same row, but only one conditional UPDATE
        can win. The loser returns without executing work and polls again.
        """

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {self._id}
                    FROM {self._table}
                    WHERE status='queued'
                      AND cancel_requested=0
                      AND attempt_count < max_attempts
                    ORDER BY id ASC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
                if not row:
                    return None
                job_id = str(row[self._id])
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET status='running', worker_id=%s, locked_at=%s, {self._heartbeat}=%s,
                        started_at=%s, finished_at=NULL, cancel_requested=0,
                        attempt_count=attempt_count + 1, {self._phase}=%s,
                        error_code=NULL, error_message=NULL
                    WHERE {self._id}=%s
                      AND status='queued'
                      AND cancel_requested=0
                      AND attempt_count < max_attempts
                    """,
                    (worker_id, now, now, now, running_phase, job_id),
                )
                return job_id if cursor.rowcount == 1 else None

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET {self._heartbeat}=NOW()
                    WHERE {self._id}=%s AND status='running' AND worker_id=%s
                    """,
                    (job_id, worker_id),
                )
                return cursor.rowcount == 1

    def owns_running_job(self, job_id: str, worker_id: str) -> bool:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT 1 AS owned
                    FROM {self._table}
                    WHERE {self._id}=%s AND status='running' AND worker_id=%s
                    LIMIT 1
                    """,
                    (job_id, worker_id),
                )
                return bool(cursor.fetchone())

    def is_cancel_requested(self, job_id: str) -> bool:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT cancel_requested FROM {self._table} WHERE {self._id}=%s",
                    (job_id,),
                )
                row = cursor.fetchone() or {}
                return bool(row.get("cancel_requested"))

    def request_cancel(self, job_id: str) -> str | None:
        terminal_extra = self._terminal_idempotency_sql()
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET cancel_requested=1, status='cancelled', finished_at=NOW(),
                        estimated_seconds_left=0, {self._heartbeat}=NOW(),
                        {self._phase}='已取消', error_code='cancelled_by_user',
                        error_message='cancel requested before worker started'
                        {terminal_extra}
                    WHERE {self._id}=%s AND status='queued'
                    """,
                    (job_id,),
                )
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET cancel_requested=1, {self._heartbeat}=NOW(),
                        {self._phase}='正在取消', error_code='cancelled_by_user',
                        error_message='cancel requested; waiting for current calculation boundary'
                    WHERE {self._id}=%s AND status='running'
                    """,
                    (job_id,),
                )
                cursor.execute(
                    f"SELECT status FROM {self._table} WHERE {self._id}=%s",
                    (job_id,),
                )
                row = cursor.fetchone()
                return str(row["status"]) if row else None

    def finish_cancelled_if_requested(self, job_id: str, worker_id: str) -> bool:
        terminal_extra = self._terminal_idempotency_sql()
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET status='cancelled', finished_at=NOW(), estimated_seconds_left=0,
                        {self._heartbeat}=NOW(), {self._phase}='已取消',
                        error_code='cancelled_by_user', error_message='cancelled by user'
                        {terminal_extra}
                    WHERE {self._id}=%s AND status='running' AND worker_id=%s
                      AND cancel_requested=1
                    """,
                    (job_id, worker_id),
                )
                return cursor.rowcount == 1

    def recover_stale(self, stale_seconds: int) -> StaleRecoveryResult:
        if stale_seconds < 1:
            raise ValueError("stale_seconds must be positive")
        terminal_extra = self._terminal_idempotency_sql()
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET status='cancelled', finished_at=NOW(), estimated_seconds_left=0,
                        {self._phase}='已取消', error_code='cancelled_by_user',
                        error_message='cancelled after stale worker recovery'
                        {terminal_extra}
                    WHERE status='running' AND cancel_requested=1
                      AND COALESCE({self._heartbeat}, started_at, locked_at)
                          < DATE_SUB(NOW(), INTERVAL %s SECOND)
                    """,
                    (stale_seconds,),
                )
                cancelled = cursor.rowcount
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET status='failed', finished_at=NOW(), estimated_seconds_left=0,
                        {self._phase}='重试耗尽', error_code='stale_retry_exhausted',
                        error_message='worker heartbeat stale and max attempts exhausted'
                        {terminal_extra}
                    WHERE status='running' AND cancel_requested=0
                      AND attempt_count >= max_attempts
                      AND COALESCE({self._heartbeat}, started_at, locked_at)
                          < DATE_SUB(NOW(), INTERVAL %s SECOND)
                    """,
                    (stale_seconds,),
                )
                failed = cursor.rowcount
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET status='queued', worker_id=NULL, locked_at=NULL, {self._heartbeat}=NULL,
                        started_at=NULL, finished_at=NULL, cancel_requested=0,
                        {self._phase}='任务已重新排队', progress_pct=0,
                        error_code='stale_worker_recovered',
                        error_message='requeued after stale worker heartbeat'
                    WHERE status='running' AND cancel_requested=0
                      AND attempt_count < max_attempts
                      AND COALESCE({self._heartbeat}, started_at, locked_at)
                          < DATE_SUB(NOW(), INTERVAL %s SECOND)
                    """,
                    (stale_seconds,),
                )
                requeued = cursor.rowcount
        return StaleRecoveryResult(requeued=requeued, failed=failed, cancelled=cancelled)
