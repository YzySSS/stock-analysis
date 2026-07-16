from __future__ import annotations

import json
import logging
import threading
from typing import Any

from app.shared.db import mysql_conn


logger = logging.getLogger(__name__)


class WorkerRuntimeRepository:
    def register(self, worker_type: str, worker_id: str, metadata: dict[str, Any] | None = None) -> None:
        payload = json.dumps(metadata or {}, ensure_ascii=False, default=str)
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO worker_runtime_heartbeat (
                        worker_type, worker_id, status, current_job_id,
                        started_at, heartbeat_at, stopped_at, metadata_json
                    ) VALUES (%s, %s, 'starting', NULL, NOW(), NOW(), NULL, %s)
                    ON DUPLICATE KEY UPDATE
                        status='starting', current_job_id=NULL,
                        started_at=NOW(), heartbeat_at=NOW(), stopped_at=NULL,
                        metadata_json=VALUES(metadata_json)
                    """,
                    (worker_type, worker_id, payload),
                )

    def heartbeat(self, worker_type: str, worker_id: str, status: str, current_job_id: str | None) -> bool:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE worker_runtime_heartbeat
                    SET last_job_started_at=CASE
                            WHEN %s='running' AND (current_job_id IS NULL OR current_job_id<>%s)
                            THEN NOW() ELSE last_job_started_at END,
                        last_job_finished_at=CASE
                            WHEN %s='idle' AND current_job_id IS NOT NULL
                            THEN NOW() ELSE last_job_finished_at END,
                        status=%s, current_job_id=%s, heartbeat_at=NOW(), stopped_at=NULL
                    WHERE worker_type=%s AND worker_id=%s
                    """,
                    (
                        status,
                        current_job_id,
                        status,
                        status,
                        current_job_id,
                        worker_type,
                        worker_id,
                    ),
                )
                return cursor.rowcount == 1

    def stop(self, worker_type: str, worker_id: str) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE worker_runtime_heartbeat
                    SET last_job_finished_at=CASE
                            WHEN current_job_id IS NOT NULL THEN NOW() ELSE last_job_finished_at END,
                        status='stopped', current_job_id=NULL, heartbeat_at=NOW(), stopped_at=NOW()
                    WHERE worker_type=%s AND worker_id=%s
                    """,
                    (worker_type, worker_id),
                )


class WorkerRuntimeHeartbeat:
    """Process-level worker lease that remains fresh while the queue is idle."""

    def __init__(
        self,
        worker_type: str,
        worker_id: str,
        *,
        interval_seconds: float = 10.0,
        repository: WorkerRuntimeRepository | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.worker_type = worker_type
        self.worker_id = worker_id
        self.interval_seconds = interval_seconds
        self.repository = repository or WorkerRuntimeRepository()
        self.metadata = metadata or {}
        self._status = "starting"
        self._current_job_id: str | None = None
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.repository.register(self.worker_type, self.worker_id, self.metadata)
        self._set_state("idle", None, immediate=True)
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.worker_type}-runtime-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def set_running(self, job_id: str) -> None:
        self._set_state("running", str(job_id), immediate=True)

    def set_idle(self) -> None:
        self._set_state("idle", None, immediate=True)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=max(1.0, min(self.interval_seconds + 1, 5.0)))
        try:
            self.repository.stop(self.worker_type, self.worker_id)
        except Exception:
            logger.warning("failed to mark worker runtime stopped worker_type=%s", self.worker_type, exc_info=True)

    def __enter__(self) -> WorkerRuntimeHeartbeat:
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        self.stop()

    def _set_state(self, status: str, current_job_id: str | None, *, immediate: bool) -> None:
        with self._state_lock:
            self._status = status
            self._current_job_id = current_job_id
        if immediate:
            self._send_heartbeat()

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self._send_heartbeat()

    def _send_heartbeat(self) -> None:
        with self._state_lock:
            status = self._status
            current_job_id = self._current_job_id
        try:
            updated = self.repository.heartbeat(
                self.worker_type,
                self.worker_id,
                status,
                current_job_id,
            )
            if not updated:
                self.repository.register(self.worker_type, self.worker_id, self.metadata)
                self.repository.heartbeat(
                    self.worker_type,
                    self.worker_id,
                    status,
                    current_job_id,
                )
        except Exception:
            logger.warning(
                "worker runtime heartbeat failed worker_type=%s worker_id=%s",
                self.worker_type,
                self.worker_id,
                exc_info=True,
            )
