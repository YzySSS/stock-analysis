from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from app.jobs.errors import error_fingerprint, infer_error_code, record_job_error, sanitize_error_message
from app.shared.db import mysql_conn


class TaskRunLogger:
    def start(self, task_name: str, run_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        sql = """
        INSERT INTO task_run_log (task_name, run_id, status, started_at, metadata_json)
        VALUES (%s, %s, %s, %s, %s)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        task_name,
                        run_id,
                        "running",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        json.dumps(metadata or {}, ensure_ascii=False),
                    ),
                )

    def finish(
        self,
        task_name: str,
        run_id: str,
        status: str,
        message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None,
    ) -> None:
        is_error = status in {"failed", "killed"}
        safe_message = sanitize_error_message(message) if is_error else message
        resolved_error_code = (error_code or infer_error_code(safe_message)) if is_error else None
        fingerprint = error_fingerprint(safe_message) if is_error else None
        sql = """
        UPDATE task_run_log
        SET status = %s,
            finished_at = %s,
            message = %s,
            error_code = %s,
            error_fingerprint = %s,
            metadata_json = %s
        WHERE task_name = %s AND run_id = %s
        ORDER BY id DESC
        LIMIT 1
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        status,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        safe_message,
                        resolved_error_code,
                        fingerprint,
                        json.dumps(metadata or {}, ensure_ascii=False),
                        task_name,
                        run_id,
                    ),
                )
        if is_error:
            record_job_error(
                "scheduled_task",
                task_name,
                resolved_error_code,
                safe_message,
            )
