from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime

from app.shared.db import mysql_conn


logger = logging.getLogger(__name__)

_SECRET_PATTERNS = (
    re.compile(r"(?i)(token|secret|api[_-]?key|password|passwd)=([^&\s]+)"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+"),
)


def sanitize_error_message(value: object, limit: int = 500) -> str:
    text = str(value or "").strip()
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(lambda match: f"{match.group(1)}***", text)
    return text[:limit]


def infer_error_code(message: object, default: str = "task_failed") -> str:
    text = str(message or "").lower()
    if "timed out" in text or "timeout" in text:
        return "upstream_timeout"
    if any(token in text for token in ("connection reset", "connection aborted", "remote disconnected", "connection refused")):
        return "upstream_connection_error"
    if any(token in text for token in ("decode", "length mismatch", "no tables found", "not in the [columns]")):
        return "upstream_schema_changed"
    if "out of memory" in text or "oom" in text or "killed" in text:
        return "resource_exhausted"
    if "invalid" in text or "validation" in text:
        return "invalid_request"
    return default


def error_fingerprint(message: object) -> str:
    normalized = sanitize_error_message(message, limit=2000).lower()
    normalized = re.sub(r"\b\d{4}-\d{1,2}-\d{1,2}(?:[ t]\d{1,2}:\d{1,2}:\d{1,2})?\b", "<date>", normalized)
    normalized = re.sub(r"\b0x[0-9a-f]+\b", "<hex>", normalized)
    normalized = re.sub(r"\b\d+(?:\.\d+)?\b", "<n>", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip() or "unknown-error"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def record_job_error(
    source_kind: str,
    job_type: str,
    error_code: str | None,
    message: object,
    *,
    count: int = 1,
    seen_at: datetime | None = None,
) -> None:
    """Best-effort daily error aggregation.

    Error telemetry must never turn a handled job failure into a worker crash, so
    this function logs aggregation failures instead of raising them to callers.
    """

    if count < 1:
        return
    seen_at = seen_at or datetime.now()
    safe_message = sanitize_error_message(message)
    code = str(error_code or infer_error_code(safe_message))[:64]
    fingerprint = error_fingerprint(safe_message)
    try:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO job_error_daily_summary (
                        error_date, source_kind, job_type, error_code, error_fingerprint,
                        occurrence_count, first_seen_at, last_seen_at, last_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        occurrence_count=occurrence_count + VALUES(occurrence_count),
                        first_seen_at=LEAST(first_seen_at, VALUES(first_seen_at)),
                        last_seen_at=GREATEST(last_seen_at, VALUES(last_seen_at)),
                        last_message=VALUES(last_message)
                    """,
                    (
                        seen_at.date(),
                        str(source_kind)[:32],
                        str(job_type)[:128],
                        code,
                        fingerprint,
                        count,
                        seen_at,
                        seen_at,
                        safe_message,
                    ),
                )
    except Exception:
        logger.warning(
            "failed to aggregate structured job error source=%s job_type=%s code=%s",
            source_kind,
            job_type,
            code,
            exc_info=True,
        )
