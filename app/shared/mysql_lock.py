from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

from app.shared.db import mysql_conn


MYSQL_LOCK_NAME_MAX_LENGTH = 64


@dataclass
class MysqlAdvisoryLockHandle:
    name: str
    conn_context: Any
    conn: Any
    connection_id: int | None = None
    released: bool = False


def _validate_lock_name(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("MySQL advisory lock name must not be empty")
    if len(normalized) > MYSQL_LOCK_NAME_MAX_LENGTH:
        raise ValueError(f"MySQL advisory lock name must be <= {MYSQL_LOCK_NAME_MAX_LENGTH} characters")
    return normalized


def acquire_mysql_advisory_lock(name: str, timeout_seconds: int = 0) -> MysqlAdvisoryLockHandle | None:
    """Try to acquire a named MySQL lock and keep its owning connection open.

    MySQL advisory locks are connection-scoped. Returning only a boolean from a
    short-lived ``mysql_conn()`` block releases the lock immediately, so callers
    must keep this handle until their whole task has finished.
    """

    lock_name = _validate_lock_name(name)
    timeout = max(int(timeout_seconds), 0)
    conn_context = mysql_conn()
    conn = conn_context.__enter__()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT GET_LOCK(%s, %s) AS locked, CONNECTION_ID() AS connection_id",
                (lock_name, timeout),
            )
            row = cursor.fetchone() or {}
    except BaseException:
        conn_context.__exit__(*sys.exc_info())
        raise

    if int(row.get("locked") or 0) != 1:
        conn_context.__exit__(None, None, None)
        return None

    connection_id = row.get("connection_id")
    return MysqlAdvisoryLockHandle(
        name=lock_name,
        conn_context=conn_context,
        conn=conn,
        connection_id=int(connection_id) if connection_id is not None else None,
    )


def release_mysql_advisory_lock(lock_handle: MysqlAdvisoryLockHandle | None) -> str | None:
    """Release a lock on its owning connection and close that connection.

    Cleanup errors are returned as text so a ``finally`` block can report them
    without hiding the task's original exception. Closing the connection still
    releases a MySQL advisory lock if the explicit RELEASE_LOCK call fails.
    """

    if lock_handle is None or lock_handle.released:
        return None

    release_error: str | None = None
    try:
        with lock_handle.conn.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (lock_handle.name,))
            row = cursor.fetchone() or {}
            if int(row.get("released") or 0) != 1:
                release_error = "lock_not_owned_by_connection"
    except Exception as exc:
        release_error = f"{type(exc).__name__}: {str(exc)[:300]}"

    try:
        lock_handle.conn_context.__exit__(None, None, None)
    except Exception as exc:
        close_error = f"{type(exc).__name__}: {str(exc)[:300]}"
        release_error = release_error or close_error
    finally:
        lock_handle.released = True

    return release_error
