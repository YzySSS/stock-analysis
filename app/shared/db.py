from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from threading import Lock
from typing import Any, Iterator

import pymysql
from pymysql.cursors import Cursor, DictCursor
from sqlalchemy.pool import QueuePool

from app.shared.settings import mysql_settings, sqlite_settings
from app.shared.observability import database_metrics


_MYSQL_POOL: QueuePool | None = None
_MYSQL_POOL_LOCK = Lock()
_MYSQL_POOL_SETTINGS_SIGNATURE: tuple[Any, ...] | None = None


def _mysql_pool_signature() -> tuple[Any, ...]:
    return (
        mysql_settings.host,
        mysql_settings.port,
        mysql_settings.user,
        mysql_settings.password,
        mysql_settings.database,
        mysql_settings.charset,
        mysql_settings.connect_timeout_seconds,
        mysql_settings.read_timeout_seconds,
        mysql_settings.write_timeout_seconds,
        mysql_settings.pool_size,
        mysql_settings.pool_max_overflow,
        mysql_settings.pool_timeout_seconds,
        mysql_settings.pool_recycle_seconds,
        mysql_settings.pool_pre_ping,
    )


class _CursorModeConnection:
    """Connection view selecting cursor shape without splitting the pool."""

    def __init__(self, connection: Any, *, dict_cursor: bool) -> None:
        self._connection = connection
        self._cursor_class = DictCursor if dict_cursor else Cursor

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        if args or kwargs:
            return self._connection.cursor(*args, **kwargs)
        return self._connection.cursor(self._cursor_class)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _new_mysql_connection() -> pymysql.connections.Connection:
    config = mysql_settings.to_pymysql_dict()
    return pymysql.connect(**config)


def _dispose_mysql_pools_locked() -> None:
    global _MYSQL_POOL
    if _MYSQL_POOL is not None:
        _MYSQL_POOL.dispose()
        _MYSQL_POOL = None


def dispose_mysql_pools() -> None:
    """Dispose idle pooled connections, primarily for process shutdown and tests."""

    global _MYSQL_POOL_SETTINGS_SIGNATURE
    with _MYSQL_POOL_LOCK:
        _dispose_mysql_pools_locked()
        _MYSQL_POOL_SETTINGS_SIGNATURE = None


def _mysql_pool() -> QueuePool:
    global _MYSQL_POOL, _MYSQL_POOL_SETTINGS_SIGNATURE
    signature = _mysql_pool_signature()
    with _MYSQL_POOL_LOCK:
        if _MYSQL_POOL_SETTINGS_SIGNATURE != signature:
            _dispose_mysql_pools_locked()
            _MYSQL_POOL_SETTINGS_SIGNATURE = signature
        if _MYSQL_POOL is None:
            _MYSQL_POOL = QueuePool(
                _new_mysql_connection,
                pool_size=mysql_settings.pool_size,
                max_overflow=mysql_settings.pool_max_overflow,
                timeout=mysql_settings.pool_timeout_seconds,
                recycle=mysql_settings.pool_recycle_seconds,
                # mysql_conn() owns the transaction boundary and always commits
                # or rolls back before returning the connection to the pool.
                reset_on_return=None,
                use_lifo=True,
            )
        return _MYSQL_POOL


def _checkout_mysql_connection(dict_cursor: bool) -> Any:
    if not mysql_settings.pool_enabled:
        return _CursorModeConnection(_new_mysql_connection(), dict_cursor=dict_cursor)

    connection = _mysql_pool().connect()
    if not mysql_settings.pool_pre_ping:
        return _CursorModeConnection(connection, dict_cursor=dict_cursor)
    try:
        connection.ping(reconnect=False)
        return _CursorModeConnection(connection, dict_cursor=dict_cursor)
    except Exception:
        connection.invalidate()
        connection.close()
        replacement = _mysql_pool().connect()
        replacement.ping(reconnect=False)
        return _CursorModeConnection(replacement, dict_cursor=dict_cursor)


def mysql_pool_diagnostics() -> dict[str, Any]:
    with _MYSQL_POOL_LOCK:
        pools = {}
        if _MYSQL_POOL is not None:
            pools["shared"] = {
                "size": _MYSQL_POOL.size(),
                "checked_in": _MYSQL_POOL.checkedin(),
                "checked_out": _MYSQL_POOL.checkedout(),
                "overflow": _MYSQL_POOL.overflow(),
                "status": _MYSQL_POOL.status(),
            }
    return {
        "enabled": mysql_settings.pool_enabled,
        "configured_pool_size": mysql_settings.pool_size,
        "configured_max_overflow": mysql_settings.pool_max_overflow,
        "pools": pools,
    }


@contextmanager
def mysql_conn(dict_cursor: bool = True) -> Iterator[Any]:
    """Yield a transactional PyMySQL-compatible connection.

    With pooling enabled, ``close()`` returns the DBAPI connection to QueuePool;
    callers retain the existing context-manager API and commit/rollback contract.
    """

    started = time.perf_counter()
    try:
        conn = _checkout_mysql_connection(dict_cursor)
    except BaseException:
        elapsed_ms = (time.perf_counter() - started) * 1000
        database_metrics.observe(
            checkout_ms=elapsed_ms,
            transaction_ms=elapsed_ms,
            success=False,
        )
        raise
    checkout_ms = (time.perf_counter() - started) * 1000
    success = False
    try:
        yield conn
        conn.commit()
        success = True
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            invalidate = getattr(conn, "invalidate", None)
            if callable(invalidate):
                invalidate()
        raise
    finally:
        try:
            conn.close()
        finally:
            database_metrics.observe(
                checkout_ms=checkout_ms,
                transaction_ms=(time.perf_counter() - started) * 1000,
                success=success,
            )


@contextmanager
def mysql_read_conn(dict_cursor: bool = True) -> Iterator[Any]:
    """Yield a read transaction and roll it back instead of issuing a commit."""

    started = time.perf_counter()
    try:
        conn = _checkout_mysql_connection(dict_cursor)
    except BaseException:
        elapsed_ms = (time.perf_counter() - started) * 1000
        database_metrics.observe(
            checkout_ms=elapsed_ms,
            transaction_ms=elapsed_ms,
            success=False,
        )
        raise
    checkout_ms = (time.perf_counter() - started) * 1000
    success = False
    try:
        yield conn
        conn.rollback()
        success = True
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            invalidate = getattr(conn, "invalidate", None)
            if callable(invalidate):
                invalidate()
        raise
    finally:
        try:
            conn.close()
        finally:
            database_metrics.observe(
                checkout_ms=checkout_ms,
                transaction_ms=(time.perf_counter() - started) * 1000,
                success=success,
            )


@contextmanager
def sqlite_stock_history_conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(sqlite_settings.stock_history_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def sqlite_sentiment_conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(sqlite_settings.sentiment_cache_path)
    try:
        yield conn
    finally:
        conn.close()


def ping_mysql() -> dict:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE() AS db, VERSION() AS version")
            return cursor.fetchone()
