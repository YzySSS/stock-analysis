from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

import pymysql
from pymysql.cursors import DictCursor

from app.shared.settings import mysql_settings, sqlite_settings


@contextmanager
def mysql_conn(dict_cursor: bool = True) -> Iterator[pymysql.connections.Connection]:
    config = mysql_settings.to_pymysql_dict()
    if dict_cursor:
        config["cursorclass"] = DictCursor
    conn = pymysql.connect(**config)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE() AS db, VERSION() AS version")
            return cursor.fetchone()
