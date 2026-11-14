"""Daily extrema, published in the SAME transaction as tracked-source changes.

A revision is a committed write token, not MAX(time)/COUNT: earlier corrections
and deletions rebuild the whole affected day. All source writes must acquire the
manifest row first. Readers use one repeatable-read snapshot; unpublished or
unsupported dates retain the original raw query. Completeness means only the
observed tracked source, never a claim of a complete exchange session.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date

from pymysql import MySQLError
from app.shared.db import mysql_conn

logger = logging.getLogger(__name__)
SCOPE = 'tracked_positive_price_v1'


def lock_day(cursor, target: date, *, allow_retiring: bool = False) -> None:
    cursor.execute("""INSERT IGNORE INTO tracking_minute_daily_manifest
        (trade_date, source_revision, status, generated_at) VALUES (%s, %s, 'building', NOW(6))""",
        (target, uuid.uuid4().hex))
    cursor.execute('SELECT status FROM tracking_minute_daily_manifest WHERE trade_date=%s FOR UPDATE', (target,))
    row = cursor.fetchone()
    status = row.get('status') if isinstance(row, dict) else row[0] if row else None
    if status == 'retiring' and not allow_retiring:
        raise RuntimeError('tracked day is retiring; source restore requires an explicit repair')


def publish_day(cursor, target: date) -> str:
    """Caller holds lock_day BEFORE source changes; failure rolls everything back."""
    revision = uuid.uuid4().hex
    cursor.execute('DELETE FROM tracking_minute_daily WHERE trade_date=%s', (target,))
    cursor.execute("""INSERT INTO tracking_minute_daily
        (code, trade_date, source_revision, min_price, max_price, valid_count, first_quote_minute, last_quote_minute)
        SELECT code, trade_date, %s, MIN(latest_price), MAX(latest_price), COUNT(*), MIN(quote_minute), MAX(quote_minute)
        FROM stock_realtime_intraday_tracked
        WHERE trade_date=%s AND latest_price IS NOT NULL AND latest_price > 0
        GROUP BY code, trade_date""", (revision, target))
    cursor.execute("""UPDATE tracking_minute_daily_manifest SET source_revision=%s,
        source_scope=%s, status=CASE WHEN
            (SELECT COUNT(*) FROM stock_realtime_intraday_tracked WHERE trade_date=%s
              AND DATE(quote_minute) <> trade_date) = 0 THEN 'ready' ELSE 'unsupported' END,
        source_rows=(SELECT COUNT(*) FROM stock_realtime_intraday_tracked WHERE trade_date=%s),
        valid_rows=(SELECT COALESCE(SUM(valid_count),0) FROM tracking_minute_daily WHERE trade_date=%s),
        anomalous_rows=(SELECT COUNT(*) FROM stock_realtime_intraday_tracked WHERE trade_date=%s AND DATE(quote_minute) <> trade_date),
        code_count=(SELECT COUNT(*) FROM tracking_minute_daily WHERE trade_date=%s),
        completeness='observed_only', generated_at=NOW(6) WHERE trade_date=%s""",
        (revision, SCOPE, target, target, target, target, target, target))
    return revision


def invalidate_displays() -> None:
    try:
        from app.display_cache.service import invalidate
        invalidate('market', 'tracking')
    except Exception:
        # Redis can fail; DB read correctness does not depend on it, hard TTLs
        # remain bounded. Never turn a successful source commit into a retry.
        logger.warning('tracking daily post-commit display invalidation failed')


def rebuild_day(target: date) -> str:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            lock_day(cursor, target)
            revision = publish_day(cursor, target)
    invalidate_displays()
    return revision


def withdraw_day(target: date) -> None:
    # DROP PARTITION implicitly commits; withdraw BEFORE DDL in a separate
    # committed transaction. A DDL failure leaves a correct live fallback.
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            lock_day(cursor, target, allow_retiring=True)
            cursor.execute("UPDATE tracking_minute_daily_manifest SET status='retiring', source_revision=%s, generated_at=NOW(6) WHERE trade_date=%s", (uuid.uuid4().hex, target))
            cursor.execute('DELETE FROM tracking_minute_daily WHERE trade_date=%s', (target,))
    invalidate_displays()


def ready_dates(cursor) -> list[date]:
    try:
        cursor.execute("SELECT trade_date FROM tracking_minute_daily_manifest WHERE status='ready' AND source_scope=%s ORDER BY trade_date", (SCOPE,))
        return [row['trade_date'] for row in cursor.fetchall() or []]
    except MySQLError as exc:
        if exc.args and exc.args[0] == 1146:
            return []  # additive migration not installed yet
        raise
