from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Iterator, Sequence

from app.orchestration.realtime_schema import intraday_table_ddl
from app.shared.db import mysql_conn, mysql_maintenance_conn, mysql_read_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


ROLLUP_INTERVALS = (5, 15)
FULL_MARKET_RAW_TRADE_DAYS = 2
ROLLUP_TRADE_DAYS = 90
TRACKED_RAW_TRADE_DAYS = 90
ROLLUP_CODE_BATCH_SIZE = 200
LIFECYCLE_LOCK_NAME = "stock_realtime_lifecycle_lock"
WRITER_LOCK_NAME = "stock_realtime_snapshot_update_lock"

PARTITIONED_TABLES = {
    "stock_realtime_intraday",
    "stock_realtime_intraday_partitioned",
    "stock_realtime_intraday_tracked",
    "stock_realtime_bar_rollup",
}

INTRADAY_COPY_COLUMNS = (
    "code",
    "source_code",
    "name",
    "trade_date",
    "quote_time",
    "quote_minute",
    "latest_price",
    "change_amount",
    "pct_chg",
    "bid_price",
    "ask_price",
    "pre_close",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "amount",
    "batch_id",
    "received_at",
    "freshness_seconds",
    "is_stale",
    "source",
    "created_at",
)


@dataclass(frozen=True)
class RealtimeLifecyclePolicy:
    raw_trade_days: int = FULL_MARKET_RAW_TRADE_DAYS
    rollup_trade_days: int = ROLLUP_TRADE_DAYS
    tracked_trade_days: int = TRACKED_RAW_TRADE_DAYS
    intervals: tuple[int, ...] = ROLLUP_INTERVALS

    def validate(self) -> "RealtimeLifecyclePolicy":
        if self.raw_trade_days < 2:
            raise ValueError("full-market raw retention must keep at least two trade days")
        if self.rollup_trade_days < self.raw_trade_days:
            raise ValueError("rollup retention must not be shorter than raw retention")
        if self.tracked_trade_days < self.raw_trade_days:
            raise ValueError("tracked raw retention must not be shorter than full-market raw retention")
        if not self.intervals or any(item not in ROLLUP_INTERVALS for item in self.intervals):
            raise ValueError(f"rollup intervals must be a subset of {ROLLUP_INTERVALS}")
        return self


def normalize_trade_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def partition_name_for_date(value: str | date | datetime) -> str:
    return f"p{normalize_trade_date(value).strftime('%Y%m%d')}"


def retained_trade_dates(values: Iterable[str | date | datetime], keep: int) -> list[date]:
    if keep <= 0:
        return []
    normalized = sorted({normalize_trade_date(value) for value in values}, reverse=True)
    return normalized[:keep]


def expired_trade_dates(values: Iterable[str | date | datetime], keep: int) -> list[date]:
    retained = set(retained_trade_dates(values, keep))
    return sorted({normalize_trade_date(value) for value in values if normalize_trade_date(value) not in retained})


def table_exists(table_name: str) -> bool:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = %s
                """,
                (table_name,),
            )
            return int((cursor.fetchone() or {}).get("total") or 0) > 0


def table_partitions(table_name: str) -> list[dict[str, Any]]:
    if table_name not in PARTITIONED_TABLES:
        raise ValueError(f"unsupported partitioned table: {table_name}")
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT partition_name, partition_description, partition_ordinal_position, table_rows
                FROM information_schema.partitions
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY partition_ordinal_position
                """,
                (table_name,),
            )
            return [
                {str(key).lower(): value for key, value in row.items()}
                for row in (cursor.fetchall() or [])
            ]


def is_partitioned(table_name: str) -> bool:
    return any(row.get("partition_name") for row in table_partitions(table_name))


def ensure_daily_partition(table_name: str, trade_date: str | date | datetime) -> bool:
    if table_name not in PARTITIONED_TABLES:
        raise ValueError(f"unsupported partitioned table: {table_name}")
    target = normalize_trade_date(trade_date)
    partition_name = partition_name_for_date(target)
    partitions = table_partitions(table_name)
    names = {str(row.get("partition_name")) for row in partitions if row.get("partition_name")}
    if not names:
        return False
    if partition_name in names:
        return False
    if "p_future" not in names:
        raise RuntimeError(f"{table_name} has no p_future partition")
    boundary = (target + timedelta(days=1)).isoformat()
    with mysql_maintenance_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                ALTER TABLE {table_name}
                REORGANIZE PARTITION p_future INTO (
                    PARTITION {partition_name} VALUES LESS THAN ('{boundary}'),
                    PARTITION p_future VALUES LESS THAN (MAXVALUE)
                )
                """
            )
    return True


def _partition_date(partition_name: Any) -> date | None:
    text = str(partition_name or "")
    if len(text) != 9 or not text.startswith("p") or not text[1:].isdigit():
        return None
    try:
        return datetime.strptime(text[1:], "%Y%m%d").date()
    except ValueError:
        return None


def _p_future_trade_dates(table_name: str) -> list[date]:
    if table_name not in PARTITIONED_TABLES:
        raise ValueError(f"unsupported partitioned table: {table_name}")
    partitions = table_partitions(table_name)
    names = {str(row.get("partition_name")) for row in partitions if row.get("partition_name")}
    if "p_future" not in names:
        return []
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT trade_date FROM {table_name} PARTITION (p_future) "
                "ORDER BY trade_date ASC LIMIT 1"
            )
            first_value = (cursor.fetchone() or {}).get("trade_date")
            cursor.execute(
                f"SELECT trade_date FROM {table_name} PARTITION (p_future) "
                "ORDER BY trade_date DESC LIMIT 1"
            )
            last_value = (cursor.fetchone() or {}).get("trade_date")
            if not first_value:
                return []
            first_date = normalize_trade_date(first_value)
            last_date = normalize_trade_date(last_value or first_value)
            if first_date == last_date:
                return [first_date]
            cursor.execute(
                f"SELECT DISTINCT trade_date FROM {table_name} PARTITION (p_future) "
                "ORDER BY trade_date ASC"
            )
            return [
                normalize_trade_date(row["trade_date"])
                for row in (cursor.fetchall() or [])
                if row.get("trade_date")
            ]


def ensure_intraday_hot_partition(trade_date: str | date | datetime) -> list[str]:
    """Ensure p_future is drained oldest-first before the minute writer inserts today."""

    target = normalize_trade_date(trade_date)
    partitions = table_partitions("stock_realtime_intraday")
    names = {str(row.get("partition_name")) for row in partitions if row.get("partition_name")}
    target_name = partition_name_for_date(target)
    if target_name in names:
        return []
    pending_dates = set(_p_future_trade_dates("stock_realtime_intraday"))
    pending_dates.add(target)
    created: list[str] = []
    for pending_date in sorted(item for item in pending_dates if item <= target):
        if ensure_daily_partition("stock_realtime_intraday", pending_date):
            created.append(partition_name_for_date(pending_date))
    return created


def _date_rows(table_name: str) -> list[date]:
    if not table_exists(table_name):
        return []
    if table_name in PARTITIONED_TABLES:
        partitions = table_partitions(table_name)
        if any(row.get("partition_name") for row in partitions):
            values = {
                parsed
                for row in partitions
                if (parsed := _partition_date(row.get("partition_name"))) is not None
            }
            values.update(_p_future_trade_dates(table_name))
            return sorted(values, reverse=True)
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT trade_date FROM {table_name} ORDER BY trade_date DESC")
            return [normalize_trade_date(row["trade_date"]) for row in cursor.fetchall() if row.get("trade_date")]


def _table_summary(table_name: str, *, exact: bool = True) -> dict[str, Any]:
    if not table_exists(table_name):
        return {"exists": False, "rows": 0, "trade_dates": []}
    if not exact:
        dates = _date_rows(table_name)
        with mysql_read_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT SUM(table_rows) AS table_rows
                    FROM information_schema.partitions
                    WHERE table_schema=DATABASE() AND table_name=%s
                    """,
                    (table_name,),
                )
                row = cursor.fetchone() or {}
        return {
            "exists": True,
            "rows": int(row.get("table_rows") or 0),
            "rows_are_approximate": True,
            "trade_days": len(dates),
            "min_trade_date": min(dates).isoformat() if dates else None,
            "max_trade_date": max(dates).isoformat() if dates else None,
        }
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) AS rows_count,
                       COUNT(DISTINCT code) AS code_count,
                       COUNT(DISTINCT trade_date) AS trade_day_count,
                       MIN(trade_date) AS min_trade_date,
                       MAX(trade_date) AS max_trade_date
                FROM {table_name}
                """
            )
            row = cursor.fetchone() or {}
    return {
        "exists": True,
        "rows": int(row.get("rows_count") or 0),
        "codes": int(row.get("code_count") or 0),
        "trade_days": int(row.get("trade_day_count") or 0),
        "min_trade_date": str(row.get("min_trade_date")) if row.get("min_trade_date") else None,
        "max_trade_date": str(row.get("max_trade_date")) if row.get("max_trade_date") else None,
    }


def build_lifecycle_plan(policy: RealtimeLifecyclePolicy | None = None) -> dict[str, Any]:
    final_policy = (policy or RealtimeLifecyclePolicy()).validate()
    raw_dates = _date_rows("stock_realtime_intraday")
    rollup_dates = _date_rows("stock_realtime_bar_rollup")
    tracked_dates = _date_rows("stock_realtime_intraday_tracked")
    return {
        "policy": asdict(final_policy),
        "raw_partitioned": is_partitioned("stock_realtime_intraday") if table_exists("stock_realtime_intraday") else False,
        "raw_trade_dates": [item.isoformat() for item in raw_dates],
        "aggregate_trade_dates": [item.isoformat() for item in sorted(raw_dates, reverse=True)],
        "raw_expired_candidates": [item.isoformat() for item in expired_trade_dates(raw_dates, final_policy.raw_trade_days)],
        "rollup_expired_candidates": [item.isoformat() for item in expired_trade_dates(rollup_dates, final_policy.rollup_trade_days)],
        "tracked_expired_candidates": [item.isoformat() for item in expired_trade_dates(tracked_dates, final_policy.tracked_trade_days)],
        "tables": {
            "raw": _table_summary("stock_realtime_intraday", exact=False),
            "rollup": _table_summary("stock_realtime_bar_rollup", exact=False),
            "tracked": _table_summary("stock_realtime_intraday_tracked", exact=False),
        },
    }


def fetch_rollup_bars(
    code: str,
    *,
    interval: int = 5,
    trade_date: str | date | datetime | None = None,
    limit: int = 400,
) -> dict[str, Any]:
    if interval not in ROLLUP_INTERVALS:
        raise ValueError(f"interval must be one of {ROLLUP_INTERVALS}")
    final_limit = max(1, min(int(limit), 5000))
    target = normalize_trade_date(trade_date) if trade_date else None
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            if target is None:
                cursor.execute(
                    """
                    SELECT MAX(trade_date) AS trade_date
                    FROM stock_realtime_bar_rollup
                    WHERE code=%s AND interval_minutes=%s
                    """,
                    (code, interval),
                )
                value = (cursor.fetchone() or {}).get("trade_date")
                target = normalize_trade_date(value) if value else None
            if target is None:
                rows = []
            else:
                cursor.execute(
                    """
                    SELECT bucket_start, bucket_end, open_price, high_price, low_price, close_price,
                           pre_close, pct_chg_close, volume_delta, amount_delta, sample_count,
                           first_quote_time, last_quote_time, source
                    FROM stock_realtime_bar_rollup
                    WHERE code=%s AND trade_date=%s AND interval_minutes=%s
                    ORDER BY bucket_start ASC
                    LIMIT %s
                    """,
                    (code, target, interval, final_limit),
                )
                rows = cursor.fetchall() or []

    def number(value: Any) -> float | None:
        return float(value) if value is not None else None

    return {
        "code": code,
        "trade_date": target.isoformat() if target else None,
        "interval_minutes": interval,
        "count": len(rows),
        "source": "stock_realtime_bar_rollup",
        "items": [
            {
                "bucket_start": str(row.get("bucket_start")) if row.get("bucket_start") else None,
                "bucket_end": str(row.get("bucket_end")) if row.get("bucket_end") else None,
                "open": number(row.get("open_price")),
                "high": number(row.get("high_price")),
                "low": number(row.get("low_price")),
                "close": number(row.get("close_price")),
                "pre_close": number(row.get("pre_close")),
                "pct_chg": number(row.get("pct_chg_close")),
                "volume": int(row.get("volume_delta")) if row.get("volume_delta") is not None else None,
                "amount": number(row.get("amount_delta")),
                "sample_count": int(row.get("sample_count") or 0),
                "first_quote_time": str(row.get("first_quote_time")) if row.get("first_quote_time") else None,
                "last_quote_time": str(row.get("last_quote_time")) if row.get("last_quote_time") else None,
                "source": row.get("source"),
            }
            for row in rows
        ],
    }


def _source_stats(trade_date: date) -> dict[str, Any]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS source_rows,
                       COUNT(DISTINCT code) AS source_codes,
                       MIN(quote_minute) AS first_quote_minute,
                       MAX(quote_minute) AS last_quote_minute
                FROM stock_realtime_intraday
                WHERE trade_date = %s AND latest_price IS NOT NULL AND latest_price > 0
                """,
                (trade_date,),
            )
            row = cursor.fetchone() or {}
    return {
        "source_rows": int(row.get("source_rows") or 0),
        "source_codes": int(row.get("source_codes") or 0),
        "first_quote_minute": row.get("first_quote_minute"),
        "last_quote_minute": row.get("last_quote_minute"),
    }


def _source_revision(trade_date: date) -> dict[str, Any]:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT batch_id, quote_minute, received_at
                FROM stock_realtime_intraday FORCE INDEX (uniq_realtime_intraday_minute)
                WHERE trade_date=%s AND latest_price IS NOT NULL AND latest_price > 0
                ORDER BY quote_minute DESC, code DESC
                LIMIT 1
                """,
                (trade_date,),
            )
            row = cursor.fetchone() or {}
    batch_id = str(row.get("batch_id") or "")
    quote_minute = str(row.get("quote_minute") or "")
    received_at = str(row.get("received_at") or "")
    payload = f"{trade_date.isoformat()}|{batch_id}|{quote_minute}|{received_at}"
    return {
        "source_fingerprint": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "source_batch_id": batch_id or None,
        "source_revision_quote_minute": row.get("quote_minute"),
        "source_revision_received_at": row.get("received_at"),
    }


def _source_codes(trade_date: date, expected_count: int) -> list[str]:
    codes: set[str] = set()
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code
                FROM stock_realtime_snapshot
                WHERE trade_date=%s AND latest_price IS NOT NULL AND latest_price > 0
                ORDER BY code
                """,
                (trade_date,),
            )
            codes.update(str(row["code"]) for row in (cursor.fetchall() or []) if row.get("code"))
            if len(codes) == expected_count:
                return sorted(codes)

            cursor.execute(
                """
                SELECT interval_minutes
                FROM stock_realtime_rollup_manifest
                WHERE trade_date=%s AND status IN ('success','partial') AND rollup_codes > 0
                ORDER BY rollup_codes DESC, interval_minutes DESC
                LIMIT 1
                """,
                (trade_date,),
            )
            preferred = (cursor.fetchone() or {}).get("interval_minutes")
            if preferred is not None:
                cursor.execute(
                    """
                    SELECT DISTINCT code
                    FROM stock_realtime_bar_rollup
                    WHERE trade_date=%s AND interval_minutes=%s
                    ORDER BY code
                    """,
                    (trade_date, preferred),
                )
                codes.update(str(row["code"]) for row in (cursor.fetchall() or []) if row.get("code"))
            if len(codes) == expected_count:
                return sorted(codes)

            cursor.execute(
                """
                SELECT DISTINCT code
                FROM stock_realtime_intraday FORCE INDEX (idx_realtime_intraday_code_time)
                WHERE trade_date=%s AND latest_price IS NOT NULL AND latest_price > 0
                ORDER BY code
                """,
                (trade_date,),
            )
            return [str(row["code"]) for row in (cursor.fetchall() or []) if row.get("code")]


def _chunks(values: Sequence[str], size: int = ROLLUP_CODE_BATCH_SIZE) -> Iterator[list[str]]:
    final_size = max(1, int(size))
    for offset in range(0, len(values), final_size):
        yield list(values[offset : offset + final_size])


def _manifest_rows_for_date(trade_date: date) -> dict[int, dict[str, Any]]:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date, interval_minutes, status, source_rows, source_codes,
                       rollup_rows, rollup_codes, first_quote_minute, last_quote_minute,
                       source_fingerprint, error_code, error_message, started_at, finished_at
                FROM stock_realtime_rollup_manifest
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            return {
                int(row["interval_minutes"]): row
                for row in (cursor.fetchall() or [])
                if row.get("interval_minutes") is not None
            }


def _manifest_matches_legacy_source(manifest: dict[str, Any], source: dict[str, Any]) -> bool:
    return bool(
        manifest.get("status") == "success"
        and int(manifest.get("source_rows") or 0) == int(source.get("source_rows") or 0)
        and int(manifest.get("source_codes") or 0) == int(source.get("source_codes") or 0)
        and manifest.get("first_quote_minute") == source.get("first_quote_minute")
        and manifest.get("last_quote_minute") == source.get("last_quote_minute")
        and int(manifest.get("rollup_rows") or 0) > 0
        and int(manifest.get("rollup_codes") or 0) == int(source.get("source_codes") or 0)
    )


def _set_manifest_source_fingerprint(trade_date: date, interval: int, fingerprint: str) -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE stock_realtime_rollup_manifest
                SET source_fingerprint=%s
                WHERE trade_date=%s AND interval_minutes=%s
                """,
                (fingerprint, trade_date, interval),
            )


def _skipped_rollup_result(
    trade_date: date,
    interval: int,
    manifest: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    return {
        "trade_date": trade_date.isoformat(),
        "interval_minutes": interval,
        "status": "skipped",
        "reason": reason,
        "manifest_status": manifest.get("status"),
        "source_rows": int(manifest.get("source_rows") or 0),
        "source_codes": int(manifest.get("source_codes") or 0),
        "rollup_rows": int(manifest.get("rollup_rows") or 0),
        "rollup_codes": int(manifest.get("rollup_codes") or 0),
        "source_fingerprint": manifest.get("source_fingerprint"),
    }


def _start_manifest(trade_date: date, interval: int, source: dict[str, Any]) -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO stock_realtime_rollup_manifest (
                    trade_date, interval_minutes, status, source_rows, source_codes,
                    first_quote_minute, last_quote_minute, source_fingerprint,
                    started_at, finished_at,
                    error_code, error_message
                ) VALUES (%s,%s,'running',%s,%s,%s,%s,%s,NOW(),NULL,NULL,NULL)
                ON DUPLICATE KEY UPDATE
                    status='running', source_rows=VALUES(source_rows), source_codes=VALUES(source_codes),
                    first_quote_minute=VALUES(first_quote_minute), last_quote_minute=VALUES(last_quote_minute),
                    source_fingerprint=VALUES(source_fingerprint),
                    started_at=NOW(), finished_at=NULL, error_code=NULL, error_message=NULL
                """,
                (
                    trade_date,
                    interval,
                    source["source_rows"],
                    source["source_codes"],
                    source["first_quote_minute"],
                    source["last_quote_minute"],
                    source["source_fingerprint"],
                ),
            )


def _finish_manifest(
    trade_date: date,
    interval: int,
    *,
    status: str,
    rollup_rows: int,
    rollup_codes: int,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE stock_realtime_rollup_manifest
                SET status=%s, rollup_rows=%s, rollup_codes=%s,
                    error_code=%s, error_message=%s, finished_at=NOW()
                WHERE trade_date=%s AND interval_minutes=%s
                """,
                (status, rollup_rows, rollup_codes, error_code, error_message, trade_date, interval),
            )


def _rollup_sql(interval: int, code_count: int | None = None) -> str:
    if interval not in ROLLUP_INTERVALS:
        raise ValueError(f"unsupported rollup interval: {interval}")
    if code_count is not None and code_count <= 0:
        raise ValueError("code_count must be positive when a code filter is requested")
    code_filter = ""
    if code_count is not None:
        code_filter = f" AND code IN ({','.join(['%s'] * code_count)})"
    return f"""
    INSERT INTO stock_realtime_bar_rollup (
        code, source_code, name, trade_date, interval_minutes, bucket_start, bucket_end,
        open_price, high_price, low_price, close_price, pre_close, pct_chg_close,
        volume_delta, amount_delta, cumulative_volume, cumulative_amount, sample_count,
        first_quote_time, last_quote_time, source
    )
    WITH bucketed AS (
        SELECT
            id, code, source_code, name, trade_date, quote_time, quote_minute,
            latest_price, pre_close, volume, amount,
            TIMESTAMP(
                trade_date,
                MAKETIME(HOUR(quote_minute), FLOOR(MINUTE(quote_minute) / {interval}) * {interval}, 0)
            ) AS bucket_start
        FROM stock_realtime_intraday
        WHERE trade_date = %s AND latest_price IS NOT NULL AND latest_price > 0{code_filter}
    ),
    ranked AS (
        SELECT
            bucketed.*,
            ROW_NUMBER() OVER (
                PARTITION BY code, trade_date, bucket_start
                ORDER BY quote_minute ASC, id ASC
            ) AS first_rank,
            ROW_NUMBER() OVER (
                PARTITION BY code, trade_date, bucket_start
                ORDER BY quote_minute DESC, id DESC
            ) AS last_rank
        FROM bucketed
    ),
    aggregated AS (
        SELECT
            code,
            MAX(CASE WHEN last_rank = 1 THEN source_code END) AS source_code,
            MAX(CASE WHEN last_rank = 1 THEN name END) AS name,
            trade_date,
            bucket_start,
            MAX(CASE WHEN first_rank = 1 THEN latest_price END) AS open_price,
            MAX(latest_price) AS high_price,
            MIN(latest_price) AS low_price,
            MAX(CASE WHEN last_rank = 1 THEN latest_price END) AS close_price,
            MAX(CASE WHEN last_rank = 1 THEN pre_close END) AS pre_close,
            MAX(CASE WHEN last_rank = 1 THEN volume END) AS cumulative_volume,
            MAX(CASE WHEN last_rank = 1 THEN amount END) AS cumulative_amount,
            COUNT(*) AS sample_count,
            MIN(quote_time) AS first_quote_time,
            MAX(quote_time) AS last_quote_time
        FROM ranked
        GROUP BY code, trade_date, bucket_start
    ),
    with_previous AS (
        SELECT
            aggregated.*,
            LAG(cumulative_volume) OVER (
                PARTITION BY code, trade_date ORDER BY bucket_start
            ) AS previous_cumulative_volume,
            LAG(cumulative_amount) OVER (
                PARTITION BY code, trade_date ORDER BY bucket_start
            ) AS previous_cumulative_amount
        FROM aggregated
    )
    SELECT
        code,
        source_code,
        name,
        trade_date,
        {interval},
        bucket_start,
        DATE_ADD(bucket_start, INTERVAL {interval} MINUTE),
        open_price,
        high_price,
        low_price,
        close_price,
        pre_close,
        CASE
            WHEN pre_close IS NULL OR pre_close <= 0 THEN NULL
            ELSE (close_price / pre_close - 1) * 100
        END,
        CASE
            WHEN cumulative_volume IS NULL THEN NULL
            ELSE GREATEST(cumulative_volume - COALESCE(previous_cumulative_volume, 0), 0)
        END,
        CASE
            WHEN cumulative_amount IS NULL THEN NULL
            ELSE GREATEST(cumulative_amount - COALESCE(previous_cumulative_amount, 0), 0)
        END,
        cumulative_volume,
        cumulative_amount,
        sample_count,
        first_quote_time,
        last_quote_time,
        'stock_realtime_intraday'
    FROM with_previous
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code), name=VALUES(name), bucket_end=VALUES(bucket_end),
        open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price),
        close_price=VALUES(close_price), pre_close=VALUES(pre_close), pct_chg_close=VALUES(pct_chg_close),
        volume_delta=VALUES(volume_delta), amount_delta=VALUES(amount_delta),
        cumulative_volume=VALUES(cumulative_volume), cumulative_amount=VALUES(cumulative_amount),
        sample_count=VALUES(sample_count), first_quote_time=VALUES(first_quote_time),
        last_quote_time=VALUES(last_quote_time), source=VALUES(source)
    """


def aggregate_trade_date(
    trade_date: str | date | datetime,
    interval: int,
    *,
    source: dict[str, Any] | None = None,
    source_codes: Sequence[str] | None = None,
) -> dict[str, Any]:
    target = normalize_trade_date(trade_date)
    if interval not in ROLLUP_INTERVALS:
        raise ValueError(f"unsupported rollup interval: {interval}")
    ensure_daily_partition("stock_realtime_bar_rollup", target)
    final_source = dict(source or _source_stats(target))
    if not final_source.get("source_fingerprint"):
        final_source.update(_source_revision(target))
    final_codes = list(source_codes) if source_codes is not None else _source_codes(
        target,
        int(final_source.get("source_codes") or 0),
    )
    _start_manifest(target, interval, final_source)
    try:
        code_batches = 0
        for code_batch in _chunks(final_codes):
            with mysql_conn(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        _rollup_sql(interval, len(code_batch)),
                        (target, *code_batch),
                    )
            code_batches += 1
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS rows_count, COUNT(DISTINCT code) AS code_count
                    FROM stock_realtime_bar_rollup
                    WHERE trade_date=%s AND interval_minutes=%s
                    """,
                    (target, interval),
                )
                row = cursor.fetchone() or {}
        rollup_rows = int(row.get("rows_count") or 0)
        rollup_codes = int(row.get("code_count") or 0)
        last_quote = final_source.get("last_quote_minute")
        session_complete = bool(last_quote and last_quote.time() >= datetime.strptime("14:55", "%H:%M").time())
        coverage_complete = (
            int(final_source.get("source_codes") or 0) > 0
            and len(final_codes) == int(final_source.get("source_codes") or 0)
            and rollup_codes == int(final_source.get("source_codes") or 0)
            and rollup_rows > 0
        )
        status = "success" if session_complete and coverage_complete else "partial"
        error_code = None if status == "success" else "incomplete_source_session"
        error_message = (
            None
            if status == "success"
            else "source session has not reached 14:55, source code enumeration differs, or rollup coverage is incomplete"
        )
        _finish_manifest(
            target,
            interval,
            status=status,
            rollup_rows=rollup_rows,
            rollup_codes=rollup_codes,
            error_code=error_code,
            error_message=error_message,
        )
        return {
            "trade_date": target.isoformat(),
            "interval_minutes": interval,
            "status": status,
            **final_source,
            "first_quote_minute": str(final_source["first_quote_minute"]) if final_source["first_quote_minute"] else None,
            "last_quote_minute": str(final_source["last_quote_minute"]) if final_source["last_quote_minute"] else None,
            "rollup_rows": rollup_rows,
            "rollup_codes": rollup_codes,
            "code_batches": code_batches,
            "enumerated_source_codes": len(final_codes),
        }
    except Exception as exc:
        _finish_manifest(
            target,
            interval,
            status="failed",
            rollup_rows=0,
            rollup_codes=0,
            error_code="rollup_failed",
            error_message=f"{type(exc).__name__}: {str(exc)[:400]}",
        )
        raise


def copy_tracked_trade_date(trade_date: str | date | datetime) -> dict[str, Any]:
    target = normalize_trade_date(trade_date)
    ensure_daily_partition("stock_realtime_intraday_tracked", target)
    columns = ", ".join(INTRADAY_COPY_COLUMNS)
    update_columns = [column for column in INTRADAY_COPY_COLUMNS if column not in {"code", "trade_date", "quote_minute", "created_at"}]
    update_sql = ", ".join(f"{column}=VALUES({column})" for column in update_columns)
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO stock_realtime_intraday_tracked ({columns})
                SELECT {', '.join(f'r.{column}' for column in INTRADAY_COPY_COLUMNS)}
                FROM stock_realtime_intraday r
                INNER JOIN (
                    SELECT code FROM portfolio_position WHERE is_active=1
                    UNION
                    SELECT code FROM selection_result
                ) tracked ON tracked.code = r.code
                WHERE r.trade_date=%s
                ON DUPLICATE KEY UPDATE {update_sql}
                """,
                (target,),
            )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS rows_count, COUNT(DISTINCT code) AS code_count
                FROM stock_realtime_intraday_tracked WHERE trade_date=%s
                """,
                (target,),
            )
            row = cursor.fetchone() or (0, 0)
    if isinstance(row, dict):
        rows_count = int(row.get("rows_count") or 0)
        code_count = int(row.get("code_count") or 0)
    else:
        rows_count = int(row[0] or 0)
        code_count = int(row[1] or 0)
    return {"trade_date": target.isoformat(), "rows": rows_count, "codes": code_count}


def _rollup_complete(trade_date: date, intervals: tuple[int, ...]) -> bool:
    placeholders = ",".join(["%s"] * len(intervals))
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM stock_realtime_rollup_manifest
                WHERE trade_date=%s AND interval_minutes IN ({placeholders}) AND status='success'
                """,
                (trade_date, *intervals),
            )
            return int((cursor.fetchone() or {}).get("total") or 0) == len(intervals)


def _delete_trade_date(table_name: str, trade_date: date) -> dict[str, Any]:
    if table_name not in PARTITIONED_TABLES:
        raise ValueError(f"unsupported retention table: {table_name}")
    partition_name = partition_name_for_date(trade_date)
    partitions = table_partitions(table_name)
    names = {str(row.get("partition_name")) for row in partitions if row.get("partition_name")}
    if partition_name in names:
        with mysql_maintenance_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {table_name} DROP PARTITION {partition_name}")
        return {"trade_date": trade_date.isoformat(), "method": "drop_partition", "deleted_rows": None}
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table_name} WHERE trade_date=%s", (trade_date,))
            deleted_rows = int(cursor.rowcount or 0)
    return {"trade_date": trade_date.isoformat(), "method": "bounded_delete", "deleted_rows": deleted_rows}


def apply_retention(policy: RealtimeLifecyclePolicy | None = None) -> dict[str, Any]:
    final_policy = (policy or RealtimeLifecyclePolicy()).validate()
    raw_actions = []
    for target in expired_trade_dates(_date_rows("stock_realtime_intraday"), final_policy.raw_trade_days):
        if _rollup_complete(target, final_policy.intervals):
            raw_actions.append(_delete_trade_date("stock_realtime_intraday", target))
        else:
            raw_actions.append({"trade_date": target.isoformat(), "method": "protected_incomplete_rollup"})

    rollup_actions = [
        _delete_trade_date("stock_realtime_bar_rollup", target)
        for target in expired_trade_dates(_date_rows("stock_realtime_bar_rollup"), final_policy.rollup_trade_days)
    ]
    tracked_actions = [
        _delete_trade_date("stock_realtime_intraday_tracked", target)
        for target in expired_trade_dates(_date_rows("stock_realtime_intraday_tracked"), final_policy.tracked_trade_days)
    ]
    return {"raw": raw_actions, "rollup": rollup_actions, "tracked": tracked_actions}


def run_lifecycle(policy: RealtimeLifecyclePolicy | None = None) -> dict[str, Any]:
    final_policy = (policy or RealtimeLifecyclePolicy()).validate()
    lock_handle = acquire_mysql_advisory_lock(LIFECYCLE_LOCK_NAME)
    if lock_handle is None:
        return {"status": "skipped", "reason": "previous_lifecycle_run_still_running"}
    writer_lock = None
    try:
        writer_lock = acquire_mysql_advisory_lock(WRITER_LOCK_NAME)
        if writer_lock is None:
            return {"status": "skipped", "reason": "realtime_writer_is_active"}

        raw_dates = sorted(_date_rows("stock_realtime_intraday"), reverse=True)
        created_raw_partitions = []
        partition_failures: list[dict[str, Any]] = []
        for target in sorted(raw_dates):
            try:
                if ensure_daily_partition("stock_realtime_intraday", target):
                    created_raw_partitions.append(partition_name_for_date(target))
            except Exception as exc:
                partition_failures.append(
                    {
                        "trade_date": target.isoformat(),
                        "stage": "raw_partition",
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {str(exc)[:400]}",
                    }
                )
                break

        rollups: list[dict[str, Any]] = []
        tracked: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = list(partition_failures)
        for target in raw_dates:
            try:
                revision = _source_revision(target)
                manifests = _manifest_rows_for_date(target)
            except Exception as exc:
                failure = {
                    "trade_date": target.isoformat(),
                    "stage": "source_inspection",
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {str(exc)[:400]}",
                }
                failures.append(failure)
                rollups.extend(
                    {**failure, "interval_minutes": interval}
                    for interval in final_policy.intervals
                )
                try:
                    tracked.append(copy_tracked_trade_date(target))
                except Exception as tracked_exc:
                    tracked_failure = {
                        "trade_date": target.isoformat(),
                        "stage": "tracked_copy",
                        "status": "failed",
                        "error": f"{type(tracked_exc).__name__}: {str(tracked_exc)[:400]}",
                    }
                    failures.append(tracked_failure)
                    tracked.append(tracked_failure)
                continue

            source: dict[str, Any] | None = None
            enumerated_codes: list[str] | None = None
            for interval in final_policy.intervals:
                try:
                    manifest = manifests.get(interval)
                    fingerprint = str(revision["source_fingerprint"])
                    if (
                        manifest
                        and manifest.get("status") in {"success", "partial"}
                        and manifest.get("source_fingerprint") == fingerprint
                        and int(manifest.get("rollup_rows") or 0) > 0
                    ):
                        rollups.append(
                            _skipped_rollup_result(
                                target,
                                interval,
                                manifest,
                                reason="source_fingerprint_unchanged",
                            )
                        )
                        continue

                    if source is None:
                        source = {**_source_stats(target), **revision}
                    if manifest and not manifest.get("source_fingerprint") and _manifest_matches_legacy_source(manifest, source):
                        _set_manifest_source_fingerprint(target, interval, fingerprint)
                        manifest = {**manifest, "source_fingerprint": fingerprint}
                        rollups.append(
                            _skipped_rollup_result(
                                target,
                                interval,
                                manifest,
                                reason="legacy_manifest_fingerprint_backfilled",
                            )
                        )
                        continue

                    if enumerated_codes is None:
                        enumerated_codes = _source_codes(target, int(source.get("source_codes") or 0))
                    rollups.append(
                        aggregate_trade_date(
                            target,
                            interval,
                            source=source,
                            source_codes=enumerated_codes,
                        )
                    )
                except Exception as exc:
                    failure = {
                        "trade_date": target.isoformat(),
                        "stage": f"rollup_{interval}m",
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {str(exc)[:400]}",
                    }
                    failures.append(failure)
                    rollups.append({**failure, "interval_minutes": interval})
            try:
                tracked.append(copy_tracked_trade_date(target))
            except Exception as exc:
                failure = {
                    "trade_date": target.isoformat(),
                    "stage": "tracked_copy",
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {str(exc)[:400]}",
                }
                failures.append(failure)
                tracked.append(failure)

        try:
            retention = apply_retention(final_policy)
        except Exception as exc:
            retention = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {str(exc)[:400]}",
            }
            failures.append({"stage": "retention", **retention})

        try:
            final_plan = build_lifecycle_plan(final_policy)
        except Exception as exc:
            final_plan = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {str(exc)[:400]}",
            }
            failures.append({"stage": "final_plan", **final_plan})

        has_partial = any(item.get("status") == "partial" for item in rollups)
        status = "partial" if failures or has_partial else "success"
        return {
            "status": status,
            "policy": asdict(final_policy),
            "processing_order": [item.isoformat() for item in raw_dates],
            "created_raw_partitions": created_raw_partitions,
            "rollups": rollups,
            "tracked": tracked,
            "failures": failures,
            "retention": retention,
            "final_plan": final_plan,
        }
    finally:
        if writer_lock is not None:
            release_mysql_advisory_lock(writer_lock)
        release_mysql_advisory_lock(lock_handle)


def migrate_intraday_to_partitions() -> dict[str, Any]:
    if is_partitioned("stock_realtime_intraday"):
        return {"status": "skipped", "reason": "already_partitioned"}

    writer_lock = acquire_mysql_advisory_lock(WRITER_LOCK_NAME)
    if writer_lock is None:
        return {"status": "skipped", "reason": "realtime_writer_is_active"}
    lifecycle_lock = acquire_mysql_advisory_lock(LIFECYCLE_LOCK_NAME)
    if lifecycle_lock is None:
        release_mysql_advisory_lock(writer_lock)
        return {"status": "skipped", "reason": "realtime_lifecycle_is_active"}

    shadow = "stock_realtime_intraday_partitioned"
    backup = f"stock_realtime_intraday_legacy_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        source_summary = _table_summary("stock_realtime_intraday")
        source_dates = sorted(_date_rows("stock_realtime_intraday"))
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {shadow}")
                cursor.execute(intraday_table_ddl(shadow, partitioned=True))
        for target in source_dates:
            ensure_daily_partition(shadow, target)

        columns = ", ".join(("id", *INTRADAY_COPY_COLUMNS))
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {shadow} ({columns}) SELECT {columns} FROM stock_realtime_intraday"
                )
        shadow_summary = _table_summary(shadow)
        if source_summary["rows"] != shadow_summary["rows"]:
            raise RuntimeError(
                f"partition migration row mismatch: source={source_summary['rows']} shadow={shadow_summary['rows']}"
            )
        if source_summary["trade_days"] != shadow_summary["trade_days"]:
            raise RuntimeError("partition migration trade-day mismatch")

        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    RENAME TABLE
                        stock_realtime_intraday TO {backup},
                        {shadow} TO stock_realtime_intraday
                    """
                )
        final_summary = _table_summary("stock_realtime_intraday")
        return {
            "status": "success",
            "backup_table": backup,
            "source": source_summary,
            "final": final_summary,
            "partitions": table_partitions("stock_realtime_intraday"),
        }
    finally:
        release_mysql_advisory_lock(lifecycle_lock)
        release_mysql_advisory_lock(writer_lock)
