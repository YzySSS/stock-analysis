from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Callable

from app.data_ingestion.market_opinion_repository import (
    delete_snapshot_payloads,
    insert_normalized_payload_values,
    normalized_payload_values,
)
from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


MARKET_OPINION_LOCK_NAME = "market_opinion_update_lock"


@dataclass(frozen=True)
class MarketOpinionLifecyclePolicy:
    intraday_trade_days: int = 5
    daily_trade_days: int = 90
    batch_size: int = 25

    def validate(self) -> "MarketOpinionLifecyclePolicy":
        if self.intraday_trade_days < 1:
            raise ValueError("intraday snapshot retention must keep at least one trade day")
        if self.daily_trade_days < self.intraday_trade_days:
            raise ValueError("daily retention must not be shorter than intraday retention")
        if not 1 <= self.batch_size <= 500:
            raise ValueError("batch size must be between 1 and 500")
        return self


def _snapshot_index() -> list[dict[str, Any]]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, trade_date, as_of_datetime, payload_version
                FROM sector_opinion_daily
                ORDER BY trade_date DESC, as_of_datetime DESC, id DESC
                """
            )
            return cursor.fetchall() or []


def retention_snapshot_ids(
    rows: list[dict[str, Any]],
    policy: MarketOpinionLifecyclePolicy,
) -> tuple[list[int], list[int], list[date]]:
    final_policy = policy.validate()
    trade_dates = sorted(
        {row["trade_date"] for row in rows if row.get("trade_date")},
        reverse=True,
    )
    retained_dates = trade_dates[: final_policy.daily_trade_days]
    intraday_dates = set(trade_dates[: final_policy.intraday_trade_days])
    retained_date_set = set(retained_dates)
    latest_as_of = {}
    for row in rows:
        trade_date = row.get("trade_date")
        as_of = row.get("as_of_datetime")
        if trade_date not in retained_date_set or trade_date in intraday_dates:
            continue
        if trade_date not in latest_as_of or as_of > latest_as_of[trade_date]:
            latest_as_of[trade_date] = as_of

    keep_ids = []
    prune_ids = []
    for row in rows:
        row_id = int(row["id"])
        trade_date = row.get("trade_date")
        keep = trade_date in intraday_dates or (
            trade_date in retained_date_set
            and row.get("as_of_datetime") == latest_as_of.get(trade_date)
        )
        (keep_ids if keep else prune_ids).append(row_id)
    return keep_ids, prune_ids, retained_dates


def build_market_opinion_lifecycle_plan(
    policy: MarketOpinionLifecyclePolicy | None = None,
) -> dict[str, Any]:
    final_policy = (policy or MarketOpinionLifecyclePolicy()).validate()
    rows = _snapshot_index()
    keep_ids, prune_ids, retained_dates = retention_snapshot_ids(rows, final_policy)
    keep_set = set(keep_ids)
    pending_normalization = sum(
        1
        for row in rows
        if int(row["id"]) in keep_set
        and int(row.get("payload_version") or 1) < 2
    )
    return {
        "policy": asdict(final_policy),
        "current_rows": len(rows),
        "current_trade_days": len({row.get("trade_date") for row in rows if row.get("trade_date")}),
        "retained_rows": len(keep_ids),
        "prunable_rows": len(prune_ids),
        "pending_normalization_rows": pending_normalization,
        "retained_trade_dates": [str(item) for item in retained_dates],
        "latest_trade_date": str(retained_dates[0]) if retained_dates else None,
    }


def _chunks(items: list[int], size: int):
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def normalize_retained_snapshots(
    keep_ids: list[int],
    *,
    batch_size: int,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, int]:
    totals = {"snapshots": 0, "stocks": 0, "news_refs": 0, "sources": 0}
    for batch_no, batch_ids in enumerate(_chunks(sorted(keep_ids), batch_size), start=1):
        placeholders = ",".join(["%s"] * len(batch_ids))
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT id, top_stocks_json, top_news_json, source_json, payload_version
                    FROM sector_opinion_daily
                    WHERE id IN ({placeholders})
                      AND (
                        payload_version < 2
                        OR top_stocks_json IS NOT NULL
                        OR top_news_json IS NOT NULL
                        OR source_json IS NOT NULL
                      )
                    ORDER BY id
                    """,
                    batch_ids,
                )
                rows = cursor.fetchall() or []
                if not rows:
                    continue
                normalized_ids = [int(row[0]) for row in rows]
                delete_snapshot_payloads(cursor, normalized_ids)
                stock_values: list[tuple[Any, ...]] = []
                news_values: list[tuple[Any, ...]] = []
                source_values: list[tuple[Any, ...]] = []
                for row in rows:
                    snapshot_id = int(row[0])
                    summary = {
                        "top_stocks_json": row[1],
                        "top_news_json": row[2],
                        "source_json": row[3],
                    }
                    stocks, news, sources = normalized_payload_values(snapshot_id, summary)
                    stock_values.extend(stocks)
                    news_values.extend(news)
                    source_values.extend(sources)
                insert_normalized_payload_values(cursor, stock_values, news_values, source_values)
                cursor.execute(
                    f"""
                    UPDATE sector_opinion_daily
                    SET top_stocks_json=NULL, top_news_json=NULL, source_json=NULL,
                        payload_version=2, payload_migrated_at=NOW()
                    WHERE id IN ({placeholders})
                    """,
                    normalized_ids,
                )
        totals["snapshots"] += len(rows)
        totals["stocks"] += len(stock_values)
        totals["news_refs"] += len(news_values)
        totals["sources"] += len(source_values)
        if progress:
            progress({"phase": "normalize", "batch": batch_no, **totals})
    return totals


def prune_snapshots(
    prune_ids: list[int],
    *,
    batch_size: int,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, int]:
    deleted = 0
    for batch_no, batch_ids in enumerate(_chunks(sorted(prune_ids), max(batch_size, 100)), start=1):
        placeholders = ",".join(["%s"] * len(batch_ids))
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                delete_snapshot_payloads(cursor, batch_ids)
                cursor.execute(
                    f"DELETE FROM sector_opinion_daily WHERE id IN ({placeholders})",
                    batch_ids,
                )
                deleted += int(cursor.rowcount or 0)
        if progress:
            progress({"phase": "prune", "batch": batch_no, "deleted_snapshots": deleted})
    return {"deleted_snapshots": deleted}


def _validate_retained_payloads(keep_ids: list[int]) -> dict[str, Any]:
    if not keep_ids:
        return {"retained_rows": 0, "normalized_rows": 0, "legacy_json_rows": 0}
    normalized = 0
    legacy_json = 0
    for batch_ids in _chunks(sorted(keep_ids), 1000):
        placeholders = ",".join(["%s"] * len(batch_ids))
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) AS retained_rows,
                           SUM(payload_version >= 2) AS normalized_rows,
                           SUM(top_stocks_json IS NOT NULL OR top_news_json IS NOT NULL OR source_json IS NOT NULL) AS legacy_json_rows
                    FROM sector_opinion_daily
                    WHERE id IN ({placeholders})
                    """,
                    batch_ids,
                )
                row = cursor.fetchone() or {}
                normalized += int(row.get("normalized_rows") or 0)
                legacy_json += int(row.get("legacy_json_rows") or 0)
    return {
        "retained_rows": len(keep_ids),
        "normalized_rows": normalized,
        "legacy_json_rows": legacy_json,
        "complete": normalized == len(keep_ids) and legacy_json == 0,
    }


def run_market_opinion_lifecycle(
    policy: MarketOpinionLifecyclePolicy | None = None,
    *,
    normalize: bool = True,
    prune: bool = True,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    final_policy = (policy or MarketOpinionLifecyclePolicy()).validate()
    lock_handle = acquire_mysql_advisory_lock(MARKET_OPINION_LOCK_NAME)
    if lock_handle is None:
        return {"status": "skipped", "reason": "market_opinion_update_is_active"}
    try:
        rows = _snapshot_index()
        keep_ids, prune_ids, retained_dates = retention_snapshot_ids(rows, final_policy)
        normalization = (
            normalize_retained_snapshots(
                keep_ids,
                batch_size=final_policy.batch_size,
                progress=progress,
            )
            if normalize
            else {"snapshots": 0, "stocks": 0, "news_refs": 0, "sources": 0}
        )
        validation = _validate_retained_payloads(keep_ids)
        if prune and not validation.get("complete"):
            raise RuntimeError("retained sector opinion snapshots are not fully normalized; pruning refused")
        pruning = (
            prune_snapshots(prune_ids, batch_size=final_policy.batch_size, progress=progress)
            if prune
            else {"deleted_snapshots": 0}
        )
        final_plan = build_market_opinion_lifecycle_plan(final_policy)
        return {
            "status": "success",
            "policy": asdict(final_policy),
            "retained_trade_dates": [str(item) for item in retained_dates],
            "normalization": normalization,
            "validation": validation,
            "pruning": pruning,
            "final_plan": final_plan,
        }
    finally:
        release_mysql_advisory_lock(lock_handle)
