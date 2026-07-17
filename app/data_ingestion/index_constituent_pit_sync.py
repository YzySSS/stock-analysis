from __future__ import annotations

import calendar
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Sequence

import tushare as ts
from dotenv import load_dotenv

from app.shared.db import mysql_conn
from app.shared.index_universe import (
    INDEX_UNIVERSE_DEFINITIONS,
    expected_index_members,
    index_member_guard_range,
    normalize_backtest_universe,
)


load_dotenv()

SOURCE = "tushare_index_weight"
FIELDS = "index_code,con_code,trade_date,weight"


def _clean_text(value: Any) -> str | None:
    if value is None or value != value:
        return None
    text = str(value).strip()
    return None if not text or text.lower() in {"nan", "none", "null"} else text


def normalize_date(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    compact = text.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:]}"
    return text[:10]


def _to_float(value: Any) -> float | None:
    if value is None or value != value:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_month(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 8:
        text = text[:6]
    if len(text) != 6 or not text.isdigit() or not 1 <= int(text[4:]) <= 12:
        raise ValueError("months must use YYYYMM, YYYY-MM or a date inside the month")
    return text


def month_periods(start_month: Any, end_month: Any) -> list[str]:
    start = normalize_month(start_month)
    end = normalize_month(end_month)
    if start > end:
        raise ValueError("start_month must not be after end_month")
    year, month = int(start[:4]), int(start[4:])
    periods: list[str] = []
    while f"{year:04d}{month:02d}" <= end:
        periods.append(f"{year:04d}{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return periods


def month_bounds(month: Any) -> tuple[str, str]:
    normalized = normalize_month(month)
    year, month_number = int(normalized[:4]), int(normalized[4:])
    last_day = calendar.monthrange(year, month_number)[1]
    return f"{normalized}01", f"{normalized}{last_day:02d}"


class IndexConstituentPitSync:
    """Persist monthly historical index-weight snapshots without current-state fallback."""

    def __init__(
        self,
        token: str | None = None,
        *,
        pro: Any | None = None,
        connection_factory: Callable[..., Any] = mysql_conn,
    ) -> None:
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if pro is None and not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = pro or ts.pro_api(self.token)
        self._connection_factory = connection_factory

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def fetch_lifecycle_map(self) -> dict[str, str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT ts_code, code
                    FROM stock_instrument_lifecycle
                    WHERE instrument_type='stock'
                    """
                )
                return {
                    str(row["ts_code"]).upper(): str(row["code"])
                    for row in (cursor.fetchall() or [])
                }

    def successful_partitions(self) -> set[tuple[str, str]]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT index_code, period_month
                    FROM index_constituent_pit_manifest
                    WHERE status='success'
                    """
                )
                return {
                    (
                        str(row["index_code"]).upper(),
                        str(row["period_month"]).replace("-", "")[:6],
                    )
                    for row in (cursor.fetchall() or [])
                }

    def _manifest(
        self,
        index_code: str,
        month: str,
        status: str,
        run_id: str,
        *,
        snapshot_date: str | None = None,
        snapshot_count: int = 0,
        source_rows: int = 0,
        matched_rows: int = 0,
        distinct_codes: int = 0,
        expected_members: int = 0,
        weight_sum: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now()
        period_month = f"{normalize_month(month)[:4]}-{normalize_month(month)[4:]}-01"
        finished_at = None if status == "running" else now
        sql = """
        INSERT INTO index_constituent_pit_manifest (
            index_code, period_month, status, source, snapshot_date,
            snapshot_count, source_rows, matched_rows, distinct_codes,
            expected_members, weight_sum, sync_run_id, started_at,
            finished_at, metadata_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            status=VALUES(status), source=VALUES(source),
            snapshot_date=VALUES(snapshot_date), snapshot_count=VALUES(snapshot_count),
            source_rows=VALUES(source_rows), matched_rows=VALUES(matched_rows),
            distinct_codes=VALUES(distinct_codes), expected_members=VALUES(expected_members),
            weight_sum=VALUES(weight_sum), sync_run_id=VALUES(sync_run_id),
            started_at=CASE WHEN VALUES(status)='running' THEN VALUES(started_at) ELSE started_at END,
            finished_at=VALUES(finished_at), metadata_json=VALUES(metadata_json)
        """
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        index_code,
                        period_month,
                        status,
                        SOURCE,
                        snapshot_date,
                        int(snapshot_count),
                        int(source_rows),
                        int(matched_rows),
                        int(distinct_codes),
                        int(expected_members),
                        weight_sum,
                        run_id,
                        now,
                        finished_at,
                        json.dumps(metadata or {}, ensure_ascii=False, default=str),
                    ),
                )

    def _fetch_month(self, index_code: str, month: str) -> list[dict[str, Any]]:
        start_date, end_date = month_bounds(month)
        frame = self.pro.index_weight(
            index_code=index_code,
            start_date=start_date,
            end_date=end_date,
            fields=FIELDS,
        )
        return [] if frame is None or frame.empty else frame.to_dict("records")

    @staticmethod
    def _normalize_rows(
        records: Sequence[dict[str, Any]],
        *,
        requested_index_code: str,
        requested_month: str,
        lifecycle_map: dict[str, str],
        run_id: str,
        synced_at: str,
    ) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
        normalized_month = normalize_month(requested_month)
        rows_by_key: dict[tuple[str, str], tuple[Any, ...]] = {}
        source_by_date: dict[str, set[str]] = defaultdict(set)
        unmatched_by_date: dict[str, set[str]] = defaultdict(set)
        invalid_rows = 0
        for row in records:
            index_code = (_clean_text(row.get("index_code")) or requested_index_code).upper()
            ts_code = (_clean_text(row.get("con_code")) or "").upper()
            effective_date = normalize_date(row.get("trade_date"))
            if (
                index_code != requested_index_code
                or not ts_code
                or not effective_date
                or effective_date.replace("-", "")[:6] != normalized_month
            ):
                invalid_rows += 1
                continue
            source_by_date[effective_date].add(ts_code)
            code = lifecycle_map.get(ts_code)
            if not code:
                unmatched_by_date[effective_date].add(ts_code)
                continue
            rows_by_key[(effective_date, code)] = (
                index_code,
                code,
                ts_code,
                effective_date,
                _to_float(row.get("weight")),
                SOURCE,
                run_id,
                synced_at,
            )

        normalized_rows = list(rows_by_key.values())
        matched_by_date: dict[str, set[str]] = defaultdict(set)
        weight_by_date: dict[str, float] = defaultdict(float)
        for row in normalized_rows:
            matched_by_date[str(row[3])].add(str(row[1]))
            if row[4] is not None:
                weight_by_date[str(row[3])] += float(row[4])
        snapshot_dates = sorted(source_by_date)
        snapshot_metrics = [
            {
                "effective_date": snapshot_date,
                "source_codes": len(source_by_date[snapshot_date]),
                "matched_codes": len(matched_by_date[snapshot_date]),
                "unmatched_codes": len(unmatched_by_date[snapshot_date]),
                "weight_sum": round(weight_by_date[snapshot_date], 8),
            }
            for snapshot_date in snapshot_dates
        ]
        return normalized_rows, {
            "invalid_rows": invalid_rows,
            "snapshot_dates": snapshot_dates,
            "snapshot_metrics": snapshot_metrics,
            "unmatched_codes": sorted(
                {code for codes in unmatched_by_date.values() for code in codes}
            )[:20],
        }

    @staticmethod
    def _validation_errors(snapshot_metrics: Sequence[dict[str, Any]], expected: int) -> list[str]:
        errors: list[str] = []
        minimum_members, maximum_members = index_member_guard_range(expected)
        if not snapshot_metrics:
            return ["source returned no monthly snapshot"]
        for item in snapshot_metrics:
            date_value = item.get("effective_date")
            source_codes = int(item.get("source_codes") or 0)
            matched_codes = int(item.get("matched_codes") or 0)
            weight_sum = float(item.get("weight_sum") or 0)
            if not minimum_members <= source_codes <= maximum_members:
                errors.append(
                    f"{date_value} source members {source_codes} outside {minimum_members}-{maximum_members}"
                )
            if matched_codes < max(minimum_members, int(source_codes * 0.98)):
                errors.append(
                    f"{date_value} matched members {matched_codes} below guarded floor"
                )
            if not 95.0 <= weight_sum <= 105.0:
                errors.append(f"{date_value} weight sum {weight_sum:.6f} outside 95-105")
        return errors

    def sync_month(
        self,
        index_code: str,
        month: str,
        run_id: str,
        lifecycle_map: dict[str, str],
    ) -> dict[str, Any]:
        index_code = normalize_backtest_universe(index_code)
        if index_code not in INDEX_UNIVERSE_DEFINITIONS:
            raise ValueError("index PIT sync only supports configured index universes")
        month = normalize_month(month)
        expected = expected_index_members(index_code)
        self._manifest(index_code, month, "running", run_id, expected_members=expected)
        try:
            source_records = self._fetch_month(index_code, month)
            normalized_rows, normalization = self._normalize_rows(
                source_records,
                requested_index_code=index_code,
                requested_month=month,
                lifecycle_map=lifecycle_map,
                run_id=run_id,
                synced_at=self._now(),
            )
            snapshot_metrics = normalization["snapshot_metrics"]
            errors = self._validation_errors(snapshot_metrics, expected)
            latest_metrics = snapshot_metrics[-1] if snapshot_metrics else {}
            payload = {
                "index_code": index_code,
                "month": month,
                "source_rows": len(source_records),
                "matched_rows": len(normalized_rows),
                "expected_members": expected,
                **normalization,
                "validation_errors": errors,
            }
            if errors:
                self._manifest(
                    index_code,
                    month,
                    "partial_success",
                    run_id,
                    snapshot_date=latest_metrics.get("effective_date"),
                    snapshot_count=len(snapshot_metrics),
                    source_rows=len(source_records),
                    matched_rows=len(normalized_rows),
                    distinct_codes=int(latest_metrics.get("matched_codes") or 0),
                    expected_members=expected,
                    weight_sum=latest_metrics.get("weight_sum"),
                    metadata=payload,
                )
                return {**payload, "status": "partial_success"}

            insert_sql = """
            INSERT INTO index_constituent_pit (
                index_code, code, constituent_ts_code, effective_date, weight,
                source, source_sync_id, source_updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                constituent_ts_code=VALUES(constituent_ts_code),
                weight=VALUES(weight), source=VALUES(source),
                source_sync_id=VALUES(source_sync_id),
                source_updated_at=VALUES(source_updated_at)
            """
            snapshot_dates = normalization["snapshot_dates"]
            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    for snapshot_date in snapshot_dates:
                        cursor.execute(
                            "DELETE FROM index_constituent_pit WHERE index_code=%s AND effective_date=%s",
                            (index_code, snapshot_date),
                        )
                    for offset in range(0, len(normalized_rows), 1000):
                        cursor.executemany(insert_sql, normalized_rows[offset : offset + 1000])

            self._manifest(
                index_code,
                month,
                "success",
                run_id,
                snapshot_date=latest_metrics.get("effective_date"),
                snapshot_count=len(snapshot_metrics),
                source_rows=len(source_records),
                matched_rows=len(normalized_rows),
                distinct_codes=int(latest_metrics.get("matched_codes") or 0),
                expected_members=expected,
                weight_sum=latest_metrics.get("weight_sum"),
                metadata=payload,
            )
            return {**payload, "status": "success"}
        except Exception as exc:
            payload = {
                "index_code": index_code,
                "month": month,
                "expected_members": expected,
                "error": f"{type(exc).__name__}: {str(exc)[:800]}",
            }
            self._manifest(
                index_code,
                month,
                "failed",
                run_id,
                expected_members=expected,
                metadata=payload,
            )
            return {**payload, "status": "failed"}

    def run(
        self,
        run_id: str,
        *,
        index_codes: Sequence[str],
        months: Sequence[str],
        pending_only: bool = False,
        pause_seconds: float = 0.2,
    ) -> dict[str, Any]:
        lifecycle_map = self.fetch_lifecycle_map()
        successful = self.successful_partitions() if pending_only else set()
        results: list[dict[str, Any]] = []
        skipped: list[str] = []
        for index_code_value in index_codes:
            index_code = normalize_backtest_universe(index_code_value)
            if index_code not in INDEX_UNIVERSE_DEFINITIONS:
                raise ValueError("ALL_A cannot be synchronized as an index constituent universe")
            for month_value in months:
                month = normalize_month(month_value)
                partition_key = (index_code, month)
                if partition_key in successful:
                    skipped.append(f"{index_code}:{month}")
                    continue
                results.append(self.sync_month(index_code, month, run_id, lifecycle_map))
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
        return {
            "run_id": run_id,
            "index_codes": list(index_codes),
            "months": list(months),
            "results": results,
            "success_partitions": [
                f"{item['index_code']}:{item['month']}"
                for item in results
                if item.get("status") == "success"
            ],
            "partial_partitions": [
                f"{item['index_code']}:{item['month']}"
                for item in results
                if item.get("status") == "partial_success"
            ],
            "failed_partitions": [
                f"{item['index_code']}:{item['month']}"
                for item in results
                if item.get("status") == "failed"
            ],
            "skipped_partitions": skipped,
        }
