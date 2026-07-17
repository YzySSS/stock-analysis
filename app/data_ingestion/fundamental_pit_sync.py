from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from typing import Any, Callable, Iterable, Sequence

import tushare as ts
from dotenv import load_dotenv

from app.shared.db import mysql_conn


load_dotenv()

SOURCE = "tushare_fina_indicator_vip"
PAGE_SIZE = 5000
QUARTER_ENDS = ("0331", "0630", "0930", "1231")
FIELDS = (
    "ts_code,ann_date,end_date,update_flag,roe,roa,grossprofit_margin,"
    "profit_to_gr,or_yoy,profit_yoy,netprofit_yoy,q_netprofit_yoy,eps"
)


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


def _first_non_null(*values: Any) -> float | None:
    for value in values:
        normalized = _to_float(value)
        if normalized is not None:
            return normalized
    return None


def quarter_end_periods(start_period: str, end_period: str) -> list[str]:
    start = str(start_period).replace("-", "")
    end = str(end_period).replace("-", "")
    if len(start) != 8 or len(end) != 8 or not start.isdigit() or not end.isdigit():
        raise ValueError("periods must use YYYYMMDD or YYYY-MM-DD")
    if start[4:] not in QUARTER_ENDS or end[4:] not in QUARTER_ENDS:
        raise ValueError("periods must be calendar quarter ends")
    if start > end:
        raise ValueError("start_period must not be after end_period")
    periods = [
        f"{year}{suffix}"
        for year in range(int(start[:4]), int(end[:4]) + 1)
        for suffix in QUARTER_ENDS
    ]
    return [period for period in periods if start <= period <= end]


class FundamentalPitSync:
    """Store financial-indicator versions with their source announcement dates."""

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

    def successful_periods(self) -> set[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT period_end_date
                    FROM fundamental_pit_manifest
                    WHERE status='success'
                    """
                )
                return {
                    str(row["period_end_date"]).replace("-", "")
                    for row in (cursor.fetchall() or [])
                }

    def _manifest(
        self,
        period: str,
        status: str,
        run_id: str,
        *,
        source_rows: int = 0,
        matched_rows: int = 0,
        distinct_codes: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now()
        period_date = normalize_date(period)
        finished_at = None if status == "running" else now
        sql = """
        INSERT INTO fundamental_pit_manifest (
            period_end_date, status, source, source_rows, matched_rows,
            distinct_codes, sync_run_id, started_at, finished_at, metadata_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            status=VALUES(status), source=VALUES(source),
            source_rows=VALUES(source_rows), matched_rows=VALUES(matched_rows),
            distinct_codes=VALUES(distinct_codes), sync_run_id=VALUES(sync_run_id),
            started_at=CASE WHEN VALUES(status)='running' THEN VALUES(started_at) ELSE started_at END,
            finished_at=VALUES(finished_at), metadata_json=VALUES(metadata_json)
        """
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        period_date,
                        status,
                        SOURCE,
                        int(source_rows),
                        int(matched_rows),
                        int(distinct_codes),
                        run_id,
                        now,
                        finished_at,
                        json.dumps(metadata or {}, ensure_ascii=False, default=str),
                    ),
                )

    def _fetch_period(self, period: str, page_size: int) -> tuple[list[dict[str, Any]], int]:
        rows: list[dict[str, Any]] = []
        pages = 0
        for offset in range(0, page_size * 100, page_size):
            frame = self.pro.fina_indicator_vip(
                period=period,
                fields=FIELDS,
                limit=page_size,
                offset=offset,
            )
            page = [] if frame is None or frame.empty else frame.to_dict("records")
            rows.extend(page)
            pages += 1
            if len(page) < page_size:
                return rows, pages
        raise RuntimeError("fina_indicator_vip pagination exceeded safety limit")

    @staticmethod
    def _normalize_rows(
        records: Sequence[dict[str, Any]],
        lifecycle_map: dict[str, str],
        run_id: str,
        synced_at: str,
    ) -> tuple[list[tuple[Any, ...]], dict[str, int]]:
        rows_by_key: dict[tuple[str, str, str, str], tuple[Any, ...]] = {}
        skipped_outside_universe = 0
        skipped_invalid = 0
        valid_field_rows = 0
        for row in records:
            ts_code = (_clean_text(row.get("ts_code")) or "").upper()
            code = lifecycle_map.get(ts_code)
            if not code:
                skipped_outside_universe += 1
                continue
            announcement_date = normalize_date(row.get("ann_date"))
            period_end_date = normalize_date(row.get("end_date"))
            if not announcement_date or not period_end_date:
                skipped_invalid += 1
                continue
            values = (
                _to_float(row.get("roe")),
                _to_float(row.get("roa")),
                _to_float(row.get("grossprofit_margin")),
                _to_float(row.get("profit_to_gr")),
                _to_float(row.get("or_yoy")),
                _first_non_null(
                    row.get("profit_yoy"),
                    row.get("netprofit_yoy"),
                    row.get("q_netprofit_yoy"),
                ),
                _to_float(row.get("eps")),
            )
            if any(value is not None for value in values):
                valid_field_rows += 1
            update_flag = _clean_text(row.get("update_flag")) or ""
            normalized = (
                code,
                ts_code,
                announcement_date,
                period_end_date,
                update_flag,
                *values,
                SOURCE,
                run_id,
                synced_at,
            )
            rows_by_key[(code, period_end_date, announcement_date, update_flag)] = normalized
        return list(rows_by_key.values()), {
            "skipped_outside_universe": skipped_outside_universe,
            "skipped_invalid": skipped_invalid,
            "valid_field_rows": valid_field_rows,
        }

    def _existing_period_codes(self, period: str) -> int:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT code) AS codes
                    FROM stock_fundamental_pit
                    WHERE period_end_date=%s
                    """,
                    (normalize_date(period),),
                )
                return int((cursor.fetchone() or {}).get("codes") or 0)

    def sync_period(
        self,
        period: str,
        run_id: str,
        lifecycle_map: dict[str, str],
        *,
        page_size: int = PAGE_SIZE,
    ) -> dict[str, Any]:
        self._manifest(period, "running", run_id)
        try:
            source_records, pages = self._fetch_period(period, page_size)
            normalized_rows, normalization = self._normalize_rows(
                source_records,
                lifecycle_map,
                run_id,
                self._now(),
            )
            distinct_codes = len({str(row[0]) for row in normalized_rows})
            existing_codes = self._existing_period_codes(period)
            period_date = date.fromisoformat(str(normalize_date(period)))
            mature_period = (date.today() - period_date).days >= 90
            coverage_floor = (
                max(1, int(existing_codes * 0.8))
                if existing_codes
                else max(1, int(len(lifecycle_map) * 0.5))
                if mature_period
                else 1
            )
            if distinct_codes < coverage_floor:
                payload = {
                    "period": period,
                    "pages": pages,
                    "source_rows": len(source_records),
                    "matched_rows": len(normalized_rows),
                    "distinct_codes": distinct_codes,
                    "existing_codes": existing_codes,
                    "coverage_floor": coverage_floor,
                    "mature_period": mature_period,
                    **normalization,
                    "reason": "source coverage below guarded floor",
                }
                self._manifest(
                    period,
                    "partial_success",
                    run_id,
                    source_rows=len(source_records),
                    matched_rows=len(normalized_rows),
                    distinct_codes=distinct_codes,
                    metadata=payload,
                )
                return {**payload, "status": "partial_success"}

            sql = """
            INSERT INTO stock_fundamental_pit (
                code, ts_code, announcement_date, period_end_date, update_flag,
                roe, roa, grossprofit_margin, netprofit_margin, revenue_yoy,
                profit_yoy, eps, source, source_sync_id, source_updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                ts_code=VALUES(ts_code), roe=VALUES(roe), roa=VALUES(roa),
                grossprofit_margin=VALUES(grossprofit_margin),
                netprofit_margin=VALUES(netprofit_margin),
                revenue_yoy=VALUES(revenue_yoy), profit_yoy=VALUES(profit_yoy),
                eps=VALUES(eps), source=VALUES(source),
                source_sync_id=VALUES(source_sync_id),
                source_updated_at=VALUES(source_updated_at)
            """
            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    for offset in range(0, len(normalized_rows), 1000):
                        cursor.executemany(sql, normalized_rows[offset : offset + 1000])

            payload = {
                "period": period,
                "pages": pages,
                "source_rows": len(source_records),
                "matched_rows": len(normalized_rows),
                "distinct_codes": distinct_codes,
                "existing_codes": existing_codes,
                "coverage_floor": coverage_floor,
                "mature_period": mature_period,
                **normalization,
            }
            self._manifest(
                period,
                "success",
                run_id,
                source_rows=len(source_records),
                matched_rows=len(normalized_rows),
                distinct_codes=distinct_codes,
                metadata=payload,
            )
            return {**payload, "status": "success"}
        except Exception as exc:
            self._manifest(
                period,
                "failed",
                run_id,
                metadata={"error": f"{type(exc).__name__}: {str(exc)[:500]}"},
            )
            return {
                "period": period,
                "status": "failed",
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            }

    def run(
        self,
        run_id: str,
        periods: Iterable[str],
        *,
        pending_only: bool = False,
        pause_seconds: float = 0.0,
        page_size: int = PAGE_SIZE,
    ) -> dict[str, Any]:
        requested_periods = list(dict.fromkeys(str(period).replace("-", "") for period in periods))
        successful = self.successful_periods() if pending_only else set()
        selected_periods = [period for period in requested_periods if period not in successful]
        lifecycle_map = self.fetch_lifecycle_map()
        results: list[dict[str, Any]] = []
        for period in selected_periods:
            results.append(
                self.sync_period(
                    period,
                    run_id,
                    lifecycle_map,
                    page_size=page_size,
                )
            )
            if pause_seconds > 0:
                time.sleep(pause_seconds)
        return {
            "run_id": run_id,
            "requested_periods": requested_periods,
            "selected_periods": selected_periods,
            "skipped_successful_periods": sorted(set(requested_periods) - set(selected_periods)),
            "lifecycle_codes": len(lifecycle_map),
            "success_periods": [row["period"] for row in results if row.get("status") == "success"],
            "partial_periods": [row for row in results if row.get("status") == "partial_success"],
            "failed_periods": [row for row in results if row.get("status") == "failed"],
            "source_rows": sum(int(row.get("source_rows") or 0) for row in results),
            "matched_rows": sum(int(row.get("matched_rows") or 0) for row in results),
            "results": results,
        }
