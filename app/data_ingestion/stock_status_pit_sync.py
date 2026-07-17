from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Sequence

import tushare as ts
from dotenv import load_dotenv

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.data_ingestion.factor_input_history_sync import FactorInputDailyRecord, FactorInputHistorySync
from app.shared.db import mysql_conn


load_dotenv()

LIFECYCLE_SOURCE = "tushare_stock_basic"
NAME_HISTORY_SOURCE = "tushare_namechange"
SUSPENSION_SOURCE = "tushare_suspend_d"
MARKET_DATA_SOURCE = "tushare_daily_basic"
NAME_PAGE_SIZE = 5000
NAME_FALLBACK_CODE_LIMIT = 500
SUSPENSION_PAGE_SIZE = 5000


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


def from_ts_code(ts_code: str) -> str:
    symbol, suffix = ts_code.split(".", 1) if "." in ts_code else (ts_code, "")
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix.upper(), suffix.lower())
    return f"{prefix}.{symbol}" if prefix else symbol


def classify_name(name: Any, change_reason: Any = None) -> dict[str, int]:
    normalized_name = (_clean_text(name) or "").upper()
    normalized_reason = (_clean_text(change_reason) or "").upper()
    return {
        "is_st": int("ST" in normalized_name or normalized_reason in {"ST", "*ST"}),
        "is_delisting_period": int("退" in normalized_name or "退市整理" in normalized_reason),
    }


def _to_float(value: Any) -> float | None:
    if value is None or value != value:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_historical_market_rows(
    kline_rows: int,
    factor_rows: int,
    *,
    no_activity_confirmed: bool = False,
) -> str:
    if kline_rows > 0 and factor_rows > 0:
        return "complete"
    if kline_rows == 0 and factor_rows == 0 and no_activity_confirmed:
        return "source_confirmed_no_market_activity"
    return "incomplete"


class StockStatusPitSync:
    """Synchronize point-in-time stock lifecycle, names, suspensions and market history.

    The normalized truth layer is deliberately separate from ``stock_basic``:
    current-universe syncs may continue to keep only live instruments, while
    backtests can resolve the historical universe without reactivating delisted
    securities in production selection.
    """

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

    def _manifest(
        self,
        dataset: str,
        partition_key: str,
        status: str,
        source: str,
        run_id: str,
        *,
        source_rows: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now()
        finished_at = None if status == "running" else now
        sql = """
        INSERT INTO stock_status_pit_manifest (
            dataset, partition_key, status, source, source_rows, sync_run_id,
            started_at, finished_at, metadata_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            status=VALUES(status), source=VALUES(source), source_rows=VALUES(source_rows),
            sync_run_id=VALUES(sync_run_id),
            started_at=CASE WHEN VALUES(status)='running' THEN VALUES(started_at) ELSE started_at END,
            finished_at=VALUES(finished_at), metadata_json=VALUES(metadata_json)
        """
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        dataset,
                        partition_key,
                        status,
                        source,
                        int(source_rows),
                        run_id,
                        now,
                        finished_at,
                        json.dumps(metadata or {}, ensure_ascii=False, default=str),
                    ),
                )

    def sync_lifecycle(self, run_id: str) -> dict[str, Any]:
        self._manifest("lifecycle", "full", "running", LIFECYCLE_SOURCE, run_id)
        synced_at = self._now()
        rows_by_code: dict[str, tuple[Any, ...]] = {}
        source_counts: dict[str, int] = {}
        try:
            for list_status in ("L", "D", "P"):
                frame = self.pro.stock_basic(
                    exchange="",
                    list_status=list_status,
                    fields="ts_code,name,industry,market,list_date,delist_date",
                )
                records = [] if frame is None or frame.empty else frame.to_dict("records")
                source_counts[list_status] = len(records)
                for row in records:
                    ts_code = _clean_text(row.get("ts_code"))
                    if not ts_code:
                        continue
                    code = from_ts_code(ts_code)
                    _, suffix = ts_code.split(".", 1) if "." in ts_code else (ts_code, "")
                    rows_by_code[code] = (
                        code,
                        ts_code,
                        _clean_text(row.get("name")) or code,
                        "stock",
                        suffix.upper() or None,
                        _clean_text(row.get("market")),
                        _clean_text(row.get("industry")),
                        list_status,
                        normalize_date(row.get("list_date")),
                        normalize_date(row.get("delist_date")),
                        LIFECYCLE_SOURCE,
                        run_id,
                        synced_at,
                    )

            sql = """
            INSERT INTO stock_instrument_lifecycle (
                code, ts_code, name, instrument_type, exchange, market, industry,
                list_status, listing_date, delisting_date, source, source_sync_id,
                source_updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                ts_code=VALUES(ts_code), name=VALUES(name),
                instrument_type=VALUES(instrument_type), exchange=VALUES(exchange),
                market=VALUES(market), industry=VALUES(industry),
                list_status=VALUES(list_status), listing_date=VALUES(listing_date),
                delisting_date=VALUES(delisting_date), source=VALUES(source),
                source_sync_id=VALUES(source_sync_id),
                source_updated_at=VALUES(source_updated_at)
            """
            removed = 0
            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    if rows_by_code:
                        cursor.executemany(sql, list(rows_by_code.values()))
                    removed = int(
                        cursor.execute(
                            """
                            DELETE FROM stock_instrument_lifecycle
                            WHERE source=%s AND (source_sync_id IS NULL OR source_sync_id<>%s)
                            """,
                            (LIFECYCLE_SOURCE, run_id),
                        )
                        or 0
                    )
            payload = {
                "rows": len(rows_by_code),
                "source_counts": source_counts,
                "removed_stale_rows": removed,
            }
            self._manifest(
                "lifecycle",
                "full",
                "success",
                LIFECYCLE_SOURCE,
                run_id,
                source_rows=len(rows_by_code),
                metadata=payload,
            )
            return payload
        except Exception as exc:
            self._manifest(
                "lifecycle",
                "full",
                "failed",
                LIFECYCLE_SOURCE,
                run_id,
                metadata={"error": f"{type(exc).__name__}: {str(exc)[:500]}"},
            )
            raise

    @staticmethod
    def _name_rows(
        records: Sequence[dict[str, Any]],
        run_id: str,
        synced_at: str,
    ) -> tuple[list[tuple[Any, ...]], int]:
        rows: list[tuple[Any, ...]] = []
        skipped_missing_start = 0
        for row in records:
            ts_code = _clean_text(row.get("ts_code"))
            name = _clean_text(row.get("name"))
            start_date = normalize_date(row.get("start_date"))
            if not ts_code or not name or not start_date:
                skipped_missing_start += 1
                continue
            flags = classify_name(name, row.get("change_reason"))
            rows.append(
                (
                    from_ts_code(ts_code),
                    name,
                    start_date,
                    normalize_date(row.get("end_date")),
                    normalize_date(row.get("ann_date")),
                    _clean_text(row.get("change_reason")),
                    flags["is_st"],
                    flags["is_delisting_period"],
                    NAME_HISTORY_SOURCE,
                    run_id,
                    synced_at,
                )
            )
        return rows, skipped_missing_start

    def _fetch_name_gap_codes(
        self,
        run_id: str,
        start_date: str,
        end_date: str,
        *,
        limit: int,
    ) -> list[dict[str, str]]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT l.code, l.ts_code
                    FROM stock_instrument_lifecycle l
                    WHERE l.instrument_type='stock'
                      AND l.listing_date <= %s
                      AND (l.delisting_date IS NULL OR l.delisting_date >= %s)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM stock_name_history n
                          WHERE n.code=l.code
                            AND n.source=%s
                            AND n.source_sync_id=%s
                            AND n.start_date <= LEAST(COALESCE(l.delisting_date, %s), %s)
                            AND COALESCE(n.end_date, '9999-12-31') >=
                                GREATEST(COALESCE(l.listing_date, %s), %s)
                      )
                    ORDER BY l.code
                    LIMIT %s
                    """,
                    (
                        end_date,
                        start_date,
                        NAME_HISTORY_SOURCE,
                        run_id,
                        end_date,
                        end_date,
                        start_date,
                        start_date,
                        max(int(limit), 0),
                    ),
                )
                return [
                    {"code": str(row["code"]), "ts_code": str(row["ts_code"])}
                    for row in (cursor.fetchall() or [])
                ]

    def sync_name_history(
        self,
        run_id: str,
        page_size: int = NAME_PAGE_SIZE,
        *,
        coverage_start_date: str | None = None,
        coverage_end_date: str | None = None,
        fallback_code_limit: int = NAME_FALLBACK_CODE_LIMIT,
        fallback_pause_seconds: float = 0.0,
    ) -> dict[str, Any]:
        self._manifest("name_history", "full", "running", NAME_HISTORY_SOURCE, run_id)
        synced_at = self._now()
        rows_by_key: dict[tuple[str, str, str], tuple[Any, ...]] = {}
        pages = 0
        skipped_missing_start = 0
        global_source_rows = 0
        fallback_source_rows = 0
        fallback_codes: list[str] = []
        fallback_empty_codes: list[str] = []
        unresolved_codes: list[dict[str, str]] = []
        try:
            for offset in range(0, page_size * 100, page_size):
                frame = self.pro.namechange(
                    limit=page_size,
                    offset=offset,
                    fields="ts_code,name,start_date,end_date,ann_date,change_reason",
                )
                records = [] if frame is None or frame.empty else frame.to_dict("records")
                pages += 1
                global_source_rows += len(records)
                normalized_rows, skipped = self._name_rows(records, run_id, synced_at)
                skipped_missing_start += skipped
                for normalized in normalized_rows:
                    rows_by_key[(normalized[0], normalized[2], normalized[1])] = normalized
                if len(records) < page_size:
                    break
            else:
                raise RuntimeError("namechange pagination exceeded safety limit")

            sql = """
            INSERT INTO stock_name_history (
                code, name, start_date, end_date, announcement_date, change_reason,
                is_st, is_delisting_period, source, source_sync_id, source_updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                end_date=VALUES(end_date), announcement_date=VALUES(announcement_date),
                change_reason=VALUES(change_reason), is_st=VALUES(is_st),
                is_delisting_period=VALUES(is_delisting_period), source=VALUES(source),
                source_sync_id=VALUES(source_sync_id),
                source_updated_at=VALUES(source_updated_at)
            """
            removed = 0
            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    global_rows = list(rows_by_key.values())
                    for offset in range(0, len(global_rows), 1000):
                        cursor.executemany(sql, global_rows[offset : offset + 1000])

            if coverage_start_date and coverage_end_date:
                gap_codes = self._fetch_name_gap_codes(
                    run_id,
                    coverage_start_date,
                    coverage_end_date,
                    limit=max(int(fallback_code_limit), 0) + 1,
                )
                if len(gap_codes) > fallback_code_limit:
                    raise RuntimeError(
                        "namechange per-code fallback exceeded safety limit: "
                        f"more than {fallback_code_limit} codes"
                    )
                fallback_rows_by_key: dict[tuple[str, str, str], tuple[Any, ...]] = {}
                for gap in gap_codes:
                    fallback_codes.append(gap["code"])
                    frame = self.pro.namechange(
                        ts_code=gap["ts_code"],
                        fields="ts_code,name,start_date,end_date,ann_date,change_reason",
                    )
                    records = [] if frame is None or frame.empty else frame.to_dict("records")
                    fallback_source_rows += len(records)
                    if not records:
                        fallback_empty_codes.append(gap["code"])
                    normalized_rows, skipped = self._name_rows(records, run_id, synced_at)
                    skipped_missing_start += skipped
                    for normalized in normalized_rows:
                        fallback_rows_by_key[(normalized[0], normalized[2], normalized[1])] = normalized
                    if fallback_pause_seconds > 0:
                        time.sleep(fallback_pause_seconds)
                if fallback_rows_by_key:
                    with self._connection_factory(dict_cursor=False) as conn:
                        with conn.cursor() as cursor:
                            fallback_rows = list(fallback_rows_by_key.values())
                            for offset in range(0, len(fallback_rows), 1000):
                                cursor.executemany(sql, fallback_rows[offset : offset + 1000])
                    rows_by_key.update(fallback_rows_by_key)
                unresolved_codes = self._fetch_name_gap_codes(
                    run_id,
                    coverage_start_date,
                    coverage_end_date,
                    limit=max(int(fallback_code_limit), 0) + 1,
                )

            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    removed = int(
                        cursor.execute(
                            """
                            DELETE FROM stock_name_history
                            WHERE source=%s AND (source_sync_id IS NULL OR source_sync_id<>%s)
                            """,
                            (NAME_HISTORY_SOURCE, run_id),
                        )
                        or 0
                    )
            payload = {
                "rows": len(rows_by_key),
                "global_source_rows": global_source_rows,
                "fallback_source_rows": fallback_source_rows,
                "pages": pages,
                "skipped_missing_start": skipped_missing_start,
                "fallback_codes": fallback_codes,
                "fallback_empty_codes": fallback_empty_codes,
                "unresolved_codes": unresolved_codes,
                "removed_stale_rows": removed,
            }
            self._manifest(
                "name_history",
                "full",
                "success",
                NAME_HISTORY_SOURCE,
                run_id,
                source_rows=global_source_rows + fallback_source_rows,
                metadata=payload,
            )
            return payload
        except Exception as exc:
            self._manifest(
                "name_history",
                "full",
                "failed",
                NAME_HISTORY_SOURCE,
                run_id,
                metadata={"error": f"{type(exc).__name__}: {str(exc)[:500]}"},
            )
            raise

    def fetch_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM factor_input_daily
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    (start_date, end_date),
                )
                return [str(row["trade_date"]) for row in (cursor.fetchall() or [])]

    def sync_suspensions(
        self,
        run_id: str,
        start_date: str,
        end_date: str,
        *,
        pause_seconds: float = 0.0,
        trade_dates: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        dates = list(trade_dates) if trade_dates is not None else self.fetch_trade_dates(start_date, end_date)
        if not dates:
            return {
                "start_date": start_date,
                "end_date": end_date,
                "trade_dates": 0,
                "successful_dates": 0,
                "failed_dates": [],
                "rows": 0,
                "source_rows": 0,
                "pages": 0,
            }
        selected_dates = {normalize_date(value) or str(value) for value in dates}
        api_start_date = min(selected_dates)
        api_end_date = max(selected_dates)
        total_rows = 0
        failed_dates: list[dict[str, str]] = []
        source_rows = 0
        pages = 0
        sql = """
        INSERT INTO stock_suspension_daily (
            code, trade_date, suspend_type, suspend_timing, source,
            source_sync_id, source_updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            source=VALUES(source), source_sync_id=VALUES(source_sync_id),
            source_updated_at=VALUES(source_updated_at)
        """
        for trade_date in selected_dates:
            self._manifest("suspension_daily", trade_date, "running", SUSPENSION_SOURCE, run_id)
        rows_by_date: dict[str, dict[tuple[str, str, str, str], tuple[Any, ...]]] = {
            trade_date: {} for trade_date in selected_dates
        }
        try:
            for offset in range(0, SUSPENSION_PAGE_SIZE * 100, SUSPENSION_PAGE_SIZE):
                frame = self.pro.suspend_d(
                    start_date=api_start_date.replace("-", ""),
                    end_date=api_end_date.replace("-", ""),
                    limit=SUSPENSION_PAGE_SIZE,
                    offset=offset,
                    fields="ts_code,trade_date,suspend_timing,suspend_type",
                )
                records = [] if frame is None or frame.empty else frame.to_dict("records")
                pages += 1
                source_rows += len(records)
                synced_at = self._now()
                for row in records:
                    ts_code = _clean_text(row.get("ts_code"))
                    suspend_type = _clean_text(row.get("suspend_type"))
                    trade_date = normalize_date(row.get("trade_date"))
                    if not ts_code or not suspend_type or trade_date not in selected_dates:
                        continue
                    timing = _clean_text(row.get("suspend_timing")) or ""
                    normalized = (
                        from_ts_code(ts_code),
                        trade_date,
                        suspend_type,
                        timing,
                        SUSPENSION_SOURCE,
                        run_id,
                        synced_at,
                    )
                    rows_by_date[trade_date][
                        (normalized[0], normalized[1], normalized[2], normalized[3])
                    ] = normalized
                if len(records) < SUSPENSION_PAGE_SIZE:
                    break
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
            else:
                raise RuntimeError("suspend_d pagination exceeded safety limit")

            for trade_date in sorted(selected_dates):
                rows = list(rows_by_date[trade_date].values())
                with self._connection_factory(dict_cursor=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM stock_suspension_daily WHERE trade_date=%s AND source=%s",
                            (trade_date, SUSPENSION_SOURCE),
                        )
                        if rows:
                            cursor.executemany(sql, rows)
                total_rows += len(rows)
                self._manifest(
                    "suspension_daily",
                    trade_date,
                    "success",
                    SUSPENSION_SOURCE,
                    run_id,
                    source_rows=len(rows),
                )
        except Exception as exc:
            for trade_date in sorted(selected_dates):
                failed_dates.append({"trade_date": trade_date, "error": str(exc)[:300]})
                self._manifest(
                    "suspension_daily",
                    trade_date,
                    "failed",
                    SUSPENSION_SOURCE,
                    run_id,
                    metadata={"error": f"{type(exc).__name__}: {str(exc)[:500]}"},
                )
        return {
            "start_date": start_date,
            "end_date": end_date,
            "trade_dates": len(dates),
            "successful_dates": len(dates) - len(failed_dates),
            "failed_dates": failed_dates,
            "rows": total_rows,
            "source_rows": source_rows,
            "pages": pages,
            "api_start_date": api_start_date,
            "api_end_date": api_end_date,
        }

    def fetch_historical_delisted_codes(self, start_date: str, end_date: str) -> list[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT code
                    FROM stock_instrument_lifecycle
                    WHERE instrument_type='stock'
                      AND list_status='D'
                      AND listing_date <= %s
                      AND delisting_date >= %s
                    ORDER BY code
                    """,
                    (end_date, start_date),
                )
                return [str(row["code"]) for row in (cursor.fetchall() or [])]

    def fetch_pending_historical_market_codes(self, start_date: str, end_date: str) -> list[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT l.code
                    FROM stock_instrument_lifecycle l
                    LEFT JOIN stock_status_pit_manifest m
                      ON m.dataset='historical_market_data' AND m.partition_key=l.code
                    WHERE l.instrument_type='stock'
                      AND l.list_status='D'
                      AND l.listing_date <= %s
                      AND l.delisting_date >= %s
                      AND COALESCE(m.status, '') <> 'success'
                    ORDER BY l.code
                    """,
                    (end_date, start_date),
                )
                return [str(row["code"]) for row in (cursor.fetchall() or [])]

    def _fetch_daily_basic_history(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> list[FactorInputDailyRecord]:
        market, symbol = code.split(".", 1)
        suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(market.lower(), market.upper())
        ts_code = f"{symbol}.{suffix}"
        frame = self.pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            fields="ts_code,trade_date,pe,pb,turnover_rate,turnover_rate_f,volume_ratio,total_mv,circ_mv",
        )
        records: list[FactorInputDailyRecord] = []
        now = self._now()
        for row in ([] if frame is None or frame.empty else frame.to_dict("records")):
            valuation_values = (
                _to_float(row.get("pe")),
                _to_float(row.get("pb")),
                _to_float(row.get("turnover_rate")),
                _to_float(row.get("volume_ratio")),
            )
            completeness = round(sum(value is not None for value in valuation_values) / 6, 4)
            records.append(
                FactorInputDailyRecord(
                    code=code,
                    trade_date=normalize_date(row.get("trade_date")) or "",
                    pe_tushare=valuation_values[0],
                    pb_tushare=valuation_values[1],
                    turnover_rate=valuation_values[2],
                    turnover_rate_f=_to_float(row.get("turnover_rate_f")),
                    volume_ratio=valuation_values[3],
                    total_mv=_to_float(row.get("total_mv")),
                    circ_mv=_to_float(row.get("circ_mv")),
                    valuation_source=MARKET_DATA_SOURCE,
                    fundamental_source="historical_fundamentals_excluded",
                    valuation_updated_at=now,
                    completeness_score=completeness,
                    source=MARKET_DATA_SOURCE,
                )
            )
        return [record for record in records if record.trade_date]

    def _fetch_no_market_activity_evidence(self, code: str, start_date: str) -> dict[str, Any]:
        previous_end = (
            datetime.strptime(start_date, "%Y-%m-%d").date() - timedelta(days=1)
        ).strftime("%Y%m%d")
        ts_code = DailyKlineSync.to_ts_code(code)
        daily = self.pro.daily(
            ts_code=ts_code,
            start_date="19900101",
            end_date=previous_end,
            limit=1,
            fields="ts_code,trade_date",
        )
        daily_basic = self.pro.daily_basic(
            ts_code=ts_code,
            start_date="19900101",
            end_date=previous_end,
            limit=1,
            fields="ts_code,trade_date",
        )

        def latest_date(frame: Any) -> str | None:
            if frame is None or frame.empty:
                return None
            values = [normalize_date(row.get("trade_date")) for row in frame.to_dict("records")]
            return max((value for value in values if value), default=None)

        last_daily = latest_date(daily)
        last_daily_basic = latest_date(daily_basic)
        return {
            "confirmed": bool(last_daily and last_daily_basic),
            "last_daily_trade_date": last_daily,
            "last_daily_basic_trade_date": last_daily_basic,
        }

    def sync_historical_market_data(
        self,
        run_id: str,
        start_date: str,
        end_date: str,
        *,
        pause_seconds: float = 0.0,
        codes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        selected_codes = list(codes) if codes is not None else self.fetch_historical_delisted_codes(start_date, end_date)
        kline_sync = DailyKlineSync(token=self.token)
        factor_sync = FactorInputHistorySync(token=self.token)
        successful_codes: list[str] = []
        no_market_activity_codes: list[str] = []
        incomplete_codes: list[dict[str, Any]] = []
        failed_codes: list[dict[str, str]] = []
        kline_rows = 0
        factor_rows = 0
        for code in selected_codes:
            self._manifest("historical_market_data", code, "running", MARKET_DATA_SOURCE, run_id)
            try:
                kline_records = kline_sync.fetch_kline_for_code(code, start_date, end_date)
                factor_records = self._fetch_daily_basic_history(code, start_date, end_date)
                saved_kline = kline_sync.save_to_mysql(kline_records)
                saved_factor = factor_sync.save_records(
                    factor_records,
                    preserve_existing_fundamentals=True,
                )
                kline_rows += saved_kline
                factor_rows += saved_factor
                no_activity_evidence: dict[str, Any] = {}
                if saved_kline == 0 and saved_factor == 0:
                    try:
                        no_activity_evidence = self._fetch_no_market_activity_evidence(
                            code,
                            start_date,
                        )
                    except Exception as evidence_exc:
                        no_activity_evidence = {
                            "confirmed": False,
                            "error": f"{type(evidence_exc).__name__}: {str(evidence_exc)[:300]}",
                        }
                classification = classify_historical_market_rows(
                    saved_kline,
                    saved_factor,
                    no_activity_confirmed=bool(no_activity_evidence.get("confirmed")),
                )
                metadata = {
                    "kline_rows": saved_kline,
                    "factor_rows": saved_factor,
                    "classification": classification,
                    "no_market_activity_evidence": no_activity_evidence,
                }
                if classification == "complete":
                    successful_codes.append(code)
                    manifest_status = "success"
                elif classification == "source_confirmed_no_market_activity":
                    no_market_activity_codes.append(code)
                    manifest_status = "success"
                else:
                    incomplete_codes.append({"code": code, **metadata})
                    manifest_status = "partial_success"
                self._manifest(
                    "historical_market_data",
                    code,
                    manifest_status,
                    MARKET_DATA_SOURCE,
                    run_id,
                    source_rows=saved_kline + saved_factor,
                    metadata=metadata,
                )
            except Exception as exc:
                failed_codes.append({"code": code, "error": str(exc)[:300]})
                self._manifest(
                    "historical_market_data",
                    code,
                    "failed",
                    MARKET_DATA_SOURCE,
                    run_id,
                    metadata={"error": f"{type(exc).__name__}: {str(exc)[:500]}"},
                )
            if pause_seconds > 0:
                time.sleep(pause_seconds)
        return {
            "start_date": start_date,
            "end_date": end_date,
            "requested_codes": len(selected_codes),
            "successful_codes": successful_codes,
            "no_market_activity_codes": no_market_activity_codes,
            "incomplete_codes": incomplete_codes,
            "failed_codes": failed_codes,
            "kline_rows": kline_rows,
            "factor_rows": factor_rows,
        }

    def run(
        self,
        run_id: str,
        start_date: str,
        end_date: str,
        *,
        stages: Iterable[str],
        pause_seconds: float = 0.0,
        suspension_recent_trade_days: int | None = None,
        pending_market_only: bool = False,
    ) -> dict[str, Any]:
        selected = set(stages)
        payload: dict[str, Any] = {
            "run_id": run_id,
            "start_date": start_date,
            "end_date": end_date,
            "stages": sorted(selected),
        }
        if "lifecycle" in selected:
            payload["lifecycle"] = self.sync_lifecycle(run_id)
        if "names" in selected:
            payload["names"] = self.sync_name_history(
                run_id,
                coverage_start_date=start_date,
                coverage_end_date=end_date,
                fallback_pause_seconds=pause_seconds,
            )
        if "suspensions" in selected:
            suspension_dates = None
            if suspension_recent_trade_days is not None:
                all_dates = self.fetch_trade_dates(start_date, end_date)
                recent_days = max(int(suspension_recent_trade_days), 0)
                suspension_dates = all_dates[-recent_days:] if recent_days else []
            payload["suspensions"] = self.sync_suspensions(
                run_id,
                start_date,
                end_date,
                pause_seconds=pause_seconds,
                trade_dates=suspension_dates,
            )
        if "market-data" in selected:
            market_codes = (
                self.fetch_pending_historical_market_codes(start_date, end_date)
                if pending_market_only
                else None
            )
            payload["market_data"] = self.sync_historical_market_data(
                run_id,
                start_date,
                end_date,
                pause_seconds=pause_seconds,
                codes=market_codes,
            )
        return payload
