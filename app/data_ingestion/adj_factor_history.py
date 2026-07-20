from __future__ import annotations

import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable

from app.data_ingestion.adj_factor_sync import AdjFactorSync, SOURCE
from app.jobs.errors import infer_error_code, sanitize_error_message
from app.shared.db import mysql_conn


DEFAULT_MINIMUM_COVERAGE_RATIO = 0.995
MAX_RESULT_DATE_SAMPLES = 20


def _date_text(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    return str(value)[:10]


def _int(value: Any) -> int:
    return int(value or 0)


def _float(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)
    return float(value or 0.0)


def _bounded_dates(values: Iterable[str]) -> list[str]:
    return list(values)[:MAX_RESULT_DATE_SAMPLES]


def partition_status(
    *,
    expected_rows: int,
    matched_rows: int,
    source_rows: int,
    minimum_coverage_ratio: float,
) -> str:
    if expected_rows <= 0:
        return "failed"
    coverage_ratio = matched_rows / expected_rows
    if coverage_ratio >= minimum_coverage_ratio:
        return "success"
    if source_rows <= 0:
        return "empty"
    return "partial_success"


def adjusted_total_return(
    start_price: float,
    start_factor: float,
    end_price: float,
    end_factor: float,
) -> float:
    values = (start_price, start_factor, end_price, end_factor)
    if any(
        value is None or not math.isfinite(float(value)) or float(value) <= 0
        for value in values
    ):
        raise ValueError("prices and adjustment factors must be positive")
    return (float(end_price) * float(end_factor)) / (
        float(start_price) * float(start_factor)
    ) - 1.0


class AdjFactorHistoryBackfill:
    def __init__(
        self,
        source: AdjFactorSync | None = None,
        connection_factory: Callable[..., Any] = mysql_conn,
    ) -> None:
        self.source = source
        self._connection_factory = connection_factory

    def _source_client(self) -> AdjFactorSync:
        if self.source is None:
            self.source = AdjFactorSync()
        return self.source

    def trade_dates(self, start_date: str, end_date: str) -> list[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_kline
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    (start_date, end_date),
                )
                return [_date_text(row["trade_date"]) for row in (cursor.fetchall() or [])]

    def date_range(self) -> tuple[str, str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date FROM daily_kline"
                )
                row = cursor.fetchone() or {}
        if not row.get("min_date") or not row.get("max_date"):
            raise RuntimeError("daily_kline has no dates for adjustment factor backfill")
        return _date_text(row["min_date"]), _date_text(row["max_date"])

    @staticmethod
    def _coverage_payload(row: dict[str, Any]) -> dict[str, Any]:
        expected_rows = _int(row.get("expected_rows"))
        matched_rows = _int(row.get("matched_rows"))
        return {
            "expected_rows": expected_rows,
            "stored_rows": _int(row.get("stored_rows") or matched_rows),
            "matched_rows": matched_rows,
            "missing_rows": max(expected_rows - matched_rows, 0),
            "coverage_ratio": round(matched_rows / expected_rows, 8)
            if expected_rows
            else 0.0,
        }

    def coverage_by_date(self, start_date: str, end_date: str) -> dict[str, dict[str, Any]]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT dk.trade_date,
                           COUNT(*) AS expected_rows,
                           COUNT(af.code) AS matched_rows
                    FROM daily_kline dk
                    LEFT JOIN adj_factor_daily af
                      ON af.code=dk.code AND af.trade_date=dk.trade_date
                    WHERE dk.trade_date BETWEEN %s AND %s
                    GROUP BY dk.trade_date
                    ORDER BY dk.trade_date
                    """,
                    (start_date, end_date),
                )
                rows = cursor.fetchall() or []
        return {
            _date_text(row["trade_date"]): self._coverage_payload(row)
            for row in rows
        }

    def coverage_for_date(self, trade_date: str) -> dict[str, Any]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS expected_rows,
                           COUNT(af.code) AS matched_rows,
                           (
                             SELECT COUNT(*)
                             FROM adj_factor_daily stored_factor
                             WHERE stored_factor.trade_date=%s
                           ) AS stored_rows
                    FROM daily_kline dk
                    LEFT JOIN adj_factor_daily af
                      ON af.code=dk.code AND af.trade_date=dk.trade_date
                    WHERE dk.trade_date=%s
                    """,
                    (trade_date, trade_date),
                )
                return self._coverage_payload(cursor.fetchone() or {})

    def successful_manifest_dates(
        self,
        start_date: str,
        end_date: str,
        minimum_coverage_ratio: float,
    ) -> set[str]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date
                    FROM adj_factor_sync_manifest
                    WHERE trade_date BETWEEN %s AND %s
                      AND status='success'
                      AND coverage_ratio >= %s
                    """,
                    (start_date, end_date, minimum_coverage_ratio),
                )
                return {
                    _date_text(row["trade_date"])
                    for row in (cursor.fetchall() or [])
                }

    def _mark_running(self, trade_date: str, run_id: str) -> None:
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO adj_factor_sync_manifest (
                        trade_date, status, source, sync_run_id, attempt_count,
                        started_at, finished_at, error_code, error_message, metadata_json
                    ) VALUES (%s,'running',%s,%s,1,NOW(),NULL,NULL,NULL,NULL)
                    ON DUPLICATE KEY UPDATE
                        status='running', source=VALUES(source), sync_run_id=VALUES(sync_run_id),
                        attempt_count=attempt_count+1, started_at=NOW(), finished_at=NULL,
                        error_code=NULL, error_message=NULL, metadata_json=NULL
                    """,
                    (trade_date, SOURCE, run_id),
                )

    def _mark_terminal(
        self,
        trade_date: str,
        status: str,
        run_id: str,
        coverage: dict[str, Any],
        *,
        source_rows: int,
        metadata: dict[str, Any],
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        safe_error = sanitize_error_message(error_message) if error_message else None
        payload = json.dumps(metadata, ensure_ascii=False, default=str)
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO adj_factor_sync_manifest (
                        trade_date, status, source, expected_kline_rows, source_rows,
                        stored_rows, matched_rows, missing_rows, coverage_ratio,
                        sync_run_id, attempt_count, started_at, finished_at,
                        error_code, error_message, metadata_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,NULL,NOW(),%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        status=VALUES(status), source=VALUES(source),
                        expected_kline_rows=VALUES(expected_kline_rows),
                        source_rows=VALUES(source_rows), stored_rows=VALUES(stored_rows),
                        matched_rows=VALUES(matched_rows), missing_rows=VALUES(missing_rows),
                        coverage_ratio=VALUES(coverage_ratio), sync_run_id=VALUES(sync_run_id),
                        finished_at=NOW(), error_code=VALUES(error_code),
                        error_message=VALUES(error_message), metadata_json=VALUES(metadata_json)
                    """,
                    (
                        trade_date,
                        status,
                        SOURCE,
                        _int(coverage.get("expected_rows")),
                        int(source_rows),
                        _int(coverage.get("stored_rows")),
                        _int(coverage.get("matched_rows")),
                        _int(coverage.get("missing_rows")),
                        float(coverage.get("coverage_ratio") or 0.0),
                        run_id,
                        error_code,
                        safe_error,
                        payload,
                    ),
                )

    def _reconcile_existing(
        self,
        trade_date: str,
        run_id: str,
        coverage: dict[str, Any],
    ) -> None:
        metadata = {
            "trade_date": trade_date,
            **coverage,
            "reconciled_existing_rows": True,
            "upstream_called": False,
        }
        self._mark_terminal(
            trade_date,
            "success",
            run_id,
            coverage,
            source_rows=_int(coverage.get("stored_rows")),
            metadata=metadata,
        )

    def sync_date(
        self,
        trade_date: str,
        run_id: str,
        minimum_coverage_ratio: float,
    ) -> dict[str, Any]:
        self._mark_running(trade_date, run_id)
        try:
            source = self._source_client()
            records = source.fetch_for_trade_date(trade_date)
            source_rows = len(records)
            saved_rows = source.save_records(records)
            coverage = self.coverage_for_date(trade_date)
            status = partition_status(
                expected_rows=_int(coverage.get("expected_rows")),
                matched_rows=_int(coverage.get("matched_rows")),
                source_rows=source_rows,
                minimum_coverage_ratio=minimum_coverage_ratio,
            )
            payload = {
                "trade_date": trade_date,
                "status": status,
                "source_rows": source_rows,
                "saved_rows": saved_rows,
                **coverage,
                "minimum_coverage_ratio": minimum_coverage_ratio,
            }
            self._mark_terminal(
                trade_date,
                status,
                run_id,
                coverage,
                source_rows=source_rows,
                metadata=payload,
            )
            return payload
        except Exception as exc:
            message = f"{type(exc).__name__}: {str(exc)[:900]}"
            try:
                coverage = self.coverage_for_date(trade_date)
            except Exception as coverage_exc:
                coverage = {
                    "expected_rows": 0,
                    "stored_rows": 0,
                    "matched_rows": 0,
                    "missing_rows": 0,
                    "coverage_ratio": 0.0,
                }
                message = (
                    f"{message}; coverage audit failed: "
                    f"{type(coverage_exc).__name__}: {str(coverage_exc)[:500]}"
                )
            payload = {
                "trade_date": trade_date,
                "status": "failed",
                "error": sanitize_error_message(message),
                **coverage,
            }
            self._mark_terminal(
                trade_date,
                "failed",
                run_id,
                coverage,
                source_rows=0,
                metadata=payload,
                error_code=infer_error_code(message),
                error_message=message,
            )
            return payload

    def run(
        self,
        run_id: str,
        start_date: str,
        end_date: str,
        *,
        pending_only: bool = True,
        pause_seconds: float = 0.5,
        minimum_coverage_ratio: float = DEFAULT_MINIMUM_COVERAGE_RATIO,
        max_days: int | None = None,
        max_failures: int = 10,
    ) -> dict[str, Any]:
        import time

        if not 0 < minimum_coverage_ratio <= 1:
            raise ValueError("minimum_coverage_ratio must be in (0, 1]")
        if max_days is not None and max_days <= 0:
            raise ValueError("max_days must be positive when provided")
        if max_failures <= 0:
            raise ValueError("max_failures must be positive")

        requested_dates = self.trade_dates(start_date, end_date)
        existing = self.coverage_by_date(start_date, end_date)
        successful_manifest_dates = self.successful_manifest_dates(
            start_date,
            end_date,
            minimum_coverage_ratio,
        )
        selected_dates: list[str] = []
        skipped_existing: list[str] = []
        for trade_date in requested_dates:
            coverage = existing.get(trade_date) or {
                "expected_rows": 0,
                "stored_rows": 0,
                "matched_rows": 0,
                "missing_rows": 0,
                "coverage_ratio": 0.0,
            }
            if pending_only and float(coverage.get("coverage_ratio") or 0.0) >= minimum_coverage_ratio:
                if trade_date not in successful_manifest_dates:
                    self._reconcile_existing(trade_date, run_id, coverage)
                skipped_existing.append(trade_date)
            else:
                selected_dates.append(trade_date)

        deferred_dates: list[str] = []
        if max_days is not None and len(selected_dates) > max_days:
            deferred_dates = selected_dates[max_days:]
            selected_dates = selected_dates[:max_days]

        results: list[dict[str, Any]] = []
        failures = 0
        aborted_dates: list[str] = []
        for index, trade_date in enumerate(selected_dates):
            result = self.sync_date(trade_date, run_id, minimum_coverage_ratio)
            results.append(result)
            if result.get("status") in {"failed", "empty"}:
                failures += 1
            if failures >= max_failures:
                aborted_dates = selected_dates[index + 1 :]
                break
            if pause_seconds > 0:
                time.sleep(pause_seconds)

        success_dates = [row["trade_date"] for row in results if row.get("status") == "success"]
        partial_dates = [row["trade_date"] for row in results if row.get("status") == "partial_success"]
        empty_dates = [row["trade_date"] for row in results if row.get("status") == "empty"]
        failed_dates = [row["trade_date"] for row in results if row.get("status") == "failed"]
        status = "success"
        if partial_dates or empty_dates or failed_dates or deferred_dates or aborted_dates:
            status = "partial_success"

        return {
            "status": status,
            "run_id": run_id,
            "start_date": start_date,
            "end_date": end_date,
            "minimum_coverage_ratio": minimum_coverage_ratio,
            "pending_only": pending_only,
            "requested_trade_days": len(requested_dates),
            "selected_trade_days": len(selected_dates),
            "processed_trade_days": len(results),
            "skipped_existing_trade_days": len(skipped_existing),
            "deferred_trade_days": len(deferred_dates),
            "aborted_trade_days": len(aborted_dates),
            "success_trade_days": len(success_dates),
            "partial_trade_days": len(partial_dates),
            "empty_trade_days": len(empty_dates),
            "failed_trade_days": len(failed_dates),
            "source_rows": sum(_int(row.get("source_rows")) for row in results),
            "rows_synced": sum(_int(row.get("saved_rows")) for row in results),
            "samples": {
                "skipped_existing": _bounded_dates(skipped_existing),
                "success": _bounded_dates(success_dates),
                "partial": _bounded_dates(partial_dates),
                "empty": _bounded_dates(empty_dates),
                "failed": _bounded_dates(failed_dates),
                "deferred": _bounded_dates(deferred_dates),
                "aborted": _bounded_dates(aborted_dates),
            },
        }

    def audit(
        self,
        start_date: str,
        end_date: str,
        *,
        minimum_coverage_ratio: float = DEFAULT_MINIMUM_COVERAGE_RATIO,
    ) -> dict[str, Any]:
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS trade_days,
                           SUM(expected_rows) AS kline_rows,
                           SUM(matched_rows) AS matched_rows,
                           SUM(expected_rows-matched_rows) AS missing_rows,
                           MIN(matched_rows/NULLIF(expected_rows,0)) AS minimum_partition_coverage_ratio,
                           SUM(matched_rows/NULLIF(expected_rows,0) < %s) AS incomplete_days
                    FROM (
                        SELECT dk.trade_date,
                               COUNT(*) AS expected_rows,
                               COUNT(af.code) AS matched_rows
                        FROM daily_kline dk
                        LEFT JOIN adj_factor_daily af
                          ON af.code=dk.code AND af.trade_date=dk.trade_date
                        WHERE dk.trade_date BETWEEN %s AND %s
                        GROUP BY dk.trade_date
                    ) coverage_by_partition
                    """,
                    (minimum_coverage_ratio, start_date, end_date),
                )
                actual = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT COUNT(*) AS manifest_days,
                           SUM(status='success') AS success_days,
                           SUM(status='partial_success') AS partial_days,
                           SUM(status='empty') AS empty_days,
                           SUM(status='failed') AS failed_days,
                           SUM(status='running') AS running_days,
                           SUM(expected_kline_rows) AS expected_rows,
                           SUM(matched_rows) AS matched_rows,
                           SUM(missing_rows) AS missing_rows,
                           MIN(coverage_ratio) AS minimum_partition_coverage_ratio,
                           SUM(coverage_ratio < %s OR status <> 'success') AS incomplete_days
                    FROM adj_factor_sync_manifest
                    WHERE trade_date BETWEEN %s AND %s
                    """,
                    (minimum_coverage_ratio, start_date, end_date),
                )
                manifest = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT SUM(
                               trade_date BETWEEN %s AND %s
                               AND (adj_factor IS NULL OR adj_factor <= 0)
                           ) AS invalid_factor_rows,
                           SUM(trade_date > CURDATE()) AS future_factor_rows
                    FROM adj_factor_daily
                    """,
                    (start_date, end_date),
                )
                invalid = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT COUNT(*) AS factor_rows,
                           MIN(trade_date) AS min_factor_date,
                           MAX(trade_date) AS max_factor_date
                    FROM adj_factor_daily
                    WHERE trade_date BETWEEN %s AND %s
                    """,
                    (start_date, end_date),
                )
                factor_range = cursor.fetchone() or {}

        expected_days = _int(actual.get("trade_days"))
        manifest_days = _int(manifest.get("manifest_days"))
        expected_rows = _int(actual.get("kline_rows"))
        matched_rows = _int(actual.get("matched_rows"))
        coverage_ratio = matched_rows / expected_rows if expected_rows else 0.0
        missing_manifest_days = max(expected_days - manifest_days, 0)
        incomplete_days = _int(actual.get("incomplete_days"))
        manifest_incomplete_days = _int(manifest.get("incomplete_days"))
        invalid_rows = _int(invalid.get("invalid_factor_rows"))
        future_rows = _int(invalid.get("future_factor_rows"))
        ready = bool(
            expected_days
            and missing_manifest_days == 0
            and incomplete_days == 0
            and manifest_incomplete_days == 0
            and coverage_ratio >= minimum_coverage_ratio
            and invalid_rows == 0
            and future_rows == 0
        )
        return {
            "status": "ready" if ready else "blocked",
            "ready": ready,
            "start_date": start_date,
            "end_date": end_date,
            "minimum_coverage_ratio": minimum_coverage_ratio,
            "trade_days": expected_days,
            "manifest_days": manifest_days,
            "missing_manifest_days": missing_manifest_days,
            "success_days": _int(manifest.get("success_days")),
            "partial_days": _int(manifest.get("partial_days")),
            "empty_days": _int(manifest.get("empty_days")),
            "failed_days": _int(manifest.get("failed_days")),
            "running_days": _int(manifest.get("running_days")),
            "incomplete_days": incomplete_days,
            "manifest_incomplete_days": manifest_incomplete_days,
            "kline_rows": expected_rows,
            "manifest_expected_rows": _int(manifest.get("expected_rows")),
            "manifest_matched_rows": _int(manifest.get("matched_rows")),
            "matched_rows": matched_rows,
            "missing_rows": _int(actual.get("missing_rows")),
            "coverage_ratio": round(coverage_ratio, 8),
            "minimum_partition_coverage_ratio": round(
                _float(actual.get("minimum_partition_coverage_ratio")), 8
            ),
            "manifest_minimum_partition_coverage_ratio": round(
                _float(manifest.get("minimum_partition_coverage_ratio")), 8
            ),
            "factor_rows": _int(factor_range.get("factor_rows")),
            "min_factor_date": _date_text(factor_range.get("min_factor_date"))
            if factor_range.get("min_factor_date")
            else None,
            "max_factor_date": _date_text(factor_range.get("max_factor_date"))
            if factor_range.get("max_factor_date")
            else None,
            "invalid_factor_rows": invalid_rows,
            "future_factor_rows": future_rows,
            "missing_factor_policy": "fail_closed_per_candidate_path",
            "return_formula": "end_price*end_factor/(start_price*start_factor)-1",
        }

    def corporate_action_samples(
        self,
        start_date: str,
        end_date: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    WITH ordered AS (
                        SELECT code, trade_date, adj_factor,
                               LAG(trade_date) OVER (PARTITION BY code ORDER BY trade_date) AS previous_trade_date,
                               LAG(adj_factor) OVER (PARTITION BY code ORDER BY trade_date) AS previous_factor
                        FROM adj_factor_daily
                        WHERE trade_date BETWEEN %s AND %s
                    )
                    SELECT o.code, o.previous_trade_date, o.trade_date,
                           o.previous_factor, o.adj_factor,
                           previous_bar.close AS previous_close,
                           current_bar.close AS current_close
                    FROM ordered o
                    INNER JOIN daily_kline previous_bar
                      ON previous_bar.code=o.code AND previous_bar.trade_date=o.previous_trade_date
                    INNER JOIN daily_kline current_bar
                      ON current_bar.code=o.code AND current_bar.trade_date=o.trade_date
                    WHERE o.previous_factor IS NOT NULL
                      AND ABS(o.adj_factor-o.previous_factor) > 0.00000001
                    ORDER BY o.trade_date DESC,
                             ABS(o.adj_factor/o.previous_factor-1) DESC
                    LIMIT %s
                    """,
                    (start_date, end_date, int(limit)),
                )
                rows = cursor.fetchall() or []

        samples = []
        for row in rows:
            previous_close = _float(row.get("previous_close"))
            current_close = _float(row.get("current_close"))
            previous_factor = _float(row.get("previous_factor"))
            current_factor = _float(row.get("adj_factor"))
            samples.append(
                {
                    "code": row.get("code"),
                    "previous_trade_date": _date_text(row.get("previous_trade_date")),
                    "trade_date": _date_text(row.get("trade_date")),
                    "previous_factor": previous_factor,
                    "factor": current_factor,
                    "factor_change_pct": round((current_factor / previous_factor - 1) * 100, 6),
                    "raw_close_return_pct": round(
                        (current_close / previous_close - 1) * 100, 6
                    ),
                    "adjusted_total_return_pct": round(
                        adjusted_total_return(
                            previous_close,
                            previous_factor,
                            current_close,
                            current_factor,
                        )
                        * 100,
                        6,
                    ),
                }
            )
        return samples
