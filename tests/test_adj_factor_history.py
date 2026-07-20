from __future__ import annotations

import math
import unittest
from pathlib import Path

import pandas as pd

from app.data_ingestion.adj_factor_history import (
    AdjFactorHistoryBackfill,
    adjusted_total_return,
    partition_status,
)
from app.data_ingestion.adj_factor_sync import AdjFactorSync
from app.orchestration.adj_factor_schema import ADJ_FACTOR_MANIFEST_DDL


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AdjFactorHistoryUnitTests(unittest.TestCase):
    def test_coverage_query_does_not_use_reserved_stored_alias(self):
        source = (PROJECT_ROOT / "app/data_ingestion/adj_factor_history.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("adj_factor_daily stored\n", source)
        self.assertIn("adj_factor_daily stored_factor", source)

    def test_partition_status_enforces_coverage_threshold(self):
        self.assertEqual(
            partition_status(
                expected_rows=1000,
                matched_rows=995,
                source_rows=1000,
                minimum_coverage_ratio=0.995,
            ),
            "success",
        )
        self.assertEqual(
            partition_status(
                expected_rows=1000,
                matched_rows=994,
                source_rows=1000,
                minimum_coverage_ratio=0.995,
            ),
            "partial_success",
        )
        self.assertEqual(
            partition_status(
                expected_rows=1000,
                matched_rows=0,
                source_rows=0,
                minimum_coverage_ratio=0.995,
            ),
            "empty",
        )

    def test_adjusted_total_return_neutralizes_a_two_for_one_split(self):
        self.assertAlmostEqual(adjusted_total_return(10.0, 1.0, 5.0, 2.0), 0.0)
        self.assertAlmostEqual(adjusted_total_return(10.0, 1.0, 5.5, 2.0), 0.1)
        for invalid in (0.0, -1.0, math.nan, math.inf):
            with self.assertRaises(ValueError):
                adjusted_total_return(10.0, 1.0, 5.0, invalid)

    def test_manifest_schema_records_resume_and_coverage_evidence(self):
        for field in (
            "trade_date DATE NOT NULL PRIMARY KEY",
            "status VARCHAR(24) NOT NULL",
            "coverage_ratio DECIMAL(12,8)",
            "attempt_count INT NOT NULL",
            "sync_run_id VARCHAR(64)",
        ):
            self.assertIn(field, ADJ_FACTOR_MANIFEST_DDL)

    def test_pending_run_reconciles_covered_dates_and_retries_only_gaps(self):
        class FakeHistory(AdjFactorHistoryBackfill):
            def __init__(self):
                self.reconciled: list[str] = []
                self.synced: list[str] = []

            def trade_dates(self, _start_date, _end_date):
                return ["2024-01-02", "2024-01-03", "2024-01-04"]

            def coverage_by_date(self, _start_date, _end_date):
                return {
                    "2024-01-02": {
                        "expected_rows": 1000,
                        "stored_rows": 1000,
                        "matched_rows": 999,
                        "missing_rows": 1,
                        "coverage_ratio": 0.999,
                    },
                    "2024-01-03": {
                        "expected_rows": 1000,
                        "stored_rows": 0,
                        "matched_rows": 0,
                        "missing_rows": 1000,
                        "coverage_ratio": 0.0,
                    },
                    "2024-01-04": {
                        "expected_rows": 1000,
                        "stored_rows": 0,
                        "matched_rows": 0,
                        "missing_rows": 1000,
                        "coverage_ratio": 0.0,
                    },
                }

            def successful_manifest_dates(self, *_args):
                return set()

            def _reconcile_existing(self, trade_date, _run_id, _coverage):
                self.reconciled.append(trade_date)

            def sync_date(self, trade_date, _run_id, _minimum_coverage_ratio):
                self.synced.append(trade_date)
                status = "success" if trade_date == "2024-01-03" else "empty"
                return {
                    "trade_date": trade_date,
                    "status": status,
                    "source_rows": 1000 if status == "success" else 0,
                    "saved_rows": 1000 if status == "success" else 0,
                }

        service = FakeHistory()
        result = service.run(
            "test-run",
            "2024-01-02",
            "2024-01-04",
            pause_seconds=0,
            max_failures=1,
        )

        self.assertEqual(service.reconciled, ["2024-01-02"])
        self.assertEqual(service.synced, ["2024-01-03", "2024-01-04"])
        self.assertEqual(result["skipped_existing_trade_days"], 1)
        self.assertEqual(result["success_trade_days"], 1)
        self.assertEqual(result["empty_trade_days"], 1)
        self.assertEqual(result["status"], "partial_success")

    def test_max_days_reports_partial_until_deferred_dates_are_resumed(self):
        class LimitedHistory(AdjFactorHistoryBackfill):
            def __init__(self):
                pass

            def trade_dates(self, _start_date, _end_date):
                return ["2024-01-02", "2024-01-03"]

            def coverage_by_date(self, _start_date, _end_date):
                return {}

            def successful_manifest_dates(self, *_args):
                return set()

            def sync_date(self, trade_date, _run_id, _minimum_coverage_ratio):
                return {
                    "trade_date": trade_date,
                    "status": "success",
                    "source_rows": 1,
                    "saved_rows": 1,
                }

        result = LimitedHistory().run(
            "test-run",
            "2024-01-02",
            "2024-01-03",
            pause_seconds=0,
            max_days=1,
        )

        self.assertEqual(result["processed_trade_days"], 1)
        self.assertEqual(result["deferred_trade_days"], 1)
        self.assertEqual(result["status"], "partial_success")

    def test_failure_path_does_not_mask_source_error_when_coverage_audit_fails(self):
        class FailingSource:
            @staticmethod
            def fetch_for_trade_date(_trade_date):
                raise RuntimeError("source unavailable")

        class FailingHistory(AdjFactorHistoryBackfill):
            def __init__(self):
                super().__init__(source=FailingSource())
                self.terminal = None

            def _mark_running(self, _trade_date, _run_id):
                return None

            def coverage_for_date(self, _trade_date):
                raise RuntimeError("audit unavailable")

            def _mark_terminal(self, *args, **kwargs):
                self.terminal = (args, kwargs)

        service = FailingHistory()
        result = service.sync_date("2024-01-02", "test-run", 0.995)

        self.assertEqual(result["status"], "failed")
        self.assertIn("source unavailable", result["error"])
        self.assertEqual(result["coverage_ratio"], 0.0)
        self.assertIsNotNone(service.terminal)

    def test_source_normalization_rejects_wrong_date_invalid_factor_and_duplicates(self):
        class FakePro:
            @staticmethod
            def adj_factor(**_kwargs):
                return pd.DataFrame(
                    [
                        {"ts_code": "600000.SH", "trade_date": "20240102", "adj_factor": 1.25},
                        {"ts_code": "600000.SH", "trade_date": "20240102", "adj_factor": 1.5},
                        {"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 1.0},
                        {"ts_code": "000002.SZ", "trade_date": "20240102", "adj_factor": 0.0},
                        {"ts_code": "000003.SZ", "trade_date": "20240102", "adj_factor": "bad"},
                        {"ts_code": None, "trade_date": "20240102", "adj_factor": 1.0},
                    ]
                )

        sync = object.__new__(AdjFactorSync)
        sync.pro = FakePro()
        records = sync.fetch_for_trade_date("2024-01-02")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].code, "sh.600000")
        self.assertEqual(records[0].trade_date, "2024-01-02")
        self.assertEqual(records[0].adj_factor, 1.5)


if __name__ == "__main__":
    unittest.main()
