from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

from app.read_models.materialization import (
    LocalReadModelMaterializer,
    build_operational_status_rows,
    build_realtime_rank_rows,
    build_tracking_summary_rows,
)


class _ScriptedCursor:
    def __init__(self, *, fetchone_values=None, fetchall_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executed: list[tuple[str, object]] = []
        self.executed_many: list[tuple[str, list[tuple]]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(str(sql).split()), params))
        return 0

    def executemany(self, sql, params):
        materialized = list(params)
        self.executed_many.append((" ".join(str(sql).split()), materialized))
        self.rowcount = len(materialized)
        return self.rowcount

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _factory_for(cursor):
    @contextmanager
    def factory(**_kwargs):
        yield _FakeConnection(cursor)

    return factory


class RealtimeRankReadModelTests(unittest.TestCase):
    def test_rank_snapshot_is_deterministic_and_keeps_one_source_batch(self):
        quote_time = datetime(2026, 7, 21, 10, 4)
        realtime = [
            {
                "code": "000002.SZ",
                "name": "B",
                "trade_date": date(2026, 7, 21),
                "quote_time": quote_time,
                "pct_chg": 3,
                "amount": 200,
                "latest_price": 12,
                "batch_id": "batch-1",
                "source": "local_quote",
            },
            {
                "code": "000001.SZ",
                "name": "A",
                "trade_date": date(2026, 7, 21),
                "quote_time": quote_time,
                "pct_chg": 5,
                "amount": 100,
                "latest_price": 10,
                "batch_id": "batch-1",
                "source": "local_quote",
            },
        ]
        first_id, first = build_realtime_rank_rows(
            realtime_rows=realtime,
            moneyflow_rows=[],
            popularity_rows=[],
            source_batch_id="batch-1",
            limit=2,
        )
        second_id, second = build_realtime_rank_rows(
            realtime_rows=realtime,
            moneyflow_rows=[],
            popularity_rows=[],
            source_batch_id="batch-1",
            limit=2,
        )

        self.assertEqual(first_id, second_id)
        self.assertEqual(first, second)
        gain = [row for row in first if row["rank_type"] == "pct_chg_top"]
        self.assertEqual([row["code"] for row in gain], ["000001.SZ", "000002.SZ"])
        self.assertTrue(all(row["source_batch_id"] == "batch-1" for row in first))

    def test_rank_score_is_bounded_when_raw_market_amount_exceeds_decimal_range(self):
        quote_time = datetime(2026, 7, 21, 15, 0)
        raw_amount = 25_000_000_000.0
        _snapshot_id, rows = build_realtime_rank_rows(
            realtime_rows=[
                {
                    "code": "600000.SH",
                    "name": "A",
                    "trade_date": date(2026, 7, 21),
                    "quote_time": quote_time,
                    "pct_chg": 1.5,
                    "amount": raw_amount,
                    "latest_price": 10,
                    "batch_id": "batch-large-amount",
                    "source": "local_quote",
                }
            ],
            moneyflow_rows=[],
            popularity_rows=[],
            source_batch_id="batch-large-amount",
            limit=100,
        )

        amount_row = next(row for row in rows if row["rank_type"] == "amount_top")
        self.assertEqual(amount_row["amount"], raw_amount)
        self.assertEqual(amount_row["rank_score"], 100.0)
        self.assertEqual(json.loads(amount_row["metrics_json"])["raw_rank_value"], raw_amount)

    def test_refresh_deletes_same_snapshot_before_atomic_batch_insert(self):
        quote_time = datetime(2026, 7, 21, 10, 4)
        cursor = _ScriptedCursor(
            fetchone_values=[
                {
                    "trade_date": date(2026, 7, 21),
                    "batch_id": "batch-1",
                    "quote_time": quote_time,
                    "source": "local_quote",
                    "row_count": 1,
                }
            ],
            fetchall_values=[
                [
                    {
                        "code": "000001.SZ",
                        "name": "A",
                        "trade_date": date(2026, 7, 21),
                        "quote_time": quote_time,
                        "latest_price": 10,
                        "pct_chg": 5,
                        "amount": 100,
                        "batch_id": "batch-1",
                        "source": "local_quote",
                    }
                ],
                [],
                [],
            ],
        )
        result = LocalReadModelMaterializer(_factory_for(cursor)).refresh_realtime_rank(limit=10)

        delete_index = next(
            index
            for index, (sql, _params) in enumerate(cursor.executed)
            if sql.startswith("DELETE FROM stock_realtime_rank_snapshot")
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["published_rows"], 2)
        self.assertEqual(len(cursor.executed_many), 1)
        self.assertGreaterEqual(delete_index, 0)
        self.assertIn("LEFT(batch_id, 18) = 'realtime_snapshot_'", cursor.executed[0][0])
        self.assertIn("INSERT INTO stock_realtime_rank_snapshot", cursor.executed_many[0][0])
        retention_sql = [
            (sql, params)
            for sql, params in cursor.executed
            if "INTERVAL 3 DAY" in sql
        ]
        self.assertEqual(len(retention_sql), 1)
        self.assertIn("DELETE FROM stock_realtime_rank_snapshot", retention_sql[0][0])
        self.assertEqual(retention_sql[0][1], (date(2026, 7, 21),))
        self.assertEqual(result["retention_days"], 3)


class TrackingSummaryReadModelTests(unittest.TestCase):
    def test_computes_maturity_win_rate_and_documents_missing_excess_returns(self):
        rows = [
            {
                "selection_result_id": 1,
                "trade_date": date(2026, 7, 1),
                "strategy_id": "a_share_sentiment",
                "registry_version": "0.4.4",
                "instrument_type": "stock",
                "code": "000001.SZ",
                "entry_price": 10,
                "close_1d": 11,
                "close_3d": 9,
                "close_5d": 12,
                "close_20d": 13,
                "metadata_json": '{"strategy_version":"0.4.4","signal_grade":"tradable"}',
                "created_at": datetime(2026, 7, 1, 16),
                "latest_future_date": date(2026, 7, 21),
                "include_in_stats": 1,
            },
            {
                "selection_result_id": 2,
                "trade_date": date(2026, 7, 2),
                "strategy_id": "a_share_sentiment",
                "registry_version": "0.4.4",
                "instrument_type": "stock",
                "code": "000002.SZ",
                "entry_price": 20,
                "close_1d": 18,
                "close_3d": None,
                "close_5d": None,
                "close_20d": None,
                "metadata_json": '{"strategy_version":"0.4.4","signal_grade":"watch"}',
                "created_at": datetime(2026, 7, 2, 16),
                "latest_future_date": date(2026, 7, 3),
                "include_in_stats": 1,
            },
        ]
        summary = build_tracking_summary_rows(
            rows,
            summary_date=date(2026, 7, 21),
            calculated_at=datetime(2026, 7, 21, 19),
        )[0]

        self.assertEqual(summary["selection_count"], 2)
        self.assertEqual(summary["tradable_count"], 1)
        self.assertEqual(summary["matured_1d_count"], 2)
        self.assertEqual(summary["win_rate_1d_pct"], 50.0)
        self.assertEqual(summary["avg_return_1d_pct"], 0.0)
        self.assertEqual(summary["matured_3d_count"], 1)
        self.assertIsNone(summary["avg_excess_1d_pct"])
        self.assertIn("unavailable_no_replayable_benchmark_series", summary["summary_json"])

    def test_no_source_rows_atomically_clears_stale_summary_for_date(self):
        cursor = _ScriptedCursor(fetchall_values=[[]])
        result = LocalReadModelMaterializer(_factory_for(cursor)).refresh_tracking_summary(
            summary_date="2026-07-21"
        )

        self.assertEqual(result["status"], "no_data")
        self.assertEqual(len(cursor.executed_many), 0)
        self.assertTrue(
            any(sql.startswith("DELETE FROM tracking_summary_daily") for sql, _ in cursor.executed)
        )


class OperationalStatusReadModelTests(unittest.TestCase):
    def test_projects_stale_task_and_rejected_manifest_without_live_probe(self):
        captured_at = datetime(2026, 7, 21, 12, 34, 50)
        snapshot_id, rows = build_operational_status_rows(
            task_rows=[
                {
                    "task_name": "upstream",
                    "run_id": "run-1",
                    "status": "running",
                    "started_at": captured_at - timedelta(hours=2),
                    "last_success_at": captured_at - timedelta(hours=3),
                }
            ],
            manifest_rows=[
                {
                    "source_name": "mysql",
                    "dataset_name": "daily_kline",
                    "batch_id": "batch-2",
                    "quality_status": "rejected",
                    "quality_reason": "coverage below gate",
                    "received_at": captured_at - timedelta(minutes=5),
                    "source_event_time_max": captured_at - timedelta(minutes=6),
                    "coverage_ratio": 0.5,
                }
            ],
            captured_at=captured_at,
        )

        self.assertEqual(snapshot_id, "ops-202607211234")
        by_type = {row["component_type"]: row for row in rows}
        self.assertEqual(by_type["task"]["status"], "stale")
        self.assertEqual(by_type["task"]["severity"], "warning")
        self.assertEqual(by_type["dataset"]["status"], "failed")
        self.assertEqual(by_type["dataset"]["error_code"], "SOURCE_BATCH_REJECTED")

    def test_empty_sources_publish_explicit_unknown_skeleton(self):
        snapshot_id, rows = build_operational_status_rows(
            task_rows=[],
            manifest_rows=[],
            captured_at=datetime(2026, 7, 21, 12, 34),
        )
        self.assertEqual(snapshot_id, "ops-202607211234")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "unknown")
        self.assertEqual(rows[0]["error_code"], "NO_LOCAL_STATUS_INPUT")

    def test_refresh_replaces_same_minute_snapshot_before_insert(self):
        cursor = _ScriptedCursor(fetchall_values=[[], []])
        result = LocalReadModelMaterializer(_factory_for(cursor)).refresh_operational_status(
            captured_at="2026-07-21T12:34:59"
        )

        self.assertEqual(result["snapshot_id"], "ops-202607211234")
        self.assertEqual(result["published_rows"], 1)
        self.assertTrue(
            any(
                sql.startswith("DELETE FROM operational_status_snapshot")
                for sql, _ in cursor.executed
            )
        )
        self.assertIn("INSERT INTO operational_status_snapshot", cursor.executed_many[0][0])
        retention_sql = [
            (sql, params)
            for sql, params in cursor.executed
            if "INTERVAL 7 DAY" in sql
        ]
        self.assertEqual(len(retention_sql), 1)
        self.assertIn("DELETE FROM operational_status_snapshot", retention_sql[0][0])
        self.assertEqual(retention_sql[0][1], (datetime(2026, 7, 21, 12, 34),))
        self.assertEqual(result["retention_days"], 7)


class LocalReadModelBoundaryTests(unittest.TestCase):
    def test_production_module_and_cli_have_no_external_provider_imports(self):
        source = Path("app/read_models/materialization.py").read_text(encoding="utf-8").lower()
        cli = Path("scripts/refresh_operational_read_models.py").read_text(encoding="utf-8").lower()
        for forbidden in ("import akshare", "import tushare", "import requests", "import tavily"):
            self.assertNotIn(forbidden, source)
            self.assertNotIn(forbidden, cli)


if __name__ == "__main__":
    unittest.main()
