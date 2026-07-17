from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date

from app.data_ingestion.stock_status_pit_sync import (
    StockStatusPitSync,
    _to_float,
    classify_historical_market_rows,
    classify_name,
    from_ts_code,
    normalize_date,
)
from scripts.run_stock_status_pit_backfill import parse_stages, to_json_safe


class FakeFrame:
    def __init__(self, rows):
        self.rows = list(rows)
        self.empty = not self.rows

    def to_dict(self, orient):
        if orient != "records":
            raise AssertionError(orient)
        return list(self.rows)


class FakePro:
    def stock_basic(self, *, list_status, **_kwargs):
        rows = {
            "L": [
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "industry": "银行",
                    "market": "主板",
                    "list_date": "19991110",
                    "delist_date": None,
                }
            ],
            "D": [
                {
                    "ts_code": "000005.SZ",
                    "name": "ST星源(退)",
                    "industry": None,
                    "market": "主板",
                    "list_date": "19901210",
                    "delist_date": "20240426",
                }
            ],
            "P": [],
        }[list_status]
        return FakeFrame(rows)

    def suspend_d(self, **_kwargs):
        return FakeFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20240102",
                    "suspend_timing": "09:30-10:30",
                    "suspend_type": "S",
                }
            ]
        )


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executed_many = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return 0

    def executemany(self, sql, params):
        rows = list(params)
        self.executed_many.append((sql, rows))
        return len(rows)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def connection_factory(cursor):
    @contextmanager
    def _connect(*_args, **_kwargs):
        yield FakeConnection(cursor)

    return _connect


class StockStatusPitSyncTests(unittest.TestCase):
    def test_to_float_preserves_numeric_market_fields(self):
        self.assertEqual(_to_float("12.34"), 12.34)
        self.assertEqual(_to_float(5), 5.0)
        self.assertIsNone(_to_float(None))
        self.assertIsNone(_to_float("not-a-number"))

    def test_normalization_and_historical_name_flags(self):
        self.assertEqual(from_ts_code("600000.SH"), "sh.600000")
        self.assertEqual(from_ts_code("920680.BJ"), "bj.920680")
        self.assertEqual(normalize_date("20260717"), "2026-07-17")
        self.assertEqual(classify_name("*ST国华"), {"is_st": 1, "is_delisting_period": 0})
        self.assertEqual(
            classify_name("国华退", "退市整理期"),
            {"is_st": 0, "is_delisting_period": 1},
        )
        self.assertEqual(classify_historical_market_rows(10, 10), "complete")
        self.assertEqual(classify_historical_market_rows(0, 0), "incomplete")
        self.assertEqual(
            classify_historical_market_rows(0, 0, no_activity_confirmed=True),
            "source_confirmed_no_market_activity",
        )
        self.assertEqual(classify_historical_market_rows(10, 0), "incomplete")

    def test_task_payload_dates_are_json_safe(self):
        self.assertEqual(
            to_json_safe({"min_trade_date": date(2024, 1, 2)}),
            {"min_trade_date": "2024-01-02"},
        )

    def test_lifecycle_sync_keeps_live_and_historical_delisted_separate(self):
        cursor = FakeCursor()
        sync = StockStatusPitSync(
            pro=FakePro(),
            connection_factory=connection_factory(cursor),
        )

        result = sync.sync_lifecycle("pit-test")

        self.assertEqual(result["rows"], 2)
        lifecycle_batches = [
            rows for sql, rows in cursor.executed_many
            if "INSERT INTO stock_instrument_lifecycle" in sql
        ]
        self.assertEqual(len(lifecycle_batches), 1)
        rows_by_code = {row[0]: row for row in lifecycle_batches[0]}
        self.assertEqual(rows_by_code["sh.600000"][7], "L")
        self.assertEqual(rows_by_code["sz.000005"][7], "D")
        self.assertEqual(rows_by_code["sz.000005"][9], "2024-04-26")

    def test_name_rows_keep_open_intervals_and_flags_for_per_code_fallback(self):
        rows, skipped = StockStatusPitSync._name_rows(
            [
                {
                    "ts_code": "000005.SZ",
                    "name": "*ST星源",
                    "start_date": "20230101",
                    "end_date": None,
                    "ann_date": "20221231",
                    "change_reason": "ST",
                },
                {"ts_code": "600000.SH", "name": "", "start_date": "20240101"},
            ],
            "pit-test",
            "2026-07-17 16:00:00",
        )

        self.assertEqual(skipped, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "sz.000005")
        self.assertEqual(rows[0][2], "2023-01-01")
        self.assertIsNone(rows[0][3])
        self.assertEqual(rows[0][6], 1)
        self.assertEqual(rows[0][9], "pit-test")

    def test_suspension_sync_fetches_a_date_range_and_manifests_empty_days(self):
        cursor = FakeCursor()
        sync = StockStatusPitSync(
            pro=FakePro(),
            connection_factory=connection_factory(cursor),
        )

        result = sync.sync_suspensions(
            "pit-test",
            "2024-01-02",
            "2024-01-03",
            trade_dates=["2024-01-02", "2024-01-03"],
        )

        self.assertEqual(result["trade_dates"], 2)
        self.assertEqual(result["successful_dates"], 2)
        self.assertEqual(result["rows"], 1)
        self.assertEqual(result["source_rows"], 1)
        self.assertEqual(result["pages"], 1)
        suspension_batches = [
            rows for sql, rows in cursor.executed_many
            if "INSERT INTO stock_suspension_daily" in sql
        ]
        self.assertEqual(len(suspension_batches), 1)
        self.assertEqual(suspension_batches[0][0][0], "sh.600000")

    def test_stage_parser_is_explicit_and_rejects_unknown_work(self):
        self.assertEqual(parse_stages("all"), ["lifecycle", "names", "suspensions", "market-data"])
        self.assertEqual(parse_stages("lifecycle,names"), ["lifecycle", "names"])
        with self.assertRaisesRegex(ValueError, "unsupported PIT stages"):
            parse_stages("lifecycle,unknown")


if __name__ == "__main__":
    unittest.main()
