from __future__ import annotations

import unittest

from app.data_ingestion.factor_input_history_sync import FactorInputHistorySync
from scripts.run_factor_input_daily_update import prefetch_daily_basic_maps


class FactorInputDailyUpdateTests(unittest.TestCase):
    def test_prefetch_calls_daily_basic_once_per_date_and_rejects_low_coverage(self):
        class FakeSync:
            def __init__(self):
                self.calls: list[str] = []

            def fetch_daily_basic_map(self, trade_date: str) -> dict:
                self.calls.append(trade_date)
                size = 8 if trade_date == "2026-07-15" else 7
                return {f"{index:06d}": {} for index in range(size)}

        sync = FakeSync()
        maps, coverage = prefetch_daily_basic_maps(
            sync,
            ["2026-07-15", "2026-07-16"],
            total_codes=10,
            min_coverage_ratio=0.8,
        )

        self.assertEqual(sync.calls, ["2026-07-15", "2026-07-16"])
        self.assertEqual(list(maps), ["2026-07-15"])
        self.assertTrue(coverage["2026-07-15"]["available"])
        self.assertFalse(coverage["2026-07-16"]["available"])

    def test_sync_uses_prefetched_daily_basic_without_refetching_upstream(self):
        sync = object.__new__(FactorInputHistorySync)
        sync.fetch_trade_dates = lambda *_args: (_ for _ in ()).throw(
            AssertionError("trade dates should use the supplied override")
        )
        sync.fetch_stock_codes = lambda limit=None, offset=0: ["sh.600000"]
        sync.fetch_stock_basic_snapshot = lambda _codes=None: {}
        sync.fetch_daily_basic_map = lambda _date: (_ for _ in ()).throw(
            AssertionError("daily_basic should use the prefetched map")
        )
        saved = []

        def save_records(records):
            rows = list(records)
            saved.extend(rows)
            return len(rows)

        sync.save_records = save_records

        result = sync.run(
            start_date="2026-07-16",
            end_date="2026-07-16",
            trade_dates_override=["2026-07-16"],
            daily_basic_maps={
                "2026-07-16": {
                    "600000": {
                        "pe_tushare": 8.5,
                        "pb_tushare": 1.1,
                        "turnover_rate": 2.3,
                    }
                }
            },
        )

        self.assertEqual(result["rows_synced"], 1)
        self.assertEqual(result["processed_days"], 1)
        self.assertEqual(saved[0].code, "sh.600000")
        self.assertEqual(saved[0].pe_tushare, 8.5)


if __name__ == "__main__":
    unittest.main()
