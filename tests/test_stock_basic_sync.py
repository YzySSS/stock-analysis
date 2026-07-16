from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from app.data_ingestion.stock_basic_sync import StockBasicSync


class FakeFrame:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.empty = not rows

    def to_dict(self, orient: str) -> list[dict]:
        if orient != "records":
            raise AssertionError(f"unexpected orient: {orient}")
        return list(self.rows)


class StockBasicSyncTests(unittest.TestCase):
    def test_nan_industry_is_normalized_to_null(self):
        self.assertIsNone(StockBasicSync.normalize_industry(float("nan")))
        self.assertIsNone(StockBasicSync.normalize_industry("nan"))
        self.assertEqual(StockBasicSync.normalize_industry("  银行  "), "银行")

    def test_delisted_codes_are_normalized_without_importing_records(self):
        sync = object.__new__(StockBasicSync)
        sync.pro = MagicMock()
        sync.pro.stock_basic.return_value = FakeFrame(
            [{"ts_code": "000004.SZ"}, {"ts_code": "600193.SH"}, {"ts_code": None}]
        )

        self.assertEqual(sync.fetch_delisted_codes(), ["sh.600193", "sz.000004"])
        sync.pro.stock_basic.assert_called_once_with(exchange="", list_status="D", fields="ts_code")

    def test_run_marks_delistings_after_refreshing_current_list(self):
        sync = object.__new__(StockBasicSync)
        sync.fetch_stock_basic = MagicMock(return_value=[object(), object()])
        sync.fetch_delisted_codes = MagicMock(return_value=["sz.000004"])
        sync.save_to_mysql = MagicMock(return_value=2)
        sync.mark_existing_delisted = MagicMock(return_value=1)
        sync.supplement_from_realtime_snapshot = MagicMock(return_value=0)

        self.assertEqual(sync.run(), 3)
        sync.mark_existing_delisted.assert_called_once_with(["sz.000004"])


if __name__ == "__main__":
    unittest.main()
