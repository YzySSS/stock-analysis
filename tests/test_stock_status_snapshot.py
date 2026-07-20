from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.refresh_stock_status_snapshot import build_snapshot_rows, normalize_stock_code


class StockStatusSnapshotTests(unittest.TestCase):
    def test_market_code_normalization_keeps_bse_codes_in_bj_namespace(self):
        self.assertEqual(normalize_stock_code("920685", "北京证券交易所"), "bj.920685")
        self.assertEqual(normalize_stock_code("688001", "上海证券交易所"), "sh.688001")
        self.assertEqual(normalize_stock_code("300001", "深圳证券交易所"), "sz.300001")

    def test_snapshot_builder_uses_source_market_for_suspensions(self):
        with (
            patch("scripts.refresh_stock_status_snapshot.fetch_paused_listing_map", return_value={}),
            patch(
                "scripts.refresh_stock_status_snapshot.fetch_recent_suspension_map",
                return_value={
                    "920685": {
                        "reason": "刊登重要公告",
                        "suspension_date": "2026-07-16",
                        "resume_date": "2026-07-29",
                        "expected_resume_date": "2026-07-30",
                        "market_name": "北京证券交易所",
                    }
                },
            ),
        ):
            rows = build_snapshot_rows("2026-07-20")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].code, "bj.920685")
        self.assertEqual(rows[0].status_label, "suspended")


if __name__ == "__main__":
    unittest.main()
