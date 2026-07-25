from __future__ import annotations

import unittest

from app.etf_rotation.data_sync import merge_fund_rows


class EtfRotationDataSyncTests(unittest.TestCase):
    def test_merge_converts_units_and_uses_bounded_stale_nav(self) -> None:
        rows = merge_fund_rows(
            fund={
                "ts_code": "512800.SH",
                "name": "华宝中证银行ETF",
                "list_date": "20170803",
                "benchmark": "中证银行指数×100%",
            },
            daily_rows=[
                {
                    "trade_date": "20260724",
                    "open": 1.0,
                    "high": 1.03,
                    "low": 0.99,
                    "close": 1.02,
                    "pre_close": 1.0,
                    "change": 0.02,
                    "pct_chg": 2.0,
                    "vol": 100,
                    "amount": 1234.5,
                }
            ],
            share_rows=[
                {
                    "trade_date": "20260724",
                    "fd_share": 4567.8,
                }
            ],
            nav_rows=[
                {
                    "nav_date": "20260723",
                    "unit_nav": 1.0,
                    "accum_nav": 1.1,
                }
            ],
            maximum_nav_staleness_days=3,
        )

        self.assertEqual(1, len(rows))
        self.assertEqual(1_234_500, rows[0]["amount_yuan"])
        self.assertEqual(4567.8, rows[0]["fund_share_10k"])
        self.assertEqual("2026-07-23", rows[0]["nav_date"])
        self.assertAlmostEqual(2.0, rows[0]["premium_discount_pct"])

    def test_merge_does_not_carry_nav_beyond_contract(self) -> None:
        rows = merge_fund_rows(
            fund={
                "ts_code": "512800.SH",
                "name": "华宝中证银行ETF",
                "list_date": "20170803",
                "benchmark": "中证银行指数×100%",
            },
            daily_rows=[{"trade_date": "20260724", "close": 1.0}],
            share_rows=[],
            nav_rows=[{"nav_date": "20260718", "unit_nav": 1.0}],
            maximum_nav_staleness_days=3,
        )

        self.assertIsNone(rows[0]["nav_date"])
        self.assertIsNone(rows[0]["premium_discount_pct"])


if __name__ == "__main__":
    unittest.main()
