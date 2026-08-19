from __future__ import annotations

import unittest
from datetime import date

from app.market_timing.intraday_alert import build_intraday_market_risk_alert


CURRENT_DATE = date(2026, 8, 19)


def overview(**overrides) -> dict:
    payload = {
        "trade_date": "2026-08-19",
        "latest_quote_time": "2026-08-19 13:37:42",
        "total": 5_544,
        "expected_total": 5_545,
        "fresh_count": 5_544,
        "up_count": 453,
        "down_count": 5_047,
        "flat_count": 44,
        "avg_pct_chg": -3.40,
        "amount_weighted_pct_chg": -5.03,
        "up_amount": 226_488_727_261,
        "down_amount": 1_676_929_622_415,
        "strong_down_count": 1_408,
        "limit_up_like": 37,
        "limit_down_like": 55,
    }
    payload.update(overrides)
    return payload


class IntradayMarketRiskAlertTests(unittest.TestCase):
    def test_extreme_broad_selloff_is_red_but_never_blocks_selection(self):
        result = build_intraday_market_risk_alert(
            overview(),
            current_date=CURRENT_DATE,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["level"], "red")
        self.assertEqual(result["title"], "全市场宽度踩踏")
        self.assertFalse(result["blocking"])
        self.assertTrue(result["selection_allowed"])
        self.assertEqual(result["operation_mode"], "strong_warning_only")
        self.assertIn("选股功能仍可正常使用", result["action_label"])
        self.assertAlmostEqual(result["metrics"]["down_ratio"], 5_047 / 5_544, places=6)
        self.assertTrue(any("1408" in item for item in result["evidence"]))

    def test_opening_breadth_breakdown_is_orange_before_extreme_drawdown(self):
        result = build_intraday_market_risk_alert(
            overview(
                up_count=782,
                down_count=4_580,
                flat_count=182,
                avg_pct_chg=-1.29,
                amount_weighted_pct_chg=-2.39,
                up_amount=173,
                down_amount=827,
                strong_down_count=181,
            ),
            current_date=CURRENT_DATE,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["level"], "orange")
        self.assertEqual(result["title"], "全市场普跌")
        self.assertFalse(result["blocking"])

    def test_early_multi_factor_weakness_is_yellow(self):
        result = build_intraday_market_risk_alert(
            overview(
                up_count=1_250,
                down_count=4_050,
                flat_count=244,
                avg_pct_chg=-0.85,
                amount_weighted_pct_chg=-0.90,
                up_amount=300,
                down_amount=700,
                strong_down_count=150,
                limit_up_like=12,
                limit_down_like=20,
            ),
            current_date=CURRENT_DATE,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["level"], "yellow")

    def test_normal_market_has_no_warning(self):
        result = build_intraday_market_risk_alert(
            overview(
                up_count=2_900,
                down_count=2_400,
                flat_count=244,
                avg_pct_chg=0.12,
                amount_weighted_pct_chg=0.20,
                up_amount=550,
                down_amount=450,
                strong_down_count=30,
                limit_up_like=45,
                limit_down_like=8,
            ),
            current_date=CURRENT_DATE,
        )

        self.assertFalse(result["active"])
        self.assertEqual(result["level"], "none")
        self.assertTrue(result["selection_allowed"])

    def test_incomplete_snapshot_fails_closed_without_false_warning(self):
        result = build_intraday_market_risk_alert(
            overview(total=2_000, expected_total=5_545, fresh_count=1_900),
            current_date=CURRENT_DATE,
        )

        self.assertFalse(result["active"])
        self.assertEqual(result["level"], "none")
        self.assertFalse(result["data_quality"]["ready"])
        self.assertEqual(result["title"], "普跌预警数据暂不可用")

    def test_previous_trade_date_is_not_presented_as_live_warning(self):
        result = build_intraday_market_risk_alert(
            overview(trade_date="2026-08-18"),
            current_date=CURRENT_DATE,
        )

        self.assertFalse(result["active"])
        self.assertEqual(result["data_quality"]["status"], "stale_trade_date")


if __name__ == "__main__":
    unittest.main()
