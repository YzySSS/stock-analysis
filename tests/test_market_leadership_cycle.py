from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.market_timing.leadership_cycle import (
    build_sector_price_series,
    classify_cycle,
    compute_breadth_metrics,
    compute_price_metrics,
    leadership_cycle_spec_hash,
    load_leadership_cycle_spec,
)


class MarketLeadershipCycleTests(unittest.TestCase):
    def test_spec_freezes_double_axis_and_no_raw_etf_price(self) -> None:
        spec = load_leadership_cycle_spec()

        self.assertEqual("market_leadership_cycle_v2", spec["model_id"])
        self.assertEqual("2.0.0", spec["version"])
        self.assertEqual(64, len(leadership_cycle_spec_hash()))
        self.assertTrue(
            any("separate axes" in item for item in spec["guardrails"])
        )
        self.assertTrue(
            any("never used as market breadth" in item for item in spec["guardrails"])
        )

    def test_post_impulse_rebound_is_not_called_seed_or_main_up(self) -> None:
        start = date(2026, 4, 1)
        values = []
        for index in range(65):
            if index <= 40:
                value = 100 + index * 1.5
            elif index <= 58:
                value = 160 - (index - 40) * 3.2
            else:
                value = 102.4 + (index - 58) * 1.2
            values.append(
                {
                    "trade_date": (start + timedelta(days=index)).isoformat(),
                    "value": value,
                }
            )

        metrics = compute_price_metrics(values)
        cycle = classify_cycle(metrics)

        self.assertEqual("ready", metrics["status"])
        self.assertGreater(metrics["prior_runup_to_high_pct"], 20)
        self.assertLess(metrics["post_high_drawdown_pct"], -15)
        self.assertEqual("rebound_candidate", cycle["cycle_state"])
        self.assertIn("B浪候选", cycle["cycle_label"])

    def test_real_breadth_does_not_use_news_coverage_count(self) -> None:
        rows = []
        for index in range(100):
            rows.append(
                {
                    "industry": "半导体",
                    "latest_close": 110 if index < 20 else 90,
                    "ma20": 100 if index < 10 else 120,
                    "ma60": 100 if index < 20 else 95,
                    "pct_chg_1d": 1 if index < 80 else -1,
                    "return_20d_pct": 1 if index < 10 else -1,
                    "kline_count_60": 60,
                }
            )

        breadth = compute_breadth_metrics(rows, ["半导体"])

        self.assertEqual("ready", breadth["status"])
        self.assertEqual(10.0, breadth["above_ma20_pct"])
        self.assertEqual(20.0, breadth["above_ma60_pct"])
        self.assertLess(breadth["score"], 35)

    def test_composite_theme_uses_equal_weight_returns_not_index_levels(self) -> None:
        rows = []
        start = date(2026, 1, 1)
        for index in range(65):
            current = (start + timedelta(days=index)).isoformat()
            rows.extend(
                [
                    {
                        "trade_date": current,
                        "industry_name": "银行",
                        "close": 1000 + index,
                        "pct_change": 1.0,
                    },
                    {
                        "trade_date": current,
                        "industry_name": "证券",
                        "close": 10000 + index,
                        "pct_change": -1.0,
                    },
                ]
            )

        series, lineage = build_sector_price_series(
            rows,
            ["银行", "证券"],
        )

        self.assertEqual("equal_weight_ths_industry_return_index", lineage["proxy_type"])
        self.assertEqual(65, len(series))
        self.assertAlmostEqual(100.0, series[-1]["value"], places=6)


if __name__ == "__main__":
    unittest.main()
