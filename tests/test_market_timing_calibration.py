from __future__ import annotations

import unittest

from app.market_timing.calibration import (
    DAILY_WEIGHTS,
    calibrate_indicator_score,
    compose_timing_state,
)


def indicator(indicator_id: str, score: float) -> dict:
    return {
        "indicator_id": indicator_id,
        "score": score,
        "signal": 1 if score >= 65 else -1 if score <= 40 else 0,
    }


class MarketTimingCalibrationTests(unittest.TestCase):
    def test_structural_futures_short_baseline_is_centered_by_rolling_rank(self):
        history = [-0.090 + index * 0.001 for index in range(40)]

        score, details = calibrate_indicator_score(
            "futures_holding_net",
            33.5,
            -0.0705,
            history_values=history,
            source_status="已接入",
            source_trade_date="2026-07-23",
            target_trade_date="2026-07-23",
        )

        self.assertIsNotNone(score)
        self.assertGreater(score, 45)
        self.assertLess(score, 55)
        self.assertEqual(details["calibration_method"], "rolling_percentile")

    def test_extreme_iv_skew_remains_bearish_after_relative_calibration(self):
        history = [0.020 + index * 0.001 for index in range(40)]

        score, _ = calibrate_indicator_score(
            "iv_skew",
            25.0,
            0.080,
            history_values=history,
            source_status="已接入",
            source_trade_date="2026-07-23",
            target_trade_date="2026-07-23",
        )

        self.assertIsNotNone(score)
        self.assertLess(score, 40)

    def test_previous_close_factor_is_shrunk_toward_neutral(self):
        fresh, _ = calibrate_indicator_score(
            "option_pcr",
            20.0,
            1.2,
            source_status="已接入",
            source_trade_date="2026-07-23",
            target_trade_date="2026-07-23",
        )
        stale, details = calibrate_indicator_score(
            "option_pcr",
            20.0,
            1.2,
            source_status="沿用最近收盘",
            source_trade_date="2026-07-22",
            target_trade_date="2026-07-23",
        )

        self.assertEqual(fresh, 20.0)
        self.assertGreater(stale, fresh)
        self.assertEqual(details["freshness_multiplier"], 0.7)

    def test_correlated_derivative_factors_cast_one_dimension_vote(self):
        indicators = [
            indicator("index_bollinger", 50),
            indicator("multi_index_trend", 50),
            indicator("index_pe_percentile", 50),
            indicator("erp", 50),
            indicator("margin_buy_ratio", 50),
            indicator("option_pcr", 20),
            indicator("qvix_volatility", 20),
            indicator("iv_skew", 20),
            indicator("futures_holding_net", 20),
            indicator("up_down_amount_pressure", 50),
        ]

        result = compose_timing_state(indicators, weights=DAILY_WEIGHTS)

        self.assertEqual(result["dimension_signals"]["derivatives"], -1)
        self.assertEqual(result["dimension_vote_sum"], -1)
        self.assertEqual(result["state"], "cautious")

    def test_independent_bullish_dimensions_can_reach_risk_on(self):
        indicators = [
            indicator("index_bollinger", 62),
            indicator("multi_index_trend", 64),
            indicator("index_pe_percentile", 53),
            indicator("erp", 57),
            indicator("margin_buy_ratio", 64),
            indicator("option_pcr", 52),
            indicator("qvix_volatility", 50),
            indicator("iv_skew", 50),
            indicator("futures_holding_net", 50),
            indicator("up_down_amount_pressure", 64),
        ]

        result = compose_timing_state(indicators, weights=DAILY_WEIGHTS)

        self.assertEqual(result["state"], "risk_on")
        self.assertEqual(result["position_upper_pct"], 80)

    def test_strong_cross_dimension_confirmation_can_reach_full_position_cap(self):
        indicators = [
            indicator("index_bollinger", 78),
            indicator("multi_index_trend", 82),
            indicator("index_pe_percentile", 68),
            indicator("erp", 72),
            indicator("margin_buy_ratio", 75),
            indicator("option_pcr", 67),
            indicator("qvix_volatility", 70),
            indicator("iv_skew", 66),
            indicator("futures_holding_net", 72),
            indicator("up_down_amount_pressure", 84),
        ]

        result = compose_timing_state(indicators, weights=DAILY_WEIGHTS)

        self.assertEqual(result["state"], "strong_risk_on")
        self.assertEqual(result["position_upper_pct"], 100)
        self.assertEqual(result["combined_signal"], 1)

    def test_broad_independent_weakness_stays_defensive(self):
        indicators = [
            indicator("index_bollinger", 28),
            indicator("multi_index_trend", 32),
            indicator("index_pe_percentile", 35),
            indicator("erp", 38),
            indicator("margin_buy_ratio", 30),
            indicator("option_pcr", 45),
            indicator("qvix_volatility", 35),
            indicator("iv_skew", 40),
            indicator("futures_holding_net", 38),
            indicator("up_down_amount_pressure", 25),
        ]

        result = compose_timing_state(indicators, weights=DAILY_WEIGHTS)

        self.assertEqual(result["state"], "defensive")
        self.assertEqual(result["position_upper_pct"], 15)


if __name__ == "__main__":
    unittest.main()
