from __future__ import annotations

import unittest

from app.stock_selection.sentiment_v06_evaluation import (
    evaluation_denominator_counts,
    load_sentiment_v06_evaluation_spec,
    preserve_first_terminal_outcome,
    simulate_sentiment_v06_execution,
    sentiment_v06_evaluation_spec_hash,
)


class SentimentV06EvaluationProtocolTests(unittest.TestCase):
    def test_protocol_is_checksum_locked_and_not_automatically_active(self):
        spec = load_sentiment_v06_evaluation_spec()

        self.assertEqual(
            sentiment_v06_evaluation_spec_hash(),
            "037f57554eaa79dbf849d6f78e84714ec0235ec1e67b9c1bd1af816864582eb5",
        )
        self.assertEqual(spec["status"], "preregistered_not_started")
        self.assertEqual(spec["decision_groups"], ["11:00:00", "13:30:00", "14:30:00"])
        self.assertEqual(spec["horizons_trade_days"]["primary"], 3)
        self.assertTrue(spec["manual_runs"]["must_not_merge_with_fixed_groups"])
        self.assertFalse(spec["promotion"]["automatic"])
        self.assertFalse(spec["promotion"]["schedule_enabled"])
        self.assertTrue(spec["execution"]["t_plus_one_sellable"])
        self.assertTrue(spec["execution"]["terminal_events_immutable"])
        self.assertTrue(spec["execution"]["blocked_exit_carries_forward"])
        self.assertEqual(spec["execution"]["minimum_entry_delay_seconds"], 60)
        self.assertTrue(spec["execution"]["round_trip_cost_includes_slippage"])

    def test_entry_before_preregistered_latency_is_deferred(self):
        outcome = simulate_sentiment_v06_execution(
            decision_as_of="2026-09-11 11:00:00",
            valid_until="2026-09-11 11:05:00",
            entry_quotes=[
                {
                    "quote_time": "2026-09-11 11:00:30",
                    "received_at": "2026-09-11 11:00:31",
                    "latest_price": 9.9,
                },
                {
                    "quote_time": "2026-09-11 11:01:00",
                    "received_at": "2026-09-11 11:01:01",
                    "latest_price": 10.0,
                },
            ],
            daily_bars=[
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-11",
                    "open": 9.8,
                    "high": 10.2,
                    "low": 9.7,
                    "close": 10.0,
                    "prev_close": 9.8,
                },
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-14",
                    "open": 10.1,
                    "high": 10.4,
                    "low": 10.0,
                    "close": 10.3,
                    "prev_close": 10.0,
                },
            ],
            horizon_trade_days=1,
            observed_through="2026-09-14 15:00:00",
        )

        self.assertEqual(outcome["status"], "complete")
        self.assertEqual(outcome["entry_time"], "2026-09-11 11:01:00")
        self.assertEqual(outcome["events"][0]["event_type"], "entry_deferred")
        self.assertEqual(outcome["implementation_cost_pct"], 0.25)
        self.assertTrue(outcome["cost_includes_slippage"])

    def test_aware_decision_clock_accepts_naive_shanghai_execution_quotes(self):
        outcome = simulate_sentiment_v06_execution(
            decision_as_of="2026-09-11T11:00:00+08:00",
            valid_until="2026-09-11 11:05:00",
            entry_quotes=[
                {
                    "quote_time": "2026-09-11 11:01:00",
                    "received_at": "2026-09-11 11:01:01",
                    "latest_price": 10.0,
                }
            ],
            daily_bars=[
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-11",
                    "open": 9.8,
                    "high": 10.2,
                    "low": 9.7,
                    "close": 10.0,
                    "prev_close": 9.8,
                },
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-14",
                    "open": 10.1,
                    "high": 10.4,
                    "low": 10.0,
                    "close": 10.3,
                    "prev_close": 10.0,
                },
            ],
            horizon_trade_days=1,
            observed_through="2026-09-14 15:00:00",
        )

        self.assertEqual(outcome["status"], "complete")
        self.assertEqual(outcome["entry_time"], "2026-09-11 11:01:00")

    def test_one_price_limit_up_quote_is_not_invented_as_a_fill(self):
        outcome = simulate_sentiment_v06_execution(
            decision_as_of="2026-09-11 11:00:00",
            valid_until="2026-09-11 11:05:00",
            entry_quotes=[
                {
                    "code": "sh.600001",
                    "quote_time": "2026-09-11 11:01:00",
                    "received_at": "2026-09-11 11:01:01",
                    "latest_price": 11.0,
                    "open_price": 11.0,
                    "high_price": 11.0,
                    "low_price": 11.0,
                    "pre_close": 10.0,
                }
            ],
            daily_bars=[],
            horizon_trade_days=3,
            observed_through="2026-09-11 11:06:00",
        )

        self.assertEqual(outcome["status"], "unfilled")
        self.assertTrue(outcome["terminal"])
        self.assertEqual(outcome["events"][0]["event_type"], "entry_blocked")
        self.assertEqual(
            outcome["events"][0]["block_reason"], "buy_blocked_limit_up"
        )

    def test_observed_through_caps_future_entry_quotes_and_bars(self):
        outcome = simulate_sentiment_v06_execution(
            decision_as_of="2026-09-11 11:00:00",
            valid_until="2026-09-11 11:05:00",
            entry_quotes=[
                {
                    "quote_time": "2026-09-11 11:04:00",
                    "received_at": "2026-09-11 11:04:01",
                    "latest_price": 10.0,
                }
            ],
            daily_bars=[],
            horizon_trade_days=3,
            observed_through="2026-09-11 11:02:00",
        )

        self.assertEqual(outcome["status"], "pending_entry")
        self.assertEqual(outcome["events"], [])

    def test_t12_t_plus_one_limit_down_exit_is_delayed_to_real_executable_price(self):
        outcome = simulate_sentiment_v06_execution(
            decision_as_of="2026-09-11 11:00:00",
            valid_until="2026-09-11 11:05:00",
            entry_quotes=[
                {
                    "quote_time": "2026-09-11 11:01:00",
                    "received_at": "2026-09-11 11:01:02",
                    "latest_price": 10.0,
                }
            ],
            daily_bars=[
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-11",
                    "open": 9.8,
                    "high": 10.4,
                    "low": 9.7,
                    "close": 10.2,
                    "prev_close": 9.8,
                },
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-14",
                    "open": 9.18,
                    "high": 9.18,
                    "low": 9.18,
                    "close": 9.18,
                    "prev_close": 10.2,
                },
                {
                    "code": "sh.600001",
                    "trade_date": "2026-09-15",
                    "open": 8.90,
                    "high": 9.10,
                    "low": 8.70,
                    "close": 9.00,
                    "prev_close": 9.18,
                },
            ],
            horizon_trade_days=1,
            observed_through="2026-09-15 15:00:00",
        )

        self.assertEqual(outcome["status"], "complete")
        self.assertEqual(outcome["target_exit_trade_date"], "2026-09-14")
        self.assertEqual(outcome["exit_trade_date"], "2026-09-15")
        self.assertEqual(outcome["exit_price"], 8.9)
        self.assertLessEqual(outcome["exit_price"], 9.1)
        self.assertEqual(
            [event["event_type"] for event in outcome["events"]],
            ["entry_filled", "exit_blocked", "exit_filled"],
        )
        self.assertEqual(
            outcome["events"][1]["block_reason"],
            "sell_blocked_limit_down",
        )

    def test_t13_first_terminal_outcome_cannot_be_rewritten(self):
        existing = {
            "status": "complete",
            "terminal": True,
            "exit_price": 9.0,
            "net_return_pct": -10.25,
        }
        later = {
            "status": "complete",
            "terminal": True,
            "exit_price": 12.0,
            "net_return_pct": 19.75,
        }

        preserved = preserve_first_terminal_outcome(existing, later)

        self.assertEqual(preserved["exit_price"], 9.0)
        self.assertEqual(preserved["net_return_pct"], -10.25)
        self.assertTrue(preserved["immutable_reused"])

    def test_t14_empty_failed_untriggered_and_unfilled_remain_denominators(self):
        counts = evaluation_denominator_counts(
            [
                {
                    "status": "success",
                    "recalled_count": 0,
                    "scored_count": 0,
                    "displayed_count": 0,
                },
                {
                    "status": "data_failed",
                    "recalled_count": 2,
                    "scored_count": 2,
                    "displayed_count": 1,
                    "conditions_met_count": 1,
                    "untriggered_count": 1,
                    "unfilled_count": 1,
                    "filled_count": 0,
                },
            ]
        )

        self.assertEqual(counts["all_decisions"], 2)
        self.assertEqual(counts["empty_decisions"], 1)
        self.assertEqual(counts["data_failed_decisions"], 1)
        self.assertEqual(counts["untriggered_candidates"], 1)
        self.assertEqual(counts["unfilled_candidates"], 1)
        self.assertEqual(counts["filled_simulated_entries"], 0)


if __name__ == "__main__":
    unittest.main()
