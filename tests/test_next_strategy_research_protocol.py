from __future__ import annotations

import hashlib
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHARTER_PATH = PROJECT_ROOT / "config/research_protocols/next_strategy_research_v1.yaml"
LOCK_PATH = PROJECT_ROOT / "config/research_protocols/next_strategy_research_v1.sha256"


class NextStrategyResearchProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw = CHARTER_PATH.read_bytes()
        cls.charter = yaml.safe_load(cls.raw)

    def test_charter_matches_immutable_hash_lock(self):
        expected = LOCK_PATH.read_text(encoding="utf-8").strip().split()[0]
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), expected)

    def test_truth_status_cannot_be_mistaken_for_validation(self):
        truth = self.charter["truth_status"]
        self.assertFalse(truth["is_validation_protocol"])
        self.assertFalse(truth["eligible_for_validation"])
        self.assertEqual(truth["validation_status"], "unvalidated")
        self.assertFalse(truth["historical_diagnostic_can_promote"])
        self.assertFalse(truth["auto_promote"])
        self.assertFalse(truth["auto_enable_for_selection"])
        self.assertFalse(truth["auto_make_default"])

    def test_candidate_is_independent_sparse_and_forbids_known_failure_patterns(self):
        family = self.charter["candidate_family"]
        question = self.charter["research_question"]
        self.assertNotIn(
            family["reserved_strategy_id"],
            {"lowvol_reversal", "v13_three_factor"},
        )
        self.assertEqual(question["minimum_picks"], 0)
        self.assertTrue(question["allow_cash"])
        self.assertFalse(question["force_fill_pick_count"])
        forbidden = set(family["forbidden_designs"])
        self.assertIn("lowvol_or_reversal_weight_tuning", forbidden)
        self.assertIn("v13_weight_or_threshold_tuning", forbidden)
        self.assertIn("absolute_share_price_alpha_bonus", forbidden)
        self.assertIn("best_offset_selection_after_results", forbidden)
        self.assertIn("diagnostic_window_parameter_tuning", forbidden)

    def test_point_in_time_and_execution_contracts_are_explicit(self):
        data = self.charter["data_contract"]
        execution = self.charter["execution_contract"]
        self.assertEqual(data["unknown_state_policy"], "fail_closed")
        self.assertIn("stock_fundamental_pit", data["required_point_in_time_inputs"])
        self.assertEqual(data["missing_adjustment_policy"], "block_implementation_lock")
        self.assertEqual(execution["entry_timing"], "next_tradable_session_open")
        self.assertEqual(execution["primary_holding_sessions"], 5)
        self.assertEqual(execution["non_overlapping_offsets"], [0, 1, 2, 3, 4])
        self.assertTrue(execution["allow_empty_portfolio"])
        self.assertEqual(
            execution["costs_bps"],
            {
                "commission_each_side": 3.0,
                "stamp_tax_sell_side": 5.0,
                "slippage_each_side": 5.0,
            },
        )

    def test_known_history_is_diagnostic_only_and_prospective_rule_is_forward(self):
        partitions = self.charter["time_partitions"]
        diagnostic = partitions["historical_diagnostic"]
        prospective = partitions["prospective_oos"]
        self.assertTrue(diagnostic["observed_history"])
        self.assertEqual(diagnostic["allowed_use"], "reject_or_mark_inconclusive_only")
        self.assertFalse(diagnostic["can_support_validation_claim"])
        self.assertIn("strictly after", prospective["start_rule"])
        self.assertEqual(prospective["fixed_window_benchmark_sessions"], 252)
        self.assertGreaterEqual(prospective["minimum_complete_signal_days"], 240)
        self.assertEqual(prospective["early_evaluation"], "forbidden")
        self.assertEqual(prospective["tuning_during_window"], "forbidden")

    def test_all_offsets_and_manual_review_are_hard_gates(self):
        gates = self.charter["evidence_gates"]
        boundaries = self.charter["implementation_boundaries"]
        self.assertGreaterEqual(gates["structural"]["minimum_completed_positions_per_offset"], 90)
        self.assertGreaterEqual(gates["structural"]["minimum_benchmark_coverage_pct"], 98.0)
        self.assertTrue(gates["turnover_and_stability"]["all_non_overlapping_offsets_must_pass"])
        self.assertEqual(gates["performance_net_of_costs"]["minimum_total_return_pct_each_offset"], 0.0)
        self.assertEqual(gates["performance_net_of_costs"]["minimum_excess_return_pct_each_offset"], 0.0)
        self.assertEqual(gates["performance_net_of_costs"]["minimum_median_offset_sharpe_ratio"], 0.5)
        self.assertEqual(gates["performance_net_of_costs"]["maximum_drawdown_floor_pct_each_offset"], -20.0)
        self.assertFalse(boundaries["run_backtest_now"])
        self.assertFalse(boundaries["create_database_validation_protocol_now"])
        self.assertFalse(boundaries["expose_in_ordinary_selection_before_manual_promotion"])
        self.assertFalse(boundaries["set_as_default_before_manual_promotion"])


if __name__ == "__main__":
    unittest.main()
