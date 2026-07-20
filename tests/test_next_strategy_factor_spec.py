from __future__ import annotations

import hashlib
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = PROJECT_ROOT / "config/research_protocols/pit_quality_trend_liquidity_factor_spec_v1.yaml"
LOCK_PATH = PROJECT_ROOT / "config/research_protocols/pit_quality_trend_liquidity_factor_spec_v1.sha256"


class NextStrategyFactorSpecTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw = SPEC_PATH.read_bytes()
        cls.spec = yaml.safe_load(cls.raw)

    def test_factor_spec_matches_immutable_hash_lock(self):
        expected = LOCK_PATH.read_text(encoding="utf-8").strip().split()[0]
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), expected)

    def test_spec_is_independent_sparse_and_unvalidated(self):
        factor_spec = self.spec["factor_spec"]
        truth = self.spec["truth_status"]
        selection = self.spec["final_score_and_selection"]
        self.assertEqual(factor_spec["strategy_id"], "pit_quality_trend_liquidity_v1")
        self.assertFalse(truth["is_strategy_implementation"])
        self.assertEqual(truth["validation_status"], "unvalidated")
        self.assertFalse(truth["selectable"])
        self.assertEqual(selection["minimum_picks"], 0)
        self.assertTrue(selection["allow_cash"])
        self.assertFalse(selection["force_fill"])

    def test_only_development_partition_was_used(self):
        partition = self.spec["research_partition"]
        self.assertEqual(partition["development_start_date"], "2024-01-02")
        self.assertEqual(partition["development_end_date"], "2025-06-30")
        self.assertEqual(partition["data_used_for_this_lock"], "development_partition_only")
        self.assertFalse(partition["performance_optimization_performed"])
        self.assertFalse(partition["historical_diagnostic_partition_read_for_selection"])
        self.assertFalse(partition["embargo_partition_read_for_selection"])

    def test_score_weights_and_forbidden_lowvol_role_are_locked(self):
        quality = self.spec["quality_score"]
        trend = self.spec["trend_confirmation_and_score"]
        gates = self.spec["eligibility_gates"]
        self.assertAlmostEqual(sum(item["weight"] for item in quality["factors"].values()), 1.0)
        self.assertAlmostEqual(sum(item["weight"] for item in trend["factors"].values()), 1.0)
        self.assertAlmostEqual(quality["weight_in_final_score"] + trend["weight_in_final_score"], 1.0)
        self.assertEqual(gates["volatility_role"], "hard_risk_gate_only_not_positive_score")

    def test_adjusted_return_gap_blocks_implementation_and_diagnostic(self):
        evidence = self.spec["development_feasibility_evidence"]
        gate = self.spec["implementation_gate"]
        self.assertEqual(evidence["adjustment_factor_coverage_ratio"], 0.0)
        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["forbidden_fallback"], "raw_unadjusted_return")
        self.assertFalse(gate["strategy_code_may_be_created_now"])
        self.assertFalse(gate["historical_diagnostic_may_run_now"])
        self.assertFalse(gate["database_validation_protocol_may_be_created_now"])

    def test_every_candidate_is_logged_and_selected_formula_is_exact(self):
        candidate_log = self.spec["candidate_log"]
        selection = self.spec["final_score_and_selection"]
        self.assertEqual(candidate_log["candidates_recorded"], len(candidate_log["candidates"]))
        self.assertLessEqual(candidate_log["candidates_recorded"], candidate_log["maximum_allowed_candidates"])
        self.assertEqual(candidate_log["selected_candidate_id"], "qtl_c03")
        self.assertEqual(selection["minimum_final_score"], 70.0)
        self.assertEqual(selection["boundary_score_separation_points"], 2.0)


if __name__ == "__main__":
    unittest.main()
