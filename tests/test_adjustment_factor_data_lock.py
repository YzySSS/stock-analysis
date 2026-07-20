from __future__ import annotations

import hashlib
import re
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = (
    PROJECT_ROOT
    / "config/research_protocols/pit_quality_trend_liquidity_adjustment_data_lock_v1.yaml"
)
HASH_PATH = LOCK_PATH.with_suffix(".sha256")
PARENT_FACTOR_SPEC_PATH = (
    PROJECT_ROOT
    / "config/research_protocols/pit_quality_trend_liquidity_factor_spec_v1.yaml"
)


class AdjustmentFactorDataLockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw = LOCK_PATH.read_bytes()
        cls.lock = yaml.safe_load(cls.raw)

    def test_data_lock_matches_immutable_hash(self):
        expected = HASH_PATH.read_text(encoding="utf-8").strip().split()[0]
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), expected)

    def test_parent_factor_spec_hash_and_implementation_commit_are_pinned(self):
        metadata = self.lock["adjustment_data_lock"]
        parent_digest = hashlib.sha256(PARENT_FACTOR_SPEC_PATH.read_bytes()).hexdigest()
        self.assertEqual(metadata["parent_factor_spec_sha256"], parent_digest)
        self.assertRegex(metadata["implementation_commit"], re.compile(r"^[0-9a-f]{40}$"))
        self.assertTrue(metadata["immutable"])

    def test_ready_means_data_only_not_strategy_validation(self):
        truth = self.lock["truth_status"]
        transition = self.lock["gate_transition"]
        self.assertTrue(truth["adjustment_data_prerequisite_ready"])
        self.assertFalse(truth["is_strategy_implementation"])
        self.assertFalse(truth["is_validation_protocol"])
        self.assertEqual(truth["strategy_validation_status"], "unvalidated")
        self.assertFalse(truth["selectable"])
        self.assertEqual(transition["next_allowed_work"], "separate_implementation_lock_review")
        self.assertFalse(transition["historical_diagnostic_may_run_before_implementation_lock"])

    def test_full_and_development_coverage_meet_the_frozen_gate(self):
        contract = self.lock["source_contract"]
        full = self.lock["full_history_evidence"]
        development = self.lock["development_partition_evidence"]
        threshold = contract["minimum_partition_coverage_ratio"]

        self.assertEqual(contract["coverage_scope_version"], "stock_instrument_type_v1")
        self.assertEqual(full["manifest_days"], full["manifest_success_days"])
        self.assertEqual(full["manifest_scope_mismatch_days"], 0)
        self.assertGreaterEqual(full["coverage_ratio"], threshold)
        self.assertGreaterEqual(full["minimum_partition_coverage_ratio"], threshold)
        self.assertEqual(full["invalid_factor_rows"], 0)
        self.assertEqual(full["future_factor_rows"], 0)
        self.assertEqual(development["coverage_ratio"], 1.0)
        self.assertEqual(development["minimum_partition_coverage_ratio"], 1.0)
        self.assertEqual(development["missing_stock_kline_rows"], 0)

    def test_known_suspension_gaps_are_not_imputed_or_raw_fallback(self):
        gaps = self.lock["known_source_gaps"]
        returns = self.lock["return_accounting_contract"]
        self.assertEqual(gaps["count"], len(gaps["codes"]))
        self.assertIn(
            "every_row_is_marked_suspend_type_S_in_stock_suspension_daily",
            gaps["verified_facts"],
        )
        self.assertEqual(gaps["handling"], "fail_closed_for_any_candidate_path_requiring_the_missing_date")
        self.assertIn(
            "do_not_guess_or_forward_fill_an_adjustment_factor",
            gaps["forbidden_handling"],
        )
        self.assertEqual(returns["missing_factor_policy"], "fail_closed_per_candidate_path")
        self.assertEqual(returns["forbidden_fallback"], "raw_unadjusted_return")
        self.assertEqual(
            returns["exact_formula"],
            "end_price*end_factor/(start_price*start_factor)-1",
        )


if __name__ == "__main__":
    unittest.main()
