from __future__ import annotations

import unittest
from pathlib import Path

from app.api.routes.backtest import strategy_display_name_for_run
from app.stock_selection.sentiment_snapshot import SELECTION_CONTRACT_FIELDS


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SentimentV06UiCompatibilityTests(unittest.TestCase):
    def test_t16_selection_keeps_legacy_contract_and_adds_shadow_fields(self):
        fields = set(SELECTION_CONTRACT_FIELDS)
        self.assertTrue(
            {
                "signal_grade",
                "validation_status",
                "score_breakdown",
                "gate_results",
                "evidence_ids",
                "ai_status",
            }.issubset(fields)
        )
        self.assertTrue(
            {
                "factor_schema_version",
                "candidate_lanes",
                "entry_eligibility",
                "entry_block_reasons",
                "decision_as_of",
                "valid_until",
                "evaluation_method_version",
                "research_entry_assessment",
            }.issubset(fields)
        )

    def test_t16_selection_page_labels_conditions_as_research_only(self):
        script = (
            PROJECT_ROOT / "app" / "api" / "web" / "js" / "selection.js"
        ).read_text(encoding="utf-8")
        page = (
            PROJECT_ROOT / "app" / "api" / "web" / "pages" / "selection.html"
        ).read_text(encoding="utf-8")

        for label in (
            "增量催化质量",
            "后续驱动力",
            "业务关系强度",
            "同时点资金确认",
            "盘中价量承接",
            "筹码与成交容量",
        ):
            self.assertIn(label, script)
        self.assertIn("盘中条件满足，但不构成买入建议", script)
        self.assertIn("research_entry_assessment", script)
        self.assertIn("selection.js?v=20260915sentimentv06", page)

    def test_t16_backtest_history_label_and_page_route_remain_compatible(self):
        script = (
            PROJECT_ROOT / "app" / "api" / "web" / "js" / "backtest.js"
        ).read_text(encoding="utf-8")
        page = (
            PROJECT_ROOT / "app" / "api" / "web" / "pages" / "backtest.html"
        ).read_text(encoding="utf-8")

        self.assertEqual(
            strategy_display_name_for_run("a_share_sentiment_v06", "0.6.0"),
            "A股舆情选股 v0.6 v0.6.0",
        )
        self.assertIn("a_share_sentiment_v05", script)
        self.assertIn("a_share_sentiment_v06", script)
        self.assertIn("backtest.js?v=20260915sentimentv06", page)

    def test_t16_stock_detail_and_tracking_keep_shadow_research_semantics(self):
        stock_script = (
            PROJECT_ROOT / "app" / "api" / "web" / "js" / "stock-detail.js"
        ).read_text(encoding="utf-8")
        stock_page = (
            PROJECT_ROOT / "app" / "api" / "web" / "pages" / "stock-detail.html"
        ).read_text(encoding="utf-8")
        tracking_script = (
            PROJECT_ROOT / "app" / "api" / "web" / "js" / "tracking.js"
        ).read_text(encoding="utf-8")
        tracking_page = (
            PROJECT_ROOT / "app" / "api" / "web" / "pages" / "tracking.html"
        ).read_text(encoding="utf-8")

        self.assertIn("renderSentimentV06SelectionRows", stock_script)
        self.assertIn("研究候选（不构成买入建议）", stock_script)
        self.assertIn("核心业务证据", stock_script)
        self.assertIn("stock-detail.js?v=20260915sentimentv06", stock_page)
        self.assertIn("formatSentimentV06ResearchInline", tracking_script)
        self.assertIn("影子研究，不构成买入建议", tracking_script)
        self.assertIn("tracking.js?v=20260915sentimentv06", tracking_page)


if __name__ == "__main__":
    unittest.main()
