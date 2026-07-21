from __future__ import annotations

import unittest

from app.data_ingestion.market_opinion_semantics import (
    classify_opinion_direction,
    classify_sector_direction,
    opinion_direction_multiplier,
    stock_sector_relation,
)
from app.stock_selection.selector import StockSelector
from scripts.run_market_opinion_update import sector_opinion_score


class MarketOpinionDirectionTests(unittest.TestCase):
    def test_positive_title_is_not_overridden_by_generic_mixed_market_text(self):
        direction = classify_opinion_direction(
            "创业板指收涨5.20%，半导体产业链全线走强",
            "午后部分高位方向出现震荡",
        )

        self.assertEqual(direction, "positive")
        self.assertEqual(opinion_direction_multiplier(direction), 1.0)

    def test_negative_news_subtracts_and_neutral_news_does_not_add(self):
        self.assertEqual(classify_opinion_direction("光伏板块跌势不止，多股暴跌"), "negative")
        self.assertEqual(opinion_direction_multiplier("negative"), -1.0)
        self.assertEqual(opinion_direction_multiplier("neutral"), 0.0)

    def test_neutral_article_volume_cannot_manufacture_a_hot_sector(self):
        score = sector_opinion_score(
            weighted_signed_impact=0,
            positive_news_count=0,
            positive_source_count=0,
            positive_stock_count=0,
            average_positive_timeliness=100,
            negative_news_count=0,
        )

        self.assertEqual(score, 0.0)

    def test_fresh_positive_directional_evidence_can_create_a_hot_sector(self):
        score = sector_opinion_score(
            weighted_signed_impact=75,
            positive_news_count=1,
            positive_source_count=1,
            positive_stock_count=1,
            average_positive_timeliness=100,
            negative_news_count=0,
        )

        self.assertGreater(score, 45)

    def test_mixed_article_is_scored_per_sector_clause(self):
        summary = "医疗服务板块走强，昭衍新药涨停；白酒板块回落，今世缘下跌"

        pharma = classify_sector_direction(
            title="三大股指震荡",
            summary=summary,
            sector_type="theme",
            sector_name="医药",
        )
        consumer = classify_sector_direction(
            title="三大股指震荡",
            summary=summary,
            sector_type="theme",
            sector_name="消费",
        )

        self.assertEqual(pharma.direction, "positive")
        self.assertIn("医疗服务", pharma.context)
        self.assertEqual(consumer.direction, "negative")
        self.assertIn("白酒", consumer.context)


class StockSectorRelationTests(unittest.TestCase):
    def setUp(self):
        self.title = "三大股指冲高回落"
        self.summary = "医疗服务板块走强，昭衍新药涨停；白酒板块回落，今世缘下跌"

    def test_stock_is_linked_to_nearby_pharma_clause(self):
        relation = stock_sector_relation(
            title=self.title,
            summary=self.summary,
            stock_name="昭衍新药",
            stock_code="sh.603127",
            stock_industry="医疗保健",
            sector_type="theme",
            sector_name="医药",
        )

        self.assertTrue(relation.supported)
        self.assertIn("昭衍新药", relation.context)
        self.assertIn("医疗服务", relation.context)

    def test_stock_is_not_cartesian_linked_to_later_consumer_clause(self):
        relation = stock_sector_relation(
            title=self.title,
            summary=self.summary,
            stock_name="昭衍新药",
            stock_code="sh.603127",
            stock_industry="医疗保健",
            sector_type="theme",
            sector_name="消费",
        )

        self.assertFalse(relation.supported)

    def test_static_industry_relation_remains_supported(self):
        relation = stock_sector_relation(
            title=self.title,
            summary=self.summary,
            stock_name="昭衍新药",
            stock_code="sh.603127",
            stock_industry="医疗保健",
            sector_type="industry",
            sector_name="医疗保健",
        )

        self.assertTrue(relation.supported)
        self.assertEqual(relation.reason, "股票静态行业一致")


class ThemeFundFlowMappingTests(unittest.TestCase):
    def test_broad_consumer_theme_does_not_inherit_consumer_electronics_flow(self):
        rows = [
            {
                "sector_type": "concept",
                "sector_name": "消费电子概念",
                "net_amount": 120,
                "pct_chg": 5,
            }
        ]

        self.assertIsNone(StockSelector._match_theme_fund_flow("消费", rows))

    def test_exact_representative_anchor_is_used(self):
        rows = [
            {
                "sector_type": "concept",
                "sector_name": "共封装光学(CPO)",
                "net_amount": 300,
                "pct_chg": 8,
            },
            {
                "sector_type": "concept",
                "sector_name": "东数西算(算力)",
                "net_amount": 30,
                "pct_chg": 1,
            },
        ]

        matched = StockSelector._match_theme_fund_flow("AI算力", rows)

        self.assertIsNotNone(matched)
        self.assertEqual(matched["sector_name"], "东数西算(算力)")
        self.assertEqual(matched["mapping_mode"], "exact_representative_anchor")


if __name__ == "__main__":
    unittest.main()
