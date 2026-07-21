from __future__ import annotations

import inspect
import unittest

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.base import BaseSelectionStrategy
from app.stock_selection.selector import StockSelector
from app.strategies.active import thematic_strategies
from app.strategies.active.a_share_sentiment_strategy import (
    AShareSentimentStrategy,
    AShareSentimentV05Strategy,
)


def grade_item(score: float, *, regime: str = "risk_on", lane: str = "direct_catalyst") -> dict:
    return {
        "code": f"{score:.2f}",
        "score": score,
        "final_score": score,
        "hard_gate_pass": True,
        "hard_gate_reasons": [],
        "watch_gate_reasons": [],
        "market_regime": regime,
        "candidate_lane": lane,
    }


class SentimentStrategyExtractionTests(unittest.TestCase):
    FROZEN_BASELINE_COMMIT = "0c4e3e9"

    def test_both_registry_classes_implement_complete_base_interface(self):
        loader = StrategyLoader()
        stable = loader.load_strategy("a_share_sentiment")
        shadow = loader.load_strategy("a_share_sentiment_v05")

        self.assertIsInstance(stable, BaseSelectionStrategy)
        self.assertIsInstance(shadow, BaseSelectionStrategy)
        self.assertFalse(inspect.isabstract(AShareSentimentStrategy))
        self.assertFalse(inspect.isabstract(AShareSentimentV05Strategy))

    def test_historical_module_path_is_a_compatibility_export(self):
        self.assertIs(thematic_strategies.AShareSentimentStrategy, AShareSentimentStrategy)
        self.assertIs(thematic_strategies.AShareSentimentV05Strategy, AShareSentimentV05Strategy)

    def test_v05_uses_the_shared_market_opinion_enrichment_path(self):
        selector = object.__new__(StockSelector)
        selector.strategy_id = "a_share_sentiment_v05"

        diagnostics = selector._attach_market_opinion_context([])

        self.assertTrue(diagnostics["enabled"])

    def test_frozen_044_golden_scores_remain_equal_within_one_e_minus_six(self):
        """Golden values captured before extraction from baseline 0c4e3e9."""

        strategy = StrategyLoader().load_strategy("a_share_sentiment")
        factor_keys = tuple(strategy.config["weights"])
        rows = [
            {
                "code": "BASELINE-A",
                "factors": {key: 70.0 for key in factor_keys},
                "strategy_raw_metrics": {
                    "trade_signal_state": "tradable",
                    "market_theme_score_delta": 2.5,
                },
            },
            {
                "code": "BASELINE-B",
                "factors": {
                    **{key: 80.0 for key in factor_keys},
                    "price_confirm": 10.0,
                    "intraday_confirm": 10.0,
                    "volume_confirm": 40.0,
                },
                "strategy_raw_metrics": {
                    "trade_signal_state": "weak",
                    "market_theme_score_delta": -1.0,
                },
            },
            {
                "code": "BASELINE-C",
                "factors": {key: 0.0 for key in factor_keys},
                "strategy_raw_metrics": {},
            },
        ]

        scored = strategy.score(rows)
        actual = {item["code"]: item["score"] for item in scored}
        expected = {
            "BASELINE-A": 72.5,
            "BASELINE-B": 47.29,
            "BASELINE-C": 0.0,
        }
        self.assertEqual(set(actual), set(expected))
        for code, expected_score in expected.items():
            self.assertAlmostEqual(actual[code], expected_score, delta=1e-6)


class SentimentV05ContractTests(unittest.TestCase):
    def setUp(self):
        self.strategy = StrategyLoader().load_strategy("a_share_sentiment_v05")

    def test_fixed_six_module_weights_match_v05_contract(self):
        self.assertEqual(
            self.strategy.config["weights"],
            {
                "catalyst_quality": 0.25,
                "persistence": 0.15,
                "relation_recognition": 0.15,
                "fund_confirmation": 0.15,
                "price_volume_confirmation": 0.20,
                "chip_liquidity_capacity": 0.10,
            },
        )
        self.assertAlmostEqual(sum(self.strategy.config["weights"].values()), 1.0)

    def test_score_boundaries_and_market_gates_are_exact(self):
        cases = [
            (59.99, "risk_on", "rejected"),
            (60.00, "risk_on", "watch"),
            (67.99, "risk_on", "watch"),
            (68.00, "risk_on", "tradable"),
            (71.99, "cautious", "watch"),
            (72.00, "cautious", "tradable"),
            (100.00, "defensive", "watch"),
            (100.00, "unknown", "watch"),
            (100.00, "stale", "watch"),
        ]
        for score, regime, expected in cases:
            with self.subTest(score=score, regime=regime):
                grade, _ = self.strategy._grade(grade_item(score, regime=regime))
                self.assertEqual(grade, expected)

    def test_ai_advice_never_changes_score_or_ranking(self):
        high = grade_item(0)
        high.update(
            {
                "code": "HIGH",
                "factors": {key: 70.0 for key in self.strategy.FACTOR_KEYS},
                "ai_overlay": {
                    "confidence": 0.99,
                    "score_adjustment": -8,
                    "evidence_ids": ["negative-evidence"],
                },
            }
        )
        low = grade_item(0)
        low.update(
            {
                "code": "LOW",
                "factors": {key: 69.0 for key in self.strategy.FACTOR_KEYS},
                "ai_overlay": {
                    "confidence": 0.99,
                    "score_adjustment": 4,
                    "evidence_ids": ["positive-evidence"],
                },
            }
        )

        scored = self.strategy.score([low, high])

        self.assertEqual([item["code"] for item in scored], ["HIGH", "LOW"])
        self.assertEqual([item["final_score"] for item in scored], [70.0, 69.0])
        self.assertEqual([item["ai_applied_adjustment"] for item in scored], [0.0, 0.0])
        self.assertEqual(
            [item["ai_advisory_adjustment"] for item in scored],
            [-8.0, 4.0],
        )

    def test_zero_factor_is_not_silently_replaced_by_neutral_score(self):
        item = {"code": "ZERO", "factors": {key: 0.0 for key in self.strategy.FACTOR_KEYS}}
        scored = self.strategy.score([item])

        self.assertEqual(scored[0]["local_score"], 0.0)
        self.assertEqual(scored[0]["final_score"], 0.0)

    def test_direct_catalyst_requires_primary_or_two_independent_sources(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
            "event_evidence": [
                {
                    "evidence_id": "e-1",
                    "source": "source-a",
                    "direction": "positive",
                    "signed_score": 1,
                    "impact_score": 85,
                    "timeliness_score": 90,
                }
            ],
        }
        rejected = self.strategy.prepare_context({"candidates": [base]})
        self.assertEqual(rejected["candidates"], [])
        self.assertIn(
            "direct_catalyst_source_not_confirmed",
            rejected["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
        )

        accepted = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "event_evidence": [
                            {
                                **base["event_evidence"][0],
                                "is_primary_source": True,
                            }
                        ],
                    }
                ]
            }
        )
        self.assertEqual(len(accepted["candidates"]), 1)
        self.assertEqual(accepted["candidates"][0]["evidence_ids"], ["e-1"])
        self.assertEqual(accepted["candidates"][0]["watch_gate_reasons"], [])

    def test_sector_source_count_cannot_bypass_direct_event_credibility_gate(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "opinion_source_count": 12,
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
        }
        one_untrusted = {
            "evidence_id": "e-1",
            "source": "unknown-blog",
            "credibility_score": 0.2,
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
        }

        rejected = self.strategy.prepare_context(
            {"candidates": [{**base, "event_evidence": [one_untrusted]}]}
        )
        self.assertEqual(rejected["candidates"], [])
        self.assertIn(
            "direct_catalyst_source_not_confirmed",
            rejected["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
        )

        accepted = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "event_evidence": [
                            {**one_untrusted, "source": "trusted-a", "credibility_score": 0.8},
                            {**one_untrusted, "evidence_id": "e-2", "source": "trusted-b", "credibility_score": 0.8},
                        ],
                    }
                ]
            }
        )
        self.assertEqual(len(accepted["candidates"]), 1)
        self.assertEqual(accepted["candidates"][0]["source_evidence_count"], 2)
        self.assertEqual(accepted["candidates"][0]["credible_source_evidence_count"], 2)

    def test_direct_sources_are_deduplicated_by_registrable_domain(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
        }
        evidence = {
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
            "credibility_score": 0.8,
        }

        domain_pairs = (
            (
                "https://news.example.com.cn/articles/1",
                "https://m.example.com.cn/articles/2",
            ),
            (
                "https://news.department.gov.cn/articles/1",
                "https://m.department.gov.cn/articles/2",
            ),
        )
        for first_url, second_url in domain_pairs:
            with self.subTest(first_url=first_url):
                result = self.strategy.prepare_context(
                    {
                        "candidates": [
                            {
                                **base,
                                "event_evidence": [
                                    {
                                        **evidence,
                                        "evidence_id": "same-domain-1",
                                        "url": first_url,
                                    },
                                    {
                                        **evidence,
                                        "evidence_id": "same-domain-2",
                                        "url": second_url,
                                    },
                                ],
                            }
                        ]
                    }
                )

                self.assertEqual(result["candidates"], [])
                rejection = result["sentiment_v05_filter_summary"]["rejections"][0]
                self.assertIn("direct_catalyst_source_not_confirmed", rejection["reasons"])

    def test_source_and_source_name_fallbacks_share_normalized_identity(self):
        evidence = {
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
            "credibility_score": 0.8,
        }
        result = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        "code": "000001.SZ",
                        "listed_trade_days": 60,
                        "amount": 100_000_000,
                        "median_amount_20": 120_000_000,
                        "candidate_lane": "direct_catalyst",
                        "freshness_status": "fresh",
                        "market_coverage_ratio": 0.99,
                        "decision_data_version": "batch-1",
                        "event_evidence": [
                            {
                                **evidence,
                                "evidence_id": "normalized-name-1",
                                "source": "Wall Street-CN",
                            },
                            {
                                **evidence,
                                "evidence_id": "normalized-name-2",
                                "source_name": " wall street cn ",
                            },
                        ],
                    }
                ]
            }
        )

        self.assertEqual(result["candidates"], [])
        self.assertIn(
            "direct_catalyst_source_not_confirmed",
            result["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
        )

    def test_source_id_precedes_url_and_primary_metadata_is_recognized(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
        }
        evidence = {
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
            "credibility_score": 0.8,
        }
        same_publisher = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "event_evidence": [
                            {
                                **evidence,
                                "evidence_id": "publisher-1",
                                "source_id": "publisher-a",
                                "url": "https://first.example.com/a",
                            },
                            {
                                **evidence,
                                "evidence_id": "publisher-2",
                                "source_id": "publisher-a",
                                "url": "https://second.example.net/b",
                            },
                        ],
                    }
                ]
            }
        )
        self.assertEqual(same_publisher["candidates"], [])
        self.assertIn(
            "direct_catalyst_source_not_confirmed",
            same_publisher["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
        )

        for field in ("source_id", "source_type"):
            with self.subTest(primary_field=field):
                primary = self.strategy.prepare_context(
                    {
                        "candidates": [
                            {
                                **base,
                                "event_evidence": [
                                    {
                                        **evidence,
                                        "evidence_id": f"primary-{field}",
                                        field: "announcement",
                                    }
                                ],
                            }
                        ]
                    }
                )
                self.assertEqual(len(primary["candidates"]), 1)
                self.assertTrue(primary["candidates"][0]["has_primary_source"])

    def test_generic_confidence_does_not_make_a_publisher_credible(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
        }
        evidence = {
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
            "confidence": 0.99,
        }

        result = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "event_evidence": [
                            {**evidence, "evidence_id": "confidence-1", "source": "blog-a"},
                            {**evidence, "evidence_id": "confidence-2", "source": "blog-b"},
                        ],
                    }
                ]
            }
        )

        self.assertEqual(result["candidates"], [])
        self.assertIn(
            "direct_catalyst_source_not_confirmed",
            result["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
        )

    def test_adverse_veto_reuses_source_credibility_and_ignores_generic_confidence(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "required_data_complete": True,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
        }
        positive_primary = {
            "evidence_id": "positive-primary",
            "source": "exchange",
            "source_type": "exchange",
            "direction": "positive",
            "signed_score": 1,
            "impact_score": 85,
            "timeliness_score": 90,
        }
        adverse = {
            "evidence_id": "negative-adverse",
            "source": "negative-publisher",
            "direction": "negative",
            "title": "公司被曝财务造假",
            "confidence": 0.99,
        }

        confidence_only = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "event_evidence": [positive_primary, adverse],
                    }
                ]
            }
        )
        self.assertEqual(len(confidence_only["candidates"]), 1)
        self.assertNotIn(
            "high_confidence_adverse_event",
            confidence_only["candidates"][0]["hard_gate_reasons"],
        )

        trusted_source_cases = (
            {"credibility_score": 0.80},
            {"source_credibility_score": 80},
            {"credibility_level": "trusted"},
            {"source_credibility_level": "A"},
            {"is_primary_source": True},
            {"source_id": "announcement"},
            {"source_type": "regulator"},
        )
        for trusted_metadata in trusted_source_cases:
            with self.subTest(trusted_metadata=trusted_metadata):
                rejected = self.strategy.prepare_context(
                    {
                        "candidates": [
                            {
                                **base,
                                "event_evidence": [
                                    positive_primary,
                                    {**adverse, **trusted_metadata},
                                ],
                            }
                        ]
                    }
                )
                self.assertEqual(rejected["candidates"], [])
                self.assertIn(
                    "high_confidence_adverse_event",
                    rejected["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
                )

    def test_lifecycle_suspension_and_delisting_states_are_hard_rejections(self):
        base = {
            "listed_trade_days": 60,
            "lifecycle_known": True,
            "amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
            "event_evidence": [
                {
                    "evidence_id": "official-1",
                    "source": "exchange",
                    "source_type": "exchange",
                    "direction": "positive",
                    "signed_score": 1,
                    "impact_score": 85,
                    "timeliness_score": 90,
                }
            ],
        }
        cases = (
            ({"code": "SUSPENDED", "is_suspended": True}, "suspended"),
            ({"code": "DELISTING", "is_delisting": True}, "delisting"),
            ({"code": "UNKNOWN", "lifecycle_known": False}, "lifecycle_unknown"),
        )

        for overrides, reason in cases:
            with self.subTest(reason=reason):
                result = self.strategy.prepare_context({"candidates": [{**base, **overrides}]})
                self.assertEqual(result["candidates"], [])
                self.assertIn(
                    reason,
                    result["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
                )

    def test_trading_day_required_data_and_liquidity_gates_use_point_in_time_fields(self):
        base = {
            "code": "000001.SZ",
            "listed_trade_days": 60,
            "required_data_complete": True,
            "amount": 100_000_000,
            "technical_latest_amount": 100_000_000,
            "median_amount_20": 120_000_000,
            "candidate_lane": "direct_catalyst",
            "freshness_status": "fresh",
            "market_coverage_ratio": 0.99,
            "decision_data_version": "batch-1",
            "event_evidence": [
                {
                    "evidence_id": "official-1",
                    "source": "exchange",
                    "source_type": "exchange",
                    "direction": "positive",
                    "signed_score": 1,
                    "impact_score": 85,
                    "timeliness_score": 90,
                }
            ],
        }
        cases = (
            ({"listed_trade_days": 59}, "new_listing"),
            ({"required_data_complete": False}, "required_data_incomplete"),
            (
                {
                    "decision_clock_mode": "intraday",
                    "realtime_amount": None,
                },
                "intraday_realtime_amount_missing",
            ),
            (
                {
                    "decision_clock_mode": "postclose",
                    "technical_latest_amount": 49_999_999,
                    "amount": 500_000_000,
                },
                "latest_liquidity_below_floor",
            ),
            (
                {
                    "median_amount_20": None,
                    "avg_amount_20": 500_000_000,
                },
                "twenty_day_liquidity_below_floor",
            ),
        )
        for overrides, reason in cases:
            with self.subTest(reason=reason):
                result = self.strategy.prepare_context(
                    {"candidates": [{**base, **overrides}]}
                )
                self.assertEqual(result["candidates"], [])
                self.assertIn(
                    reason,
                    result["sentiment_v05_filter_summary"]["rejections"][0]["reasons"],
                )

        intraday = self.strategy.prepare_context(
            {
                "candidates": [
                    {
                        **base,
                        "decision_clock_mode": "intraday",
                        "realtime_amount": 60_000_000,
                    }
                ]
            }
        )
        self.assertEqual(len(intraday["candidates"]), 1)
        self.assertEqual(
            intraday["candidates"][0]["latest_liquidity_amount_source"],
            "realtime_amount",
        )

    def test_cautious_market_allows_only_one_tradable_signal(self):
        rows = []
        for index, score in enumerate((90.0, 89.0, 88.0), start=1):
            item = grade_item(score, regime="cautious")
            item.update(
                {
                    "code": f"C{index}",
                    "opinion_sector_name": f"theme-{index}",
                    "industry": f"industry-{index}",
                }
            )
            rows.append(item)

        selected = self.strategy.select(rows)

        self.assertEqual([item["signal_grade"] for item in selected], ["tradable", "watch", "watch"])
        self.assertEqual(selected[1]["grade_reason"], "cautious_tradable_cap_reached")

    def test_theme_and_industry_caps_only_downgrade_trade_grade(self):
        rows = []
        for code, score, theme in (
            ("A", 90.0, "theme-a"),
            ("B", 89.0, "theme-b"),
            ("C", 88.0, "theme-c"),
        ):
            item = grade_item(score)
            item.update({"code": code, "opinion_sector_name": theme, "industry": "same-industry"})
            rows.append(item)

        selected = self.strategy.select(rows)

        self.assertEqual([item["signal_grade"] for item in selected], ["tradable", "tradable", "watch"])
        self.assertEqual(selected[2]["grade_reason"], "industry_tradable_cap_reached")
        self.assertEqual(selected[2]["validation_status"], "shadow_only")
        self.assertIn("gate_results", selected[2])


if __name__ == "__main__":
    unittest.main()
