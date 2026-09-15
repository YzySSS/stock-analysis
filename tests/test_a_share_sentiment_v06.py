from __future__ import annotations

import copy
import json
import unittest
from datetime import datetime

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.selector import StockSelector
from app.stock_selection.sentiment_v06_intraday import summarize_intraday_path


DECISION = datetime(2026, 9, 11, 11, 0, 0)


def minute_rows(prices: list[float], *, final_minute: int = 59) -> list[dict]:
    minutes = [30, 45, final_minute]
    rows = []
    cumulative_volume = 100_000
    cumulative_amount = prices[0] * cumulative_volume
    for index, (minute, price) in enumerate(zip(minutes, prices)):
        if index:
            cumulative_volume += 100_000
            cumulative_amount += price * 100_000
        rows.append(
            {
                "code": "sh.600001",
                "trade_date": "2026-09-11",
                "quote_time": f"2026-09-11 10:{minute:02d}:00",
                "received_at": f"2026-09-11 10:{minute:02d}:05",
                "latest_price": price,
                "volume": cumulative_volume,
                "amount": cumulative_amount,
                "batch_id": f"batch-{minute}",
                "source": "fixture",
                "is_stale": 0,
            }
        )
    return rows


def direct_candidate(*, realtime_net: float | None = 2_000_000) -> dict:
    event_id = "e" * 64
    path = summarize_intraday_path(
        minute_rows([10.0, 10.2, 10.4]),
        decision_as_of=DECISION,
        event_available_at="2026-09-11 10:46:00",
        minimum_samples=3,
        quote_ttl_seconds=180,
    )
    return {
        "code": "sh.600001",
        "name": "甲公司",
        "industry": "机械设备",
        "is_st": False,
        "lifecycle_known": True,
        "is_suspended": False,
        "is_delisting": False,
        "list_status": "L",
        "listed_trade_days": 300,
        "required_data_complete": True,
        "decision_clock_mode": "intraday",
        "decision_as_of": "2026-09-11 11:00:00",
        "market_coverage_ratio": 0.99,
        "realtime_market_coverage_ratio": 0.99,
        "market_state": "strong",
        "technical_latest_amount": 600_000_000,
        "realtime_amount": 500_000_000,
        "median_amount_20": 1_000_000_000,
        "volume_ratio": 2.0,
        "realtime_price": 10.4,
        "realtime_pct_chg": 4.0,
        "realtime_trade_date": "2026-09-11",
        "realtime_quote_time": "2026-09-11 10:59:00",
        "realtime_received_at": "2026-09-11 10:59:05",
        "realtime_mf_net": realtime_net,
        "realtime_mf_amount": 100_000_000,
        "realtime_mf_trade_date": "2026-09-11",
        "realtime_mf_quote_time": "2026-09-11 10:59:00",
        "realtime_mf_received_at": "2026-09-11 10:59:06",
        "realtime_mf_source_unit": "元",
        "chip_winner_rate": 0.45,
        "chip_weight_avg": 9.8,
        "is_limit_up": False,
        "candidate_lanes": ["direct_catalyst"],
        "direct_event_ids": [event_id],
        "adverse_event_ids": [],
        "evidence_ids": ["n" * 64],
        "evidence_quality": {"status": "complete", "available_count": 1},
        "event_evidence_source": "append_only_ledger",
        "sentiment_v06_events": [
            {
                "canonical_event_id": event_id,
                "event_revision": 1,
                "content_role": "original_catalyst",
                "direction": "positive",
                "confirmation_status": "confirmed_primary",
                "event_confidence": 1.0,
                "persistence_score": 100.0,
                "driver_horizon": "staged",
                "available_evidence_count": 1,
                "effective_until": "2026-09-15 15:00:00",
                "is_terminal": False,
                "evidence": [
                    {
                        "evidence_id": "n" * 64,
                        "impact_score": 100.0,
                        "source_time": "2026-09-11 10:00:00",
                        "received_at": "2026-09-11 10:01:00",
                    }
                ],
            }
        ],
        "sentiment_v06_relations": [
            {
                "relation_id": "r" * 64,
                "canonical_event_id": event_id,
                "relation_type": "direct_business",
                "relation_status": "confirmed_primary",
                "relation_score": 100.0,
                "evidence_excerpt": "甲公司公告签订订单",
            }
        ],
        "intraday_path": path,
        "candidate_reasons": [],
        "candidate_risks": [],
        "missing_fields": [],
    }


class IntradayFeatureTests(unittest.TestCase):
    def test_aware_decision_clock_compares_with_naive_shanghai_quote_clock(self):
        result = summarize_intraday_path(
            minute_rows([10.0, 10.2, 10.4]),
            decision_as_of="2026-09-11T11:00:00+08:00",
        )

        self.assertEqual(result["data_status"], "complete")
        self.assertEqual(result["excluded_after_decision"], 0)

    def test_t08_sustained_support_and_spike_fade_are_distinct(self):
        supported = summarize_intraday_path(
            minute_rows([10.0, 10.2, 10.4]),
            decision_as_of=DECISION,
        )
        faded = summarize_intraday_path(
            minute_rows([10.0, 11.0, 10.5]),
            decision_as_of=DECISION,
        )

        self.assertEqual(supported["data_status"], "complete")
        self.assertEqual(supported["path_state"], "sustained_support")
        self.assertEqual(faded["data_status"], "complete")
        self.assertEqual(faded["path_state"], "spike_fade")
        self.assertLess(faded["high_drawdown_pct"], supported["high_drawdown_pct"])

    def test_t10_missing_stale_and_after_decision_paths_fail_closed(self):
        missing = summarize_intraday_path([], decision_as_of=DECISION)
        stale = summarize_intraday_path(
            minute_rows([10.0, 10.1, 10.2], final_minute=50),
            decision_as_of=DECISION,
            quote_ttl_seconds=180,
        )
        future_rows = minute_rows([10.0, 10.1, 10.2])
        future_rows[-1]["received_at"] = "2026-09-11 11:00:01"
        future = summarize_intraday_path(future_rows, decision_as_of=DECISION)

        self.assertEqual(missing["data_status"], "insufficient_samples")
        self.assertEqual(stale["data_status"], "stale")
        self.assertEqual(future["data_status"], "insufficient_samples")
        self.assertEqual(future["excluded_after_decision"], 1)
        self.assertEqual(missing["path_state"], "unknown")

    def test_receive_clock_before_source_clock_is_not_complete(self):
        rows = minute_rows([10.0, 10.2, 10.4])
        rows[-1]["received_at"] = "2026-09-11 10:58:59"

        result = summarize_intraday_path(rows, decision_as_of=DECISION)

        self.assertEqual(result["data_status"], "mixed_clock")
        self.assertFalse(result["receive_order_valid"])


class SentimentV06StrategyTests(unittest.TestCase):
    def setUp(self):
        self.strategy = StrategyLoader().load_strategy("a_share_sentiment_v06")

    def _run(self, item: dict) -> list[dict]:
        context = self.strategy.prepare_context({"candidates": [item]})
        factors = self.strategy.compute_factors(context)
        scored = self.strategy.score(factors)
        return self.strategy.select(scored)

    def test_t04_price_change_cannot_change_event_or_relation_factors(self):
        first = direct_candidate()
        second = {**copy.deepcopy(first), "realtime_pct_chg": 6.5}

        first_factors = self.strategy._lane_factor_set(first, "direct_catalyst")[0]
        second_factors = self.strategy._lane_factor_set(second, "direct_catalyst")[0]

        for key in ("catalyst_quality", "persistence", "relation_recognition"):
            self.assertEqual(first_factors[key], second_factors[key])
        self.assertNotEqual(
            first_factors["price_volume_confirmation"],
            second_factors["price_volume_confirmation"],
        )

    def test_t06_none_zero_and_negative_fund_values_are_distinct(self):
        unknown, unknown_quality = self.strategy._fund_factor(
            {
                "decision_clock_mode": "intraday",
                "realtime_trade_date": "2026-09-11",
                "realtime_mf_trade_date": "2026-09-11",
                "realtime_mf_amount": 100.0,
                "realtime_mf_source_unit": "元",
            },
            "direct_catalyst",
        )
        zero, zero_quality = self.strategy._fund_factor(
            {
                "decision_clock_mode": "intraday",
                "realtime_trade_date": "2026-09-11",
                "realtime_mf_trade_date": "2026-09-11",
                "realtime_mf_amount": 100.0,
                "realtime_mf_net": 0.0,
                "realtime_mf_source_unit": "元",
            },
            "direct_catalyst",
        )
        negative, negative_quality = self.strategy._fund_factor(
            {
                "decision_clock_mode": "intraday",
                "realtime_trade_date": "2026-09-11",
                "realtime_mf_trade_date": "2026-09-11",
                "realtime_mf_amount": 100.0,
                "realtime_mf_net": -10.0,
                "realtime_mf_source_unit": "元",
            },
            "direct_catalyst",
        )

        self.assertIsNone(unknown)
        self.assertEqual(unknown_quality["status"], "unknown")
        self.assertEqual(zero, 50.0)
        self.assertTrue(zero_quality["current_clock_complete"])
        self.assertLess(negative, zero)
        self.assertLess(negative_quality["net_flow_intensity_pct"], 0)

    def test_realtime_net_never_borrows_quote_turnover_as_denominator(self):
        value, quality = self.strategy._fund_factor(
            {
                "decision_clock_mode": "intraday",
                "realtime_trade_date": "2026-09-11",
                "realtime_mf_trade_date": "2026-09-11",
                "realtime_mf_net": 10.0,
                "realtime_amount": 1_000.0,
                "realtime_mf_source_unit": "元",
            },
            "direct_catalyst",
        )

        self.assertIsNone(value)
        self.assertFalse(quality["current_clock_complete"])
        self.assertEqual(quality["stock_clock"], "unknown")

    def test_theme_fund_flow_normalizes_source_units_before_scoring(self):
        yi_score, yi_quality = self.strategy._theme_fund(
            {
                "market_theme_fund_flow": {
                    "net_amount": 2.0,
                    "pct_chg": 1.0,
                    "source_unit": "亿元",
                }
            }
        )
        yuan_score, yuan_quality = self.strategy._theme_fund(
            {
                "market_theme_fund_flow": {
                    "net_amount": 200_000_000.0,
                    "pct_chg": 1.0,
                    "source_unit": "元",
                }
            }
        )

        self.assertEqual(yi_score, yuan_score)
        self.assertEqual(yi_quality["net_amount_yi"], 2.0)
        self.assertEqual(yuan_quality["net_amount_yi"], 2.0)
        self.assertTrue(yuan_quality["unit_compatible"])

    def test_t07_high_score_cannot_bypass_negative_same_clock_fund_gate(self):
        rows = self._run(direct_candidate(realtime_net=-20_000_000))

        self.assertEqual(len(rows), 1)
        result = rows[0]
        self.assertGreater(result["final_score"], 68)
        self.assertEqual(result["entry_eligibility"], "observe")
        self.assertIn("fund_same_clock", result["entry_block_reasons"])
        self.assertEqual(result["signal_grade"], "watch")
        self.assertEqual(result["trade_grade_label"], "仅观察")

    def test_complete_conditions_still_emit_research_candidate_not_trade(self):
        rows = self._run(direct_candidate())

        self.assertEqual(len(rows), 1)
        result = rows[0]
        self.assertEqual(result["entry_eligibility"], "conditions_met")
        self.assertEqual(result["signal_grade"], "watch")
        self.assertEqual(result["trade_grade_state"], "watch")
        self.assertEqual(result["trade_grade_label"], "研究候选")
        self.assertEqual(result["validation_status"], "shadow_only")

    def test_aware_decision_clock_keeps_naive_shanghai_evidence_visible(self):
        candidate = direct_candidate()
        candidate["decision_as_of"] = "2026-09-11T11:00:00+08:00"

        rows = self._run(candidate)

        self.assertEqual(rows[0]["entry_eligibility"], "conditions_met")

    def test_snapshot_bridge_evidence_can_rank_but_not_pass_ledger_gate(self):
        candidate = direct_candidate()
        candidate["event_evidence_source"] = "snapshot_bridge"

        rows = self._run(candidate)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_eligibility"], "observe")
        self.assertIn("event_ledger", rows[0]["entry_block_reasons"])

    def test_t10_old_moneyflow_clock_cannot_pass_current_confirmation(self):
        candidate = direct_candidate()
        candidate["realtime_mf_quote_time"] = "2026-09-11 10:50:00"
        candidate["realtime_mf_received_at"] = "2026-09-11 10:50:05"

        rows = self._run(candidate)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_eligibility"], "observe")
        self.assertIn("same_clock_freshness", rows[0]["entry_block_reasons"])

    def test_receive_clock_before_moneyflow_source_clock_fails_closed(self):
        candidate = direct_candidate()
        candidate["realtime_mf_received_at"] = "2026-09-11 10:58:59"

        rows = self._run(candidate)

        self.assertEqual(rows[0]["entry_eligibility"], "observe")
        self.assertIn("source_and_receive_cutoff", rows[0]["entry_block_reasons"])

    def test_background_theme_mapping_can_rank_but_cannot_become_conditions_met(self):
        candidate = direct_candidate()
        candidate["candidate_lanes"] = ["theme_leader"]
        candidate["direct_event_ids"] = []
        candidate["opinion_sector_name"] = "机器人"
        candidate["theme_current_rank"] = 1
        candidate["theme_current_pool_size"] = 10
        candidate["theme_current_breadth_ratio"] = 0.8
        candidate["sentiment_v06_relations"] = [
            {
                **candidate["sentiment_v06_relations"][0],
                "relation_type": "theme_mapping",
                "relation_status": "background_only",
                "relation_score": 70.0,
            }
        ]

        rows = self._run(candidate)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["primary_lane"], "theme_leader")
        self.assertGreaterEqual(rows[0]["final_score"], 68)
        self.assertEqual(rows[0]["entry_eligibility"], "observe")
        self.assertIn("event_relation", rows[0]["entry_block_reasons"])

    def test_ineligible_theme_lane_cannot_hide_eligible_direct_lane(self):
        candidate = direct_candidate()
        candidate["candidate_lanes"] = ["direct_catalyst", "theme_leader"]
        candidate["opinion_sector_name"] = "机器人"
        candidate["theme_current_rank"] = 1
        candidate["theme_current_pool_size"] = 10
        candidate["theme_current_breadth_ratio"] = 0.8
        candidate["sentiment_v06_relations"].append(
            {
                "relation_id": "t" * 64,
                "canonical_event_id": "e" * 64,
                "relation_type": "theme_mapping",
                "relation_status": "background_only",
                "relation_score": 70.0,
            }
        )
        context = self.strategy.prepare_context({"candidates": [candidate]})
        factor_rows = self.strategy.compute_factors(context)
        factor_rows[0]["lane_factor_scores"]["theme_leader"] = {
            key: 100.0 for key in self.strategy.FACTOR_KEYS
        }

        result = self.strategy.score(factor_rows)[0]

        self.assertEqual(
            result["lane_scores"]["theme_leader"]["entry_eligibility"],
            "observe",
        )
        self.assertEqual(result["primary_lane"], "direct_catalyst")
        self.assertEqual(result["entry_eligibility"], "conditions_met")

    def test_chip_winner_rate_accepts_ratio_and_percentage_source_scales(self):
        ratio_score, ratio_quality = self.strategy._chip_capacity_factor(
            {
                "median_amount_20": 100_000_000,
                "chip_winner_rate": 0.95,
                "realtime_price": 10.0,
            }
        )
        percent_score, percent_quality = self.strategy._chip_capacity_factor(
            {
                "median_amount_20": 100_000_000,
                "chip_winner_rate": 95.0,
                "realtime_price": 10.0,
            }
        )

        self.assertEqual(ratio_score, percent_score)
        self.assertEqual(ratio_quality["chip_winner_rate_pct"], 95.0)
        self.assertEqual(percent_quality["chip_winner_rate_pct"], 95.0)
        self.assertEqual(ratio_quality["chip_penalty"], 10.0)

    def test_secondary_theme_overlap_cannot_bypass_theme_limit(self):
        rows = self.strategy.select(
            [
                {
                    "code": "sh.600001",
                    "industry": "机械设备",
                    "final_score": 80.0,
                    "entry_eligibility": "observe",
                    "opinion_sector_name": "机器人",
                    "sentiment_v06_concentration_themes": ["机器人", "人工智能"],
                },
                {
                    "code": "sz.000001",
                    "industry": "软件服务",
                    "final_score": 79.0,
                    "entry_eligibility": "observe",
                    "opinion_sector_name": "数字经济",
                    "sentiment_v06_concentration_themes": ["数字经济", "人工智能"],
                },
            ]
        )

        self.assertEqual([row["code"] for row in rows], ["sh.600001"])

    def test_t15_repeated_new_version_scoring_is_deterministic(self):
        first = self._run(copy.deepcopy(direct_candidate()))
        second = self._run(copy.deepcopy(direct_candidate()))

        self.assertEqual(first, second)

    def test_selector_emits_research_assessment_without_buy_plan(self):
        selector = StockSelector("a_share_sentiment_v06")

        rows = selector.run({"candidates": [direct_candidate()]})

        self.assertEqual(len(rows), 1)
        self.assertNotIn("trade_plan", rows[0])
        self.assertTrue(rows[0]["research_entry_assessment"]["research_only"])
        self.assertIsNone(
            rows[0]["research_entry_assessment"]["trade_instruction"]
        )


class SelectorDualRecallTests(unittest.TestCase):
    class Repository:
        def load_market_opinion_rows(self, **_kwargs):
            sector = {
                "id": 1,
                "payload_version": 1,
                "trade_date": "2026-09-11",
                "sector_type": "theme",
                "sector_name": "机器人",
                "as_of_datetime": "2026-09-11 10:59:00",
                "sector_score": 75.0,
                "weighted_impact_score": 80.0,
                "news_count": 1,
                "source_count": 1,
                "stock_count": 2,
                "positive_news_count": 1,
                "negative_news_count": 0,
                "top_stocks_json": json.dumps(
                    [
                        {
                            "code": "sh.600002",
                            "name": "乙公司",
                            "industry": "专用机械",
                            "score": 72,
                            "pct_chg": 5.0,
                            "amount": 100_000_000,
                            "match_type": "sector_candidate",
                            "match_reason": "板块候选池",
                        },
                    ],
                    ensure_ascii=False,
                ),
                "top_news_json": json.dumps(
                    [
                        {
                            "raw_id": 10,
                            "title": "工信部印发机器人产业实施方案",
                            "source_id": "government",
                            "source_name": "工信部",
                            "published_at": "2026-09-11 10:00:00",
                            "impact_score": 85.0,
                            "direction": "positive",
                            "event_type": "policy",
                        }
                    ],
                    ensure_ascii=False,
                ),
                "source_json": json.dumps(["government"]),
            }
            return [sector], []

    class DirectOnlyRepository:
        def load_market_opinion_rows(self, **_kwargs):
            return [], []

        def load_sentiment_v06_direct_candidate_codes(self, **_kwargs):
            return ["sh.600001"]

        def load_sentiment_v06_event_rows(self, **_kwargs):
            return [
                {
                    "relation_id": "r" * 64,
                    "canonical_event_id": "e" * 64,
                    "event_revision": 1,
                    "code": "sh.600001",
                    "relation_type": "direct_business",
                    "relation_status": "confirmed_primary",
                    "relation_score": 95.0,
                    "relation_evidence_id": "n" * 64,
                    "relation_evidence_excerpt": "甲公司公告中标10亿元订单",
                    "relation_reason": "公司公告事实",
                    "is_adverse_veto": 0,
                    "valid_from": "2026-09-11 10:01:00",
                    "valid_until": "2026-09-15 15:00:00",
                    "relation_rule_version": "sentiment-v06-relation-v1",
                    "event_type": "major_order",
                    "revision_hash": "v" * 64,
                    "content_role": "original_catalyst",
                    "direction": "positive",
                    "facts_json": json.dumps(
                        {
                            "driver_horizon": "staged",
                            "persistence_score": 76.0,
                            "is_one_off": False,
                            "is_terminal": False,
                        }
                    ),
                    "evidence_id": "n" * 64,
                    "source_id": "company_announcement",
                    "original_publisher": "甲公司",
                    "collection_channel": "newsnow",
                    "source_type": "announcement",
                    "credibility_rule_version": "sentiment-v06-source-v1",
                    "credibility_score": 0.95,
                    "impact_score": 90.0,
                    "is_primary_source": 1,
                    "is_independent_confirmation": 0,
                    "source_time": "2026-09-11 10:00:00",
                    "published_at": "2026-09-11 10:00:00",
                    "first_seen_at": "2026-09-11 10:01:00",
                    "received_at": "2026-09-11 10:01:00",
                    "available_at": "2026-09-11 10:01:00",
                    "effective_until": "2026-09-15 15:00:00",
                    "title": "甲公司公告中标10亿元订单",
                    "evidence_excerpt": "甲公司公告中标10亿元订单",
                    "raw_payload_hash": "p" * 64,
                }
            ]

    def test_t11_current_theme_front_runner_is_recalled_despite_yesterday_weakness(self):
        selector = StockSelector(
            "a_share_sentiment_v06",
            repository=self.Repository(),  # type: ignore[arg-type]
        )
        candidates = [
            {
                "code": "sh.600001",
                "name": "甲公司",
                "industry": "专用机械",
                "trade_date": "2026-09-10",
                "realtime_pct_chg": 6.0,
                "candidate_reasons": [],
            },
            {
                "code": "sh.600002",
                "name": "乙公司",
                "industry": "专用机械",
                "trade_date": "2026-09-10",
                "realtime_pct_chg": 1.0,
                "candidate_reasons": [],
            },
        ]

        diagnostics = selector._attach_market_opinion_context(
            candidates, decision_as_of=DECISION
        )

        self.assertEqual(diagnostics["matched_candidates"], 2)
        self.assertIn("theme_leader", candidates[0]["candidate_lanes"])
        self.assertEqual(candidates[0]["theme_current_rank"], 1)
        self.assertEqual(candidates[0]["theme_current_breadth_ratio"], 1.0)
        self.assertEqual(candidates[0]["opinion_match_type"], "sector_candidate_current")

    def test_direct_event_recall_does_not_require_theme_snapshot_membership(self):
        selector = StockSelector(
            "a_share_sentiment_v06",
            repository=self.DirectOnlyRepository(),  # type: ignore[arg-type]
        )
        candidates = [
            {
                "code": "sh.600001",
                "name": "甲公司",
                "trade_date": "2026-09-10",
                "candidate_reasons": [],
            }
        ]

        diagnostics = selector._attach_market_opinion_context(
            candidates, decision_as_of=DECISION
        )

        self.assertEqual(diagnostics["sector_snapshot_status"], "missing")
        self.assertEqual(diagnostics["direct_recall_codes"], 1)
        self.assertEqual(candidates[0]["candidate_lanes"], ["direct_catalyst"])
        self.assertEqual(candidates[0]["opinion_match_type"], "direct_event_ledger")
        self.assertEqual(candidates[0]["direct_event_ids"], ["e" * 64])


class SelectorMarketRegimeTests(unittest.TestCase):
    def test_current_broad_selloff_is_a_v06_gate_without_changing_alert_contract(self):
        candidates = [
            {
                "code": f"sh.{index:06d}",
                "lifecycle_known": True,
                "list_status": "L",
                "listed_trade_days": 100,
                "realtime_pct_chg": -4.0,
                "realtime_amount": 1_000_000.0,
                "realtime_quote_time": "2026-09-11 10:59:00",
                "realtime_received_at": "2026-09-11 10:59:05",
                "realtime_trade_date": "2026-09-11",
                "realtime_is_stale": False,
                "market_state": "bull",
            }
            for index in range(3000)
        ]

        diagnostics = StockSelector._attach_v06_market_regime(
            candidates,
            decision_as_of=DECISION,
            quote_ttl_seconds=180,
        )

        self.assertEqual(diagnostics["regime"], "defensive")
        self.assertEqual(diagnostics["alert"]["level"], "red")
        self.assertFalse(diagnostics["alert"]["blocking"])
        self.assertTrue(diagnostics["alert"]["selection_allowed"])
        self.assertEqual(candidates[0]["market_regime"], "defensive")
        self.assertEqual(candidates[0]["realtime_market_coverage_ratio"], 1.0)

    def test_market_regime_normalizes_aware_decision_to_shanghai_clock(self):
        candidates = [
            {
                "code": "sh.600001",
                "lifecycle_known": True,
                "list_status": "L",
                "listed_trade_days": 100,
                "realtime_pct_chg": 1.0,
                "realtime_amount": 1_000_000.0,
                "realtime_quote_time": "2026-09-11 10:59:00",
                "realtime_received_at": "2026-09-11 10:59:05",
                "realtime_trade_date": "2026-09-11",
                "realtime_is_stale": False,
                "market_state": "bull",
            }
        ]

        diagnostics = StockSelector._attach_v06_market_regime(
            candidates,
            decision_as_of=datetime.fromisoformat("2026-09-11T11:00:00+08:00"),
            quote_ttl_seconds=180,
        )

        self.assertEqual(diagnostics["observed_count"], 1)
        self.assertEqual(diagnostics["fresh_count"], 1)


if __name__ == "__main__":
    unittest.main()
