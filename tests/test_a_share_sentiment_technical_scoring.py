from __future__ import annotations

import unittest

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.selector import StockSelector


def _candidate(code: str, *, strong_structure: bool) -> dict:
    common = {
        "code": code,
        "name": code,
        "industry": "软件服务",
        "is_st": False,
        "close": 10.0 if strong_structure else 9.0,
        "amount": 120_000_000,
        "total_mv": 1_000_000,
        "pct_chg_1d": 2.0,
        "realtime_pct_chg": 2.0,
        "intraday_high_drawdown_pct": -0.5,
        "intraday_open_drawdown_pct": 1.0,
        "realtime_amount_ratio": 0.8,
        "net_mf_amount": 900,
        "buy_lg_amount": 800,
        "sell_lg_amount": 400,
        "buy_elg_amount": 500,
        "sell_elg_amount": 250,
        "market_strength": 68,
        "opinion_sector_score": 82,
        "opinion_news_count": 5,
        "opinion_source_count": 3,
        "opinion_positive_news_count": 4,
        "opinion_negative_news_count": 0,
        "opinion_weighted_impact_score": 80,
        "opinion_stock_score": 76,
        "opinion_stock_recognition_score": 75,
        "opinion_match_type": "sector_candidate",
        "opinion_match_reason": "热点板块候选",
        "opinion_sources": [],
        "market_theme_tier": "mainline",
        "market_theme_label": "主线命中",
        "market_theme_score_delta": 0,
    }
    if strong_structure:
        common.update(
            {
                "realtime_price": 10.3,
                "ma5": 10.1,
                "ma10": 9.9,
                "ma20": 9.7,
                "ma30": 9.5,
                "avg_amount_5": 100_000_000,
                "avg_amount_20": 80_000_000,
                "volume_ratio": 1.6,
                "chip_cost_15pct": 9.4,
                "chip_cost_50pct": 10.0,
                "chip_cost_85pct": 10.6,
                "chip_weight_avg": 10.0,
                "chip_winner_rate": 55,
            }
        )
    else:
        common.update(
            {
                "realtime_price": 9.2,
                "ma5": 9.4,
                "ma10": 9.6,
                "ma20": 9.8,
                "ma30": 10.0,
                "avg_amount_5": 180_000_000,
                "avg_amount_20": 220_000_000,
                "volume_ratio": 0.6,
                "chip_cost_15pct": 8.0,
                "chip_cost_50pct": 10.5,
                "chip_cost_85pct": 13.0,
                "chip_weight_avg": 10.6,
                "chip_winner_rate": 20,
            }
        )
    return common


class AShareSentimentTechnicalScoringTests(unittest.TestCase):
    def setUp(self):
        self.strategy = StrategyLoader().load_strategy("a_share_sentiment")

    def test_daily_volume_and_chip_are_soft_scores_not_hard_filters(self):
        strong = _candidate("strong", strong_structure=True)
        weak = _candidate("weak", strong_structure=False)

        context = self.strategy.prepare_context({"candidates": [strong, weak]})

        self.assertEqual([item["code"] for item in context["candidates"]], ["strong", "weak"])

        factor_rows = self.strategy.compute_factors(context)
        by_code = {item["code"]: item for item in factor_rows}
        for factor in ("daily_trend", "volume_confirm", "chip_structure"):
            self.assertGreater(by_code["strong"]["factors"][factor], by_code["weak"]["factors"][factor])

        scored = {item["code"]: item for item in self.strategy.score(factor_rows)}
        self.assertGreater(scored["strong"]["score"], scored["weak"]["score"])
        self.assertEqual(by_code["weak"]["strategy_raw_metrics"]["daily_trend_state"], "weak")
        self.assertTrue(any("不作硬过滤" in risk for risk in by_code["weak"]["candidate_risks"]))

    def test_missing_structure_data_is_neutral_instead_of_excluded(self):
        candidate = _candidate("missing", strong_structure=True)
        for key in (
            "ma5",
            "ma10",
            "ma20",
            "ma30",
            "avg_amount_5",
            "avg_amount_20",
            "volume_ratio",
            "chip_cost_15pct",
            "chip_cost_50pct",
            "chip_cost_85pct",
            "chip_weight_avg",
            "chip_winner_rate",
        ):
            candidate.pop(key, None)

        context = self.strategy.prepare_context({"candidates": [candidate]})
        factor_row = self.strategy.compute_factors(context)[0]

        self.assertEqual(len(context["candidates"]), 1)
        self.assertEqual(factor_row["factors"]["daily_trend"], 50.0)
        self.assertEqual(factor_row["factors"]["volume_confirm"], 50.0)
        self.assertEqual(factor_row["factors"]["chip_structure"], 50.0)
        self.assertEqual(factor_row["strategy_raw_metrics"]["chip_structure_state"], "unavailable")

    def test_factor_weights_remain_normalized(self):
        total = sum(float(value or 0) for value in self.strategy.config["weights"].values())
        self.assertAlmostEqual(total, 1.0, places=8)

    def test_market_context_uses_broad_index_and_breadth_components(self):
        strong = _candidate("market-strong", strong_structure=True)
        weak = _candidate("market-weak", strong_structure=True)
        strong.update(
            {
                "market_index_trend_score": 78,
                "market_index_day_score": 72,
                "market_breadth_score": 68,
                "market_volume_score": 62,
                "market_index_pct_chg": 1.2,
                "market_index_count": 3,
                "market_index_codes": "000300.SH,000852.SH,000905.SH",
            }
        )
        weak.update(
            {
                "market_index_trend_score": 28,
                "market_index_day_score": 32,
                "market_breadth_score": 35,
                "market_volume_score": 40,
                "market_index_pct_chg": -1.4,
                "market_index_count": 3,
                "market_index_codes": "000300.SH,000852.SH,000905.SH",
            }
        )

        rows = self.strategy.compute_factors(self.strategy.prepare_context({"candidates": [strong, weak]}))
        by_code = {item["code"]: item for item in rows}

        self.assertGreater(by_code["market-strong"]["factors"]["market_context"], by_code["market-weak"]["factors"]["market_context"])
        self.assertEqual(by_code["market-strong"]["strategy_raw_metrics"]["market_index_count"], 3)
        self.assertIn("沪深300", by_code["market-strong"]["strategy_raw_metrics"]["market_context_reason"])

    def test_deep_v_repair_is_kept_as_watch_instead_of_marked_tradable(self):
        candidate = _candidate("deep-v", strong_structure=True)
        candidate.update(
            {
                "realtime_pct_chg": 4.0,
                "intraday_high_drawdown_pct": -0.5,
                "intraday_open_drawdown_pct": 5.0,
                "intraday_low_pct": -9.5,
                "intraday_amplitude_pct": 14.0,
                "intraday_repair_pct": 15.0,
            }
        )

        context = self.strategy.prepare_context({"candidates": [candidate]})
        factor_row = self.strategy.compute_factors(context)[0]

        self.assertEqual(factor_row["strategy_raw_metrics"]["trade_signal_state"], "watch")
        self.assertEqual(factor_row["strategy_raw_metrics"]["trade_signal_label"], "深V高波动观察")
        self.assertTrue(any("盘中曾深跌" in risk for risk in factor_row["candidate_risks"]))

    def test_neutral_direct_news_cannot_create_a_direct_news_candidate(self):
        neutral = _candidate("neutral-news", strong_structure=True)
        neutral.update(
            {
                "opinion_match_type": "direct_news_match",
                "opinion_stock_score": 80,
                "opinion_stock_news": [
                    {
                        "title": "公司发布业务进展公告",
                        "direction": "neutral",
                        "impact_score": 90,
                        "timeliness_score": 95,
                    }
                ],
            }
        )
        positive = _candidate("positive-news", strong_structure=True)
        positive.update(
            {
                "opinion_match_type": "direct_news_match",
                "opinion_stock_score": 80,
                "opinion_stock_news": [
                    {
                        "title": "公司获大额订单",
                        "direction": "positive",
                        "impact_score": 90,
                        "timeliness_score": 95,
                    }
                ],
            }
        )

        context = self.strategy.prepare_context({"candidates": [neutral, positive]})

        self.assertEqual([item["code"] for item in context["candidates"]], ["positive-news"])

    def test_sentiment_candidate_uses_pit_fundamentals_not_legacy_snapshot(self):
        selector = StockSelector(strategy_id="a_share_sentiment")
        row = {
            "code": "sh.603127",
            "name": "昭衍新药",
            "industry": "医疗保健",
            "roe": 99,
            "roa": 88,
            "eps": 9,
            "legacy_fundamental_period": "2024-12-31",
            "pit_roe": 8.5,
            "pit_roa": 4.2,
            "pit_eps": 0.31,
            "pit_revenue_yoy": 6.0,
            "pit_profit_yoy": -3.0,
            "pit_fundamental_period": "2026-03-31",
            "pit_fundamental_publish_date": "2026-04-28",
            "pit_fundamental_source": "tushare_fina_indicator",
            "pit_fundamental_available": 1,
        }

        candidate = selector._build_candidate(row)

        self.assertEqual(candidate["roe"], 8.5)
        self.assertEqual(candidate["roa"], 4.2)
        self.assertEqual(candidate["eps"], 0.31)
        self.assertEqual(candidate["fundamental_period"], "2026-03-31")
        self.assertEqual(candidate["fundamental_publish_date"], "2026-04-28")
        self.assertTrue(candidate["pit_fundamental_available"])

    def test_only_one_tradable_grade_per_theme_without_removing_candidates(self):
        selector = StockSelector(strategy_id="a_share_sentiment")

        def item(code: str, theme: str, score: float) -> dict:
            return {
                "code": code,
                "name": code,
                "score": score,
                "close": 10.0,
                "opinion_sector_name": theme,
                "opinion_sector_type": "theme",
                "factors": {"daily_trend": 75, "chip_structure": 70, "market_context": 60},
                "strategy_raw_metrics": {
                    "sentiment_mode": "market_opinion_v2",
                    "opinion_sector_name": theme,
                    "opinion_sector_type": "theme",
                    "trade_signal_state": "tradable",
                    "trade_signal_label": "强势可交易",
                    "trade_signal_reason": "盘中确认",
                    "daily_trend_state": "confirmed",
                    "daily_trend_label": "日线趋势确认",
                    "chip_structure_state": "supportive",
                    "chip_structure_label": "筹码结构有支撑",
                },
                "trade_plan": {
                    "version": "selection_trade_plan_v3_risk_control",
                    "risk_control": {
                        "compliant": True,
                        "actual_stop_loss_pct": 5.0,
                        "take_profit_1_risk_reward": 1.2,
                    },
                },
                "candidate_reasons": [],
                "candidate_risks": [],
            }

        finalized = selector.finalize_sentiment_results(
            [item("same-theme-1", "创新药", 80), item("same-theme-2", "创新药", 79), item("other-theme", "机器人", 78)]
        )

        self.assertEqual(len(finalized), 3)
        self.assertEqual([row["trade_grade_state"] for row in finalized], ["tradable", "watch", "tradable"])
        self.assertEqual(finalized[1]["theme_trade_slot_state"], "duplicate_theme")
        self.assertIn("同一主题", finalized[1]["trade_grade_reason"])
        self.assertEqual(sum(row["trade_grade_state"] == "tradable" for row in finalized if row["opinion_sector_name"] == "创新药"), 1)


if __name__ == "__main__":
    unittest.main()
