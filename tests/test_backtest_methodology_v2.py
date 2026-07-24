from __future__ import annotations

import unittest
from datetime import datetime

from app.backtest.policy import (
    BACKTEST_METHODOLOGY_VERSION,
    LEGACY_BACKTEST_METHODOLOGY_VERSION,
    research_disclosure,
)
from app.backtest.service import BacktestRequest, BacktestService
from app.stock_selection.turtle_trade_plan import EXPECTED_SPEC_SHA256


def bar(trade_date: str, open_price: float, close_price: float) -> dict:
    return {
        "code": "sh.600000",
        "trade_date": trade_date,
        "open": open_price,
        "high": max(open_price, close_price) + 0.2,
        "low": min(open_price, close_price) - 0.2,
        "close": close_price,
        "prev_close": open_price,
        "adj_factor": 1.0,
    }


class BacktestMethodologyV2Tests(unittest.TestCase):
    def test_new_disclosure_does_not_relabel_legacy_runs(self):
        legacy = research_disclosure()
        current = research_disclosure(BACKTEST_METHODOLOGY_VERSION)

        self.assertEqual(legacy["methodology_version"], LEGACY_BACKTEST_METHODOLOGY_VERSION)
        self.assertEqual(current["methodology_version"], BACKTEST_METHODOLOGY_VERSION)
        self.assertIn("T+1", current["risk_notice"])
        self.assertIn("月度成分权重快照", current["risk_notice"])

    def test_pick_does_not_use_signal_day_open_as_entry(self):
        pick = BacktestService()._build_pick(
            {"code": "sh.600000", "open": 9.9, "score": 80, "factors": {}, "explain": {}},
            "2026-07-01",
        )

        self.assertIsNone(pick["entry_price"])
        self.assertEqual(pick["entry_price_type"], "next_open")

    def test_pick_preserves_frozen_trade_plan_for_execution_evaluation(self):
        trade_plan = {
            "version": "selection_trade_plan_v3_risk_control",
            "research_shadow": {
                "version": "selection_trade_plan_v4_turtle_risk",
                "spec_hash": EXPECTED_SPEC_SHA256,
            },
        }

        pick = BacktestService()._build_pick(
            {
                "code": "sh.600000",
                "score": 80,
                "factors": {},
                "explain": {},
                "trade_plan": trade_plan,
            },
            "2026-07-01",
        )

        self.assertEqual(pick["trade_plan"], trade_plan)

    def test_one_day_trade_enters_after_signal_date(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                bar("2026-07-02", 10.5, 10.7),
                bar("2026-07-03", 10.8, 10.6),
            ]
        }
        picks = [{"code": "sh.600000", "entry_price": None, "entry_price_type": "next_open"}]
        request = BacktestRequest(
            strategy_id="test_strategy",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="1d",
            trade_strategy_id="next_open_1d",
        )

        trades, rejections = service._build_trades("run", "test_strategy", "2026-07-01", picks, request)

        self.assertEqual(rejections, {})
        self.assertEqual(trades[0]["trade_date"], "2026-07-01")
        self.assertEqual(trades[0]["entry_date"], "2026-07-02")
        self.assertEqual(trades[0]["entry_price"], 10.5)
        self.assertEqual(trades[0]["exit_date_1d"], "2026-07-03")
        self.assertEqual(picks[0]["entry_price"], 10.5)

    def test_three_day_trade_exits_on_third_holding_session(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                bar("2026-07-02", 10.0, 10.1),
                bar("2026-07-03", 10.2, 10.3),
                bar("2026-07-06", 10.4, 10.8),
            ]
        }
        request = BacktestRequest(
            strategy_id="test_strategy",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="3d",
            trade_strategy_id="hold_3d_close",
        )

        trades, _ = service._build_trades(
            "run",
            "test_strategy",
            "2026-07-01",
            [{"code": "sh.600000", "entry_price": None}],
            request,
        )

        self.assertEqual(trades[0]["entry_date"], "2026-07-02")
        self.assertEqual(trades[0]["exit_date_3d"], "2026-07-06")
        self.assertEqual(trades[0]["exit_price_3d"], 10.8)

    def test_turtle_trade_waits_for_trigger_adds_only_after_profit_and_trails_winner(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                {
                    **bar("2026-07-02", 9.90, 10.02),
                    "high": 10.05,
                    "low": 9.85,
                },
                {
                    **bar("2026-07-03", 10.05, 10.12),
                    "high": 10.15,
                    "low": 9.95,
                },
                {
                    **bar("2026-07-06", 10.15, 10.25),
                    "high": 10.30,
                    "low": 10.05,
                },
                {
                    **bar("2026-07-07", 10.25, 10.35),
                    "high": 10.40,
                    "low": 10.15,
                },
                {
                    **bar("2026-07-08", 10.35, 10.50),
                    "high": 10.55,
                    "low": 10.25,
                },
                {
                    **bar("2026-07-09", 10.30, 10.05),
                    "high": 10.35,
                    "low": 10.00,
                },
            ]
        }
        request = BacktestRequest(
            strategy_id="a_share_sentiment",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="turtle_selection_risk_v1",
            trade_strategy_id="turtle_selection_risk_v1",
        )
        pick = {
            "code": "sh.600000",
            "entry_price": None,
            "trade_plan": {
                "version": "selection_trade_plan_v3_risk_control",
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "state": "watch",
                    "n20": 0.20,
                    "entry": {
                        "setup": "breakout_20d",
                        "trigger": 10.00,
                        "zone_low": 10.00,
                        "zone_high": 10.10,
                        "expires_after_trade_days": 3,
                    },
                    "risk": {},
                    "exits": {"trend_exit": 9.50},
                },
            },
        }

        trades, rejections = service._build_trades(
            "run",
            request.strategy_id,
            "2026-07-01",
            [pick],
            request,
        )

        self.assertEqual(rejections, {})
        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade["entry_date"], "2026-07-02")
        self.assertEqual(trade["first_entry_price"], 10.00)
        self.assertEqual(trade["add_count"], 1)
        self.assertEqual(trade["unit_count"], 2)
        self.assertEqual(trade["entry_price"], 10.05)
        self.assertEqual(trade["exit_date_3d"], "2026-07-09")
        self.assertEqual(trade["exit_reason"], "trailing_stop")
        self.assertGreater(trade["return_3d_pct"], 0)
        self.assertEqual(
            trade["trade_plan_spec_hash"],
            EXPECTED_SPEC_SHA256,
        )

    def test_turtle_time_exit_requires_no_half_n_progress_after_five_sessions(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                {
                    **bar(f"2026-07-{day:02d}", 10.00, 10.04),
                    "high": 10.08,
                    "low": 9.90,
                }
                for day in (2, 3, 6, 7, 8)
            ]
        }
        request = BacktestRequest(
            strategy_id="a_share_sentiment",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="turtle_selection_risk_v1",
            trade_strategy_id="turtle_selection_risk_v1",
        )
        pick = {
            "code": "sh.600000",
            "entry_price": None,
            "trade_plan": {
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "state": "watch",
                    "n20": 0.20,
                    "entry": {
                        "setup": "breakout_20d",
                        "trigger": 10.00,
                        "zone_low": 10.00,
                        "zone_high": 10.10,
                        "expires_after_trade_days": 3,
                    },
                    "risk": {},
                    "exits": {"trend_exit": 9.50},
                },
            },
        }

        trades, rejections = service._build_trades(
            "run",
            request.strategy_id,
            "2026-07-01",
            [pick],
            request,
        )

        self.assertEqual(rejections, {})
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["add_count"], 0)
        self.assertEqual(trades[0]["exit_date_3d"], "2026-07-08")
        self.assertEqual(trades[0]["exit_reason"], "time_exit_no_progress")

    def test_turtle_open_winner_is_censored_only_after_twenty_holding_sessions(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                {
                    **bar(
                        f"2026-07-{day:02d}",
                        10.00 + index * 0.04,
                        10.04 + index * 0.04,
                    ),
                    "high": 10.08 + index * 0.04,
                    "low": 9.90 + index * 0.04,
                }
                for index, day in enumerate(range(2, 22))
            ]
        }
        request = BacktestRequest(
            strategy_id="a_share_sentiment",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="turtle_selection_risk_v1",
            trade_strategy_id="turtle_selection_risk_v1",
        )
        pick = {
            "code": "sh.600000",
            "entry_price": None,
            "trade_plan": {
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "state": "watch",
                    "n20": 0.20,
                    "entry": {
                        "setup": "breakout_20d",
                        "trigger": 10.00,
                        "zone_low": 10.00,
                        "zone_high": 10.10,
                        "expires_after_trade_days": 3,
                    },
                    "risk": {},
                    "exits": {"trend_exit": 9.50},
                },
            },
        }

        trades, rejections = service._build_trades(
            "run",
            request.strategy_id,
            "2026-07-01",
            [pick],
            request,
        )

        self.assertEqual(trades, [])
        self.assertEqual(
            rejections,
            {"turtle_open_at_evaluation_horizon": 1},
        )

    def test_turtle_intraday_trigger_uses_stop_first_when_daily_path_is_ambiguous(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                {
                    **bar("2026-07-02", 9.80, 9.95),
                    "high": 10.08,
                    "low": 9.55,
                },
            ]
        }
        request = BacktestRequest(
            strategy_id="a_share_sentiment",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="turtle_selection_risk_v1",
            trade_strategy_id="turtle_selection_risk_v1",
        )
        pick = {
            "code": "sh.600000",
            "entry_price": None,
            "trade_plan": {
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "state": "watch",
                    "n20": 0.20,
                    "entry": {
                        "setup": "breakout_20d",
                        "trigger": 10.00,
                        "zone_low": 10.00,
                        "zone_high": 10.10,
                        "expires_after_trade_days": 3,
                    },
                    "risk": {},
                    "exits": {"trend_exit": 9.50},
                },
            },
        }

        trades, rejections = service._build_trades(
            "run",
            request.strategy_id,
            "2026-07-01",
            [pick],
            request,
        )

        self.assertEqual(rejections, {})
        self.assertEqual(len(trades), 1)
        self.assertEqual(
            trades[0]["exit_reason"],
            "same_day_ohlc_ambiguous_stop_first",
        )
        self.assertEqual(trades[0]["exit_price_3d"], 9.60)
        self.assertLess(trades[0]["return_3d_pct"], 0)

    def test_turtle_holding_does_not_time_exit_before_five_complete_sessions(self):
        service = BacktestService()
        service._fetch_future_bars = lambda *_args, **_kwargs: {
            "sh.600000": [
                {
                    **bar("2026-07-02", 10.00, 10.05),
                    "high": 10.08,
                    "low": 9.90,
                },
                {
                    **bar("2026-07-03", 10.05, 10.10),
                    "high": 10.12,
                    "low": 9.95,
                },
            ]
        }
        request = BacktestRequest(
            strategy_id="a_share_sentiment",
            start_date="2026-07-01",
            end_date="2026-07-01",
            return_mode="turtle_selection_risk_v1",
            trade_strategy_id="turtle_selection_risk_v1",
        )
        pick = {
            "code": "sh.600000",
            "entry_price": None,
            "trade_plan": {
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "state": "watch",
                    "n20": 0.20,
                    "entry": {
                        "setup": "breakout_20d",
                        "trigger": 10.00,
                        "zone_low": 10.00,
                        "zone_high": 10.10,
                        "expires_after_trade_days": 3,
                    },
                    "risk": {},
                    "exits": {"trend_exit": 9.50},
                },
            },
        }

        trades, rejections = service._build_trades(
            "run",
            request.strategy_id,
            "2026-07-01",
            [pick],
            request,
        )

        self.assertEqual(trades, [])
        self.assertEqual(rejections, {"turtle_holding_pending": 1})

    def test_unknown_fundamentals_are_removed_but_t_day_valuation_is_kept(self):
        row = BacktestService._sanitize_point_in_time_fields(
            {
                "trade_date": "2026-07-16",
                "roe": 20,
                "pe_tushare": 10,
                "eps": 1.2,
                "is_st": True,
                "turnover_rate": 1.5,
            }
        )

        self.assertIsNone(row["roe"])
        self.assertEqual(row["pe_tushare"], 10)
        self.assertIsNone(row["eps"])
        self.assertTrue(row["pit_fundamental_unknown"])
        self.assertTrue(row["is_st"])
        self.assertTrue(row["pit_status_unknown"])
        self.assertEqual(row["turnover_rate"], 1.5)

    def test_announcement_date_fundamentals_are_kept_only_when_known_by_signal_day(self):
        known = BacktestService._sanitize_point_in_time_fields(
            {
                "trade_date": "2026-07-16",
                "fundamental_publish_date": "2026-04-29",
                "fundamental_period": "2026-03-31",
                "pit_fundamental_available": True,
                "roe": 12.5,
                "eps": 1.2,
                "pit_status_available": True,
                "is_st": False,
            }
        )
        future = BacktestService._sanitize_point_in_time_fields(
            {
                "trade_date": "2026-07-16",
                "fundamental_publish_date": "2026-08-01",
                "fundamental_period": "2026-06-30",
                "pit_fundamental_available": True,
                "roe": 13.0,
                "eps": 1.3,
                "pit_status_available": True,
                "is_st": False,
            }
        )

        self.assertEqual(known["roe"], 12.5)
        self.assertEqual(known["eps"], 1.2)
        self.assertTrue(known["pit_fundamental_available"])
        self.assertFalse(known["pit_fundamental_unknown"])
        self.assertIsNone(future["roe"])
        self.assertIsNone(future["eps"])
        self.assertFalse(future["pit_fundamental_available"])

    def test_point_in_time_st_flag_is_preserved_only_with_interval_evidence(self):
        historical = BacktestService._sanitize_point_in_time_fields(
            {"is_st": True, "pit_status_available": True}
        )
        current_only = BacktestService._sanitize_point_in_time_fields(
            {"is_st": True, "pit_status_available": False}
        )

        self.assertTrue(historical["is_st"])
        self.assertFalse(historical["pit_status_unknown"])
        self.assertTrue(current_only["is_st"])
        self.assertTrue(current_only["pit_status_unknown"])

    def test_run_insert_persists_reproducibility_metadata(self):
        executed: dict = {}

        class FakeRepository:
            @staticmethod
            def create_run(values):
                executed.update(values)

        class FakeStrategy:
            config = {"max_picks": 3}

        class FakeSelector:
            strategy_meta = {"version": "v2.1"}
            strategy = FakeStrategy()

        request = BacktestRequest(
            strategy_id="test_strategy",
            start_date="2026-07-01",
            end_date="2026-07-02",
            trade_strategy_id="next_open_1d",
            is_system_test=True,
            validation_baseline_id="b3-smoke",
        )
        service = BacktestService(repository=FakeRepository())
        service._fetch_data_cutoff = lambda _end_date: "2026-07-02"

        service._create_run("run", request, FakeSelector(), datetime(2026, 7, 15), status="queued", progress_total_days=2)

        self.assertEqual(executed["methodology_version"], BACKTEST_METHODOLOGY_VERSION)
        self.assertTrue(executed["strategy_config_hash"])
        self.assertEqual(executed["is_system_test"], 1)
        self.assertEqual(executed["validation_baseline_id"], "b3-smoke")
        self.assertEqual(executed["status"], "queued")
        self.assertEqual(executed["progress_total_days"], 2)
        request_json = __import__("json").loads(executed["request_json"])
        methodology_json = __import__("json").loads(executed["methodology_json"])
        self.assertEqual(request_json["universe_code"], "ALL_A")
        self.assertEqual(methodology_json["universe_code"], "ALL_A")

    def test_missing_index_snapshot_fails_closed_before_candidate_loading(self):
        class FakeRepository:
            @staticmethod
            def load_index_universe_snapshot(_universe_code, _trade_date):
                return {}

        service = BacktestService(repository=FakeRepository())

        with self.assertRaisesRegex(ValueError, "fail-closed"):
            service._ensure_index_universe_snapshot("000300.SH", "2026-07-16")


if __name__ == "__main__":
    unittest.main()
