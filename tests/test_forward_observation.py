from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from app.stock_selection.forward_observation import (
    ForwardObservationRepository,
    ForwardObservationService,
    ForwardProtocolDriftError,
    ForwardProtocolSpec,
    build_forward_outcome,
    build_turtle_forward_outcome,
    strategy_config_hash,
)
from app.stock_selection.turtle_trade_plan import EXPECTED_SPEC_SHA256
from app.stock_selection.run_tasks import SelectionRunService, SelectionTaskPayload
from scripts.run_strategy_forward_observation import (
    BASE_REQUEST,
    STRATEGY_VERSION,
    build_protocol_spec,
    submit_observation,
    verify_immutable_source,
)


def protocol_spec(**overrides):
    values = {
        "protocol_id": "protocol-v1",
        "strategy_id": "a_share_sentiment",
        "strategy_version": "0.3.1",
        "protocol_version": "prospective_after_close_v1",
        "execution_time": "16:20:00",
        "timezone": "Asia/Shanghai",
        "entry_rule": "next_tradable_open",
        "horizons": (1, 3, 5, 20),
        "benchmark_codes": ("000300.SH",),
        "minimum_observation_days": 20,
        "minimum_candidate_count": 50,
        "immutable_tag": "tag-v1",
        "implementation_commit": "a" * 40,
        "strategy_config_hash": "b" * 64,
        "ai_policy": "progressive_recorded_fallback",
        "strategy_snapshot": {"version": 1},
        "request": dict(BASE_REQUEST),
        "started_on": "2026-07-21",
    }
    values.update(overrides)
    return ForwardProtocolSpec(**values)


def existing_protocol_row(spec: ForwardProtocolSpec):
    return {
        "protocol_id": spec.protocol_id,
        "strategy_id": spec.strategy_id,
        "strategy_version": spec.strategy_version,
        "protocol_version": spec.protocol_version,
        "execution_time": timedelta(hours=16, minutes=20),
        "timezone": spec.timezone,
        "entry_rule": spec.entry_rule,
        "horizons_json": list(spec.horizons),
        "benchmark_codes_json": list(spec.benchmark_codes),
        "minimum_observation_days": spec.minimum_observation_days,
        "minimum_candidate_count": spec.minimum_candidate_count,
        "immutable_tag": spec.immutable_tag,
        "implementation_commit": spec.implementation_commit,
        "strategy_config_hash": spec.strategy_config_hash,
        "ai_policy": spec.ai_policy,
        "strategy_snapshot_json": spec.strategy_snapshot,
        "request_json": spec.request,
        "started_on": date.fromisoformat(spec.started_on),
    }


class ForwardProtocolTests(unittest.TestCase):
    def test_protocol_comparison_accepts_equivalent_database_types(self):
        spec = protocol_spec()
        ForwardObservationRepository._assert_protocol_unchanged(existing_protocol_row(spec), spec)

    def test_protocol_comparison_fails_closed_on_threshold_drift(self):
        spec = protocol_spec()
        existing = existing_protocol_row(spec)
        existing["request_json"] = {**spec.request, "score_threshold": 55.0}

        with self.assertRaisesRegex(ForwardProtocolDriftError, "request"):
            ForwardObservationRepository._assert_protocol_unchanged(existing, spec)

    def test_config_hash_is_order_independent(self):
        first = strategy_config_hash({"version": "1", "id": "x"}, {"b": 2, "a": 1})
        second = strategy_config_hash({"id": "x", "version": "1"}, {"a": 1, "b": 2})
        self.assertEqual(first, second)

    def test_current_protocol_spec_locks_threshold_version_and_horizons(self):
        spec = build_protocol_spec("c" * 40)
        self.assertEqual(spec.strategy_version, STRATEGY_VERSION)
        self.assertEqual(spec.request["score_threshold"], 60.0)
        self.assertEqual(spec.request["max_picks"], 3)
        self.assertEqual(spec.horizons, (1, 3, 5, 20))
        self.assertEqual(spec.entry_rule, "next_tradable_open")

    def test_source_freeze_allows_descendant_commit_when_strategy_paths_are_unchanged(self):
        tag_commit = "a" * 40
        head_commit = "b" * 40

        def git_output(*args):
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return head_commit
            if args[:3] == ("rev-list", "-n", "1"):
                return tag_commit
            if args[:2] == ("merge-base", "--is-ancestor"):
                return ""
            if args[:2] == ("diff", "--name-only"):
                return ""
            raise AssertionError(args)

        with patch("scripts.run_strategy_forward_observation._git_output", side_effect=git_output):
            self.assertEqual(verify_immutable_source("tag-v1"), tag_commit)

    def test_source_freeze_rejects_strategy_path_drift_after_tag(self):
        def git_output(*args):
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return "b" * 40
            if args[:3] == ("rev-list", "-n", "1"):
                return "a" * 40
            if args[:2] == ("merge-base", "--is-ancestor"):
                return ""
            if args[:2] == ("diff", "--name-only"):
                return "app/stock_selection/selector.py"
            raise AssertionError(args)

        with patch("scripts.run_strategy_forward_observation._git_output", side_effect=git_output):
            with self.assertRaisesRegex(RuntimeError, "source drift"):
                verify_immutable_source("tag-v1")


class ForwardOutcomeTests(unittest.TestCase):
    @staticmethod
    def _path(days: int = 20):
        start = date(2026, 7, 21)
        return [
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "open": 10.0,
                "high": 10.5 + index * 0.1,
                "low": 9.5 - index * 0.01,
                "close": 10.0 + index * 0.1,
                "adj_factor": 1.0,
            }
            for index in range(days)
        ]

    def test_outcome_uses_next_open_and_exact_trading_day_horizons(self):
        path = self._path()
        benchmark = {
            "000300.SH": [
                {"trade_date": row["trade_date"], "open": 100.0, "close": 100.0 + index}
                for index, row in enumerate(path)
            ]
        }

        outcome = build_forward_outcome(path, horizons=(1, 3, 5, 20), benchmark_paths=benchmark)

        self.assertEqual(outcome["status"], "complete")
        self.assertEqual(outcome["entry_trade_date"], "2026-07-21")
        self.assertEqual(outcome["entry_price"], 10.0)
        self.assertEqual(outcome["returns"]["1"], 0.0)
        self.assertEqual(outcome["returns"]["3"], 2.0)
        self.assertEqual(outcome["returns"]["5"], 4.0)
        self.assertEqual(outcome["returns"]["20"], 19.0)
        self.assertEqual(outcome["horizons"]["3"]["benchmark_returns_pct"]["000300.SH"], 2.0)
        self.assertEqual(outcome["horizons"]["3"]["excess_returns_pct"]["000300.SH"], 0.0)
        self.assertEqual(outcome["price_adjustment_mode"], "adjusted_total_return")

    def test_outcome_adjusts_for_split_and_keeps_immature_horizons_null(self):
        path = self._path(days=3)
        path[2]["close"] = 5.5
        path[2]["adj_factor"] = 2.0

        outcome = build_forward_outcome(path, horizons=(1, 3, 5, 20))

        self.assertEqual(outcome["status"], "partial")
        self.assertEqual(outcome["returns"]["3"], 10.0)
        self.assertIsNone(outcome["returns"]["5"])
        self.assertIsNone(outcome["max_favorable_5d_pct"])

    def test_missing_adjustment_factor_is_explicit_raw_fallback(self):
        path = self._path(days=1)
        path[0]["adj_factor"] = None

        outcome = build_forward_outcome(path, horizons=(1,))

        self.assertEqual(outcome["status"], "complete")
        self.assertEqual(outcome["price_adjustment_mode"], "raw_fallback")

    def test_same_day_open_protocol_is_preserved_in_outcome(self):
        path = self._path(days=1)

        outcome = build_forward_outcome(
            path,
            horizons=(1,),
            entry_rule="same_day_open",
        )

        self.assertEqual(outcome["entry_trade_date"], "2026-07-21")
        self.assertEqual(outcome["entry_rule"], "same_day_open")
        self.assertEqual(outcome["returns"]["1"], 0.0)

    @staticmethod
    def _turtle_item() -> dict:
        return {
            "code": "sh.600000",
            "selected_price_quote_time": "2026-07-21 09:25:00",
            "trade_plan": {
                "research_shadow": {
                    "version": "selection_trade_plan_v4_turtle_risk",
                    "spec_hash": EXPECTED_SPEC_SHA256,
                    "decision_time": "2026-07-21 09:25:00",
                    "state": "watch",
                    "reference_price": 9.95,
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
                }
            },
        }

    def test_turtle_forward_outcome_persists_plan_entry_add_and_exit_evidence(self):
        path = [
            {
                "trade_date": f"2026-07-{21 + index:02d}",
                "open": 9.95 if index == 0 else 10.05 + index * 0.05,
                "high": 10.05 + index * 0.10,
                "low": 9.85 + index * 0.05,
                "close": 10.02 + index * 0.10,
                "prev_close": 9.90 if index == 0 else 10.02 + (index - 1) * 0.10,
                "adj_factor": 1.0,
            }
            for index in range(6)
        ]
        path[-1]["low"] = 9.95
        path[-1]["close"] = 10.00

        evaluation, events = build_turtle_forward_outcome(
            raw_item=self._turtle_item(),
            price_path=path,
            strategy_id="a_share_sentiment",
            observation_id="obs-v4",
            signal_trade_date="2026-07-21",
            source_snapshot_id="snapshot-v4",
        )

        self.assertEqual(evaluation["status"], "complete")
        self.assertEqual(
            evaluation["trade"]["trade_plan_spec_hash"],
            EXPECTED_SPEC_SHA256,
        )
        self.assertEqual(
            [item["event_type"] for item in events],
            ["plan_created", "entry_triggered", "add_triggered", "exit_triggered"],
        )
        self.assertEqual(events[0]["snapshot_id"], "snapshot-v4")

    def test_turtle_forward_outcome_stays_pending_before_holding_window_matures(self):
        path = [
            {
                "trade_date": f"2026-07-{21 + index:02d}",
                "open": 10.00,
                "high": 10.08,
                "low": 9.90,
                "close": 10.04,
                "prev_close": 9.95,
                "adj_factor": 1.0,
            }
            for index in range(2)
        ]

        evaluation, events = build_turtle_forward_outcome(
            raw_item=self._turtle_item(),
            price_path=path,
            strategy_id="a_share_sentiment",
            observation_id="obs-pending",
            signal_trade_date="2026-07-21",
            source_snapshot_id=None,
        )

        self.assertEqual(evaluation["status"], "pending")
        self.assertEqual(
            evaluation["rejection_reason"],
            "turtle_holding_pending",
        )
        self.assertEqual(
            [item["event_type"] for item in events],
            ["plan_created"],
        )

    def test_turtle_forward_outcome_marks_open_winner_as_censored(self):
        path = [
            {
                "trade_date": (
                    date(2026, 7, 21) + timedelta(days=index)
                ).isoformat(),
                "open": 10.00 + index * 0.04,
                "high": 10.08 + index * 0.04,
                "low": 9.90 + index * 0.04,
                "close": 10.04 + index * 0.04,
                "prev_close": 9.95 + index * 0.04,
                "adj_factor": 1.0,
            }
            for index in range(20)
        ]

        evaluation, events = build_turtle_forward_outcome(
            raw_item=self._turtle_item(),
            price_path=path,
            strategy_id="a_share_sentiment",
            observation_id="obs-censored",
            signal_trade_date="2026-07-21",
            source_snapshot_id=None,
        )

        self.assertEqual(evaluation["status"], "censored")
        self.assertEqual(
            evaluation["rejection_reason"],
            "turtle_open_at_evaluation_horizon",
        )
        self.assertEqual(events[-1]["event_type"], "evaluation_censored")

    def test_refresh_keeps_pick_open_when_signal_horizon_matures_before_trade_plan(self):
        repository = MagicMock()
        events = MagicMock()
        raw_item = self._turtle_item()
        repository.pending_outcome_picks.return_value = [
            {
                "id": 1,
                "code": "sh.600000",
                "signal_trade_date": "2026-07-21",
                "entry_rule": "next_tradable_open",
                "horizons_json": [1, 3, 5, 20],
                "benchmark_codes_json": [],
                "raw_json": raw_item,
                "strategy_id": "a_share_sentiment",
                "observation_id": "obs-late-entry",
                "source_snapshot_id": "snapshot-late-entry",
            }
        ]
        path = []
        for index in range(20):
            triggered = index >= 2
            base = 10.00 + max(index - 2, 0) * 0.04
            path.append(
                {
                    "trade_date": (
                        date(2026, 7, 21) + timedelta(days=index)
                    ).isoformat(),
                    "open": base if triggered else 9.80,
                    "high": base + 0.08 if triggered else 9.90,
                    "low": base - 0.10 if triggered else 9.70,
                    "close": base + 0.04 if triggered else 9.82,
                    "prev_close": base - 0.02,
                    "adj_factor": 1.0,
                }
            )
        repository.load_price_path.return_value = path
        repository.load_benchmark_path.return_value = {}

        result = ForwardObservationService(
            repository=repository,
            trade_plan_events=events,
        ).refresh_outcomes()

        self.assertEqual(result["partial"], 1)
        saved_outcome = repository.update_pick_outcome.call_args.args[1]
        self.assertEqual(saved_outcome["status"], "partial")
        self.assertEqual(
            saved_outcome["trade_plan_evaluation"]["status"],
            "pending",
        )


class ForwardSubmissionTests(unittest.TestCase):
    def test_non_trading_day_is_recorded_as_skip_without_selection_task(self):
        repository = MagicMock()
        repository.latest_data_trade_date.return_value = date(2026, 7, 17)
        selection_service = MagicMock()

        result = submit_observation(
            today=date(2026, 7, 20),
            repository=repository,
            selection_service=selection_service,
            protocol_spec=protocol_spec(),
        )

        self.assertEqual(result["status"], "skipped")
        repository.ensure_protocol.assert_called_once()
        repository.reserve_observation.assert_not_called()
        selection_service.submit.assert_not_called()

    def test_forward_payload_requires_protocol_and_observation_as_pair(self):
        service = SelectionRunService(repository=MagicMock(), job_states=MagicMock())
        with patch("app.stock_selection.run_tasks.StrategyService") as strategy_service:
            strategy_service.return_value.require_runtime_ready.return_value = {"runtime_ready": True}
            with self.assertRaisesRegex(ValueError, "must be supplied together"):
                service.submit(
                    {
                        **BASE_REQUEST,
                        "forward_protocol_id": "protocol-v1",
                    }
                )

    def test_successful_worker_completion_finalizes_forward_observation(self):
        repository = MagicMock()
        repository.finish_success.return_value = True
        service = SelectionRunService(repository=repository, job_states=MagicMock())
        request = SelectionTaskPayload(
            **BASE_REQUEST,
            forward_protocol_id="protocol-v1",
            forward_observation_id="observation-v1",
        )
        forward_repository = MagicMock()

        with patch(
            "app.stock_selection.run_tasks.ForwardObservationRepository",
            return_value=forward_repository,
        ):
            service._finish_success(
                "run-v1",
                "worker-v1",
                {"count": 0, "results": []},
                request,
            )

        forward_repository.finalize_success.assert_called_once_with(
            protocol_id="protocol-v1",
            observation_id="observation-v1",
            selection_run_id="run-v1",
            result={"count": 0, "results": []},
        )


class ForwardEvidenceTests(unittest.TestCase):
    def test_summary_keeps_preliminary_evidence_distinct_from_validation(self):
        repository = MagicMock()
        repository.evidence_rows.return_value = (
            {
                "protocol_id": "protocol-v1",
                "strategy_version": "0.3.1",
                "protocol_version": "prospective_after_close_v1",
                "execution_time": timedelta(hours=16, minutes=20),
                "timezone": "Asia/Shanghai",
                "entry_rule": "next_tradable_open",
                "horizons_json": [1, 3, 5, 20],
                "benchmark_codes_json": ["000300.SH"],
                "minimum_observation_days": 1,
                "minimum_candidate_count": 50,
                "started_on": date(2026, 7, 21),
            },
            [
                {
                    "observation_id": "obs-v1",
                    "signal_trade_date": date(2026, 7, 21),
                    "status": "success",
                    "result_count": 1,
                    "ai_mode": "local_fallback",
                }
            ],
            [
                {
                    "observation_id": "obs-v1",
                    "signal_trade_date": date(2026, 7, 21),
                    "code": "sh.600000",
                    "name": "浦发银行",
                    "return_1d_pct": 2.0,
                    "return_3d_pct": 4.0,
                    "max_favorable_5d_pct": 6.0,
                    "max_adverse_5d_pct": -2.0,
                    "price_adjustment_mode": "adjusted_total_return",
                    "outcome_status": "partial",
                    "outcome_json": {
                        "horizons": {
                            "1": {
                                "benchmark_returns_pct": {"000300.SH": 1.0},
                                "excess_returns_pct": {"000300.SH": 1.0},
                            }
                        }
                    },
                }
            ],
            [
                {
                    "observation_id": "obs-v1",
                    "code": "sh.600000",
                    "action_type": "bought",
                    "action_price": 10.0,
                }
            ],
        )

        summary = ForwardObservationService(repository=repository).evidence_summary("a_share_sentiment")

        self.assertEqual(summary["status"], "preliminary_ready")
        self.assertEqual(summary["validation_status"], "unvalidated")
        self.assertEqual(summary["metrics"]["1"]["average_return_pct"], 2.0)
        self.assertEqual(
            summary["metrics"]["1"]["average_excess_return_pct"]["000300.SH"],
            1.0,
        )
        self.assertEqual(summary["user_discipline"]["bought_pick_count"], 1)
        self.assertEqual(summary["recent_picks"][0]["last_action"], "bought")

    def test_unconfigured_strategy_is_explicitly_unvalidated(self):
        repository = MagicMock()
        repository.evidence_rows.return_value = ({}, [], [], [])

        summary = ForwardObservationService(repository=repository).evidence_summary("test_strategy")

        self.assertEqual(summary["status"], "not_configured")
        self.assertEqual(summary["validation_status"], "unvalidated")

    def test_superseded_protocol_is_retained_but_marked_contaminated(self):
        repository = MagicMock()
        repository.evidence_rows.return_value = (
            {
                "protocol_id": "v050-protocol",
                "strategy_version": "0.5.0",
                "minimum_observation_days": 5,
                "minimum_candidate_count": 15,
            },
            [],
            [],
            [],
        )
        loader = MagicMock()
        loader.get_strategy_meta.return_value = {
            "id": "a_share_sentiment_v05",
            "version": "0.5.1",
            "supersedes": {
                "version": "0.5.0",
                "evidence_status": "contaminated_retained",
                "evidence_note": "mixed-clock fund confirmation",
            },
        }

        summary = ForwardObservationService(
            repository=repository,
            strategy_loader=loader,
        ).evidence_summary("a_share_sentiment_v05")

        self.assertEqual(summary["current_strategy_version"], "0.5.1")
        self.assertEqual(summary["protocol"]["strategy_version"], "0.5.0")
        self.assertEqual(summary["evidence_status"], "contaminated_retained")
        self.assertEqual(summary["evidence_note"], "mixed-clock fund confirmation")


if __name__ == "__main__":
    unittest.main()
