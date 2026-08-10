from __future__ import annotations

import subprocess
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.automatic_observation import (
    AUTOMATIC_OBSERVATION_SOURCE,
    AutomaticObservationCampaignService,
    AutomaticObservationPolicy,
    _time_text,
    discover_automatic_observation_policies,
)
from scripts.run_automatic_strategy_observation import (
    main as run_observation_main,
    verify_deployed_strategy_tags,
)


class AutomaticObservationPolicyTests(unittest.TestCase):
    def test_registry_enrolls_v051_in_thirty_day_opening_observation(self):
        policies = discover_automatic_observation_policies(
            StrategyLoader(),
            today=date(2026, 8, 10),
        )

        self.assertEqual(len(policies), 1)
        policy = policies[0]
        self.assertEqual(policy.baseline_strategy_id, "a_share_sentiment")
        self.assertEqual(policy.baseline_strategy_version, "0.4.4")
        self.assertEqual(policy.candidate_strategy_id, "a_share_sentiment_v05")
        self.assertEqual(policy.candidate_strategy_version, "0.5.1")
        self.assertEqual(policy.start_on, date(2026, 8, 11))
        self.assertEqual(policy.target_trade_days, 30)
        self.assertEqual(policy.execution_time, "09:25:00")
        self.assertEqual(policy.entry_rule, "same_day_open")
        self.assertEqual(policy.max_picks, 3)

    def test_new_shadow_strategy_cannot_opt_out_of_standard_observation(self):
        loader = MagicMock()
        loader.registry = {
            "automatic_observation_defaults": {
                "enabled": True,
                "mode": "paired_first_n_trade_days",
                "baseline_strategy_id": "baseline",
                "target_trade_days": 5,
                "execution_time": "09:25:00",
                "entry_rule": "same_day_open",
                "engine": "sentiment_snapshot_pair",
            },
            "strategies": [
                {
                    "id": "baseline",
                    "version": "1.0.0",
                    "immutable_tag": "baseline-v1",
                    "mode": "frozen_baseline",
                },
                {
                    "id": "new_strategy",
                    "version": "0.1.0",
                    "immutable_tag": "new-strategy-v0.1.0",
                    "mode": "shadow_only",
                    "automatic_observation": {"enabled": False},
                },
            ],
        }
        loader.get_strategy_meta.return_value = loader.registry["strategies"][0]

        with self.assertRaisesRegex(ValueError, "cannot opt out"):
            discover_automatic_observation_policies(
                loader,
                today=date(2026, 8, 1),
            )

    def test_database_time_values_are_normalized_before_campaign_drift_check(self):
        self.assertEqual(
            _time_text(timedelta(hours=9, minutes=25)),
            "09:25:00",
        )

    def test_shadow_strategy_cannot_shorten_standard_observation(self):
        loader = MagicMock()
        loader.registry = {
            "automatic_observation_defaults": {
                "enabled": True,
                "mode": "paired_first_n_trade_days",
                "baseline_strategy_id": "baseline",
                "target_trade_days": 5,
                "execution_time": "09:25:00",
                "entry_rule": "same_day_open",
                "engine": "sentiment_snapshot_pair",
            },
            "strategies": [
                {
                    "id": "baseline",
                    "version": "1.0.0",
                    "immutable_tag": "baseline-v1",
                    "mode": "frozen_baseline",
                },
                {
                    "id": "new_strategy",
                    "version": "0.1.0",
                    "immutable_tag": "new-strategy-v0.1.0",
                    "mode": "shadow_only",
                    "automatic_observation": {"target_trade_days": 4},
                },
            ],
        }
        loader.get_strategy_meta.return_value = loader.registry["strategies"][0]

        with self.assertRaisesRegex(ValueError, "at least the standard 5-day"):
            discover_automatic_observation_policies(
                loader,
                today=date(2026, 8, 10),
            )


class AutomaticObservationServiceTests(unittest.TestCase):
    def test_pair_is_persisted_only_through_forward_observation_repository(self):
        policy = _policy()
        loader = MagicMock()
        loader.get_strategy_meta.side_effect = lambda strategy_id: {
            "id": strategy_id,
            "version": (
                policy.baseline_strategy_version
                if strategy_id == policy.baseline_strategy_id
                else policy.candidate_strategy_version
            ),
        }
        loader.load_config.return_value = {
            "selection": {"score_threshold": 60.0}
        }
        campaigns = MagicMock()
        campaigns.ensure_campaign.return_value = _campaign(policy)
        campaigns.refresh_progress.side_effect = [
            _campaign(policy),
            _campaign(policy, completed_trade_days=1),
        ]
        forward = MagicMock()
        forward.finalize_paired_success.return_value = [
            {
                "observation_id": "baseline-observation",
                "protocol_id": "baseline-protocol",
                "source_snapshot_id": "baseline-snapshot",
                "result_count": 1,
                "status": "success",
            },
            {
                "observation_id": "candidate-observation",
                "protocol_id": "candidate-protocol",
                "source_snapshot_id": "candidate-snapshot",
                "result_count": 1,
                "status": "success",
            },
        ]
        materializer = MagicMock()
        materializer.materialize_pair.return_value = {
            "status": "success",
            "dual_input_hash": "a" * 64,
            "runs": {
                policy.baseline_strategy_id: {
                    "snapshot_id": "baseline-snapshot",
                    "snapshot_status": "ready",
                    "quality_status": "passed",
                },
                policy.candidate_strategy_id: {
                    "snapshot_id": "candidate-snapshot",
                    "snapshot_status": "ready",
                    "quality_status": "passed",
                },
            },
        }
        strategy_service = MagicMock()
        strategy_service.published_sentiment_observation_result.side_effect = (
            lambda **kwargs: {
                "count": 1,
                "results": [
                    {
                        "code": (
                            "sh.600000"
                            if kwargs["strategy_id"] == policy.baseline_strategy_id
                            else "sz.000001"
                        )
                    }
                ],
                "input_snapshot_id": kwargs["snapshot_id"],
                "ai_mode": "local_core",
            }
        )
        service = AutomaticObservationCampaignService(
            loader=loader,
            campaign_repository=campaigns,
            forward_repository=forward,
            materializer=materializer,
            strategy_service=strategy_service,
        )

        result = service._run_policy(
            policy,
            today=date(2026, 8, 11),
            implementation_commit="b" * 40,
        )

        self.assertEqual(result["status"], "recorded")
        self.assertFalse(result["selection_result_written"])
        materializer.materialize_pair.assert_called_once_with(
            baseline_strategy_id=policy.baseline_strategy_id,
            candidate_strategy_id=policy.candidate_strategy_id,
            max_picks=3,
        )
        self.assertEqual(forward.ensure_protocol.call_count, 2)
        protocol_specs = [
            call.args[0] for call in forward.ensure_protocol.call_args_list
        ]
        self.assertEqual(
            {
                spec.strategy_snapshot["methodology"]["sample_policy"]
                for spec in protocol_specs
            },
            {"retain_exactly_30_successful_paired_trade_days_including_zero_pick_days"},
        )
        records = forward.finalize_paired_success.call_args.args[0]
        self.assertEqual(len(records), 2)
        self.assertEqual(
            {record["observation_source"] for record in records},
            {AUTOMATIC_OBSERVATION_SOURCE},
        )
        self.assertEqual(
            {record["paired_input_hash"] for record in records},
            {"a" * 64},
        )
        self.assertTrue(
            all(
                record["result"]["automatic_observation"][
                    "writes_selection_result"
                ]
                is False
                for record in records
            )
        )

    def test_campaign_before_start_does_not_materialize_candidates(self):
        policy = _policy(start_on=date(2026, 8, 11))
        campaigns = MagicMock()
        campaigns.ensure_campaign.return_value = _campaign(policy)
        campaigns.refresh_progress.return_value = _campaign(policy)
        materializer = MagicMock()
        service = AutomaticObservationCampaignService(
            loader=MagicMock(),
            campaign_repository=campaigns,
            forward_repository=MagicMock(),
            materializer=materializer,
            strategy_service=MagicMock(),
        )

        result = service._run_policy(
            policy,
            today=date(2026, 8, 10),
            implementation_commit="b" * 40,
        )

        self.assertEqual(result["status"], "scheduled")
        materializer.materialize_pair.assert_not_called()

    def test_completed_campaign_does_not_require_quote_snapshot(self):
        policy = _policy()
        campaigns = MagicMock()
        campaigns.campaign_state.return_value = _campaign(
            policy,
            completed_trade_days=30,
            status="completed",
        )
        service = AutomaticObservationCampaignService(
            loader=MagicMock(),
            campaign_repository=campaigns,
            forward_repository=MagicMock(),
            materializer=MagicMock(),
            strategy_service=MagicMock(),
        )

        pending = service.policies_requiring_snapshot(
            today=date(2026, 9, 22),
            policies=[policy],
        )

        self.assertEqual(pending, [])
        campaigns.campaign_state.assert_called_once_with(policy.campaign_id)


class AutomaticObservationDeploymentTests(unittest.TestCase):
    def test_deployment_verification_requires_both_immutable_tags(self):
        service = MagicMock()
        service.policies.return_value = [_policy()]

        def git_output(*args):
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return "b" * 40
            if args[:3] == ("rev-list", "-n", "1"):
                return "a" * 40
            if args[:2] == ("merge-base", "--is-ancestor"):
                return ""
            raise AssertionError(args)

        with patch(
            "scripts.run_automatic_strategy_observation._git_output",
            side_effect=git_output,
        ):
            result = verify_deployed_strategy_tags(
                service,
                today=date(2026, 8, 11),
            )

        self.assertEqual(
            set(result["tags"]),
            {"a-share-sentiment-v0.4.4", "a-share-sentiment-v0.5.1"},
        )

    def test_deployment_verification_rejects_non_ancestor_tag(self):
        service = MagicMock()
        service.policies.return_value = [_policy()]

        def git_output(*args):
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return "b" * 40
            if args[:3] == ("rev-list", "-n", "1"):
                return "a" * 40
            if args[:2] == ("merge-base", "--is-ancestor"):
                raise subprocess.CalledProcessError(1, args)
            raise AssertionError(args)

        with patch(
            "scripts.run_automatic_strategy_observation._git_output",
            side_effect=git_output,
        ):
            with self.assertRaisesRegex(RuntimeError, "not an ancestor"):
                verify_deployed_strategy_tags(
                    service,
                    today=date(2026, 8, 11),
                )


class AutomaticObservationExecutionTests(unittest.TestCase):
    def test_completed_campaign_skips_quote_wait_in_runner(self):
        policy = _policy()
        service = MagicMock()
        service.policies_requiring_snapshot.return_value = []
        service.run.return_value = {
            "status": "success",
            "campaigns": [{"status": "completed"}],
        }
        deployment = {
            "head": "b" * 40,
            "policies": [policy],
            "tags": {
                policy.baseline_immutable_tag: "a" * 40,
                policy.candidate_immutable_tag: "b" * 40,
            },
        }

        with (
            patch(
                "scripts.run_automatic_strategy_observation.AutomaticObservationCampaignService",
                return_value=service,
            ),
            patch(
                "scripts.run_automatic_strategy_observation.verify_deployed_strategy_tags",
                return_value=deployment,
            ),
            patch(
                "scripts.run_automatic_strategy_observation.acquire_mysql_advisory_lock",
                return_value=object(),
            ),
            patch(
                "scripts.run_automatic_strategy_observation.release_mysql_advisory_lock",
                return_value=None,
            ),
            patch("scripts.run_automatic_strategy_observation.TaskRunLogger"),
            patch(
                "scripts.run_automatic_strategy_observation.wait_for_call_auction_snapshot"
            ) as wait_for_snapshot,
            patch("builtins.print"),
        ):
            result = run_observation_main(
                ["--date", "2026-08-11", "--wait-seconds", "0"]
            )

        self.assertEqual(result, 0)
        wait_for_snapshot.assert_not_called()
        service.run.assert_called_once_with(
            today=date(2026, 8, 11),
            implementation_commit="b" * 40,
        )


class AutomaticObservationFrontendTests(unittest.TestCase):
    def test_strategy_page_exposes_source_and_same_day_entry_labels(self):
        source = Path("app/api/web/js/strategies.js").read_text(
            encoding="utf-8"
        )
        page = Path("app/api/web/pages/strategies.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("自动 ${observationTargetDays || '-'} 日配对观察", source)
        self.assertIn("历史证据已污染 · 禁止比较", source)
        self.assertIn("当日开盘（09:25 信号）", source)
        self.assertIn("与用户手动选股及其 14 天统计完全分开", source)
        self.assertIn("strategies.js?v=20260810v051", page)


def _policy(
    *,
    start_on: date = date(2026, 8, 11),
) -> AutomaticObservationPolicy:
    return AutomaticObservationPolicy(
        baseline_strategy_id="a_share_sentiment",
        candidate_strategy_id="a_share_sentiment_v05",
        baseline_strategy_version="0.4.4",
        candidate_strategy_version="0.5.1",
        baseline_immutable_tag="a-share-sentiment-v0.4.4",
        candidate_immutable_tag="a-share-sentiment-v0.5.1",
        start_on=start_on,
        target_trade_days=30,
        execution_time="09:25:00",
        timezone="Asia/Shanghai",
        entry_rule="same_day_open",
        horizons=(1, 3, 5, 20),
        benchmark_codes=("000300.SH", "000905.SH", "000852.SH"),
        max_picks=3,
        minimum_realtime_coverage=0.90,
        engine="sentiment_snapshot_pair",
    )


def _campaign(
    policy: AutomaticObservationPolicy,
    *,
    completed_trade_days: int = 0,
    status: str = "active",
) -> dict:
    return {
        "campaign_id": policy.campaign_id,
        "baseline_strategy_id": policy.baseline_strategy_id,
        "baseline_strategy_version": policy.baseline_strategy_version,
        "candidate_strategy_id": policy.candidate_strategy_id,
        "candidate_strategy_version": policy.candidate_strategy_version,
        "status": status,
        "completed_trade_days": completed_trade_days,
        "target_trade_days": policy.target_trade_days,
        "metadata_json": {"implementation_commit": "b" * 40},
        "last_signal_trade_date": None,
    }


if __name__ == "__main__":
    unittest.main()
