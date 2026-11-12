from __future__ import annotations

import json
import unittest
from dataclasses import replace
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from app.api.auth import SiteAuthenticator
from app.stock_selection.automatic_observation import (
    AutomaticObservationCampaignService, AutomaticObservationCampaignRepository,
    discover_automatic_observation_policies, observation_window,
)
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.factor_evaluation_v2 import StrategyFactorEvaluationRepository
from app.stock_selection.forward_observation import ForwardProtocolDriftError
from app.stock_selection.research_review import summarize_cohort, build_periodic_review
from tests.test_automatic_strategy_observation import _policy, _campaign
from tests.test_site_auth import build_settings


def connection_with_results(results):
    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchall.side_effect = results
    return connection, cursor


class ClockTests(unittest.TestCase):
    def test_window_excludes_late_early_and_wrong_day(self):
        for time, expected in [("09:24:59", False), ("09:25:00", True),
                               ("09:28:59", True), ("09:29:00", False), ("14:30:00", False)]:
            self.assertEqual(observation_window(date(2026, 11, 13), datetime.fromisoformat('2026-11-13T' + time)), expected)
        self.assertFalse(observation_window(date(2026, 11, 12), datetime(2026, 11, 13, 9, 26)))

    def test_late_direct_service_call_cannot_materialize(self):
        campaigns = MagicMock()
        campaigns.campaign_state.return_value = {}
        materializer = MagicMock()
        service = AutomaticObservationCampaignService(campaign_repository=campaigns,
            materializer=materializer, clock=lambda: datetime(2026, 11, 13, 14, 30))
        result = service._run_policy(_policy(), today=date(2026, 11, 13), implementation_commit='a' * 40)
        self.assertEqual(result['status'], 'missed_window')
        materializer.materialize_pair.assert_not_called()
        campaigns.ensure_campaign.assert_not_called()

    def test_readiness_counts_only_window_quotes(self):
        conn, cursor = connection_with_results([])
        cursor.fetchone.side_effect = [{'expected_entities': 100},
            {'actual_entities': 0, 'latest_quote_time': datetime(2026, 11, 13, 14, 30)}]
        result = AutomaticObservationCampaignRepository(connection_factory=lambda **kw: conn).quote_readiness(
            signal_date=date(2026, 11, 13), execution_time='09:25:00', minimum_coverage=.9)
        self.assertFalse(result['ready'])
        query, params = cursor.execute.call_args_list[1].args
        self.assertIn('realtime.quote_time < %s', query)
        self.assertEqual(params[-1], '2026-11-13 09:29:00')

    def test_continuous_campaign_has_new_identity_not_old_thirty_day_experiment(self):
        before = discover_automatic_observation_policies(StrategyLoader(), today=date(2026, 11, 12))[0]
        after = discover_automatic_observation_policies(StrategyLoader(), today=date(2026, 11, 13))[0]
        self.assertEqual(before.target_trade_days, 30)
        self.assertEqual(after.target_trade_days, 0)
        self.assertNotEqual(before.campaign_id, after.campaign_id)
        self.assertEqual(after.candidate_strategy_version, before.candidate_strategy_version)


class AdjustmentTests(unittest.TestCase):
    def build(self, factors, *, prior=None):
        days = [date(2026, 11, d) for d in [9, 10, 11]]
        bars = [dict(code='sh.600000', trade_date=d, open=10, high=10, low=10, close=10, adj_factor=f)
                for d, f in zip(days, factors)]
        conn, cursor = connection_with_results([bars, [], [prior] if prior else []])
        repo = StrategyFactorEvaluationRepository(read_connection_factory=lambda: conn)
        rows = repo._build_outcomes_for_horizon(factor_rows=[dict(id=1, snapshot_id='s',
                strategy_id='x', strategy_version='1', code='sh.600000')],
            signal_date=date(2026, 11, 6), entry_date=days[0], exit_date=days[-1], horizon_days=3)
        return rows[0]

    def test_missing_middle_factor_never_produces_false_minus_fifty_percent(self):
        row = self.build([2, None, 2])
        self.assertAlmostEqual(row[11], 0)
        self.assertIsNone(row[17])
        self.assertIsNone(row[18])
        self.assertEqual(row[20], 'incomplete_adjustment_path')
        self.assertFalse(json.loads(row[22])['optimization_eligible'])

    def test_missing_endpoint_does_not_fall_back_to_raw_price(self):
        row = self.build([None, 2, 2])
        self.assertIsNone(row[11])
        self.assertEqual(row[20], 'missing_adjustment_factor')

    def test_complete_flat_path_has_zero_excursions(self):
        row = self.build([2, 2, 2])
        self.assertEqual(row[19], 'ready')
        self.assertEqual(row[17:19], (0, 0))
        self.assertTrue(json.loads(row[22])['optimization_eligible'])

    def test_retry_restores_label_and_keeps_prior_evidence(self):
        row = self.build([2, 2, 2], prior=dict(factor_snapshot_id=1,
            metadata_json={'attempt_count': 2}, label_hash='old',
            block_reason='missing_adjustment_factor', computed_at='2026-11-11'))
        metadata = json.loads(row[22])
        self.assertEqual(row[19], 'ready')
        self.assertEqual(metadata['attempt_count'], 3)
        self.assertEqual(metadata['retry_history'][0]['label_hash'], 'old')

    def test_retry_sql_selects_recoverable_rows_but_not_ready_labels(self):
        conn, cursor = connection_with_results([
            [{'trade_date': date(2026, 11, 9)}, {'trade_date': date(2026, 11, 10)}], []])
        repo = StrategyFactorEvaluationRepository(read_connection_factory=lambda: conn)
        result = repo.refresh_outcomes(horizons=[1])
        self.assertEqual(result['manifest_count'], 0)
        sql = cursor.execute.call_args_list[-1].args[0]
        self.assertIn("mo.execution_status='blocked'", sql)
        self.assertIn('INTERVAL 1 DAY', sql)
        self.assertIn('< 5', sql)


class IdentityAndAuthenticationTests(unittest.TestCase):
    def test_campaign_rejects_different_implementation_identity(self):
        conn, cursor = connection_with_results([])
        policy = _policy()
        row = _campaign(policy)
        row.update(observation_source='automatic_observation', execution_time=policy.execution_time,
            timezone=policy.timezone, entry_rule=policy.entry_rule, started_on=policy.start_on,
            protocol_ids_json={'baseline': 'b', 'candidate': 'c'},
            metadata_json={'implementation_identity': {'source_sha256': 'old'}, 'clock_contract': 'opening_window_v2'})
        cursor.fetchone.return_value = row
        repo = AutomaticObservationCampaignRepository(connection_factory=lambda **kw: conn)
        with patch('app.stock_selection.automatic_observation.implementation_identity', return_value={'source_sha256': 'new'}):
            with self.assertRaisesRegex(ForwardProtocolDriftError, 'drift'):
                repo.ensure_campaign(policy, implementation_commit='a' * 40, protocol_ids={'baseline': 'b', 'candidate': 'c'})

    def test_password_change_invalidates_token_with_same_secret(self):
        settings = build_settings()
        token, _ = SiteAuthenticator(settings).issue_session()
        changed = replace(settings, password_hash=build_settings(password='a different password').password_hash)
        self.assertIsNone(SiteAuthenticator(changed).read_session(token))

    def test_epoch_change_invalidates_token_across_authenticator_instances(self):
        settings = build_settings()
        token, _ = SiteAuthenticator(settings).issue_session()
        self.assertIsNotNone(SiteAuthenticator(settings).read_session(token))
        self.assertIsNone(SiteAuthenticator(replace(settings, session_epoch='2')).read_session(token))


class ReviewTests(unittest.TestCase):
    def test_missed_and_empty_days_are_not_pooled_and_no_automatic_promotion(self):
        day1, day2 = date(2026, 11, 12), date(2026, 11, 13)
        campaign = {'campaign_id': 'new', 'protocol_ids_json': {'baseline': 'b', 'candidate': 'c'}}
        result = summarize_cohort(campaign=campaign, expected_dates=[day1, day2], picks=[],
            observations=[dict(protocol_id=p, signal_trade_date=day1, status='success', result_count=0) for p in ['b', 'c']])
        self.assertEqual(result['roles']['baseline']['empty_days'], 1)
        self.assertEqual(result['roles']['baseline']['missed_dates'], [str(day2)])
        self.assertEqual(result['paired_days'], 1)
        self.assertFalse(result['promotion_allowed'])
        self.assertFalse(result['automatic_parameter_change'])

    def test_missing_calendar_fails_closed(self):
        conn, cursor = connection_with_results([[], []])
        with self.assertRaisesRegex(RuntimeError, 'calendar'):
            build_periodic_review(start=date(2026, 11, 12), end=date(2026, 11, 13), connection_factory=lambda: conn)

class EvaluationContractTests(unittest.TestCase):
    def test_new_evaluations_cannot_overwrite_old_evaluation_ids(self):
        from app.stock_selection.factor_evaluation_v2 import evaluate_factor_records, version_adjusted_path_evaluation
        result = evaluate_factor_records([], strategy_id='x', strategy_version='1',
            scope_name='eligible_pool', horizon_days=5, factor_keys=['x'])
        old = result['evaluations'][0]['evaluation_id']
        old_hash = result['evaluations'][0]['spec_hash']
        version_adjusted_path_evaluation(result)
        new = result['evaluations'][0]
        self.assertNotEqual(old, new['evaluation_id'])
        self.assertNotEqual(old_hash, new['spec_hash'])
        self.assertLessEqual(len(new['evaluation_id']), 128)
        self.assertEqual(new['protocol_id'], 'strategy_factor_evaluation_adjusted_path_v3')

    def test_continuous_protocol_has_real_minimum_evidence_thresholds(self):
        service = AutomaticObservationCampaignService()
        policy = discover_automatic_observation_policies(StrategyLoader(), today=date(2026,11,13))[0]
        spec = service._protocol_spec(policy, role='candidate', strategy_id=policy.candidate_strategy_id,
            strategy_version=policy.candidate_strategy_version, immutable_tag=policy.candidate_immutable_tag,
            implementation_commit='a' * 40, protocol_id='new')
        self.assertTrue(spec.request['continuous'])
        self.assertEqual(spec.minimum_observation_days, 30)
        self.assertEqual(spec.minimum_candidate_count, 90)

    def test_total_absence_of_cohort_is_reported_as_missing_not_zero_pick(self):
        day = date(2026,11,13)
        conn, cursor = connection_with_results([[], [{'cal_date': day, 'is_open': 1}], [], []])
        result = build_periodic_review(start=day,end=day,connection_factory=lambda:conn)
        baseline = result['cohorts'][0]['roles']['baseline']
        self.assertEqual(baseline['missed_dates'], [str(day)])
        self.assertEqual(baseline['empty_days'], 0)


if __name__ == '__main__':
    unittest.main()
