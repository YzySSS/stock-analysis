from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import MagicMock, patch

from app.api.routes import dashboard, system, etf_rotation
from app.etf_rotation import display


class DashboardSectionTests(unittest.TestCase):
    def test_summary_cold_skips_all_hidden_builders(self):
        tracker = MagicMock()
        tracker.build_latest_selection_snapshot.return_value = []
        tracker.to_dict_list.return_value = []
        with patch('app.api.routes.dashboard.SelectionResultTracker', return_value=tracker), \
             patch.object(dashboard, '_dashboard_market_overview', return_value={'risk_alert': {'level': 'red'}}), \
             patch.object(dashboard, '_dashboard_leadership', return_value={'model_id': 'm'}), \
             patch.object(dashboard, 'build_market_timing_signal') as timing, \
             patch.object(dashboard, '_dashboard_hot_themes') as themes, \
             patch.object(dashboard, '_dashboard_emotion_board') as emotion, \
             patch('app.display_cache.service.entry', return_value=None):
            result = dashboard.dashboard_summary(limit=8, compact=True, section='summary')
        self.assertEqual('red', result['market_overview']['risk_alert']['level'])
        self.assertNotIn('market_timing', result)
        self.assertEqual({'model_id': 'm'}, result['market_leadership'])
        timing.assert_not_called(); themes.assert_not_called(); emotion.assert_not_called()

    def test_cached_research_sections_are_exact_and_do_not_mutate_cache(self):
        payload = {'market_timing': {'state': 'cautious', 'shadow_v20': {'x': [1]}},
                   'hot_themes': {'items': [1]}, 'emotion_board': {'x': [2]}, 'latest_tracking_count': 3}
        original = copy.deepcopy(payload)
        with patch('app.display_cache.service.entry', return_value={'payload': payload}):
            self.assertEqual({'market_timing': payload['market_timing']}, dashboard.dashboard_summary(limit=8, section='timing'))
            self.assertEqual({'hot_themes': payload['hot_themes'], 'emotion_board': payload['emotion_board']}, dashboard.dashboard_summary(limit=8, section='emotion'))
        self.assertEqual(original, payload)

    def test_visible_leadership_read_omits_hidden_forecast_json_without_changing_mainline(self):
        from app.market_timing.scenario_forecast import MarketScenarioForecastRepository
        context = MagicMock(); cursor = MagicMock()
        context.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.side_effect = [[{'trade_date': '2026-11-13'}], [{
            'sector_name': 'x', 'sector_type': 'industry', 'leadership_state': 'confirmed',
            'cycle_state': 'main_up', 'confidence': 0.9, 'leadership_score': 90,
            'price_evidence_status': 'ready', 'breadth_metrics_json': '{"status":"ready"}',
            'data_quality_json': '{}'}]]
        cursor.fetchone.return_value = {'trade_date': '2026-11-13'}
        result = MarketScenarioForecastRepository(read_connection_factory=lambda: context).latest(include_forecasts=False)
        self.assertEqual([], result['forecasts'])
        self.assertEqual('present', result['market_mainline']['status'])
        sql = cursor.execute.call_args_list[0].args[0]
        self.assertNotIn('SELECT *', sql)
        self.assertNotIn('probabilities_json', sql)
        self.assertNotIn('feature_json', sql)
        leadership_sql = cursor.execute.call_args_list[2].args[0]
        self.assertNotIn('SELECT *', leadership_sql)
        self.assertIn('data_quality_json', leadership_sql)
        self.assertIn('LIMIT 24', leadership_sql)


class SystemSummaryTests(unittest.TestCase):
    def test_all_task_cards_and_actionable_dq_samples_preserved(self):
        samples = [{'code': str(i), 'classification': 'actionable_missing'} for i in range(8)]
        source = {'task_runs': [{'task_name': str(i), 'status': 'failed', 'message': 'err',
            'metadata': {'rows': i, 'source_errors': {'provider': 'failed'}, 'large_hidden_trace': 'x'*10000}} for i in range(43)],
            'data_quality': {'counts': {'pass': 12, 'warn': 1, 'fail': 0}, 'status': 'warn',
            'checks': [{'status': 'pass', 'samples': ['hidden']}, {'status': 'warn', 'label': 'gap',
                'message': 'missing', 'samples': [{'classification': 'explained'}]+samples}]}}
        original = copy.deepcopy(source)
        result = system._compact_system_payload(source)
        self.assertEqual(43, len(result['task_runs']))
        self.assertEqual(source['data_quality']['counts'], result['data_quality']['counts'])
        self.assertEqual(samples[:5], result['data_quality']['checks'][0]['samples'])
        self.assertEqual({'rows': 0, 'source_errors': {'provider': 'failed'}}, result['task_runs'][0]['metadata'])
        self.assertEqual(original, source)

    def test_cold_task_query_projects_json_in_sql_and_keeps_stale_gate(self):
        conn_context = MagicMock(); cursor = MagicMock()
        conn_context.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.return_value = [{'task_name': 'stock_popularity_update', 'status': 'running',
            'running_age_seconds': system.TASK_RUNNING_STALE_SECONDS+1, 'metadata_json': '{"rows":7}'}]
        with patch.object(system, 'mysql_read_conn', return_value=conn_context):
            items = system._latest_task_runs(compact=True)
        sql = cursor.execute.call_args.args[0]
        self.assertIn('JSON_OBJECT(', sql)
        self.assertIn('MAX(id)', sql)
        self.assertIn('JSON_VALID', sql)
        self.assertTrue(items[0]['stale'])
        self.assertEqual(7, items[0]['metadata']['rows'])


class EtfDisplayTests(unittest.TestCase):
    def cursor_context(self, rows):
        context = MagicMock(); cursor = MagicMock()
        context.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.return_value = rows
        return context, cursor

    def test_compact_candidate_query_is_run_bounded_and_does_not_read_full_evidence(self):
        context, cursor = self.cursor_context([{'sector_name': 'x', 'is_selected': 0, 'is_eligible': 1,
            'gate_json': '{"source_ready":true}', 'evidence_json': '{"sector_cycle_label":"主升"}'}])
        with patch.object(display, 'mysql_read_conn', return_value=context):
            result = display.candidates('run-one')
        sql, params = cursor.execute.call_args.args
        self.assertNotIn('SELECT *', sql)
        self.assertIn("JSON_EXTRACT(evidence_json, '$.sector_cycle_label')", sql)
        self.assertIn('WHERE run_id=%s ORDER BY rank_no', sql)
        self.assertNotIn('average_amount_20d_yuan', sql)
        self.assertEqual(('run-one',), params)
        self.assertTrue(result[0]['is_eligible']); self.assertFalse(result[0]['is_selected'])
        self.assertEqual('主升', result[0]['evidence']['sector_cycle_label'])
        with patch.object(display, 'mysql_read_conn', return_value=context):
            display.candidates('run-one', details=True)
        self.assertIn('average_amount_20d_yuan', cursor.execute.call_args.args[0])

    def test_outcomes_uses_same_run_without_large_metadata(self):
        context, cursor = self.cursor_context([])
        with patch.object(display, 'mysql_read_conn', return_value=context):
            self.assertEqual([], display.outcomes('run-one'))
        sql, params = cursor.execute.call_args.args
        self.assertEqual(('run-one',), params)
        self.assertNotIn('metadata_json', sql)
        self.assertIn('WHERE run_id=%s', sql)

    def test_spec_hash_mismatch_does_not_silently_mix_releases(self):
        with self.assertRaises(Exception) as caught:
            etf_rotation.etf_rotation_spec(spec_hash='outdated')
        self.assertEqual(409, caught.exception.status_code)


if __name__ == '__main__':
    unittest.main()
