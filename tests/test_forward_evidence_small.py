from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from app.display_reads import forward_evidence as display
from app.stock_selection.forward_observation import ForwardObservationService


class SmallProtocolTests(unittest.TestCase):
    def test_every_exact_recent_boundary_retains_global_aggregate_path(self):
        for n, p, a in [(20, 1, 0), (1, 50, 0), (1, 1, 100), (21, 0, 0)]:
            with self.subTest(counts=(n, p, a)):
                cursor = SimpleNamespace(execute=lambda *_: self.fail('boundary must not issue small read'))
                self.assertIsNone(display._small_protocol_summary(
                    cursor, {}, [{}] * n, [{}] * p, {i: {} for i in range(a)}, 'unknown',
                ))

    def test_small_summary_uses_entire_evidence_including_orphan_actions(self):
        protocol = {'protocol_id': 'tiny', 'minimum_observation_days': 30,
                    'minimum_candidate_count': 90, 'strategy_version': '0.5.0'}
        observations = [{'observation_id': 'o', 'status': 'success', 'result_count': 2, 'ai_mode': 'off'},
                        {'observation_id': 'failed', 'status': 'failed', 'result_count': 0}]
        picks = [{'id': i, 'observation_id': 'o', 'code': f'code{i}', 'signal_trade_date': date(2026, 1, 1),
                  'return_1d_pct': v, 'outcome_status': 'complete'} for i, v in [(1, .0001), (2, .0002)]]
        details = [{'id': i, 'price_adjustment_mode': 'adjusted_total_return', 'max_favorable_5d_pct': i,
                    'outcome_json': {'horizons': {'1': {'benchmark_returns_pct': {'x.a"b': ' 1.25 ', 'bool': True,
                    'invalid': 'not-a-number'}, 'excess_returns_pct': {'x': .0001}}}}} for i in [1, 2]]
        actions = {i: {'id': i, 'observation_id': 'o', 'code': code, 'action_type': kind,
                       'action_at': '2026-01-01'}
                   for i, code, kind in [(1, 'code1', 'bought'), (2, 'code1', 'bought'), (3, 'orphan', 'saved')]}
        queries = []
        cursor = SimpleNamespace(execute=lambda sql, params: queries.append((sql, params)), fetchall=lambda: details)
        expected_picks = [{**p, **d} for p, d in zip(picks, details)]
        repository = SimpleNamespace(evidence_rows=lambda _: (protocol, observations, expected_picks, list(actions.values())))
        old = ForwardObservationService(repository=repository).evidence_summary('a_share_sentiment_v05')
        new = display._small_protocol_summary(cursor, protocol, observations, picks, actions, 'a_share_sentiment_v05')
        self.assertEqual(new, old)
        self.assertEqual(new['user_discipline']['bought_pick_count'], 1)
        self.assertEqual(new['actions']['bought'], 2)
        self.assertEqual(len(queries), 1)
        self.assertEqual(queries[0][1], ['tiny', 1, 2])
        self.assertIn('AND id IN (%s,%s)', queries[0][0])
        self.assertNotIn('SELECT *', queries[0][0])
        self.assertNotIn('raw_json', queries[0][0])
        self.assertIn('JSON_EXTRACT(outcome_json', queries[0][0])
        self.assertNotIn(' AS outcome_json', queries[0][0].split('JSON_OBJECT')[0])

    def test_zero_pick_and_zero_observation_protocol_needs_no_additional_sql(self):
        protocol = {'protocol_id': 'zero', 'minimum_observation_days': 30, 'minimum_candidate_count': 90}
        cursor = SimpleNamespace(execute=lambda *_: self.fail('empty picks must not query details'))
        result = display._small_protocol_summary(cursor, protocol, [], [], {}, 'unknown')
        self.assertEqual(result['sample']['candidate_count'], 0)
        self.assertEqual(result['sample']['observation_days'], 0)
        self.assertIsNone(result['metrics']['1']['average_return_pct'])
        self.assertEqual(result['status'], 'collecting')


if __name__ == '__main__':
    unittest.main()
