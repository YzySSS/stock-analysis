import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from app.backtest.sentiment_evidence import EvidenceArchive
from app.backtest.sentiment_replay import digest, replay_bundle


def fixture():
    source = dict(id=1, batch_id='b', source_event_time_max='2026-11-02 09:00:00',
                  received_at='2026-11-02 09:00:01', status='published', quality_status='passed',
                  metadata_json={'required': True})
    candidate = dict(snapshot_id='s', code='sh.600000', name='浦发', row_hash='legacy',
                     is_selected=1, rank_no=1, trade_grade_state='watch',
                     source_lineage_json=[dict(source_manifest_id=1, batch_id='b',
                         source_time=source['source_event_time_max'], received_at=source['received_at'])])
    manifest = dict(snapshot_id='s', strategy_id='a_share_sentiment_v06', strategy_version='0.6.0',
                    status='ready', quality_status='passed', coverage_ratio=.99,
                    decision_as_of='2026-11-02 09:32:00', published_at='2026-11-02 09:32:10',
                    strategy_config_hash='config', implementation_hash='impl', candidate_count=1,
                    source_manifest_ids_json=[1])
    bundle = dict(manifest=manifest,candidates=[candidate],sources=[source])
    start=date(2026,11,3)
    calendar=[dict(cal_date=start+timedelta(days=i),is_open=int((start+timedelta(days=i)).weekday()<5),
                   source='tushare.trade_cal') for i in range(16)]
    opens=[r['cal_date'] for r in calendar if r['is_open']][:5]
    bars=[dict(code='sh.600000',trade_date=day,open=10,high=11,low=9,close=10.5,
               adj_factor=1,prev_close=10,pit_status_available=1,volume=100,name='浦发') for day in opens]
    return bundle,calendar,bars


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.bundle,self.calendar,self.bars=fixture()

    def run_replay(self,**kwargs):
        return replay_bundle(self.bundle,calendar=self.calendar,bars=self.bars,
                             observed_through=datetime(2026,11,16,20),**kwargs)

    def test_original_watch_is_not_upgraded_or_treated_as_execution(self):
        r=self.run_replay()
        self.assertEqual(r['labels'][0]['original_trade_grade'],'watch')
        self.assertFalse(r['selector_recomputed'])
        self.assertFalse(r['full_history_backtest_available'])
        self.assertEqual(r['ready_count'],1)
        self.assertLess(r['avg_return_pct'],5)

    def test_splits_use_full_adjusted_path(self):
        for bar in self.bars[1:]:
            for key in ('open','high','low','close','prev_close'):
                bar[key]/=2
            bar['adj_factor']=2
        self.assertGreater(self.run_replay()['avg_return_pct'],4)

    def test_missing_session_never_shifts_later_bar_into_window(self):
        self.bars.pop(1)
        r=self.run_replay()
        self.assertEqual(r['ready_count'],0)
        self.assertIsNone(r['avg_return_pct'])
        self.assertEqual(r['excluded_count'],1)

    def test_missing_middle_adjustment_never_counts_return(self):
        self.bars[1]['adj_factor']=None
        self.assertEqual(self.run_replay()['labels'][0]['reason'],'incomplete_adjustment_path')

    def test_limit_up_buy_is_excluded(self):
        self.bars[0]['prev_close']=9
        self.assertEqual(self.run_replay()['labels'][0]['reason'],'buy_blocked_limit_up')

    def test_unknown_name_status_is_excluded(self):
        self.bars[0]['pit_status_available']=0
        self.assertEqual(self.run_replay()['ready_count'],0)

    def test_future_snapshot_cannot_be_replayed_before_publish(self):
        self.bundle['manifest']['published_at']='2026-12-01 00:00:00'
        with self.assertRaisesRegex(ValueError,'published'):
            self.run_replay()

    def test_missing_parent_and_mixed_identity_rejected(self):
        self.bundle['sources']=[]
        with self.assertRaisesRegex(ValueError,'missing'):
            self.run_replay()

    def test_source_receive_clock_never_backdated(self):
        self.bundle['sources'][0]['received_at']='2026-11-03 00:00:00'
        with self.assertRaisesRegex(ValueError,'clock'):
            self.run_replay()

    def test_source_publication_clock_never_after_decision(self):
        self.bundle['sources'][0]['source_event_time_max']='2026-11-03 00:00:00'
        with self.assertRaisesRegex(ValueError,'clock'):
            self.run_replay()

    def test_unpublished_snapshot_rejected(self):
        self.bundle['manifest']['status']='building'
        with self.assertRaisesRegex(ValueError,'ready'):
            self.run_replay()

    def test_calendar_gap_rejected_even_when_prices_exist(self):
        self.calendar.pop(1)
        with self.assertRaisesRegex(ValueError,'calendar'):
            self.run_replay()

    def test_publication_not_reference_trade_date_controls_entry(self):
        self.bundle['manifest']['published_at']='2026-11-03 00:01:00'
        self.assertEqual(self.run_replay()['labels'][0]['entry_date'],'2026-11-04')

    def test_invalid_fee_nan_rejected(self):
        with self.assertRaises(ValueError):self.run_replay(commission_bps=float('nan'))

    def test_duplicate_bar_rejected(self):
        self.bars.append(dict(self.bars[0]))
        with self.assertRaisesRegex(ValueError,'duplicate'):
            self.run_replay()


class ArchiveTest(unittest.TestCase):
    def test_roundtrip_reuse_tamper_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            a=EvidenceArchive(Path(directory));value={'hello':'真实证据'}
            r=a.put('snapshot',value)
            self.assertEqual(a.load('snapshot',r['sha256']),value)
            self.assertTrue(a.put('snapshot',value)['reused'])
            import gzip
            path=Path(directory)/f"snapshot-{r['sha256']}.json.gz"
            path.write_bytes(gzip.compress(b'{"changed":1}'))
            with self.assertRaisesRegex(ValueError,'hash'):
                a.load('snapshot',r['sha256'])

    def test_quota_failure_preserves_existing_files(self):
        with tempfile.TemporaryDirectory() as directory:
            a=EvidenceArchive(Path(directory))
            r=a.put('snapshot',{'first':1})
            with patch.object(a, 'max_bytes', 1):
                with self.assertRaisesRegex(ValueError,'quota'):
                    a.put('snapshot',{'second':2})
            self.assertEqual(a.load('snapshot',r['sha256']),{'first':1})

    def test_input_hash_matches_original_rows_not_new_capture_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            a=EvidenceArchive(Path(directory));rows=[{'code':'sh.600000'}]
            r=a.put('inputs',{'rows':rows,'captured_at':'now'},key=digest(rows))
            self.assertEqual(a.load('inputs',r['sha256'])['rows'],rows)


class ReplayBoundaryTest(unittest.TestCase):
    def test_empty_decision_preserved_without_zero_return(self):
        bundle,calendar,bars=fixture()
        bundle['candidates']=[]
        bundle['manifest']['candidate_count']=0
        result=replay_bundle(bundle,calendar=calendar,bars=[],observed_through=datetime(2026,11,16))
        self.assertTrue(result['empty_decision'])
        self.assertIsNone(result['avg_return_pct'])

    def test_unmatured_path_not_missing_data_failure_or_zero_return(self):
        bundle,calendar,bars=fixture()
        result=replay_bundle(bundle,calendar=calendar,bars=[],observed_through=datetime(2026,11,3))
        self.assertEqual(result['pending_count'],1)
        self.assertEqual(result['excluded_count'],0)
        self.assertIsNone(result['avg_return_pct'])

    def test_optional_partial_source_preserved_not_upgraded_to_complete(self):
        bundle,calendar,bars=fixture()
        bundle['sources'][0]['metadata_json']['required']=False
        bundle['sources'][0]['quality_status']='partial'
        result=replay_bundle(bundle,calendar=calendar,bars=bars,observed_through=datetime(2026,11,16))
        self.assertEqual(result['ready_count'],1)
        self.assertEqual(bundle['sources'][0]['quality_status'],'partial')

    def test_full_input_capture_uses_same_materialized_payload_hash(self):
        from types import SimpleNamespace
        from app.backtest.sentiment_evidence import archiving_materializer
        rows=[{'code':'sh.600000','close':10.0}]
        with tempfile.TemporaryDirectory() as directory:
            archive=EvidenceArchive(directory)
            service=archiving_materializer(archive=archive)
            batch=SimpleNamespace(payload_hash=digest(rows))
            with patch('app.stock_selection.sentiment_snapshot_materializer.MySQLSentimentSnapshotInputRepository._register_selection_read_view',return_value=batch):
                result=service.input_repository._register_selection_read_view(None,
                    SimpleNamespace(strategy_id="a_share_sentiment_v06",decision_as_of=datetime(2026,11,17),source_manifest_ids=[1]),rows)
            self.assertEqual(result.payload_hash,digest(rows))
            self.assertEqual(archive.load('inputs',digest(rows))['rows'],rows)

    def test_sealed_offline_path_reproduces_report_without_database(self):
        from app.backtest.sentiment_replay import clock
        bundle,calendar,bars=fixture()
        report=replay_bundle(bundle,calendar=calendar,bars=bars,observed_through=datetime(2026,11,16))
        with tempfile.TemporaryDirectory() as directory:
            archive=EvidenceArchive(directory)
            ref=archive.put('replay',{'inputs':dict(bundle=bundle,calendar=calendar,bars=bars),'report':report})
            stored=archive.load('replay',ref['sha256'])
            repeated=replay_bundle(**stored['inputs'],observed_through=clock(stored['report']['observed_through']))
            self.assertEqual(report,repeated)

    def test_409_gate_is_not_replaced_by_recorded_snapshot_diagnostic(self):
        from fastapi import HTTPException
        from app.api.routes.backtest import run_backtest,BacktestRunRequest
        with patch('app.api.routes.backtest.BacktestService') as service:
            service.return_value.submit.side_effect=ValueError('该策略不可回测')
            with self.assertRaises(HTTPException) as ctx:
                run_backtest(BacktestRunRequest(strategy_id='a_share_sentiment_v06',start_date='2026-11-02',end_date='2026-11-16'))
            self.assertEqual(ctx.exception.status_code,409)
            self.assertEqual(ctx.exception.detail['code'],'STRATEGY_NOT_REPLAYABLE')

    def test_snapshot_api_preserves_missing_evidence_409(self):
        from fastapi import HTTPException
        from app.api.routes.backtest import replay_recorded_sentiment_snapshot
        with patch('app.backtest.sentiment_replay.SentimentReplayRepository') as repository:
            repository.return_value.replay.side_effect=ValueError('source manifests missing')
            with self.assertRaises(HTTPException) as ctx:
                replay_recorded_sentiment_snapshot('s',3)
            self.assertEqual(ctx.exception.status_code,409)


class ReviewRegressionTest(unittest.TestCase):
    def test_older_frequent_versions_keep_snapshot_evidence_without_full_market_duplication(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from app.backtest.sentiment_evidence import archiving_materializer, seal_published_result
        archive = Mock()
        service = archiving_materializer(archive=archive)
        batch = SimpleNamespace(payload_hash='unused')
        with patch('app.stock_selection.sentiment_snapshot_materializer.MySQLSentimentSnapshotInputRepository._register_selection_read_view', return_value=batch):
            result = service.input_repository._register_selection_read_view(None,
                SimpleNamespace(strategy_id='a_share_sentiment'), [{'code': 'sh.600000'}])
        self.assertIs(result, batch)
        archive.put.assert_not_called()
        self.assertEqual(service.evidence_archive_events[0]['status'], 'skipped')
        bundle, _, _ = fixture()
        repository = Mock()
        repository.load_bundle.return_value = bundle
        archive.put.return_value = {'kind': 'snapshot'}
        sealed = seal_published_result({'snapshot_id': 's', 'snapshot_status': 'ready'},
            archive=archive, repository=repository, input_events=service.evidence_archive_events)
        self.assertEqual(sealed['status'], 'snapshot_only')
        archive.put.assert_called_once_with('snapshot', bundle)

    def test_close_exit_checks_close_not_open_limit_down(self):
        bundle, calendar, bars = fixture()
        bars[2].update(open=9, low=9, close=10.5, prev_close=10)
        result = replay_bundle(bundle, calendar=calendar, bars=bars,
                               observed_through=datetime(2026,11,16))
        self.assertEqual(result['ready_count'], 1)
        bars[2].update(open=10, low=9, close=9)
        result = replay_bundle(bundle, calendar=calendar, bars=bars,
                               observed_through=datetime(2026,11,16))
        self.assertEqual(result['labels'][0]['reason'], 'sell_blocked_limit_down')

    def test_duplicate_calendar_fails_closed(self):
        bundle, calendar, bars = fixture()
        calendar.append(dict(calendar[0]))
        with self.assertRaisesRegex(ValueError, 'duplicate independent calendar'):
            replay_bundle(bundle, calendar=calendar, bars=bars)

    def test_source_required_flag_does_not_accept_truthy_string(self):
        bundle, calendar, bars = fixture()
        bundle['sources'][0]['metadata_json']['required'] = 'false'
        with self.assertRaisesRegex(ValueError, 'required source'):
            replay_bundle(bundle, calendar=calendar, bars=bars)

    def test_input_archive_quota_does_not_roll_back_decision(self):
        from types import SimpleNamespace
        from app.backtest.sentiment_evidence import archiving_materializer
        rows = [{'code':'sh.600000'}]
        with tempfile.TemporaryDirectory() as directory:
            archive = EvidenceArchive(directory, max_bytes=1)
            service = archiving_materializer(archive=archive)
            batch = SimpleNamespace(payload_hash=digest(rows))
            with patch('app.stock_selection.sentiment_snapshot_materializer.MySQLSentimentSnapshotInputRepository._register_selection_read_view', return_value=batch):
                result = service.input_repository._register_selection_read_view(None,
                    SimpleNamespace(strategy_id="a_share_sentiment_v06",decision_as_of=datetime(2026,11,17),source_manifest_ids=[1]),rows)
            self.assertIs(result, batch)
            self.assertEqual(service.evidence_archive_events[0]['status'], 'warning')
            self.assertIn('quota', service.evidence_archive_events[0]['reason'])

    def test_snapshot_seal_io_failure_is_reported_not_selection_failure(self):
        from app.backtest.sentiment_evidence import seal_published_result
        from unittest.mock import Mock
        bundle, _, _ = fixture()
        archive = Mock()
        archive.put.side_effect = OSError('disk full')
        repository = Mock()
        repository.load_bundle.return_value = bundle
        result = seal_published_result({'snapshot_id':'s','snapshot_status':'ready'},
            archive=archive, repository=repository)
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['events'][0]['error_type'], 'OSError')

    def test_input_capture_clock_is_also_tamper_checked(self):
        import gzip
        import json
        with tempfile.TemporaryDirectory() as directory:
            archive=EvidenceArchive(directory)
            reference=archive.put('inputs', {'rows':[{'code':'sh.600000'}], 'captured_at':'original'})
            path=Path(directory)/f"inputs-{reference['sha256']}.json.gz"
            value=json.loads(gzip.decompress(path.read_bytes()))
            value['captured_at']='backdated'
            path.write_bytes(gzip.compress(json.dumps(value).encode()))
            with self.assertRaisesRegex(ValueError,'envelope content hash'):
                archive.load('inputs', reference['sha256'])

    def test_offline_cli_preserves_archived_horizon_clock_and_fees(self):
        import contextlib
        import io
        import json
        from scripts.replay_sentiment_snapshot import main
        bundle, calendar, bars = fixture()
        report = replay_bundle(bundle,calendar=calendar,bars=bars,holding_days=5,
            observed_through=datetime(2026,11,16),commission_bps=17,stamp_tax_bps=9,slippage_bps=21)
        with tempfile.TemporaryDirectory() as directory:
            ref = EvidenceArchive(directory).put('replay',{
                'inputs':dict(bundle=bundle,calendar=calendar,bars=bars),'report':report})
            output = io.StringIO()
            with patch('scripts.replay_sentiment_snapshot.SentimentReplayRepository', side_effect=AssertionError('offline must not use DB')):
                with contextlib.redirect_stdout(output):
                    code = main(['--sealed-report-sha256',ref['sha256'],'--archive-dir',directory])
            self.assertEqual(code,0)
            self.assertEqual(json.loads(output.getvalue()),report)


if __name__ == '__main__':
    unittest.main()
