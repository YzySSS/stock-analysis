from datetime import date, datetime
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from scripts.refresh_sentiment_v06_postclose import refresh_postclose


class SentimentPostcloseRefreshTest(unittest.TestCase):
    def setUp(self):
        self.clock = datetime(2026, 11, 16, 18, 57, tzinfo=ZoneInfo('Asia/Shanghai'))
        self.repo = Mock()
        self.repo.calendar_day.return_value = {
            'cal_date': date(2026, 11, 16), 'is_open': 1, 'source': 'tushare.trade_cal'}
        self.repo.coverage.return_value = {'expected_rows': 100, 'covered_rows': 99}
        self.calls = []
        def sync(name):
            def run(start, end, **kwargs):
                self.calls.append((name, start, end))
                return {'trade_dates': 1, 'processed_days': 1, 'rows_synced': 99}
            return lambda: SimpleNamespace(run=run)
        self.money = sync('moneyflow')
        self.chip = sync('chip')
        self.materializer = Mock()
        self.materializer.return_value.materialize.return_value = {
            'snapshot_status': 'ready', 'quality_status': 'passed', 'trade_date': '2026-11-16'}

    def run_refresh(self, **kwargs):
        return refresh_postclose(now=self.clock, repository=self.repo,
                                 moneyflow_factory=self.money, chip_factory=self.chip,
                                 materializer_factory=self.materializer, **kwargs)

    def test_real_dependency_order_before_snapshot(self):
        self.assertEqual(self.run_refresh()['status'], 'success')
        self.assertEqual(self.calls, [('moneyflow', '2026-11-16', '2026-11-16'),
                                     ('chip', '2026-11-16', '2026-11-16')])
        self.materializer.return_value.materialize.assert_called_once_with(
            strategy_id='a_share_sentiment_v06', allow_shadow=True)

    def test_missing_calendar_fails_without_provider_or_publish(self):
        self.repo.calendar_day.return_value = {}
        with self.assertRaisesRegex(ValueError, 'calendar'):
            self.run_refresh()
        self.assertFalse(self.calls)
        self.materializer.assert_not_called()

    def test_preopen_is_not_postclose_and_does_not_refresh(self):
        self.clock = self.clock.replace(hour=1)
        self.assertEqual(self.run_refresh()['status'], 'skipped')
        self.assertFalse(self.calls)
        self.materializer.assert_not_called()

    def test_weekend_does_not_refresh(self):
        self.repo.calendar_day.return_value['is_open'] = 0
        self.assertEqual(self.run_refresh()['status'], 'skipped')
        self.assertFalse(self.calls)

    def test_dry_run_never_calls_sources_or_materializer(self):
        self.assertEqual(self.run_refresh(dry_run=True)['status'], 'dry_run')
        self.assertFalse(self.calls)
        self.materializer.assert_not_called()

    def test_empty_provider_cannot_be_successful_publication(self):
        self.money = lambda: SimpleNamespace(run=lambda *a, **kw: {
            'trade_dates': 1, 'processed_days': 1, 'rows_synced': 0})
        with self.assertRaisesRegex(ValueError, 'no data'):
            self.run_refresh()
        self.materializer.assert_not_called()

    def test_partially_missing_valid_intersection_preserves_old_snapshot(self):
        self.repo.coverage.return_value = {'expected_rows': 100, 'covered_rows': 97}
        with self.assertRaisesRegex(ValueError, 'below 98%'):
            self.run_refresh()
        self.materializer.assert_not_called()

    def test_no_daily_partition_not_replaced_by_previous_day(self):
        self.money = lambda: SimpleNamespace(run=lambda *a, **kw: {
            'trade_dates': 0, 'processed_days': 0, 'rows_synced': 0})
        with self.assertRaisesRegex(ValueError, 'no data'):
            self.run_refresh()
        self.materializer.assert_not_called()

    def test_snapshot_must_still_pass_all_existing_quality_gates(self):
        self.materializer.return_value.materialize.return_value['quality_status'] = 'failed'
        with self.assertRaisesRegex(ValueError, 'ready/passed'):
            self.run_refresh()


if __name__ == '__main__':
    unittest.main()
