from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import Mock

from app.backtest.service import BacktestService, BacktestRequest
from app.backtest.policy import research_disclosure, PREVIOUS_PIT_INDEX_METHODOLOGY_VERSION
from app.api.routes.backtest import build_trade_horizon_rows
from app.jobs.readiness import _merge_factor_partition_coverage, _data_hard_reasons


def calendar(start='2026-11-09', days=5):
    return [{'cal_date':str(date.fromisoformat(start)+timedelta(days=i)),
             'is_open':1, 'source':'tushare.trade_cal'} for i in range(days)]


def bar(day, price=10, factor=1):
    return {'code':'sh.600000','trade_date':day,'open':price, 'high':price+.1,
            'low':price-.1,'close':price,'prev_close':price,'adj_factor':factor}


class DataPartitionTests(unittest.TestCase):
    def test_single_fresh_row_does_not_make_partition_ready(self):
        data={'health':'healthy','expected_latest_trade_date':'2026-11-16',
              'factor_input_latest_trade_date':'2026-11-16',
              'independent_calendar':{'health':'healthy'}}
        merged=_merge_factor_partition_coverage(data,{'expected_rows':5500,'covered_rows':1})
        self.assertEqual(merged['health'],'error')
        self.assertTrue(any('覆盖不足' in reason for reason in _data_hard_reasons(merged)))

    def test_empty_partition_not_100_percent(self):
        self.assertEqual(_merge_factor_partition_coverage({'health':'healthy'}, {})['health'],'error')

    def test_complete_partition_never_clears_another_error(self):
        self.assertEqual(_merge_factor_partition_coverage({'health':'error'},
                         {'expected_rows':5500,'covered_rows':5500})['health'],'error')


class BacktestDataIntegrityTests(unittest.TestCase):
    def test_missing_whole_signal_day_is_not_silently_skipped(self):
        repository=Mock()
        repository.fetch_calendar_rows.return_value=calendar(days=2)
        repository.fetch_trade_dates.return_value=[{'trade_date':'2026-11-09','universe_rows':5500,
                                                   'daily_rows':5500,'factor_rows':5500}]
        with self.assertRaisesRegex(ValueError,'2026-11-10 历史数据覆盖不足'):
            BacktestService(repository=repository)._fetch_trade_dates('2026-11-09','2026-11-10')

    def test_calendar_hole_and_wrong_source_fail_closed(self):
        rows=calendar(days=3)
        with self.assertRaisesRegex(ValueError,'交易日历'):
            BacktestService._validated_calendar(rows[:1]+rows[2:],'2026-11-09','2026-11-11')
        rows[1]['source']='daily_kline'
        with self.assertRaisesRegex(ValueError,'交易日历'):
            BacktestService._validated_calendar(rows,'2026-11-09','2026-11-11')

    def test_partial_factor_partition_rejected_on_a_signal_day(self):
        repository=Mock()
        repository.fetch_calendar_rows.return_value=calendar(days=1)
        repository.fetch_trade_dates.return_value=[{'trade_date':'2026-11-09','universe_rows':5500,
                                                   'daily_rows':5500,'factor_rows':5}]
        with self.assertRaisesRegex(ValueError,'覆盖不足'):
            BacktestService(repository=repository)._fetch_trade_dates('2026-11-09','2026-11-09')

    def test_missing_next_session_does_not_move_entry_to_resumption(self):
        repository=Mock()
        repository.fetch_calendar_rows.return_value=calendar(days=3)
        repository.fetch_future_bar_rows.return_value=[bar('2026-11-10'),bar('2026-11-11')]
        service=BacktestService(repository=repository)
        bars=service._fetch_future_bars(['sh.600000'],'2026-11-08',2)['sh.600000']
        self.assertEqual(bars[0]['trade_date'],'2026-11-09')
        self.assertTrue(bars[0]['missing_bar'])
        self.assertEqual(bars[1]['trade_date'],'2026-11-10')
        self.assertEqual(repository.fetch_future_bar_rows.call_args.args[-1],'2026-11-10')

    def test_missing_entry_factor_or_intermediate_factor_never_uses_raw_prices(self):
        request=BacktestRequest('test','2026-11-09','2026-11-09',return_mode='3d',use_adjusted_price=True)
        for missing_at in (0,1,2):
            with self.subTest(missing_at=missing_at):
                bars=[bar('2026-11-10'),bar('2026-11-11'),bar('2026-11-12')]
                bars[missing_at]['adj_factor']=None
                service=BacktestService()
                service._fetch_future_bars=lambda *_a,**_k:{'sh.600000':bars}
                trades,rejections=service._build_trades('run','test','2026-11-09',
                                                       [{'code':'sh.600000'}],request)
                self.assertEqual(trades,[])
                self.assertEqual(rejections,{'incomplete_adjustment_path':1})

    def test_immature_trade_not_counted_as_success_or_zero_return(self):
        service=BacktestService()
        service._fetch_future_bars=lambda *_a,**_k:{'sh.600000':[bar('2026-11-10')]}
        trades,rejections=service._build_trades('run','test','2026-11-09',[{'code':'sh.600000'}],
                                               BacktestRequest('test','2026-11-09','2026-11-09'))
        self.assertEqual(trades,[])
        self.assertEqual(rejections,{'incomplete_holding_path':1})
        summary=service._build_run_summary([],trades,'1d',rejections,1)
        self.assertIsNone(summary['avg_return_pct'])
        self.assertIsNone(summary['best_trade'])

    def test_adjusted_barrier_is_converted_to_raw_price_once(self):
        # 2-for-1 split: raw 5.3 * factor2 = comparable10.6 => exactly +6%.
        service=BacktestService()
        bars=[bar('2026-11-10'),{**bar('2026-11-11',5.25,2),'high':5.31}]
        request=BacktestRequest('test','2026-11-09','2026-11-09',use_adjusted_price=True)
        exit_bar,price,reason,factor=service._resolve_triple_barrier_exit(bars,10,request,6,-3,5)
        self.assertEqual(reason,'take_profit')
        self.assertAlmostEqual(price,5.3)
        self.assertEqual(service._pct_return(10,price,1,factor,request),6)

    def test_gap_through_stop_uses_gap_open_not_unavailable_stop_price(self):
        service=BacktestService()
        bars=[bar('2026-11-10'),bar('2026-11-11',9)]
        request=BacktestRequest('test','2026-11-09','2026-11-09')
        _,price,reason,_=service._resolve_triple_barrier_exit(bars,10,request,6,-3,5)
        self.assertEqual(reason,'stop_loss')
        self.assertEqual(price,9)

    def test_execution_unknown_price_and_state_are_not_assumed_tradable(self):
        self.assertEqual(BacktestService._execution_block_reason_v6({'open':10},'buy'),'missing_previous_close')
        self.assertEqual(BacktestService._execution_block_reason_v6(
            {'open':10,'prev_close':10,'pit_status_available':0},'buy'),'unknown_execution_status')

    def test_horizon_adjustment_does_not_fall_back_to_raw(self):
        trade={'trade_date':'2026-11-09','entry_date':'2026-11-10','entry_price':10,'code':'sh.600000'}
        key=('2026-11-09','sh.600000')
        adjusted=build_trade_horizon_rows([trade],{key:[bar('2026-11-10',10,None)]},True)
        raw=build_trade_horizon_rows([trade],{key:[bar('2026-11-10',10,None)]},False)
        self.assertIsNone(adjusted[key][0]['close_return_pct'])
        self.assertEqual(raw[key][0]['close_return_pct'],0)

    def test_v5_historical_disclosure_preserved(self):
        old=research_disclosure(PREVIOUS_PIT_INDEX_METHODOLOGY_VERSION)
        self.assertIn('DQ5',old['risk_notice'])
        self.assertEqual(old['methodology_version'],PREVIOUS_PIT_INDEX_METHODOLOGY_VERSION)



class IndependentCalendarSyncTests(unittest.TestCase):
    def test_reject_duplicate_days_or_invalid_flags(self):
        from app.data_ingestion.trade_calendar_sync import validate_calendar_records
        day={'exchange':'SSE','cal_date':'20241101','is_open':1}
        with self.assertRaisesRegex(ValueError,'不连续或重复'):
            validate_calendar_records([day,day],date(2024,11,1),date(2024,11,2))
        with self.assertRaisesRegex(ValueError,'标志异常'):
            validate_calendar_records([{**day,'is_open':2}],date(2024,11,1),date(2024,11,1))

    def test_existing_conflict_is_never_overwritten(self):
        from app.data_ingestion.trade_calendar_sync import sync_calendar
        from tests.test_backtest_repository import FakeCursor, connection_factory
        cursor=FakeCursor(fetchall_values=[[{'cal_date':'2024-11-01','is_open':0,'source':'tushare.trade_cal'}]])
        provider=Mock()
        provider.trade_cal.return_value.to_dict.return_value=[{'exchange':'SSE','cal_date':'20241101','is_open':1}]
        with self.assertRaisesRegex(ValueError,'拒绝覆盖'):
            sync_calendar('2024-11-01','2024-11-01',apply=True,provider=provider,
                          connection_factory=connection_factory(cursor))
        self.assertEqual(cursor.executed_many,[])

    def test_only_missing_dates_inserted_and_repeat_is_idempotent(self):
        from app.data_ingestion.trade_calendar_sync import sync_calendar
        from tests.test_backtest_repository import FakeCursor, connection_factory
        provider=Mock()
        provider.trade_cal.return_value.to_dict.return_value=[{'exchange':'SSE','cal_date':'20241101','is_open':1}]
        for existing in ([],[{'cal_date':'2024-11-01','is_open':1,'source':'tushare.trade_cal'}]):
            cursor=FakeCursor(fetchall_values=[existing])
            result=sync_calendar('2024-11-01','2024-11-01',apply=True,provider=provider,
                                 connection_factory=connection_factory(cursor))
            self.assertEqual(result['inserted_days'],0 if existing else 1)
            self.assertEqual(len(cursor.executed_many),0 if existing else 1)


class FinancialPathEdgesTests(unittest.TestCase):
    def test_next_open_exit_risk_excludes_later_intraday_move(self):
        service=BacktestService()
        service._fetch_future_bars=lambda *_a,**_k:{'sh.600000':[
            bar('2026-11-10'),{**bar('2026-11-11'),'high':100,'low':1}]}
        trades,_=service._build_trades('run','test','2026-11-09',[{'code':'sh.600000'}],
                                      BacktestRequest('test','2026-11-09','2026-11-09'))
        self.assertEqual(trades[0]['max_gain_pct'],1)
        self.assertEqual(trades[0]['max_drawdown_pct'],-1)

    def test_barrier_early_exit_does_not_require_unobserved_later_prices(self):
        service=BacktestService()
        service._fetch_future_bars=lambda *_a,**_k:{'sh.600000':[
            bar('2026-11-10'),{**bar('2026-11-11'),'high':10.7}]}
        trades,reasons=service._build_trades('run','test','2026-11-09',[{'code':'sh.600000'}],
            BacktestRequest('test','2026-11-09','2026-11-09',return_mode='triple_barrier_5d'))
        self.assertEqual(reasons,{})
        self.assertEqual(trades[0]['return_3d_pct'],6)

    def test_nonfinite_price_path_rejected(self):
        self.assertEqual(BacktestService._path_issue([{**bar('2026-11-10'),'high':float('nan')}],1,False),
                         'missing_or_invalid_holding_bar')

if __name__=='__main__':
    unittest.main()
