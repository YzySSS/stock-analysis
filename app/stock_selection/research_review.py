"""Bounded, cohort-isolated review. This module never edits strategy parameters."""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from statistics import mean
from typing import Any

from app.shared.db import mysql_read_conn
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.automatic_observation import discover_automatic_observation_policies
from app.stock_selection.factor_evaluation_v2 import _json_value, _to_float


def summarize_cohort(*, campaign, observations, picks, expected_dates):
    protocols = _json_value(campaign.get('protocol_ids_json'), {})
    result = {}
    for role, protocol in protocols.items():
        decisions = [r for r in observations if r['protocol_id'] == protocol]
        successful = [r for r in decisions if r['status'] == 'success']
        observed = {r['signal_trade_date'] for r in successful}
        rows = [r for r in picks if r['protocol_id'] == protocol]
        # Repeated stock/day decisions must not increase the effective sample size.
        rows = list({(r['signal_trade_date'], r['code']): r for r in rows}.values())
        valid = [r for r in rows if r.get('price_adjustment_mode') == 'adjusted_total_return'
                 and _to_float(r.get('return_5d_pct')) is not None]
        net_returns = [float(r['return_5d_pct']) - 0.25 for r in valid]
        counts = Counter(r['code'] for r in rows)
        result[role] = {
            'expected_days': len(expected_dates), 'successful_days': len(observed),
            'missed_dates': sorted(str(d) for d in set(expected_dates) - observed),
            'empty_days': sum(int(r.get('result_count') or 0) == 0 for r in successful),
            'data_failed_decisions': sum(r['status'] != 'success' for r in decisions),
            'stock_day_samples': len(rows), 'adjusted_5d_labels': len(valid),
            'missing_or_immature_labels': len(rows) - len(valid),
            'mean_5d_net_diagnostic_pct': mean(net_returns) if net_returns else None,
            'worst_5d_net_diagnostic_pct': min(net_returns) if net_returns else None,
            'largest_stock_sample_share': max(counts.values()) / len(rows) if rows else None,
            'execution_validation': 'not_validated_open_price_diagnostic_only',
        }
    paired = len(set.intersection(*[
        {r['signal_trade_date'] for r in observations if r['protocol_id'] == protocol and r['status'] == 'success'}
        for protocol in protocols.values()
    ])) if len(protocols) == 2 else 0
    sufficient = paired >= 30 and all(r['adjusted_5d_labels'] >= 60 and not r['missed_dates'] for r in result.values())
    return {
        'campaign_id': campaign['campaign_id'], 'roles': result, 'paired_days': paired,
        'next_action': 'design_new_frozen_shadow_experiment' if sufficient else 'continue_collecting_or_repair_data',
        'automatic_parameter_change': False, 'promotion_allowed': False,
        'promotion_blockers': ['independent_out_of_sample_validation_required',
                               'tradability_and_t_plus_one_simulation_required',
                               'human_review_required'],
    }


def build_periodic_review(*, start: date, end: date, connection_factory=mysql_read_conn) -> dict[str, Any]:
    if not 0 <= (end - start).days <= 366:
        raise ValueError('review window must be ordered and at most 366 days')
    with connection_factory() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM strategy_observation_campaign
                WHERE started_on<=%s ORDER BY started_on DESC LIMIT 101''', (end,))
            campaigns = cursor.fetchall() or []
            if len(campaigns) > 100:
                raise RuntimeError('review campaign budget exceeded')
            reports = []
            # Also report an entirely missed cohort (no Cron execution at all),
            # rather than silently succeeding with an empty report.
            known = {row['campaign_id'] for row in campaigns}
            for policy in discover_automatic_observation_policies(StrategyLoader(), today=end):
                if policy.start_on <= end and policy.campaign_epoch and policy.campaign_id not in known:
                    campaigns.append({
                        'campaign_id': policy.campaign_id, 'started_on': policy.start_on,
                        'protocol_ids_json': {role: policy.protocol_id(role, sid, ver)
                            for role, sid, ver in (
                                ('baseline', policy.baseline_strategy_id, policy.baseline_strategy_version),
                                ('candidate', policy.candidate_strategy_id, policy.candidate_strategy_version))},
                        'metadata_json': {'clock_contract': 'opening_window_v2'},
                    })
            campaigns = [c for c in campaigns if _json_value(c.get('metadata_json'), {}).get('clock_contract') == 'opening_window_v2']
            calendar_start = max(end - timedelta(days=366), min([start] + [c['started_on'] for c in campaigns]))
            cursor.execute('''SELECT cal_date, is_open FROM etf_rotation_trade_calendar
                WHERE exchange_code='SSE' AND source='tushare.trade_cal'
                AND cal_date BETWEEN %s AND %s ORDER BY cal_date''', (calendar_start, end))
            calendar = cursor.fetchall() or []
            if len(calendar) != (end - calendar_start).days + 1:
                raise RuntimeError('incomplete independent trade calendar; cannot measure missed observations')
            market_dates = [r['cal_date'] for r in calendar if r['is_open']]
            for campaign in campaigns:
                metadata = _json_value(campaign.get('metadata_json'), {})
                if metadata.get('clock_contract') != 'opening_window_v2':
                    continue  # Legacy protocols remain readable but are never pooled.
                sample_start = max(calendar_start, campaign['started_on'])
                params = (campaign['campaign_id'], sample_start, end)
                cursor.execute('''SELECT protocol_id, signal_trade_date, status, result_count
                    FROM strategy_forward_observation WHERE campaign_id=%s
                    AND signal_trade_date BETWEEN %s AND %s ORDER BY signal_trade_date LIMIT 1001''', params)
                observations = cursor.fetchall() or []
                cursor.execute('''SELECT protocol_id, signal_trade_date, code, price_adjustment_mode,
                    return_5d_pct FROM strategy_forward_pick WHERE campaign_id=%s
                    AND signal_trade_date BETWEEN %s AND %s ORDER BY signal_trade_date, id LIMIT 10001''', params)
                picks = cursor.fetchall() or []
                if len(observations) > 1000 or len(picks) > 10000:
                    raise RuntimeError('review sample budget exceeded')
                expected = [d for d in market_dates if d >= sample_start
                            and (not campaign.get('completed_on') or d <= campaign['completed_on'])]
                cumulative = summarize_cohort(campaign=campaign, observations=observations,
                                               picks=picks, expected_dates=expected)
                cumulative['period'] = summarize_cohort(
                    campaign=campaign, observations=[r for r in observations if r['signal_trade_date'] >= start],
                    picks=[r for r in picks if r['signal_trade_date'] >= start],
                    expected_dates=[d for d in expected if d >= start])
                reports.append(cumulative)
    return {'status': 'success', 'window': [str(start), str(end)], 'research_only': True,
            'automatic_parameter_change': False, 'cohorts': reports}
