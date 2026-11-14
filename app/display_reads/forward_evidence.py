"""Display-only cumulative SQL aggregates plus bounded recent evidence.

No protocol writer or frozen research implementation is changed. All statements
share one repeatable-read snapshot; recent rows never define cumulative totals.
"""
from __future__ import annotations

from collections import defaultdict
from fractions import Fraction
from types import SimpleNamespace

from app.shared.db import mysql_read_conn
from app.stock_selection.forward_observation import ForwardObservationService, _from_json, _float

HORIZONS = (1, 3, 5, 20)
RISK_FIELDS = ('max_favorable_5d_pct', 'max_adverse_5d_pct', 'max_favorable_20d_pct', 'max_adverse_20d_pct')
PICK_COLUMNS = '''id, observation_id, campaign_id, observation_source, source_snapshot_id,
    signal_trade_date, code, name, rank_no, score, theme_name, trade_grade_state,
    entry_trade_date, entry_price, return_1d_pct, return_3d_pct, return_5d_pct, return_20d_pct, outcome_status'''
ACTION_COLUMNS = 'id, observation_id, code, action_type, action_at, action_price, note, source'


def _rounded_average(cursor, value, query, params):
    if value is None:
        return None
    number = float(value)
    # SQL decimal AVG and statistics.mean(float(...)) may round differently at
    # exact half steps. Only that rare boundary needs scalar frequency groups.
    if abs(abs(number * 10000) % 1 - .5) < 1e-6:
        cursor.execute(query, params)
        total, count = Fraction(), 0
        for row in cursor.fetchall() or []:
            frequency = int(row['n'])
            total += Fraction.from_float(float(row['v'])) * frequency
            count += frequency
        return round(float(total / count), 4) if count else None
    return round(number, 4)


def evidence_summary(strategy_id: str) -> dict:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('''SELECT * FROM strategy_forward_protocol WHERE strategy_id=%s
                ORDER BY started_on DESC, id DESC LIMIT 1''', (strategy_id,))
            protocol = cursor.fetchone() or {}
            if not protocol:
                return {'status': 'not_configured', 'validation_status': 'unvalidated', 'strategy_id': strategy_id}
            protocol_id = protocol['protocol_id']
            cursor.execute('''SELECT observation_id,campaign_id,observation_source,source_snapshot_id,
                paired_input_hash,signal_trade_date,status,result_count,ai_mode,selection_run_id
                FROM strategy_forward_observation WHERE protocol_id=%s
                ORDER BY signal_trade_date DESC,id DESC LIMIT 20''', (protocol_id,))
            observations = list(reversed(cursor.fetchall() or []))
            cursor.execute(f'''SELECT {PICK_COLUMNS} FROM strategy_forward_pick WHERE protocol_id=%s
                ORDER BY signal_trade_date DESC,rank_no DESC,id DESC LIMIT 50''', (protocol_id,))
            picks = list(reversed(cursor.fetchall() or []))
            cursor.execute(f'''SELECT {ACTION_COLUMNS} FROM strategy_forward_action WHERE protocol_id=%s
                ORDER BY action_at DESC,id DESC LIMIT 100''', (protocol_id,))
            actions_by_id = {row['id']: row for row in cursor.fetchall() or []}
            # The old payload exposes every action type for the 50 visible
            # picks. Keep that exact contract, but never hydrate metadata_json.
            if picks:
                placeholders = ','.join(['(%s,%s)'] * len(picks))
                params = [protocol_id]
                for row in picks:
                    params.extend((row['observation_id'], row['code']))
                cursor.execute(f'''SELECT {ACTION_COLUMNS} FROM strategy_forward_action
                    WHERE protocol_id=%s AND (observation_id,code) IN ({placeholders})
                    ORDER BY action_at,id''', params)
                actions_by_id.update({row['id']: row for row in cursor.fetchall() or []})
            actions = sorted(actions_by_id.values(), key=lambda row: (row['action_at'], row['id']))
            # Reuse the immutable formatter/version/contamination policy. Its
            # temporary recent-only metrics are replaced by whole-protocol SQL.
            repository = SimpleNamespace(evidence_rows=lambda _: (protocol, observations, picks, actions))
            result = ForwardObservationService(repository=repository).evidence_summary(strategy_id)
            cursor.execute('''SELECT status,ai_mode,COUNT(*) AS n,
                SUM(result_count=0) AS zero_n FROM strategy_forward_observation
                WHERE protocol_id=%s GROUP BY status,ai_mode''', (protocol_id,))
            observation_days = failed = zero = 0
            modes = defaultdict(int)
            for row in cursor.fetchall() or []:
                if row['status'] == 'success':
                    observation_days += int(row['n'])
                    zero += int(row['zero_n'] or 0)
                    if row['ai_mode']:
                        modes[str(row['ai_mode'])] += int(row['n'])
                if row['status'] == 'failed':
                    failed += int(row['n'])
            cursor.execute('''SELECT action_type,COUNT(*) AS n FROM strategy_forward_action
                WHERE protocol_id=%s GROUP BY action_type''', (protocol_id,))
            action_counts = {row['action_type']: int(row['n']) for row in cursor.fetchall() or [] if row['action_type']}
            joined = '''FROM strategy_forward_pick p LEFT JOIN (
                SELECT observation_id,code,1 AS acted,
                    MAX(action_type IN ('saved','bought','skipped','sold')) AS decided,
                    MAX(action_type='bought') AS bought,MAX(action_type='sold') AS sold
                FROM strategy_forward_action WHERE protocol_id=%s GROUP BY observation_id,code
                ) a ON a.observation_id=p.observation_id AND a.code=p.code WHERE p.protocol_id=%s'''
            columns = ['COUNT(*) AS candidate_count', "SUM(p.outcome_status='complete') AS complete_n",
                "SUM(p.price_adjustment_mode='adjusted_total_return') AS adjusted_n",
                "SUM(p.price_adjustment_mode='raw_fallback') AS raw_n"]
            columns += [f'SUM(COALESCE(a.{key},0)) AS {key}_n' for key in ('acted','decided','bought','sold')]
            for h in HORIZONS:
                field = f'p.return_{h}d_pct'
                for prefix, expression in [('',field),('bought_',f'CASE WHEN a.bought=1 THEN {field} END')]:
                    columns += [f'COUNT({expression}) AS {prefix}n{h}',f'AVG({expression}) AS {prefix}avg{h}',
                                f'SUM(({expression}) > 0) AS {prefix}win{h}']
            columns += [f'AVG(p.{field}) AS {field}' for field in RISK_FIELDS]
            cursor.execute('SELECT ' + ','.join(columns) + ' ' + joined, (protocol_id, protocol_id))
            aggregate = cursor.fetchone() or {}
            metrics, bought_metrics = {}, {}
            for h in HORIZONS:
                for prefix, target in [('',metrics),('bought_',bought_metrics)]:
                    count = int(aggregate.get(f'{prefix}n{h}') or 0)
                    field = f'p.return_{h}d_pct'
                    where = ' AND a.bought=1' if prefix else ''
                    average = _rounded_average(cursor, aggregate.get(f'{prefix}avg{h}'),
                        f'SELECT {field} AS v,COUNT(*) AS n ' + joined + where + f' AND {field} IS NOT NULL GROUP BY {field}',
                        (protocol_id,protocol_id))
                    target[str(h)] = {'sample_size': count, 'average_return_pct': average,
                        'win_rate_pct': round(int(aggregate.get(f'{prefix}win{h}') or 0) / count * 100, 2) if count else None}
                    if not prefix:
                        target[str(h)].update(average_benchmark_return_pct={},average_excess_return_pct={})
            # JSON_TABLE expands only the two benchmark maps; do not transfer
            # outcome price paths/lineage or whole per-pick outcome JSON.
            benchmark_from = '''FROM strategy_forward_pick p
                JOIN JSON_TABLE('[1,3,5,20]', '$[*]' COLUMNS(horizon INT PATH '$')) h
                JOIN JSON_TABLE('["benchmark_returns_pct","excess_returns_pct"]', '$[*]'
                    COLUMNS(kind VARCHAR(32) PATH '$')) k
                JOIN JSON_TABLE(JSON_KEYS(JSON_EXTRACT(p.outcome_json,
                    CONCAT('$.horizons."',h.horizon,'".',k.kind))), '$[*]'
                    COLUMNS(benchmark VARCHAR(128) PATH '$')) b
                WHERE p.protocol_id=%s'''
            expression = "JSON_EXTRACT(p.outcome_json,CONCAT('$.horizons.\"',h.horizon,'\".',k.kind,'.',JSON_QUOTE(b.benchmark)))"
            cursor.execute(f'''SELECT h.horizon,k.kind,b.benchmark,{expression} AS raw_value,
                COUNT(*) AS n {benchmark_from} GROUP BY h.horizon,k.kind,b.benchmark,raw_value''', (protocol_id,))
            # Frequency groups preserve statistics.mean's exact float semantics,
            # numeric strings/booleans and arbitrary benchmark map keys without
            # transferring complete per-pick outcome objects.
            benchmark_totals = {}
            for row in cursor.fetchall() or []:
                value = _float(_from_json(row['raw_value']))
                if value is None:
                    continue
                key = (str(row['horizon']), row['kind'], row['benchmark'])
                total, count = benchmark_totals.get(key, (Fraction(), 0))
                frequency = int(row['n'])
                benchmark_totals[key] = (total + Fraction.from_float(value) * frequency, count + frequency)
            for (horizon, kind, benchmark), (total, count) in benchmark_totals.items():
                key = 'average_benchmark_return_pct' if kind=='benchmark_returns_pct' else 'average_excess_return_pct'
                metrics[horizon][key][benchmark] = round(float(total / count), 4)
            candidates = int(aggregate.get('candidate_count') or 0)
            min_days, min_candidates = int(protocol['minimum_observation_days'] or 0),int(protocol['minimum_candidate_count'] or 0)
            result.update(status='preliminary_ready' if observation_days>=min_days or candidates>=min_candidates else 'collecting',
                sample={'observation_days': observation_days,'candidate_count':candidates,'zero_pick_days':zero,'failed_days':failed,
                    'observation_target_met':observation_days>=min_days,'candidate_target_met':candidates>=min_candidates},
                metrics=metrics,ai_modes=dict(sorted(modes.items())),actions=action_counts)
            quality = {'forward_returns':metrics,'complete_20d_pick_count':int(aggregate.get('complete_n') or 0),
                'adjusted_pick_count':int(aggregate.get('adjusted_n') or 0),'raw_fallback_pick_count':int(aggregate.get('raw_n') or 0)}
            for field,label in zip(RISK_FIELDS,('average_mfe_5d_pct','average_mae_5d_pct','average_mfe_20d_pct','average_mae_20d_pct')):
                quality[label] = _rounded_average(cursor,aggregate.get(field),
                    f'SELECT {field} AS v,COUNT(*) AS n FROM strategy_forward_pick WHERE protocol_id=%s AND {field} IS NOT NULL GROUP BY {field}', (protocol_id,))
            result['advice_quality'] = quality
            result['user_discipline'] = {f'{key}_pick_count':int(aggregate.get(f'{key}_n') or 0) for key in ('acted','decided','bought','sold')}
            result['user_discipline'].update(decision_rate_pct=round(int(aggregate.get('decided_n') or 0)/candidates*100,2) if candidates else None,
                bought_forward_returns=bought_metrics)
            return result
