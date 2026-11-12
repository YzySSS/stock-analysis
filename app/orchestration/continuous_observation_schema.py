from app.shared.db import mysql_conn


def ensure_continuous_observation_schema():
    """Allow a new cohort without overwriting a frozen candidate/version cohort."""
    with mysql_conn(dict_cursor=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute('SHOW INDEX FROM strategy_observation_campaign')
            rows = cursor.fetchall() or []
            name = 'uniq_strategy_observation_candidate_version'
            old = sorted([r for r in rows if r['Key_name'] == name], key=lambda r: r['Seq_in_index'])
            if old:
                if [r['Column_name'] for r in old] != ['candidate_strategy_id', 'candidate_strategy_version']:
                    raise RuntimeError('unexpected campaign index definition')
                if {int(r['Non_unique']) for r in old} != {0}:
                    raise RuntimeError('unexpected campaign index uniqueness')
                cursor.execute('''ALTER TABLE strategy_observation_campaign
                    DROP INDEX uniq_strategy_observation_candidate_version,
                    ADD INDEX idx_observation_candidate_version
                        (candidate_strategy_id, candidate_strategy_version),
                    ALGORITHM=INPLACE, LOCK=NONE''')
            else:
                replacement = sorted([r for r in rows if r['Key_name'] == 'idx_observation_candidate_version'],
                                     key=lambda r: r['Seq_in_index'])
                if ([r['Column_name'] for r in replacement] != ['candidate_strategy_id', 'candidate_strategy_version']
                        or {int(r['Non_unique']) for r in replacement} != {1}):
                    raise RuntimeError('missing campaign replacement index')
    return {'status': 'ok', 'changed': bool(old)}
