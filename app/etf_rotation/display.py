"""Bounded display reads over immutable ETF runs, never a signal builder."""
from __future__ import annotations

from app.etf_rotation.service import _json_value
from app.shared.db import mysql_read_conn


def latest_compact(spec: dict) -> dict | None:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT run_id, trade_date, status, timing_state, candidate_count,
                eligible_count, selected_count, selection_cap, earliest_execution_at,
                model_id, version, spec_hash,
                JSON_OBJECT('complete_candidate_count', JSON_EXTRACT(diagnostics_json, '$.complete_candidate_count'),
                    'rejected_gate_counts', JSON_EXTRACT(diagnostics_json, '$.rejected_gate_counts')) AS diagnostics_json
                FROM etf_rotation_signal_run WHERE model_id=%s AND version=%s
                ORDER BY trade_date DESC, created_at DESC LIMIT 1""", (spec['model_id'], spec['version']))
            row = cursor.fetchone()
    if not row:
        return None
    item = dict(row)
    item['diagnostics'] = _json_value(item.pop('diagnostics_json'), {})
    item['candidates'] = candidates(str(item['run_id']))
    item.update(research_only=True, automatic_trading=False)
    return item


def candidates(run_id: str, *, details: bool = False) -> list[dict]:
    # Fixed frozen universe (14), run-id predicate before JSON decoding. Only
    # visible cycle evidence; hidden score columns are requested on expansion.
    score_columns = ', sector_score, etf_score, average_amount_20d_yuan, share_change_20d_pct, premium_discount_pct' if details else ''
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""SELECT sector_name, ts_code, fund_name, is_selected, is_eligible,
                combined_score, gate_json,
                JSON_OBJECT('sector_cycle_label', JSON_EXTRACT(evidence_json, '$.sector_cycle_label')) AS evidence_json
                {score_columns}
                FROM etf_rotation_signal_candidate WHERE run_id=%s ORDER BY rank_no""", (run_id,))
            rows = list(cursor.fetchall() or [])
    result = []
    for row in rows:
        item = dict(row)
        item['gates'] = _json_value(item.pop('gate_json'), {})
        item['evidence'] = _json_value(item.pop('evidence_json'), {})
        item['is_selected'] = bool(item['is_selected'])
        item['is_eligible'] = bool(item['is_eligible'])
        result.append(item)
    return result


def outcomes(run_id: str) -> list[dict]:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT ts_code, horizon_days, outcome_status,
                entry_trade_date, entry_price, exit_trade_date, exit_price, gross_return_pct
                FROM etf_rotation_forward_outcome WHERE run_id=%s
                ORDER BY signal_candidate_id, horizon_days""", (run_id,))
            return list(cursor.fetchall() or [])
