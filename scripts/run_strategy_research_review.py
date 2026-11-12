from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.shared.task_log import TaskRunLogger
from app.stock_selection.research_review import build_periodic_review


def main():
    parser = argparse.ArgumentParser(description='Produce immutable research diagnostics; never tune or promote a strategy')
    parser.add_argument('--period', choices=['weekly', 'monthly'], default='weekly')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'logs' / 'research-reviews')
    args = parser.parse_args()
    today = datetime.now(ZoneInfo('Asia/Shanghai')).date()
    end = today - timedelta(days=1)
    start = end - timedelta(days=6) if args.period == 'weekly' else (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    if args.period == 'monthly':
        end = today.replace(day=1) - timedelta(days=1)
    logger = TaskRunLogger()
    run_id = f'research_review_{args.period}_{datetime.now():%Y%m%d%H%M%S%f}'
    logger.start('strategy_research_review', run_id, {'period': args.period})
    try:
        report = build_periodic_review(start=start, end=end)
        payload = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        args.output_dir.mkdir(parents=True, exist_ok=True)
        path = args.output_dir / f'{args.period}-{end}-{digest[:16]}.json'
        try:
            with path.open('x', encoding='utf-8') as f:
                f.write(payload)
        except FileExistsError:
            if path.read_text() != payload:
                raise RuntimeError('immutable report content mismatch')
        result = {'status': 'success', 'report_path': str(path), 'sha256': digest,
                  'cohort_count': len(report['cohorts']), 'automatic_parameter_change': False}
        logger.finish('strategy_research_review', run_id, 'success', None, result)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        logger.finish('strategy_research_review', run_id, 'failed', str(exc)[:500], {})
        raise


if __name__ == '__main__':
    raise SystemExit(main())
