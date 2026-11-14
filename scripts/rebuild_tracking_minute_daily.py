"""Bounded explicit-date repair/backfill; daily lifecycle keeps future dates current."""
from __future__ import annotations
import argparse
import json
import time
from datetime import date, timedelta
from app.tracking.minute_daily import rebuild_day


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not 0 <= (args.end - args.start).days <= 180:
        parser.error('range must be ordered and no longer than 181 calendar dates')
    target = args.start
    while target <= args.end:
        result = {'trade_date': str(target), 'status': 'dry_run'}
        if args.apply:
            result.update(source_revision=rebuild_day(target), status='published')
        print(json.dumps(result), flush=True)
        target += timedelta(days=1)
        if args.apply:
            time.sleep(0.15)


if __name__ == '__main__':
    main()
