"""Read/replay one recorded sentiment decision; no selector/provider/job writes."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.backtest.sentiment_replay import SentimentReplayRepository, replay_bundle, clock
from app.backtest.sentiment_evidence import EvidenceArchive, DEFAULT_ARCHIVE


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--snapshot-id")
    identity.add_argument("--sealed-report-sha256", help="offline replay of previously sealed full evidence/path")
    parser.add_argument("--holding-days", type=int, choices=(1, 3, 5))
    parser.add_argument("--archive", action="store_true", help="seal current DB evidence and report locally")
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE)
    args = parser.parse_args(argv)
    try:
        archive = EvidenceArchive(args.archive_dir)
        if args.sealed_report_sha256:
            stored = archive.load('replay', args.sealed_report_sha256)
            inputs = stored['inputs']
            report = stored['report']
            fees = report['fees_bps']
            result = replay_bundle(**inputs, holding_days=args.holding_days or report['holding_days'],
                observed_through=clock(report['observed_through']),
                commission_bps=fees['commission_each_side'], stamp_tax_bps=fees['stamp_tax_sell'],
                slippage_bps=fees['slippage_each_side'])
        else:
            repository = SentimentReplayRepository()
            result = repository.replay(args.snapshot_id, holding_days=args.holding_days or 3,
                                       capture_inputs=args.archive)
            inputs = result.pop('_sealed_inputs', None)
        if args.archive:
            result['archived_evidence'] = archive.put('snapshot', inputs['bundle'])
            result['archived_report'] = archive.put('replay', {'report': dict(result), 'inputs': inputs})
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except (LookupError, ValueError, OSError) as exc:
        print(json.dumps({"status": "rejected", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
