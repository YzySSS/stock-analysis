from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.valuation_sync import ValuationSync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batched valuation sync jobs until coverage target or exhaustion.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--pause-between-batches", type=float, default=2.0)
    parser.add_argument("--max-batches", type=int, default=0, help="0 means no explicit batch cap")
    parser.add_argument("--stale-after-days", type=int, default=7)
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--all", action="store_true", help="Sync stale records instead of only missing ones")
    parser.add_argument("--cooldown-days", type=int, default=1, help="How many days to skip codes that had no valuation source for the same trade date")
    parser.add_argument("--state-file", default=str(PROJECT_ROOT / "logs" / "valuation_sync_missing_codes.json"))
    parser.add_argument("--stop-after-no-progress", type=int, default=5, help="Stop after this many consecutive no-progress batches")
    parser.add_argument("--retry-on-error", type=int, default=3, help="Retry a failed batch this many times before aborting")
    parser.add_argument("--retry-wait-seconds", type=float, default=10.0, help="Seconds to wait before retrying after a batch error")
    parser.add_argument("--pb-only", action="store_true", help="Only continue backfilling rows where PB is still missing")
    return parser


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = build_parser().parse_args()
    sync = ValuationSync()
    state_path = Path(args.state_file)
    state = load_state(state_path)

    batch_no = 0
    summaries = []
    total_scanned = 0
    total_updated = 0
    total_missing_source = 0
    no_progress_streak = 0

    while True:
        if args.max_batches and batch_no >= args.max_batches:
            break

        attempt = 0
        result = None
        trade_date = None
        exclude_list = []
        now_ts = int(time.time())

        while True:
            try:
                trade_date = sync.get_trade_date()
                trade_state = state.get(trade_date, {})
                now_ts = int(time.time())
                exclude_list = [code for code, expiry in trade_state.items() if int(expiry) > now_ts]

                result = sync.run(
                    limit=args.batch_size,
                    instrument_type=args.instrument_type,
                    only_missing=not args.all,
                    stale_after_days=args.stale_after_days,
                    exclude_codes=exclude_list,
                    require_missing_pb=args.pb_only,
                    allow_missing_pe_only=not args.pb_only,
                )
                break
            except Exception as e:
                attempt += 1
                payload = {
                    "batch_no": batch_no + 1,
                    "status": "retrying" if attempt <= args.retry_on_error else "failed",
                    "attempt": attempt,
                    "error": str(e)[:500],
                }
                print(json.dumps(payload, ensure_ascii=False), flush=True)
                if attempt > args.retry_on_error:
                    raise
                time.sleep(args.retry_wait_seconds)

        batch_no += 1
        summaries.append(result.to_dict())
        total_scanned += result.scanned
        total_updated += result.updated
        total_missing_source += result.missing_source

        missing_codes = []
        if result.missing_source:
            from app.shared.db import mysql_conn
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT metadata_json FROM task_run_log WHERE task_name='valuation_sync' AND run_id=%s ORDER BY id DESC LIMIT 1",
                        (result.run_id,),
                    )
                    row = cursor.fetchone() or {}
                    metadata = row.get('metadata_json') or {}
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                    missing_codes = metadata.get('missing_codes') or []
            expiry = now_ts + args.cooldown_days * 86400
            next_trade_state = state.setdefault(trade_date, {})
            for code in missing_codes:
                next_trade_state[code] = expiry
            save_state(state_path, state)

        payload = {
            "batch_no": batch_no,
            **result.to_dict(),
            "excluded_codes": len(exclude_list),
            "new_missing_codes": len(missing_codes),
            "no_progress_streak": no_progress_streak,
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)

        if result.scanned < args.batch_size:
            break
        if result.updated == 0 and result.missing_source == 0:
            break
        if result.updated == 0:
            no_progress_streak += 1
        else:
            no_progress_streak = 0
        if no_progress_streak >= args.stop_after_no_progress:
            break

        time.sleep(args.pause_between_batches)

    print(json.dumps({
        "finished": True,
        "batch_size": args.batch_size,
        "batches": batch_no,
        "total_scanned": total_scanned,
        "total_updated": total_updated,
        "total_missing_source": total_missing_source,
        "runs": summaries,
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
