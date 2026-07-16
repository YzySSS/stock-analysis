from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.realtime_lifecycle import build_lifecycle_plan, migrate_intraday_to_partitions


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely rebuild stock_realtime_intraday with daily partitions")
    parser.add_argument("--execute", action="store_true", help="perform the shadow-table migration; default is dry-run")
    args = parser.parse_args()

    if not args.execute:
        print(json.dumps({"status": "dry_run", **build_lifecycle_plan()}, ensure_ascii=False, default=str))
        return

    result = migrate_intraday_to_partitions()
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
