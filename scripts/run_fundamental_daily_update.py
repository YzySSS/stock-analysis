from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.fundamental_sync import FundamentalSync


def main() -> None:
    sync = FundamentalSync(sleep_seconds=0.35)
    result = sync.run(limit=120, only_missing=True, stale_after_days=30)
    print(json.dumps(result.to_dict(), ensure_ascii=False))


if __name__ == "__main__":
    main()
