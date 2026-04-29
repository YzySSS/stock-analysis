from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn


FIND_SQL = """
SELECT
    sr.id,
    sr.run_id,
    sr.code,
    sr.trade_date,
    (
        SELECT MAX(d2.trade_date)
        FROM daily_kline d2
        WHERE d2.code = sr.code
          AND d2.trade_date <= sr.trade_date
    ) AS fallback_trade_date,
    sr.metadata_json
FROM selection_result sr
WHERE sr.trade_date > (
    SELECT MAX(d3.trade_date)
    FROM daily_kline d3
    WHERE d3.code = sr.code
      AND d3.trade_date <= sr.trade_date
)
"""


def main() -> None:
    fixed = 0
    skipped = 0
    details = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(FIND_SQL)
            rows = cursor.fetchall() or []
            for row in rows:
                row_id, run_id, code, wrong_trade_date, fallback_trade_date, metadata_json = row
                if not fallback_trade_date:
                    skipped += 1
                    continue
                metadata = metadata_json or {}
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                raw_metrics = metadata.setdefault("raw_metrics", {})
                raw_metrics["trade_date"] = str(fallback_trade_date)
                cursor.execute(
                    "SELECT id FROM selection_result WHERE trade_date=%s AND strategy_id=(SELECT strategy_id FROM selection_result WHERE id=%s) AND code=%s AND id<>%s LIMIT 1",
                    (fallback_trade_date, row_id, code, row_id),
                )
                duplicate = cursor.fetchone()
                if duplicate:
                    cursor.execute("DELETE FROM selection_result WHERE id=%s", (row_id,))
                    details.append({
                        "id": row_id,
                        "run_id": run_id,
                        "code": code,
                        "old_trade_date": str(wrong_trade_date),
                        "new_trade_date": str(fallback_trade_date),
                        "action": "deleted_duplicate",
                    })
                    fixed += 1
                    continue
                cursor.execute(
                    "UPDATE selection_result SET trade_date=%s, metadata_json=%s WHERE id=%s",
                    (fallback_trade_date, json.dumps(metadata, ensure_ascii=False), row_id),
                )
                fixed += 1
                details.append({
                    "id": row_id,
                    "run_id": run_id,
                    "code": code,
                    "old_trade_date": str(wrong_trade_date),
                    "new_trade_date": str(fallback_trade_date),
                })
    print(json.dumps({"fixed": fixed, "skipped": skipped, "details": details}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
