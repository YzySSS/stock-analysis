from __future__ import annotations

import argparse
import json
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "pe_baidu_valuation_backfill"
SOURCE = "akshare_stock_zh_valuation_baidu_pe_ttm"


@dataclass
class PeRecord:
    code: str
    name: str
    pe: float | None
    pe_date: str | None
    error: str | None = None


def plain_code(code: str) -> str:
    return str(code or "").split(".")[-1]


def to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def fetch_missing_codes(limit: int, include_nonpositive_eps: bool) -> list[dict[str, Any]]:
    sql = """
    SELECT code, name, eps
    FROM stock_basic
    WHERE is_delisted = 0
      AND instrument_type = 'stock'
      AND pe_tushare IS NULL
    """
    if not include_nonpositive_eps:
        sql += " AND (eps IS NULL OR eps > 0)"
    sql += " ORDER BY (eps IS NULL), code"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall() or [])


def fetch_pe(row: dict[str, Any]) -> PeRecord:
    import akshare as ak

    code = str(row.get("code") or "")
    name = str(row.get("name") or "")
    try:
        df = ak.stock_zh_valuation_baidu(symbol=plain_code(code), indicator="市盈率(TTM)", period="近一年")
        if df is None or df.empty:
            return PeRecord(code=code, name=name, pe=None, pe_date=None, error="empty_source")
        df = df.dropna(subset=["value"])
        if df.empty:
            return PeRecord(code=code, name=name, pe=None, pe_date=None, error="empty_pe")
        latest = df.iloc[-1]
        pe = to_float(latest.get("value"))
        pe_date = str(latest.get("date")) if latest.get("date") is not None else None
        if pe is None or pe <= 0 or pe > 10000:
            return PeRecord(code=code, name=name, pe=None, pe_date=pe_date, error=f"invalid_pe:{pe}")
        return PeRecord(code=code, name=name, pe=round(pe, 4), pe_date=pe_date)
    except Exception as exc:
        return PeRecord(code=code, name=name, pe=None, pe_date=None, error=f"{type(exc).__name__}:{str(exc)[:180]}")


def save_records(records: list[PeRecord]) -> int:
    valid = [(r.pe, r.code) for r in records if r.pe is not None]
    if not valid:
        return 0
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE stock_basic
                SET pe_tushare = %s,
                    valuation_updated_at = NOW()
                WHERE code = %s AND pe_tushare IS NULL
                """,
                valid,
            )
            return cursor.rowcount


def coverage() -> dict[str, int]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*) AS active_stock,
                  SUM(pe_tushare IS NOT NULL) AS pe_non_null,
                  SUM(pe_tushare IS NULL) AS pe_missing,
                  SUM(pe_tushare IS NULL AND eps > 0) AS pe_missing_positive_eps,
                  SUM(pe_tushare IS NULL AND eps <= 0) AS pe_missing_nonpositive_eps
                FROM stock_basic
                WHERE is_delisted = 0 AND instrument_type = 'stock'
                """
            )
            row = cursor.fetchone() or {}
            return {key: int(value or 0) for key, value in row.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing stock_basic.pe_tushare from Baidu PE TTM valuation.")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--include-nonpositive-eps", action="store_true")
    args = parser.parse_args()

    before = coverage()
    rows = fetch_missing_codes(limit=args.limit, include_nonpositive_eps=args.include_nonpositive_eps)
    run_id = f"pe_baidu_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"limit": args.limit, "workers": args.workers, "source": SOURCE, "before": before})
    started = time.time()

    records: list[PeRecord] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {executor.submit(fetch_pe, row): row for row in rows}
        for future in as_completed(future_map):
            records.append(future.result())

    updated = save_records(records)
    after = coverage()
    errors: dict[str, int] = {}
    for record in records:
        if record.error:
            errors[record.error.split(":", 1)[0]] = errors.get(record.error.split(":", 1)[0], 0) + 1
    payload = {
        "run_id": run_id,
        "status": "success",
        "source": SOURCE,
        "scanned": len(rows),
        "fetched": len([r for r in records if r.pe is not None]),
        "updated": updated,
        "elapsed_seconds": round(time.time() - started, 2),
        "before": before,
        "after": after,
        "errors": errors,
        "sample": [r.__dict__ for r in sorted(records, key=lambda item: item.code)[:10]],
    }
    logger.finish(TASK_NAME, run_id, "success", f"PE Baidu valuation backfill updated={updated}", payload)
    print(json.dumps(payload, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
