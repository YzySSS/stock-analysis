from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.orchestration.stock_popularity_schema import ensure_stock_popularity_schema
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_popularity_update"
LOCK_NAME = "stock_popularity_update_lock"
SOURCE = "akshare_baidu_hot_search"


@dataclass
class PopularityRow:
    code: str
    name: str
    trade_date: str
    quote_time: str
    quote_minute: str
    source: str
    source_rank: int | None
    source_score: float | None
    pct_chg: float | None
    popularity_score: float | None
    raw_json: dict[str, Any]


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", "", text).upper()


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace("%", "").replace(",", "").strip()
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def acquire_lock() -> bool:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS locked", (LOCK_NAME,))
            row = cursor.fetchone() or {}
            return int(row.get("locked") or 0) == 1


def release_lock() -> None:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))


def stock_alias_map() -> dict[str, tuple[str, str]]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code, name
                FROM stock_basic
                WHERE is_delisted = 0 AND instrument_type = 'stock'
                """
            )
            rows = cursor.fetchall() or []
    aliases: dict[str, tuple[str, str]] = {}
    for row in rows:
        code = str(row.get("code") or "")
        name = str(row.get("name") or "")
        if not code or not name:
            continue
        plain_code = code.split(".")[-1]
        for alias in {name, normalize_text(name), plain_code, f"{plain_code}{normalize_text(name)}"}:
            key = normalize_text(alias)
            if key:
                aliases[key] = (code, name)
    return aliases


def match_stock(raw_name: Any, aliases: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    text = normalize_text(raw_name)
    if not text:
        return None
    if text in aliases:
        return aliases[text]
    match = re.search(r"([0-9]{6})", text)
    if match and match.group(1) in aliases:
        return aliases[match.group(1)]
    # Baidu commonly returns values like 京东方A without market suffix.
    for key, stock in aliases.items():
        if len(key) >= 3 and key in text:
            return stock
    return None


def score_from_rank(rank_no: int | None, source_score: float | None, total: int) -> float | None:
    rank_score = None
    if rank_no and rank_no > 0:
        rank_score = max(0.0, 100.0 - (rank_no - 1) * (70.0 / max(total - 1, 1)))
    heat_score = None
    if source_score is not None and source_score > 0:
        # Compress large hot-search counts while keeping the top tail visible.
        heat_score = min(100.0, math.log10(source_score + 1) / 6.0 * 100.0)
    if rank_score is None:
        return round(heat_score, 4) if heat_score is not None else None
    if heat_score is None:
        return round(rank_score, 4)
    return round(rank_score * 0.72 + heat_score * 0.28, 4)


def fetch_baidu_rows(now: datetime) -> list[PopularityRow]:
    import akshare as ak

    aliases = stock_alias_map()
    trade_date = now.date().isoformat()
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    quote_minute = minute_floor(now).strftime("%Y-%m-%d %H:%M:%S")
    df = ak.stock_hot_search_baidu(symbol="A股", date=now.strftime("%Y%m%d"))
    total = len(df.index)
    rows: list[PopularityRow] = []
    seen: set[str] = set()
    for index, item in df.iterrows():
        matched = match_stock(item.get("名称/代码"), aliases)
        if not matched:
            continue
        code, name = matched
        if code in seen:
            continue
        seen.add(code)
        rank_no = int(index) + 1
        source_score = parse_float(item.get("综合热度"))
        pct_chg = parse_float(item.get("涨跌幅"))
        rows.append(
            PopularityRow(
                code=code,
                name=name,
                trade_date=trade_date,
                quote_time=quote_time,
                quote_minute=quote_minute,
                source=SOURCE,
                source_rank=rank_no,
                source_score=source_score,
                pct_chg=pct_chg,
                popularity_score=score_from_rank(rank_no, source_score, total),
                raw_json={str(k): str(v) for k, v in item.to_dict().items()},
            )
        )
    return rows


def save_rows(rows: list[PopularityRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "intraday_rows": 0, "deleted_old_rows": 0}
    values = [
        (
            r.code,
            r.name,
            r.trade_date,
            r.quote_time,
            r.source,
            r.source_rank,
            r.source_score,
            r.pct_chg,
            r.popularity_score,
            json.dumps(r.raw_json, ensure_ascii=False),
        )
        for r in rows
    ]
    intraday_values = [
        (
            r.code,
            r.name,
            r.trade_date,
            r.quote_time,
            r.quote_minute,
            r.source,
            r.source_rank,
            r.source_score,
            r.pct_chg,
            r.popularity_score,
            json.dumps(r.raw_json, ensure_ascii=False),
        )
        for r in rows
    ]
    cutoff = (date.today() - timedelta(days=max(retention_days - 1, 0))).isoformat()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO stock_popularity_snapshot (
                    code, name, trade_date, quote_time, source, source_rank, source_score,
                    pct_chg, popularity_score, raw_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), trade_date=VALUES(trade_date), quote_time=VALUES(quote_time),
                    source=VALUES(source), source_rank=VALUES(source_rank), source_score=VALUES(source_score),
                    pct_chg=VALUES(pct_chg), popularity_score=VALUES(popularity_score), raw_json=VALUES(raw_json)
                """,
                values,
            )
            snapshot_rows = cursor.rowcount
            cursor.executemany(
                """
                INSERT INTO stock_popularity_intraday (
                    code, name, trade_date, quote_time, quote_minute, source, source_rank, source_score,
                    pct_chg, popularity_score, raw_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), quote_time=VALUES(quote_time), source_rank=VALUES(source_rank),
                    source_score=VALUES(source_score), pct_chg=VALUES(pct_chg),
                    popularity_score=VALUES(popularity_score), raw_json=VALUES(raw_json)
                """,
                intraday_values,
            )
            intraday_rows = cursor.rowcount
            cursor.execute("DELETE FROM stock_popularity_intraday WHERE trade_date < %s", (cutoff,))
            deleted_old_rows = cursor.rowcount
    return {"snapshot_rows": snapshot_rows, "intraday_rows": intraday_rows, "deleted_old_rows": deleted_old_rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retention-days", type=int, default=3)
    args = parser.parse_args()

    ensure_stock_popularity_schema()
    if not acquire_lock():
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    now = datetime.now()
    run_id = f"stock_popularity_{now.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"source": SOURCE, "retention_days": args.retention_days})
    started = time.time()
    try:
        rows = fetch_baidu_rows(datetime.now())
        db_result = save_rows(rows, retention_days=args.retention_days)
        payload = {
            "run_id": run_id,
            "status": "success",
            "source": SOURCE,
            "rows": len(rows),
            "elapsed_seconds": round(time.time() - started, 2),
            "top": [r.__dict__ for r in rows[:5]],
            **db_result,
        }
        logger.finish(TASK_NAME, run_id, "success", f"stock popularity updated, rows={len(rows)}", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {"run_id": run_id, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)[:500]}
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_lock()


if __name__ == "__main__":
    main()
