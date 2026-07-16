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

from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_popularity_update"
LOCK_NAME = "stock_popularity_update_lock"
PRIMARY_SOURCE = "baidu_hot_search"
FALLBACK_SOURCE = "eastmoney_hot_rank"


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


@dataclass
class PopularityFetchResult:
    rows: list[PopularityRow]
    source_used: str
    source_errors: dict[str, str]


class PopularitySourceUnavailable(RuntimeError):
    def __init__(self, source_errors: dict[str, str]) -> None:
        self.source_errors = source_errors
        summary = "; ".join(f"{source}: {error}" for source, error in source_errors.items())
        super().__init__(summary or "all popularity sources unavailable")


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


def extract_baidu_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise PopularitySourceUnavailable({PRIMARY_SOURCE: f"invalid payload type: {type(payload).__name__}"})
    result = payload.get("Result")
    body: Any = None
    if isinstance(result, dict):
        listing = result.get("list")
        if isinstance(listing, dict):
            body = listing.get("body")
        elif isinstance(listing, list):
            body = listing
    elif isinstance(result, list) and result and all(isinstance(item, dict) for item in result):
        body = result
    if not isinstance(body, list) or not body:
        result_code = payload.get("ResultCode")
        raise PopularitySourceUnavailable(
            {PRIMARY_SOURCE: f"upstream unavailable or schema changed (ResultCode={result_code}, Result={type(result).__name__})"}
        )
    return [item for item in body if isinstance(item, dict)]


def fetch_baidu_rows(now: datetime) -> list[PopularityRow]:
    import requests

    response = requests.get(
        "https://finance.pae.baidu.com/selfselect/listsugrecomm",
        params={
            "bizType": "wisexmlnew",
            "dsp": "iphone",
            "product": "search",
            "style": "tablelist",
            "market": "ab",
            "type": "今日",
            "day": now.strftime("%Y%m%d"),
            "hour": str(now.hour),
            "pn": "0",
            "rn": "12",
            "finClientType": "pc",
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10,
    )
    response.raise_for_status()
    items = extract_baidu_items(response.json())
    aliases = stock_alias_map()
    trade_date = now.date().isoformat()
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    quote_minute = minute_floor(now).strftime("%Y-%m-%d %H:%M:%S")
    total = len(items)
    rows: list[PopularityRow] = []
    seen: set[str] = set()
    for index, item in enumerate(items):
        matched = match_stock(item.get("name") or item.get("名称/代码"), aliases)
        if not matched:
            continue
        code, name = matched
        if code in seen:
            continue
        seen.add(code)
        rank_no = index + 1
        source_score = parse_float(item.get("heat") if item.get("heat") is not None else item.get("综合热度"))
        pct_chg = parse_float(item.get("pxChangeRate") if item.get("pxChangeRate") is not None else item.get("涨跌幅"))
        rows.append(
            PopularityRow(
                code=code,
                name=name,
                trade_date=trade_date,
                quote_time=quote_time,
                quote_minute=quote_minute,
                source=PRIMARY_SOURCE,
                source_rank=rank_no,
                source_score=source_score,
                pct_chg=pct_chg,
                popularity_score=score_from_rank(rank_no, source_score, total),
                raw_json={str(k): str(v) for k, v in item.items()},
            )
        )
    if not rows:
        raise PopularitySourceUnavailable({PRIMARY_SOURCE: "response contained no stock rows matching the local universe"})
    return rows


def fetch_eastmoney_rows(now: datetime) -> list[PopularityRow]:
    import requests

    aliases = stock_alias_map()
    items: list[dict[str, Any]] = []
    last_error = "empty response"
    for attempt in range(2):
        try:
            response = requests.post(
                "https://emappdata.eastmoney.com/stockrank/getAllCurrentList",
                json={
                    "appId": "appId01",
                    "globalId": "786e4c21-70dc-435a-93bb-38",
                    "marketType": "",
                    "pageNo": 1,
                    "pageSize": 100,
                },
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, list) or not data:
                raise ValueError(f"invalid data shape: {type(data).__name__}")
            items = [item for item in data if isinstance(item, dict)]
            if items:
                break
            raise ValueError("empty rank items")
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:260]}"
            if attempt == 0:
                time.sleep(0.5)
    if not items:
        raise PopularitySourceUnavailable({FALLBACK_SOURCE: last_error})
    trade_date = now.date().isoformat()
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    quote_minute = minute_floor(now).strftime("%Y-%m-%d %H:%M:%S")
    total = len(items)
    rows: list[PopularityRow] = []
    seen: set[str] = set()
    for index, item in enumerate(items):
        matched = match_stock(item.get("sc") or item.get("code"), aliases)
        if not matched:
            continue
        code, name = matched
        if code in seen:
            continue
        seen.add(code)
        rank_value = parse_float(item.get("rk") if item.get("rk") is not None else item.get("当前排名"))
        rank_no = int(rank_value) if rank_value is not None and rank_value > 0 else index + 1
        rows.append(
            PopularityRow(
                code=code,
                name=name,
                trade_date=trade_date,
                quote_time=quote_time,
                quote_minute=quote_minute,
                source=FALLBACK_SOURCE,
                source_rank=rank_no,
                source_score=None,
                pct_chg=None,
                popularity_score=score_from_rank(rank_no, None, total),
                raw_json={str(k): str(v) for k, v in item.items()},
            )
        )
    if not rows:
        raise PopularitySourceUnavailable({FALLBACK_SOURCE: "response contained no stock rows matching the local universe"})
    return rows


def fetch_popularity_rows(now: datetime) -> PopularityFetchResult:
    source_errors: dict[str, str] = {}
    for source, fetcher in (
        (PRIMARY_SOURCE, fetch_baidu_rows),
        (FALLBACK_SOURCE, fetch_eastmoney_rows),
    ):
        try:
            rows = fetcher(now)
            return PopularityFetchResult(rows=rows, source_used=source, source_errors=source_errors)
        except Exception as exc:
            nested_errors = getattr(exc, "source_errors", None)
            if isinstance(nested_errors, dict):
                source_errors.update({str(key): str(value)[:300] for key, value in nested_errors.items()})
            else:
                source_errors[source] = f"{type(exc).__name__}: {str(exc)[:300]}"
    raise PopularitySourceUnavailable(source_errors)


def save_rows(rows: list[PopularityRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "intraday_rows": 0, "deleted_stale_snapshot_rows": 0, "deleted_old_rows": 0}
    current_codes = sorted({row.code for row in rows})
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
            snapshot_placeholders = ",".join(["%s"] * len(current_codes))
            cursor.execute(
                f"DELETE FROM stock_popularity_snapshot WHERE code NOT IN ({snapshot_placeholders})",
                tuple(current_codes),
            )
            deleted_stale_snapshot_rows = cursor.rowcount
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
    return {
        "snapshot_rows": snapshot_rows,
        "intraday_rows": intraday_rows,
        "deleted_stale_snapshot_rows": deleted_stale_snapshot_rows,
        "deleted_old_rows": deleted_old_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retention-days", type=int, default=3)
    args = parser.parse_args()

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    now = datetime.now()
    run_id = f"stock_popularity_{now.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    started = time.time()
    try:
        logger.start(
            TASK_NAME,
            run_id,
            {"sources": [PRIMARY_SOURCE, FALLBACK_SOURCE], "retention_days": args.retention_days},
        )
        fetch_result = fetch_popularity_rows(datetime.now())
        rows = fetch_result.rows
        db_result = save_rows(rows, retention_days=args.retention_days)
        status = "partial_success" if fetch_result.source_errors else "success"
        payload = {
            "run_id": run_id,
            "status": status,
            "source_used": fetch_result.source_used,
            "source_errors": fetch_result.source_errors,
            "rows": len(rows),
            "elapsed_seconds": round(time.time() - started, 2),
            "top": [r.__dict__ for r in rows[:5]],
            **db_result,
        }
        logger.finish(
            TASK_NAME,
            run_id,
            status,
            f"stock popularity updated via {fetch_result.source_used}, rows={len(rows)}",
            payload,
        )
        print(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {
            "run_id": run_id,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
            "source_errors": getattr(exc, "source_errors", None),
        }
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(json.dumps({"status": "warning", "reason": "release_lock_failed", "error": release_error}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    main()
