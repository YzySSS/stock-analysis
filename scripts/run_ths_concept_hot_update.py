#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402

TASK_NAME = "ths_concept_hot_update"
LOCK_NAME = "ths_concept_hot_update_lock"
SOURCE = "akshare_stock_board_concept_summary_ths"


@dataclass
class ThsConceptHotRow:
    concept_name: str
    concept_code: str | None
    summary_date: str | None
    quote_time: str
    driver_event: str | None
    leading_stock: str | None
    member_count: int | None
    ths_score: float


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def to_int(value: Any) -> int | None:
    num = to_float(value)
    return int(num) if num is not None else None


def clean_text(value: Any, limit: int = 512) -> str | None:
    text = str(value or "").strip()
    if text in {"--", "-", "nan", "None"}:
        return None
    return text[:limit] if text else None


def row_value(item: Any, candidates: list[str]) -> Any:
    for key in candidates:
        value = item.get(key)
        if value is not None:
            return value
    return None


def parse_date(value: Any) -> str | None:
    text = clean_text(value, 32)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def score_row(summary_date: str | None, driver_event: str | None, leading_stock: str | None, member_count: int | None, now: datetime) -> float:
    score = 44.0
    if summary_date:
        days = max((now.date() - date.fromisoformat(summary_date)).days, 0)
        if days <= 1:
            score += 34
        elif days <= 7:
            score += 26
        elif days <= 30:
            score += 18
        elif days <= 90:
            score += 10
        else:
            score += 3
    if driver_event:
        score += 10
    if leading_stock:
        score += 6
    if member_count:
        score += min(member_count / 25, 10)
    return round(max(0, min(score, 100)), 2)


def fetch_rows(now: datetime) -> list[ThsConceptHotRow]:
    import akshare as ak

    summary_error: str | None = None
    try:
        summary_df = ak.stock_board_concept_summary_ths()
    except Exception as exc:
        summary_error = f"{type(exc).__name__}: {str(exc)[:200]}"
        summary_df = None
    name_df = ak.stock_board_concept_name_ths()
    code_by_name = {
        clean_text(row_value(item, ["name", "概念名称", "板块名称", "名称"]), 128): clean_text(row_value(item, ["code", "概念代码", "代码"]), 32)
        for _, item in name_df.iterrows()
        if clean_text(row_value(item, ["name", "概念名称", "板块名称", "名称"]), 128)
    }
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    rows: list[ThsConceptHotRow] = []
    source_df = summary_df if summary_df is not None and not getattr(summary_df, "empty", False) else name_df
    for _, item in source_df.iterrows():
        name = clean_text(row_value(item, ["概念名称", "name", "板块名称", "名称"]), 128)
        if not name:
            continue
        summary_date = parse_date(row_value(item, ["日期", "summary_date", "更新时间", "时间"]))
        driver_event = clean_text(row_value(item, ["驱动事件", "事件", "原因"]), 512)
        leading_stock = clean_text(row_value(item, ["龙头股", "领涨股"]), 128)
        member_count = to_int(row_value(item, ["成分股数量", "成份股数量", "股票数量", "公司家数"]))
        rows.append(
            ThsConceptHotRow(
                concept_name=name,
                concept_code=code_by_name.get(name),
                summary_date=summary_date,
                quote_time=quote_time,
                driver_event=driver_event,
                leading_stock=leading_stock,
                member_count=member_count,
                ths_score=score_row(summary_date, driver_event, leading_stock, member_count, now),
            )
        )
    if summary_error:
        print(json.dumps({"status": "summary_source_warning", "error": summary_error}, ensure_ascii=False), file=sys.stderr)
    return rows


def save_rows(rows: list[ThsConceptHotRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "deleted_old_rows": 0}
    values = [
        (
            row.concept_name,
            row.concept_code,
            row.summary_date,
            row.quote_time,
            row.driver_event,
            row.leading_stock,
            row.member_count,
            row.ths_score,
            SOURCE,
        )
        for row in rows
    ]
    sql = """
    INSERT INTO ths_concept_hot_snapshot (
        concept_name, concept_code, summary_date, quote_time, driver_event,
        leading_stock, member_count, ths_score, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        concept_code=VALUES(concept_code),
        summary_date=VALUES(summary_date),
        quote_time=VALUES(quote_time),
        driver_event=VALUES(driver_event),
        leading_stock=VALUES(leading_stock),
        member_count=VALUES(member_count),
        ths_score=VALUES(ths_score),
        source=VALUES(source)
    """
    cutoff = (date.today() - timedelta(days=max(retention_days - 1, 0))).isoformat()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
            snapshot_rows = cursor.rowcount
            cursor.execute("DELETE FROM ths_concept_hot_snapshot WHERE DATE(quote_time) < %s", (cutoff,))
            deleted_old_rows = cursor.rowcount
    return {"snapshot_rows": snapshot_rows, "deleted_old_rows": deleted_old_rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retention-days", type=int, default=3)
    args = parser.parse_args()

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    now = datetime.now()
    run_id = f"ths_concept_hot_{now.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    started = time.time()
    try:
        logger.start(TASK_NAME, run_id, {"retention_days": args.retention_days})
        rows = fetch_rows(datetime.now())
        db_result = save_rows(rows, retention_days=args.retention_days)
        elapsed = round(time.time() - started, 2)
        payload = {
            "run_id": run_id,
            "status": "success",
            "rows": len(rows),
            "elapsed_seconds": elapsed,
            **db_result,
        }
        logger.finish(TASK_NAME, run_id, "success", f"ths concept hot updated, rows={len(rows)}, elapsed={elapsed}s", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        payload = {"run_id": run_id, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)[:500]}
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(json.dumps({"status": "warning", "reason": "release_lock_failed", "error": release_error}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    main()
