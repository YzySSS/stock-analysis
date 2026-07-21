from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_realtime_moneyflow_update"
LOCK_NAME = "stock_realtime_moneyflow_update_lock"
SOURCE = "akshare_ths_stock_fund_flow_individual"
SOURCE_UNIT = "元"


@dataclass
class RealtimeMoneyflowRow:
    code: str
    source_code: str
    name: str | None
    trade_date: str
    quote_time: str
    quote_minute: str
    latest_price: float | None
    pct_chg: float | None
    turnover_rate: float | None
    inflow_amount: float | None
    outflow_amount: float | None
    net_amount: float | None
    amount: float | None


def is_trading_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (dtime(9, 25) <= t <= dtime(11, 35)) or (dtime(12, 55) <= t <= dtime(15, 8))


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def clean_text(value: Any, limit: int = 64) -> str | None:
    text = str(value or "").strip()
    return text[:limit] if text else None


def normalize_code(raw: Any) -> str | None:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(digits) < 6:
        return None
    code = digits[-6:]
    if code.startswith(("60", "68", "90")):
        return f"sh.{code}"
    if code.startswith(("00", "30", "20")):
        return f"sz.{code}"
    if code.startswith(("43", "83", "87", "88", "92")):
        return f"bj.{code}"
    return code


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return None if math.isnan(number) or math.isinf(number) else number
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--", "None", "nan"}:
        return None
    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    if "万" in text:
        multiplier = 10_000.0
        text = text.replace("万元", "").replace("万", "")
    elif "亿" in text:
        multiplier = 100_000_000.0
        text = text.replace("亿元", "").replace("亿", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group(0)) * multiplier
    except ValueError:
        return None
    return None if math.isnan(number) or math.isinf(number) else number


def fetch_rows(now: datetime) -> list[RealtimeMoneyflowRow]:
    import akshare as ak

    try:
        df = ak.stock_fund_flow_individual(symbol="即时")
    except Exception as exc:
        raise RuntimeError(f"realtime moneyflow source unavailable: {type(exc).__name__}: {str(exc)[:300]}") from exc
    if df is None or getattr(df, "empty", False):
        raise RuntimeError("realtime moneyflow source returned an empty response")
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    quote_minute = minute_floor(now).strftime("%Y-%m-%d %H:%M:%S")
    trade_date = now.date().isoformat()
    rows: list[RealtimeMoneyflowRow] = []
    for _, item in df.iterrows():
        code = normalize_code(item.get("股票代码"))
        source_code = str(item.get("股票代码") or "").strip()
        if not code or not source_code:
            continue
        rows.append(
            RealtimeMoneyflowRow(
                code=code,
                source_code=source_code[:16],
                name=clean_text(item.get("股票简称")),
                trade_date=trade_date,
                quote_time=quote_time,
                quote_minute=quote_minute,
                latest_price=parse_number(item.get("最新价")),
                pct_chg=parse_number(item.get("涨跌幅")),
                turnover_rate=parse_number(item.get("换手率")),
                inflow_amount=parse_number(item.get("流入资金")),
                outflow_amount=parse_number(item.get("流出资金")),
                net_amount=parse_number(item.get("净额")),
                amount=parse_number(item.get("成交额")),
            )
        )
    return rows


def save_rows(rows: list[RealtimeMoneyflowRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "intraday_rows": 0, "deleted_stale_snapshot_rows": 0, "deleted_old_rows": 0}

    current_trade_date = max(row.trade_date for row in rows)

    values = [
        (
            r.code,
            r.source_code,
            r.name,
            r.trade_date,
            r.quote_time,
            r.latest_price,
            r.pct_chg,
            r.turnover_rate,
            r.inflow_amount,
            r.outflow_amount,
            r.net_amount,
            r.amount,
            SOURCE,
            SOURCE_UNIT,
        )
        for r in rows
    ]
    intraday_values = [
        (
            r.code,
            r.source_code,
            r.name,
            r.trade_date,
            r.quote_time,
            r.quote_minute,
            r.latest_price,
            r.pct_chg,
            r.turnover_rate,
            r.inflow_amount,
            r.outflow_amount,
            r.net_amount,
            r.amount,
            SOURCE,
            SOURCE_UNIT,
        )
        for r in rows
    ]
    snapshot_sql = """
    INSERT INTO stock_realtime_moneyflow_snapshot (
        code, source_code, name, trade_date, quote_time, latest_price, pct_chg, turnover_rate,
        inflow_amount, outflow_amount, net_amount, amount, source, source_unit
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code), name=VALUES(name), trade_date=VALUES(trade_date), quote_time=VALUES(quote_time),
        latest_price=VALUES(latest_price), pct_chg=VALUES(pct_chg), turnover_rate=VALUES(turnover_rate),
        inflow_amount=VALUES(inflow_amount), outflow_amount=VALUES(outflow_amount), net_amount=VALUES(net_amount),
        amount=VALUES(amount), source=VALUES(source), source_unit=VALUES(source_unit)
    """
    intraday_sql = """
    INSERT INTO stock_realtime_moneyflow_intraday (
        code, source_code, name, trade_date, quote_time, quote_minute, latest_price, pct_chg, turnover_rate,
        inflow_amount, outflow_amount, net_amount, amount, source, source_unit
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code), name=VALUES(name), quote_time=VALUES(quote_time), latest_price=VALUES(latest_price),
        pct_chg=VALUES(pct_chg), turnover_rate=VALUES(turnover_rate), inflow_amount=VALUES(inflow_amount),
        outflow_amount=VALUES(outflow_amount), net_amount=VALUES(net_amount), amount=VALUES(amount),
        source=VALUES(source), source_unit=VALUES(source_unit)
    """
    cutoff = (date.today() - timedelta(days=max(retention_days - 1, 0))).isoformat()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(snapshot_sql, values)
            snapshot_rows = cursor.rowcount
            cursor.execute("DELETE FROM stock_realtime_moneyflow_snapshot WHERE trade_date < %s", (current_trade_date,))
            deleted_stale_snapshot_rows = cursor.rowcount
            cursor.executemany(intraday_sql, intraday_values)
            intraday_rows = cursor.rowcount
            cursor.execute("DELETE FROM stock_realtime_moneyflow_intraday WHERE trade_date < %s", (cutoff,))
            deleted_old_rows = cursor.rowcount
    return {
        "snapshot_rows": snapshot_rows,
        "intraday_rows": intraday_rows,
        "deleted_stale_snapshot_rows": deleted_stale_snapshot_rows,
        "deleted_old_rows": deleted_old_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="ignore trading time")
    parser.add_argument("--retention-days", type=int, default=1, help="intraday history retention days")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force and not is_trading_time(now):
        print(json.dumps({"status": "skipped", "reason": "outside_trading_time", "now": now.isoformat(timespec="seconds")}, ensure_ascii=False))
        return
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    run_id = f"stock_realtime_moneyflow_{now.strftime('%Y%m%d_%H%M%S')}"
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
            "latest_quote_time": max((row.quote_time for row in rows), default=None),
            **db_result,
        }
        logger.finish(TASK_NAME, run_id, "success", f"stock realtime moneyflow updated, rows={len(rows)}, elapsed={elapsed}s", payload)
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
