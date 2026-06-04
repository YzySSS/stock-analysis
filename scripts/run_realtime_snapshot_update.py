from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.orchestration.realtime_schema import ensure_realtime_schema
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_realtime_snapshot_update"
LOCK_NAME = "stock_realtime_snapshot_update_lock"
STATE_FILE = PROJECT_ROOT / "logs" / "realtime_snapshot_state.json"
SOURCE = "akshare_stock_zh_a_spot"


@dataclass
class RealtimeRow:
    code: str
    source_code: str
    name: str | None
    trade_date: str
    quote_time: str
    quote_minute: str
    latest_price: float | None
    change_amount: float | None
    pct_chg: float | None
    bid_price: float | None
    ask_price: float | None
    pre_close: float | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    volume: int | None
    amount: float | None


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"consecutive_failures": 0, "degraded_until": None, "last_success_at": None}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def is_trading_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (dtime(9, 15) <= t <= dtime(11, 35)) or (dtime(12, 55) <= t <= dtime(15, 5))


def should_skip_for_degrade(now: datetime, state: dict) -> tuple[bool, str | None]:
    degraded_until = parse_dt(state.get("degraded_until"))
    if degraded_until and now < degraded_until:
        return True, f"degraded until {degraded_until.isoformat(timespec='seconds')}"
    return False, None


def normalize_code(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text.startswith(("sh", "sz", "bj")) and len(text) >= 8:
        return f"{text[:2]}.{text[-6:]}"
    digits = "".join(ch for ch in text if ch.isdigit())
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


def parse_quote_datetime(value: Any, fallback: datetime) -> datetime:
    raw = str(value or "").strip()
    for fmt in ["%Y-%m-%d %H:%M:%S", "%H:%M:%S", "%H:%M"]:
        try:
            parsed = datetime.strptime(raw, fmt)
            if fmt.startswith("%Y"):
                return parsed
            return datetime.combine(fallback.date(), parsed.time())
        except Exception:
            pass
    return fallback


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def fetch_spot_rows(ak_module: Any, attempts: int, retry_seconds: float):
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return ak_module.stock_zh_a_spot()
        except Exception as exc:
            last_error = exc
            if attempt >= max(1, attempts):
                raise
            time.sleep(max(0, retry_seconds))
    if last_error:
        raise last_error
    raise RuntimeError("stock_zh_a_spot returned no data")


def convert_rows(df, now: datetime) -> list[RealtimeRow]:
    rows: list[RealtimeRow] = []
    for _, item in df.iterrows():
        code = normalize_code(item.get("代码"))
        if not code:
            continue
        quote_dt = parse_quote_datetime(item.get("时间戳"), now)
        rows.append(
            RealtimeRow(
                code=code,
                source_code=str(item.get("代码") or "")[:16],
                name=str(item.get("名称") or "")[:64] or None,
                trade_date=quote_dt.date().isoformat(),
                quote_time=quote_dt.strftime("%Y-%m-%d %H:%M:%S"),
                quote_minute=minute_floor(quote_dt).strftime("%Y-%m-%d %H:%M:%S"),
                latest_price=to_float(item.get("最新价")),
                change_amount=to_float(item.get("涨跌额")),
                pct_chg=to_float(item.get("涨跌幅")),
                bid_price=to_float(item.get("买入")),
                ask_price=to_float(item.get("卖出")),
                pre_close=to_float(item.get("昨收")),
                open_price=to_float(item.get("今开")),
                high_price=to_float(item.get("最高")),
                low_price=to_float(item.get("最低")),
                volume=to_int(item.get("成交量")),
                amount=to_float(item.get("成交额")),
            )
        )
    return rows


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


def save_rows(rows: list[RealtimeRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "intraday_rows": 0, "deleted_old_rows": 0}

    values = [
        (
            r.code,
            r.source_code,
            r.name,
            r.trade_date,
            r.quote_time,
            r.latest_price,
            r.change_amount,
            r.pct_chg,
            r.bid_price,
            r.ask_price,
            r.pre_close,
            r.open_price,
            r.high_price,
            r.low_price,
            r.volume,
            r.amount,
            SOURCE,
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
            r.change_amount,
            r.pct_chg,
            r.bid_price,
            r.ask_price,
            r.pre_close,
            r.open_price,
            r.high_price,
            r.low_price,
            r.volume,
            r.amount,
            SOURCE,
        )
        for r in rows
    ]

    snapshot_sql = """
    INSERT INTO stock_realtime_snapshot (
        code, source_code, name, trade_date, quote_time, latest_price, change_amount, pct_chg,
        bid_price, ask_price, pre_close, open_price, high_price, low_price, volume, amount, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code), name=VALUES(name), trade_date=VALUES(trade_date), quote_time=VALUES(quote_time),
        latest_price=VALUES(latest_price), change_amount=VALUES(change_amount), pct_chg=VALUES(pct_chg),
        bid_price=VALUES(bid_price), ask_price=VALUES(ask_price), pre_close=VALUES(pre_close), open_price=VALUES(open_price),
        high_price=VALUES(high_price), low_price=VALUES(low_price), volume=VALUES(volume), amount=VALUES(amount), source=VALUES(source)
    """
    intraday_sql = """
    INSERT INTO stock_realtime_intraday (
        code, source_code, name, trade_date, quote_time, quote_minute, latest_price, change_amount, pct_chg,
        bid_price, ask_price, pre_close, open_price, high_price, low_price, volume, amount, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code), name=VALUES(name), quote_time=VALUES(quote_time), latest_price=VALUES(latest_price),
        change_amount=VALUES(change_amount), pct_chg=VALUES(pct_chg), bid_price=VALUES(bid_price), ask_price=VALUES(ask_price),
        pre_close=VALUES(pre_close), open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price),
        volume=VALUES(volume), amount=VALUES(amount), source=VALUES(source)
    """
    today_text = date.today().isoformat()
    latest_trade_date = max((r.trade_date for r in rows if r.trade_date), default=today_text)
    calendar_cutoff = (date.today() - timedelta(days=max(retention_days - 1, 0))).isoformat()
    if retention_days <= 1 and latest_trade_date <= today_text:
        cutoff = max(calendar_cutoff, latest_trade_date)
    else:
        cutoff = calendar_cutoff
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(snapshot_sql, values)
            snapshot_rows = cursor.rowcount
            cursor.executemany(intraday_sql, intraday_values)
            intraday_rows = cursor.rowcount
            cursor.execute("DELETE FROM stock_realtime_intraday WHERE trade_date < %s", (cutoff,))
            deleted_old_rows = cursor.rowcount
    return {
        "snapshot_rows": snapshot_rows,
        "intraday_rows": intraday_rows,
        "deleted_old_rows": deleted_old_rows,
        "retention_cutoff": cutoff,
        "latest_trade_date": latest_trade_date,
    }


def mark_success(state: dict, now: datetime) -> dict:
    state.update({
        "consecutive_failures": 0,
        "degraded_until": None,
        "last_success_at": now.isoformat(timespec="seconds"),
        "last_error": None,
    })
    save_state(state)
    return state


def mark_failure(state: dict, now: datetime, error: Exception, threshold: int, degraded_minutes: int) -> dict:
    failures = int(state.get("consecutive_failures") or 0) + 1
    state["consecutive_failures"] = failures
    state["last_error"] = f"{type(error).__name__}: {str(error)[:300]}"
    state["last_failed_at"] = now.isoformat(timespec="seconds")
    if failures >= threshold:
        state["degraded_until"] = (now + timedelta(minutes=degraded_minutes)).isoformat(timespec="seconds")
    save_state(state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="ignore trading time and degrade window")
    parser.add_argument("--retention-days", type=int, default=1, help="intraday history retention days; default keeps today only")
    parser.add_argument("--failure-threshold", type=int, default=3)
    parser.add_argument("--degraded-minutes", type=int, default=5)
    parser.add_argument("--fetch-attempts", type=int, default=2, help="retry AkShare realtime fetch for transient source errors")
    parser.add_argument("--fetch-retry-seconds", type=float, default=2.0)
    args = parser.parse_args()

    ensure_realtime_schema()
    now = datetime.now()
    run_id = f"realtime_snapshot_{now.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    state = load_state()

    if not args.force and not is_trading_time(now):
        payload = {"reason": "outside_trading_time", "now": now.isoformat(timespec="seconds")}
        print(json.dumps({"status": "skipped", **payload}, ensure_ascii=False))
        return

    if not args.force:
        should_skip, reason = should_skip_for_degrade(now, state)
        if should_skip:
            payload = {"reason": reason, "state": state}
            print(json.dumps({"status": "skipped", **payload}, ensure_ascii=False))
            return

    if not acquire_lock():
        payload = {"reason": "previous_run_still_running"}
        print(json.dumps({"status": "skipped", **payload}, ensure_ascii=False))
        return

    logger.start(TASK_NAME, run_id, {"retention_days": args.retention_days, "state": state})
    started = time.time()
    try:
        import akshare as ak

        df = fetch_spot_rows(ak, args.fetch_attempts, args.fetch_retry_seconds)
        rows = convert_rows(df, datetime.now())
        db_result = save_rows(rows, retention_days=args.retention_days)
        elapsed = round(time.time() - started, 2)
        state = mark_success(state, datetime.now())
        latest_quote_time = max((r.quote_time for r in rows), default=None)
        payload = {
            "run_id": run_id,
            "status": "success",
            "rows": len(rows),
            "elapsed_seconds": elapsed,
            "latest_quote_time": latest_quote_time,
            "retention_days": args.retention_days,
            **db_result,
        }
        logger.finish(TASK_NAME, run_id, "success", f"realtime snapshot updated, rows={len(rows)}, elapsed={elapsed}s", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        elapsed = round(time.time() - started, 2)
        state = mark_failure(state, datetime.now(), exc, args.failure_threshold, args.degraded_minutes)
        payload = {
            "run_id": run_id,
            "status": "failed",
            "elapsed_seconds": elapsed,
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
            "state": state,
        }
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_lock()


if __name__ == "__main__":
    main()
