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

from app.orchestration.market_fund_flow_schema import ensure_market_fund_flow_schema
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "market_fund_flow_update"
LOCK_NAME = "market_fund_flow_update_lock"
SOURCE = "akshare_stock_fund_flow_industry_concept"
SOURCE_UNIT = "亿元"


@dataclass
class FundFlowRow:
    sector_type: str
    sector_name: str
    trade_date: str
    quote_time: str
    quote_minute: str
    rank_no: int | None
    sector_index: float | None
    pct_chg: float | None
    inflow_amount: float | None
    outflow_amount: float | None
    net_amount: float | None
    company_count: int | None
    leading_stock: str | None
    leading_stock_pct_chg: float | None
    leading_stock_price: float | None


def is_trading_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (dtime(9, 25) <= t <= dtime(11, 35)) or (dtime(12, 55) <= t <= dtime(15, 8))


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


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


def clean_text(value: Any, limit: int = 128) -> str | None:
    text = str(value or "").strip()
    return text[:limit] if text else None


def row_value(item: Any, candidates: list[str]) -> Any:
    for key in candidates:
        value = item.get(key)
        if value is not None:
            return value
    return None


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


def fetch_rows(now: datetime, include_concept: bool = True) -> list[FundFlowRow]:
    import akshare as ak

    quote_minute = minute_floor(now).strftime("%Y-%m-%d %H:%M:%S")
    quote_time = now.strftime("%Y-%m-%d %H:%M:%S")
    trade_date = now.date().isoformat()
    fetchers: list[tuple[str, Any]] = [("industry", ak.stock_fund_flow_industry)]
    if include_concept:
        fetchers.append(("concept", ak.stock_fund_flow_concept))

    rows: list[FundFlowRow] = []
    errors: list[str] = []
    for sector_type, fetcher in fetchers:
        try:
            df = fetcher(symbol="即时")
        except Exception as exc:
            errors.append(f"{sector_type}: {type(exc).__name__}: {str(exc)[:160]}")
            continue
        if df is None or getattr(df, "empty", False):
            errors.append(f"{sector_type}: empty response")
            continue
        for _, item in df.iterrows():
            name = clean_text(row_value(item, ["行业", "概念", "板块", "名称"]))
            if not name:
                continue
            rows.append(
                FundFlowRow(
                    sector_type=sector_type,
                    sector_name=name,
                    trade_date=trade_date,
                    quote_time=quote_time,
                    quote_minute=quote_minute,
                    rank_no=to_int(row_value(item, ["序号", "排名"])),
                    sector_index=to_float(row_value(item, ["行业指数", "板块指数", "指数"])),
                    pct_chg=to_float(row_value(item, ["行业-涨跌幅", "涨跌幅", "板块涨跌幅"])),
                    inflow_amount=to_float(row_value(item, ["流入资金", "主力流入", "净流入"])),
                    outflow_amount=to_float(row_value(item, ["流出资金", "主力流出"])),
                    net_amount=to_float(row_value(item, ["净额", "净流入", "主力净流入"])),
                    company_count=to_int(row_value(item, ["公司家数", "股票家数", "成分股数量"])),
                    leading_stock=clean_text(row_value(item, ["领涨股", "龙头股"]), 64),
                    leading_stock_pct_chg=to_float(row_value(item, ["领涨股-涨跌幅", "领涨股涨跌幅"])),
                    leading_stock_price=to_float(row_value(item, ["当前价", "领涨股最新价"])),
                )
            )
    if not rows and errors:
        raise RuntimeError("; ".join(errors))
    if errors:
        print(json.dumps({"status": "partial_source_warning", "errors": errors}, ensure_ascii=False), file=sys.stderr)
    return rows


def save_rows(rows: list[FundFlowRow], retention_days: int) -> dict:
    if not rows:
        return {"snapshot_rows": 0, "intraday_rows": 0, "deleted_old_rows": 0}

    values = [
        (
            r.sector_type,
            r.sector_name,
            r.trade_date,
            r.quote_time,
            r.rank_no,
            r.sector_index,
            r.pct_chg,
            r.inflow_amount,
            r.outflow_amount,
            r.net_amount,
            r.company_count,
            r.leading_stock,
            r.leading_stock_pct_chg,
            r.leading_stock_price,
            SOURCE,
            SOURCE_UNIT,
        )
        for r in rows
    ]
    intraday_values = [
        (
            r.sector_type,
            r.sector_name,
            r.trade_date,
            r.quote_time,
            r.quote_minute,
            r.rank_no,
            r.sector_index,
            r.pct_chg,
            r.inflow_amount,
            r.outflow_amount,
            r.net_amount,
            r.company_count,
            r.leading_stock,
            r.leading_stock_pct_chg,
            r.leading_stock_price,
            SOURCE,
            SOURCE_UNIT,
        )
        for r in rows
    ]

    snapshot_sql = """
    INSERT INTO market_sector_fund_flow_snapshot (
        sector_type, sector_name, trade_date, quote_time, rank_no, sector_index, pct_chg,
        inflow_amount, outflow_amount, net_amount, company_count, leading_stock,
        leading_stock_pct_chg, leading_stock_price, source, source_unit
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        trade_date=VALUES(trade_date), quote_time=VALUES(quote_time), rank_no=VALUES(rank_no),
        sector_index=VALUES(sector_index), pct_chg=VALUES(pct_chg), inflow_amount=VALUES(inflow_amount),
        outflow_amount=VALUES(outflow_amount), net_amount=VALUES(net_amount), company_count=VALUES(company_count),
        leading_stock=VALUES(leading_stock), leading_stock_pct_chg=VALUES(leading_stock_pct_chg),
        leading_stock_price=VALUES(leading_stock_price), source=VALUES(source), source_unit=VALUES(source_unit)
    """
    intraday_sql = """
    INSERT INTO market_sector_fund_flow_intraday (
        sector_type, sector_name, trade_date, quote_time, quote_minute, rank_no, sector_index, pct_chg,
        inflow_amount, outflow_amount, net_amount, company_count, leading_stock,
        leading_stock_pct_chg, leading_stock_price, source, source_unit
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        quote_time=VALUES(quote_time), rank_no=VALUES(rank_no), sector_index=VALUES(sector_index),
        pct_chg=VALUES(pct_chg), inflow_amount=VALUES(inflow_amount), outflow_amount=VALUES(outflow_amount),
        net_amount=VALUES(net_amount), company_count=VALUES(company_count), leading_stock=VALUES(leading_stock),
        leading_stock_pct_chg=VALUES(leading_stock_pct_chg), leading_stock_price=VALUES(leading_stock_price),
        source=VALUES(source), source_unit=VALUES(source_unit)
    """
    cutoff = (date.today() - timedelta(days=max(retention_days - 1, 0))).isoformat()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(snapshot_sql, values)
            snapshot_rows = cursor.rowcount
            cursor.executemany(intraday_sql, intraday_values)
            intraday_rows = cursor.rowcount
            cursor.execute("DELETE FROM market_sector_fund_flow_intraday WHERE trade_date < %s", (cutoff,))
            deleted_old_rows = cursor.rowcount
    return {"snapshot_rows": snapshot_rows, "intraday_rows": intraday_rows, "deleted_old_rows": deleted_old_rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="ignore trading time")
    parser.add_argument("--retention-days", type=int, default=1, help="intraday history retention days")
    parser.add_argument("--no-concept", action="store_true", help="only fetch industry fund flow")
    args = parser.parse_args()

    ensure_market_fund_flow_schema()
    now = datetime.now()
    if not args.force and not is_trading_time(now):
        print(json.dumps({"status": "skipped", "reason": "outside_trading_time", "now": now.isoformat(timespec="seconds")}, ensure_ascii=False))
        return
    if not acquire_lock():
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    run_id = f"market_fund_flow_{now.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"retention_days": args.retention_days, "include_concept": not args.no_concept})
    started = time.time()
    try:
        rows = fetch_rows(datetime.now(), include_concept=not args.no_concept)
        db_result = save_rows(rows, retention_days=args.retention_days)
        elapsed = round(time.time() - started, 2)
        sector_counts: dict[str, int] = {}
        for row in rows:
            sector_counts[row.sector_type] = sector_counts.get(row.sector_type, 0) + 1
        payload = {
            "run_id": run_id,
            "status": "success",
            "rows": len(rows),
            "sector_counts": sector_counts,
            "elapsed_seconds": elapsed,
            **db_result,
        }
        logger.finish(TASK_NAME, run_id, "success", f"market fund flow updated, rows={len(rows)}, elapsed={elapsed}s", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        payload = {"run_id": run_id, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)[:500]}
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_lock()


if __name__ == "__main__":
    main()
