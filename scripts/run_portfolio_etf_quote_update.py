#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.orchestration.realtime_schema import ensure_realtime_schema
from app.shared.db import mysql_conn

SOURCE = "portfolio_etf_quote"


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def compact_code(code: str) -> str:
    return "".join(ch for ch in str(code or "") if ch.isdigit())[-6:]


def held_etf_codes() -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT p.code
                FROM portfolio_position p
                INNER JOIN stock_basic sb ON sb.code = p.code
                WHERE p.is_active = 1
                  AND sb.instrument_type = 'etf'
                ORDER BY p.code
                """
            )
            return [row["code"] for row in cursor.fetchall()]


def fetch_etf_history(code: str, days: int) -> list[dict[str, Any]]:
    symbol = compact_code(code)
    if len(symbol) != 6:
        raise ValueError(f"invalid ETF code: {code}")
    try:
        return fetch_etf_history_akshare(code, symbol, days)
    except Exception:
        try:
            return fetch_etf_history_eastmoney(code, symbol, days)
        except Exception:
            snapshot = fetch_etf_snapshot_tencent(code, symbol)
            return [snapshot] if snapshot else []


def fetch_etf_history_akshare(code: str, symbol: str, days: int) -> list[dict[str, Any]]:
    import akshare as ak

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = datetime(datetime.now().year - 1, 1, 1).strftime("%Y%m%d")
    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
    if df is None or getattr(df, "empty", True):
        return []
    return [
        row
        for _, raw in df.tail(days).iterrows()
        if (
            row := normalize_history_row(
                code=code,
                source_code=symbol,
                trade_date=str(raw.get("日期") or "")[:10],
                open_price=raw.get("开盘"),
                high=raw.get("最高"),
                low=raw.get("最低"),
                close=raw.get("收盘"),
                volume=raw.get("成交量"),
                amount=raw.get("成交额"),
                pct_chg=raw.get("涨跌幅"),
                change_amount=raw.get("涨跌额"),
            )
        )
    ]


def fetch_etf_history_eastmoney(code: str, symbol: str, days: int) -> list[dict[str, Any]]:
    import requests

    market_id = "1" if code.startswith("sh.") else "0"
    response = requests.get(
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        params={
            "secid": f"{market_id}.{symbol}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "0",
            "end": "20500101",
            "lmt": str(max(1, days)),
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    klines = (((payload or {}).get("data") or {}).get("klines") or [])[-days:]
    rows: list[dict[str, Any]] = []
    for item in klines:
        parts = str(item).split(",")
        if len(parts) < 11:
            continue
        row = normalize_history_row(
            code=code,
            source_code=symbol,
            trade_date=parts[0],
            open_price=parts[1],
            close=parts[2],
            high=parts[3],
            low=parts[4],
            volume=parts[5],
            amount=parts[6],
            pct_chg=parts[8],
            change_amount=parts[9],
        )
        if row:
            rows.append(row)
    return rows


def fetch_etf_snapshot_tencent(code: str, symbol: str) -> dict[str, Any] | None:
    import requests

    market = code.split(".", 1)[0].lower()
    response = requests.get(
        f"https://qt.gtimg.cn/q={market}{symbol}",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=8,
    )
    response.raise_for_status()
    text = response.content.decode("gbk", errors="ignore")
    if '="' not in text:
        return None
    raw = text.split('="', 1)[1].split('";', 1)[0]
    parts = raw.split("~")
    if len(parts) < 38:
        return None
    quote_time_raw = parts[30] if len(parts) > 30 else ""
    trade_date = f"{quote_time_raw[:4]}-{quote_time_raw[4:6]}-{quote_time_raw[6:8]}" if len(quote_time_raw) >= 8 else datetime.now().date().isoformat()
    quote_time = (
        f"{trade_date} {quote_time_raw[8:10]}:{quote_time_raw[10:12]}:{quote_time_raw[12:14]}"
        if len(quote_time_raw) >= 14
        else f"{trade_date} 15:00:00"
    )
    price = to_float(parts[3])
    pre_close = to_float(parts[4])
    change_amount = to_float(parts[31]) if len(parts) > 31 else None
    amount_wan = to_float(parts[37]) if len(parts) > 37 else None
    return {
        "code": code,
        "source_code": symbol,
        "trade_date": trade_date,
        "quote_time": quote_time,
        "open": to_float(parts[5]),
        "high": to_float(parts[33]) if len(parts) > 33 else None,
        "low": to_float(parts[34]) if len(parts) > 34 else None,
        "close": price,
        "volume": to_int(parts[36]) if len(parts) > 36 else None,
        "amount": amount_wan * 10000 if amount_wan is not None else None,
        "change_amount": change_amount,
        "pct_chg": to_float(parts[32]) if len(parts) > 32 else None,
        "pre_close": pre_close,
        "source": "tencent_quote",
    }


def normalize_history_row(
    code: str,
    source_code: str,
    trade_date: Any,
    open_price: Any,
    high: Any,
    low: Any,
    close: Any,
    volume: Any,
    amount: Any,
    pct_chg: Any,
    change_amount: Any,
) -> dict[str, Any] | None:
    close_value = to_float(close)
    trade_date_text = str(trade_date or "")[:10]
    if not trade_date_text or close_value is None:
        return None
    change_value = to_float(change_amount)
    pre_close = close_value - change_value if change_value is not None else None
    return {
        "code": code,
        "source_code": source_code,
        "trade_date": trade_date_text,
        "open": to_float(open_price),
        "high": to_float(high),
        "low": to_float(low),
        "close": close_value,
        "volume": to_int(volume),
        "amount": to_float(amount),
        "change_amount": change_value,
        "pct_chg": to_float(pct_chg),
        "pre_close": pre_close,
        "source": SOURCE,
    }


def save_daily_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO daily_kline (code, trade_date, open, high, low, close, volume, amount, source)
    VALUES (%(code)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(amount)s, %(source)s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open),
        high=VALUES(high),
        low=VALUES(low),
        close=VALUES(close),
        volume=VALUES(volume),
        amount=VALUES(amount),
        source=VALUES(source)
    """
    payload = [{**row, "source": row.get("source") or SOURCE} for row in rows]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, payload)
            return cursor.rowcount


def save_snapshot(code: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    latest = rows[-1]
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT name FROM stock_basic WHERE code = %s LIMIT 1", (code,))
            basic = cursor.fetchone() or {}
    quote_time = latest.get("quote_time") or f"{latest['trade_date']} 15:00:00"
    sql = """
    INSERT INTO stock_realtime_snapshot (
        code, source_code, name, trade_date, quote_time, latest_price, change_amount, pct_chg,
        bid_price, ask_price, pre_close, open_price, high_price, low_price, volume, amount, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        source_code=VALUES(source_code),
        name=VALUES(name),
        trade_date=VALUES(trade_date),
        quote_time=VALUES(quote_time),
        latest_price=VALUES(latest_price),
        change_amount=VALUES(change_amount),
        pct_chg=VALUES(pct_chg),
        pre_close=VALUES(pre_close),
        open_price=VALUES(open_price),
        high_price=VALUES(high_price),
        low_price=VALUES(low_price),
        volume=VALUES(volume),
        amount=VALUES(amount),
        source=VALUES(source)
    """
    values = (
        code,
        latest.get("source_code"),
        basic.get("name"),
        latest.get("trade_date"),
        quote_time,
        latest.get("close"),
        latest.get("change_amount"),
        latest.get("pct_chg"),
        latest.get("pre_close"),
        latest.get("open"),
        latest.get("high"),
        latest.get("low"),
        latest.get("volume"),
        latest.get("amount"),
        latest.get("source") or SOURCE,
    )
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.rowcount


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync held ETF daily close and snapshot for portfolio page.")
    parser.add_argument("--codes", nargs="*", help="ETF codes to sync; default active ETF portfolio positions.")
    parser.add_argument("--days", type=int, default=90, help="Daily bars to keep in daily_kline.")
    args = parser.parse_args()

    ensure_realtime_schema()
    codes = args.codes or held_etf_codes()
    result = {"codes": codes, "updated": [], "failed": []}
    for code in codes:
        try:
            rows = fetch_etf_history(code, max(1, args.days))
            daily_rows = save_daily_rows(rows)
            snapshot_rows = save_snapshot(code, rows)
            result["updated"].append(
                {
                    "code": code,
                    "daily_rows": daily_rows,
                    "snapshot_rows": snapshot_rows,
                    "latest_trade_date": rows[-1]["trade_date"] if rows else None,
                    "latest_price": rows[-1]["close"] if rows else None,
                }
            )
        except Exception as exc:
            result["failed"].append({"code": code, "error": str(exc)[:300]})
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
