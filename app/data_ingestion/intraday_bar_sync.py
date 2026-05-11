from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.orchestration.intraday_bar_schema import ensure_intraday_bar_schema
from app.shared.db import mysql_conn

SOURCE = "akshare_stock_zh_a_hist_min_em"


@dataclass
class IntradayBar:
    code: str
    trade_date: str
    minute_time: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    avg_price: float | None
    volume: int | None
    amount: float | None


def normalize_source_code(code: str) -> str:
    text = str(code or "").strip().lower()
    if "." in text:
        return text.split(".", 1)[1]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else text


def normalize_code(code: str) -> str:
    text = str(code or "").strip().lower()
    if text.startswith(("sh.", "sz.", "bj.")):
        return text
    digits = normalize_source_code(text)
    if digits.startswith(("60", "68", "90")):
        return f"sh.{digits}"
    if digits.startswith(("00", "30", "20")):
        return f"sz.{digits}"
    if digits.startswith(("43", "83", "87", "88", "92")):
        return f"bj.{digits}"
    return text


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def latest_trade_date_for_code(code: str) -> str | None:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT trade_date FROM stock_realtime_snapshot WHERE code = %s LIMIT 1", (code,))
            row = cursor.fetchone() or {}
            value = row.get("trade_date")
            if value:
                return value.isoformat() if hasattr(value, "isoformat") else str(value)
            cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline WHERE code = %s", (code,))
            row = cursor.fetchone() or {}
            value = row.get("trade_date")
            return value.isoformat() if value and hasattr(value, "isoformat") else (str(value) if value else None)


def parse_minute(value: Any) -> datetime | None:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def cached_bars(code: str, trade_date: str) -> list[dict[str, Any]]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT minute_time, open, high, low, close, avg_price, volume, amount, source, updated_at
                FROM stock_intraday_bar
                WHERE code = %s AND trade_date = %s
                ORDER BY minute_time ASC
                """,
                (code, trade_date),
            )
            rows = cursor.fetchall()
    return [serialize_bar(row) for row in rows]


def serialize_bar(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "minute_time": str(row.get("minute_time")) if row.get("minute_time") else None,
        "open": to_float(row.get("open")),
        "high": to_float(row.get("high")),
        "low": to_float(row.get("low")),
        "close": to_float(row.get("close")),
        "avg_price": to_float(row.get("avg_price")),
        "volume": row.get("volume"),
        "amount": to_float(row.get("amount")),
        "source": row.get("source") or SOURCE,
        "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
    }


def _bars_from_eastmoney(code: str, trade_date: str) -> list[IntradayBar]:
    import akshare as ak

    source_code = normalize_source_code(code)
    start = f"{trade_date} 09:30:00"
    end = f"{trade_date} 15:00:00"
    df = ak.stock_zh_a_hist_min_em(symbol=source_code, start_date=start, end_date=end, period="1", adjust="")
    bars: list[IntradayBar] = []
    final_code = normalize_code(code)
    for _, item in df.iterrows():
        minute = parse_minute(item.get("时间"))
        if not minute or minute.date().isoformat() != trade_date:
            continue
        bars.append(
            IntradayBar(
                code=final_code,
                trade_date=trade_date,
                minute_time=minute.strftime("%Y-%m-%d %H:%M:%S"),
                open=to_float(item.get("开盘")),
                high=to_float(item.get("最高")),
                low=to_float(item.get("最低")),
                close=to_float(item.get("收盘")),
                avg_price=to_float(item.get("均价")),
                volume=to_int(item.get("成交量")),
                amount=to_float(item.get("成交额")),
            )
        )
    return bars


def _bars_from_sina(code: str, trade_date: str) -> list[IntradayBar]:
    import akshare as ak

    final_code = normalize_code(code)
    source_code = normalize_source_code(final_code)
    if final_code.startswith(("sh.", "sz.")):
        sina_code = final_code.replace(".", "")
    else:
        sina_code = source_code
    df = ak.stock_zh_a_minute(symbol=sina_code, period="1", adjust="")
    bars: list[IntradayBar] = []
    for _, item in df.iterrows():
        minute = parse_minute(item.get("day"))
        if not minute or minute.date().isoformat() != trade_date:
            continue
        bars.append(
            IntradayBar(
                code=final_code,
                trade_date=trade_date,
                minute_time=minute.strftime("%Y-%m-%d %H:%M:%S"),
                open=to_float(item.get("open")),
                high=to_float(item.get("high")),
                low=to_float(item.get("low")),
                close=to_float(item.get("close")),
                avg_price=None,
                volume=to_int(item.get("volume")),
                amount=to_float(item.get("amount")),
            )
        )
    return bars


def fetch_akshare_bars(code: str, trade_date: str) -> list[IntradayBar]:
    try:
        bars = _bars_from_sina(code, trade_date)
        if bars:
            return bars
    except Exception:
        pass
    return _bars_from_eastmoney(code, trade_date)


def save_bars(bars: list[IntradayBar]) -> int:
    if not bars:
        return 0
    sql = """
    INSERT INTO stock_intraday_bar (
        code, trade_date, minute_time, open, high, low, close, avg_price, volume, amount, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), avg_price=VALUES(avg_price),
        volume=VALUES(volume), amount=VALUES(amount), source=VALUES(source)
    """
    values = [
        (bar.code, bar.trade_date, bar.minute_time, bar.open, bar.high, bar.low, bar.close, bar.avg_price, bar.volume, bar.amount, SOURCE)
        for bar in bars
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
            return cursor.rowcount


def get_or_fetch_intraday_bars(code: str, trade_date: str | None = None, refresh: bool = False) -> dict[str, Any]:
    ensure_intraday_bar_schema()
    final_code = normalize_code(code)
    final_trade_date = trade_date or latest_trade_date_for_code(final_code) or date.today().isoformat()
    cached = [] if refresh else cached_bars(final_code, final_trade_date)
    source_status = "cached"
    saved_rows = 0
    if not cached:
        bars = fetch_akshare_bars(final_code, final_trade_date)
        saved_rows = save_bars(bars)
        cached = cached_bars(final_code, final_trade_date)
        source_status = "fetched" if cached else "empty"
    return {
        "code": final_code,
        "trade_date": final_trade_date,
        "source": SOURCE,
        "source_status": source_status,
        "count": len(cached),
        "saved_rows": saved_rows,
        "items": cached,
    }
