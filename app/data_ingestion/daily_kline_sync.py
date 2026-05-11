from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence

import tushare as ts
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn

load_dotenv(PROJECT_ROOT / ".env")

SOURCE = "tushare_daily"


@dataclass
class DailyKlineRecord:
    code: str
    trade_date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    amount: float | None
    source: str = SOURCE


class DailyKlineSync:
    """Sync daily K-line data from Tushare.

    Tushare is now the official daily K-line source.  AkShare realtime EOD bars may
    write earlier with source=akshare_realtime_eod; this sync runs later and
    overwrites the same date with source=tushare_daily for final calibration.

    Unit normalization:
    - Tushare vol is in hands (手), daily_kline.volume stores shares, so * 100.
    - Tushare amount is in thousand yuan (千元), daily_kline.amount stores yuan, so * 1000.
    """

    def __init__(self, token: str | None = None) -> None:
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)

    @staticmethod
    def to_ts_code(code: str) -> str:
        market, symbol = code.split(".", 1) if "." in code else ("", code)
        suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(market.lower())
        return f"{symbol}.{suffix}" if suffix else code.upper()

    @staticmethod
    def from_ts_code(ts_code: str) -> str:
        symbol, suffix = ts_code.split(".", 1) if "." in ts_code else (ts_code, "")
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix.upper(), suffix.lower())
        return f"{prefix}.{symbol}" if prefix else symbol

    @staticmethod
    def normalize_trade_date(value: str) -> str:
        value = str(value)
        if "-" in value:
            return value
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    @staticmethod
    def compact_date(value: str) -> str:
        return value.replace("-", "")

    def latest_open_trade_date(self, end_date: str | None = None, lookback_days: int = 15) -> str:
        end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()
        start = end - timedelta(days=lookback_days)
        df = self.pro.trade_cal(
            exchange="SSE",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields="cal_date,is_open",
        )
        if df is None or df.empty:
            raise RuntimeError("Tushare trade_cal 未返回交易日")
        open_days = df[df["is_open"] == 1].sort_values("cal_date")
        if open_days.empty:
            raise RuntimeError("Tushare trade_cal 未找到最近开市日")
        return self.normalize_trade_date(str(open_days.iloc[-1]["cal_date"]))

    def fetch_codes(
        self,
        limit: int | None = None,
        instrument_type: str = "stock",
        offset: int = 0,
        codes: Sequence[str] | None = None,
    ) -> List[str]:
        if codes:
            return list(codes)

        sql = "SELECT code FROM stock_basic WHERE instrument_type = %s ORDER BY code"
        params: list[object] = [instrument_type]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([int(limit), int(offset)])
        elif offset > 0:
            sql += " LIMIT 18446744073709551615 OFFSET %s"
            params.append(int(offset))

        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return [row["code"] for row in cursor.fetchall()]

    def _rows_to_records(self, df) -> List[DailyKlineRecord]:
        records: List[DailyKlineRecord] = []
        if df is None or df.empty:
            return records
        for row in df.to_dict("records"):
            vol = row.get("vol")
            amount = row.get("amount")
            records.append(
                DailyKlineRecord(
                    code=self.from_ts_code(str(row.get("ts_code"))),
                    trade_date=self.normalize_trade_date(str(row.get("trade_date"))),
                    open=float(row["open"]) if row.get("open") == row.get("open") else None,
                    high=float(row["high"]) if row.get("high") == row.get("high") else None,
                    low=float(row["low"]) if row.get("low") == row.get("low") else None,
                    close=float(row["close"]) if row.get("close") == row.get("close") else None,
                    volume=int(float(vol) * 100) if vol == vol else None,
                    amount=float(amount) * 1000 if amount == amount else None,
                )
            )
        return records

    def fetch_kline_for_code(self, code: str, start_date: str, end_date: str) -> List[DailyKlineRecord]:
        df = self.pro.daily(
            ts_code=self.to_ts_code(code),
            start_date=self.compact_date(start_date),
            end_date=self.compact_date(end_date),
            fields="ts_code,trade_date,open,high,low,close,vol,amount",
        )
        return self._rows_to_records(df)

    def fetch_kline_for_trade_date(self, trade_date: str) -> List[DailyKlineRecord]:
        df = self.pro.daily(
            trade_date=self.compact_date(trade_date),
            fields="ts_code,trade_date,open,high,low,close,vol,amount",
        )
        return self._rows_to_records(df)

    def save_to_mysql(self, records: Iterable[DailyKlineRecord]) -> int:
        rows = list(records)
        if not rows:
            return 0
        sql = """
        INSERT INTO daily_kline (
            code, trade_date, open, high, low, close, volume, amount, source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume),
            amount = VALUES(amount),
            source = VALUES(source)
        """
        data = [
            (r.code, r.trade_date, r.open, r.high, r.low, r.close, r.volume, r.amount, r.source)
            for r in rows
        ]
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
        return len(rows)

    def _trade_dates_between(self, start_date: str, end_date: str) -> list[str]:
        df = self.pro.trade_cal(
            exchange="SSE",
            start_date=self.compact_date(start_date),
            end_date=self.compact_date(end_date),
            fields="cal_date,is_open",
        )
        if df is None or df.empty:
            return []
        return [self.normalize_trade_date(str(v)) for v in df[df["is_open"] == 1].sort_values("cal_date")["cal_date"].tolist()]

    def run(
        self,
        days: int = 5,
        limit: int | None = 100,
        instrument_type: str = "stock",
        start_date: str | None = None,
        end_date: str | None = None,
        offset: int = 0,
        codes: Sequence[str] | None = None,
        pause_seconds: float = 0.0,
        relogin_every: int = 10,
    ) -> dict:
        del relogin_every  # kept for backward-compatible CLI/API signature
        if not end_date:
            end_date = self.latest_open_trade_date()
        if not start_date:
            start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")

        total_rows = 0
        success_codes = 0
        failed_codes: list[dict[str, str]] = []
        selected_codes: list[str] = []

        if codes:
            selected_codes = self.fetch_codes(limit=limit, instrument_type=instrument_type, offset=offset, codes=codes)
            for code in selected_codes:
                try:
                    total_rows += self.save_to_mysql(self.fetch_kline_for_code(code, start_date, end_date))
                    success_codes += 1
                except Exception as exc:
                    failed_codes.append({"code": code, "error": str(exc)})
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
        elif start_date == end_date and limit is None and offset == 0:
            rows = self.fetch_kline_for_trade_date(start_date)
            total_rows = self.save_to_mysql(rows)
            success_codes = len({r.code for r in rows})
            selected_codes = [r.code for r in rows]
        else:
            selected_codes = self.fetch_codes(limit=limit, instrument_type=instrument_type, offset=offset)
            if limit is None and offset == 0:
                for trade_date in self._trade_dates_between(start_date, end_date):
                    rows = self.fetch_kline_for_trade_date(trade_date)
                    total_rows += self.save_to_mysql(rows)
                    success_codes += len({r.code for r in rows})
                    if pause_seconds > 0:
                        time.sleep(pause_seconds)
            else:
                for code in selected_codes:
                    try:
                        total_rows += self.save_to_mysql(self.fetch_kline_for_code(code, start_date, end_date))
                        success_codes += 1
                    except Exception as exc:
                        failed_codes.append({"code": code, "error": str(exc)})
                    if pause_seconds > 0:
                        time.sleep(pause_seconds)

        return {
            "start_date": start_date,
            "end_date": end_date,
            "requested_codes": len(selected_codes),
            "success_codes": success_codes,
            "failed_codes": failed_codes,
            "rows_synced": total_rows,
            "offset": offset,
            "limit": limit,
            "instrument_type": instrument_type,
            "source": SOURCE,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch sync daily kline history into MySQL via Tushare")
    parser.add_argument("--days", type=int, default=5, help="Fallback rolling window days when start/end not provided")
    parser.add_argument("--start-date", type=str, help="History start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="History end date, format YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100, help="How many codes to sync in this batch")
    parser.add_argument("--offset", type=int, default=0, help="Code offset for batch paging")
    parser.add_argument("--instrument-type", type=str, default="stock", help="Instrument type in stock_basic")
    parser.add_argument("--codes", type=str, help="Comma-separated explicit code list, overrides DB selection")
    parser.add_argument("--pause-seconds", type=float, default=0.0, help="Optional pause between codes/dates")
    parser.add_argument("--relogin-every", type=int, default=10, help="Deprecated BaoStock compatibility option")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    sync = DailyKlineSync()
    result = sync.run(
        days=args.days,
        limit=args.limit,
        instrument_type=args.instrument_type,
        start_date=args.start_date,
        end_date=args.end_date,
        offset=args.offset,
        codes=[code.strip() for code in args.codes.split(",") if code.strip()] if args.codes else None,
        pause_seconds=args.pause_seconds,
        relogin_every=args.relogin_every,
    )
    print(result)
