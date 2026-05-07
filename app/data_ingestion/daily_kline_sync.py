from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Sequence

import baostock as bs

from app.shared.db import mysql_conn


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
    source: str = "baostock"


class DailyKlineSync:
    def login_baostock(self) -> None:
        result = bs.login()
        if result.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {result.error_msg}")

    def logout_baostock(self) -> None:
        try:
            bs.logout()
        except Exception:
            pass

    def ensure_login(self) -> None:
        self.logout_baostock()
        self.login_baostock()

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
        if offset > 0:
            sql += " LIMIT %s, 18446744073709551615"
            params.append(offset)
        if limit:
            if offset > 0:
                sql = sql.replace("18446744073709551615", "%s")
                params.append(int(limit))
            else:
                sql += " LIMIT %s"
                params.append(int(limit))

        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return [row["code"] for row in cursor.fetchall()]

    def fetch_kline_for_code(self, code: str, start_date: str, end_date: str) -> List[DailyKlineRecord]:
        rs = bs.query_history_k_data_plus(
            code,
            "date,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"拉取 {code} 日线失败: {rs.error_msg}")

        records: List[DailyKlineRecord] = []
        while rs.next():
            row = rs.get_row_data()
            records.append(
                DailyKlineRecord(
                    code=code,
                    trade_date=row[0],
                    open=float(row[1]) if row[1] else None,
                    high=float(row[2]) if row[2] else None,
                    low=float(row[3]) if row[3] else None,
                    close=float(row[4]) if row[4] else None,
                    volume=int(float(row[5])) if row[5] else None,
                    amount=float(row[6]) if row[6] else None,
                )
            )
        return records

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
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        selected_codes = self.fetch_codes(
            limit=limit,
            instrument_type=instrument_type,
            offset=offset,
            codes=codes,
        )

        self.login_baostock()
        total_rows = 0
        success_codes = 0
        failed_codes: list[dict[str, str]] = []
        try:
            for idx, code in enumerate(selected_codes, start=1):
                if relogin_every > 0 and idx > 1 and (idx - 1) % relogin_every == 0:
                    self.ensure_login()
                try:
                    total_rows += self.save_to_mysql(self.fetch_kline_for_code(code, start_date, end_date))
                    success_codes += 1
                except Exception as exc:
                    if "用户未登录" in str(exc):
                        try:
                            self.ensure_login()
                            total_rows += self.save_to_mysql(self.fetch_kline_for_code(code, start_date, end_date))
                            success_codes += 1
                            continue
                        except Exception as retry_exc:
                            failed_codes.append({"code": code, "error": str(retry_exc)})
                    else:
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
                "relogin_every": relogin_every,
            }
        finally:
            self.logout_baostock()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch sync daily kline history into MySQL")
    parser.add_argument("--days", type=int, default=5, help="Fallback rolling window days when start/end not provided")
    parser.add_argument("--start-date", type=str, help="History start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="History end date, format YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100, help="How many codes to sync in this batch")
    parser.add_argument("--offset", type=int, default=0, help="Code offset for batch paging")
    parser.add_argument("--instrument-type", type=str, default="stock", help="Instrument type in stock_basic")
    parser.add_argument("--codes", type=str, help="Comma-separated explicit code list, overrides DB selection")
    parser.add_argument("--pause-seconds", type=float, default=0.0, help="Optional pause between codes")
    parser.add_argument("--relogin-every", type=int, default=10, help="Refresh BaoStock login after every N codes")
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
