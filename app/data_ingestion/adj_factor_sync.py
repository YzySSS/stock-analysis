from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List

import tushare as ts
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn

load_dotenv(PROJECT_ROOT / ".env")

SOURCE = "tushare_adj_factor"


@dataclass
class AdjFactorRecord:
    code: str
    trade_date: str
    adj_factor: float
    source: str = SOURCE


class AdjFactorSync:
    def __init__(self, token: str | None = None) -> None:
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)

    @staticmethod
    def from_ts_code(ts_code: str) -> str:
        symbol, suffix = ts_code.split(".", 1) if "." in ts_code else (ts_code, "")
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix.upper(), suffix.lower())
        return f"{prefix}.{symbol}" if prefix else symbol

    @staticmethod
    def compact_date(value: str) -> str:
        return value.replace("-", "")

    @staticmethod
    def normalize_trade_date(value: object) -> str:
        value = str(value)
        return value if "-" in value else f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    def fetch_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        sql = """
        SELECT DISTINCT trade_date
        FROM daily_kline
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (start_date, end_date))
                return [str(row["trade_date"]) for row in cursor.fetchall()]

    def fetch_for_trade_date(self, trade_date: str) -> list[AdjFactorRecord]:
        df = self.pro.adj_factor(
            trade_date=self.compact_date(trade_date),
            fields="ts_code,trade_date,adj_factor",
        )
        if df is None or df.empty:
            return []
        records: list[AdjFactorRecord] = []
        for row in df.to_dict("records"):
            factor = row.get("adj_factor")
            if factor != factor or factor is None:
                continue
            records.append(
                AdjFactorRecord(
                    code=self.from_ts_code(str(row.get("ts_code"))),
                    trade_date=self.normalize_trade_date(row.get("trade_date")),
                    adj_factor=float(factor),
                )
            )
        return records

    def save_records(self, records: Iterable[AdjFactorRecord]) -> int:
        rows = list(records)
        if not rows:
            return 0
        sql = """
        INSERT INTO adj_factor_daily (code, trade_date, adj_factor, source)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            adj_factor=VALUES(adj_factor),
            source=VALUES(source)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, [(r.code, r.trade_date, r.adj_factor, r.source) for r in rows])
        return len(rows)

    def run(self, start_date: str, end_date: str, pause_seconds: float = 0.0) -> dict:
        import time

        trade_dates = self.fetch_trade_dates(start_date, end_date)
        rows_synced = 0
        processed_days = 0
        for trade_date in trade_dates:
            rows_synced += self.save_records(self.fetch_for_trade_date(trade_date))
            processed_days += 1
            if pause_seconds > 0:
                time.sleep(pause_seconds)
        return {
            "start_date": start_date,
            "end_date": end_date,
            "trade_dates": len(trade_dates),
            "processed_days": processed_days,
            "rows_synced": rows_synced,
            "source": SOURCE,
        }


if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    print(AdjFactorSync().run(start, today))
