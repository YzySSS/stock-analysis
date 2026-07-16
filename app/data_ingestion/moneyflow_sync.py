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

SOURCE = "tushare_moneyflow"


@dataclass
class MoneyflowRecord:
    code: str
    trade_date: str
    buy_sm_vol: int | None = None
    buy_sm_amount: float | None = None
    sell_sm_vol: int | None = None
    sell_sm_amount: float | None = None
    buy_md_vol: int | None = None
    buy_md_amount: float | None = None
    sell_md_vol: int | None = None
    sell_md_amount: float | None = None
    buy_lg_vol: int | None = None
    buy_lg_amount: float | None = None
    sell_lg_vol: int | None = None
    sell_lg_amount: float | None = None
    buy_elg_vol: int | None = None
    buy_elg_amount: float | None = None
    sell_elg_vol: int | None = None
    sell_elg_amount: float | None = None
    net_mf_vol: int | None = None
    net_mf_amount: float | None = None
    source: str = SOURCE


class MoneyflowSync:
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

    @staticmethod
    def _float(value: object) -> float | None:
        return float(value) if value is not None and value == value else None

    @staticmethod
    def _int(value: object) -> int | None:
        return int(float(value)) if value is not None and value == value else None

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

    def fetch_for_trade_date(self, trade_date: str) -> list[MoneyflowRecord]:
        fields = (
            "ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,"
            "buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,"
            "buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,"
            "buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount"
        )
        df = self.pro.moneyflow(trade_date=self.compact_date(trade_date), fields=fields)
        if df is None or df.empty:
            return []
        records: list[MoneyflowRecord] = []
        for row in df.to_dict("records"):
            records.append(
                MoneyflowRecord(
                    code=self.from_ts_code(str(row.get("ts_code"))),
                    trade_date=self.normalize_trade_date(row.get("trade_date")),
                    buy_sm_vol=self._int(row.get("buy_sm_vol")),
                    buy_sm_amount=self._float(row.get("buy_sm_amount")),
                    sell_sm_vol=self._int(row.get("sell_sm_vol")),
                    sell_sm_amount=self._float(row.get("sell_sm_amount")),
                    buy_md_vol=self._int(row.get("buy_md_vol")),
                    buy_md_amount=self._float(row.get("buy_md_amount")),
                    sell_md_vol=self._int(row.get("sell_md_vol")),
                    sell_md_amount=self._float(row.get("sell_md_amount")),
                    buy_lg_vol=self._int(row.get("buy_lg_vol")),
                    buy_lg_amount=self._float(row.get("buy_lg_amount")),
                    sell_lg_vol=self._int(row.get("sell_lg_vol")),
                    sell_lg_amount=self._float(row.get("sell_lg_amount")),
                    buy_elg_vol=self._int(row.get("buy_elg_vol")),
                    buy_elg_amount=self._float(row.get("buy_elg_amount")),
                    sell_elg_vol=self._int(row.get("sell_elg_vol")),
                    sell_elg_amount=self._float(row.get("sell_elg_amount")),
                    net_mf_vol=self._int(row.get("net_mf_vol")),
                    net_mf_amount=self._float(row.get("net_mf_amount")),
                )
            )
        return records

    def save_records(self, records: Iterable[MoneyflowRecord]) -> int:
        rows = list(records)
        if not rows:
            return 0
        sql = """
        INSERT INTO stock_moneyflow_daily (
            code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
            buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
            buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
            buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
            net_mf_vol, net_mf_amount, source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            buy_sm_vol=VALUES(buy_sm_vol), buy_sm_amount=VALUES(buy_sm_amount),
            sell_sm_vol=VALUES(sell_sm_vol), sell_sm_amount=VALUES(sell_sm_amount),
            buy_md_vol=VALUES(buy_md_vol), buy_md_amount=VALUES(buy_md_amount),
            sell_md_vol=VALUES(sell_md_vol), sell_md_amount=VALUES(sell_md_amount),
            buy_lg_vol=VALUES(buy_lg_vol), buy_lg_amount=VALUES(buy_lg_amount),
            sell_lg_vol=VALUES(sell_lg_vol), sell_lg_amount=VALUES(sell_lg_amount),
            buy_elg_vol=VALUES(buy_elg_vol), buy_elg_amount=VALUES(buy_elg_amount),
            sell_elg_vol=VALUES(sell_elg_vol), sell_elg_amount=VALUES(sell_elg_amount),
            net_mf_vol=VALUES(net_mf_vol), net_mf_amount=VALUES(net_mf_amount), source=VALUES(source)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, [tuple(r.__dict__.values()) for r in rows])
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
    print(MoneyflowSync().run(start, today))
