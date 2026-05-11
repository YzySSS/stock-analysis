from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

import tushare as ts
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn

load_dotenv(PROJECT_ROOT / ".env")

SOURCE = "tushare_stock_basic"


@dataclass
class StockBasicRecord:
    code: str
    name: str
    instrument_type: str = "stock"
    market: str | None = None
    industry: str | None = None
    is_st: int = 0
    is_delisted: int = 0
    listing_date: str | None = None


class StockBasicSync:
    """Sync A-share basic list from Tushare.

    BaoStock used to be the primary basic-list source, but Tushare has better
    coverage for the current A-share universe including Beijing Stock Exchange
    codes.  AkShare realtime snapshot is kept only as a supplement for brand-new
    codes that appear intraday before Tushare's list refreshes.
    """

    def __init__(self, token: str | None = None) -> None:
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)

    @staticmethod
    def normalize_industry(value: str | None) -> str | None:
        if not value:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def from_ts_code(ts_code: str) -> str:
        symbol, suffix = ts_code.split(".", 1) if "." in ts_code else (ts_code, "")
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix.upper(), suffix.lower())
        return f"{prefix}.{symbol}" if prefix else symbol

    @staticmethod
    def normalize_listing_date(value: object) -> str | None:
        if value is None or value != value:
            return None
        value = str(value)
        if not value or value == "None":
            return None
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(value) == 8 else value

    def ensure_columns(self) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM stock_basic")
                columns = {row[0] for row in cursor.fetchall()}
                if "instrument_type" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN instrument_type VARCHAR(16) DEFAULT 'other'")

    def fetch_stock_basic(self) -> List[StockBasicRecord]:
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date",
        )
        rows: List[StockBasicRecord] = []
        if df is None or df.empty:
            return rows
        for row in df.to_dict("records"):
            code = self.from_ts_code(str(row.get("ts_code")))
            name = str(row.get("name") or code)
            market = code.split(".", 1)[0] if "." in code else None
            rows.append(
                StockBasicRecord(
                    code=code,
                    name=name,
                    instrument_type="stock",
                    market=market,
                    industry=self.normalize_industry(row.get("industry")),
                    is_st=1 if "ST" in name.upper() else 0,
                    is_delisted=0,
                    listing_date=self.normalize_listing_date(row.get("list_date")),
                )
            )
        return rows

    def save_to_mysql(self, records: List[StockBasicRecord]) -> int:
        if not records:
            return 0
        self.ensure_columns()
        sql = """
        INSERT INTO stock_basic (
            code, name, instrument_type, market, industry, is_st, is_delisted, listing_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            instrument_type = VALUES(instrument_type),
            market = VALUES(market),
            industry = VALUES(industry),
            is_st = VALUES(is_st),
            is_delisted = VALUES(is_delisted),
            listing_date = VALUES(listing_date)
        """
        data = [
            (
                r.code,
                r.name,
                r.instrument_type,
                r.market,
                r.industry,
                r.is_st,
                r.is_delisted,
                r.listing_date,
            )
            for r in records
        ]
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
        return len(records)

    def supplement_from_realtime_snapshot(self) -> int:
        """Add stock-like codes from AkShare realtime that Tushare has not listed yet."""
        self.ensure_columns()
        sql = """
        INSERT INTO stock_basic (
            code, name, instrument_type, market, industry, is_st, is_delisted, listing_date
        )
        SELECT
            r.code,
            COALESCE(NULLIF(r.name, ''), r.code) AS name,
            'stock' AS instrument_type,
            SUBSTRING_INDEX(r.code, '.', 1) AS market,
            NULL AS industry,
            CASE WHEN UPPER(COALESCE(r.name, '')) LIKE '%%ST%%' THEN 1 ELSE 0 END AS is_st,
            0 AS is_delisted,
            NULL AS listing_date
        FROM stock_realtime_snapshot r
        LEFT JOIN stock_basic sb ON sb.code = r.code
        WHERE sb.code IS NULL
          AND (
            r.code LIKE 'sh.60%%' OR r.code LIKE 'sh.68%%'
            OR r.code LIKE 'sz.00%%' OR r.code LIKE 'sz.30%%'
            OR r.code LIKE 'bj.%%'
          )
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                return int(cursor.execute(sql) or 0)

    def run(self) -> int:
        records = self.fetch_stock_basic()
        saved = self.save_to_mysql(records)
        supplemented = self.supplement_from_realtime_snapshot()
        return saved + supplemented


if __name__ == "__main__":
    sync = StockBasicSync()
    count = sync.run()
    print(f"stock_basic synced: {count} source={SOURCE}")
