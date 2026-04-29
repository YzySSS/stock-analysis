from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict

import baostock as bs

from app.shared.db import mysql_conn


@dataclass
class StockBasicRecord:
    code: str
    name: str
    instrument_type: str = "other"
    market: str | None = None
    industry: str | None = None
    is_st: int = 0
    is_delisted: int = 0
    listing_date: str | None = None


class StockBasicSync:
    @staticmethod
    def normalize_industry(value: str | None) -> str | None:
        if not value:
            return None
        value = value.strip()
        return value or None

    @staticmethod
    def detect_instrument_type(code: str, name: str) -> str:
        upper_name = name.upper()
        lower_code = code.lower()
        if any(keyword in name for keyword in ["ETF", "基金"]):
            return "etf"
        if any(keyword in name for keyword in ["指数", "综指", "成指", "国债", "企债", "债券"]):
            return "index"
        if lower_code.startswith(("sh.60", "sh.68", "sz.00", "sz.30", "bj.")):
            return "stock"
        return "other"

    def ensure_columns(self) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM stock_basic")
                columns = {row[0] for row in cursor.fetchall()}
                if "instrument_type" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN instrument_type VARCHAR(16) DEFAULT 'other'")

    def login_baostock(self) -> None:
        result = bs.login()
        if result.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {result.error_msg}")

    def logout_baostock(self) -> None:
        try:
            bs.logout()
        except Exception:
            pass

    def fetch_basic_detail_map(self) -> Dict[str, Dict[str, str | None]]:
        detail_map: Dict[str, Dict[str, str | None]] = {}
        rs = bs.query_stock_basic()
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            code = row[0]
            detail_map[code] = {
                "listing_date": row[2] or None,
                "out_date": row[3] or None,
            }
        return detail_map

    def fetch_industry_map(self) -> Dict[str, str | None]:
        industry_map: Dict[str, str | None] = {}
        rs = bs.query_stock_industry()
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            code = row[1]
            industry_map[code] = self.normalize_industry(row[3])
        return industry_map

    def fetch_stock_basic(self) -> List[StockBasicRecord]:
        rows: List[StockBasicRecord] = []
        detail_map = self.fetch_basic_detail_map()
        industry_map = self.fetch_industry_map()
        for offset in range(0, 7):
            day = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=day)
            rows.clear()
            while rs.error_code == "0" and rs.next():
                row: Dict[str, str] = rs.get_row_data()
                code = row[0]
                name = row[2]
                market = code.split(".")[0] if "." in code else None
                detail = detail_map.get(code, {})
                rows.append(
                    StockBasicRecord(
                        code=code,
                        name=name,
                        instrument_type=self.detect_instrument_type(code, name),
                        market=market,
                        industry=industry_map.get(code),
                        is_st=1 if "ST" in name.upper() else 0,
                        is_delisted=1 if (detail.get("out_date") and detail.get("out_date") < day) else 0,
                        listing_date=detail.get("listing_date"),
                    )
                )
            if rows:
                return rows
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

    def run(self) -> int:
        self.login_baostock()
        try:
            records = self.fetch_stock_basic()
            return self.save_to_mysql(records)
        finally:
            self.logout_baostock()


if __name__ == "__main__":
    sync = StockBasicSync()
    count = sync.run()
    print(f"stock_basic synced: {count}")
