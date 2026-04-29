from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import tushare as ts

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


@dataclass
class ValuationRecord:
    code: str
    trade_date: str
    pe_tushare: Optional[float]
    pb_tushare: Optional[float]


@dataclass
class ValuationSyncResult:
    run_id: str
    trade_date: Optional[str] = None
    scanned: int = 0
    updated: int = 0
    missing_source: int = 0
    started_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    finished_at: Optional[str] = None

    def finish(self) -> "ValuationSyncResult":
        self.finished_at = datetime.now().isoformat(timespec="seconds")
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "scanned": self.scanned,
            "updated": self.updated,
            "missing_source": self.missing_source,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class ValuationSync:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)
        self.task_logger = TaskRunLogger()

    def ensure_columns(self) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM stock_basic")
                columns = {row[0] for row in cursor.fetchall()}
                if "pe_tushare" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN pe_tushare DECIMAL(12,4) DEFAULT NULL")
                if "pb_tushare" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN pb_tushare DECIMAL(12,4) DEFAULT NULL")
                if "valuation_updated_at" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN valuation_updated_at DATETIME DEFAULT NULL")

    def get_trade_date(self) -> str:
        today = datetime.now().strftime("%Y%m%d")
        df = self.pro.daily_basic(trade_date=today, fields="ts_code,pe,pb")
        if not df.empty:
            return today
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        return yesterday

    def fetch_daily_basic_map(self, trade_date: str) -> Dict[str, ValuationRecord]:
        df = self.pro.daily_basic(trade_date=trade_date, fields="ts_code,pe,pb")
        result: Dict[str, ValuationRecord] = {}
        for _, row in df.iterrows():
            code = str(row["ts_code"]).split(".")[0]
            pe = row["pe"]
            pb = row["pb"]
            result[code] = ValuationRecord(
                code=code,
                trade_date=trade_date,
                pe_tushare=float(pe) if pe == pe else None,
                pb_tushare=float(pb) if pb == pb else None,
            )
        return result

    def fetch_stock_codes(
        self,
        limit: Optional[int] = None,
        instrument_type: Optional[str] = None,
        only_missing: bool = True,
        stale_after_days: Optional[int] = 7,
        exclude_codes: Optional[List[str]] = None,
        require_missing_pb: bool = False,
        allow_missing_pe_only: bool = True,
    ) -> List[str]:
        sql = "SELECT code FROM stock_basic WHERE is_delisted = 0"
        params: List[Any] = []
        if instrument_type:
            sql += " AND instrument_type = %s"
            params.append(instrument_type)
        if only_missing:
            if require_missing_pb:
                sql += " AND pb_tushare IS NULL"
            elif allow_missing_pe_only:
                sql += " AND (pe_tushare IS NULL OR pb_tushare IS NULL)"
            else:
                sql += " AND pb_tushare IS NULL"
        elif stale_after_days is not None:
            cutoff = datetime.now() - timedelta(days=stale_after_days)
            sql += " AND (valuation_updated_at IS NULL OR valuation_updated_at < %s)"
            params.append(cutoff.strftime("%Y-%m-%d %H:%M:%S"))
        if exclude_codes:
            placeholders = ",".join(["%s"] * len(exclude_codes))
            sql += f" AND code NOT IN ({placeholders})"
            params.extend(exclude_codes)
        sql += " ORDER BY code"
        if limit:
            sql += f" LIMIT {int(limit)}"
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return [row["code"] for row in cursor.fetchall()]

    @staticmethod
    def normalize_code(code: str) -> str:
        return code.split(".")[-1] if "." in code else code

    def save_to_mysql(
        self,
        records: Dict[str, ValuationRecord],
        limit: Optional[int] = None,
        instrument_type: Optional[str] = None,
        only_missing: bool = True,
        stale_after_days: Optional[int] = 7,
        exclude_codes: Optional[List[str]] = None,
        require_missing_pb: bool = False,
        allow_missing_pe_only: bool = True,
    ) -> tuple[int, int, int, List[str]]:
        codes = self.fetch_stock_codes(
            limit=limit,
            instrument_type=instrument_type,
            only_missing=only_missing,
            stale_after_days=stale_after_days,
            exclude_codes=exclude_codes,
            require_missing_pb=require_missing_pb,
            allow_missing_pe_only=allow_missing_pe_only,
        )
        updated = 0
        missing_source = 0
        missing_codes: List[str] = []
        sql = """
        UPDATE stock_basic
        SET pe_tushare = %s,
            pb_tushare = %s,
            valuation_updated_at = NOW()
        WHERE code = %s
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                for code in codes:
                    item = records.get(self.normalize_code(code))
                    if not item:
                        missing_source += 1
                        missing_codes.append(code)
                        continue
                    cursor.execute(sql, (item.pe_tushare, item.pb_tushare, code))
                    updated += 1
        return len(codes), updated, missing_source, missing_codes

    @staticmethod
    def build_run_id() -> str:
        return f"valuation_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def run(
        self,
        limit: Optional[int] = 200,
        instrument_type: Optional[str] = None,
        only_missing: bool = True,
        stale_after_days: Optional[int] = 7,
        exclude_codes: Optional[List[str]] = None,
        require_missing_pb: bool = False,
        allow_missing_pe_only: bool = True,
    ) -> ValuationSyncResult:
        self.ensure_columns()
        result = ValuationSyncResult(run_id=self.build_run_id())
        self.task_logger.start(
            task_name="valuation_sync",
            run_id=result.run_id,
            metadata={
                "limit": limit,
                "instrument_type": instrument_type,
                "only_missing": only_missing,
                "stale_after_days": stale_after_days,
                "exclude_codes": exclude_codes or [],
                "require_missing_pb": require_missing_pb,
                "allow_missing_pe_only": allow_missing_pe_only,
            },
        )
        try:
            trade_date = self.get_trade_date()
            result.trade_date = trade_date
            records = self.fetch_daily_basic_map(trade_date)
            scanned, updated, missing_source, missing_codes = self.save_to_mysql(
                records,
                limit=limit,
                instrument_type=instrument_type,
                only_missing=only_missing,
                stale_after_days=stale_after_days,
                exclude_codes=exclude_codes,
                require_missing_pb=require_missing_pb,
                allow_missing_pe_only=allow_missing_pe_only,
            )
            result.scanned = scanned
            result.updated = updated
            result.missing_source = missing_source
            payload = result.to_dict() | {"missing_codes": missing_codes}
            result.finish()
            self.task_logger.finish(
                task_name="valuation_sync",
                run_id=result.run_id,
                status="success",
                message=f"valuation sync completed, updated={result.updated}",
                metadata=payload,
            )
            return result
        except Exception as e:
            result.finish()
            self.task_logger.finish(
                task_name="valuation_sync",
                run_id=result.run_id,
                status="failed",
                message=str(e)[:500],
                metadata=result.to_dict(),
            )
            raise


if __name__ == "__main__":
    sync = ValuationSync()
    summary = sync.run()
    print(summary.to_dict())
