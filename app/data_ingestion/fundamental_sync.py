from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import tushare as ts

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


@dataclass
class FundamentalRecord:
    code: str
    roe: Optional[float]
    roa: Optional[float]
    grossprofit_margin: Optional[float]
    netprofit_margin: Optional[float]
    revenue_yoy: Optional[float]
    profit_yoy: Optional[float]
    period: Optional[str]


@dataclass
class SyncFailure:
    code: str
    ts_code: str
    period: Optional[str]
    error: str


@dataclass
class FundamentalSyncResult:
    run_id: str
    scanned: int = 0
    updated: int = 0
    no_data: int = 0
    failed: int = 0
    throttled: int = 0
    started_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    finished_at: Optional[str] = None
    failures: List[SyncFailure] = field(default_factory=list)

    def finish(self) -> "FundamentalSyncResult":
        self.finished_at = datetime.now().isoformat(timespec="seconds")
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "scanned": self.scanned,
            "updated": self.updated,
            "no_data": self.no_data,
            "failed": self.failed,
            "throttled": self.throttled,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "failures": [
                {
                    "code": item.code,
                    "ts_code": item.ts_code,
                    "period": item.period,
                    "error": item.error,
                }
                for item in self.failures
            ],
        }


class FundamentalSync:
    def __init__(self, token: Optional[str] = None, sleep_seconds: float = 0.3):
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)
        self.sleep_seconds = sleep_seconds
        self.periods = ["20241231", "20240930", "20240630", "20240331", "20231231"]
        self.task_logger = TaskRunLogger()

    def ensure_columns(self) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM stock_basic")
                columns = {row[0] for row in cursor.fetchall()}
                if "roe" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN roe DECIMAL(12,4) DEFAULT NULL")
                if "roa" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN roa DECIMAL(12,4) DEFAULT NULL")
                if "grossprofit_margin" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN grossprofit_margin DECIMAL(12,4) DEFAULT NULL")
                if "netprofit_margin" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN netprofit_margin DECIMAL(12,4) DEFAULT NULL")
                if "revenue_yoy" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN revenue_yoy DECIMAL(12,4) DEFAULT NULL")
                if "profit_yoy" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN profit_yoy DECIMAL(12,4) DEFAULT NULL")
                if "fundamental_period" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN fundamental_period VARCHAR(16) DEFAULT NULL")
                if "fundamental_updated_at" not in columns:
                    cursor.execute("ALTER TABLE stock_basic ADD COLUMN fundamental_updated_at DATETIME DEFAULT NULL")

    def fetch_stock_codes(
        self,
        limit: Optional[int] = 200,
        only_missing: bool = True,
        stale_after_days: Optional[int] = 30,
    ) -> List[str]:
        sql = """
        SELECT code FROM stock_basic
        WHERE is_delisted = 0
          AND instrument_type = 'stock'
          AND name NOT LIKE '%%指数%%'
        """
        params: List[Any] = []

        if only_missing:
            sql += " AND (roe IS NULL OR roa IS NULL OR grossprofit_margin IS NULL OR revenue_yoy IS NULL)"
        elif stale_after_days is not None:
            cutoff = datetime.now() - timedelta(days=stale_after_days)
            sql += " AND (fundamental_updated_at IS NULL OR fundamental_updated_at < %s)"
            params.append(cutoff.strftime("%Y-%m-%d %H:%M:%S"))

        sql += " ORDER BY code"
        if limit:
            sql += f" LIMIT {int(limit)}"

        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                codes = []
                for row in cursor.fetchall():
                    code = row["code"]
                    if "." in code:
                        prefix, raw = code.split(".", 1)
                        if prefix in {"sh", "sz", "bj"}:
                            codes.append(code)
                    elif len(code) == 6 and code.isdigit():
                        market = "sh" if code.startswith("6") else "bj" if code.startswith(("8", "4")) else "sz"
                        codes.append(f"{market}.{code}")
                return codes

    @staticmethod
    def to_ts_code(code: str) -> str:
        raw = code.split(".")[-1] if "." in code else code
        prefix = code.split(".")[0] if "." in code else ""
        if prefix == "sh" or raw.startswith("6"):
            return f"{raw}.SH"
        if prefix == "bj" or raw.startswith("8") or raw.startswith("4"):
            return f"{raw}.BJ"
        return f"{raw}.SZ"

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if value == value else None

    def fetch_single_fundamental(self, code: str, result: Optional[FundamentalSyncResult] = None) -> Optional[FundamentalRecord]:
        ts_code = self.to_ts_code(code)
        fields = "ts_code,end_date,roe,roa,grossprofit_margin,profit_to_gr,or_yoy,netprofit_yoy,q_netprofit_yoy,profit_yoy"
        for period in self.periods:
            try:
                df = self.pro.fina_indicator(ts_code=ts_code, period=period, fields=fields)
                if df.empty:
                    continue
                latest = df.iloc[0]
                record = FundamentalRecord(
                    code=code,
                    roe=self._to_float(latest.get("roe")),
                    roa=self._to_float(latest.get("roa")),
                    grossprofit_margin=self._to_float(latest.get("grossprofit_margin")),
                    netprofit_margin=self._to_float(latest.get("profit_to_gr")),
                    revenue_yoy=self._to_float(latest.get("or_yoy")),
                    profit_yoy=(
                        self._to_float(latest.get("profit_yoy"))
                        or self._to_float(latest.get("netprofit_yoy"))
                        or self._to_float(latest.get("q_netprofit_yoy"))
                    ),
                    period=str(latest.get("end_date") or period),
                )
                if any(
                    value is not None
                    for value in [
                        record.roe,
                        record.roa,
                        record.grossprofit_margin,
                        record.netprofit_margin,
                        record.revenue_yoy,
                        record.profit_yoy,
                    ]
                ):
                    return record
            except Exception as e:
                message = str(e)
                if "最多访问" in message or "每分钟最多访问" in message or "频次" in message:
                    if result:
                        result.throttled += 1
                    time.sleep(10)
                    continue
                if result is not None:
                    result.failed += 1
                    result.failures.append(
                        SyncFailure(code=code, ts_code=ts_code, period=period, error=message[:300])
                    )
                return None
        return None

    def save_record(self, record: FundamentalRecord) -> None:
        sql = """
        UPDATE stock_basic
        SET roe = %s,
            roa = %s,
            grossprofit_margin = %s,
            netprofit_margin = %s,
            revenue_yoy = %s,
            profit_yoy = %s,
            fundamental_period = %s,
            fundamental_updated_at = NOW()
        WHERE code = %s
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        record.roe,
                        record.roa,
                        record.grossprofit_margin,
                        record.netprofit_margin,
                        record.revenue_yoy,
                        record.profit_yoy,
                        record.period,
                        record.code,
                    ),
                )

    @staticmethod
    def build_run_id() -> str:
        return f"fundamental_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def run(
        self,
        limit: Optional[int] = 50,
        only_missing: bool = True,
        stale_after_days: Optional[int] = 30,
    ) -> FundamentalSyncResult:
        self.ensure_columns()
        result = FundamentalSyncResult(run_id=self.build_run_id())
        self.task_logger.start(
            task_name="fundamental_sync",
            run_id=result.run_id,
            metadata={
                "limit": limit,
                "only_missing": only_missing,
                "stale_after_days": stale_after_days,
            },
        )

        try:
            codes = self.fetch_stock_codes(limit=limit, only_missing=only_missing, stale_after_days=stale_after_days)
            result.scanned = len(codes)

            for code in codes:
                record = self.fetch_single_fundamental(code, result=result)
                if not record:
                    result.no_data += 1
                    continue
                self.save_record(record)
                result.updated += 1
                time.sleep(self.sleep_seconds)

            result.finish()
            self.task_logger.finish(
                task_name="fundamental_sync",
                run_id=result.run_id,
                status="success",
                message=f"fundamental sync completed, updated={result.updated}",
                metadata=result.to_dict(),
            )
            return result
        except Exception as e:
            result.finish()
            self.task_logger.finish(
                task_name="fundamental_sync",
                run_id=result.run_id,
                status="failed",
                message=str(e)[:500],
                metadata=result.to_dict(),
            )
            raise


if __name__ == "__main__":
    sync = FundamentalSync()
    summary = sync.run()
    print(summary.to_dict())
