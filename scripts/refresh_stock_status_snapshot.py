from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import akshare as ak

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

LOOKBACK_DAYS = 30
TASK_NAME = "stock_status_snapshot_refresh"


@dataclass
class StockStatusSnapshotRow:
    code: str
    trade_date: str
    status_label: str
    status_reason: str | None = None
    suspension_date: str | None = None
    resume_date: str | None = None
    paused_listing_date: str | None = None
    expected_resume_date: str | None = None
    source: str = "derived"


def normalize_date(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value)
    if raw in {"NaT", "None", ""}:
        return None
    return raw[:10]


def normalize_stock_code(code: object, market_name: object = None) -> str:
    symbol = str(code or "").strip()
    market = str(market_name or "").strip()
    if "北京" in market or symbol.startswith(("4", "8", "920")):
        prefix = "bj"
    elif "上海" in market or symbol.startswith("6"):
        prefix = "sh"
    elif "深圳" in market:
        prefix = "sz"
    else:
        prefix = "sz"
    return f"{prefix}.{symbol}"


def resolve_trade_date() -> str:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS latest_trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            latest_trade_date = row.get("latest_trade_date")
            if latest_trade_date:
                return str(latest_trade_date)
    return (date.today() - timedelta(days=1)).isoformat()


def fetch_paused_listing_map() -> dict[str, str]:
    try:
        df = ak.stock_info_sh_delist(symbol="全部")
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    result: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row.get("公司代码") or "").strip()
        paused_date = normalize_date(row.get("暂停上市日期"))
        if code and paused_date:
            result[code] = paused_date
    return result


def fetch_recent_suspension_map(trade_date: str) -> dict[str, dict]:
    latest_dt = datetime.strptime(trade_date, "%Y-%m-%d")
    result: dict[str, dict] = {}
    for days_back in range(LOOKBACK_DAYS + 1):
        query_date = (latest_dt - timedelta(days=days_back)).strftime("%Y%m%d")
        try:
            df = ak.stock_tfp_em(date=query_date)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            code = str(row.get("代码") or "").strip()
            if not code or code in result:
                continue
            result[code] = {
                "suspension_date": normalize_date(row.get("停牌时间")),
                "resume_date": normalize_date(row.get("停牌截止时间")),
                "reason": row.get("停牌原因"),
                "expected_resume_date": normalize_date(row.get("预计复牌时间")),
                "market_name": row.get("所属市场"),
            }
    return result


def build_snapshot_rows(trade_date: str) -> list[StockStatusSnapshotRow]:
    paused_map = fetch_paused_listing_map()
    suspension_map = fetch_recent_suspension_map(trade_date)

    rows: list[StockStatusSnapshotRow] = []
    seen: set[str] = set()

    for code, paused_listing_date in paused_map.items():
        seen.add(code)
        suspension = suspension_map.get(code, {})
        rows.append(
            StockStatusSnapshotRow(
                code=normalize_stock_code(code, "上海证券交易所"),
                trade_date=trade_date,
                status_label="paused_listing",
                status_reason=suspension.get("reason") or "暂停上市",
                suspension_date=suspension.get("suspension_date"),
                resume_date=suspension.get("resume_date"),
                paused_listing_date=paused_listing_date,
                expected_resume_date=suspension.get("expected_resume_date"),
                source="akshare",
            )
        )

    for code, suspension in suspension_map.items():
        if code in seen:
            continue
        rows.append(
            StockStatusSnapshotRow(
                code=normalize_stock_code(code, suspension.get("market_name")),
                trade_date=trade_date,
                status_label="suspended",
                status_reason=suspension.get("reason") or "停牌",
                suspension_date=suspension.get("suspension_date"),
                resume_date=suspension.get("resume_date"),
                expected_resume_date=suspension.get("expected_resume_date"),
                source="akshare",
            )
        )
    return rows


def save_snapshot(rows: list[StockStatusSnapshotRow]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO stock_status_snapshot (
        code, trade_date, status_label, status_reason,
        suspension_date, resume_date, paused_listing_date, expected_resume_date, source
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        status_label = VALUES(status_label),
        status_reason = VALUES(status_reason),
        suspension_date = VALUES(suspension_date),
        resume_date = VALUES(resume_date),
        paused_listing_date = VALUES(paused_listing_date),
        expected_resume_date = VALUES(expected_resume_date),
        source = VALUES(source)
    """
    data = [
        (
            row.code,
            row.trade_date,
            row.status_label,
            row.status_reason,
            row.suspension_date,
            row.resume_date,
            row.paused_listing_date,
            row.expected_resume_date,
            row.source,
        )
        for row in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, data)
    return len(rows)


def main() -> None:
    logger = TaskRunLogger()
    trade_date = resolve_trade_date()
    run_id = f"stock_status_snapshot_{trade_date.replace('-', '')}"
    metadata = {"trade_date": trade_date, "lookback_days": LOOKBACK_DAYS}
    logger.start(task_name=TASK_NAME, run_id=run_id, metadata=metadata)
    try:
        rows = build_snapshot_rows(trade_date)
        saved = save_snapshot(rows)
        payload = {**metadata, "saved": saved}
        logger.finish(
            task_name=TASK_NAME,
            run_id=run_id,
            status="success",
            message=f"stock status snapshot refreshed, saved={saved}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name=TASK_NAME,
            run_id=run_id,
            status="failed",
            message=str(exc)[:500],
            metadata=metadata,
        )
        raise


if __name__ == "__main__":
    main()
