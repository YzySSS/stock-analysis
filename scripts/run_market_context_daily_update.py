from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import tushare as ts

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "market_context_daily_update"


def _latest_trade_date() -> str:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            if row.get("trade_date"):
                return str(row["trade_date"])
    return (datetime.now().date() - timedelta(days=1)).isoformat()


def _local_breadth_score(trade_date: str) -> tuple[float | None, dict]:
    sql = """
    SELECT
        SUM(CASE WHEN cur.close > prev.close THEN 1 ELSE 0 END) AS up_count,
        COUNT(*) AS total_count,
        SUM(cur.amount) AS total_amount
    FROM daily_kline cur
    INNER JOIN (
        SELECT d1.code, d1.close
        FROM daily_kline d1
        INNER JOIN (
            SELECT code, MAX(trade_date) AS prev_date
            FROM daily_kline
            WHERE trade_date < %s
            GROUP BY code
        ) p ON d1.code = p.code AND d1.trade_date = p.prev_date
    ) prev ON cur.code = prev.code
    INNER JOIN stock_basic sb ON cur.code = sb.code
    WHERE cur.trade_date = %s AND sb.instrument_type='stock' AND sb.is_delisted=0
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, trade_date))
            row = cursor.fetchone() or {}
    total = int(row.get("total_count") or 0)
    up = int(row.get("up_count") or 0)
    score = round(up / total * 100, 2) if total else None
    return score, {"up_count": up, "total_count": total, "total_amount": float(row.get("total_amount") or 0)}


def _fetch_index_rows(index_code: str, trade_date: str, lookback: int = 60) -> list[dict]:
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN 未配置")
    pro = ts.pro_api(token)
    end = trade_date.replace("-", "")
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=lookback * 2)).strftime("%Y%m%d")
    df = pro.index_daily(ts_code=index_code, start_date=start, end_date=end, fields="ts_code,trade_date,close,pct_chg,vol,amount")
    if df is None or df.empty:
        return []
    rows = df.sort_values("trade_date").to_dict("records")
    return rows


def _score_market(index_rows: list[dict], breadth_score: float | None) -> dict:
    if not index_rows:
        return {
            "trend_score": None,
            "volume_score": None,
            "sentiment_score": None,
            "market_strength": breadth_score,
            "market_state": "neutral",
            "index_close": None,
            "index_pct_chg": None,
        }
    latest = index_rows[-1]
    closes = [float(row["close"]) for row in index_rows if row.get("close") == row.get("close")]
    vols = [float(row["vol"] or 0) for row in index_rows]
    close = float(latest.get("close") or 0)
    pct_chg = float(latest.get("pct_chg") or 0)
    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else close
    price_20 = closes[-20] if len(closes) >= 20 else closes[0]
    ret_20 = (close - price_20) / price_20 if price_20 else 0
    trend_score = max(0, min(100, 50 + ret_20 * 300 + (10 if close >= ma20 else -10)))
    vol_ma20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else (vols[-1] if vols else 0)
    volume_score = max(0, min(100, 50 + ((vols[-1] / vol_ma20 - 1) * 50 if vol_ma20 else 0)))
    sentiment_score = max(0, min(100, 50 + pct_chg * 5))
    breadth = breadth_score if breadth_score is not None else 50
    market_strength = round(trend_score * 0.4 + breadth * 0.3 + volume_score * 0.2 + sentiment_score * 0.1, 2)
    market_state = "bull" if market_strength >= 60 else "bear" if market_strength <= 40 else "neutral"
    return {
        "trend_score": round(trend_score, 2),
        "volume_score": round(volume_score, 2),
        "sentiment_score": round(sentiment_score, 2),
        "market_strength": market_strength,
        "market_state": market_state,
        "index_close": close,
        "index_pct_chg": pct_chg,
    }


def save_context(trade_date: str, index_code: str, payload: dict) -> None:
    sql = """
    INSERT INTO market_context_daily (
        trade_date, index_code, market_state, trend_score, breadth_score, volume_score,
        sentiment_score, market_strength, index_close, index_pct_chg, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        market_state=VALUES(market_state), trend_score=VALUES(trend_score), breadth_score=VALUES(breadth_score),
        volume_score=VALUES(volume_score), sentiment_score=VALUES(sentiment_score), market_strength=VALUES(market_strength),
        index_close=VALUES(index_close), index_pct_chg=VALUES(index_pct_chg), source=VALUES(source)
    """
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, index_code, payload.get("market_state"), payload.get("trend_score"), payload.get("breadth_score"), payload.get("volume_score"), payload.get("sentiment_score"), payload.get("market_strength"), payload.get("index_close"), payload.get("index_pct_chg"), "tushare.index_daily+daily_kline"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date")
    parser.add_argument("--index-code", default="000300.SH")
    args = parser.parse_args()
    trade_date = args.trade_date or _latest_trade_date()
    run_id = f"market_context_{trade_date.replace('-', '')}_{args.index_code.replace('.', '')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"trade_date": trade_date, "index_code": args.index_code})
    try:
        breadth_score, breadth_meta = _local_breadth_score(trade_date)
        index_rows = _fetch_index_rows(args.index_code, trade_date)
        payload = _score_market(index_rows, breadth_score)
        payload["breadth_score"] = breadth_score
        payload["breadth_meta"] = breadth_meta
        payload["trade_date"] = trade_date
        payload["index_code"] = args.index_code
        save_context(trade_date, args.index_code, payload)
        logger.finish(TASK_NAME, run_id, "success", f"market context updated, strength={payload.get('market_strength')}", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], {"trade_date": trade_date, "index_code": args.index_code})
        raise


if __name__ == "__main__":
    main()
