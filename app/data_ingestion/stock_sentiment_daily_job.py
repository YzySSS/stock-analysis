from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta

from app.data_ingestion.news_credibility import NewsCredibilityChecker
from app.data_ingestion.news_filter import NewsFilter
from app.data_ingestion.news_provider import NewsAggregator
from app.data_ingestion.sentiment_sync import LocalSentimentScorer, save_daily, save_news
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_sentiment_daily_update"


def resolve_trade_date() -> str:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            if row.get("trade_date"):
                return str(row["trade_date"])
    return (datetime.now().date() - timedelta(days=1)).isoformat()


def fetch_codes(limit: int, active_only: bool = True, universe: str = "selection_score") -> list[dict]:
    if universe == "selection_score":
        sql = """
        SELECT sb.code, sb.name, MAX(sr.score) AS selection_score
        FROM selection_result sr
        INNER JOIN stock_basic sb ON sr.code = sb.code
        WHERE sr.trade_date = (SELECT MAX(trade_date) FROM selection_result)
          AND sb.instrument_type='stock' AND sb.is_delisted=0 AND sb.is_st=0
        GROUP BY sb.code, sb.name
        ORDER BY selection_score DESC, sb.code
        LIMIT %s
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (limit,))
                rows = cursor.fetchall() or []
        if rows:
            return rows

    sql = """
    SELECT sb.code, sb.name, NULL AS selection_score
    FROM stock_basic sb
    WHERE sb.instrument_type='stock' AND sb.is_delisted=0 AND sb.is_st=0
    """
    if active_only:
        sql += " AND EXISTS (SELECT 1 FROM daily_kline dk WHERE dk.code=sb.code AND dk.trade_date=(SELECT MAX(trade_date) FROM daily_kline))"
    sql += " ORDER BY sb.code LIMIT %s"
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (limit,))
            return cursor.fetchall() or []



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--universe", choices=["selection_score", "stock_basic"], default="selection_score", help="selection_score uses latest selection_result by score, fallback to stock_basic")
    parser.add_argument("--tavily-top-n", type=int, default=50, help="run Tavily fine search only for the first N stocks in the selected universe")
    parser.add_argument("--akshare-only", action="store_true", help="disable Tavily fine search")
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    args = parser.parse_args()
    trade_date = args.trade_date or resolve_trade_date()
    run_id = f"stock_sentiment_{trade_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"trade_date": trade_date, "limit": args.limit, "universe": args.universe, "tavily_top_n": args.tavily_top_n, "akshare_only": args.akshare_only})
    try:
        aggregator = NewsAggregator()
        calculator = LocalSentimentScorer()
        credibility_checker = NewsCredibilityChecker()
        codes = fetch_codes(args.limit, universe=args.universe)
        updated = failed = total_news = raw_total_news = filtered_out = tavily_runs = akshare_runs = 0
        for index, row in enumerate(codes, start=1):
            code = row["code"]
            raw_code = code.split(".")[-1]
            try:
                # 去重状态必须按股票隔离，否则同一条市场新闻会影响后续股票的入库判断。
                news_filter = NewsFilter(min_credibility=0.35, max_age_days=7)
                raw_news = aggregator.get_stock_news(raw_code, row.get("name") or raw_code, sources=["akshare"])
                akshare_runs += 1
                if not args.akshare_only and index <= args.tavily_top_n and aggregator.tavily.is_available():
                    fine_news = aggregator.get_stock_news(raw_code, row.get("name") or raw_code, sources=["tavily"])
                    tavily_runs += 1
                    raw_news = aggregator._deduplicate(raw_news + fine_news)
                news = news_filter.filter(raw_news, raw_code, row.get("name") or raw_code, top_n=10)
                news_count, pos, neg, score, avg_cred, avg_quality = save_news(code, news, calculator, credibility_checker, news_filter)
                save_daily(code, trade_date, score, news_count, pos, neg, avg_cred, avg_quality, raw_news_count=len(raw_news))
                updated += 1
                total_news += news_count
                raw_total_news += len(raw_news)
                filtered_out += max(len(raw_news) - news_count, 0)
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
            except Exception:
                failed += 1
        payload = {"trade_date": trade_date, "requested": len(codes), "updated": updated, "failed": failed, "raw_news": raw_total_news, "total_news": total_news, "filtered_out": filtered_out, "universe": args.universe, "akshare_runs": akshare_runs, "tavily_runs": tavily_runs, "tavily_top_n": args.tavily_top_n}
        logger.finish(TASK_NAME, run_id, "success", f"stock sentiment updated, updated={updated}, news={total_news}, filtered={filtered_out}", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], {"trade_date": trade_date, "limit": args.limit})
        raise

