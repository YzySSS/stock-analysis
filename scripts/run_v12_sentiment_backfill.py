from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for path in [PROJECT_ROOT, SRC_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.orchestration.market_sentiment_schema import ensure_market_sentiment_schema
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger
from app.stock_selection.selector import StockSelector
from news_provider import NewsAggregator
from news_credibility import NewsCredibilityChecker
from news_filter import NewsFilter
from sentiment_factor import SentimentFactorCalculator

from scripts.run_sentiment_daily_update import save_daily, save_news

TASK_NAME = "v12_sentiment_backfill"


def trade_dates(start_date: str, end_date: str) -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT trade_date
                FROM factor_input_daily
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
                """,
                (start_date, end_date),
            )
            return [str(row["trade_date"]) for row in cursor.fetchall()]


def load_candidates_for_date(trade_date: str, instrument_type: str = "stock") -> list[dict[str, Any]]:
    selector = StockSelector(strategy_id="v12_legacy")
    sql = """
    SELECT
        sb.code,
        sb.name,
        sb.instrument_type,
        f.pe_tushare,
        f.pb_tushare,
        f.roe,
        f.roa,
        f.grossprofit_margin,
        f.netprofit_margin,
        f.revenue_yoy,
        f.profit_yoy,
        sb.eps,
        dk.close,
        dk.amount,
        dk.trade_date,
        ssd.sentiment_score,
        ssd.news_count,
        mcd.market_strength,
        mcd.market_state
    FROM factor_input_daily f
    INNER JOIN stock_basic sb ON sb.code = f.code
    INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
    LEFT JOIN stock_sentiment_daily ssd ON ssd.code = f.code AND ssd.trade_date = f.trade_date
    LEFT JOIN market_context_daily mcd ON mcd.trade_date = f.trade_date AND mcd.index_code = '000300.SH'
    WHERE f.trade_date = %s
      AND sb.instrument_type = %s
      AND sb.is_delisted = 0
      AND dk.close IS NOT NULL
    ORDER BY sb.code
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, instrument_type))
            rows = cursor.fetchall() or []
    return [selector._build_candidate(row) for row in rows]


def preliminary_v12_top(trade_date: str, limit: int, instrument_type: str = "stock") -> list[dict[str, Any]]:
    selector = StockSelector(strategy_id="v12_legacy")
    candidates = load_candidates_for_date(trade_date, instrument_type=instrument_type)
    computed = selector.strategy.compute_factors({"candidates": candidates})
    scored = selector.strategy.score(computed)
    return scored[:limit]


def existing_sentiment_codes(trade_date: str, min_news_count: int = 1) -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT code FROM stock_sentiment_daily WHERE trade_date=%s AND COALESCE(news_count,0) >= %s",
                (trade_date, min_news_count),
            )
            return {row["code"] for row in cursor.fetchall() or []}


def backfill(args: argparse.Namespace) -> dict[str, Any]:
    ensure_market_sentiment_schema()
    dates = trade_dates(args.start_date, args.end_date)
    aggregator = NewsAggregator()
    calculator = SentimentFactorCalculator(cache_dir=str(PROJECT_ROOT / "logs" / "sentiment_cache"))
    credibility_checker = NewsCredibilityChecker()

    totals = {
        "trade_dates": dates,
        "date_count": len(dates),
        "candidate_limit": args.candidate_limit,
        "updated": 0,
        "skipped_existing": 0,
        "failed": 0,
        "raw_news": 0,
        "effective_news": 0,
        "filtered_out": 0,
        "akshare_runs": 0,
        "tavily_runs": 0,
        "date_summaries": [],
    }

    for trade_date in dates:
        existing = existing_sentiment_codes(trade_date) if args.skip_existing else set()
        top_rows = preliminary_v12_top(trade_date, args.candidate_limit, instrument_type=args.instrument_type)
        day = {"trade_date": trade_date, "requested": len(top_rows), "updated": 0, "skipped_existing": 0, "failed": 0, "raw_news": 0, "effective_news": 0}
        print(json.dumps({"stage": "date_start", **day}, ensure_ascii=False), flush=True)
        for index, row in enumerate(top_rows, start=1):
            code = row["code"]
            if code in existing:
                day["skipped_existing"] += 1
                totals["skipped_existing"] += 1
                continue
            raw_code = code.split(".")[-1]
            news_filter = NewsFilter(min_credibility=args.min_credibility, max_age_days=args.max_age_days)
            try:
                raw_news = aggregator.get_stock_news(raw_code, row.get("name") or raw_code, sources=["akshare"])
                totals["akshare_runs"] += 1
                if not args.akshare_only and index <= args.tavily_top_n and aggregator.tavily.is_available():
                    fine_news = aggregator.get_stock_news(raw_code, row.get("name") or raw_code, sources=["tavily"])
                    raw_news = aggregator._deduplicate(raw_news + fine_news)
                    totals["tavily_runs"] += 1
                news = news_filter.filter(raw_news, raw_code, row.get("name") or raw_code, top_n=args.news_top_n)
                news_count, pos, neg, score, avg_cred, avg_quality = save_news(code, news, calculator, credibility_checker, news_filter)
                save_daily(code, trade_date, score, news_count, pos, neg, avg_cred, avg_quality, raw_news_count=len(raw_news))
                day["updated"] += 1
                day["raw_news"] += len(raw_news)
                day["effective_news"] += news_count
                totals["updated"] += 1
                totals["raw_news"] += len(raw_news)
                totals["effective_news"] += news_count
                totals["filtered_out"] += max(len(raw_news) - news_count, 0)
                print(json.dumps({"stage": "stock", "trade_date": trade_date, "index": index, "code": code, "raw_news": len(raw_news), "effective_news": news_count}, ensure_ascii=False), flush=True)
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
            except Exception as exc:
                day["failed"] += 1
                totals["failed"] += 1
                print(json.dumps({"stage": "stock_error", "trade_date": trade_date, "code": code, "error": str(exc)[:300]}, ensure_ascii=False), flush=True)
        totals["date_summaries"].append(day)
        print(json.dumps({"stage": "date_done", **day}, ensure_ascii=False), flush=True)
        if args.date_pause_seconds > 0:
            time.sleep(args.date_pause_seconds)
    return totals


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill V12 candidate sentiment by trade date")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--candidate-limit", type=int, default=50)
    parser.add_argument("--tavily-top-n", type=int, default=10)
    parser.add_argument("--news-top-n", type=int, default=10)
    parser.add_argument("--max-age-days", type=int, default=30)
    parser.add_argument("--min-credibility", type=float, default=0.35)
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--date-pause-seconds", type=float, default=2.0)
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--akshare-only", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_id = f"v12_sentiment_backfill_{args.start_date.replace('-', '')}_{args.end_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}"
    logger = TaskRunLogger()
    metadata = vars(args)
    logger.start(TASK_NAME, run_id, metadata)
    started = datetime.now()
    try:
        result = backfill(args)
        payload = {**metadata, **result, "run_id": run_id, "elapsed_seconds": round((datetime.now() - started).total_seconds(), 2)}
        logger.finish(TASK_NAME, run_id, "success", "v12 sentiment backfill completed", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], metadata)
        raise


if __name__ == "__main__":
    main()
