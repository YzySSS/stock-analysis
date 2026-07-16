from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Dict, List

from app.data_ingestion.news_credibility import NewsCredibilityChecker
from app.data_ingestion.news_filter import NewsFilter
from app.data_ingestion.news_provider import NewsAggregator
from app.data_ingestion.sentiment_sync import LocalSentimentScorer, save_daily, save_news
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger
from app.stock_selection.selector import StockSelector

TASK_NAME = "strategy_sentiment_refresh"
DEFAULT_STRATEGIES = ["fund_chip_repair", "quality_lowvol", "leader_tactics", "a_share_sentiment"]


def latest_trade_date() -> str:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            if row.get("trade_date"):
                return str(row["trade_date"])
    return datetime.now().date().isoformat()


def existing_sentiment_codes(trade_date: str, min_news_count: int = 1) -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code
                FROM stock_sentiment_daily
                WHERE trade_date=%s AND COALESCE(news_count, 0) >= %s
                """,
                (trade_date, min_news_count),
            )
            return {row["code"] for row in cursor.fetchall() or []}


def build_candidate_pool(strategies: List[str], top_per_strategy: int, score_threshold: float, include_existing: bool) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    by_code: Dict[str, Dict[str, Any]] = {}
    diagnostics: Dict[str, Any] = {}
    for strategy_id in strategies:
        selector = StockSelector(strategy_id=strategy_id, strategy_overrides={"score_threshold": score_threshold, "max_picks": top_per_strategy})
        bundle = selector.load_candidates_from_mysql(instrument_type="stock")
        context = selector.strategy.prepare_context(bundle)
        factor_rows = selector.strategy.compute_factors(context)
        scored = selector.strategy.score(factor_rows)
        selected = [item for item in scored if float(item.get("score") or 0) >= score_threshold][:top_per_strategy]
        diagnostics[strategy_id] = {
            "candidate_count": len(scored),
            "selected_for_refresh": len(selected),
            "diagnostics": {
                key: value for key, value in context.items()
                if key.endswith("_summary") or key.endswith("_diagnostics")
            },
        }
        for rank, item in enumerate(selected, start=1):
            code = item.get("code")
            if not code:
                continue
            current = by_code.get(code)
            score = float(item.get("score") or 0)
            if not current:
                by_code[code] = {
                    "code": code,
                    "name": item.get("name") or code,
                    "trade_date": item.get("trade_date"),
                    "best_score": score,
                    "strategies": [strategy_id],
                    "best_rank": rank,
                }
            else:
                current["best_score"] = max(float(current.get("best_score") or 0), score)
                current["best_rank"] = min(int(current.get("best_rank") or rank), rank)
                if strategy_id not in current["strategies"]:
                    current["strategies"].append(strategy_id)
    rows = list(by_code.values())
    rows.sort(key=lambda x: (len(x.get("strategies") or []), float(x.get("best_score") or 0), -int(x.get("best_rank") or 999999)), reverse=True)
    if not include_existing:
        trade_date = latest_trade_date()
        existing = existing_sentiment_codes(trade_date)
        rows = [row for row in rows if row.get("code") not in existing]
    return rows, diagnostics


def refresh_pool(rows: List[Dict[str, Any]], trade_date: str, tavily_top_n: int, sleep_seconds: float, news_top_n: int) -> Dict[str, Any]:
    aggregator = NewsAggregator()
    calculator = LocalSentimentScorer()
    credibility_checker = NewsCredibilityChecker()
    updated = failed = total_news = raw_total_news = filtered_out = tavily_runs = akshare_runs = 0
    errors = []
    for index, row in enumerate(rows, start=1):
        code = row["code"]
        raw_code = str(code).split(".")[-1]
        name = row.get("name") or raw_code
        news_filter = NewsFilter(min_credibility=0.35, max_age_days=7)
        try:
            raw_news = aggregator.get_stock_news(raw_code, name, sources=["akshare"])
            akshare_runs += 1
            if index <= tavily_top_n and aggregator.tavily.is_available():
                fine_news = aggregator.get_stock_news(raw_code, name, sources=["tavily"])
                tavily_runs += 1
                raw_news = aggregator._deduplicate(raw_news + fine_news)
            news = news_filter.filter(raw_news, raw_code, name, top_n=news_top_n)
            news_count, pos, neg, score, avg_cred, avg_quality = save_news(code, news, calculator, credibility_checker, news_filter)
            save_daily(code, trade_date, score, news_count, pos, neg, avg_cred, avg_quality, raw_news_count=len(raw_news))
            updated += 1
            total_news += news_count
            raw_total_news += len(raw_news)
            filtered_out += max(len(raw_news) - news_count, 0)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as exc:
            failed += 1
            if len(errors) < 10:
                errors.append({"code": code, "error": str(exc)[:300]})
    return {
        "trade_date": trade_date,
        "requested": len(rows),
        "updated": updated,
        "failed": failed,
        "raw_news": raw_total_news,
        "total_news": total_news,
        "filtered_out": filtered_out,
        "akshare_runs": akshare_runs,
        "tavily_runs": tavily_runs,
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh sentiment for strategy-relevant TopN candidate pool.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--top-per-strategy", type=int, default=80)
    parser.add_argument("--score-threshold", type=float, default=55)
    parser.add_argument("--max-codes", type=int, default=180)
    parser.add_argument("--tavily-top-n", type=int, default=30)
    parser.add_argument("--news-top-n", type=int, default=10)
    parser.add_argument("--sleep-seconds", type=float, default=0.15)
    parser.add_argument("--include-existing", action="store_true")
    args = parser.parse_args()

    trade_date = args.trade_date or latest_trade_date()
    strategies = [item.strip() for item in args.strategies.split(",") if item.strip()]
    run_id = f"strategy_sentiment_{trade_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}"
    logger = TaskRunLogger()
    meta = {"trade_date": trade_date, "strategies": strategies, "top_per_strategy": args.top_per_strategy, "max_codes": args.max_codes, "tavily_top_n": args.tavily_top_n}
    logger.start(TASK_NAME, run_id, meta)
    try:
        pool, diagnostics = build_candidate_pool(strategies, args.top_per_strategy, args.score_threshold, args.include_existing)
        pool = pool[: args.max_codes]
        result = refresh_pool(pool, trade_date, args.tavily_top_n, args.sleep_seconds, args.news_top_n)
        payload = {**result, "pool_size": len(pool), "strategies": strategies, "strategy_diagnostics": diagnostics, "codes": [row["code"] for row in pool]}
        logger.finish(TASK_NAME, run_id, "success", f"strategy sentiment refreshed, updated={result['updated']}, news={result['total_news']}", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], meta)
        raise

