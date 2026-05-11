from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
for path in [PROJECT_ROOT, SRC_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.orchestration.market_sentiment_schema import ensure_market_sentiment_schema
from app.shared.db import mysql_conn
from news_credibility import NewsCredibilityChecker
from news_filter import NewsFilter
from news_provider import NewsAggregator
from sentiment_factor import SentimentFactorCalculator
from scripts.run_sentiment_daily_update import save_daily, save_news


def _existing_sentiment_codes(trade_date: str, min_news_count: int = 1) -> set[str]:
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


def refresh_v12_candidate_sentiment(
    candidates: List[Dict[str, Any]],
    candidate_limit: int = 40,
    news_top_n: int = 10,
    max_age_days: int = 7,
    min_credibility: float = 0.35,
    sleep_seconds: float = 0.1,
    skip_existing: bool = False,
) -> Dict[str, Any]:
    """Refresh recent sentiment for preliminary V12 candidates.

    V12's product flow is intentionally two-stage:
    1. score the market with currently available/fallback sentiment;
    2. fetch fresh Tavily news for the preliminary top candidates;
    3. caller ranks the preliminary pool by the refreshed sentiment score.
    """

    ensure_market_sentiment_schema()
    rows = candidates[:candidate_limit]
    aggregator = NewsAggregator()
    calculator = SentimentFactorCalculator(cache_dir=str(PROJECT_ROOT / "logs" / "sentiment_cache"))
    credibility_checker = NewsCredibilityChecker()

    existing_by_date: dict[str, set[str]] = {}
    summary: Dict[str, Any] = {
        "enabled": True,
        "requested": len(rows),
        "candidate_limit": candidate_limit,
        "updated": 0,
        "skipped_existing": 0,
        "failed": 0,
        "raw_news": 0,
        "effective_news": 0,
        "filtered_out": 0,
        "akshare_runs": 0,
        "tavily_runs": 0,
        "source": "tavily",
        "news_top_n": news_top_n,
        "errors": [],
    }

    if not aggregator.tavily.is_available():
        summary["failed"] = len(rows)
        summary["errors"].append({"error": "TAVILY_API_KEY is not configured"})
        return summary

    for index, row in enumerate(rows, start=1):
        code = row.get("code")
        trade_date = row.get("trade_date")
        if not code or not trade_date:
            summary["failed"] += 1
            summary["errors"].append({"code": code, "error": "missing code/trade_date"})
            continue

        trade_date_text = str(trade_date)
        if skip_existing:
            if trade_date_text not in existing_by_date:
                existing_by_date[trade_date_text] = _existing_sentiment_codes(trade_date_text)
            if code in existing_by_date[trade_date_text]:
                summary["skipped_existing"] += 1
                continue

        raw_code = str(code).split(".")[-1]
        name = row.get("name") or raw_code
        news_filter = NewsFilter(min_credibility=min_credibility, max_age_days=max_age_days)
        try:
            raw_news = aggregator.get_stock_news(raw_code, name, sources=["tavily"])
            summary["tavily_runs"] += 1

            news = news_filter.filter(raw_news, raw_code, name, top_n=news_top_n)
            news_count, pos, neg, score, avg_cred, avg_quality = save_news(
                code, news, calculator, credibility_checker, news_filter
            )
            save_daily(
                code,
                trade_date_text,
                score,
                news_count,
                pos,
                neg,
                avg_cred,
                avg_quality,
                raw_news_count=len(raw_news),
            )
            summary["updated"] += 1
            summary["raw_news"] += len(raw_news)
            summary["effective_news"] += news_count
            summary["filtered_out"] += max(len(raw_news) - news_count, 0)
            if trade_date_text in existing_by_date and news_count > 0:
                existing_by_date[trade_date_text].add(code)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as exc:
            summary["failed"] += 1
            if len(summary["errors"]) < 5:
                summary["errors"].append({"code": code, "error": str(exc)[:300]})

    return summary
