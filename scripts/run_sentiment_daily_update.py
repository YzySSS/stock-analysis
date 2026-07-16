from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for path in [PROJECT_ROOT, SRC_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger
from news_provider import NewsAggregator
from news_credibility import NewsCredibilityChecker
from news_filter import NewsFilter
from sentiment_factor import SentimentFactorCalculator

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


def normalize_datetime(value: object) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"]:
        try:
            return datetime.strptime(raw[:len(fmt)], fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return None


def quality_level(score: float) -> str:
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium"
    if score >= 40:
        return "low"
    return "very_low"


def credibility_level_from_score(score: float) -> str:
    score100 = score * 100 if score <= 1 else score
    if score100 >= 90:
        return "S"
    if score100 >= 70:
        return "A"
    if score100 >= 50:
        return "B"
    if score100 >= 30:
        return "C"
    return "D"


def credibility_reason(source: str, source_credibility: float, url_reason: str, url_credibility: float) -> str:
    return (
        f"来源名评分 {source or '未知'}={source_credibility:.2f}；"
        f"URL评分={url_credibility:.2f}（{url_reason}）"
    )[:255]


def save_news(code: str, news: list[dict], calculator: SentimentFactorCalculator, credibility_checker: NewsCredibilityChecker, news_filter: NewsFilter) -> tuple[int, int, int, float | None, float | None, float | None]:
    if not news:
        return 0, 0, 0, None, None, None
    pos = neg = 0
    scores: list[float] = []
    credibilities: list[float] = []
    qualities: list[float] = []
    sql = """
    INSERT INTO stock_news (
        code, title, summary, source, url, published_at, sentiment_score, credibility_score,
        credibility_level, credibility_reason, quality_score, quality_level, raw_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        summary=VALUES(summary), source=VALUES(source), published_at=VALUES(published_at),
        sentiment_score=VALUES(sentiment_score), credibility_score=VALUES(credibility_score),
        credibility_level=VALUES(credibility_level), credibility_reason=VALUES(credibility_reason),
        quality_score=VALUES(quality_score), quality_level=VALUES(quality_level), raw_json=VALUES(raw_json)
    """
    rows = []
    for item in news:
        text = f"{item.get('title','')} {item.get('content','')}"
        score = float(calculator._simple_sentiment(text))
        source_credibility = float(news_filter.get_source_credibility(item.get("source", "")))
        checked = credibility_checker.check_credibility(str(item.get("url") or ""), str(item.get("title") or ""))
        url_credibility = checked.score / 100
        # AkShare 新闻经常使用 eastmoney 聚合页承载第三方媒体正文；不能只看 URL 域名，
        # 否则大量新闻都会被压成东方财富/新浪同一个可信度。来源名与 URL 加权更稳定。
        credibility = (source_credibility * 0.65 + url_credibility * 0.35) if item.get("url") else source_credibility
        credibility_level = credibility_level_from_score(credibility)
        reason = credibility_reason(str(item.get("source") or ""), source_credibility, checked.reason, url_credibility)
        quality = float(item.get("quality_score") or news_filter.calculate_quality_score(item))
        if score > 0.05:
            pos += 1
        elif score < -0.05:
            neg += 1
        scores.append(score)
        credibilities.append(credibility)
        qualities.append(quality)
        rows.append((
            code,
            str(item.get("title") or "")[:512],
            str(item.get("content") or "")[:2000],
            str(item.get("source") or "")[:128],
            str(item.get("url") or "")[:1024],
            normalize_datetime(item.get("datetime")),
            round(score, 4),
            round(credibility, 4),
            credibility_level,
            reason,
            round(quality, 4),
            quality_level(quality),
            json.dumps(item, ensure_ascii=False, default=str),
        ))
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
    weighted = sum(s * c for s, c in zip(scores, credibilities)) / max(sum(credibilities), 1e-9)
    credibility_avg = sum(credibilities) / len(credibilities)
    quality_avg = sum(qualities) / len(qualities)
    return len(news), pos, neg, round(weighted, 4), round(credibility_avg, 4), round(quality_avg, 4)


def save_daily(code: str, trade_date: str, sentiment_score: float | None, news_count: int, positive_count: int, negative_count: int, credibility_avg: float | None, quality_avg: float | None, raw_news_count: int = 0) -> None:
    sql = """
    INSERT INTO stock_sentiment_daily (
        code, trade_date, sentiment_score, news_count, raw_news_count, filtered_news_count,
        positive_count, negative_count, credibility_avg, quality_avg, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        sentiment_score=VALUES(sentiment_score), news_count=VALUES(news_count), raw_news_count=VALUES(raw_news_count),
        filtered_news_count=VALUES(filtered_news_count), positive_count=VALUES(positive_count),
        negative_count=VALUES(negative_count), credibility_avg=VALUES(credibility_avg), quality_avg=VALUES(quality_avg),
        source=VALUES(source)
    """
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (code, trade_date, sentiment_score, news_count, raw_news_count, news_count, positive_count, negative_count, credibility_avg, quality_avg, "news_aggregator"))


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
        calculator = SentimentFactorCalculator(cache_dir=str(PROJECT_ROOT / "logs" / "sentiment_cache"))
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


if __name__ == "__main__":
    main()
