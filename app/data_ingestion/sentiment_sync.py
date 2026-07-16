from __future__ import annotations

import json
from datetime import datetime
from typing import Protocol

from app.data_ingestion.news_credibility import NewsCredibilityChecker
from app.data_ingestion.news_filter import NewsFilter
from app.shared.db import mysql_conn


class SentimentScorer(Protocol):
    def score(self, text: str) -> float: ...


class LocalSentimentScorer:
    """Deterministic keyword scorer used by the persisted sentiment pipeline."""

    POSITIVE_WORDS = (
        "上涨", "涨停", "利好", "增长", "突破", "强势", "看好", "反弹",
        "增持", "买入", "推荐", "买入评级", "强烈推荐", "增持评级",
        "超预期", "龙头", "领涨", "创新高", "放量上涨", "资金流入",
        "业绩大增", "利润增长", "营收增长", "订单饱满", "产能扩张",
        "政策支持", "行业景气", "供需紧张", "涨价", "产品提价",
        "技术突破", "研发成功", "新药获批", "订单暴增", "中标",
        "并购重组", "资产注入", "股权激励", "回购", "分红",
        "北向资金流入", "机构加仓", "主力买入", "游资抢筹",
    )
    NEGATIVE_WORDS = (
        "下跌", "跌停", "利空", "下滑", "跌破", "弱势", "看空", "调整",
        "减持", "卖出", "回避", "卖出评级", "减持评级", "中性评级",
        "低于预期", "风险", "暴雷", "踩雷", "业绩下滑", "利润下降",
        "亏损", "亏损扩大", "营收下滑", "订单减少", "产能过剩",
        "政策打压", "行业低迷", "供过于求", "降价", "产品降价",
        "技术失败", "研发失败", "新药被拒", "订单取消", "失标",
        "分拆", "资产剥离", "股权质押", "爆仓", "债务违约",
        "北向资金流出", "机构减仓", "主力卖出", "游资出逃",
        "立案调查", "监管函", "问询函", "关注函", "警示函",
        "财务造假", "信披违规", "内幕交易", "操纵市场",
    )

    def score(self, text: str) -> float:
        if not text:
            return 0.0
        normalized = text.lower()
        positive_count = sum(1 for word in self.POSITIVE_WORDS if word in normalized)
        negative_count = sum(1 for word in self.NEGATIVE_WORDS if word in normalized)
        total = positive_count + negative_count
        if total == 0:
            return 0.0
        sentiment = (positive_count - negative_count) / max(total, 3)
        return max(-1.0, min(1.0, sentiment))


def normalize_datetime(value: object) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw[: len(fmt)], fmt).strftime("%Y-%m-%d %H:%M:%S")
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


def save_news(
    code: str,
    news: list[dict],
    scorer: SentimentScorer,
    credibility_checker: NewsCredibilityChecker,
    news_filter: NewsFilter,
) -> tuple[int, int, int, float | None, float | None, float | None]:
    if not news:
        return 0, 0, 0, None, None, None
    positive = negative = 0
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
        text = f"{item.get('title', '')} {item.get('content', '')}"
        score = float(scorer.score(text))
        source_credibility = float(news_filter.get_source_credibility(item.get("source", "")))
        checked = credibility_checker.check_credibility(str(item.get("url") or ""), str(item.get("title") or ""))
        url_credibility = checked.score / 100
        credibility = (
            source_credibility * 0.65 + url_credibility * 0.35
            if item.get("url")
            else source_credibility
        )
        level = credibility_level_from_score(credibility)
        reason = credibility_reason(
            str(item.get("source") or ""),
            source_credibility,
            checked.reason,
            url_credibility,
        )
        quality = float(item.get("quality_score") or news_filter.calculate_quality_score(item))
        if score > 0.05:
            positive += 1
        elif score < -0.05:
            negative += 1
        scores.append(score)
        credibilities.append(credibility)
        qualities.append(quality)
        rows.append(
            (
                code,
                str(item.get("title") or "")[:512],
                str(item.get("content") or "")[:2000],
                str(item.get("source") or "")[:128],
                str(item.get("url") or "")[:1024],
                normalize_datetime(item.get("datetime")),
                round(score, 4),
                round(credibility, 4),
                level,
                reason,
                round(quality, 4),
                quality_level(quality),
                json.dumps(item, ensure_ascii=False, default=str),
            )
        )
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
    weighted = sum(score * credibility for score, credibility in zip(scores, credibilities)) / max(
        sum(credibilities),
        1e-9,
    )
    credibility_avg = sum(credibilities) / len(credibilities)
    quality_avg = sum(qualities) / len(qualities)
    return (
        len(news),
        positive,
        negative,
        round(weighted, 4),
        round(credibility_avg, 4),
        round(quality_avg, 4),
    )


def save_daily(
    code: str,
    trade_date: str,
    sentiment_score: float | None,
    news_count: int,
    positive_count: int,
    negative_count: int,
    credibility_avg: float | None,
    quality_avg: float | None,
    raw_news_count: int = 0,
) -> None:
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
            cursor.execute(
                sql,
                (
                    code,
                    trade_date,
                    sentiment_score,
                    news_count,
                    raw_news_count,
                    news_count,
                    positive_count,
                    negative_count,
                    credibility_avg,
                    quality_avg,
                    "news_aggregator",
                ),
            )
