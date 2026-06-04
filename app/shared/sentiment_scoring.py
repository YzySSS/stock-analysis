from __future__ import annotations

from typing import Any


SOURCE_RATINGS: dict[str, tuple[str, float, str]] = {
    "exchange": ("S", 0.95, "交易所/监管公告源"),
    "announcement": ("S", 0.95, "公司公告源"),
    "cls": ("A", 0.86, "财联社快讯源"),
    "cls-hot": ("A", 0.86, "财联社热榜源"),
    "jin10": ("A", 0.82, "金十数据快讯源"),
    "wallstreetcn": ("A", 0.80, "华尔街见闻资讯源"),
    "gelonghui": ("B", 0.74, "格隆汇资讯源"),
    "xueqiu": ("B", 0.66, "雪球社区/行情源"),
    "baidu": ("B", 0.64, "百度聚合源"),
    "mktnews": ("B", 0.62, "市场新闻聚合源"),
    "toutiao": ("C", 0.55, "今日头条聚合源"),
    "weibo": ("C", 0.50, "微博社区源"),
    "zhihu": ("C", 0.50, "知乎社区源"),
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _source_key(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("source_id") or value.get("id") or value.get("source") or value.get("source_name")
    return str(value or "").strip().lower()


def score_source(source: Any) -> dict[str, Any]:
    key = _source_key(source)
    level, score, reason = SOURCE_RATINGS.get(key, ("C", 0.52, "未配置评级的普通新闻源"))
    return {
        "source_id": key or None,
        "credibility_level": level,
        "credibility_score": score,
        "credibility_reason": reason,
    }


def score_sources(sources: list[Any]) -> dict[str, Any]:
    scored = [score_source(source) for source in sources if source]
    if not scored:
        return {"credibility_level": "C", "credibility_score": 0.52, "credibility_reason": "暂无来源评级明细"}
    avg = sum(float(item["credibility_score"]) for item in scored) / len(scored)
    best_level = min((item["credibility_level"] for item in scored), default="C")
    if avg >= 0.88:
        level = "S"
    elif avg >= 0.76:
        level = "A"
    elif avg >= 0.62:
        level = "B"
    else:
        level = "C"
    top_sources = sorted({item.get("source_id") or "" for item in scored if item.get("source_id")})[:4]
    return {
        "credibility_level": level,
        "credibility_score": round(avg, 2),
        "credibility_reason": f"按信源评级表计算，覆盖来源：{', '.join(top_sources) or best_level}",
    }


def sentiment_from_news(item: dict[str, Any]) -> float:
    direction = str(item.get("direction") or "").lower()
    signed = _to_float(item.get("signed_score"), None)
    impact = _to_float(item.get("impact_score"), 50.0) or 50.0
    if signed is not None:
        return round(max(-1.0, min(1.0, signed / 100.0)), 4)
    if direction == "positive":
        return round(min(1.0, impact / 100.0), 4)
    if direction == "negative":
        return round(max(-1.0, -impact / 100.0), 4)
    return 0.0


def quality_from_news(item: dict[str, Any]) -> tuple[float | None, str | None]:
    impact = _to_float(item.get("impact_score"), None)
    timeliness = _to_float(item.get("timeliness_score"), None)
    values = [value for value in [impact, timeliness] if value is not None]
    if not values:
        return None, item.get("timeliness_level")
    score = round(sum(values) / len(values), 2)
    if score >= 85:
        level = "高"
    elif score >= 70:
        level = "中高"
    elif score >= 55:
        level = "中"
    else:
        level = "低"
    return score, level


def enrich_opinion_news_item(item: dict[str, Any], fallback_reason: str | None = None) -> dict[str, Any]:
    source_rating = score_source(item.get("source_id") or item.get("source") or item.get("source_name"))
    quality_score, quality_level = quality_from_news(item)
    sentiment_score = sentiment_from_news(item)
    reason = fallback_reason or source_rating["credibility_reason"]
    if item.get("event_type"):
        reason = f"{reason}；事件类型 {item.get('event_type')}"
    return {
        **item,
        "sentiment_score": sentiment_score,
        "credibility_score": source_rating["credibility_score"],
        "credibility_level": source_rating["credibility_level"],
        "credibility_reason": reason,
        "quality_score": quality_score,
        "quality_level": quality_level or item.get("timeliness_level"),
    }
