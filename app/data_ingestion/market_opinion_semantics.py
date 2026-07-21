from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

from app.shared.market_opinion_taxonomy import THEME_KEYWORDS


POSITIVE_DIRECTION_KEYWORDS: tuple[str, ...] = (
    "涨停",
    "大涨",
    "走强",
    "拉升",
    "上涨",
    "收涨",
    "新高",
    "反弹",
    "扭亏",
    "预增",
    "增长",
    "利好",
    "支持",
    "提振",
    "获批",
    "中标",
    "订单",
    "签约",
    "突破",
    "量产",
    "增持",
    "回购",
    "净流入",
)

NEGATIVE_DIRECTION_KEYWORDS: tuple[str, ...] = (
    "减持",
    "处罚",
    "问询",
    "立案",
    "退市",
    "亏损",
    "暴雷",
    "事故",
    "召回",
    "停产",
    "裁员",
    "制裁",
    "禁令",
    "造假",
    "下滑",
    "下跌",
    "走弱",
    "回落",
    "承压",
    "大跌",
    "跌停",
    "跌超",
    "下挫",
    "暴跌",
    "跌势",
    "跳水",
    "杀跌",
    "冲高回落",
    "风险提示",
)

CLAUSE_SPLIT_RE = re.compile(r"[。！？!?；;，,\n\r]+")
RELATION_FORWARD_TERMS = ("带动", "受益", "所属", "业务", "布局")


def normalize_opinion_text(value: str | None) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", value or ""))


def opinion_clauses(title: str | None, summary: str | None) -> list[str]:
    text = f"{title or ''}。{summary or ''}"
    return [normalize_opinion_text(part) for part in CLAUSE_SPLIT_RE.split(text) if normalize_opinion_text(part)]


def classify_opinion_direction(title: str | None, summary: str | None = None) -> str:
    title_text = normalize_opinion_text(title)
    summary_text = normalize_opinion_text(summary)

    def score(terms: Iterable[str]) -> float:
        return sum(title_text.count(term) * 2.0 + summary_text.count(term) for term in terms)

    positive = score(POSITIVE_DIRECTION_KEYWORDS)
    negative = score(NEGATIVE_DIRECTION_KEYWORDS)
    if negative > positive * 1.15 and negative > 0:
        return "negative"
    if positive > negative * 1.15 and positive > 0:
        return "positive"
    return "neutral"


def opinion_direction_multiplier(direction: str | None) -> float:
    return {"positive": 1.0, "negative": -1.0}.get(str(direction or "neutral"), 0.0)


@dataclass(frozen=True)
class StockSectorRelation:
    supported: bool
    score: float
    reason: str
    context: str = ""


@dataclass(frozen=True)
class OpinionDirectionContext:
    direction: str
    context: str = ""


def _contains_any(text: str, terms: Iterable[str]) -> bool:
    return any(normalize_opinion_text(term) in text for term in terms if normalize_opinion_text(term))


def classify_sector_direction(
    *,
    title: str | None,
    summary: str | None,
    sector_type: str | None,
    sector_name: str | None,
) -> OpinionDirectionContext:
    """Classify a sector from its local clauses, not the whole mixed article."""

    normalized_type = str(sector_type or "").strip()
    normalized_name = str(sector_name or "").strip()
    if normalized_type == "theme":
        terms: tuple[str, ...] = THEME_KEYWORDS.get(normalized_name, ())
    elif normalized_name:
        terms = (normalized_name,)
    else:
        terms = ()
    if not terms:
        return OpinionDirectionContext("neutral", "")

    clauses = opinion_clauses(title, summary)
    contexts: list[str] = []
    for index, clause in enumerate(clauses):
        if not _contains_any(clause, terms):
            continue
        context = clause
        if classify_opinion_direction(context, None) == "neutral" and index + 1 < len(clauses):
            context = f"{context}，{clauses[index + 1]}"
        if context not in contexts:
            contexts.append(context)
    if not contexts:
        return OpinionDirectionContext("neutral", "")
    context = "；".join(contexts)
    return OpinionDirectionContext(classify_opinion_direction(context, None), context)


def stock_sector_relation(
    *,
    title: str | None,
    summary: str | None,
    stock_name: str | None,
    stock_code: str | None,
    stock_industry: str | None,
    sector_type: str | None,
    sector_name: str | None,
) -> StockSectorRelation:
    """Validate one stock-sector edge without article-level Cartesian joins.

    A stock is linked to its own static industry directly.  Cross-industry and
    theme links require the stock and sector keyword to occur in the same
    clause, or in the immediately preceding topic clause ("医药走强，某股涨停").
    We deliberately do not use an arbitrary later clause, which is the common
    source of "某医药股，白酒板块..." false links.
    """

    sector_type = str(sector_type or "").strip()
    sector_name = str(sector_name or "").strip()
    stock_name = normalize_opinion_text(stock_name)
    stock_code = str(stock_code or "").split(".")[-1]
    stock_industry = str(stock_industry or "").strip()
    if not sector_type or not sector_name or (not stock_name and not stock_code):
        return StockSectorRelation(False, 0.0, "missing_relation_fields")

    clauses = opinion_clauses(title, summary)
    stock_indexes = [
        index
        for index, clause in enumerate(clauses)
        if (stock_name and stock_name in clause) or (stock_code and stock_code in clause)
    ]
    if sector_type == "industry" and stock_industry == sector_name:
        context = clauses[stock_indexes[0]] if stock_indexes else normalize_opinion_text(title)
        return StockSectorRelation(True, 100.0, "股票静态行业一致", context)

    terms: tuple[str, ...]
    if sector_type == "theme":
        terms = THEME_KEYWORDS.get(sector_name, ())
    else:
        terms = (sector_name,)
    if not terms:
        return StockSectorRelation(False, 0.0, "unknown_sector_taxonomy")

    for index in stock_indexes:
        clause = clauses[index]
        if _contains_any(clause, terms):
            return StockSectorRelation(True, 100.0, "股票与板块关键词同一子句", clause)
        if index > 0 and _contains_any(clauses[index - 1], terms):
            context = f"{clauses[index - 1]}，{clause}"
            return StockSectorRelation(True, 90.0, "前置板块子句直接引出股票", context)
        if index + 1 < len(clauses) and _contains_any(clauses[index + 1], terms):
            next_clause = clauses[index + 1]
            if _contains_any(next_clause, RELATION_FORWARD_TERMS):
                context = f"{clause}，{next_clause}"
                return StockSectorRelation(True, 82.0, "后置因果子句关联股票与板块", context)
    return StockSectorRelation(False, 0.0, "股票与板块仅在同一文章不同语义片段出现")
