#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.newsnow_client import (  # noqa: E402
    DEFAULT_NEWSNOW_SOURCES,
    NEWSNOW_SOURCE_META,
    NewsNowClient,
    NewsNowItem,
)
from app.data_ingestion.market_opinion_repository import save_sector_summaries_normalized  # noqa: E402
from app.shared.db import mysql_conn  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402

TASK_NAME = "market_opinion_update"
LOCK_NAME = "market_opinion_update_lock"

NEGATIVE_KEYWORDS = [
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
    "大跌",
    "跌停",
    "跌超",
    "下挫",
    "跳水",
    "杀跌",
    "调整",
    "冲高回落",
    "风险提示",
]

EVENT_KEYWORDS: list[tuple[str, str, int]] = [
    ("policy", r"政策|国常会|发改委|工信部|财政部|商务部|央行|证监会|国务院|印发|支持|试点|补贴", 30),
    ("major_order", r"重大订单|订单|中标|签约|采购|供应商|合同", 26),
    ("ma_restructure", r"并购|重组|收购|注入|借壳|定增", 28),
    ("earnings", r"业绩|预增|扭亏|利润|营收|财报|分红", 22),
    ("price_hike", r"涨价|提价|供不应求|缺货|库存下降", 22),
    ("tech_breakthrough", r"突破|首发|发布|量产|投产|获批|临床|专利|研发", 20),
    ("hot_theme", r"人工智能|AI|大模型|算力|数据中心|机器人|半导体|芯片|低空经济|固态电池|新能源|光伏|储能|军工|卫星", 20),
    ("market_attention", r"热搜|热门|爆火|涨停|异动|拉升|资金流入|主力", 14),
]

SHORT_LIVED_RISK_PATTERN = re.compile(r"火灾|爆炸|事故|停产|召回|突发|闪崩|跌停|大跌|传闻|网传|辟谣|澄清|异动", re.I)

TIME_DECAY_PROFILES: dict[str, dict[str, Any]] = {
    # Policy and industrial-planning items can shape a theme for months, but
    # they still decay so stale policy does not dominate fresh market action.
    "policy": {"fresh": 7, "active": 30, "cooling": 180, "expires": 365, "long_lived": True},
    "ma_restructure": {"fresh": 3, "active": 14, "cooling": 60, "expires": 180, "long_lived": True},
    "major_order": {"fresh": 3, "active": 7, "cooling": 30, "expires": 90, "long_lived": False},
    "earnings": {"fresh": 3, "active": 7, "cooling": 30, "expires": 90, "long_lived": False},
    "price_hike": {"fresh": 3, "active": 14, "cooling": 45, "expires": 120, "long_lived": False},
    "tech_breakthrough": {"fresh": 7, "active": 30, "cooling": 120, "expires": 365, "long_lived": True},
    "hot_theme": {"fresh": 1, "active": 3, "cooling": 7, "expires": 30, "long_lived": False},
    "market_attention": {"fresh": 1, "active": 3, "cooling": 7, "expires": 14, "long_lived": False},
    "negative_risk": {"fresh": 1, "active": 3, "cooling": 7, "expires": 30, "long_lived": False},
    "short_lived_risk": {"fresh": 1, "active": 3, "cooling": 7, "expires": 14, "long_lived": False},
    "general": {"fresh": 1, "active": 3, "cooling": 7, "expires": 30, "long_lived": False},
}

THEME_KEYWORDS: dict[str, list[str]] = {
    "AI算力": ["人工智能", "AI", "大模型", "算力", "数据中心", "GPU", "云计算", "智能体"],
    "机器人": ["机器人", "人形机器人", "具身智能", "减速器", "伺服", "机器视觉"],
    "半导体": ["半导体", "芯片", "国产芯片", "晶圆", "光刻", "光刻胶", "封测", "存储", "先进封装", "PCB", "EDA"],
    "新能源车": ["新能源车", "汽车", "智能驾驶", "自动驾驶", "车路云", "无人驾驶", "车企"],
    "锂电池": ["锂电", "电池", "固态电池", "钠电池", "储能", "正极", "负极", "电解液"],
    "绿电": ["绿电", "绿色电力", "绿电直连", "新型电力系统", "虚拟电厂", "特高压", "风电", "光伏", "水电", "核电"],
    "低空经济": ["低空经济", "无人机", "eVTOL", "飞行汽车", "通航"],
    "军工航天": ["军工", "航天", "卫星", "商业航天", "火箭", "北斗"],
    "医药": ["医药", "创新药", "减肥药", "疫苗", "医疗器械", "CXO", "药企"],
    "有色金属": ["有色", "铜", "铝", "黄金", "稀土", "锂矿", "钴", "镍"],
    "化工农业": ["化肥", "农药", "粮食", "种业", "农产品", "尿素"],
    "油气": ["石油", "原油", "燃油", "油价", "天然气", "LNG"],
    # Keep the financial theme narrow. Plain "证券" often appears as a research
    # house speaker (e.g. "中信证券：看好AI") or in overseas/SEC headlines, while
    # generic central-bank/rate-cut news is macro liquidity rather than a direct
    # A-share brokerage/financial-stock catalyst.
    "金融": ["证券公司", "证券板块", "证券股", "券商", "券商ETF", "中证协", "保险", "银行股", "银行指数", "银行板块", "并购重组"],
    "房地产": ["房地产", "地产", "楼市", "房贷", "城中村"],
    "消费": ["消费", "白酒", "食品", "旅游", "免税", "零售", "餐饮"],
    "传媒游戏": ["游戏", "短剧", "影视", "传媒", "版权", "广告"],
}

THEME_INDUSTRY_HINTS: dict[str, list[str]] = {
    "AI算力": ["软件服务", "通信设备", "IT设备", "半导体", "元器件", "互联网"],
    "机器人": ["专用机械", "机械基件", "电器仪表", "元器件", "软件服务"],
    "半导体": ["半导体", "元器件", "IT设备", "互联网"],
    "新能源车": ["汽车整车", "汽车配件", "电气设备", "元器件", "IT设备"],
    "锂电池": ["电气设备", "化工原料", "小金属", "汽车配件"],
    "绿电": ["新型电力", "水力发电", "电气设备"],
    "低空经济": ["航空", "通信设备", "运输设备", "专用机械"],
    "军工航天": ["航空", "船舶", "通信设备", "专用机械"],
    "医药": ["化学制药", "生物制药", "医疗保健", "中成药", "医药商业"],
    "有色金属": ["小金属", "铜", "铝", "铅锌", "黄金"],
    "化工农业": ["农药化肥", "种植业", "农业综合", "饲料", "化工原料"],
    "油气": ["石油开采", "石油加工", "供气供热"],
    "金融": ["证券", "银行", "保险", "多元金融"],
    "房地产": ["全国地产", "区域地产", "房产服务", "园区开发"],
    "消费": ["食品", "白酒", "旅游景点", "旅游服务", "家用电器", "百货"],
    "传媒游戏": ["影视音像", "出版业", "广告包装", "互联网", "文教休闲"],
}

GENERIC_INDUSTRIES = {"互联网", "综合类", "其他行业"}

RESEARCH_HOUSE_SPEAKERS = {
    "中信证券",
    "中信建投",
    "中金公司",
    "国泰君安",
    "招商证券",
    "华泰证券",
    "海通证券",
    "广发证券",
    "东方证券",
    "太平洋",
}

AMBIGUOUS_STOCK_NAME_ALIASES = {
    # Listed-company short names that are also broad market themes/common nouns.
    # Matching these by name alone turns sector news into fake stock-specific news.
    "机器人",
    "太阳能",
    "太平洋",
    "线上线下",
    "新华网",
    "人民网",
}

DATA_PROVIDER_SPEAKERS = {
    # Data vendors can be the speaker/source of commodity-price news. In titles
    # like "上海钢联：碳酸锂价格上涨", the stock is not the event subject.
    "上海钢联",
}


@dataclass(frozen=True)
class StockRef:
    code: str
    name: str
    industry: str | None
    aliases: tuple[str, ...]


@dataclass
class MysqlLockHandle:
    conn_context: Any
    conn: Any


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return round(max(low, min(value, high)), 4)


def acquire_lock() -> MysqlLockHandle | None:
    conn_context = mysql_conn()
    conn = conn_context.__enter__()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS locked", (LOCK_NAME,))
            row = cursor.fetchone() or {}
            locked = int(row.get("locked") or 0) == 1
        if locked:
            return MysqlLockHandle(conn_context=conn_context, conn=conn)
    except Exception:
        conn_context.__exit__(*sys.exc_info())
        raise

    conn_context.__exit__(None, None, None)
    return None


def release_lock(lock_handle: MysqlLockHandle | None) -> None:
    if lock_handle is None:
        return

    release_error: Exception | None = None
    try:
        with lock_handle.conn.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
    except Exception as exc:
        release_error = exc
    try:
        lock_handle.conn_context.__exit__(None, None, None)
    except Exception as exc:
        release_error = release_error or exc
    if release_error:
        print(
            json.dumps(
                {"status": "warning", "reason": "release_lock_failed", "error": str(release_error)[:300]},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )


def parse_datetime_arg(value: str | None) -> datetime:
    if not value:
        return datetime.now()
    text = value.strip().replace("T", " ")
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return datetime.strptime(text[: len(fmt)], fmt)
        except Exception:
            pass
    raise ValueError(f"invalid datetime: {value}")


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", value or ""))


def is_research_house_speaker(text: str, alias: str | None = None) -> bool:
    """Return True when a brokerage name is only the report publisher/speaker.

    Headlines like "中信证券：继续看好AIDC产业链" should not be treated as
    positive opinion for the listed broker itself, nor as evidence that the
    brokerage sector is hot. The subject is the covered industry/stock after the
    colon, while the broker is only the author.
    """
    compact = normalize_text(text)
    if alias and alias in RESEARCH_HOUSE_SPEAKERS:
        pattern = rf"(?:^|[丨｜]){re.escape(normalize_text(alias))}(?:国际|\(香港\)|（香港）)?[:：]"
        if re.search(pattern, compact):
            return True
    broker_suffix = r"(?:国际|\(香港\)|（香港）)?"
    if alias and alias.endswith("证券"):
        pattern = rf"(?:^|[丨｜]){re.escape(normalize_text(alias))}{broker_suffix}[:：]"
        if re.search(pattern, compact):
            return True
    return bool(re.search(r"(?:^|[丨｜])[^丨｜：:]{0,12}证券(?:国际|\(香港\)|（香港）)?[:：]", compact))


def is_data_provider_speaker(text: str, alias: str | None = None) -> bool:
    compact = normalize_text(text)
    if alias and alias in DATA_PROVIDER_SPEAKERS:
        escaped = re.escape(normalize_text(alias))
        return bool(re.search(rf"(?:^|[丨｜]){escaped}(?:[:：]|发布数据|发布显示|数据显示|监测显示)", compact))
    return False


def load_stock_refs() -> tuple[list[StockRef], list[str]]:
    sql = """
    SELECT code, name, industry
    FROM stock_basic
    WHERE instrument_type='stock' AND is_delisted=0 AND is_st=0
      AND name IS NOT NULL AND name <> ''
    """
    refs: list[StockRef] = []
    industries: set[str] = set()
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor.fetchall() or []:
                name = str(row.get("name") or "").strip()
                if not name:
                    continue
                industry = str(row.get("industry") or "").strip() or None
                if industry:
                    industries.add(industry)
                clean_name = name.replace("*", "").replace("ST", "").strip()
                aliases = {name, clean_name, row["code"].split(".")[-1]}
                aliases = {a for a in aliases if a not in AMBIGUOUS_STOCK_NAME_ALIASES}
                # Two-character stock names have many false positives in news titles.
                aliases = {a for a in aliases if a and (len(a) >= 3 or a.isdigit())}
                if not aliases:
                    continue
                refs.append(StockRef(code=row["code"], name=name, industry=industry, aliases=tuple(sorted(aliases, key=len, reverse=True))))
    return refs, sorted(industries)


def source_score(item: NewsNowItem) -> float:
    meta = NEWSNOW_SOURCE_META.get(item.source_id, {})
    score = float(meta.get("base_score") or 55)
    if item.source_type == "realtime":
        score += 2
    if item.status == "cache":
        score -= 2
    return clamp(score)


def importance_score(title: str, summary: str | None = None) -> tuple[float, str, str]:
    text = f"{title} {summary or ''}"
    direction = "neutral"
    score = 42.0
    event_type = "general"
    if any(keyword in text for keyword in NEGATIVE_KEYWORDS):
        direction = "negative"
        score += 18
        event_type = "short_lived_risk" if SHORT_LIVED_RISK_PATTERN.search(text) else "negative_risk"
    for kind, pattern, bonus in EVENT_KEYWORDS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            score += bonus
            if event_type == "general":
                event_type = kind
            elif direction != "negative" and bonus >= 20:
                event_type = kind
    if re.search(r"公告|官方|确认|发布会|监管", text):
        score += 8
    if re.search(r"传闻|网传|据称|或将|可能", text):
        score -= 10
    if direction == "neutral" and score >= 65:
        direction = "positive"
    return clamp(score), direction, event_type


def timeliness_score(event_type: str | None, usable_at: datetime, as_of: datetime) -> dict[str, Any]:
    profile = TIME_DECAY_PROFILES.get(event_type or "general", TIME_DECAY_PROFILES["general"])
    age_days = max((as_of - usable_at).total_seconds() / 86400, 0.0)
    fresh = float(profile["fresh"])
    active = float(profile["active"])
    cooling = float(profile["cooling"])
    expires = float(profile["expires"])

    if age_days <= fresh:
        score = 100.0
        level = "1天内" if fresh <= 1 else "新鲜"
    elif age_days <= active:
        ratio = (age_days - fresh) / max(active - fresh, 0.0001)
        score = 92.0 - ratio * 12.0
        level = "3天内" if active <= 3 else "活跃"
    elif age_days <= cooling:
        ratio = (age_days - active) / max(cooling - active, 0.0001)
        score = 78.0 - ratio * 28.0
        level = "7天内" if cooling <= 7 else "降温"
    elif age_days <= expires:
        ratio = (age_days - cooling) / max(expires - cooling, 0.0001)
        floor = 20.0 if profile.get("long_lived") else 0.0
        score = 48.0 - ratio * (48.0 - floor)
        level = "一个月内" if expires <= 30 else "长尾"
    else:
        score = 0.0
        level = "失效"

    return {
        "score": clamp(score),
        "level": level,
        "age_days": round(age_days, 3),
        "expires_days": int(expires),
        "effective_until": usable_at + timedelta(days=expires),
    }


def timeliness_level(score: float) -> str:
    if score >= 90:
        return "高时效"
    if score >= 70:
        return "有效"
    if score >= 45:
        return "降温"
    if score > 0:
        return "长尾"
    return "失效"


def amplification_score(item: NewsNowItem) -> float:
    rank = item.rank_no or 30
    rank_score = max(25.0, 100.0 - (rank - 1) * 3.0)
    if item.source_type == "hottest":
        rank_score += 5
    if item.source_column == "finance":
        rank_score += 3
    return clamp(rank_score)


def impact_score(src: float, importance: float, amplification: float, timeliness: float = 100.0) -> float:
    base = src * 0.25 + importance * 0.38 + amplification * 0.22 + timeliness * 0.15
    return clamp(base * (0.35 + 0.65 * max(timeliness, 0) / 100))


def match_stocks(item: NewsNowItem, refs: list[StockRef]) -> list[dict[str, Any]]:
    text = normalize_text(f"{item.title} {item.summary or ''}")
    matches: list[dict[str, Any]] = []
    for ref in refs:
        hit_alias = next((alias for alias in ref.aliases if normalize_text(alias) in text), None)
        if not hit_alias:
            continue
        if is_research_house_speaker(item.title, ref.name):
            continue
        if is_data_provider_speaker(item.title, ref.name):
            continue
        score = 92.0 if hit_alias == ref.name else 82.0 if hit_alias.isdigit() else 76.0
        matches.append(
            {
                "code": ref.code,
                "name": ref.name,
                "industry": ref.industry,
                "match_score": score,
                "match_reason": f"标题/摘要命中股票别名：{hit_alias}",
            }
        )
    return matches[:20]


def match_sectors(item: NewsNowItem, stock_matches: list[dict[str, Any]], industries: list[str]) -> list[dict[str, Any]]:
    text = normalize_text(f"{item.title} {item.summary or ''}")
    sectors: dict[tuple[str, str], dict[str, Any]] = {}
    for match in stock_matches:
        industry = match.get("industry")
        if industry:
            key = ("industry", industry)
            sectors[key] = {"sector_type": "industry", "sector_name": industry, "match_score": 82.0, "match_reason": "由命中股票所属行业映射"}
    for industry in industries:
        if industry in GENERIC_INDUSTRIES:
            continue
        if len(industry) >= 3 and normalize_text(industry) in text:
            key = ("industry", industry)
            sectors.setdefault(key, {"sector_type": "industry", "sector_name": industry, "match_score": 78.0, "match_reason": "标题/摘要直接命中行业名"})
    for theme, keywords in THEME_KEYWORDS.items():
        hit = next((kw for kw in keywords if normalize_text(kw) and normalize_text(kw) in text), None)
        if hit:
            if theme == "金融" and is_research_house_speaker(item.title):
                continue
            key = ("theme", theme)
            sectors[key] = {"sector_type": "theme", "sector_name": theme, "match_score": 74.0, "match_reason": f"主题关键词命中：{hit}"}
    return list(sectors.values())[:20]


def save_raw_item(
    item: NewsNowItem,
    src_score: float,
    imp_score: float,
    amp_score: float,
    time_score: float,
    time_level: str,
    effective_until: datetime,
    final_score: float,
    direction: str,
    event_type: str,
) -> int:
    sql = """
    INSERT INTO market_opinion_raw (
        source_id, source_name, source_column, source_type, rank_no, item_id, title, summary,
        url, mobile_url, published_at, crawl_time, first_seen_at, last_seen_at, trade_date, status, source_score,
        importance_score, amplification_score, timeliness_score, timeliness_level, effective_until,
        impact_score, direction, event_type, title_hash, raw_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        id=LAST_INSERT_ID(id), source_name=VALUES(source_name), source_column=VALUES(source_column),
        source_type=VALUES(source_type), rank_no=VALUES(rank_no), item_id=VALUES(item_id),
        summary=VALUES(summary), url=VALUES(url), mobile_url=VALUES(mobile_url),
        published_at=CASE
            WHEN published_at IS NULL THEN VALUES(published_at)
            WHEN VALUES(published_at) IS NULL THEN published_at
            WHEN VALUES(published_at) < published_at THEN VALUES(published_at)
            ELSE published_at
        END,
        crawl_time=VALUES(crawl_time),
        first_seen_at=CASE
            WHEN first_seen_at IS NULL THEN VALUES(first_seen_at)
            WHEN VALUES(first_seen_at) IS NULL THEN first_seen_at
            WHEN VALUES(first_seen_at) < first_seen_at THEN VALUES(first_seen_at)
            ELSE first_seen_at
        END,
        last_seen_at=VALUES(last_seen_at),
        trade_date=LEAST(trade_date, VALUES(trade_date)),
        status=VALUES(status), source_score=VALUES(source_score), importance_score=VALUES(importance_score),
        amplification_score=VALUES(amplification_score), timeliness_score=VALUES(timeliness_score),
        timeliness_level=VALUES(timeliness_level), effective_until=VALUES(effective_until),
        impact_score=VALUES(impact_score),
        direction=VALUES(direction), event_type=VALUES(event_type), raw_json=VALUES(raw_json)
    """
    effective_time = item.effective_time
    values = (
        item.source_id,
        item.source_name,
        item.source_column,
        item.source_type,
        item.rank_no,
        item.item_id,
        item.title,
        item.summary,
        item.url,
        item.mobile_url,
        effective_time.strftime("%Y-%m-%d %H:%M:%S"),
        item.crawl_time.strftime("%Y-%m-%d %H:%M:%S"),
        item.crawl_time.strftime("%Y-%m-%d %H:%M:%S"),
        item.crawl_time.strftime("%Y-%m-%d %H:%M:%S"),
        effective_time.date().isoformat(),
        item.status,
        src_score,
        imp_score,
        amp_score,
        time_score,
        time_level,
        effective_until.strftime("%Y-%m-%d %H:%M:%S"),
        final_score,
        direction,
        event_type,
        item.title_hash,
        json.dumps(item.raw, ensure_ascii=False, default=str),
    )
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            return int(cursor.lastrowid)


def replace_matches(raw_id: int, stock_matches: list[dict[str, Any]], sector_matches: list[dict[str, Any]]) -> None:
    stock_sql = """
    INSERT INTO market_opinion_stock_match (raw_id, code, name, industry, match_score, match_reason)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE name=VALUES(name), industry=VALUES(industry), match_score=VALUES(match_score), match_reason=VALUES(match_reason)
    """
    sector_sql = """
    INSERT INTO market_opinion_sector_match (raw_id, sector_type, sector_name, match_score, match_reason)
    VALUES (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE match_score=VALUES(match_score), match_reason=VALUES(match_reason)
    """
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM market_opinion_stock_match WHERE raw_id=%s", (raw_id,))
            cursor.execute("DELETE FROM market_opinion_sector_match WHERE raw_id=%s", (raw_id,))
            if stock_matches:
                cursor.executemany(
                    stock_sql,
                    [(raw_id, m["code"], m.get("name"), m.get("industry"), m.get("match_score"), m.get("match_reason")) for m in stock_matches],
                )
            if sector_matches:
                cursor.executemany(
                    sector_sql,
                    [(raw_id, m["sector_type"], m["sector_name"], m.get("match_score"), m.get("match_reason")) for m in sector_matches],
                )


def aggregate_sectors(as_of: datetime, lookback_days: int) -> list[dict[str, Any]]:
    max_valid_days = max(int(profile["expires"]) for profile in TIME_DECAY_PROFILES.values())
    start_at = as_of - timedelta(days=max(lookback_days, max_valid_days))
    sql = """
    SELECT
        sec.sector_type, sec.sector_name, sec.match_score AS sector_match_score,
        r.id AS raw_id, r.title, r.summary, r.source_id, r.source_name, r.impact_score, r.direction, r.event_type,
        r.source_score, r.importance_score, r.amplification_score,
        COALESCE(r.published_at, r.first_seen_at, r.crawl_time) AS usable_at,
        sm.code, sm.name, sm.industry, sm.match_score AS stock_match_score, sm.match_reason AS stock_match_reason
    FROM market_opinion_sector_match sec
    INNER JOIN market_opinion_raw r ON r.id = sec.raw_id
    LEFT JOIN market_opinion_stock_match sm ON sm.raw_id = r.id
    WHERE COALESCE(r.published_at, r.first_seen_at, r.crawl_time) >= %s
      AND COALESCE(r.published_at, r.first_seen_at, r.crawl_time) <= %s
    ORDER BY sec.sector_type, sec.sector_name, r.impact_score DESC, usable_at DESC
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (start_at.strftime("%Y-%m-%d %H:%M:%S"), as_of.strftime("%Y-%m-%d %H:%M:%S")))
            rows = cursor.fetchall() or []

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["sector_type"], row["sector_name"])
        bucket = grouped.setdefault(
            key,
            {
                "sector_type": row["sector_type"],
                "sector_name": row["sector_name"],
                "raw": {},
                "sources": set(),
                "stocks": {},
                "positive_news_count": 0,
                "negative_news_count": 0,
            },
        )
        raw_id = row["raw_id"]
        if raw_id not in bucket["raw"]:
            usable_at = row["usable_at"]
            if isinstance(usable_at, str):
                usable_at_dt = datetime.fromisoformat(usable_at)
            else:
                usable_at_dt = usable_at
            time_info = timeliness_score(row.get("event_type"), usable_at_dt, as_of)
            if time_info["score"] <= 0:
                continue
            current_impact = impact_score(
                float(row.get("source_score") or 0),
                float(row.get("importance_score") or 0),
                float(row.get("amplification_score") or 0),
                float(time_info["score"] or 0),
            )
            signed = -current_impact if row.get("direction") == "negative" else current_impact
            bucket["raw"][raw_id] = {
                "raw_id": raw_id,
                "title": row["title"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "impact_score": current_impact,
                "signed_score": signed,
                "direction": row["direction"],
                "event_type": row["event_type"],
                "published_at": str(row["usable_at"]),
                "timeliness_score": time_info["score"],
                "timeliness_level": time_info["level"],
                "age_days": time_info["age_days"],
                "effective_until": time_info["effective_until"].strftime("%Y-%m-%d %H:%M:%S"),
            }
            bucket["sources"].add(row["source_id"])
            if row.get("direction") == "negative":
                bucket["negative_news_count"] += 1
            elif row.get("direction") == "positive":
                bucket["positive_news_count"] += 1
        if row.get("code"):
            news = bucket["raw"].get(raw_id)
            if not news:
                continue
            stock_name = str(row.get("name") or "").strip()
            raw_text = normalize_text(f"{row.get('title') or ''} {row.get('summary') or ''}")
            code_digits = str(row.get("code") or "").split(".")[-1]
            if stock_name and normalize_text(stock_name) not in raw_text and code_digits not in raw_text:
                continue
            stock = bucket["stocks"].setdefault(
                row["code"],
                {
                    "code": row["code"],
                    "name": row.get("name"),
                    "industry": row.get("industry"),
                    "score": 0.0,
                    "news_count": 0,
                    "matched_news": [],
                    "match_type": "direct_news_match",
                    "match_reason": row.get("stock_match_reason") or "新闻直接命中股票",
                },
            )
            stock["score"] += float(news["impact_score"] or 0) * float(row.get("stock_match_score") or 70) / 100
            stock["news_count"] += 1
            stock["matched_news"].append(news)

    summaries: list[dict[str, Any]] = []
    for bucket in grouped.values():
        news_items = list(bucket["raw"].values())
        if not news_items:
            continue
        weighted = sum(item["signed_score"] for item in news_items) / len(news_items)
        avg_timeliness = sum(float(item.get("timeliness_score") or 0) for item in news_items) / len(news_items)
        source_count = len(bucket["sources"])
        stock_count = len(bucket["stocks"])
        news_count = len(news_items)
        negative_penalty = min(bucket["negative_news_count"] * 2.0, 18.0)
        sector_score = clamp(
            max(weighted, 0) * 0.68
            + min(news_count, 20) * 1.25
            + min(source_count, 8) * 3.0
            + min(stock_count, 10) * 1.6
            + avg_timeliness * 0.10
            - negative_penalty
        )
        top_news = sorted(news_items, key=lambda r: abs(r["signed_score"]), reverse=True)[:5]
        for stock in bucket["stocks"].values():
            stock["matched_news"] = sorted(
                stock.get("matched_news") or [],
                key=lambda r: (abs(float(r.get("signed_score") or 0)), str(r.get("published_at") or "")),
                reverse=True,
            )[:3]
        top_stocks = sorted(bucket["stocks"].values(), key=lambda r: (r["score"], r["news_count"]), reverse=True)[:30]
        if len(top_stocks) < 20:
            seen_codes = {row["code"] for row in top_stocks}
            for candidate in load_sector_candidate_stocks(bucket["sector_type"], bucket["sector_name"], as_of, limit=30):
                if candidate["code"] in seen_codes:
                    continue
                top_stocks.append(candidate)
                seen_codes.add(candidate["code"])
                if len(top_stocks) >= 30:
                    break
        summaries.append(
            {
                "trade_date": as_of.date().isoformat(),
                "sector_type": bucket["sector_type"],
                "sector_name": bucket["sector_name"],
                "as_of_datetime": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "sector_score": round(sector_score, 4),
                "weighted_impact_score": round(weighted, 4),
                "timeliness_score": round(avg_timeliness, 4),
                "timeliness_level": timeliness_level(avg_timeliness),
                "news_count": news_count,
                "source_count": source_count,
                "stock_count": stock_count,
                "positive_news_count": bucket["positive_news_count"],
                "negative_news_count": bucket["negative_news_count"],
                "top_stocks": top_stocks,
                "top_news": top_news,
                "sources": sorted(bucket["sources"]),
            }
        )
    return sorted(summaries, key=lambda row: row["sector_score"], reverse=True)


def load_sector_candidate_stocks(sector_type: str, sector_name: str, as_of: datetime, limit: int = 8) -> list[dict[str, Any]]:
    """Pick current candidate stocks for a hot sector using only market data <= as_of.

    These are not news-derived facts; they are a no-lookahead bridge so P0 can
    validate whether a detected hot theme contains plausible A-share candidates.
    """
    if sector_type == "industry":
        industries = [sector_name]
    else:
        industries = THEME_INDUSTRY_HINTS.get(sector_name, [])
    if not industries:
        return []
    date_operator = "<=" if as_of.time() >= dt_time(15, 5) else "<"
    placeholders = ",".join(["%s"] * len(industries))
    sql = f"""
    SELECT sb.code, sb.name, sb.industry, dk.trade_date, dk.close,
           CASE WHEN prev.close IS NOT NULL AND prev.close > 0 THEN (dk.close - prev.close) / prev.close * 100 ELSE 0 END AS pct_chg,
           dk.amount,
           fid.turnover_rate, fid.volume_ratio, mf.net_mf_amount
    FROM stock_basic sb
    INNER JOIN daily_kline dk ON dk.code = sb.code
      AND dk.trade_date = (
          SELECT MAX(d2.trade_date)
          FROM daily_kline d2
          WHERE d2.code = sb.code AND d2.trade_date {date_operator} %s
      )
    LEFT JOIN daily_kline prev ON prev.code = sb.code
      AND prev.trade_date = (
          SELECT MAX(p2.trade_date)
          FROM daily_kline p2
          WHERE p2.code = sb.code AND p2.trade_date < dk.trade_date
      )
    LEFT JOIN factor_input_daily fid ON fid.code = sb.code AND fid.trade_date = dk.trade_date
    LEFT JOIN stock_moneyflow_daily mf ON mf.code = sb.code AND mf.trade_date = dk.trade_date
    WHERE sb.instrument_type='stock' AND sb.is_delisted=0 AND sb.is_st=0
      AND sb.industry IN ({placeholders})
      AND dk.amount IS NOT NULL
    ORDER BY pct_chg DESC, COALESCE(fid.turnover_rate, 0) DESC, COALESCE(dk.amount, 0) DESC
    LIMIT %s
    """
    params = [as_of.date().isoformat(), *industries, limit]
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall() or []
    result = []
    for row in rows:
        amount = float(row.get("amount") or 0)
        pct = float(row.get("pct_chg") or 0)
        turnover = float(row.get("turnover_rate") or 0)
        candidate_score = clamp(
            38
            + max(min(pct, 10), -10) * 3.0
            + min(turnover, 12) * 1.8
            + min(amount / 100_000_000, 20) * 0.9
            + min(float(row.get("volume_ratio") or 0), 4) * 2.0
        )
        result.append(
            {
                "code": row["code"],
                "name": row.get("name"),
                "industry": row.get("industry"),
                "score": candidate_score,
                "news_count": 0,
                "match_type": "sector_candidate",
                "match_reason": "板块候选池：使用不晚于as_of的最新行情按涨幅/换手/成交额排序，偏向主题内前排",
                "data_trade_date": str(row.get("trade_date")),
                "pct_chg": float(row.get("pct_chg") or 0),
                "amount": amount,
            }
        )
    return result


def save_sector_summaries(summaries: list[dict[str, Any]], trade_date: str | None = None, as_of_datetime: str | None = None) -> None:
    final_trade_date = trade_date or (summaries[0]["trade_date"] if summaries else None)
    final_as_of = as_of_datetime or (summaries[0]["as_of_datetime"] if summaries else None)
    if not final_trade_date or not final_as_of:
        return
    save_sector_summaries_normalized(
        summaries,
        trade_date=final_trade_date,
        as_of_datetime=final_as_of,
    )


def select_sources(args: argparse.Namespace) -> list[str]:
    if args.sources:
        sources = [part.strip() for part in args.sources.split(",") if part.strip()]
    else:
        sources = list(DEFAULT_NEWSNOW_SOURCES)
    if args.source_offset:
        sources = sources[args.source_offset :]
    if args.source_limit:
        sources = sources[: args.source_limit]
    return sources


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NewsNow market opinion and aggregate recent hot sectors without lookahead bias.")
    parser.add_argument("--sources", help="comma separated NewsNow source ids")
    parser.add_argument("--source-offset", type=int, default=0)
    parser.add_argument("--source-limit", type=int, default=0)
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--as-of", help="anti-lookahead cutoff datetime, default now")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=int, default=12)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    lock_handle = acquire_lock()
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "previous_run_still_running"}, ensure_ascii=False))
        return

    as_of = parse_datetime_arg(args.as_of)
    sources = select_sources(args)
    run_id = f"market_opinion_{as_of.strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"sources": sources, "lookback_days": args.lookback_days, "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"), "dry_run": args.dry_run})
    started = time.time()
    try:
        client = NewsNowClient(timeout_seconds=args.timeout_seconds)
        stock_refs, industries = load_stock_refs()
        fetched = saved = matched_stocks = matched_sectors = failed_sources = 0
        errors: dict[str, str] = {}
        cutoff = as_of - timedelta(days=args.lookback_days)
        for source_id in sources:
            try:
                _, rows = client.fetch_source(source_id)
                fetched += len(rows)
                upper_bound = as_of if args.as_of else datetime.now()
                for item in rows:
                    if item.effective_time < cutoff or item.effective_time > upper_bound:
                        continue
                    src = source_score(item)
                    imp, direction, event_type = importance_score(item.title, item.summary)
                    amp = amplification_score(item)
                    time_info = timeliness_score(event_type, item.effective_time, upper_bound)
                    final = impact_score(src, imp, amp, time_info["score"])
                    stock_matches = match_stocks(item, stock_refs)
                    sector_matches = match_sectors(item, stock_matches, industries)
                    if not sector_matches:
                        # A previous version of the matcher may have mapped this title.
                        # Upsert the raw row and clear stale matches so summaries do not
                        # keep using outdated sector/stock links.
                        if not args.dry_run:
                            raw_id = save_raw_item(
                                item,
                                src,
                                imp,
                                amp,
                                time_info["score"],
                                time_info["level"],
                                time_info["effective_until"],
                                final,
                                direction,
                                event_type,
                            )
                            replace_matches(raw_id, [], [])
                        continue
                    if not args.dry_run:
                        raw_id = save_raw_item(
                            item,
                            src,
                            imp,
                            amp,
                            time_info["score"],
                            time_info["level"],
                            time_info["effective_until"],
                            final,
                            direction,
                            event_type,
                        )
                        replace_matches(raw_id, stock_matches, sector_matches)
                    saved += 1
                    matched_stocks += len(stock_matches)
                    matched_sectors += len(sector_matches)
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
            except Exception as exc:
                failed_sources += 1
                errors[source_id] = f"{type(exc).__name__}: {str(exc)[:300]}"
        aggregate_as_of = as_of if args.as_of else datetime.now()
        summaries = aggregate_sectors(aggregate_as_of, args.lookback_days) if not args.dry_run else []
        if not args.dry_run:
            save_sector_summaries(
                summaries,
                aggregate_as_of.date().isoformat(),
                aggregate_as_of.strftime("%Y-%m-%d %H:%M:%S"),
            )
        payload = {
            "run_id": run_id,
            "status": "success" if failed_sources == 0 else "partial_success",
            "as_of": aggregate_as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "lookback_days": args.lookback_days,
            "sources": sources,
            "fetched_items": fetched,
            "saved_items": saved,
            "stock_matches": matched_stocks,
            "sector_matches": matched_sectors,
            "sector_summary_count": len(summaries),
            "top_sectors": summaries[:8],
            "failed_sources": failed_sources,
            "errors": errors,
            "elapsed_seconds": round(time.time() - started, 2),
            "anti_lookahead_rule": "COALESCE(published_at, crawl_time) <= as_of_datetime; future source pubDate is capped at crawl_time; timeliness_score is recomputed by event type at aggregate time",
        }
        logger.finish(TASK_NAME, run_id, payload["status"], f"market opinion updated, saved={saved}, sectors={len(summaries)}", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {"run_id": run_id, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)[:500]}
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(json.dumps(payload, ensure_ascii=False))
        raise
    finally:
        release_lock(lock_handle)


if __name__ == "__main__":
    main()
