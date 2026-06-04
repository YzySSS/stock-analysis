from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import requests

DEFAULT_NEWSNOW_BASE_URL = "https://newsnow.busiyi.world"

NEWSNOW_SOURCE_META: dict[str, dict[str, Any]] = {
    "cls": {"name": "财联社", "column": "finance", "type": "realtime", "base_score": 92},
    "cls-hot": {"name": "财联社", "column": "finance", "type": "hottest", "base_score": 90},
    "wallstreetcn": {"name": "华尔街见闻", "column": "finance", "type": "realtime", "base_score": 84},
    "wallstreetcn-hot": {"name": "华尔街见闻", "column": "finance", "type": "hottest", "base_score": 82},
    "xueqiu": {"name": "雪球", "column": "finance", "type": "hottest", "base_score": 72},
    "jin10": {"name": "金十数据", "column": "finance", "type": "realtime", "base_score": 78},
    "mktnews": {"name": "MKTNews", "column": "finance", "type": "realtime", "base_score": 76},
    "gelonghui": {"name": "格隆汇", "column": "finance", "type": "realtime", "base_score": 74},
    "fastbull": {"name": "法布财经", "column": "finance", "type": "realtime", "base_score": 70},
    "baidu": {"name": "百度热搜", "column": "china", "type": "hottest", "base_score": 58},
    "toutiao": {"name": "今日头条", "column": "china", "type": "hottest", "base_score": 56},
    "weibo": {"name": "微博", "column": "china", "type": "hottest", "base_score": 54},
    "zhihu": {"name": "知乎", "column": "china", "type": "hottest", "base_score": 58},
    "thepaper": {"name": "澎湃新闻", "column": "china", "type": "hottest", "base_score": 70},
    "ifeng": {"name": "凤凰网", "column": "china", "type": "hottest", "base_score": 62},
}

DEFAULT_NEWSNOW_SOURCES = [
    "cls",
    "cls-hot",
    "wallstreetcn",
    "wallstreetcn-hot",
    "xueqiu",
    "jin10",
    "mktnews",
    "gelonghui",
    "fastbull",
    "baidu",
    "toutiao",
    "weibo",
    "zhihu",
]


@dataclass(frozen=True)
class NewsNowItem:
    source_id: str
    source_name: str | None
    source_column: str | None
    source_type: str | None
    status: str | None
    rank_no: int | None
    item_id: str | None
    title: str
    summary: str | None
    url: str | None
    mobile_url: str | None
    published_at: datetime | None
    crawl_time: datetime
    raw: dict[str, Any]

    @property
    def effective_time(self) -> datetime:
        """Timestamp used for anti-lookahead queries.

        We never allow a source-provided pubDate later than our crawl time to become
        usable before it was actually observed by this system.
        """
        if not self.published_at:
            return self.crawl_time
        if self.published_at > self.crawl_time + timedelta(minutes=5):
            return self.crawl_time
        return self.published_at

    @property
    def title_hash(self) -> str:
        return hashlib.sha256(self.title.strip().lower().encode("utf-8")).hexdigest()


def parse_timestamp(value: Any, crawl_time: datetime) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        # NewsNow/source APIs may use seconds or milliseconds.
        if number > 10_000_000_000:
            number = number / 1000
        try:
            return datetime.fromtimestamp(number)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_timestamp(int(text), crawl_time)
    normalized = text.replace("T", " ").replace("Z", "").split("+")[0]
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"]:
        try:
            return datetime.strptime(normalized[: len(fmt)], fmt)
        except Exception:
            pass
    return None


class NewsNowClient:
    def __init__(self, base_url: str | None = None, timeout_seconds: int = 12) -> None:
        self.base_url = (base_url or os.getenv("NEWSNOW_BASE_URL") or DEFAULT_NEWSNOW_BASE_URL).rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; stock-analysis-newsnow/1.0; +https://www.yzysstock.cloud)",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    def fetch_source(self, source_id: str, latest: bool = True) -> tuple[str | None, list[NewsNowItem]]:
        crawl_time = datetime.now()
        url = urljoin(f"{self.base_url}/", "api/s")
        params: dict[str, Any] = {"id": source_id}
        if latest:
            params["latest"] = ""
        resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
        resp.raise_for_status()
        payload = resp.json()
        status = payload.get("status")
        meta = NEWSNOW_SOURCE_META.get(source_id, {})
        rows: list[NewsNowItem] = []
        for idx, raw in enumerate(payload.get("items") or [], start=1):
            title = str(raw.get("title") or "").strip()
            if not title:
                continue
            extra = raw.get("extra") if isinstance(raw.get("extra"), dict) else {}
            summary = raw.get("summary") or raw.get("description") or extra.get("hover") or extra.get("info")
            item_id = raw.get("id") or raw.get("key") or raw.get("url") or title
            rows.append(
                NewsNowItem(
                    source_id=source_id,
                    source_name=meta.get("name"),
                    source_column=meta.get("column"),
                    source_type=meta.get("type"),
                    status=status,
                    rank_no=idx,
                    item_id=str(item_id)[:191] if item_id is not None else None,
                    title=title[:512],
                    summary=str(summary)[:2000] if summary else None,
                    url=str(raw.get("url"))[:1024] if raw.get("url") else None,
                    mobile_url=str(raw.get("mobileUrl"))[:1024] if raw.get("mobileUrl") else None,
                    published_at=parse_timestamp(raw.get("pubDate") or raw.get("date") or raw.get("createdAt"), crawl_time),
                    crawl_time=crawl_time,
                    raw=raw,
                )
            )
        return status, rows
