#!/usr/bin/env python3
"""
新闻获取模块 - 多源整合

用于后台舆情任务，不依赖 OpenClaw 对话技能。
优先级：Tavily（结构化 API） -> AkShare（免费财经源） -> DuckDuckGo（免费搜索兜底） -> RSS（市场新闻兜底）
"""

import logging
import os
import re
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote_plus, urlparse

logger = logging.getLogger(__name__)


def source_from_url(url: str, default: str = "") -> str:
    """从 URL 提取稳定来源名，避免 Tavily/DuckDuckGo 这类聚合源掩盖真实媒体域名。"""
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain or default
    except Exception:
        return default


def extract_date_from_text(*parts: str | None) -> str | None:
    """Extract an explicit publication/event date from provider text.

    Search APIs often return evergreen stock pages without a publish timestamp.
    For short-term sentiment, using the crawl/search time would make stale pages
    look fresh, so only return a date when the text itself contains one.
    """
    text = " ".join(str(part or "") for part in parts)
    patterns = [
        r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})日?",
        r"(\d{1,2})月(\d{1,2})日",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            if len(match.groups()) == 3:
                year, month, day = match.groups()
            else:
                year = str(datetime.now().year)
                month, day = match.groups()
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
except Exception:
    pass


class AkShareNewsProvider:
    """使用 AkShare 获取财经新闻（免费）。"""

    def get_stock_news(self, stock_code: str) -> List[Dict]:
        try:
            import akshare as ak

            news_df = ak.stock_news_em(symbol=stock_code[:6])
            results = []
            for _, row in news_df.head(20).iterrows():
                results.append({
                    "title": row.get("新闻标题") or row.get("title") or "",
                    "content": str(row.get("新闻内容") or row.get("content") or "")[:1000],
                    "url": row.get("新闻链接") or row.get("url") or "",
                    "datetime": str(row.get("发布时间") or row.get("datetime") or ""),
                    "source": row.get("文章来源") or "东方财富",
                    "provider": "akshare_stock_news_em",
                })
            return results
        except Exception as e:
            logger.warning(f"AkShare 获取新闻失败: {e}")
            return []

    def get_market_news(self, limit: int = 20) -> List[Dict]:
        try:
            import akshare as ak

            news_df = ak.stock_info_global_em()
            results = []
            for _, row in news_df.head(limit).iterrows():
                results.append({
                    "title": row.get("title", ""),
                    "content": row.get("content", ""),
                    "datetime": str(row.get("datetime", "")),
                    "source": "东方财富",
                })
            return results
        except Exception as e:
            logger.warning(f"获取市场新闻失败: {e}")
            return []


class RSSNewsProvider:
    """RSS 订阅获取市场新闻。"""

    RSS_SOURCES = {
        "财新": "https://www.caixin.com/rss.xml",
        "新浪财经": "https://rss.sina.com.cn/roll/finance/hot_roll.xml",
        "东方财富": "https://www.eastmoney.com/rss.xml",
    }

    def get_rss_news(self, source: str = "新浪财经", limit: int = 10) -> List[Dict]:
        try:
            import feedparser

            url = self.RSS_SOURCES.get(source)
            if not url:
                return []
            feed = feedparser.parse(url)
            results = []
            for entry in feed.entries[:limit]:
                results.append({
                    "title": entry.get("title", ""),
                    "content": str(entry.get("summary", ""))[:500],
                    "url": entry.get("link", ""),
                    "datetime": entry.get("published", ""),
                    "source": source,
                })
            return results
        except Exception as e:
            logger.warning(f"RSS 获取失败: {e}")
            return []


class TavilyNewsProvider:
    """Tavily API 新闻搜索（结构化主源）。"""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            logger.warning("未配置 TAVILY_API_KEY，Tavily 新闻源不可用")

    def is_available(self) -> bool:
        return bool(self.api_key)

    def search_stock_news(self, stock_name: str, stock_code: str, days: int = 3) -> List[Dict]:
        if not self.api_key:
            return []
        query = f"{stock_name} {stock_code} 股票 最新 财经 新闻"
        return self._search(query, days)

    def search_query(self, query: str, days: int = 3) -> List[Dict]:
        if not self.api_key:
            return []
        return self._search(query, days)

    def _search(self, query: str, days: int = 3) -> List[Dict]:
        try:
            import requests

            response = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": self.api_key,
                    "query": query,
                    "search_depth": "basic",
                    "include_answer": False,
                    "max_results": 10,
                    "include_domains": [
                        "sina.com.cn", "163.com", "ifeng.com", "cnstock.com", "cs.com.cn",
                        "stcn.com", "eastmoney.com", "hexun.com", "jrj.com", "10jqka.com.cn",
                    ],
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered = []
            for item in results:
                published = item.get("published_date", "")
                pub_date = None
                if published:
                    try:
                        pub_date = datetime.fromisoformat(published.replace("Z", "+00:00")).replace(tzinfo=None)
                    except Exception:
                        pass
                inferred_date = extract_date_from_text(item.get("title", ""), item.get("content", ""))
                if not pub_date and inferred_date:
                    try:
                        pub_date = datetime.strptime(inferred_date, "%Y-%m-%d")
                    except Exception:
                        pub_date = None
                if pub_date and pub_date >= cutoff_date:
                    filtered.append({
                        "title": item.get("title", ""),
                        "content": str(item.get("content", ""))[:800],
                        "url": item.get("url", ""),
                        "source": item.get("source") or source_from_url(item.get("url", ""), "Tavily"),
                        "datetime": published or inferred_date,
                    })
            return filtered
        except Exception as e:
            logger.warning(f"Tavily 搜索失败: {e}")
            return []


class DuckDuckGoNewsProvider:
    """DuckDuckGo 免费搜索兜底。

    仅作为批量舆情任务的末级 fallback。DuckDuckGo 返回 HTML，结构和限流都不如 Tavily 稳定。
    """

    def search_stock_news(self, stock_name: str, stock_code: str, days: int = 3) -> List[Dict]:
        try:
            import re
            import requests

            query = quote_plus(f"{stock_name} {stock_code} 股票 财经 新闻 最近")
            response = requests.get(
                f"https://html.duckduckgo.com/html/?q={query}",
                headers={"User-Agent": "Mozilla/5.0 stock-analysis sentiment bot"},
                timeout=20,
            )
            response.raise_for_status()
            html = response.text
            if response.status_code == 202 or "anomaly" in html.lower():
                logger.info("DuckDuckGo 返回反爬/验证页，本次跳过该兜底源")
                return []
            blocks = re.findall(r'<a rel="nofollow" class="result__a" href="([^"]+)">(.*?)</a>.*?<a class="result__snippet"[^>]*>(.*?)</a>', html, re.S)
            results = []
            for url, title, snippet in blocks[:10]:
                clean_title = unescape(re.sub(r"<.*?>", "", title)).strip()
                clean_snippet = unescape(re.sub(r"<.*?>", "", snippet)).strip()
                if not clean_title:
                    continue
                results.append({
                    "title": clean_title,
                    "content": clean_snippet,
                    "url": unescape(url),
                    "source": "DuckDuckGo",
                    "datetime": extract_date_from_text(clean_title, clean_snippet),
                })
            return results
        except Exception as e:
            logger.warning(f"DuckDuckGo 搜索失败: {e}")
            return []


class NewsAggregator:
    """新闻聚合器 - 后台任务使用。"""

    def __init__(self):
        self.tavily = TavilyNewsProvider()
        self.akshare = AkShareNewsProvider()
        self.duckduckgo = DuckDuckGoNewsProvider()
        self.rss = RSSNewsProvider()

    def get_stock_news(self, stock_code: str, stock_name: str, sources: List[str] | None = None) -> List[Dict]:
        """
        聚合多源新闻。

        默认优先级：AkShare -> Tavily -> DuckDuckGo -> RSS。
        sources 可指定子集，例如 ['tavily', 'duckduckgo']。
        """
        ordered_sources = sources or ["akshare", "tavily", "duckduckgo", "rss"]
        enabled = set(ordered_sources)
        all_news: List[Dict] = []

        for source in ordered_sources:
            if source == "akshare" and "akshare" in enabled:
                news = self.akshare.get_stock_news(stock_code)
                all_news.extend(news)
                logger.info(f"AkShare 获取 {len(news)} 条新闻")
            elif source == "tavily" and "tavily" in enabled and self.tavily.is_available():
                news = self.tavily.search_stock_news(stock_name, stock_code, days=3)
                all_news.extend(news)
                logger.info(f"Tavily 获取 {len(news)} 条新闻")
            elif source == "duckduckgo" and "duckduckgo" in enabled and len(all_news) < 5:
                news = self.duckduckgo.search_stock_news(stock_name, stock_code, days=3)
                all_news.extend(news)
                logger.info(f"DuckDuckGo 获取 {len(news)} 条新闻")
            elif source == "rss" and "rss" in enabled and len(all_news) < 5:
                news = self.rss.get_rss_news(limit=10)
                # RSS 是市场新闻，不一定和个股强相关，只作为空结果兜底。
                all_news.extend(news)
                logger.info(f"RSS 获取 {len(news)} 条市场新闻")

            if len(all_news) >= 15:
                return self._deduplicate(all_news)[:15]

        return self._deduplicate(all_news)[:15]

    def _deduplicate(self, news_list: List[Dict]) -> List[Dict]:
        seen = set()
        unique = []
        for item in news_list:
            title = str(item.get("title", "")).strip()
            if title and title not in seen:
                seen.add(title)
                unique.append(item)
        return unique


if __name__ == "__main__":
    print("🧪 新闻获取模块测试")
    aggregator = NewsAggregator()
    for label, fn in [
        ("Tavily", lambda: aggregator.tavily.search_stock_news("平安银行", "000001")),
        ("AkShare", lambda: aggregator.akshare.get_stock_news("000001")),
        ("DuckDuckGo", lambda: aggregator.duckduckgo.search_stock_news("平安银行", "000001")),
    ]:
        news = fn()
        print(f"{label}: {len(news)} 条")
        if news:
            print(f"  示例: {news[0].get('title', '')[:60]}")
