#!/usr/bin/env python3
"""
新闻获取模块 - 多源整合
支持：AkShare、Coze Web Search、Tavily、RSS等
"""

import os
import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class AkShareNewsProvider:
    """使用AkShare获取财经新闻（免费）"""
    
    def get_stock_news(self, stock_code: str) -> List[Dict]:
        """
        获取个股新闻
        
        Returns:
            新闻列表 [{title, content, url, datetime}]
        """
        try:
            import akshare as ak
            
            # 获取个股新闻
            news_df = ak.stock_news_em(symbol=stock_code[:6])
            
            results = []
            for _, row in news_df.head(10).iterrows():
                results.append({
                    "title": row.get("title", ""),
                    "content": row.get("content", "")[:500],  # 截取前500字
                    "url": row.get("url", ""),
                    "datetime": str(row.get("datetime", "")),
                    "source": "东方财富"
                })
            
            return results
            
        except Exception as e:
            logger.error(f"AkShare获取新闻失败: {e}")
            return []
    
    def get_market_news(self, limit: int = 20) -> List[Dict]:
        """获取市场热点新闻"""
        try:
            import akshare as ak
            
            # 获取财经快讯
            news_df = ak.stock_info_global_em()
            
            results = []
            for _, row in news_df.head(limit).iterrows():
                results.append({
                    "title": row.get("title", ""),
                    "content": row.get("content", ""),
                    "datetime": str(row.get("datetime", "")),
                    "source": "东方财富"
                })
            
            return results
            
        except Exception as e:
            logger.error(f"获取市场新闻失败: {e}")
            return []


class CozeWebSearchProvider:
    """使用Coze Web Search SDK获取新闻"""
    
    def __init__(self):
        try:
            from coze_coding_dev_sdk import SearchClient
            self.client = SearchClient()
            self.available = True
        except Exception as e:
            logger.warning(f"Coze Web Search 初始化失败: {e}")
            self.client = None
            self.available = False
    
    def is_available(self) -> bool:
        """检查是否可用"""
        return self.available
    
    def search_stock_news(self, stock_name: str, stock_code: str = "", days: int = 3) -> List[Dict]:
        """
        使用Coze Web Search搜索股票新闻
        
        Args:
            stock_name: 股票名称
            stock_code: 股票代码
            days: 最近几天的新闻
        
        Returns:
            新闻列表
        """
        if not self.available:
            return []
        
        try:
            query = f"{stock_name} {stock_code} 股票" if stock_code else f"{stock_name} 股票"
            
            # 使用 Coze SDK 进行搜索
            results = self.client.web_search(query, count=10, need_summary=True)
            
            # 解析结果
            news_list = []
            for item in results.web_items[:10]:
                news_list.append({
                    "title": item.title,
                    "content": item.content or item.snippet or "",
                    "url": item.url,
                    "source": item.site_name or "未知来源",
                    "datetime": item.publish_time or datetime.now().isoformat()
                })
            
            logger.info(f"✅ Coze搜索获取 {len(news_list)} 条新闻")
            return news_list
            
        except Exception as e:
            logger.error(f"Coze搜索失败: {e}")
            return []


class RSSNewsProvider:
    """RSS订阅获取新闻"""
    
    RSS_SOURCES = {
        "财新": "https://www.caixin.com/rss.xml",
        "新浪财经": "https://rss.sina.com.cn/roll/finance/hot_roll.xml",
        "东方财富": "https://www.eastmoney.com/rss.xml"
    }
    
    def get_rss_news(self, source: str = "新浪财经", limit: int = 10) -> List[Dict]:
        """从RSS获取新闻"""
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
                    "content": entry.get("summary", "")[:500],
                    "url": entry.get("link", ""),
                    "datetime": entry.get("published", ""),
                    "source": source
                })
            
            return results
            
        except Exception as e:
            logger.error(f"RSS获取失败: {e}")
            return []


class TavilyNewsProvider:
    """Tavily API新闻搜索（专业方案）"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            logger.warning("⚠️ 未配置TAVILY_API_KEY，Tavily新闻源不可用")
    
    def is_available(self) -> bool:
        """检查是否可用"""
        return bool(self.api_key)
    
    def search_stock_news(self, stock_name: str, stock_code: str, days: int = 3) -> List[Dict]:
        """
        搜索股票相关新闻
        
        Args:
            stock_name: 股票名称
            stock_code: 股票代码
            days: 最近几天的新闻
        
        Returns:
            新闻列表
        """
        if not self.api_key:
            return []
        
        query = f"{stock_name} {stock_code} 股票 最新"
        return self._search(query, days)
    
    def _search(self, query: str, days: int = 3) -> List[Dict]:
        """执行搜索"""
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
                        "sina.com.cn", "163.com", "ifeng.com",
                        "cnstock.com", "cs.com.cn", "stcn.com",
                        "eastmoney.com", "hexun.com", "jrj.com"
                    ]
                },
                timeout=30
            )
            
            data = response.json()
            results = data.get("results", [])
            
            # 过滤和格式化
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered = []
            
            for r in results:
                published = r.get("published_date", "")
                pub_date = None
                
                if published:
                    try:
                        pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    except:
                        pass
                
                # 如果没有日期或日期在范围内，保留
                if not pub_date or pub_date >= cutoff_date:
                    filtered.append({
                        "title": r.get("title", ""),
                        "content": r.get("content", "")[:800],
                        "url": r.get("url", ""),
                        "source": r.get("source", ""),
                        "datetime": published or datetime.now().isoformat()
                    })
            
            return filtered
            
        except Exception as e:
            logger.error(f"Tavily搜索失败: {e}")
            return []


class NewsAggregator:
    """新闻聚合器 - 整合多源（Tavily优先）"""
    
    def __init__(self):
        self.tavily = TavilyNewsProvider()
        self.akshare = AkShareNewsProvider()
        self.coze = CozeWebSearchProvider()
        self.rss = RSSNewsProvider()
    
    def get_stock_news(self, 
                       stock_code: str, 
                       stock_name: str,
                       sources: List[str] = None) -> List[Dict]:
        """
        聚合多源新闻（Tavily专业方案优先）
        
        优先级：
        1. Tavily API（专业方案，首选）
        2. AkShare（免费备选）
        3. Coze Web Search（补充）
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            sources: 指定来源 ['tavily', 'akshare', 'coze', 'rss']，None表示自动选择
        
        Returns:
            合并的新闻列表
        """
        all_news = []
        
        # 1. Coze Web Search（首选 - 无需额外配置）
        if self.coze.is_available():
            news = self.coze.search_stock_news(stock_name, stock_code)
            all_news.extend(news)
            logger.info(f"✅ Coze搜索获取 {len(news)} 条新闻")
            
            # Coze质量高，如果获取到足够新闻直接返回
            if len(news) >= 5:
                logger.info(f"Coze新闻充足，直接返回")
                return self._deduplicate(all_news)[:15]
        
        # 2. Tavily API（备选 - 需要配置API Key）
        if len(all_news) < 5 and self.tavily.is_available():
            news = self.tavily.search_stock_news(stock_name, stock_code, days=3)
            all_news.extend(news)
            logger.info(f"Tavily获取 {len(news)} 条新闻")
            
            if len(news) >= 5:
                logger.info(f"Tavily新闻充足，直接返回")
                return self._deduplicate(all_news)[:15]
        
        # 3. AkShare（免费备选）
        if len(all_news) < 5:
            news = self.akshare.get_stock_news(stock_code)
            all_news.extend(news)
            logger.info(f"AkShare获取 {len(news)} 条新闻")
        
        return self._deduplicate(all_news)[:15]
    
    def _deduplicate(self, news_list: List[Dict]) -> List[Dict]:
        """去重（按标题）"""
        seen = set()
        unique = []
        for n in news_list:
            title = n.get("title", "")
            if title and title not in seen:
                seen.add(title)
                unique.append(n)
        return unique


if __name__ == "__main__":
    print("🧪 新闻获取模块测试")
    print("="*60)
    
    aggregator = NewsAggregator()
    
    # 测试AkShare
    print("\n1. 测试 AkShare 获取新闻...")
    news = aggregator.akshare.get_stock_news("000001")
    print(f"   获取 {len(news)} 条新闻")
    if news:
        print(f"   示例: {news[0]['title'][:50]}...")
    
    # 测试Coze
    print("\n2. 测试 Coze Web Search...")
    news = aggregator.coze.search_stock_news("平安银行")
    print(f"   获取 {len(news)} 条新闻")
    if news:
        print(f"   示例: {news[0]['title'][:50]}...")
    
    print("\n" + "="*60)
    print("✅ 测试完成")
    print("\n推荐用法:")
    print("  from news_provider import NewsAggregator")
    print("  news = NewsAggregator().get_stock_news('000001.SZ', '平安银行')")
