#!/usr/bin/env python3
"""
新闻可信度评估模块
对新闻来源进行可信度分级和验证
"""

from typing import Dict, List
from dataclasses import dataclass
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


@dataclass
class NewsCredibility:
    """新闻可信度"""
    score: int  # 0-100
    level: str  # S/A/B/C/D
    reason: str  # 评级理由


class NewsCredibilityChecker:
    """
    新闻可信度检查器
    
    可信度分级：
    - S级 (90-100): 官方/权威财经媒体
    - A级 (70-89): 主流财经媒体/知名券商研报
    - B级 (50-69): 一般财经媒体/自媒体大号
    - C级 (30-49): 自媒体/论坛/博客
    - D级 (0-29): 未知来源/可疑网站
    """
    
    # S级：官方权威
    S_LEVEL_SOURCES = {
        'cs.com.cn': '中国证券报（证监会指定披露媒体）',
        'cninfo.com.cn': '巨潮资讯网（官方信息披露平台）',
        'sse.com.cn': '上交所官网',
        'szse.cn': '深交所官网',
        'bse.cn': '北交所官网',
        'csrc.gov.cn': '证监会官网',
        'gov.cn': '政府官网',
    }
    
    # A级：主流财经媒体
    A_LEVEL_SOURCES = {
        'xinhuanet.com': '新华网',
        'people.com.cn': '人民网',
        'eastmoney.com': '东方财富网',
        'sina.com.cn': '新浪财经',
        '163.com': '网易财经',
        'sohu.com': '搜狐财经',
        'ifeng.com': '凤凰财经',
        'hexun.com': '和讯网',
        'cs.com.cn': '中国证券报',
        'stcn.com': '证券时报',
        '21jingji.com': '21世纪经济报道',
        'jjckb.cn': '经济参考报',
        'cnstock.com': '中国证券网',
        'p5w.net': '全景网',
    }
    
    # B级：一般财经媒体
    B_LEVEL_SOURCES = {
        'yicai.com': '第一财经',
        'jiemian.com': '界面新闻',
        'cls.cn': '财联社',
        'wallstreetcn.com': '华尔街见闻',
        'caijing.com.cn': '财经网',
        'nbd.com.cn': '每日经济新闻',
        'jinse.com': '金色财经',
        'chaindd.com': '链得得',
        'tech.sina.com.cn': '新浪科技',
        'finance.qq.com': '腾讯财经',
    }
    
    # C级：自媒体/平台
    C_LEVEL_SOURCES = {
        'weibo.com': '微博',
        'zhihu.com': '知乎',
        'xueqiu.com': '雪球',
        'taoguba.com.cn': '淘股吧',
        'guba.eastmoney.com': '东方财富股吧',
        'toutiao.com': '今日头条',
        'baijiahao.baidu.com': '百家号',
        'so.com': '360搜索',
        'sogou.com': '搜狗',
    }
    
    # 可疑域名特征
    SUSPICIOUS_PATTERNS = [
        'click', 'ad', 'track', 'popup',
        'short', 'tiny', 'bit.ly', 't.cn'
    ]
    
    def check_credibility(self, url: str, title: str = "") -> NewsCredibility:
        """
        检查新闻可信度
        
        Args:
            url: 新闻链接
            title: 新闻标题
        
        Returns:
            NewsCredibility 可信度评估
        """
        try:
            domain = self._extract_domain(url)
            
            # 1. 检查S级来源
            for s_domain, description in self.S_LEVEL_SOURCES.items():
                if s_domain in domain:
                    return NewsCredibility(
                        score=95,
                        level='S',
                        reason=f'官方权威来源：{description}'
                    )
            
            # 2. 检查A级来源
            for a_domain, description in self.A_LEVEL_SOURCES.items():
                if a_domain in domain:
                    return NewsCredibility(
                        score=80,
                        level='A',
                        reason=f'主流财经媒体：{description}'
                    )
            
            # 3. 检查B级来源
            for b_domain, description in self.B_LEVEL_SOURCES.items():
                if b_domain in domain:
                    return NewsCredibility(
                        score=60,
                        level='B',
                        reason=f'财经媒体：{description}'
                    )
            
            # 4. 检查C级来源
            for c_domain, description in self.C_LEVEL_SOURCES.items():
                if c_domain in domain:
                    return NewsCredibility(
                        score=40,
                        level='C',
                        reason=f'自媒体/社区：{description}（仅供参考）'
                    )
            
            # 5. 检查可疑链接
            if self._is_suspicious(url):
                return NewsCredibility(
                    score=20,
                    level='D',
                    reason='⚠️ 可疑来源：短链接/广告域名，建议谨慎对待'
                )
            
            # 6. 未知来源
            return NewsCredibility(
                score=50,
                level='B',
                reason=f'未知来源：{domain}（建议交叉验证）'
            )
            
        except Exception as e:
            logger.error(f"可信度检查失败: {e}")
            return NewsCredibility(
                score=30,
                level='C',
                reason='无法评估来源可信度'
            )
    
    def _extract_domain(self, url: str) -> str:
        """提取域名"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # 去除www前缀
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return url.lower()
    
    def _is_suspicious(self, url: str) -> bool:
        """检查是否为可疑链接"""
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in self.SUSPICIOUS_PATTERNS)
    
    def get_credibility_emoji(self, level: str) -> str:
        """获取可信度表情"""
        emoji_map = {
            'S': '⭐⭐⭐⭐⭐',
            'A': '⭐⭐⭐⭐',
            'B': '⭐⭐⭐',
            'C': '⭐⭐',
            'D': '⭐'
        }
        return emoji_map.get(level, '❓')
    
    def get_credibility_color(self, level: str) -> str:
        """获取可信度颜色标识"""
        color_map = {
            'S': '🟢',
            'A': '🟢',
            'B': '🟡',
            'C': '🟠',
            'D': '🔴'
        }
        return color_map.get(level, '⚪')


class NewsWithCredibility:
    """带可信度的新闻"""
    
    def __init__(self, news_item: Dict):
        self.title = news_item.get('title', '')
        self.content = news_item.get('content', '')
        self.url = news_item.get('url', '')
        self.source = news_item.get('source', '')
        self.datetime = news_item.get('datetime', '')
        
        # 评估可信度
        checker = NewsCredibilityChecker()
        self.credibility = checker.check_credibility(self.url, self.title)
        self.credibility_emoji = checker.get_credibility_emoji(self.credibility.level)
        self.credibility_color = checker.get_credibility_color(self.credibility.level)
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'source': self.source,
            'datetime': self.datetime,
            'credibility_score': self.credibility.score,
            'credibility_level': self.credibility.level,
            'credibility_reason': self.credibility.reason,
            'credibility_emoji': self.credibility_emoji,
        }
    
    def to_markdown(self) -> str:
        """转换为Markdown格式"""
        return (
            f"{self.credibility_color} [{self.credibility_level}级] "
            f"{self.credibility_emoji} {self.title}\n"
            f"   来源: {self.source} | 可信度: {self.credibility.score}/100\n"
            f"   评级: {self.credibility.reason}\n"
            f"   链接: {self.url[:80]}..."
        )


if __name__ == "__main__":
    print("🧪 新闻可信度评估测试")
    print("="*70)
    
    checker = NewsCredibilityChecker()
    
    # 测试不同来源
    test_urls = [
        ("https://www.cs.com.cn/ssgs/202403/t20240316_6412341.html", "证监会指定媒体"),
        ("https://finance.sina.com.cn/stock/2024-03-16/doc-inxxxxx.shtml", "新浪财经"),
        ("https://xueqiu.com/1234567/32165498", "雪球"),
        ("https://bit.ly/xxxxx", "短链接"),
        ("https://www.unknown-site.com/news/12345", "未知来源"),
    ]
    
    print("\n可信度分级测试:\n")
    
    for url, desc in test_urls:
        result = checker.check_credibility(url)
        emoji = checker.get_credibility_emoji(result.level)
        color = checker.get_credibility_color(result.level)
        
        print(f"{color} {desc}")
        print(f"   链接: {url[:60]}...")
        print(f"   评级: {result.level}级 {emoji} ({result.score}/100)")
        print(f"   理由: {result.reason}")
        print()
    
    print("="*70)
    print("\n可信度分级标准:")
    print("  ⭐⭐⭐⭐⭐ S级 (90-100): 官方/权威财经媒体")
    print("  ⭐⭐⭐⭐   A级 (70-89):  主流财经媒体")
    print("  ⭐⭐⭐     B级 (50-69):  一般财经媒体")
    print("  ⭐⭐       C级 (30-49):  自媒体/社区")
    print("  ⭐         D级 (0-29):   可疑来源")
