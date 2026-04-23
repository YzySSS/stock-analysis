#!/usr/bin/env python3
"""
新闻质量过滤器
==============
过滤低质、无关、重复的新闻

使用:
  from news_filter import NewsFilter
  
  filter = NewsFilter()
  quality_news = filter.filter(news_list, stock_code, stock_name)
"""

import re
from typing import List, Dict, Tuple
from datetime import datetime, timedelta


class NewsFilter:
    """新闻质量过滤器"""
    
    # 来源可信度评分 (0-1)
    SOURCE_CREDIBILITY = {
        # 官方权威媒体 (1.0)
        '新华社': 1.0, '人民日报': 1.0, '央视': 1.0, '央视新闻': 1.0,
        '中国证券报': 0.95, '上海证券报': 0.95, '证券时报': 0.95, '证券日报': 0.95,
        '经济观察报': 0.9, '第一财经': 0.9, '财联社': 0.9, '华尔街见闻': 0.9,
        
        # 主流财经门户 (0.7-0.8)
        '东方财富': 0.8, '新浪财经': 0.8, '同花顺': 0.8, '雪球': 0.75,
        '搜狐财经': 0.7, '网易财经': 0.7, '腾讯财经': 0.7,
        
        # 行业媒体 (0.6-0.7)
        '36氪': 0.65, '虎嗅': 0.65, '界面新闻': 0.7, '财新网': 0.75,
        
        # 自媒体/论坛 (0.3-0.5)
        '微博': 0.4, '微信公众号': 0.4, '知乎': 0.45,
        '股吧': 0.3, '雪球讨论': 0.5,
    }
    
    # 低质量关键词（标题党、无实质内容）
    LOW_QUALITY_KEYWORDS = [
        '震惊', '重磅', '突发', '炸锅', '爆了', '沸腾', '炸裂',
        '深夜', '凌晨', '刚刚', '刚刚发布', '紧急',
        '机构看好', '机构关注', '机构调研',  # 过于笼统
        '一文读懂', '深度解析', '最全梳理',  # 可能是汇总，价值低
        '涨停', '跌停', '暴涨', '暴跌',  # 情绪化严重
        '内幕', '泄密', '爆料', '传闻',  # 未经证实
        '网友', '网友表示', '网友热议',  # 非专业来源
    ]
    
    # 广告/推广关键词
    AD_KEYWORDS = [
        '广告', '推广', '赞助', '合作', '投稿',
        '点击阅读', '查看原文', '立即下载',
        '免费试用', '限时优惠', '扫码关注',
    ]
    
    # 重复内容检测（相似度阈值）
    SIMILARITY_THRESHOLD = 0.7
    
    def __init__(self, min_credibility: float = 0.3, max_age_days: int = 7):
        """
        初始化过滤器
        
        Args:
            min_credibility: 最低可信度阈值 (0-1)
            max_age_days: 最大新闻年龄（天）
        """
        self.min_credibility = min_credibility
        self.max_age_days = max_age_days
        self.seen_titles = set()  # 用于去重
    
    def get_source_credibility(self, source: str) -> float:
        """获取来源可信度"""
        if not source:
            return 0.5
        
        source = source.strip()
        
        # 精确匹配
        if source in self.SOURCE_CREDIBILITY:
            return self.SOURCE_CREDIBILITY[source]
        
        # 模糊匹配
        for key, score in self.SOURCE_CREDIBILITY.items():
            if key in source or source in key:
                return score
        
        # 默认中等可信度
        return 0.5
    
    def is_low_quality_title(self, title: str) -> bool:
        """检查标题是否低质量（标题党）"""
        if not title:
            return True
        
        title_lower = title.lower()
        
        # 检查低质量关键词
        for keyword in self.LOW_QUALITY_KEYWORDS:
            if keyword in title_lower:
                return True
        
        # 检查广告关键词
        for keyword in self.AD_KEYWORDS:
            if keyword in title_lower:
                return True
        
        # 标题过短（<10字，可能是无效标题）
        if len(title.strip()) < 10:
            return True
        
        # 标题过长（>60字，可能是摘要而非标题）
        if len(title.strip()) > 60:
            return True
        
        # 检查标点符号滥用（多个感叹号）
        if title.count('！') > 1 or title.count('!') > 1:
            return True
        
        # 检查数字堆砌（如"3大原因、5个信号"）
        if re.search(r'\d+大|\d+个|\d+只', title):
            return True
        
        return False
    
    def is_relevant(self, news: Dict, stock_code: str, stock_name: str) -> bool:
        """检查新闻是否与股票相关"""
        title = news.get('title', '')
        content = news.get('content', '')[:200]  # 只看前200字
        
        text = (title + ' ' + content).lower()
        
        # 必须包含股票代码或名称
        code_lower = stock_code.lower()
        name_lower = stock_name.lower()
        
        # 检查股票代码（多种格式）
        code_patterns = [
            code_lower,  # 000001
            code_lower[:6],  # 000001
            code_lower[:6] + '.sz',  # 000001.sz
            code_lower[:6] + '.sh',  # 000001.sh
        ]
        
        has_code = any(p in text for p in code_patterns)
        has_name = name_lower in text
        
        # 如果既没有代码也没有名称，可能是无关新闻
        if not has_code and not has_name:
            return False
        
        # 检查是否包含其他股票代码（可能是汇总新闻）
        other_stocks = re.findall(r'\d{6}', text)
        if len(other_stocks) > 3:
            # 包含3个以上股票代码，可能是市场综述而非个股新闻
            return False
        
        return True
    
    def is_duplicate(self, title: str) -> bool:
        """检查是否重复标题"""
        # 简化标题（去除标点、空格）
        simplified = re.sub(r'[^\w\u4e00-\u9fff]', '', title)
        
        if simplified in self.seen_titles:
            return True
        
        self.seen_titles.add(simplified)
        return False
    
    def is_too_old(self, news_date: str) -> bool:
        """检查新闻是否过期"""
        if not news_date:
            return False  # 无日期默认不过期
        
        try:
            # 解析日期
            if isinstance(news_date, str):
                if ' ' in news_date:
                    news_date = news_date.split(' ')[0]
                news_dt = datetime.strptime(news_date, '%Y-%m-%d')
            else:
                news_dt = news_date
            
            # 检查是否超过最大年龄
            cutoff = datetime.now() - timedelta(days=self.max_age_days)
            return news_dt < cutoff
            
        except:
            return False
    
    def calculate_quality_score(self, news: Dict) -> float:
        """
        计算新闻质量分 (0-100)
        
        评分维度:
        - 来源可信度 (40%)
        - 标题质量 (30%)
        - 内容长度 (20%)
        - 时效性 (10%)
        """
        score = 0.0
        
        # 1. 来源可信度 (40分)
        source = news.get('source', '')
        credibility = self.get_source_credibility(source)
        score += credibility * 40
        
        # 2. 标题质量 (30分)
        title = news.get('title', '')
        if not self.is_low_quality_title(title):
            score += 30
        
        # 3. 内容长度 (20分)
        content = news.get('content', '')
        content_len = len(content)
        if content_len >= 500:
            score += 20
        elif content_len >= 200:
            score += 15
        elif content_len >= 100:
            score += 10
        else:
            score += 5
        
        # 4. 时效性 (10分)
        news_date = news.get('datetime', news.get('date', ''))
        if not self.is_too_old(news_date):
            score += 10
        
        return score
    
    def filter(self, news_list: List[Dict], stock_code: str = None, 
               stock_name: str = None, top_n: int = None) -> List[Dict]:
        """
        过滤新闻列表
        
        Args:
            news_list: 原始新闻列表
            stock_code: 股票代码（用于相关性过滤）
            stock_name: 股票名称（用于相关性过滤）
            top_n: 只返回前N条高质量新闻
        
        Returns:
            过滤后的新闻列表
        """
        filtered = []
        
        for news in news_list:
            # 1. 检查来源可信度
            source = news.get('source', '')
            credibility = self.get_source_credibility(source)
            if credibility < self.min_credibility:
                continue
            
            # 2. 检查标题质量
            title = news.get('title', '')
            if self.is_low_quality_title(title):
                continue
            
            # 3. 检查相关性（如果提供了股票代码）
            if stock_code and stock_name:
                if not self.is_relevant(news, stock_code, stock_name):
                    continue
            
            # 4. 检查重复
            if self.is_duplicate(title):
                continue
            
            # 5. 检查时效性
            news_date = news.get('datetime', news.get('date', ''))
            if self.is_too_old(news_date):
                continue
            
            # 6. 计算质量分
            quality_score = self.calculate_quality_score(news)
            news['quality_score'] = quality_score
            
            filtered.append(news)
        
        # 按质量分排序
        filtered.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
        
        # 只返回前N条
        if top_n and len(filtered) > top_n:
            filtered = filtered[:top_n]
        
        return filtered
    
    def get_stats(self) -> Dict:
        """获取过滤统计"""
        return {
            'seen_titles': len(self.seen_titles),
            'min_credibility': self.min_credibility,
            'max_age_days': self.max_age_days
        }


# 便捷函数
def filter_news(news_list: List[Dict], stock_code: str = None, 
                stock_name: str = None, top_n: int = 5) -> List[Dict]:
    """
    快速过滤新闻
    
    Args:
        news_list: 原始新闻列表
        stock_code: 股票代码
        stock_name: 股票名称
        top_n: 返回前N条
    
    Returns:
        过滤后的新闻列表
    """
    filter = NewsFilter()
    return filter.filter(news_list, stock_code, stock_name, top_n)


if __name__ == "__main__":
    # 测试
    test_news = [
        {
            'title': '平安银行2025年对公业务发挥"压舱石"作用',
            'content': '平安银行2025年对公业务表现亮眼，发挥了压舱石作用...',
            'source': '东方财富',
            'datetime': '2026-04-02'
        },
        {
            'title': '震惊！这只股票突然暴涨，网友都炸了！',
            'content': '快来看看发生了什么...',
            'source': '自媒体',
            'datetime': '2026-04-02'
        },
        {
            'title': '一文读懂银行股投资逻辑',
            'content': '银行股是A股重要板块，本文从多个维度解析...',
            'source': '新浪财经',
            'datetime': '2026-04-01'
        },
        {
            'title': '平安银行对公业务发挥"压舱石"作用',  # 重复
            'content': '重复内容...',
            'source': '同花顺',
            'datetime': '2026-04-02'
        },
    ]
    
    print("原始新闻:", len(test_news))
    for n in test_news:
        print(f"  - {n['title'][:30]}... (来源: {n['source']})")
    
    filter = NewsFilter()
    filtered = filter.filter(test_news, '000001', '平安银行', top_n=5)
    
    print("\n过滤后:", len(filtered))
    for n in filtered:
        print(f"  - {n['title'][:30]}... (质量分: {n.get('quality_score', 0):.1f})")
