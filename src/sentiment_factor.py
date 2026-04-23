#!/usr/bin/env python3
"""
舆情因子模块 - V10多因子评分系统 (Phase 1)
=====================================
整合 news_provider + ai_sentiment_analyzer + news_credibility
权重：5%
评分范围：-10 ~ +10

作者：大X & 小X
日期：2026-03-24
版本：1.0.0
"""

import os
import json
import logging
import sqlite3
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class NewsItem:
    """新闻条目"""
    title: str
    content: str
    source: str
    datetime: str
    url: str = ""


@dataclass
class SentimentResult:
    """情感分析结果"""
    score: float  # -1.0 ~ +1.0
    confidence: float  # 0.0 ~ 1.0
    category: str  # 'positive', 'negative', 'neutral'
    summary: str = ""


class SentimentFactorCalculator:
    """
    舆情因子计算器 - V10集成版
    
    权重：5%
    评分范围：-10 ~ +10
    
    特性：
    1. 多源新闻获取（AkShare + Coze）
    2. AI情感分析（DeepSeek/MiniMax）
    3. 新闻来源可信度评估
    4. 本地缓存机制（SQLite）
    5. 批量计算支持
    6. 异常降级处理
    """
    
    # 可信度权重映射（根据用户建议）
    CREDIBILITY_WEIGHTS = {
        # 官方媒体
        '新华社': 1.0, '人民日报': 1.0, '央视': 1.0,
        '证券时报': 0.95, '上海证券报': 0.95, '中国证券报': 0.95,
        '经济观察报': 0.9, '第一财经': 0.9, '财联社': 0.9,
        # 主流门户
        '新浪财经': 0.75, '东方财富': 0.75, '同花顺': 0.75,
        '腾讯财经': 0.7, '网易财经': 0.7,
        # 自媒体（降权）
        '自媒体': 0.3, '论坛': 0.3, '微博': 0.3,
    }
    
    # 默认可信度
    DEFAULT_CREDIBILITY = 0.5
    
    # API调用间隔（避免限流）
    API_DELAY = 0.1  # 秒
    
    def __init__(self, cache_dir: str = None):
        """
        初始化舆情因子计算器
        
        Args:
            cache_dir: 缓存目录，默认为 src/data_cache
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(__file__), 'data_cache')
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # 初始化数据库连接
        self.cache_db = os.path.join(cache_dir, 'sentiment_cache.db')
        self._init_cache_db()
        
        # 延迟导入（避免循环依赖）
        self._news_provider = None
        self._sentiment_analyzer = None
        
        logger.info(f"✅ 舆情因子计算器初始化完成，缓存: {self.cache_db}")
    
    def _init_cache_db(self):
        """初始化缓存数据库"""
        try:
            with sqlite3.connect(self.cache_db) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS sentiment_cache (
                        code TEXT NOT NULL,
                        date TEXT NOT NULL,
                        sentiment_score REAL,
                        news_count INTEGER,
                        credibility_avg REAL,
                        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (code, date)
                    )
                ''')
                # 创建索引加速查询
                conn.execute('CREATE INDEX IF NOT EXISTS idx_code_date ON sentiment_cache(code, date)')
                conn.commit()
                logger.debug("缓存数据库初始化完成")
        except Exception as e:
            logger.error(f"缓存数据库初始化失败: {e}")
    
    def _get_news_provider(self):
        """延迟初始化新闻提供者"""
        if self._news_provider is None:
            try:
                from news_provider import NewsAggregator
                self._news_provider = NewsAggregator()
                logger.debug("新闻提供者初始化成功")
            except Exception as e:
                logger.error(f"新闻提供者初始化失败: {e}")
                self._news_provider = None
        return self._news_provider
    
    def _get_sentiment_analyzer(self):
        """延迟初始化情感分析器"""
        if self._sentiment_analyzer is None:
            try:
                from ai_sentiment_analyzer import SentimentAnalyzer
                self._sentiment_analyzer = SentimentAnalyzer()
                logger.debug("情感分析器初始化成功")
            except Exception as e:
                logger.error(f"情感分析器初始化失败: {e}")
                self._sentiment_analyzer = None
        return self._sentiment_analyzer
    
    def get_source_credibility(self, source: str) -> float:
        """
        获取新闻来源可信度
        
        Args:
            source: 新闻来源名称
            
        Returns:
            可信度权重 0.0 ~ 1.0
        """
        if not source:
            return self.DEFAULT_CREDIBILITY
        
        source = source.strip()
        
        # 精确匹配
        if source in self.CREDIBILITY_WEIGHTS:
            return self.CREDIBILITY_WEIGHTS[source]
        
        # 模糊匹配
        for key, weight in self.CREDIBILITY_WEIGHTS.items():
            if key in source or source in key:
                return weight
        
        # 检查是否包含特定关键词
        high_cred_keywords = ['证券报', '时报', '财联社', '新华社', '人民日报']
        low_cred_keywords = ['自媒体', '微博', '论坛', '贴吧']
        
        for kw in high_cred_keywords:
            if kw in source:
                return 0.85
        
        for kw in low_cred_keywords:
            if kw in source:
                return 0.3
        
        return self.DEFAULT_CREDIBILITY
    
    def get_cached_sentiment(self, code: str, date: str) -> Optional[Dict]:
        """
        获取缓存的舆情数据
        
        Args:
            code: 股票代码
            date: 日期 YYYY-MM-DD
            
        Returns:
            缓存数据或None
        """
        try:
            with sqlite3.connect(self.cache_db) as conn:
                cursor = conn.execute(
                    '''SELECT sentiment_score, news_count, credibility_avg 
                       FROM sentiment_cache WHERE code = ? AND date = ?''',
                    (code, date)
                )
                row = cursor.fetchone()
                if row:
                    logger.debug(f"{code} 命中缓存: {date}")
                    return {
                        'sentiment_score': row[0],
                        'news_count': row[1],
                        'credibility_avg': row[2]
                    }
        except Exception as e:
            logger.debug(f"读取缓存失败: {e}")
        return None
    
    def cache_sentiment(self, code: str, date: str, result: Dict):
        """
        缓存舆情数据
        
        Args:
            code: 股票代码
            date: 日期
            result: 舆情分析结果
        """
        try:
            with sqlite3.connect(self.cache_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO sentiment_cache 
                    (code, date, sentiment_score, news_count, credibility_avg, cached_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (code, date, result.get('sentiment_score', 0),
                     result.get('news_count', 0), result.get('credibility_avg', 0.5)))
                conn.commit()
                logger.debug(f"{code} 缓存已更新: {date}")
        except Exception as e:
            logger.debug(f"写入缓存失败: {e}")
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """解析日期时间字符串"""
        if not datetime_str:
            return None
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%Y/%m/%d %H:%M:%S',
            '%Y/%m/%d',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str[:len(fmt)], fmt)
            except:
                continue
        
        return None
    
    def calculate_sentiment_factor(self, code: str, name: str, 
                                    date: str = None) -> Dict:
        """
        计算舆情因子得分 - V10集成版（核心方法）
        
        Args:
            code: 股票代码
            name: 股票名称
            date: 目标日期，默认今天
            
        Returns:
            {
                'score': float,              # 最终因子得分 -10 ~ +10
                'raw_sentiment': float,      # 原始情感分 -1 ~ +1
                'news_count': int,           # 新闻数量
                'credibility_avg': float,    # 平均可信度
                'details': Dict              # 详细信息
            }
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 检查缓存
        cached = self.get_cached_sentiment(code, date)
        if cached:
            logger.debug(f"{code} 使用缓存的舆情数据")
            return self._convert_to_factor(cached)
        
        # 2. 获取新闻（近3日）
        try:
            news_provider = self._get_news_provider()
            if news_provider is None:
                logger.warning("新闻提供者未初始化，使用降级方案")
                return self._fallback_result(code, name)
            
            news_list = news_provider.get_stock_news(code, name)
            
            if not news_list:
                logger.debug(f"{code} 未找到相关新闻")
                return self._default_result()
            
            # 过滤近3日新闻
            cutoff = datetime.now() - timedelta(days=3)
            recent_news = []
            for news in news_list:
                news_date = self._parse_datetime(news.get('datetime', ''))
                if news_date and news_date >= cutoff:
                    recent_news.append(news)
                elif not news_date:
                    # 无法解析日期则保留
                    recent_news.append(news)
            
            if not recent_news:
                logger.debug(f"{code} 近3日无新闻")
                return self._default_result()
            
        except Exception as e:
            logger.warning(f"{code} 获取新闻失败: {e}")
            return self._fallback_result(code, name)
        
        # 3. 情感分析 + 可信度计算
        # 优先使用本地关键词匹配（稳定可靠），API作为备选
        weighted_sentiments = []
        credibility_sum = 0
        analyzed_count = 0
        
        for i, news in enumerate(recent_news[:10]):  # 最多分析10条
            try:
                # 情感分析：优先使用本地关键词匹配（不依赖外部API）
                sentiment_score = self._simple_sentiment(
                    news.get('title', '') + ' ' + news.get('content', '')[:200]
                )
                
                # 如果本地分析为中性(0)，尝试使用AI分析（如果有的话）
                if sentiment_score == 0:
                    sentiment_analyzer = self._get_sentiment_analyzer()
                    if sentiment_analyzer:
                        try:
                            sentiment_result = sentiment_analyzer.analyze_sentiment(
                                code, name, [news]
                            )
                            sentiment_score = sentiment_result.get('sentiment_score', 0)
                        except:
                            pass  # API失败时保持本地分析结果
                
                # 可信度
                credibility = self.get_source_credibility(news.get('source', ''))
                
                weighted_sentiments.append(sentiment_score * credibility)
                credibility_sum += credibility
                analyzed_count += 1
                
            except Exception as e:
                logger.debug(f"分析单条新闻失败: {e}")
                continue
        
        if not weighted_sentiments:
            logger.debug(f"{code} 无有效情感分析结果")
            return self._default_result()
        
        # 4. 计算加权平均情感分
        avg_sentiment = sum(weighted_sentiments) / max(1, credibility_sum)
        
        # 5. 考虑新闻热度（数量）
        news_count = len(recent_news)
        heat_bonus = min(2.0, news_count * 0.2)  # 最高+2分
        
        # 6. 综合得分公式（根据用户建议）
        # 舆情得分 = 加权情感分 * (1 + 热度加成/10)
        final_sentiment = avg_sentiment * (1 + heat_bonus / 10)
        
        result = {
            'sentiment_score': final_sentiment,
            'news_count': news_count,
            'credibility_avg': credibility_sum / analyzed_count if analyzed_count > 0 else 0.5,
            'raw_sentiment': avg_sentiment,
            'heat_bonus': heat_bonus,
            'analyzed_count': analyzed_count
        }
        
        # 7. 缓存结果
        self.cache_sentiment(code, date, result)
        
        return self._convert_to_factor(result)
    
    def _simple_sentiment(self, text: str) -> float:
        """
        增强版本地情感分析（不依赖外部API）
        
        Args:
            text: 文本内容
            
        Returns:
            情感得分 -1.0 ~ +1.0
        """
        if not text:
            return 0.0
        
        text = text.lower()
        
        # 正面关键词（扩展版）
        positive_words = [
            '上涨', '涨停', '利好', '增长', '突破', '强势', '看好', '反弹',
            '增持', '买入', '推荐', '买入评级', '强烈推荐', '增持评级',
            '超预期', '龙头', '领涨', '创新高', '放量上涨', '资金流入',
            '业绩大增', '利润增长', '营收增长', '订单饱满', '产能扩张',
            '政策支持', '行业景气', '供需紧张', '涨价', '产品提价',
            '技术突破', '研发成功', '新药获批', '订单暴增', '中标',
            '并购重组', '资产注入', '股权激励', '回购', '分红',
            '北向资金流入', '机构加仓', '主力买入', '游资抢筹'
        ]
        
        # 负面关键词（扩展版）
        negative_words = [
            '下跌', '跌停', '利空', '下滑', '跌破', '弱势', '看空', '调整',
            '减持', '卖出', '回避', '卖出评级', '减持评级', '中性评级',
            '低于预期', '风险', '暴雷', '踩雷', '业绩下滑', '利润下降',
            '亏损', '亏损扩大', '营收下滑', '订单减少', '产能过剩',
            '政策打压', '行业低迷', '供过于求', '降价', '产品降价',
            '技术失败', '研发失败', '新药被拒', '订单取消', '失标',
            '分拆', '资产剥离', '股权质押', '爆仓', '债务违约',
            '北向资金流出', '机构减仓', '主力卖出', '游资出逃',
            '立案调查', '监管函', '问询函', '关注函', '警示函',
            '财务造假', '信披违规', '内幕交易', '操纵市场'
        ]
        
        pos_count = sum(1 for w in positive_words if w in text)
        neg_count = sum(1 for w in negative_words if w in text)
        
        # 加权计算
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        
        # 归一化到 -1.0 ~ +1.0
        sentiment = (pos_count - neg_count) / max(total, 3)  # 至少3个词才满额
        return max(-1.0, min(1.0, sentiment))
    
    def _convert_to_factor(self, data: Dict) -> Dict:
        """
        转换为V10因子得分（-10 ~ +10）
        
        Args:
            data: 原始舆情数据
            
        Returns:
            标准化后的因子结果
        """
        sentiment = data.get('sentiment_score', 0)
        news_count = data.get('news_count', 0)
        
        # 映射到 -10 ~ +10
        factor_score = sentiment * 10  # -1~1 映射到 -10~10
        
        # 根据用户建议的评分细则调整
        final_score = 0  # 基础分
        sentiment_level = 'neutral'
        
        # 正面舆情
        if factor_score > 5:
            final_score = 8
            sentiment_level = 'strong_positive'
        elif factor_score > 3:
            final_score = 5
            sentiment_level = 'positive'
        elif factor_score > 1:
            final_score = 3
            sentiment_level = 'weak_positive'
        elif factor_score > 0:
            final_score = 1
            sentiment_level = 'slight_positive'
        
        # 负面舆情
        elif factor_score < -5:
            final_score = -10  # 强烈减分
            sentiment_level = 'strong_negative'
        elif factor_score < -3:
            final_score = -6
            sentiment_level = 'negative'
        elif factor_score < -1:
            final_score = -3
            sentiment_level = 'weak_negative'
        elif factor_score < 0:
            final_score = -1
            sentiment_level = 'slight_negative'
        
        # 新闻热度加成
        heat_level = 'low'
        if news_count >= 10:
            final_score += 2
            heat_level = 'high'
        elif news_count >= 5:
            final_score += 1
            heat_level = 'medium'
        
        # 限制范围
        final_score = max(-10, min(10, final_score))
        
        return {
            'score': final_score,
            'raw_sentiment': data.get('raw_sentiment', 0),
            'news_count': news_count,
            'credibility_avg': data.get('credibility_avg', 0.5),
            'details': {
                'sentiment_level': sentiment_level,
                'heat_level': heat_level,
                'analyzed_count': data.get('analyzed_count', 0)
            }
        }
    
    def _default_result(self) -> Dict:
        """默认结果（无新闻时）- 中性"""
        return {
            'score': 0,
            'raw_sentiment': 0,
            'news_count': 0,
            'credibility_avg': 0.5,
            'details': {
                'sentiment_level': 'neutral',
                'heat_level': 'none',
                'note': '无相关新闻'
            }
        }
    
    def _fallback_result(self, code: str, name: str) -> Dict:
        """
        降级方案：使用缓存数据或返回中性
        
        Args:
            code: 股票代码
            name: 股票名称
            
        Returns:
            降级后的舆情因子结果
        """
        # 尝试获取最近缓存
        try:
            with sqlite3.connect(self.cache_db) as conn:
                cursor = conn.execute(
                    '''SELECT sentiment_score, news_count, credibility_avg 
                       FROM sentiment_cache 
                       WHERE code = ? ORDER BY date DESC LIMIT 1''',
                    (code,)
                )
                row = cursor.fetchone()
                if row:
                    logger.debug(f"{code} 使用历史缓存数据（API异常）")
                    return self._convert_to_factor({
                        'sentiment_score': row[0],
                        'news_count': 0,
                        'credibility_avg': row[2],
                        'note': '使用缓存数据（API异常）'
                    })
        except Exception as e:
            logger.debug(f"降级查询失败: {e}")
        
        # 最终降级：返回中性
        result = self._default_result()
        result['details']['note'] = 'API异常，使用默认中性值'
        return result
    
    def batch_calculate(self, stock_list: List[Tuple[str, str]], 
                        date: str = None,
                        progress_interval: int = 100,
                        use_cache_only: bool = False) -> Dict[str, Dict]:
        """
        批量计算舆情因子（用于选股前）
        
        Args:
            stock_list: [(code, name), ...]
            date: 目标日期，默认今天
            progress_interval: 进度报告间隔
            use_cache_only: 仅使用缓存（不实时搜索，更快）
            
        Returns:
            {code: factor_result, ...}
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        results = {}
        total = len(stock_list)
        cache_hits = 0
        new_calcs = 0
        
        logger.info(f"📊 开始批量计算舆情因子: {total}只股票 (仅缓存: {use_cache_only})")
        
        for i, (code, name) in enumerate(stock_list):
            if i % progress_interval == 0 and i > 0:
                logger.info(f"  舆情分析进度: {i}/{total} ({i/total*100:.1f}%), 缓存命中: {cache_hits}")
            
            try:
                # 先检查缓存
                cached = self.get_cached_sentiment(code, date)
                if cached:
                    results[code] = self._convert_to_factor(cached)
                    cache_hits += 1
                    continue
                
                # 仅缓存模式：无缓存则返回中性
                if use_cache_only:
                    results[code] = self._default_result()
                    results[code]['details']['note'] = '无缓存数据'
                    continue
                
                # 实时计算
                result = self.calculate_sentiment_factor(code, name, date)
                results[code] = result
                new_calcs += 1
                
            except Exception as e:
                logger.debug(f"{code} 舆情分析异常: {e}")
                results[code] = self._fallback_result(code, name)
            
            # 添加延迟避免API限流
            if not use_cache_only and i % 10 == 0 and i > 0:
                import time
                time.sleep(self.API_DELAY)
        
        logger.info(f"✅ 舆情因子计算完成: {total}只 (缓存命中: {cache_hits}, 新增: {new_calcs})")
        return results
    
    def get_sentiment_stats(self, date: str = None) -> Dict:
        """
        获取舆情统计信息
        
        Args:
            date: 日期，默认今天
            
        Returns:
            统计信息
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            with sqlite3.connect(self.cache_db) as conn:
                # 总记录数
                cursor = conn.execute('SELECT COUNT(*) FROM sentiment_cache WHERE date = ?', (date,))
                total = cursor.fetchone()[0]
                
                # 正面舆情数
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM sentiment_cache WHERE date = ? AND sentiment_score > 0',
                    (date,)
                )
                positive = cursor.fetchone()[0]
                
                # 负面舆情数
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM sentiment_cache WHERE date = ? AND sentiment_score < 0',
                    (date,)
                )
                negative = cursor.fetchone()[0]
                
                return {
                    'date': date,
                    'total_cached': total,
                    'positive': positive,
                    'negative': negative,
                    'neutral': total - positive - negative
                }
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}


# ============================================================================
# 单例模式（避免重复初始化）
# ============================================================================

_sentiment_calculator = None

def get_sentiment_calculator() -> SentimentFactorCalculator:
    """
    获取舆情因子计算器单例
    
    Returns:
        SentimentFactorCalculator 实例
    """
    global _sentiment_calculator
    if _sentiment_calculator is None:
        _sentiment_calculator = SentimentFactorCalculator()
    return _sentiment_calculator


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 测试
    calc = SentimentFactorCalculator()
    
    # 单只测试
    result = calc.calculate_sentiment_factor("000001", "平安银行")
    print(f"\n单只测试结果:")
    print(f"  股票: 平安银行 (000001)")
    print(f"  舆情得分: {result['score']}")
    print(f"  原始情感: {result['raw_sentiment']}")
    print(f"  新闻数量: {result['news_count']}")
    print(f"  可信度: {result['credibility_avg']}")
    print(f"  详情: {result['details']}")
    
    # 批量测试
    test_stocks = [
        ("000001", "平安银行"),
        ("000002", "万科A"),
        ("600519", "贵州茅台"),
    ]
    
    print(f"\n批量测试: {len(test_stocks)}只股票")
    batch_results = calc.batch_calculate(test_stocks, progress_interval=1)
    
    for code, result in batch_results.items():
        print(f"  {code}: 得分={result['score']}, 新闻={result['news_count']}")
    
    # 统计信息
    stats = calc.get_sentiment_stats()
    print(f"\n今日舆情统计: {stats}")
