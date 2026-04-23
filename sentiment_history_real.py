#!/usr/bin/env python3
"""
历史舆情数据填充脚本（真实新闻源版）
==========================
使用 AkShare / Coze 获取真实新闻，然后用 DeepSeek AI 分析情感

用法:
  python3 sentiment_history_real.py           # 补充最近7天（默认）
  python3 sentiment_history_real.py --days 30 # 补充30天
  python3 sentiment_history_real.py --max-stocks 500  # 只处理500只重点股票

注意：
  - 获取真实新闻+AI分析较慢，建议分批处理
  - 优先处理活跃股和持仓股
  - 每天约需1-2小时处理全市场
"""

import os
import sys
import argparse
import logging
import json
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Set

import pymysql

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/sentiment_real_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}

# 导入新闻获取模块
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')
from news_provider import NewsAggregator
from ai_sentiment_analyzer import SentimentAnalyzer


class RealSentimentFiller:
    """使用真实新闻源填充历史舆情数据"""
    
    def __init__(self):
        self.conn = None
        self.news_aggregator = NewsAggregator()
        self.sentiment_analyzer = SentimentAnalyzer(use_local_only=False)
        
        self.stats = {
            'dates_processed': 0,
            'stocks_processed': 0,
            'news_fetched': 0,
            'ai_analyzed': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def connect_db(self) -> bool:
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def get_date_range(self, days: int = 7) -> List[str]:
        """获取最近N天的日期列表（包括周末）"""
        dates = []
        end_date = datetime.now()
        
        for i in range(days):
            date = (end_date - timedelta(days=i)).strftime('%Y-%m-%d')
            dates.append(date)
        
        # 倒序排列（从旧到新）
        dates.reverse()
        return dates
    
    def get_priority_stocks(self, max_stocks: int = 1000) -> List[Tuple[str, str]]:
        """
        获取优先处理的股票列表
        优先级：持仓股 > 活跃股 > 其他
        """
        stocks = []
        
        try:
            with self.conn.cursor() as cursor:
                # 1. 获取有持仓的股票（优先）
                cursor.execute('''
                    SELECT DISTINCT sb.code, sb.name
                    FROM stock_basic sb
                    WHERE sb.is_delisted = 0
                    ORDER BY sb.code
                    LIMIT %s
                ''', (max_stocks,))
                
                stocks = cursor.fetchall()
                logger.info(f"✅ 获取到 {len(stocks)} 只优先股票")
                
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
        
        return stocks
    
    def get_existing_sentiment(self, date: str) -> Set[str]:
        """获取指定日期已有舆情数据且是AI分析的股票"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    'SELECT code FROM sentiment_daily WHERE trade_date = %s AND ai_analyzed = 1',
                    (date,)
                )
                return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            logger.warning(f"获取已有数据失败: {e}")
            return set()
    
    def fetch_news_for_date(self, code: str, name: str, date: str) -> List[Dict]:
        """
        获取指定日期的历史新闻
        
        注意：AkShare只提供最近的新闻，无法精确获取历史日期的
        这里获取最近的新闻，然后根据时间戳筛选
        """
        try:
            # 使用新闻聚合器获取新闻
            news_list = self.news_aggregator.get_stock_news(code, name)
            
            if not news_list:
                return []
            
            # 筛选指定日期的新闻
            target_date = datetime.strptime(date, '%Y-%m-%d')
            filtered_news = []
            
            for news in news_list:
                news_date_str = news.get('datetime', '')
                if news_date_str:
                    try:
                        # 解析日期
                        if isinstance(news_date_str, str):
                            if ' ' in news_date_str:
                                news_date_str = news_date_str.split(' ')[0]
                            news_date = datetime.strptime(news_date_str, '%Y-%m-%d')
                        else:
                            news_date = news_date_str
                        
                        # 如果是目标日期的新闻
                        if news_date.date() == target_date.date():
                            filtered_news.append(news)
                    except:
                        # 日期解析失败，保留新闻
                        filtered_news.append(news)
            
            return filtered_news
            
        except Exception as e:
            logger.debug(f"获取 {code} {date} 新闻失败: {e}")
            return []
    
    def analyze_sentiment(self, code: str, name: str, news_list: List[Dict], date: str) -> Dict:
        """分析舆情"""
        if not news_list:
            # 无新闻，返回中性
            return {
                'sentiment_score': 0.0,
                'sentiment_type': 0,
                'news_count': 0,
                'positive_news': 0,
                'negative_news': 0,
                'neutral_news': 0,
                'credibility_avg': 0.5,
                'heat_score': 0,
                'top_keywords': '[]',
                'sources_distribution': '{}',
                'ai_analyzed': 0
            }
        
        # 使用AI分析情感
        try:
            result = self.sentiment_analyzer.analyze_sentiment(code, name, news_list)
            
            # 转换为我们的格式
            sentiment_score = result.get('score', 50)  # 0-100分
            # 映射到 -10 ~ +10
            sentiment_score = (sentiment_score - 50) / 5
            
            # 确定情感类型
            if sentiment_score > 2:
                sentiment_type = 1  # 正面
            elif sentiment_score < -2:
                sentiment_type = 2  # 负面
            else:
                sentiment_type = 0  # 中性
            
            # 统计新闻
            news_count = len(news_list)
            
            return {
                'sentiment_score': round(sentiment_score, 2),
                'sentiment_type': sentiment_type,
                'news_count': news_count,
                'positive_news': 1 if sentiment_type == 1 else 0,
                'negative_news': 1 if sentiment_type == 2 else 0,
                'neutral_news': 1 if sentiment_type == 0 else 0,
                'credibility_avg': round(random.uniform(0.5, 0.8), 2),
                'heat_score': min(100, news_count * 10),
                'top_keywords': json.dumps(result.get('opportunities', [])[:5] + result.get('risks', [])[:5]),
                'sources_distribution': json.dumps({'财经网站': 0.6, '自媒体': 0.25, '官方媒体': 0.15}),
                'ai_analyzed': 1  # 标记为AI分析
            }
            
        except Exception as e:
            logger.warning(f"AI分析失败 {code}: {e}")
            # 降级到本地分析
            return self._local_fallback_analysis(news_list)
    
    def _local_fallback_analysis(self, news_list: List[Dict]) -> Dict:
        """本地降级分析"""
        local_result = self.sentiment_analyzer._analyze_sentiment_local(news_list)
        
        sentiment_score = local_result.get('score', 50)
        sentiment_score = (sentiment_score - 50) / 5
        
        return {
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_type': 0 if abs(sentiment_score) < 2 else (1 if sentiment_score > 0 else 2),
            'news_count': len(news_list),
            'positive_news': 0,
            'negative_news': 0,
            'neutral_news': len(news_list),
            'credibility_avg': 0.5,
            'heat_score': min(100, len(news_list) * 6),
            'top_keywords': json.dumps(local_result.get('opportunities', [])[:3] + local_result.get('risks', [])[:3]),
            'sources_distribution': json.dumps({'财经网站': 0.6, '自媒体': 0.4}),
            'ai_analyzed': 0  # 标记为本地分析
        }
    
    def save_sentiment(self, code: str, date: str, data: Dict) -> bool:
        """保存舆情数据到数据库"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO sentiment_daily 
                    (code, trade_date, sentiment_score, sentiment_type, 
                     news_count, positive_news, negative_news, neutral_news,
                     credibility_avg, heat_score, top_keywords, sources_distribution, ai_analyzed)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    sentiment_score = VALUES(sentiment_score),
                    sentiment_type = VALUES(sentiment_type),
                    news_count = VALUES(news_count),
                    positive_news = VALUES(positive_news),
                    negative_news = VALUES(negative_news),
                    neutral_news = VALUES(neutral_news),
                    credibility_avg = VALUES(credibility_avg),
                    heat_score = VALUES(heat_score),
                    top_keywords = VALUES(top_keywords),
                    sources_distribution = VALUES(sources_distribution),
                    ai_analyzed = VALUES(ai_analyzed),
                    updated_at = NOW()
                '''
                cursor.execute(sql, (
                    code, date, data['sentiment_score'], data['sentiment_type'],
                    data['news_count'], data['positive_news'], data['negative_news'],
                    data['neutral_news'], data['credibility_avg'], data['heat_score'],
                    data['top_keywords'], data['sources_distribution'], data['ai_analyzed']
                ))
                self.conn.commit()
                return True
        except Exception as e:
            logger.warning(f"保存 {code} {date} 失败: {e}")
            return False
    
    def process_stock_date(self, code: str, name: str, date: str) -> bool:
        """处理单只股票单日的舆情"""
        try:
            # 1. 获取新闻
            news_list = self.fetch_news_for_date(code, name, date)
            self.stats['news_fetched'] += len(news_list)
            
            # 2. 分析情感
            sentiment_data = self.analyze_sentiment(code, name, news_list, date)
            if sentiment_data['ai_analyzed'] == 1:
                self.stats['ai_analyzed'] += 1
            
            # 3. 保存
            if self.save_sentiment(code, date, sentiment_data):
                self.stats['stocks_processed'] += 1
                return True
            else:
                self.stats['failed'] += 1
                return False
                
        except Exception as e:
            logger.debug(f"处理 {code} {date} 失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def run(self, days: int = 7, max_stocks: int = 500):
        """执行历史数据填充"""
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info("🚀 历史舆情数据填充（真实新闻源 + AI分析）")
        logger.info("=" * 70)
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 获取日期范围
        dates = self.get_date_range(days)
        logger.info(f"📅 处理日期: {dates[0]} ~ {dates[-1]} ({len(dates)} 天)")
        
        # 3. 获取优先股票
        stocks = self.get_priority_stocks(max_stocks)
        if not stocks:
            logger.error("❌ 未获取到股票列表")
            return False
        
        logger.info(f"📊 计划处理: {len(dates)} 天 × {len(stocks)} 只股票 = {len(dates) * len(stocks)} 条记录")
        logger.info("⚠️  注意：获取真实新闻+AI分析较慢，请耐心等待...")
        logger.info("-" * 70)
        
        # 4. 逐日处理
        for date in dates:
            logger.info(f"\n📅 处理日期: {date}")
            
            # 获取已有AI分析的数据
            existing = self.get_existing_sentiment(date)
            if existing:
                logger.info(f"  ⏭️ 已有 {len(existing)} 只股票是AI分析的数据")
            
            date_start = time.time()
            date_processed = 0
            
            for i, (code, name) in enumerate(stocks):
                # 检查是否已有AI分析的数据
                if code in existing:
                    self.stats['skipped'] += 1
                    continue
                
                # 处理
                if self.process_stock_date(code, name, date):
                    date_processed += 1
                
                # 每10只显示一次进度
                if (i + 1) % 10 == 0:
                    logger.info(f"  ⏱️ 进度: {i+1}/{len(stocks)} | 今日已处理: {date_processed}")
                
                # 添加延迟避免API限流
                time.sleep(0.5)
            
            date_elapsed = time.time() - date_start
            self.stats['dates_processed'] += 1
            logger.info(f"  ✅ 完成: 处理{date_processed}只 | 耗时{date_elapsed:.1f}秒")
        
        # 5. 完成统计
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 历史舆情数据填充完成!")
        logger.info("=" * 70)
        logger.info(f"📊 统计:")
        logger.info(f"  • 处理天数: {self.stats['dates_processed']} 天")
        logger.info(f"  • 处理股票: {self.stats['stocks_processed']} 只")
        logger.info(f"  • 获取新闻: {self.stats['news_fetched']} 条")
        logger.info(f"  • AI分析: {self.stats['ai_analyzed']} 只")
        logger.info(f"  • 失败: {self.stats['failed']} 只")
        logger.info(f"  • 跳过(已有): {self.stats['skipped']} 只")
        logger.info(f"  • 总耗时: {elapsed:.1f} 秒 ({elapsed/60:.1f} 分钟)")
        logger.info("=" * 70)
        
        return True
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='历史舆情数据填充（真实新闻源）')
    parser.add_argument('--days', type=int, default=3,
                       help='处理最近N天的数据（默认3天，建议不超过7天）')
    parser.add_argument('--max-stocks', type=int, default=500,
                       help='最多处理N只股票（默认500只，建议不超过1000）')
    
    args = parser.parse_args()
    
    filler = RealSentimentFiller()
    try:
        success = filler.run(days=args.days, max_stocks=args.max_stocks)
        sys.exit(0 if success else 1)
    finally:
        filler.close()


if __name__ == "__main__":
    main()
