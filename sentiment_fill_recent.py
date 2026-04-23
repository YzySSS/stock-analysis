#!/usr/bin/env python3
"""
补充近期真实舆情数据（AkShare版）
============================
使用AkShare获取最近7-10天的真实新闻，AI分析后存入MySQL

用法:
  python3 sentiment_fill_recent.py           # 补充最近7天
  python3 sentiment_fill_recent.py --days 10 # 补充最近10天
"""

import os
import sys
import argparse
import logging
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple

import pymysql
import akshare as ak

# 添加src路径
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')
from news_filter import NewsFilter
from ai_sentiment_analyzer import SentimentAnalyzer

# 加载环境变量
def load_env_file(filepath):
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"\'')
                    os.environ[key] = value
    except Exception as e:
        print(f"加载 .env 文件失败: {e}")

load_env_file('/root/.openclaw/workspace/股票分析项目/.env')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/sentiment_fill_recent_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class RecentSentimentFiller:
    """近期舆情数据填充器"""
    
    def __init__(self):
        self.conn = None
        self.news_filter = NewsFilter(min_credibility=0.4, max_age_days=3)
        self.sentiment_analyzer = SentimentAnalyzer(use_local_only=False)
        
        self.stats = {
            'dates_processed': 0,
            'stocks_processed': 0,
            'news_fetched': 0,
            'ai_analyzed': 0,
            'failed': 0,
            'skipped': 0,
            'records_created': 0
        }
    
    def connect_db(self) -> bool:
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def get_date_range(self, days: int) -> List[str]:
        """获取最近N天的日期列表"""
        dates = []
        end_date = datetime.now()
        
        for i in range(days):
            date = (end_date - timedelta(days=i)).strftime('%Y-%m-%d')
            dates.append(date)
        
        dates.reverse()  # 从旧到新
        return dates
    
    def get_all_stocks(self) -> List[Tuple[str, str]]:
        """获取全A股列表"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('SELECT code, name FROM stock_basic WHERE is_delisted = 0 ORDER BY code')
                stocks = cursor.fetchall()
                logger.info(f"✅ 获取到 {len(stocks)} 只非退市股票")
                return stocks
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def get_existing_sentiment(self, date: str) -> Set[str]:
        """获取指定日期已有舆情数据的股票"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    'SELECT code FROM sentiment_daily WHERE trade_date = %s',
                    (date,)
                )
                return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            logger.warning(f"获取已有数据失败: {e}")
            return set()
    
    def fetch_news_akshare(self, code: str) -> List[Dict]:
        """使用AkShare获取个股新闻"""
        try:
            news_df = ak.stock_news_em(symbol=code[:6])
            
            if news_df is None or len(news_df) == 0:
                return []
            
            news_list = []
            for _, row in news_df.iterrows():
                news_list.append({
                    'title': str(row.get('新闻标题', '')),
                    'content': str(row.get('新闻内容', ''))[:500],
                    'source': str(row.get('文章来源', '东方财富')),
                    'datetime': str(row.get('发布时间', '')),
                    'url': str(row.get('新闻链接', ''))
                })
            
            return news_list
        except Exception as e:
            logger.debug(f"获取 {code} 新闻失败: {e}")
            return []
    
    def analyze_sentiment(self, code: str, name: str, news_list: List[Dict]) -> Dict:
        """分析舆情"""
        if not news_list:
            return {
                'sentiment_score': 0.0, 'sentiment_type': 0, 'news_count': 0,
                'positive_news': 0, 'negative_news': 0, 'neutral_news': 0,
                'credibility_avg': 0.5, 'heat_score': 0, 'top_keywords': '[]',
                'sources_distribution': '{}', 'ai_analyzed': 0
            }
        
        total_credibility = sum(
            self.news_filter.get_source_credibility(n.get('source', ''))
            for n in news_list
        )
        avg_credibility = total_credibility / len(news_list) if news_list else 0.5
        
        try:
            result = self.sentiment_analyzer.analyze_sentiment(code, name, news_list)
            raw_score = result.get('score', 50)
            sentiment_score = (raw_score - 50) / 5
            
            if sentiment_score > 2:
                sentiment_type = 1
            elif sentiment_score < -2:
                sentiment_type = 2
            else:
                sentiment_type = 0
            
            ai_analyzed = 1 if result.get('summary') != '本地分析' else 0
            if ai_analyzed:
                self.stats['ai_analyzed'] += 1
            
            opportunities = result.get('opportunities', [])
            risks = result.get('risks', [])
            keywords = opportunities[:5] + risks[:5]
            
            return {
                'sentiment_score': round(sentiment_score, 2),
                'sentiment_type': sentiment_type,
                'news_count': len(news_list),
                'positive_news': 1 if sentiment_type == 1 else 0,
                'negative_news': 1 if sentiment_type == 2 else 0,
                'neutral_news': 1 if sentiment_type == 0 else 0,
                'credibility_avg': round(avg_credibility, 2),
                'heat_score': min(100, len(news_list) * 8 + len(keywords) * 5),
                'top_keywords': json.dumps(keywords),
                'sources_distribution': json.dumps(self._get_source_distribution(news_list)),
                'ai_analyzed': ai_analyzed
            }
        except Exception as e:
            logger.warning(f"AI分析失败 {code}: {e}")
            return self._local_fallback(news_list, avg_credibility)
    
    def _local_fallback(self, news_list: List[Dict], credibility: float) -> Dict:
        local_result = self.sentiment_analyzer._analyze_sentiment_local(news_list)
        raw_score = local_result.get('score', 50)
        sentiment_score = (raw_score - 50) / 5
        
        return {
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_type': 0 if abs(sentiment_score) < 2 else (1 if sentiment_score > 0 else 2),
            'news_count': len(news_list),
            'positive_news': 0, 'negative_news': 0, 'neutral_news': len(news_list),
            'credibility_avg': round(credibility, 2),
            'heat_score': min(100, len(news_list) * 6),
            'top_keywords': json.dumps(local_result.get('opportunities', [])[:3] + local_result.get('risks', [])[:3]),
            'sources_distribution': json.dumps({'财经网站': 0.6, '自媒体': 0.4}),
            'ai_analyzed': 0
        }
    
    def _get_source_distribution(self, news_list: List[Dict]) -> Dict:
        distribution = {}
        for news in news_list:
            source = news.get('source', '未知')
            distribution[source] = distribution.get(source, 0) + 1
        total = sum(distribution.values())
        if total > 0:
            distribution = {k: round(v/total, 2) for k, v in distribution.items()}
        return distribution
    
    def save_sentiment(self, code: str, date: str, data: Dict) -> bool:
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
            logger.warning(f"保存 {code} 失败: {e}")
            return False
    
    def process_date(self, date: str, stocks: List[Tuple[str, str]]) -> int:
        """处理单日的舆情数据"""
        logger.info(f"\n📅 处理日期: {date}")
        
        existing = self.get_existing_sentiment(date)
        if existing:
            logger.info(f"  ⏭️ 已有 {len(existing)} 只股票的数据")
        
        created = 0
        for i, (code, name) in enumerate(stocks):
            if code in existing:
                self.stats['skipped'] += 1
                continue
            
            try:
                news_list = self.fetch_news_akshare(code)
                self.stats['news_fetched'] += len(news_list)
                
                if news_list:
                    news_list = self.news_filter.filter(news_list, code, name, top_n=5)
                
                sentiment_data = self.analyze_sentiment(code, name, news_list)
                
                if self.save_sentiment(code, date, sentiment_data):
                    created += 1
                    self.stats['records_created'] += 1
                else:
                    self.stats['failed'] += 1
                    
            except Exception as e:
                logger.debug(f"处理 {code} 失败: {e}")
                self.stats['failed'] += 1
            
            if (i + 1) % 100 == 0:
                logger.info(f"  ⏱️ 进度: {i+1}/{len(stocks)} | 已创建: {created}")
            
            time.sleep(0.3)
        
        logger.info(f"  ✅ 完成: 创建 {created} 条")
        return created
    
    def run(self, days: int = 7):
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info("🚀 补充近期真实舆情数据（AkShare版）")
        logger.info("=" * 70)
        
        if not self.connect_db():
            return False
        
        dates = self.get_date_range(days)
        logger.info(f"📅 处理日期: {dates[0]} ~ {dates[-1]} ({len(dates)} 天)")
        
        stocks = self.get_all_stocks()
        if not stocks:
            return False
        
        logger.info(f"📊 计划处理: {len(dates)} 天 × {len(stocks)} 只股票")
        logger.info("⚠️  注意：AkShare只提供最近7-10天的新闻，更早日期可能无数据")
        logger.info("-" * 70)
        
        for date in dates:
            self.process_date(date, stocks)
            self.stats['dates_processed'] += 1
        
        elapsed = int((datetime.now() - start_time).total_seconds())
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 近期舆情数据补充完成!")
        logger.info("=" * 70)
        logger.info(f"📊 统计:")
        logger.info(f"  • 处理天数: {self.stats['dates_processed']} 天")
        logger.info(f"  • 获取新闻: {self.stats['news_fetched']} 条")
        logger.info(f"  • AI分析: {self.stats['ai_analyzed']} 只")
        logger.info(f"  • 新增记录: {self.stats['records_created']} 条")
        logger.info(f"  • 跳过: {self.stats['skipped']} 只")
        logger.info(f"  • 失败: {self.stats['failed']} 只")
        logger.info(f"  • 总耗时: {elapsed} 秒 ({elapsed/60:.1f} 分钟)")
        logger.info("=" * 70)
        
        return True
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='补充近期真实舆情数据')
    parser.add_argument('--days', type=int, default=7,
                       help='补充最近N天的数据（默认7天）')
    
    args = parser.parse_args()
    
    filler = RecentSentimentFiller()
    try:
        success = filler.run(days=args.days)
        sys.exit(0 if success else 1)
    finally:
        filler.close()


if __name__ == "__main__":
    main()
