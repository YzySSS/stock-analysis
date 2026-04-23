#!/usr/bin/env python3
"""
历史舆情数据填充脚本
==================
基于历史价格数据生成模拟舆情数据，用于回测

用法:
  python3 sentiment_history_fill.py           # 填充最近30天
  python3 sentiment_history_fill.py --start 2026-03-01 --end 2026-04-02  # 指定日期范围
  python3 sentiment_history_fill.py --batch-size 200  # 调整批次大小
"""

import os
import sys
import argparse
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import pymysql

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/sentiment_history_fill_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库连接配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class SentimentHistoryFiller:
    """历史舆情数据填充器"""
    
    def __init__(self):
        self.conn = None
        self.stats = {
            'total_stocks': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'records_created': 0
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
    
    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取日期范围内的交易日"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT DISTINCT trade_date 
                    FROM stock_kline 
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                ''', (start_date, end_date))
                dates = [row[0].strftime('%Y-%m-%d') if isinstance(row[0], datetime) else str(row[0]) for row in cursor.fetchall()]
                logger.info(f"📅 获取到 {len(dates)} 个交易日: {dates[0]} ~ {dates[-1]}")
                return dates
        except Exception as e:
            logger.error(f"获取交易日失败: {e}")
            return []
    
    def get_all_stocks(self) -> List[Tuple[str, str]]:
        """获取全A股列表"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT code, name 
                    FROM stock_basic 
                    WHERE is_delisted = 0
                    ORDER BY code
                ''')
                stocks = cursor.fetchall()
                logger.info(f"✅ 获取到 {len(stocks)} 只非退市股票")
                return stocks
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def get_existing_sentiment(self, date: str) -> set:
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
    
    def get_price_change(self, code: str, date: str) -> float:
        """获取指定日期的涨跌幅"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT pct_change 
                    FROM stock_kline 
                    WHERE code = %s AND trade_date = %s
                ''', (code, date))
                row = cursor.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
        except Exception as e:
            pass
        return 0.0
    
    def generate_sentiment(self, code: str, name: str, date: str, price_change: float) -> Dict:
        """
        基于涨跌幅生成舆情数据
        
        逻辑:
        - 涨幅大 -> 正面舆情概率高
        - 跌幅大 -> 负面舆情概率高
        - 平盘 -> 中性舆情为主
        """
        # 基于涨跌幅确定基础情感倾向
        if price_change > 5:
            base_sentiment = 2  # 正面
            sentiment_score = random.uniform(5.0, 10.0)
            positive_ratio = random.uniform(0.6, 0.8)
        elif price_change > 2:
            base_sentiment = 1  # 偏正面
            sentiment_score = random.uniform(2.0, 5.0)
            positive_ratio = random.uniform(0.5, 0.7)
        elif price_change < -5:
            base_sentiment = 2  # 负面
            sentiment_score = random.uniform(-10.0, -5.0)
            positive_ratio = random.uniform(0.1, 0.3)
        elif price_change < -2:
            base_sentiment = 2  # 偏负面
            sentiment_score = random.uniform(-5.0, -2.0)
            positive_ratio = random.uniform(0.2, 0.4)
        else:
            base_sentiment = 0  # 中性
            sentiment_score = random.uniform(-2.0, 2.0)
            positive_ratio = random.uniform(0.3, 0.5)
        
        # 新闻数量（热度）
        news_count = random.randint(1, 15)
        
        # 各类新闻数量
        positive_news = int(news_count * positive_ratio)
        negative_news = int(news_count * (1 - positive_ratio) * random.uniform(0.5, 0.8))
        neutral_news = news_count - positive_news - negative_news
        
        # 可信度
        credibility_avg = round(random.uniform(0.45, 0.75), 2)
        
        # 热度分
        heat_score = min(100, news_count * 6 + random.randint(-10, 10))
        
        # 关键词（基于涨跌幅）
        if price_change > 3:
            keywords = ['上涨', '突破', '资金流入', '业绩增长']
        elif price_change < -3:
            keywords = ['下跌', '调整', '资金流出', '业绩下滑']
        else:
            keywords = ['震荡', '整理', '观望', '持平']
        
        # 是否使用AI分析（历史数据统一标记为0）
        ai_analyzed = 0
        
        return {
            'code': code,
            'trade_date': date,
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_type': base_sentiment,
            'news_count': news_count,
            'positive_news': max(0, positive_news),
            'negative_news': max(0, negative_news),
            'neutral_news': max(0, neutral_news),
            'credibility_avg': credibility_avg,
            'heat_score': max(0, heat_score),
            'top_keywords': json.dumps(keywords),
            'sources_distribution': json.dumps({'财经网站': 0.5, '自媒体': 0.3, '官方媒体': 0.2}),
            'ai_analyzed': ai_analyzed
        }
    
    def save_sentiment(self, data: Dict) -> bool:
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
                    ai_analyzed = VALUES(ai_analyzed)
                '''
                cursor.execute(sql, (
                    data['code'], data['trade_date'], data['sentiment_score'],
                    data['sentiment_type'], data['news_count'], data['positive_news'],
                    data['negative_news'], data['neutral_news'], data['credibility_avg'],
                    data['heat_score'], data['top_keywords'], data['sources_distribution'],
                    data['ai_analyzed']
                ))
                self.conn.commit()
                return True
        except Exception as e:
            logger.warning(f"保存 {data['code']} 舆情数据失败: {e}")
            return False
    
    def log_update(self, update_date: str, total: int, success: int, failed: int, 
                   skip: int, duration: int, status: str = 'success'):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO sentiment_update_log 
                    (update_date, update_type, total_stocks, success_count, 
                     fail_count, skip_count, start_time, end_time, duration_seconds, status)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s)
                '''
                cursor.execute(sql, (update_date, 'backfill', total, success, failed, skip, duration, status))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"记录日志失败: {e}")
    
    def fill_date(self, date: str, stocks: List[Tuple[str, str]], batch_size: int = 200) -> Dict:
        """填充指定日期的舆情数据"""
        logger.info(f"\n📅 处理日期: {date}")
        
        # 获取已有数据的股票
        existing = self.get_existing_sentiment(date)
        if existing:
            logger.info(f"  ⏭️ 已有 {len(existing)} 只股票的数据")
        
        date_stats = {'processed': 0, 'created': 0, 'skipped': 0, 'failed': 0}
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            
            for code, name in batch:
                # 检查是否已有数据
                if code in existing:
                    date_stats['skipped'] += 1
                    continue
                
                # 获取涨跌幅
                price_change = self.get_price_change(code, date)
                
                # 生成舆情数据
                sentiment_data = self.generate_sentiment(code, name, date, price_change)
                
                # 保存
                if self.save_sentiment(sentiment_data):
                    date_stats['created'] += 1
                else:
                    date_stats['failed'] += 1
                
                date_stats['processed'] += 1
            
            # 进度显示
            progress = min(100, (i + len(batch)) / len(stocks) * 100)
            if (i // batch_size + 1) % 5 == 0:
                logger.info(f"  ⏱️ 进度: {progress:.1f}% | 已处理 {date_stats['processed']}")
        
        logger.info(f"  ✅ 完成: 创建{date_stats['created']} | 跳过{date_stats['skipped']} | 失败{date_stats['failed']}")
        return date_stats
    
    def run(self, start_date: str = None, end_date: str = None, batch_size: int = 200):
        """执行历史数据填充"""
        start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("🚀 历史舆情数据填充")
        logger.info("=" * 60)
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 确定日期范围
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"📅 日期范围: {start_date} ~ {end_date}")
        
        # 3. 获取交易日列表
        trading_dates = self.get_trading_dates(start_date, end_date)
        if not trading_dates:
            logger.error("❌ 未获取到交易日")
            return False
        
        # 4. 获取股票列表
        stocks = self.get_all_stocks()
        if not stocks:
            logger.error("❌ 未获取到股票列表")
            return False
        
        self.stats['total_stocks'] = len(stocks)
        logger.info(f"📊 计划处理: {len(trading_dates)} 个交易日 × {len(stocks)} 只股票 = {len(trading_dates) * len(stocks)} 条记录")
        
        # 5. 逐日填充
        total_created = 0
        for date in trading_dates:
            date_stats = self.fill_date(date, stocks, batch_size)
            total_created += date_stats['created']
            self.stats['processed'] += date_stats['processed']
            self.stats['skipped'] += date_stats['skipped']
            self.stats['failed'] += date_stats['failed']
        
        # 6. 完成统计
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ 历史舆情数据填充完成!")
        logger.info("=" * 60)
        logger.info(f"📊 统计:")
        logger.info(f"  • 交易日: {len(trading_dates)} 天")
        logger.info(f"  • 股票数: {self.stats['total_stocks']} 只")
        logger.info(f"  • 处理记录: {self.stats['processed']}")
        logger.info(f"  • 新增数据: {total_created} 条")
        logger.info(f"  • 跳过(已有): {self.stats['skipped']} 条")
        logger.info(f"  • 失败: {self.stats['failed']} 条")
        logger.info(f"  • 总耗时: {duration} 秒")
        logger.info("=" * 60)
        
        # 7. 记录日志
        self.log_update(
            end_date, 
            self.stats['total_stocks'] * len(trading_dates),
            total_created,
            self.stats['failed'],
            self.stats['skipped'],
            duration
        )
        
        return True
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='历史舆情数据填充')
    parser.add_argument('--start', type=str, default=None,
                       help='开始日期 (格式: YYYY-MM-DD)，默认30天前')
    parser.add_argument('--end', type=str, default=None,
                       help='结束日期 (格式: YYYY-MM-DD)，默认今天')
    parser.add_argument('--batch-size', type=int, default=200,
                       help='每批处理的股票数量（默认200）')
    
    args = parser.parse_args()
    
    filler = SentimentHistoryFiller()
    try:
        success = filler.run(
            start_date=args.start,
            end_date=args.end,
            batch_size=args.batch_size
        )
        sys.exit(0 if success else 1)
    finally:
        filler.close()


if __name__ == "__main__":
    import json
    main()
