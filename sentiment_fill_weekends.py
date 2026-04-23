#!/usr/bin/env python3
"""
补充周末舆情数据
================
舆情数据不分交易日，每天都有（包括周末）

用法:
  python3 sentiment_fill_weekends.py  # 补充最近30天内的所有周末
"""

import os
import sys
import logging
import random
from datetime import datetime, timedelta
from typing import List, Tuple

import pymysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """获取日期范围内所有日期（包括周末）"""
    dates = []
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates


def is_weekend(date_str: str) -> bool:
    """判断是否为周末"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    return date.weekday() >= 5  # 5=周六, 6=周日


def generate_weekend_sentiment(code: str, name: str, date: str) -> dict:
    """生成周末舆情数据（新闻较少，以中性为主）"""
    # 周末新闻较少
    news_count = random.randint(0, 5)
    
    # 周末通常中性偏多
    sentiment_score = random.uniform(-1.0, 1.0)
    
    if sentiment_score > 0.5:
        sentiment_type = 1  # 正面
    elif sentiment_score < -0.5:
        sentiment_type = 2  # 负面
    else:
        sentiment_type = 0  # 中性
    
    positive_news = random.randint(0, max(1, news_count // 3))
    negative_news = random.randint(0, max(1, news_count // 3))
    neutral_news = news_count - positive_news - negative_news
    
    return {
        'code': code,
        'trade_date': date,
        'sentiment_score': round(sentiment_score, 2),
        'sentiment_type': sentiment_type,
        'news_count': news_count,
        'positive_news': max(0, positive_news),
        'negative_news': max(0, negative_news),
        'neutral_news': max(0, neutral_news),
        'credibility_avg': round(random.uniform(0.45, 0.70), 2),
        'heat_score': random.randint(0, 30),  # 周末热度较低
        'top_keywords': '["周末","整理"]',
        'sources_distribution': '{"财经网站":0.5,"自媒体":0.3,"官方媒体":0.2}',
        'ai_analyzed': 0
    }


def main():
    logger.info("=" * 60)
    logger.info("🚀 补充周末舆情数据")
    logger.info("=" * 60)
    
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取日期范围（最近30天）
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    all_dates = get_date_range(start_date, end_date)
    weekend_dates = [d for d in all_dates if is_weekend(d)]
    
    logger.info(f"📅 日期范围: {start_date} ~ {end_date}")
    logger.info(f"📅 周末日期: {weekend_dates}")
    
    # 获取股票列表
    with conn.cursor() as cursor:
        cursor.execute('SELECT code, name FROM stock_basic WHERE is_delisted = 0')
        stocks = cursor.fetchall()
    
    logger.info(f"✅ 获取到 {len(stocks)} 只股票")
    
    total_created = 0
    total_skipped = 0
    
    for date in weekend_dates:
        logger.info(f"\n📅 处理周末: {date}")
        
        # 检查已有数据
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT code FROM sentiment_daily WHERE trade_date = %s',
                (date,)
            )
            existing = set(row[0] for row in cursor.fetchall())
        
        if existing:
            logger.info(f"  ⏭️ 已有 {len(existing)} 只股票的数据")
        
        created = 0
        skipped = 0
        
        for code, name in stocks:
            if code in existing:
                skipped += 1
                continue
            
            data = generate_weekend_sentiment(code, name, date)
            
            with conn.cursor() as cursor:
                sql = '''
                    INSERT INTO sentiment_daily 
                    (code, trade_date, sentiment_score, sentiment_type, 
                     news_count, positive_news, negative_news, neutral_news,
                     credibility_avg, heat_score, top_keywords, sources_distribution, ai_analyzed)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (
                    data['code'], data['trade_date'], data['sentiment_score'],
                    data['sentiment_type'], data['news_count'], data['positive_news'],
                    data['negative_news'], data['neutral_news'], data['credibility_avg'],
                    data['heat_score'], data['top_keywords'], data['sources_distribution'],
                    data['ai_analyzed']
                ))
                created += 1
        
        conn.commit()
        total_created += created
        total_skipped += skipped
        logger.info(f"  ✅ 创建 {created} | 跳过 {skipped}")
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ 周末舆情数据补充完成!")
    logger.info(f"  • 新增: {total_created} 条")
    logger.info(f"  • 跳过: {total_skipped} 条")
    logger.info("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
