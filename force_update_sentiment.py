#!/usr/bin/env python3
"""
全A股舆情数据强制更新（无视缓存）
==========================
用于首次全量更新或数据重置

用法:
  python3 force_update_sentiment.py --batch 500 --start 0     # 更新前500只
  
或者后台执行全部:
  nohup python3 force_update_sentiment.py > /tmp/sentiment_force.log 2>&1 &
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
import time
import sqlite3
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ALL_A_STOCKS_FILE = os.path.join(os.path.dirname(__file__), 'data', 'all_a_stocks.txt')

def load_all_stocks() -> list:
    """加载全A股列表"""
    if os.path.exists(ALL_A_STOCKS_FILE):
        try:
            with open(ALL_A_STOCKS_FILE, 'r', encoding='utf-8') as f:
                codes = [line.strip() for line in f if line.strip()]
            return [(code, f"股票{code}") for code in codes]
        except Exception as e:
            logger.error(f"读取全A股列表失败: {e}")
    return []

def force_update_batch(batch_size: int = 500, start_index: int = 0):
    """强制更新（无视缓存）"""
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    # 使用今天的日期
    today = datetime.now().strftime('%Y-%m-%d')
    
    all_stocks = load_all_stocks()
    total = len(all_stocks)
    
    if not all_stocks:
        logger.error("无法加载股票列表")
        return
    
    end_index = min(start_index + batch_size, total)
    batch_stocks = all_stocks[start_index:end_index]
    
    logger.info("="*60)
    logger.info(f"强制更新批次: {start_index+1}-{end_index} / {total}")
    logger.info("="*60)
    
    updated = 0
    failed = 0
    start_time = time.time()
    
    for i, (code, name) in enumerate(batch_stocks):
        actual_index = start_index + i + 1
        
        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            speed = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(batch_stocks) - i - 1) / speed if speed > 0 else 0
            logger.info(f"进度: {actual_index}/{total} ({(actual_index/total*100):.1f}%) | 速度: {speed:.1f}只/秒 | 剩余: {remaining:.0f}秒")
        
        try:
            # 直接计算并保存（无视缓存）
            result = calc.calculate_sentiment_factor(code, name, today)
            
            # 如果已有数据，强制覆盖
            try:
                db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'sentiment_cache.db')
                with sqlite3.connect(db_path) as conn:
                    conn.execute('''
                        INSERT OR REPLACE INTO sentiment_cache 
                        (code, date, sentiment_score, news_count, credibility_avg, cached_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (code, today, result['raw_sentiment'], result['news_count'], result['credibility_avg']))
                    conn.commit()
                updated += 1
            except Exception as e:
                logger.warning(f"{code} 数据库写入失败: {e}")
                failed += 1
            
            # 每50只休息1秒
            if (i + 1) % 50 == 0:
                time.sleep(1)
                
        except Exception as e:
            logger.warning(f"{code} 更新失败: {e}")
            failed += 1
            time.sleep(0.5)
    
    elapsed = time.time() - start_time
    
    logger.info("="*60)
    logger.info(f"✅ 批次完成: 成功{updated}, 失败{failed}")
    logger.info(f"⏱️ 耗时: {elapsed:.1f}秒, 平均: {elapsed/len(batch_stocks):.2f}秒/只")
    logger.info("="*60)

def main():
    parser = argparse.ArgumentParser(description='强制更新舆情数据')
    parser.add_argument('--batch', type=int, default=500, help='每批处理数量')
    parser.add_argument('--start', type=int, default=0, help='开始索引')
    
    args = parser.parse_args()
    
    force_update_batch(batch_size=args.batch, start_index=args.start)

if __name__ == "__main__":
    main()
