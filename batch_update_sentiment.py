#!/usr/bin/env python3
"""
全A股舆情数据分批次更新
======================
分批次更新，避免资源占用过高

用法:
  python3 batch_update_sentiment.py --batch 100 --start 0    # 更新前100只
  python3 batch_update_sentiment.py --batch 100 --start 100  # 更新101-200只
  python3 batch_update_sentiment.py --batch 500 --start 0    # 更新前500只
  
或者使用循环脚本批量执行:
  for i in 0 500 1000 1500 2000 2500 3000 3500 4000 4500 5000; do
    python3 batch_update_sentiment.py --batch 500 --start $i
  done
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

# 全A股列表文件
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

def get_existing_count(date: str) -> int:
    """获取已存在的记录数"""
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'sentiment_cache.db')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache WHERE date=?", (date,))
            return cursor.fetchone()[0]
    except:
        return 0

def batch_update(batch_size: int = 500, start_index: int = 0):
    """
    分批次更新舆情数据
    
    Args:
        batch_size: 每批处理数量
        start_index: 开始索引
    """
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 加载全A股
    all_stocks = load_all_stocks()
    total = len(all_stocks)
    
    if not all_stocks:
        logger.error("无法加载股票列表")
        return
    
    # 获取本批次
    end_index = min(start_index + batch_size, total)
    batch_stocks = all_stocks[start_index:end_index]
    
    logger.info("="*60)
    logger.info(f"批次更新: {start_index+1}-{end_index} / {total}")
    logger.info("="*60)
    
    updated = 0
    skipped = 0
    failed = 0
    start_time = time.time()
    
    for i, (code, name) in enumerate(batch_stocks):
        actual_index = start_index + i + 1
        
        # 每10只输出一次进度
        if (i + 1) % 10 == 0:
            logger.info(f"进度: {actual_index}/{total} ({(actual_index/total*100):.1f}%)")
        
        try:
            # 检查是否已存在
            existing = calc.get_cached_sentiment(code, today)
            if existing:
                skipped += 1
                continue
            
            # 计算舆情因子
            result = calc.calculate_sentiment_factor(code, name, today)
            updated += 1
            
            # 每50只休息1秒，避免请求过快
            if (i + 1) % 50 == 0:
                time.sleep(1)
                
        except Exception as e:
            logger.warning(f"{code} 更新失败: {e}")
            failed += 1
            time.sleep(0.5)  # 出错后短暂休息
    
    elapsed = time.time() - start_time
    
    # 统计当前总数
    current_total = get_existing_count(today)
    
    logger.info("="*60)
    logger.info(f"✅ 批次完成: 成功{updated}, 跳过{skipped}, 失败{failed}")
    logger.info(f"⏱️ 耗时: {elapsed:.1f}秒, 当前总数: {current_total}/{total}")
    logger.info("="*60)
    
    return updated, skipped, failed

def main():
    parser = argparse.ArgumentParser(description='分批次更新舆情数据')
    parser.add_argument('--batch', type=int, default=500, help='每批处理数量')
    parser.add_argument('--start', type=int, default=0, help='开始索引')
    
    args = parser.parse_args()
    
    batch_update(batch_size=args.batch, start_index=args.start)

if __name__ == "__main__":
    main()
