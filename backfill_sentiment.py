#!/usr/bin/env python3
"""
历史舆情数据回溯更新
=====================
补充昨天和前天的舆情数据，建立3天数据基础

用法:
  python3 backfill_sentiment.py --date 2026-03-24  # 补充3月24日数据
  python3 backfill_sentiment.py --date 2026-03-23  # 补充3月23日数据
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
import time
import sqlite3
from datetime import datetime, timedelta

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

def get_existing_count(target_date: str) -> int:
    """获取指定日期已存在的记录数"""
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'sentiment_cache.db')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache WHERE date=?", (target_date,))
            return cursor.fetchone()[0]
    except:
        return 0

def backfill_sentiment(target_date: str, batch_size: int = 500):
    """
    回溯更新指定日期的舆情数据
    
    Args:
        target_date: 目标日期 (YYYY-MM-DD)
        batch_size: 每批处理数量
    """
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    
    # 检查已存在的数量
    existing_count = get_existing_count(target_date)
    logger.info(f"📊 {target_date} 已有数据: {existing_count}只")
    
    # 加载全A股
    all_stocks = load_all_stocks()
    total = len(all_stocks)
    
    if not all_stocks:
        logger.error("无法加载股票列表")
        return
    
    logger.info("="*60)
    logger.info(f"回溯更新: {target_date}")
    logger.info(f"目标: {total}只股票")
    logger.info("="*60)
    
    updated = 0
    skipped = 0
    failed = 0
    start_time = time.time()
    
    for i in range(0, total, batch_size):
        batch = all_stocks[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total - 1) // batch_size + 1
        
        logger.info(f"\n批次 {batch_num}/{total_batches}: 处理 {len(batch)} 只股票")
        
        for j, (code, name) in enumerate(batch):
            actual_index = i + j + 1
            
            if (j + 1) % 10 == 0:
                logger.info(f"  进度: {actual_index}/{total} ({actual_index/total*100:.1f}%)")
            
            try:
                # 检查是否已存在
                existing = calc.get_cached_sentiment(code, target_date)
                if existing:
                    skipped += 1
                    continue
                
                # 计算舆情因子（使用历史日期）
                result = calc.calculate_sentiment_factor(code, name, target_date)
                updated += 1
                
            except Exception as e:
                logger.warning(f"{code} 更新失败: {e}")
                failed += 1
            
            # 每50只休息1秒
            if (j + 1) % 50 == 0:
                time.sleep(1)
        
        # 批次间休息
        if i + batch_size < total:
            time.sleep(2)
    
    elapsed = time.time() - start_time
    final_count = get_existing_count(target_date)
    
    logger.info("="*60)
    logger.info(f"✅ 回溯完成: {target_date}")
    logger.info(f"   成功: {updated}, 跳过: {skipped}, 失败: {failed}")
    logger.info(f"   总记录: {final_count}/{total}")
    logger.info(f"   耗时: {elapsed:.1f}秒")
    logger.info("="*60)

def main():
    parser = argparse.ArgumentParser(description='回溯更新历史舆情数据')
    parser.add_argument('--date', type=str, required=True, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--batch', type=int, default=500, help='每批处理数量')
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        logger.error("日期格式错误，请使用 YYYY-MM-DD 格式")
        return
    
    backfill_sentiment(target_date=args.date, batch_size=args.batch)

if __name__ == "__main__":
    main()
