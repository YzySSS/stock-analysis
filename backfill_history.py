#!/usr/bin/env python3
"""
历史数据回填脚本 - 简化版
==========================

分阶段回填2024-2025年数据:
- 阶段1: 2025-06-01 至 2025-12-22 (~130交易日)
- 阶段2: 2024-06-01 至 2025-05-31 (~250交易日)  
- 阶段3: 2024-01-01 至 2024-05-31 (~110交易日)

用法:
  python3 backfill_history.py --phase 1  # 回填阶段1
  python3 backfill_history.py --phase 2  # 回填阶段2
  python3 backfill_history.py --phase 3  # 回填阶段3
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import pymysql
import baostock as bs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306,
    'user': 'openclaw_user', 'password': 'open@2026',
    'database': 'stock', 'charset': 'utf8mb4'
}

# 分阶段配置
PHASES = {
    1: ('2025-06-01', '2025-12-22'),
    2: ('2024-06-01', '2025-05-31'),
    3: ('2024-01-01', '2024-05-31')
}

def backfill_phase(phase_num):
    start_date, end_date = PHASES[phase_num]
    logger.info(f"=" * 60)
    logger.info(f"🚀 开始回填阶段{phase_num}: {start_date} 至 {end_date}")
    logger.info(f"=" * 60)
    
    # 连接数据库
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 登录BaoStock
    bs.login()
    
    try:
        # 获取股票列表
        cursor.execute("SELECT code, market FROM stock_basic WHERE is_delisted=0 AND is_etf=0")
        stocks = cursor.fetchall()
        logger.info(f"📊 共 {len(stocks)} 只股票需要回填")
        
        total_inserted = 0
        batch_size = 100
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            logger.info(f"📦 处理批次 {i//batch_size + 1}/{(len(stocks)+batch_size-1)//batch_size}")
            
            for code, market in batch:
                bs_code = f"{market}.{code}"
                
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount,turn,pctChg",
                        start_date=start_date, end_date=end_date,
                        frequency="d", adjustflag="3"
                    )
                    
                    if rs.error_code != '0':
                        continue
                    
                    records = []
                    while rs.next():
                        row = rs.get_row_data()
                        if not row[1]:
                            continue
                        records.append((
                            code, row[0], float(row[1]), float(row[2]),
                            float(row[3]), float(row[4]), int(float(row[5])),
                            float(row[6]), float(row[7]), float(row[8])
                        ))
                    
                    if records:
                        cursor.executemany('''
                            INSERT INTO stock_kline (code,trade_date,open,high,low,close,volume,amount,turnover,pct_change)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                            open=VALUES(open),high=VALUES(high),low=VALUES(low),close=VALUES(close),
                            volume=VALUES(volume),amount=VALUES(amount),turnover=VALUES(turnover),pct_change=VALUES(pct_change)
                        ''', records)
                        conn.commit()
                        total_inserted += len(records)
                        
                except Exception as e:
                    logger.debug(f"⚠️ {code} 失败: {e}")
                    continue
            
            logger.info(f"✅ 已插入 {total_inserted:,} 条记录")
        
        logger.info(f"🎉 阶段{phase_num}完成! 共插入 {total_inserted:,} 条记录")
        
    finally:
        bs.logout()
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', type=int, required=True, choices=[1,2,3])
    args = parser.parse_args()
    backfill_phase(args.phase)
