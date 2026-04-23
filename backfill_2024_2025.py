#!/usr/bin/env python3
"""
历史数据回填脚本 - 2024-2025年（V12回测专用）
===========================
补充2024-2025年历史数据用于完整回测验证
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import baostock as bs
import pymysql
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/.openclaw/workspace/股票分析项目/logs/backfill_2024_2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}


def get_stock_list():
    """获取需要回填2024-2025数据的所有股票"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 获取所有有效股票
        cursor.execute("""
            SELECT code FROM stock_basic 
            WHERE is_delisted = 0 
            AND is_etf = 0
            AND code NOT LIKE '8%'
            AND code NOT LIKE '4%'
            AND code NOT LIKE '83%'
            ORDER BY code
        """)
        all_codes = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"总股票: {len(all_codes)}只")
        return all_codes
    finally:
        conn.close()


def get_stock_data_count(code, year):
    """获取某股票某年的数据条数"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM stock_kline 
            WHERE code = %s AND YEAR(trade_date) = %s
        """, (code, year))
        return cursor.fetchone()[0]
    except:
        return 0
    finally:
        conn.close()


def fetch_and_insert_stock_data(code, start_date='2024-01-01', end_date='2025-12-31'):
    """获取并插入单只股票的历史数据"""
    try:
        # 检查2024和2025年的数据量
        count_2024 = get_stock_data_count(code, 2024)
        count_2025 = get_stock_data_count(code, 2025)
        
        # 如果都超过200条（约一年交易日），视为已完成
        if count_2024 >= 200 and count_2025 >= 200:
            return 0, "跳过（已有数据）"
        
        # 转换股票代码为BaoStock格式
        if code.startswith('6'):
            bs_code = f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            bs_code = f"sz.{code}"
        else:
            return 0, f"未知代码格式"
        
        # 获取BaoStock数据
        fields = "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg"
        rs = bs.query_history_k_data_plus(
            bs_code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"  # 前复权
        )
        
        if rs.error_code != '0':
            return 0, f"获取数据失败: {rs.error_msg}"
        
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            trade_date = row[0]
            
            # 只处理2024和2025年的数据
            year = int(trade_date[:4])
            if year not in [2024, 2025]:
                continue
            
            try:
                # 安全转换数值
                open_price = float(row[2]) if row[2] else None
                high_price = float(row[3]) if row[3] else None
                low_price = float(row[4]) if row[4] else None
                close_price = float(row[5]) if row[5] else None
                preclose = float(row[6]) if row[6] else None
                volume = int(float(row[7])) if row[7] else None
                amount = float(row[8]) if row[8] else None
                turnover = float(row[9]) if row[9] else None
                
                # 计算涨跌幅（限制范围防止溢出）
                if row[10]:
                    pct_change = float(row[10])
                    pct_change = max(-99.9, min(999.9, pct_change))  # 限制范围
                elif close_price and preclose and preclose != 0:
                    pct_change = (close_price - preclose) / preclose * 100
                    pct_change = max(-99.9, min(999.9, pct_change))
                else:
                    pct_change = None
                
                # 计算振幅
                if high_price and low_price and preclose and preclose != 0:
                    amplitude = (high_price - low_price) / preclose * 100
                    amplitude = max(-99.9, min(999.9, amplitude))
                else:
                    amplitude = None
                
                data_list.append((
                    code, trade_date, open_price, high_price, low_price, close_price,
                    volume, amount, amplitude, pct_change,
                    close_price - preclose if close_price and preclose else None,
                    turnover
                ))
            except Exception as e:
                continue
        
        if not data_list:
            return 0, "无新数据"
        
        # 批量插入数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        try:
            cursor.executemany("""
                INSERT INTO stock_kline 
                (code, trade_date, open, high, low, close, volume, amount, amplitude, pct_change, change_amount, turnover)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
                volume=VALUES(volume), amount=VALUES(amount), amplitude=VALUES(amplitude),
                pct_change=VALUES(pct_change), change_amount=VALUES(change_amount), turnover=VALUES(turnover)
            """, data_list)
            
            conn.commit()
            return len(data_list), "成功"
        finally:
            conn.close()
            
    except Exception as e:
        return 0, f"处理失败: {e}"


def backfill_all_stocks():
    """回填所有股票的历史数据"""
    logger.info("=" * 70)
    logger.info("开始回填2024-2025年历史数据")
    logger.info("=" * 70)
    
    # 登录BaoStock
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"BaoStock登录失败: {lg.error_msg}")
        return
    
    try:
        stocks = get_stock_list()
        total = len(stocks)
        
        logger.info(f"总股票数: {total}")
        logger.info(f"时间范围: 2024-01-01 至 2025-12-31")
        logger.info(f"预计交易日: ~485天")
        logger.info("")
        
        # 分批处理
        batch_size = 50
        total_inserted = 0
        total_skipped = 0
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stocks) + batch_size - 1) // batch_size
            
            logger.info(f"处理批次 {batch_num}/{total_batches}: {len(batch)} 只")
            
            batch_inserted = 0
            batch_success = 0
            
            for code in batch:
                inserted, msg = fetch_and_insert_stock_data(code)
                
                if inserted > 0:
                    logger.info(f"  ✅ {code}: 插入 {inserted} 条 ({msg})")
                    batch_inserted += inserted
                    batch_success += 1
                    total_inserted += inserted
                else:
                    total_skipped += 1
                
                time.sleep(0.05)  # 避免请求过快
            
            logger.info(f"  批次完成: 成功 {batch_success} 只, 插入 {batch_inserted} 条")
            logger.info("")
            
            # 每10批次暂停一下
            if batch_num % 10 == 0:
                logger.info(f"进度: {min(i+batch_size, total)}/{total} ({min(i+batch_size, total)/total*100:.1f}%)")
                logger.info(f"累计插入: {total_inserted} 条")
                time.sleep(2)
        
        logger.info("=" * 70)
        logger.info(f"回填完成!")
        logger.info(f"总插入: {total_inserted} 条")
        logger.info(f"跳过: {total_skipped} 只")
        logger.info("=" * 70)
        
    finally:
        bs.logout()


if __name__ == "__main__":
    backfill_all_stocks()
