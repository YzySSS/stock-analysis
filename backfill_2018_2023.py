#!/usr/bin/env python3
"""
历史数据回填脚本 - 2018-2023年
===========================
后台运行，补充历史数据用于完整回测验证
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import baostock as bs
import pymysql
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/.openclaw/workspace/股票分析项目/logs/backfill_2018_2023.log'),
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
    """获取所有股票代码（排除已回填的）"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 获取已回填的股票（有2018年数据的视为已回填）
        cursor.execute("""
            SELECT DISTINCT code FROM stock_kline 
            WHERE trade_date BETWEEN '2018-01-01' AND '2018-12-31'
        """)
        completed = {row[0] for row in cursor.fetchall()}
        
        # 获取待回填的股票
        cursor.execute("""
            SELECT code FROM stock_basic 
            WHERE is_delisted = 0 
            AND is_etf = 0
            AND code NOT LIKE '8%'
            AND code NOT LIKE '4%'
            AND code NOT LIKE '83%'
        """)
        all_codes = [row[0] for row in cursor.fetchall()]
        
        # 排除已完成的
        pending = [c for c in all_codes if c not in completed]
        
        logger.info(f"总股票: {len(all_codes)} | 已完成: {len(completed)} | 待回填: {len(pending)}")
        return pending
    finally:
        conn.close()


def get_existing_dates(code):
    """获取股票已存在的数据日期"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT trade_date FROM stock_kline 
            WHERE code = %s AND trade_date BETWEEN '2018-01-01' AND '2023-12-31'
        """, (code,))
        existing = {row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()}
        return existing
    except:
        return set()
    finally:
        conn.close()


def fetch_and_insert_stock_data(code, start_date='2018-01-01', end_date='2023-12-31'):
    """获取并插入单只股票的历史数据"""
    try:
        # 检查已存在的数据
        existing_dates = get_existing_dates(code)
        
        # 转换股票代码为BaoStock格式
        if code.startswith('6'):
            bs_code = f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            bs_code = f"sz.{code}"
        else:
            logger.warning(f"{code}: 未知代码格式，跳过")
            return 0
        
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
            logger.error(f"{code}: 获取数据失败 - {rs.error_msg}")
            return 0
        
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            trade_date = row[0]
            
            # 跳过已存在的数据
            if trade_date in existing_dates:
                continue
            
            try:
                # 计算涨跌幅
                if row[10]:  # pctChg
                    pct_change = float(row[10])
                elif row[5] and row[6]:  # close and preclose
                    pct_change = (float(row[5]) - float(row[6])) / float(row[6]) * 100
                else:
                    pct_change = None
                
                # 计算振幅
                if row[2] and row[3] and row[6]:  # high, low, preclose
                    amplitude = (float(row[2]) - float(row[3])) / float(row[6]) * 100
                else:
                    amplitude = None
                
                data_list.append((
                    code,
                    trade_date,
                    float(row[2]) if row[2] else None,   # open
                    float(row[3]) if row[3] else None,   # high
                    float(row[4]) if row[4] else None,   # low
                    float(row[5]) if row[5] else None,   # close
                    int(float(row[7])) if row[7] else None,  # volume
                    float(row[8]) if row[8] else None,   # amount
                    amplitude,
                    pct_change,
                    float(row[5]) - float(row[6]) if row[5] and row[6] else None,  # change_amount
                    float(row[9]) if row[9] else None    # turnover
                ))
            except Exception as e:
                logger.warning(f"{code} {trade_date}: 解析数据失败 - {e}")
                continue
        
        if not data_list:
            return 0
        
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
            return len(data_list)
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"{code}: 处理失败 - {e}")
        return 0


def backfill_all_stocks():
    """回填所有股票的历史数据"""
    logger.info("=" * 70)
    logger.info("开始回填2018-2023年历史数据")
    logger.info("=" * 70)
    
    # 登录BaoStock
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"BaoStock登录失败: {lg.error_msg}")
        return
    
    try:
        codes = get_stock_list()
        total_codes = len(codes)
        
        logger.info(f"总股票数: {total_codes}")
        logger.info(f"时间范围: 2018-01-01 至 2023-12-31")
        logger.info(f"预计交易日: 约1200天")
        
        total_inserted = 0
        failed_codes = []
        
        # 分批处理
        batch_size = 50
        for i in range(0, total_codes, batch_size):
            batch = codes[i:i+batch_size]
            logger.info(f"\n处理批次 {i//batch_size + 1}/{(total_codes-1)//batch_size + 1}: {len(batch)} 只")
            
            for code in batch:
                try:
                    inserted = fetch_and_insert_stock_data(code)
                    total_inserted += inserted
                    
                    if inserted > 0:
                        logger.info(f"  ✅ {code}: 插入 {inserted} 条")
                    else:
                        logger.info(f"  ⏭️ {code}: 无新数据")
                    
                    # 限速
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"  ❌ {code}: 失败 - {e}")
                    failed_codes.append(code)
            
            # 每批暂停
            time.sleep(1)
            
            # 进度报告
            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"\n📊 进度: {i+len(batch)}/{total_codes} | 累计插入: {total_inserted} 条")
        
        logger.info("\n" + "=" * 70)
        logger.info("回填完成!")
        logger.info(f"总插入: {total_inserted} 条")
        logger.info(f"失败: {len(failed_codes)} 只")
        if failed_codes:
            logger.info(f"失败股票: {', '.join(failed_codes[:10])}...")
        logger.info("=" * 70)
        
        # 更新统计
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                MIN(trade_date) as min_date,
                MAX(trade_date) as max_date,
                COUNT(DISTINCT trade_date) as days,
                COUNT(*) as total_records
            FROM stock_kline
        """)
        
        row = cursor.fetchone()
        logger.info(f"\n📊 数据库统计:")
        logger.info(f"  最小日期: {row[0]}")
        logger.info(f"  最大日期: {row[1]}")
        logger.info(f"  交易日数: {row[2]}")
        logger.info(f"  总记录数: {row[3]}")
        
        conn.close()
        
    finally:
        bs.logout()


def verify_data_completeness():
    """验证数据完整性"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    logger.info("\n📋 数据完整性检查:")
    
    # 检查2018-2023年数据
    for year in range(2018, 2024):
        cursor.execute("""
            SELECT COUNT(DISTINCT trade_date), COUNT(*)
            FROM stock_kline
            WHERE trade_date BETWEEN %s AND %s
        """, (f"{year}-01-01", f"{year}-12-31"))
        
        days, records = cursor.fetchone()
        logger.info(f"  {year}年: {days} 交易日, {records} 条记录")
    
    conn.close()


if __name__ == '__main__':
    # 创建日志目录
    os.makedirs('/root/.openclaw/workspace/股票分析项目/logs', exist_ok=True)
    
    # 执行回填
    backfill_all_stocks()
    
    # 验证数据
    verify_data_completeness()
    
    logger.info("\n✅ 所有任务完成!")
