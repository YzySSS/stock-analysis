#!/usr/bin/env python3
"""
更新股票市值数据 - 使用BaoStock
"""
import baostock as bs
import pymysql
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}

def update_market_cap():
    # 登录BaoStock
    lg = bs.login()
    logger.info(f'BaoStock登录: {lg.error_msg}')
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 获取最新交易日
        cursor.execute('SELECT MAX(trade_date) FROM stock_kline')
        latest_date = cursor.fetchone()[0]
        logger.info(f'最新交易日: {latest_date}')
        
        # 获取股票列表
        cursor.execute('SELECT code, name FROM stock_basic WHERE is_delisted=0 AND is_st=0 LIMIT 100')
        stocks = cursor.fetchall()
        
        updated = 0
        for code, name in stocks:
            # 获取证券资料
            rs = bs.query_stock_basic(code=code)
            if rs.error_code == '0':
                data = rs.get_row_data()
                if len(data) > 15:
                    # 总股本通常在特定字段
                    total_shares = data[15] if len(data) > 15 else None
                    logger.info(f'{code} {name}: 总股本={total_shares}')
                    
                    if total_shares:
                        # 获取当日收盘价
                        cursor.execute('SELECT close FROM stock_kline WHERE code=%s AND trade_date=%s', (code, latest_date))
                        row = cursor.fetchone()
                        if row and row[0]:
                            close_price = float(row[0])
                            market_cap = close_price * float(total_shares) / 100000000  # 亿元
                            
                            cursor.execute('UPDATE stock_kline SET market_cap=%s WHERE code=%s AND trade_date=%s',
                                         (market_cap, code, latest_date))
                            updated += 1
                            
                            if updated % 10 == 0:
                                conn.commit()
                                logger.info(f'已更新 {updated} 只')
        
        conn.commit()
        logger.info(f'✅ 完成! 更新 {updated} 只股票市值')
        
    finally:
        bs.logout()
        conn.close()

if __name__ == '__main__':
    update_market_cap()
