#!/usr/bin/env python3
"""
更新股票市值数据 - 使用东方财富接口
"""

import pymysql
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
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


def get_market_cap_eastmoney(code):
    """从东方财富获取市值（亿元）"""
    try:
        secid = f"1.{code}" if code.startswith('6') else f"0.{code}"
        url = f'https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f57,f58,f116'
        resp = requests.get(url, timeout=5)
        data = resp.json()
        
        # f116 是总市值（元）
        market_cap = data.get('data', {}).get('f116')
        if market_cap:
            return float(market_cap) / 100000000  # 转亿元
        return None
    except Exception as e:
        return None


def update_market_cap():
    """更新所有股票的市值数据"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 获取最新交易日
        cursor.execute('SELECT MAX(trade_date) FROM stock_kline')
        latest_date = cursor.fetchone()[0]
        logger.info(f'最新交易日: {latest_date}')
        
        # 获取所有需要更新市值的股票
        cursor.execute('''
            SELECT DISTINCT k.code 
            FROM stock_kline k
            WHERE k.trade_date = %s
            AND (k.market_cap IS NULL OR k.market_cap = 0)
            AND k.close > 0
        ''', (latest_date,))
        
        codes = [row[0] for row in cursor.fetchall()]
        logger.info(f'需要更新市值的股票: {len(codes)} 只')
        
        if not codes:
            logger.info('所有股票已有市值数据')
            return
        
        # 批量获取市值
        updated = 0
        failed = 0
        batch_size = 100
        
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i+batch_size]
            logger.info(f'处理批次 {i//batch_size + 1}/{(len(codes)-1)//batch_size + 1}: {len(batch)} 只')
            
            for code in batch:
                market_cap = get_market_cap_eastmoney(code)
                
                if market_cap:
                    cursor.execute('''
                        UPDATE stock_kline 
                        SET market_cap = %s 
                        WHERE code = %s AND trade_date = %s
                    ''', (market_cap, code, latest_date))
                    if cursor.rowcount > 0:
                        updated += 1
                else:
                    failed += 1
                
                time.sleep(0.05)  # 限速
            
            conn.commit()
            logger.info(f'  已更新: {updated}, 失败: {failed}')
        
        logger.info(f'✅ 完成! 总更新: {updated}, 失败: {failed}')
        
        # 显示统计
        cursor.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN market_cap IS NOT NULL AND market_cap > 0 THEN 1 ELSE 0 END) as has_cap,
                   AVG(market_cap) as avg_cap
            FROM stock_kline 
            WHERE trade_date = %s
        ''', (latest_date,))
        
        total, has_cap, avg_cap = cursor.fetchone()
        logger.info(f'市值统计: 总数={total}, 有数据={has_cap}, 覆盖率={has_cap/total*100:.1f}%, 平均={avg_cap:.2f}亿')
        
    finally:
        conn.close()


if __name__ == '__main__':
    update_market_cap()
