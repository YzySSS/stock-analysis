#!/usr/bin/env python3
"""
智能历史数据补充脚本 (带报告输出)
================================
每天晚上20:00运行，检查当天是否为交易日，补充缺失的历史数据
执行完成后输出结构化报告
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import sqlite3
import baostock as bs
import time
import json
from datetime import datetime, timedelta

def is_trading_day(date_str: str) -> bool:
    """检查指定日期是否为交易日（简单判断：周一到周五）"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    return date.weekday() < 5

def get_missing_dates(db_path: str, all_stocks: list) -> dict:
    """获取各日期缺失数据情况"""
    conn = sqlite3.connect(db_path)
    
    # 检查最近5天
    dates_to_check = []
    for i in range(7):
        d = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        if is_trading_day(d):
            dates_to_check.append(d)
        if len(dates_to_check) >= 3:
            break
    
    missing_info = {}
    for date in dates_to_check:
        cursor = conn.execute('SELECT COUNT(*) FROM stock_prices WHERE date=?', (date,))
        count = cursor.fetchone()[0]
        missing = len(all_stocks) - count
        if missing > 0:
            missing_info[date] = {'count': count, 'missing': missing, 'total': len(all_stocks)}
    
    conn.close()
    return missing_info

def update_date_data(target_date: str, db_path: str, all_stocks: list) -> int:
    """补充指定日期的数据"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute('SELECT code FROM stock_prices WHERE date=?', (target_date,))
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    to_update = [s for s in all_stocks if s not in existing]
    if not to_update:
        return 0
    
    batch_size = 400
    total_updated = 0
    
    for i in range(0, len(to_update), batch_size):
        batch = to_update[i:i+batch_size]
        
        result = bs.login()
        if result.error_code != '0':
            continue
        
        conn = sqlite3.connect(db_path)
        batch_updated = 0
        
        for code in batch:
            try:
                if code.startswith(('00', '30', '15', '16', '18')):
                    bs_code = f'sz.{code}'
                else:
                    bs_code = f'sh.{code}'
                
                rs = bs.query_history_k_data_plus(
                    bs_code, 'date,close,volume',
                    start_date=target_date, end_date=target_date,
                    frequency='d', adjustflag='2'
                )
                
                if rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    if row[1]:
                        conn.execute('''
                            INSERT OR REPLACE INTO stock_prices (code, date, close_price, volume)
                            VALUES (?, ?, ?, ?)
                        ''', (code, row[0], float(row[1]), int(row[2]) if row[2] else 0))
                        batch_updated += 1
            except:
                pass
        
        conn.commit()
        conn.close()
        bs.logout()
        total_updated += batch_updated
        time.sleep(0.3)
    
    return total_updated

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    report = {
        'date': today,
        'is_trading_day': is_trading_day(today),
        'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updates': [],
        'total_added': 0,
        'status': 'success'
    }
    
    # 检查今天是否为交易日
    if not report['is_trading_day']:
        report['status'] = 'skipped'
        report['message'] = f'今天 ({today}) 是周末/节假日，无需补充数据'
        print(json.dumps(report, ensure_ascii=False))
        return
    
    # 加载股票列表
    db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'stock_history.db')
    all_a_stocks_file = os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt")
    
    if os.path.exists(all_a_stocks_file):
        with open(all_a_stocks_file, 'r') as f:
            all_stocks = [line.strip() for line in f if line.strip()]
    else:
        alt_file = os.path.join(os.path.dirname(__file__), 'data', 'all_a_stocks.txt')
        with open(alt_file, 'r') as f:
            all_stocks = [line.strip() for line in f if line.strip()]
    
    report['total_stocks'] = len(all_stocks)
    
    # 获取缺失数据情况
    missing_info = get_missing_dates(db_path, all_stocks)
    
    if not missing_info:
        report['status'] = 'no_action'
        report['message'] = '所有历史数据已完整，无需补充'
        report['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(json.dumps(report, ensure_ascii=False))
        return
    
    # 开始补充
    for date in sorted(missing_info.keys()):
        info = missing_info[date]
        added = update_date_data(date, db_path, all_stocks)
        report['total_added'] += added
        
        # 检查补充后状态
        conn = sqlite3.connect(db_path)
        cursor = conn.execute('SELECT COUNT(*) FROM stock_prices WHERE date=?', (date,))
        new_count = cursor.fetchone()[0]
        conn.close()
        
        report['updates'].append({
            'date': date,
            'before': info['count'],
            'after': new_count,
            'added': added,
            'progress': round(new_count / len(all_stocks) * 100, 1)
        })
    
    # 最终统计
    conn = sqlite3.connect(db_path)
    cursor = conn.execute('SELECT COUNT(DISTINCT code), COUNT(*), MAX(date) FROM stock_prices')
    stock_count, record_count, max_date = cursor.fetchone()
    conn.close()
    
    report['final_stats'] = {
        'stock_count': stock_count,
        'record_count': record_count,
        'max_date': max_date
    }
    report['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    report['message'] = f"数据补充完成，共新增 {report['total_added']} 条记录"
    
    print(json.dumps(report, ensure_ascii=False))

if __name__ == "__main__":
    main()
