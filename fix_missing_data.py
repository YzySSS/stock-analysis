#!/usr/bin/env python3
"""
补充历史数据更新脚本
"""
import sys
sys.path.insert(0, 'src')
import baostock as bs
import sqlite3
import time

def update_missing_data(target_date):
    db_path = 'src/data_cache/stock_history.db'
    
    # 读取所有股票
    with open('data/all_a_stocks.txt', 'r') as f:
        all_stocks = [line.strip() for line in f if line.strip()]
    
    print(f'📅 目标日期: {target_date}')
    print(f'📊 全A股总数: {len(all_stocks)} 只')
    
    conn = sqlite3.connect(db_path)
    
    # 获取已有数据
    cursor = conn.execute('SELECT code FROM stock_prices WHERE date=?', (target_date,))
    existing = {row[0] for row in cursor.fetchall()}
    print(f'✅ 已有数据: {len(existing)} 只')
    
    # 需要补充的
    stocks_to_update = [s for s in all_stocks if s not in existing]
    print(f'🔄 需要补充: {len(stocks_to_update)} 只')
    
    conn.close()
    
    if not stocks_to_update:
        print('✅ 无需更新')
        return
    
    # 分批更新
    batch_size = 300
    total_updated = 0
    total_batches = (len(stocks_to_update) - 1) // batch_size + 1
    
    for i in range(0, len(stocks_to_update), batch_size):
        batch = stocks_to_update[i:i+batch_size]
        batch_num = i // batch_size + 1
        print(f'\n📦 批次 {batch_num}/{total_batches}: {len(batch)} 只')
        
        result = bs.login()
        if result.error_code != '0':
            print(f'❌ 登录失败: {result.error_msg}')
            continue
        
        batch_updated = 0
        conn = sqlite3.connect(db_path)
        
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
            except Exception as e:
                pass
            time.sleep(0.01)
        
        conn.commit()
        conn.close()
        bs.logout()
        
        total_updated += batch_updated
        print(f'   批次更新: {batch_updated} 只 | 累计: {total_updated} 只')
    
    print(f'\n✅ {target_date} 更新完成! 共 {total_updated} 只')

if __name__ == '__main__':
    # 先更新3月25日
    update_missing_data('2026-03-25')
    print('\n' + '='*50)
    # 再更新3月26日
    update_missing_data('2026-03-26')
