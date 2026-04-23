#!/usr/bin/env python3
"""
V11_IC_Optimized 完整两年回测 - 优化版
========================================
"""

import pandas as pd
import numpy as np
import pymysql
import json
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

print("="*70)
print("V11_IC_Optimized 完整两年回测 (2024-2025)")
print("="*70)

# 1. 加载所有数据（优化：一次加载，但按code分组处理）
print("\n[1/3] 加载历史数据...")
conn = pymysql.connect(**DB_CONFIG)

# 获取股票列表
cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"  有效股票数: {len(stock_codes)}")

# 分批加载数据（每批100只）
all_results = []
batch_size = 100
total_batches = (len(stock_codes) + batch_size - 1) // batch_size

for batch_idx in range(total_batches):
    batch_codes = stock_codes[batch_idx * batch_size : (batch_idx + 1) * batch_size]
    codes_str = ','.join([f"'{c}'" for c in batch_codes])
    
    print(f"  处理批次 {batch_idx + 1}/{total_batches} ({len(batch_codes)}只股票)...")
    
    sql = f"""
    SELECT code, trade_date, close, turnover, amount
    FROM stock_kline 
    WHERE code IN ({codes_str})
    AND trade_date BETWEEN '2023-11-01' AND '2025-12-31'
    AND amount >= 500000
    ORDER BY code, trade_date
    """
    
    df = pd.read_sql(sql, conn)
    
    if len(df) == 0:
        continue
    
    for col in ['close', 'turnover', 'amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    # 过滤指数
    df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]
    
    # 获取2024-2025的交易日
    all_dates = df[df['trade_date'] >= '2024-01-01']['trade_date'].unique()
    selection_dates = sorted(all_dates)[::3]
    
    # 计算因子并回测
    for code in df['code'].unique():
        stock_df = df[df['code'] == code].sort_values('trade_date')
        
        for sel_date in selection_dates:
            hist = stock_df[stock_df['trade_date'] <= sel_date].tail(70)
            if len(hist) < 20:
                continue
            
            # Turnover
            turnover_score = max(0, min(100, 100 - (hist['turnover'].tail(20).mean() - 2) * 5))
            
            # LowVol
            returns = hist['close'].pct_change().dropna()
            vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
            lowvol_score = max(0, min(100, 100 - vol * 200))
            
            # Reversal
            price_now = hist['close'].iloc[-1]
            price_20d = hist['close'].iloc[-21] if len(hist) >= 21 else hist['close'].iloc[0]
            reversal_score = max(0, min(100, 50 - ((price_now - price_20d) / price_20d) * 150))
            
            total = turnover_score * 0.35 + lowvol_score * 0.35 + reversal_score * 0.30
            
            if total >= 45:
                # 找3日后的价格
                future = stock_df[stock_df['trade_date'] > sel_date]
                if len(future) >= 3:
                    exit_price = future.iloc[2]['close']
                    ret = (exit_price - price_now) / price_now
                    net_ret = ret - 0.0028
                    all_results.append({
                        'date': sel_date.strftime('%Y-%m-%d'),
                        'code': code,
                        'score': total,
                        'entry_price': price_now,
                        'exit_price': exit_price,
                        'net_return': net_ret
                    })

conn.close()

print(f"\n[2/3] 回测完成! 总交易次数: {len(all_results)}")

# 统计
if all_results:
    df_results = pd.DataFrame(all_results)
    df_results = df_results.sort_values('score', ascending=False).groupby('date').head(5).reset_index(drop=True)
    
    win_trades = len(df_results[df_results['net_return'] > 0])
    total_trades = len(df_results)
    win_rate = win_trades / total_trades
    
    print("\n" + "="*70)
    print("回测结果 (2024-2025)")
    print("="*70)
    print(f"总交易次数: {total_trades}")
    print(f"盈利次数: {win_trades}")
    print(f"胜率: {win_rate*100:.2f}%")
    print(f"累计收益: {df_results['net_return'].sum()*100:.2f}%")
    print(f"平均收益: {df_results['net_return'].mean()*100:.3f}%")
    
    # 存入数据库
    print("\n[3/3] 存入数据库...")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    run_id = 'V11_IC_2024_2025_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    
    cursor.execute('''
        INSERT INTO backtest_summary 
        (run_id, strategy_version, start_date, end_date, initial_capital,
         total_trades, win_trades, win_rate, total_return, avg_net_return, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        run_id, 'V11_IC_Optimized', '2024-01-02', '2025-12-31', 1000000,
        total_trades, win_trades, win_rate,
        float(df_results['net_return'].sum()),
        float(df_results['net_return'].mean()),
        'V11_IC_Optimized 完整两年回测'
    ))
    
    for _, row in df_results.iterrows():
        cursor.execute('''
            INSERT INTO backtest_trades 
            (run_id, strategy_version, code, select_date, select_score,
             entry_price, exit_price, net_return, exit_reason, hold_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            run_id, 'V11_IC_Optimized', row['code'], row['date'], row['score'],
            row['entry_price'], row['exit_price'], row['net_return'], 'time_exit', 3
        ))
    
    conn.commit()
    conn.close()
    
    # 保存CSV
    df_results.to_csv(f'backtest_results/v11_ic/trades_2024_2025_{run_id}.csv', index=False)
    
    print(f"✅ 已存入数据库 (run_id: {run_id})")
    print(f"✅ CSV已保存")

print("\n完成!")
