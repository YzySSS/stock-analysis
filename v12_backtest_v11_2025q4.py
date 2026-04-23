#!/usr/bin/env python3
"""
V11_IC_Optimized 2025年Q4回测
=============================
"""

import pandas as pd
import numpy as np
import pymysql
import json
import os
from datetime import datetime

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

print("="*70)
print("V11_IC_Optimized 2025年Q4回测")
print("="*70)

# 1. 加载数据
print("\n[1/3] 加载2025年Q4数据...")
conn = pymysql.connect(**DB_CONFIG)

cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"  有效股票数: {len(stock_codes)}")

sql = """
SELECT code, trade_date, close, turnover, amount
FROM stock_kline 
WHERE trade_date BETWEEN '2025-08-01' AND '2025-12-31'
AND amount >= 500000
ORDER BY code, trade_date
"""

df = pd.read_sql(sql, conn)
conn.close()

df = df[df['code'].isin(stock_codes)]
df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]

for col in ['close', 'turnover', 'amount']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df['trade_date'] = pd.to_datetime(df['trade_date'])

print(f"  K线记录数: {len(df):,}")

# 2. 回测
print("\n[2/3] 运行回测...")
all_dates = df[(df['trade_date'] >= '2025-10-01') & (df['trade_date'] <= '2025-12-31')]['trade_date'].unique()
selection_dates = sorted(all_dates)[::3]
print(f"  选股日数量: {len(selection_dates)}")

results = []
processed = 0

for code in df['code'].unique():
    stock_df = df[df['code'] == code].sort_values('trade_date')
    
    for sel_date in selection_dates:
        hist = stock_df[stock_df['trade_date'] <= sel_date].tail(70)
        if len(hist) < 20:
            continue
        
        turnover_score = max(0, min(100, 100 - (hist['turnover'].tail(20).mean() - 2) * 5))
        returns = hist['close'].pct_change().dropna()
        vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
        lowvol_score = max(0, min(100, 100 - vol * 200))
        price_now = hist['close'].iloc[-1]
        price_20d = hist['close'].iloc[-21] if len(hist) >= 21 else hist['close'].iloc[0]
        reversal_score = max(0, min(100, 50 - ((price_now - price_20d) / price_20d) * 150))
        total = turnover_score * 0.35 + lowvol_score * 0.35 + reversal_score * 0.30
        
        if total >= 45:
            future = stock_df[stock_df['trade_date'] > sel_date]
            if len(future) >= 3:
                exit_price = future.iloc[2]['close']
                ret = (exit_price - price_now) / price_now
                net_ret = ret - 0.0028
                results.append({
                    'date': sel_date.strftime('%Y-%m-%d'),
                    'code': code,
                    'score': total,
                    'turnover_score': turnover_score,
                    'lowvol_score': lowvol_score,
                    'reversal_score': reversal_score,
                    'entry_price': price_now,
                    'exit_price': exit_price,
                    'net_return': net_ret
                })
    
    processed += 1
    if processed % 1000 == 0:
        print(f"    已处理 {processed} 只股票, 累计交易 {len(results)} 笔")

print(f"\n  完成! 总候选交易: {len(results)} 笔")

# 3. 存储
if results:
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('score', ascending=False).groupby('date').head(5).reset_index(drop=True)
    
    win_trades = len(df_results[df_results['net_return'] > 0])
    total_trades = len(df_results)
    win_rate = win_trades / total_trades
    
    print("\n" + "="*70)
    print("回测结果 (2025年Q4)")
    print("="*70)
    print(f"总交易次数: {total_trades}")
    print(f"盈利次数: {win_trades}")
    print(f"胜率: {win_rate*100:.2f}%")
    print(f"累计收益: {df_results['net_return'].sum()*100:.2f}%")
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    run_id = 'V11_IC_2025Q4_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    
    cursor.execute('''
        INSERT INTO backtest_summary 
        (run_id, strategy_version, start_date, end_date, initial_capital,
         total_trades, win_trades, win_rate, total_return, avg_net_return, factor_weights, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        run_id, 'V11_IC_Optimized', '2025-10-01', '2025-12-31', 1000000,
        total_trades, win_trades, win_rate,
        float(df_results['net_return'].sum()),
        float(df_results['net_return'].mean()),
        json.dumps({'turnover': 0.35, 'lowvol': 0.35, 'reversal': 0.30}),
        'V11_IC_Optimized 2025年Q4回测'
    ))
    
    for _, row in df_results.iterrows():
        cursor.execute('''
            INSERT INTO backtest_trades 
            (run_id, strategy_version, code, select_date, select_score, factor_scores,
             entry_date, entry_price, exit_date, exit_price, net_return, exit_reason, hold_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            run_id, 'V11_IC_Optimized', row['code'], row['date'], row['score'],
            json.dumps({'turnover': row['turnover_score'], 'lowvol': row['lowvol_score'], 'reversal': row['reversal_score']}),
            row['date'], row['entry_price'], row['date'], row['exit_price'],
            row['net_return'], 'time_exit', 3
        ))
    
    conn.commit()
    conn.close()
    
    os.makedirs('backtest_results/v11_ic', exist_ok=True)
    df_results.to_csv(f'backtest_results/v11_ic/trades_2025q4_{run_id}.csv', index=False)
    
    print(f"✅ 已存入数据库 (run_id: {run_id})")

print("\n完成!")
