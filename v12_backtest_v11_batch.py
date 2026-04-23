#!/usr/bin/env python3
"""
V11_IC_Optimized 分批回测 (按月处理)
=====================================
优化内存使用，按月处理数据
"""

import pandas as pd
import numpy as np
import pymysql
import json
from datetime import datetime

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

print("="*70)
print("V11_IC_Optimized 分批回测 (2024-2025)")
print("="*70)

# 1. 获取股票列表
print("\n[1/5] 加载股票列表...")
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()
conn.close()
print(f"  有效股票数: {len(stock_codes)}")

# 2. 按月加载数据并回测
print("\n[2/5] 按月回测...")

def calc_factors(group):
    if len(group) < 20:
        return pd.Series({'total': 0})
    turnover_score = max(0, min(100, 100 - (group['turnover'].tail(20).mean() - 2) * 5))
    returns = group['close'].pct_change().dropna()
    vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
    lowvol_score = max(0, min(100, 100 - vol * 200))
    price_now = group['close'].iloc[-1]
    price_20d = group['close'].iloc[-21] if len(group) >= 21 else group['close'].iloc[0]
    reversal_score = max(0, min(100, 50 - ((price_now - price_20d) / price_20d) * 150))
    total = turnover_score * 0.35 + lowvol_score * 0.35 + reversal_score * 0.30
    return pd.Series({'turnover': turnover_score, 'lowvol': lowvol_score, 'reversal': reversal_score, 'total': total, 'close': price_now})

# 按月处理
months = [
    ('2024-01', '2024-02'), ('2024-02', '2024-03'), ('2024-03', '2024-04'),
    ('2024-04', '2024-05'), ('2024-05', '2024-06'), ('2024-06', '2024-07'),
    ('2024-07', '2024-08'), ('2024-08', '2024-09'), ('2024-09', '2024-10'),
    ('2024-10', '2024-11'), ('2024-11', '2024-12'), ('2024-12', '2025-01'),
    ('2025-01', '2025-02'), ('2025-02', '2025-03'), ('2025-03', '2025-04'),
    ('2025-04', '2025-05'), ('2025-05', '2025-06'), ('2025-06', '2025-07'),
    ('2025-07', '2025-08'), ('2025-08', '2025-09'), ('2025-09', '2025-10'),
    ('2025-10', '2025-11'), ('2025-11', '2025-12'), ('2025-12', '2026-01')
]

all_results = []

for month_idx, (start, end) in enumerate(months):
    print(f"  处理 {start}... ({month_idx+1}/{len(months)})")
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        sql = f"""
        SELECT code, trade_date, open, close, turnover, amount
        FROM stock_kline 
        WHERE trade_date >= '{start}-01' AND trade_date < '{end}-01'
        AND amount >= 500000
        ORDER BY code, trade_date
        """
        df = pd.read_sql(sql, conn)
        conn.close()
        
        if len(df) == 0:
            continue
        
        # 过滤
        df = df[df['code'].isin(stock_codes)]
        df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]
        
        for col in ['open', 'close', 'turnover', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 选股
        trading_days = sorted(df['trade_date'].unique())
        selection_days = trading_days[::3]
        
        for i, date in enumerate(selection_days[:-1]):
            day_df = df[df['trade_date'] <= date].groupby('code').apply(
                lambda x: x.tail(70) if len(x) >= 20 else None
            ).dropna()
            
            if len(day_df) == 0:
                continue
            
            factors = day_df.groupby('code').apply(calc_factors)
            factors = factors[factors['total'] >= 45].sort_values('total', ascending=False).head(5)
            
            if len(factors) == 0:
                continue
            
            exit_date = selection_days[i + 1]
            exit_prices = df[df['trade_date'] == exit_date][['code', 'close']].set_index('code')['close']
            
            for code, row in factors.iterrows():
                if code in exit_prices.index:
                    ret = (exit_prices[code] - row['close']) / row['close']
                    net_ret = ret - 0.0028
                    all_results.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'exit_date': exit_date.strftime('%Y-%m-%d'),
                        'code': code,
                        'score': row['total'],
                        'turnover_score': row['turnover'],
                        'lowvol_score': row['lowvol'],
                        'reversal_score': row['reversal'],
                        'entry_price': row['close'],
                        'exit_price': exit_prices[code],
                        'return': ret,
                        'net_return': net_ret
                    })
        
        print(f"    累计交易: {len(all_results)}")
        
    except Exception as e:
        print(f"    错误: {e}")
        continue

print(f"\n完成! 总交易次数: {len(all_results)}")

# 3. 统计结果
if all_results:
    df_results = pd.DataFrame(all_results)
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
    
    # 4. 存入数据库
    print("\n[4/5] 存入数据库...")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    run_id = 'V11_IC_2024_2025_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 插入汇总
    cursor.execute('''
        INSERT INTO backtest_summary 
        (run_id, strategy_version, start_date, end_date, initial_capital, final_capital,
         strategy_params, total_trades, win_trades, loss_trades, win_rate,
         total_return, avg_net_return, avg_hold_days, factor_weights, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        run_id, 'V11_IC_Optimized', '2024-01-02', '2025-12-31',
        1000000, 1000000 * (1 + df_results['net_return'].sum()),
        json.dumps({'threshold': 45, 'hold_days': 3}),
        total_trades, win_trades, total_trades - win_trades, win_rate,
        float(df_results['net_return'].sum()), float(df_results['net_return'].mean()),
        3, json.dumps({'turnover': 0.35, 'lowvol': 0.35, 'reversal': 0.30}),
        'V11_IC_Optimized 2024-2025分批回测'
    ))
    
    # 批量插入交易明细
    for _, row in df_results.iterrows():
        cursor.execute('''
            INSERT INTO backtest_trades 
            (run_id, strategy_version, code, select_date, select_score,
             entry_date, entry_price, exit_date, exit_price,
             gross_return, net_return, total_cost, exit_reason, hold_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            run_id, 'V11_IC_Optimized', row['code'], row['date'], row['score'],
            row['date'], row['entry_price'], row['exit_date'], row['exit_price'],
            row['return'], row['net_return'], 0.0028, 'time_exit', 3
        ))
    
    conn.commit()
    conn.close()
    
    print(f"✅ 已存入数据库 (run_id: {run_id})")
    
    # 5. 保存CSV
    df_results.to_csv(f'backtest_results/v11_ic/trades_2024_2025_{run_id}.csv', index=False)
    print(f"✅ CSV已保存")

print("\n完成!")
