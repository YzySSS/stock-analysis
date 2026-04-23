#!/usr/bin/env python3
"""
V11_IC_Optimized 向量化快速回测
================================
使用pandas向量化计算，大幅提升速度
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
import json
from datetime import datetime
import pymysql

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

print("="*60)
print("V11_IC_Optimized 快速回测")
print("="*60)

# 1. 加载数据
print("\n[1/4] 加载数据...")
conn = pymysql.connect(**DB_CONFIG)

# 获取股票列表(过滤ETF/ST/退市)
cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()

sql = """
SELECT code, trade_date, open, close, turnover, amount
FROM stock_kline 
WHERE trade_date BETWEEN '2024-01-01' AND '2024-04-30'
AND amount >= 500000
ORDER BY code, trade_date
"""
df = pd.read_sql(sql, conn)
conn.close()

# 过滤ETF和指数
df = df[df['code'].isin(stock_codes)]
# 过滤掉指数代码 (399/899开头的指数, sh/sz前缀)
df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]

for col in ['open', 'close', 'turnover', 'amount']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['trade_date'] = pd.to_datetime(df['trade_date'])
print(f"  记录数: {len(df):,}")

# 2. 计算因子
print("\n[2/4] 计算因子...")

def calc_factors(group):
    if len(group) < 20:
        return pd.Series({'total': 0})
    
    # Turnover (低换手)
    turnover_score = max(0, min(100, 100 - (group['turnover'].tail(20).mean() - 2) * 5))
    
    # LowVol (低波动)
    returns = group['close'].pct_change().dropna()
    vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
    lowvol_score = max(0, min(100, 100 - vol * 200))
    
    # Reversal (超跌)
    price_now = group['close'].iloc[-1]
    price_20d = group['close'].iloc[-21] if len(group) >= 21 else group['close'].iloc[0]
    reversal_score = max(0, min(100, 50 - ((price_now - price_20d) / price_20d) * 150))
    
    total = turnover_score * 0.35 + lowvol_score * 0.35 + reversal_score * 0.30
    
    return pd.Series({
        'turnover': turnover_score,
        'lowvol': lowvol_score,
        'reversal': reversal_score,
        'total': total,
        'close': price_now,
        'date': group['trade_date'].iloc[-1]
    })

# 按日期分组计算因子
results = []
trading_days = sorted(df['trade_date'].unique())
selection_days = trading_days[::3]  # 每3天

for i, date in enumerate(selection_days[:-1]):
    print(f"  处理 {date.strftime('%Y-%m-%d')}...")
    
    # 获取当日数据
    day_df = df[df['trade_date'] <= date].groupby('code').apply(
        lambda x: x.tail(70) if len(x) >= 20 else None
    ).dropna()
    
    if len(day_df) == 0:
        continue
    
    # 计算因子
    factors = day_df.groupby('code').apply(calc_factors)
    factors = factors[factors['total'] >= 45].sort_values('total', ascending=False).head(5)
    
    if len(factors) == 0:
        continue
    
    # 获取3日后价格
    exit_date = selection_days[i + 1]
    exit_prices = df[df['trade_date'] == exit_date][['code', 'close']].set_index('code')['close']
    
    for code, row in factors.iterrows():
        if code in exit_prices.index:
            ret = (exit_prices[code] - row['close']) / row['close']
            cost = 0.0028  # 万3 + 千0.5 + 千2
            net_ret = ret - cost
            results.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': code,
                'score': row['total'],
                'return': ret,
                'net_return': net_ret
            })

# 3. 统计结果
print("\n[3/4] 统计结果...")
if results:
    df_results = pd.DataFrame(results)
    win_trades = len(df_results[df_results['net_return'] > 0])
    total_trades = len(df_results)
    win_rate = win_trades / total_trades
    
    print("\n" + "="*60)
    print("回测结果 (2024年Q1)")
    print("="*60)
    print(f"总交易次数: {total_trades}")
    print(f"盈利次数: {win_trades}")
    print(f"亏损次数: {total_trades - win_trades}")
    print(f"胜率: {win_rate*100:.2f}%")
    print(f"平均收益: {df_results['net_return'].mean()*100:.3f}%")
    print(f"累计收益: {df_results['net_return'].sum()*100:.2f}%")
    print(f"最大单笔盈利: {df_results['net_return'].max()*100:.2f}%")
    print(f"最大单笔亏损: {df_results['net_return'].min()*100:.2f}%")
    
    # 4. 保存结果
    print("\n[4/4] 保存结果...")
    os.makedirs('backtest_results/v11_ic', exist_ok=True)
    df_results.to_csv('backtest_results/v11_ic/trades_q1.csv', index=False)
    print("  已保存到 backtest_results/v11_ic/trades_q1.csv")
    
    # 打印明细
    print("\n交易明细 (前20笔):")
    for _, row in df_results.head(20).iterrows():
        print(f"  {row['date']} {row['code']}: 得分={row['score']:.1f}, 收益={row['net_return']*100:.2f}%")
else:
    print("无交易记录")

print("\n完成!")
