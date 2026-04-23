#!/usr/bin/env python3
"""
V11_IC_Optimized 完整两年回测 (2024-2025)
===========================================
将结果存入数据库
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
print("V11_IC_Optimized 完整两年回测 (2024-2025)")
print("="*70)

# 1. 加载数据
print("\n[1/5] 加载两年历史数据...")
conn = pymysql.connect(**DB_CONFIG)

# 获取股票列表(过滤ETF/ST/退市)
cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"  有效股票数: {len(stock_codes)}")

# 加载K线数据
sql = """
SELECT code, trade_date, open, close, turnover, amount
FROM stock_kline 
WHERE trade_date BETWEEN '2024-01-01' AND '2025-12-31'
AND amount >= 500000
ORDER BY code, trade_date
"""
df = pd.read_sql(sql, conn)
conn.close()

# 过滤ETF和指数
df = df[df['code'].isin(stock_codes)]
df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]

for col in ['open', 'close', 'turnover', 'amount']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['trade_date'] = pd.to_datetime(df['trade_date'])
print(f"  K线记录数: {len(df):,}")

# 2. 计算因子
print("\n[2/5] 计算因子...")

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

# 3. 回测
print("\n[3/5] 运行回测...")
trading_days = sorted(df['trade_date'].unique())
print(f"  交易日数量: {len(trading_days)}")

selection_days = trading_days[::3]  # 每3天选股
results = []

for i, date in enumerate(selection_days[:-1]):
    if i % 20 == 0:
        print(f"  处理 {date.strftime('%Y-%m-%d')}... ({i+1}/{len(selection_days)})")
    
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

print(f"\n  完成! 总交易次数: {len(results)}")

# 4. 统计结果
print("\n[4/5] 统计结果...")
df_results = pd.DataFrame(results)
win_trades = len(df_results[df_results['net_return'] > 0])
total_trades = len(df_results)
win_rate = win_trades / total_trades if total_trades > 0 else 0

print("\n" + "="*70)
print("回测结果 (2024-2025两年)")
print("="*70)
print(f"总交易次数: {total_trades}")
print(f"盈利次数: {win_trades}")
print(f"亏损次数: {total_trades - win_trades}")
print(f"胜率: {win_rate*100:.2f}%")
print(f"平均收益: {df_results['net_return'].mean()*100:.3f}%")
print(f"累计收益: {df_results['net_return'].sum()*100:.2f}%")
print(f"最大单笔盈利: {df_results['net_return'].max()*100:.2f}%")
print(f"最大单笔亏损: {df_results['net_return'].min()*100:.2f}%")

# 5. 存入数据库
print("\n[5/5] 存入数据库...")
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# 生成回测批次ID
run_id = 'V11_IC_2024_2025_' + datetime.now().strftime('%Y%m%d_%H%M%S')
strategy_version = 'V11_IC_Optimized'

# 插入交易明细
insert_sql = '''
INSERT INTO backtest_trades 
(run_id, strategy_version, code, select_date, select_score, factor_scores,
 entry_date, entry_price, exit_date, exit_price,
 gross_return, net_return, total_cost, exit_reason, hold_days)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

for _, row in df_results.iterrows():
    factor_scores = json.dumps({
        'turnover': float(row['turnover_score']),
        'lowvol': float(row['lowvol_score']),
        'reversal': float(row['reversal_score'])
    })
    cursor.execute(insert_sql, (
        run_id, strategy_version, row['code'],
        row['date'], float(row['score']), factor_scores,
        row['date'], float(row['entry_price']),
        row['exit_date'], float(row['exit_price']),
        float(row['return']), float(row['net_return']),
        0.0028, 'time_exit', 3
    ))

print(f"  ✅ 插入 {len(df_results)} 条交易明细")

# 插入汇总记录
summary_sql = '''
INSERT INTO backtest_summary 
(run_id, strategy_version, start_date, end_date, initial_capital, final_capital,
 strategy_params, total_trades, win_trades, loss_trades, win_rate,
 total_return, avg_net_return, total_cost, avg_hold_days, factor_weights, remark)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

cursor.execute(summary_sql, (
    run_id, strategy_version, '2024-01-02', '2025-12-31',
    1000000, 1000000 * (1 + df_results['net_return'].sum()),
    json.dumps({
        'score_threshold': 45,
        'max_positions': 5,
        'hold_days': 3,
        'stop_loss': -0.08,
        'commission': 0.0003,
        'stamp_tax': 0.0005,
        'slippage': 0.002
    }),
    total_trades, win_trades, total_trades - win_trades, win_rate,
    float(df_results['net_return'].sum()),
    float(df_results['net_return'].mean()),
    0.0028 * total_trades, 3,
    json.dumps({'turnover': 0.35, 'lowvol': 0.35, 'reversal': 0.30}),
    'V11_IC_Optimized 2024-2025完整两年回测'
))

print(f"  ✅ 插入汇总记录")

conn.commit()
conn.close()

# 保存CSV备份
df_results.to_csv(f'backtest_results/v11_ic/trades_2024_2025_{run_id}.csv', index=False)
print(f"  ✅ CSV备份已保存")

print("\n" + "="*70)
print("回测完成!")
print(f"run_id: {run_id}")
print("="*70)
