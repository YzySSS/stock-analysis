#!/usr/bin/env python3
"""
V12_MarketAdaptive 2024年Q1回测 (简化版)
=========================================
"""

import pandas as pd
import numpy as np
import pymysql
import json
import os
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

print("="*70)
print("V12_MarketAdaptive 2024年Q1回测")
print("="*70)

# 1. 连接数据库
conn = pymysql.connect(**DB_CONFIG)

# 2. 加载数据
print("\n[1/3] 加载数据...")
cursor = conn.cursor()
cursor.execute("SELECT code FROM stock_basic WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0")
stock_codes = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"  有效股票数: {len(stock_codes)}")

# 加载2024年Q1数据（提前3个月）
sql = """
SELECT code, trade_date, close, turnover, amount
FROM stock_kline 
WHERE trade_date BETWEEN '2023-10-01' AND '2024-03-31'
AND amount >= 500000
ORDER BY code, trade_date
"""
df = pd.read_sql(sql, conn)

# 过滤
df = df[df['code'].isin(stock_codes)]
df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]

for col in ['close', 'turnover', 'amount']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df['trade_date'] = pd.to_datetime(df['trade_date'])

print(f"  K线记录数: {len(df):,}")

# 3. 获取沪深300数据用于判断市场状态
print("\n[2/3] 获取沪深300数据...")
cursor = conn.cursor()
cursor.execute("""
    SELECT trade_date, close FROM stock_kline 
    WHERE code = '000001' AND trade_date BETWEEN '2023-10-01' AND '2024-03-31'
    ORDER BY trade_date
""")
hs300_rows = cursor.fetchall()
cursor.close()

hs300_df = pd.DataFrame(hs300_rows, columns=['date', 'close'])
hs300_df['close'] = pd.to_numeric(hs300_df['close'])
hs300_df['date'] = pd.to_datetime(hs300_df['date'])
hs300_df = hs300_df.sort_values('date')

# 计算市场状态
hs300_df['ma20'] = hs300_df['close'].rolling(20).mean()
hs300_df['ret20'] = hs300_df['close'].pct_change(20)

def get_market_state(row):
    if pd.isna(row['ma20']) or pd.isna(row['ret20']):
        return 'oscillation'
    if row['close'] > row['ma20'] and row['ret20'] > 0.05:
        return 'bull'
    elif row['close'] < row['ma20'] and row['ret20'] < -0.05:
        return 'bear'
    return 'oscillation'

hs300_df['market_state'] = hs300_df.apply(get_market_state, axis=1)
market_state_map = dict(zip(hs300_df['date'].dt.strftime('%Y-%m-%d'), hs300_df['market_state']))

print(f"  市场状态: 牛市{sum(hs300_df['market_state']=='bull')}天, 熊市{sum(hs300_df['market_state']=='bear')}天, 震荡{sum(hs300_df['market_state']=='oscillation')}天")

# 因子权重配置
FACTOR_WEIGHTS = {
    'bull': {'turnover': 0.40, 'reversal': 0.30, 'lowvol': 0.30},
    'bear': {'turnover': 0.0, 'reversal': 0.0, 'lowvol': 0.50, 'quality': 0.30, 'value': 0.20},
    'oscillation': {'turnover': 0.30, 'reversal': 0.25, 'lowvol': 0.35, 'quality': 0.10}
}

# 4. 回测
print("\n[3/3] 运行回测...")

# 获取2024年Q1的选股日
all_dates = df[df['trade_date'] >= '2024-01-01']['trade_date'].unique()
selection_dates = sorted(all_dates)[::3]
print(f"  选股日数量: {len(selection_dates)}")

results = []
processed = 0

for code in df['code'].unique():
    stock_df = df[df['code'] == code].sort_values('trade_date')
    
    for sel_date in selection_dates:
        date_str = sel_date.strftime('%Y-%m-%d')
        
        # 获取历史数据
        hist = stock_df[stock_df['trade_date'] <= sel_date].tail(70)
        if len(hist) < 20:
            continue
        
        # 计算因子
        turnover_20d = hist['turnover'].tail(20).mean()
        turnover_score = max(0, min(100, 100 - (turnover_20d - 2) * 5))
        
        returns = hist['close'].pct_change().dropna()
        vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
        lowvol_score = max(0, min(100, 100 - vol * 200))
        
        price_now = hist['close'].iloc[-1]
        price_20d = hist['close'].iloc[-21] if len(hist) >= 21 else hist['close'].iloc[0]
        ret_20d = (price_now - price_20d) / price_20d
        reversal_score = max(0, min(100, 50 - ret_20d * 150))
        
        # 根据市场状态计算加权得分
        market_state = market_state_map.get(date_str, 'oscillation')
        weights = FACTOR_WEIGHTS.get(market_state, FACTOR_WEIGHTS['oscillation'])
        
        total_score = 0
        if 'turnover' in weights:
            total_score += turnover_score * weights['turnover']
        if 'lowvol' in weights:
            total_score += lowvol_score * weights['lowvol']
        if 'reversal' in weights:
            total_score += reversal_score * weights['reversal']
        if 'quality' in weights:
            total_score += 50 * weights['quality']  # 默认质量分
        if 'value' in weights:
            total_score += 50 * weights['value']    # 默认估值分
        
        if total_score >= 45:
            # 找3日后的卖出价格
            future = stock_df[stock_df['trade_date'] > sel_date]
            if len(future) >= 3:
                exit_price = future.iloc[2]['close']
                gross_ret = (exit_price - price_now) / price_now
                net_ret = gross_ret - 0.0028  # 扣除成本
                
                results.append({
                    'date': date_str,
                    'code': code,
                    'market_state': market_state,
                    'score': total_score,
                    'turnover_score': turnover_score,
                    'lowvol_score': lowvol_score,
                    'reversal_score': reversal_score,
                    'entry_price': float(price_now),
                    'exit_price': float(exit_price),
                    'net_return': net_ret
                })
    
    processed += 1
    if processed % 1000 == 0:
        print(f"    已处理 {processed} 只股票, 累计交易 {len(results)} 笔")

print(f"\n  完成! 总候选交易: {len(results)} 笔")

# 5. 统计和存储
if results:
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('score', ascending=False).groupby('date').head(5).reset_index(drop=True)
    
    win_trades = len(df_results[df_results['net_return'] > 0])
    total_trades = len(df_results)
    win_rate = win_trades / total_trades
    
    print("\n" + "="*70)
    print("回测结果 (2024年Q1)")
    print("="*70)
    print(f"总交易次数: {total_trades}")
    print(f"盈利次数: {win_trades}")
    print(f"胜率: {win_rate*100:.2f}%")
    print(f"累计收益: {df_results['net_return'].sum()*100:.2f}%")
    
    # 按市场状态统计
    print("\n按市场状态表现:")
    for state in df_results['market_state'].unique():
        state_df = df_results[df_results['market_state'] == state]
        state_win = len(state_df[state_df['net_return'] > 0])
        print(f"  {state}: 交易{len(state_df)}次, 胜率{state_win/len(state_df)*100:.1f}%, 收益{state_df['net_return'].sum()*100:.1f}%")
    
    # 存入数据库
    print("\n存入数据库...")
    cursor = conn.cursor()
    run_id = 'V12_MA_2024Q1_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    
    cursor.execute('''
        INSERT INTO backtest_summary 
        (run_id, strategy_version, start_date, end_date, total_trades, win_trades, 
         win_rate, total_return, avg_net_return, factor_weights, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        run_id, 'V12_MarketAdaptive', '2024-01-02', '2024-03-31',
        total_trades, win_trades, win_rate,
        float(df_results['net_return'].sum()),
        float(df_results['net_return'].mean()),
        json.dumps(FACTOR_WEIGHTS),
        'V12_MarketAdaptive 2024Q1 市场自适应回测'
    ))
    
    for _, row in df_results.iterrows():
        exit_date = (datetime.strptime(row['date'], '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d')
        cursor.execute('''
            INSERT INTO backtest_trades 
            (run_id, strategy_version, code, select_date, select_score, factor_scores,
             entry_date, entry_price, exit_date, exit_price, net_return, exit_reason, hold_days, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            run_id, 'V12_MarketAdaptive', row['code'], row['date'], row['score'],
            json.dumps({'turnover': row['turnover_score'], 'lowvol': row['lowvol_score'], 'reversal': row['reversal_score']}),
            row['date'], row['entry_price'], exit_date, row['exit_price'],
            row['net_return'], 'time_exit', 3, f"market_state:{row['market_state']}"
        ))
    
    conn.commit()
    print(f"✅ 已存入数据库 (run_id: {run_id})")
    
    # 保存CSV
    os.makedirs('backtest_results/v12_ma', exist_ok=True)
    df_results.to_csv(f'backtest_results/v12_ma/trades_2024q1_{run_id}.csv', index=False)

conn.close()
print("\n完成!")
