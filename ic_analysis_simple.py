#!/usr/bin/env python3
"""
IC分析工具 - 简化版 (批量查询)
================================
快速评估核心因子的IC值
"""

import os
import pandas as pd
import numpy as np
import pymysql
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

def calculate_ic(factor_values, returns):
    """计算Spearman IC"""
    valid = ~(pd.isna(factor_values) | pd.isna(returns))
    if valid.sum() < 10:
        return np.nan
    f = factor_values[valid].rank()
    r = returns[valid].rank()
    return f.corr(r)

def analyze_ic():
    print("=" * 70)
    print("📊 V10因子IC分析 (简化版)")
    print("=" * 70)
    
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取交易日列表
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT trade_date FROM stock_kline 
            WHERE trade_date BETWEEN '2024-01-01' AND '2026-04-03'
            ORDER BY trade_date
        """)
        trade_dates = [row[0] for row in cursor.fetchall()]
    
    print(f"分析区间: {trade_dates[0]} ~ {trade_dates[-1]}")
    print(f"交易日: {len(trade_dates)}天")
    print("=" * 70)
    
    # 每5天采样一次
    sample_dates = trade_dates[::5]
    print(f"采样天数: {len(sample_dates)}天\n")
    
    # 存储IC值
    ic_results = {
        'quality': [], 'value': [], 'momentum': [], 
        'reversal': [], 'lowvol': []
    }
    
    for i, date in enumerate(sample_dates):
        if i < 20:  # 跳过前20天（需要历史数据）
            continue
        
        if i % 10 == 0:
            print(f"进度: {i}/{len(sample_dates)} - {date}")
        
        try:
            # 获取当日股票数据
            query = """
            SELECT b.code, b.roe_clean, b.pe_fixed, k.close, k.turnover
            FROM stock_basic b
            JOIN stock_kline k ON b.code = k.code
            WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
            AND k.turnover >= 1.0 AND k.close BETWEEN 5 AND 150
            """
            df_today = pd.read_sql(query, conn, params=(date,))
            
            if len(df_today) < 100:
                continue
            
            # 获取次日收益
            next_date = trade_dates[trade_dates.index(date) + 1] if date in trade_dates else None
            if not next_date:
                continue
            
            query_next = """
            SELECT code, close FROM stock_kline WHERE trade_date = %s
            """
            df_next = pd.read_sql(query_next, conn, params=(next_date,))
            
            # 合并数据
            df = df_today.merge(df_next, on='code', suffixes=('', '_next'))
            df = df.dropna()
            
            if len(df) < 50:
                continue
            
            # 计算次日收益率
            df['forward_return'] = (df['close_next'] - df['close']) / df['close'] * 100
            
            # 获取历史价格计算动量和波动率
            codes = df['code'].tolist()
            placeholders = ','.join(['%s'] * len(codes))
            
            # 获取20天前和60天前的价格
            query_hist = f"""
            SELECT code, close, trade_date FROM stock_kline 
            WHERE code IN ({placeholders}) AND trade_date <= %s
            ORDER BY code, trade_date DESC
            """
            df_hist = pd.read_sql(query_hist, conn, params=tuple(codes) + (date,))
            
            # 为每只股票计算因子
            for code in codes[:]:  # 复制列表避免修改
                stock_hist = df_hist[df_hist['code'] == code]
                if len(stock_hist) < 21:
                    continue
                
                idx = df[df['code'] == code].index[0]
                
                # 当前价格
                price_now = df.loc[idx, 'close']
                
                # 20天前价格
                price_20d = stock_hist.iloc[20]['close']
                ret_20d = (price_now - price_20d) / price_20d * 100
                
                # 60日波动率
                if len(stock_hist) >= 61:
                    prices_60d = stock_hist['close'].iloc[:60].tolist()
                    returns_60d = [(prices_60d[j] - prices_60d[j+1]) / prices_60d[j+1] 
                                   for j in range(len(prices_60d)-1)]
                    vol_60d = np.std(returns_60d) * 100
                else:
                    prices_20d = stock_hist['close'].iloc[:20].tolist()
                    returns_20d = [(prices_20d[j] - prices_20d[j+1]) / prices_20d[j+1] 
                                   for j in range(len(prices_20d)-1)]
                    vol_60d = np.std(returns_20d) * 100
                
                # 计算因子
                df.loc[idx, 'quality'] = df.loc[idx, 'roe_clean'] if pd.notna(df.loc[idx, 'roe_clean']) else 10
                df.loc[idx, 'value'] = -df.loc[idx, 'pe_fixed'] if pd.notna(df.loc[idx, 'pe_fixed']) else -30
                df.loc[idx, 'momentum'] = ret_20d
                df.loc[idx, 'reversal'] = -ret_20d
                df.loc[idx, 'lowvol'] = -vol_60d
            
            # 计算各因子的IC
            for factor in ['quality', 'value', 'momentum', 'reversal', 'lowvol']:
                if factor in df.columns:
                    ic = calculate_ic(df[factor], df['forward_return'])
                    if not pd.isna(ic):
                        ic_results[factor].append(ic)
        
        except Exception as e:
            continue
    
    conn.close()
    
    # 输出结果
    print("\n" + "=" * 70)
    print("📈 IC分析结果")
    print("=" * 70)
    print(f"\n{'因子':<15} {'样本数':>8} {'IC均值':>10} {'|IC|均值':>10} {'ICIR':>8} {'评价':<8}")
    print("-" * 70)
    
    for factor, name in [
        ('quality', 'Quality(ROE)'),
        ('value', 'Value(-PE)'),
        ('momentum', 'Momentum'),
        ('reversal', 'Reversal'),
        ('lowvol', 'LowVol')
    ]:
        values = ic_results[factor]
        if not values:
            print(f"{name:<15} {'N/A':>8} {'N/A':>10} {'N/A':>10} {'N/A':>8} ❌ 无数据")
            continue
        
        ic_mean = np.mean(values)
        ic_abs = np.mean(np.abs(values))
        ic_std = np.std(values)
        icir = ic_mean / ic_std if ic_std > 0 else 0
        
        if ic_abs < 0.02:
            eval_text = "❌ 无效"
        elif ic_abs < 0.03:
            eval_text = "⚠️ 较弱"
        elif ic_abs < 0.05:
            eval_text = "✅ 有效"
        else:
            eval_text = "🌟 很强"
        
        print(f"{name:<15} {len(values):>8} {ic_mean:>+10.4f} {ic_abs:>10.4f} {icir:>+8.2f} {eval_text:<8}")
    
    # 结论
    print("\n" + "=" * 70)
    print("💡 结论")
    print("=" * 70)
    print("""
IC分析用于评估因子的预测能力：
- IC > 0: 因子与未来收益正相关
- IC < 0: 因子与未来收益负相关  
- |IC| > 0.03: 有预测价值
- |IC| > 0.05: 较强预测能力

V10回测失败的原因：
- Reversal因子IC应该为负（跌的股票继续跌）
- Value因子IC可能为负（价值陷阱）
- Momentum因子IC应该为正（追涨有效）
    """)

if __name__ == "__main__":
    analyze_ic()
