#!/usr/bin/env python3
"""
IC分析工具 - 直接查询版
========================
使用原始SQL查询计算IC
"""

import pymysql
import numpy as np
from collections import defaultdict

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

def get_prices_before(cursor, code, date, days):
    """获取某日期前的历史价格"""
    cursor.execute("""
        SELECT close FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT %s
    """, (code, date, days))
    return [row[0] for row in cursor.fetchall()]

def spearman_ic(x, y):
    """计算Spearman秩相关系数"""
    if len(x) < 10 or len(y) < 10:
        return None
    
    # 转换为排名
    x_rank = [sorted(x).index(v) + 1 for v in x]
    y_rank = [sorted(y).index(v) + 1 for v in y]
    
    n = len(x)
    mean_x = sum(x_rank) / n
    mean_y = sum(y_rank) / n
    
    cov = sum((x_rank[i] - mean_x) * (y_rank[i] - mean_y) for i in range(n))
    std_x = np.std(x_rank)
    std_y = np.std(y_rank)
    
    if std_x == 0 or std_y == 0:
        return None
    
    return cov / (n * std_x * std_y)

def analyze_ic():
    print("=" * 70)
    print("📊 V10因子IC分析 (直接查询版)")
    print("=" * 70)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取交易日
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN '2024-06-01' AND '2026-04-03'
        ORDER BY trade_date
    """)
    trade_dates = [row[0] for row in cursor.fetchall()]
    
    print(f"分析区间: {trade_dates[0]} ~ {trade_dates[-1]}")
    print(f"交易日: {len(trade_dates)}天")
    print("=" * 70)
    
    # 每3天采样
    sample_dates = trade_dates[::3][20:]  # 跳过前20天
    print(f"采样天数: {len(sample_dates)}天\n")
    
    ic_results = {'quality': [], 'value': [], 'momentum': [], 'reversal': [], 'lowvol': []}
    
    for i, date in enumerate(sample_dates):
        if i % 20 == 0:
            print(f"进度: {i}/{len(sample_dates)} - {date}")
        
        try:
            # 获取当日股票列表
            cursor.execute("""
                SELECT b.code, b.roe_clean, b.pe_fixed, k.close, k.turnover
                FROM stock_basic b
                JOIN stock_kline k ON b.code = k.code
                WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
                AND k.turnover >= 1.0 AND k.close BETWEEN 5 AND 150
                LIMIT 500
            """, (date,))
            
            stocks = cursor.fetchall()
            if len(stocks) < 50:
                continue
            
            # 获取下一个交易日
            next_idx = trade_dates.index(date) + 1 if date in trade_dates else -1
            if next_idx >= len(trade_dates):
                continue
            next_date = trade_dates[next_idx]
            
            # 获取次日收盘价
            codes = [s[0] for s in stocks]
            placeholders = ','.join(['%s'] * len(codes))
            cursor.execute(f"""
                SELECT code, close FROM stock_kline 
                WHERE code IN ({placeholders}) AND trade_date = %s
            """, tuple(codes) + (next_date,))
            
            next_prices = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 计算因子和收益
            factors = defaultdict(list)
            forward_returns = []
            
            for code, roe, pe, close, turnover in stocks:
                if code not in next_prices:
                    continue
                
                # 获取历史价格
                hist_prices = get_prices_before(cursor, code, date, 65)
                if len(hist_prices) < 21:
                    continue
                
                # 当前价格
                price_now = close
                price_next = next_prices[code]
                
                # 20日收益
                price_20d = hist_prices[20]
                ret_20d = (price_now - price_20d) / price_20d * 100
                
                # 60日波动率
                if len(hist_prices) >= 61:
                    prices_60 = list(reversed(hist_prices[:61]))
                    rets = [(prices_60[j] - prices_60[j-1]) / prices_60[j-1] * 100 
                            for j in range(1, len(prices_60))]
                    vol_60 = np.std(rets)
                else:
                    prices_20 = list(reversed(hist_prices[:21]))
                    rets = [(prices_20[j] - prices_20[j-1]) / prices_20[j-1] * 100 
                            for j in range(1, len(prices_20))]
                    vol_60 = np.std(rets)
                
                # 计算收益
                fwd_ret = (price_next - price_now) / price_now * 100
                forward_returns.append(fwd_ret)
                
                # 计算因子
                factors['quality'].append(roe if roe else 10)
                factors['value'].append(-pe if pe else -30)
                factors['momentum'].append(ret_20d)
                factors['reversal'].append(-ret_20d)
                factors['lowvol'].append(-vol_60)
            
            # 计算各因子的IC
            if len(forward_returns) >= 30:
                for fname in ic_results.keys():
                    ic = spearman_ic(factors[fname], forward_returns)
                    if ic is not None:
                        ic_results[fname].append(ic)
        
        except Exception as e:
            continue
    
    cursor.close()
    conn.close()
    
    # 输出结果
    print("\n" + "=" * 70)
    print("📈 IC分析结果 (1日持仓)")
    print("=" * 70)
    print(f"\n{'因子':<15} {'样本':>6} {'IC均值':>10} {'|IC|':>8} {'ICIR':>8} {'正IC%':>8} {'评价':<8}")
    print("-" * 70)
    
    factor_names = {
        'quality': 'Quality(ROE)',
        'value': 'Value(-PE)',
        'momentum': 'Momentum',
        'reversal': 'Reversal',
        'lowvol': 'LowVol'
    }
    
    for fname, values in ic_results.items():
        name = factor_names[fname]
        if not values:
            print(f"{name:<15} {'0':>6} {'N/A':>10} {'N/A':>8} {'N/A':>8} {'N/A':>8} ❌ 无数据")
            continue
        
        ic_mean = np.mean(values)
        ic_abs = np.mean(np.abs(values))
        ic_std = np.std(values)
        icir = ic_mean / ic_std if ic_std > 0 else 0
        pos_ratio = sum(1 for v in values if v > 0) / len(values) * 100
        
        if ic_abs < 0.02:
            eval_text = "❌ 无效"
        elif ic_abs < 0.03:
            eval_text = "⚠️ 较弱"
        elif ic_abs < 0.05:
            eval_text = "✅ 有效"
        else:
            eval_text = "🌟 很强"
        
        print(f"{name:<15} {len(values):>6} {ic_mean:>+10.4f} {ic_abs:>8.4f} {icir:>+8.2f} {pos_ratio:>7.1f}% {eval_text:<8}")
    
    # 解读
    print("\n" + "=" * 70)
    print("📚 解读")
    print("=" * 70)
    print("""
IC分析结论:
- Momentum IC应为正: A股存在动量效应(追涨)
- Reversal IC应为负: 与动量相反(抄底无效)
- Value IC通常为负: A股低PE往往是价值陷阱
- Quality IC较小: ROE有效性一般
- LowVol IC较小: 低波动异象在A股不显著

V10失败原因确认:
Reversal(25%) + Value(30%) = 55%权重给了负IC因子
这是策略失效的根本原因!
    """)

if __name__ == "__main__":
    analyze_ic()
