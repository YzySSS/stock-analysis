#!/usr/bin/env python3
"""
IC分析工具 - 多周期版本
========================
测试不同持仓周期的因子IC值

持仓周期: 1日 / 3日 / 5日 / 10日
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

def get_future_price(cursor, code, start_date, trade_dates, days_forward):
    """获取未来第N天的价格"""
    try:
        start_idx = trade_dates.index(start_date)
        target_idx = start_idx + days_forward
        if target_idx >= len(trade_dates):
            return None
        target_date = trade_dates[target_idx]
        
        cursor.execute("""
            SELECT close FROM stock_kline 
            WHERE code = %s AND trade_date = %s
        """, (code, target_date))
        row = cursor.fetchone()
        return row[0] if row else None
    except:
        return None

def pearson_correlation(x, y):
    """计算Pearson相关系数"""
    if len(x) < 10:
        return None
    
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    
    cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)
    
    if var_x == 0 or var_y == 0:
        return None
    
    return cov / (var_x * var_y) ** 0.5

def spearman_ic(x, y):
    """计算Spearman秩相关系数"""
    if len(x) < 10 or len(y) < 10:
        return None
    
    def rank_with_ties(values):
        sorted_vals = sorted([(v, i) for i, v in enumerate(values)])
        ranks = [0] * len(values)
        i = 0
        while i < len(sorted_vals):
            j = i
            while j < len(sorted_vals) and sorted_vals[j][0] == sorted_vals[i][0]:
                j += 1
            rank = (i + j + 1) / 2
            for k in range(i, j):
                ranks[sorted_vals[k][1]] = rank
            i = j
        return ranks
    
    x_rank = rank_with_ties(x)
    y_rank = rank_with_ties(y)
    
    return pearson_correlation(x_rank, y_rank)

def analyze_ic_for_period(cursor, trade_dates, sample_dates, hold_days):
    """分析指定持仓周期的IC"""
    ic_results = {'quality': [], 'value': [], 'momentum': [], 'reversal': [], 'lowvol': []}
    
    for i, date in enumerate(sample_dates):
        if i % 20 == 0:
            print(f"    持仓{hold_days}日 - 进度: {i}/{len(sample_dates)}")
        
        try:
            # 获取当日股票列表
            cursor.execute("""
                SELECT b.code, b.roe_clean, b.pe_fixed, k.close, k.turnover
                FROM stock_basic b
                JOIN stock_kline k ON b.code = k.code COLLATE utf8mb4_unicode_ci
                WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
                AND k.turnover >= 1.0 AND k.close BETWEEN 5 AND 150
                LIMIT 500
            """, (date,))
            
            stocks = cursor.fetchall()
            if len(stocks) < 50:
                continue
            
            # 计算因子和收益
            factors = defaultdict(list)
            forward_returns = []
            
            for code, roe, pe, close, turnover in stocks:
                # 获取未来价格
                future_price = get_future_price(cursor, code, date, trade_dates, hold_days)
                if future_price is None:
                    continue
                
                # 获取历史价格
                hist_prices = get_prices_before(cursor, code, date, 65)
                if len(hist_prices) < 21:
                    continue
                
                # 当前价格
                price_now = close
                
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
                fwd_ret = (future_price - price_now) / price_now * 100
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
    
    return ic_results

def print_ic_results(results, hold_days):
    """打印IC结果"""
    factor_names = {
        'quality': 'Quality(ROE)',
        'value': 'Value(-PE)',
        'momentum': 'Momentum',
        'reversal': 'Reversal',
        'lowvol': 'LowVol'
    }
    
    print(f"\n{'因子':<15} {'样本':>6} {'IC均值':>10} {'|IC|':>8} {'ICIR':>8} {'正IC%':>8} {'评价':<8}")
    print("-" * 70)
    
    for fname, values in results.items():
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
    
    return {fname: {'ic_mean': np.mean(vals), 'ic_abs': np.mean(np.abs(vals))} 
            for fname, vals in results.items() if vals}

def analyze_ic():
    print("=" * 70)
    print("📊 V10因子IC分析 - 多周期版本")
    print("=" * 70)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取交易日
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN '2024-06-01' AND '2026-03-15'
        ORDER BY trade_date
    """)
    trade_dates = [row[0] for row in cursor.fetchall()]
    
    print(f"分析区间: {trade_dates[0]} ~ {trade_dates[-1]}")
    print(f"交易日: {len(trade_dates)}天")
    print("=" * 70)
    
    # 每5天采样
    sample_dates = trade_dates[::5][10:]
    print(f"采样天数: {len(sample_dates)}天\n")
    
    # 存储各周期结果
    all_results = {}
    
    # 测试不同持仓周期
    for hold_days in [1, 3, 5, 10]:
        print(f"\n{'='*70}")
        print(f"📈 持仓周期: {hold_days}日")
        print(f"{'='*70}")
        
        results = analyze_ic_for_period(cursor, trade_dates, sample_dates, hold_days)
        all_results[hold_days] = print_ic_results(results, hold_days)
    
    cursor.close()
    conn.close()
    
    # 汇总对比
    print("\n" + "=" * 70)
    print("📊 多周期IC对比汇总")
    print("=" * 70)
    
    print(f"\n{'因子':<15} {'1日|IC|':>10} {'3日|IC|':>10} {'5日|IC|':>10} {'10日|IC|':>10} {'最优周期':<10}")
    print("-" * 70)
    
    factors = ['quality', 'value', 'momentum', 'reversal', 'lowvol']
    factor_names = {
        'quality': 'Quality(ROE)',
        'value': 'Value(-PE)',
        'momentum': 'Momentum',
        'reversal': 'Reversal',
        'lowvol': 'LowVol'
    }
    
    for fname in factors:
        name = factor_names[fname]
        ics = []
        for hold_days in [1, 3, 5, 10]:
            ic_val = all_results.get(hold_days, {}).get(fname, {}).get('ic_abs', 0)
            ics.append(ic_val)
        
        best_period = [1, 3, 5, 10][np.argmax(ics)]
        
        print(f"{name:<15} {ics[0]:>10.4f} {ics[1]:>10.4f} {ics[2]:>10.4f} {ics[3]:>10.4f} {str(best_period)+'日':<10}")
    
    # 建议
    print("\n" + "=" * 70)
    print("💡 最优持仓周期建议")
    print("=" * 70)
    
    # 计算每个周期的平均|IC|
    avg_ics = {}
    for hold_days in [1, 3, 5, 10]:
        period_data = all_results.get(hold_days, {})
        if period_data:
            avg_ic = np.mean([data.get('ic_abs', 0) for data in period_data.values()])
            avg_ics[hold_days] = avg_ic
    
    best_overall = max(avg_ics.items(), key=lambda x: x[1])
    
    print(f"\n各周期平均|IC|:")
    for period, avg_ic in sorted(avg_ics.items()):
        marker = " ⭐ 推荐" if period == best_overall[0] else ""
        print(f"  {period}日持仓: {avg_ic:.4f}{marker}")
    
    print(f"""
📋 策略设计建议:
1. 最优持仓周期: {best_overall[0]}日 (平均|IC|={best_overall[1]:.4f})
2. 应优先使用|IC|最高的因子组合
3. 避免使用|IC|<0.03的因子
4. IC符号决定因子方向(正=正向,负=反向)
    """)

if __name__ == "__main__":
    analyze_ic()
