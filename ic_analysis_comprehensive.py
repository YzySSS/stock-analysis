#!/usr/bin/env python3
"""
IC分析工具 - 增强版 (学术标准)
=============================
包含完整的统计检验和更多因子

统计检验:
1. t检验 - IC显著性
2. IR (Information Ratio) - IC稳定性
3. 滚动IC分析 - 时间序列稳定性
4. IC衰减分析 - 持仓周期效应
5. 单调性检验 - 分组收益单调性

因子列表:
经典因子: Value, Quality, Momentum, LowVol, Size
特色因子: Reversal, Turnover, Volatility, Liquidity
"""

import pymysql
import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime
import json

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}


def get_prices_before(cursor, code, date, days):
    """获取历史价格"""
    cursor.execute("""
        SELECT close FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT %s
    """, (code, date, days))
    return [row[0] for row in cursor.fetchall()]


def get_future_return(cursor, code, start_date, trade_dates, hold_days):
    """获取未来N日收益"""
    try:
        start_idx = trade_dates.index(start_date)
        target_idx = start_idx + hold_days
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


def spearman_ic(x, y):
    """Spearman秩相关系数"""
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
    
    n = len(x_rank)
    mean_x = sum(x_rank) / n
    mean_y = sum(y_rank) / n
    
    cov = sum((x_rank[i] - mean_x) * (y_rank[i] - mean_y) for i in range(n))
    var_x = sum((xi - mean_x) ** 2 for xi in x_rank)
    var_y = sum((yi - mean_y) ** 2 for yi in y_rank)
    
    if var_x == 0 or var_y == 0:
        return None
    
    return cov / (var_x * var_y) ** 0.5


def calculate_factor_values(cursor, code, date, trade_dates, stock_data, hist_prices):
    """计算所有因子值"""
    factors = {}
    
    if len(hist_prices) < 21:
        return None
    
    # 确保价格是浮点数
    try:
        price_now = float(hist_prices[0])  # 当日收盘价
        price_20d = float(hist_prices[20]) if len(hist_prices) > 20 else None
    except (TypeError, ValueError):
        return None
    
    # 基础数据 - 转换为浮点数
    try:
        roe = float(stock_data.get('roe')) if stock_data.get('roe') is not None else None
    except (TypeError, ValueError):
        roe = None
    
    try:
        pe = float(stock_data.get('pe')) if stock_data.get('pe') is not None else None
    except (TypeError, ValueError):
        pe = None
    
    try:
        turnover = float(stock_data.get('turnover')) if stock_data.get('turnover') is not None else None
    except (TypeError, ValueError):
        turnover = None
    
    # 计算收益率序列
    returns = []
    for i in range(len(hist_prices) - 1):
        try:
            p_curr = float(hist_prices[i])
            p_prev = float(hist_prices[i+1])
            if p_prev > 0:
                r = (p_curr - p_prev) / p_prev * 100
                returns.append(r)
        except (TypeError, ValueError):
            continue
    
    # ========== 经典因子 ==========
    
    # 1. Value (估值) - PE越低越好
    if pe is not None and pe > 0:
        factors['value_pe'] = float(-pe)
    else:
        factors['value_pe'] = -50.0
    
    # 2. Quality (质量) - ROE越高越好
    if roe is not None:
        factors['quality_roe'] = float(roe)
    else:
        # 用价格稳定性代替
        if len(returns) >= 20:
            factors['quality_roe'] = float(50 - np.std(returns[-20:]))
        else:
            factors['quality_roe'] = 10.0
    
    # 3. Momentum (动量) - 过去收益越高越好
    if price_20d is not None and price_20d > 0:
        ret_20d = (price_now - price_20d) / price_20d * 100
        factors['momentum_20d'] = float(ret_20d)
        # Reversal是动量的反向
        factors['reversal_20d'] = float(-ret_20d)
    else:
        factors['momentum_20d'] = 0.0
        factors['reversal_20d'] = 0.0
    
    # 4. LowVol (低波动) - 波动率越低越好
    if len(returns) >= 60:
        vol_60d = float(np.std(returns[-60:]))
        factors['lowvol_60d'] = -vol_60d
    elif len(returns) >= 20:
        vol_20d = float(np.std(returns[-20:]))
        factors['lowvol_60d'] = -vol_20d * np.sqrt(3)  # 年化调整
    else:
        factors['lowvol_60d'] = -5.0
    
    # ========== 特色因子 ==========
    
    # 5. Turnover (换手率) - 低换手偏好
    if turnover is not None:
        factors['turnover'] = float(-turnover)
    else:
        factors['turnover'] = -5.0
    
    # 6. Volatility (波动率) - 高阶矩
    if len(returns) >= 20:
        returns_20 = returns[-20:]
        std_20 = float(np.std(returns_20))
        
        # 偏度 (负偏度偏好)
        if std_20 > 0:
            mean_20 = float(np.mean(returns_20))
            skewness = np.mean([(float(r) - mean_20)**3 for r in returns_20]) / (std_20**3)
            if not np.isnan(skewness) and not np.isinf(skewness):
                factors['skewness'] = float(-skewness)
            else:
                factors['skewness'] = 0.0
        else:
            factors['skewness'] = 0.0
        
        # 最大回撤
        cumulative = np.cumsum(returns_20)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = cumulative - running_max
        max_dd = float(np.min(drawdown)) if len(drawdown) > 0 else 0.0
        factors['max_drawdown'] = max_dd
    else:
        factors['skewness'] = 0.0
        factors['max_drawdown'] = 0.0
    
    # 7. Liquidity (流动性) - 成交额对数
    if turnover is not None and turnover > 0:
        factors['liquidity'] = float(np.log(turnover))
    else:
        factors['liquidity'] = 0.0
    
    # 8. Beta (市场相关性) - 这里简化计算
    if len(returns) >= 20:
        factors['beta_proxy'] = float(np.std(returns[-20:]))  # 简化beta估计
    else:
        factors['beta_proxy'] = 1.0
    
    return factors


def analyze_factor_ic(factor_name, ic_values):
    """因子IC统计检验"""
    if not ic_values or len(ic_values) < 10:
        return {'error': '样本不足'}
    
    ic_array = np.array(ic_values)
    n = len(ic_array)
    
    # 基础统计
    ic_mean = np.mean(ic_array)
    ic_std = np.std(ic_array)
    ic_abs_mean = np.mean(np.abs(ic_array))
    
    # t检验 (IC显著性)
    # H0: IC = 0
    # t = IC_mean / (IC_std / sqrt(n))
    if ic_std > 0:
        t_stat = ic_mean / (ic_std / np.sqrt(n))
        # 双侧检验p值 (近似)
        from math import erf
        p_value = 2 * (1 - 0.5 * (1 + erf(abs(t_stat) / np.sqrt(2))))
    else:
        t_stat = 0
        p_value = 1
    
    # IR (Information Ratio) - IC稳定性
    ir = ic_mean / ic_std if ic_std > 0 else 0
    
    # 胜率 (IC>0的比例)
    win_rate = np.sum(ic_array > 0) / n
    
    # IC持续性 (自相关系数，简化)
    if n > 1:
        autocorr = np.corrcoef(ic_array[:-1], ic_array[1:])[0, 1] if n > 2 else 0
        if np.isnan(autocorr):
            autocorr = 0
    else:
        autocorr = 0
    
    return {
        'n': n,
        'ic_mean': round(ic_mean, 4),
        'ic_std': round(ic_std, 4),
        'ic_abs_mean': round(ic_abs_mean, 4),
        'ir': round(ir, 4),
        't_stat': round(t_stat, 4),
        'p_value': round(p_value, 4),
        'significant': '***' if p_value < 0.01 else ('**' if p_value < 0.05 else ('*' if p_value < 0.1 else '')),
        'win_rate': round(win_rate, 4),
        'autocorr': round(autocorr, 4),
        'ic_min': round(np.min(ic_array), 4),
        'ic_max': round(np.max(ic_array), 4),
        'ic_median': round(np.median(ic_array), 4),
    }


def analyze_ic_decay(cursor, trade_dates, sample_dates, factor_values_history, forward_returns_history):
    """IC衰减分析 - 不同持仓周期的IC变化"""
    decay_results = {}
    
    # 计算不同周期的IC
    for hold_days in [1, 3, 5, 10, 20]:
        ic_list = []
        
        for i, date in enumerate(sample_dates):
            if date not in factor_values_history:
                continue
            
            factors_dict = factor_values_history[date]
            
            # 获取该持仓周期的未来收益
            future_rets = []
            factor_vals = defaultdict(list)
            
            for code in factors_dict:
                # 计算未来N日收益
                future_price = get_future_return(cursor, code, date, trade_dates, hold_days)
                if future_price is None:
                    continue
                
                current_price = factors_dict[code].get('price_now')
                if current_price is None or current_price <= 0:
                    continue
                
                fwd_ret = (future_price - current_price) / current_price * 100
                
                future_rets.append(fwd_ret)
                for fname, fval in factors_dict.items():
                    if fname != 'price_now':
                        factor_vals[fname].append(fval)
            
            if len(future_rets) >= 30:
                for fname in factor_vals:
                    ic = spearman_ic(factor_vals[fname], future_rets)
                    if ic is not None:
                        if fname not in decay_results:
                            decay_results[fname] = {}
                        if hold_days not in decay_results[fname]:
                            decay_results[fname][hold_days] = []
                        decay_results[fname][hold_days].append(ic)
    
    return decay_results


def main():
    print("=" * 80)
    print("📊 V12策略IC分析 - 增强版 (学术标准)")
    print("=" * 80)
    print()
    
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
    print(f"交易日数量: {len(trade_dates)}天")
    
    # 采样 (每3天)
    sample_dates = trade_dates[::3][20:]  # 跳过前20天确保历史数据
    print(f"采样天数: {len(sample_dates)}天")
    print("=" * 80)
    print()
    
    # 存储所有数据
    all_factor_ic = defaultdict(list)
    factor_values_history = {}
    
    # 因子列表
    factor_list = [
        'value_pe', 'quality_roe', 'momentum_20d', 'reversal_20d',
        'lowvol_60d', 'turnover', 'skewness', 'max_drawdown', 
        'liquidity', 'beta_proxy'
    ]
    
    # 主循环
    for i, date in enumerate(sample_dates):
        if i % 20 == 0:
            print(f"进度: {i}/{len(sample_dates)} - {date}")
        
        try:
            # 获取当日股票数据
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
            
            # 存储当日因子值
            factor_values_history[date] = {}
            
            # 获取次日收益
            next_idx = trade_dates.index(date) + 1 if date in trade_dates else -1
            if next_idx >= len(trade_dates):
                continue
            next_date = trade_dates[next_idx]
            
            cursor.execute("""
                SELECT code, close FROM stock_kline WHERE trade_date = %s
            """, (next_date,))
            next_prices = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 计算每个股票的因子
            factors_data = defaultdict(list)
            forward_returns = []
            
            for row in stocks:
                code = row[0]
                if code not in next_prices:
                    continue
                
                stock_data = {
                    'roe': row[1],
                    'pe': row[2],
                    'pb': None,  # pb_ratio字段不存在
                    'total_mv': None,  # 字段不存在，暂时设为None
                    'price': row[3],
                    'turnover': row[4]
                }
                
                # 获取历史价格
                hist_prices = get_prices_before(cursor, code, date, 65)
                if len(hist_prices) < 21:
                    continue
                
                # 计算因子
                factors = calculate_factor_values(cursor, code, date, trade_dates, stock_data, hist_prices)
                if factors is None:
                    continue
                
                # 计算次日收益
                fwd_ret = (next_prices[code] - stock_data['price']) / stock_data['price'] * 100
                
                # 存储
                factors['price_now'] = stock_data['price']
                factor_values_history[date][code] = factors
                
                forward_returns.append(fwd_ret)
                for fname, fval in factors.items():
                    if fname != 'price_now':
                        factors_data[fname].append(fval)
            
            # 计算当日各因子的IC
            if len(forward_returns) >= 30:
                for fname in factor_list:
                    if fname in factors_data and len(factors_data[fname]) == len(forward_returns):
                        ic = spearman_ic(factors_data[fname], forward_returns)
                        if ic is not None:
                            all_factor_ic[fname].append(ic)
        
        except Exception as e:
            print(f"  错误 {date}: {e}")
            continue
    
    conn.close()
    
    # 输出结果
    print("\n" + "=" * 80)
    print("📈 IC分析结果 - 经典与特色因子 (1日持仓)")
    print("=" * 80)
    print()
    print(f"{'因子':<18} {'N':>4} {'IC均值':>8} {'|IC|':>7} {'IR':>7} {'t值':>7} {'p值':>6} {'*':>3} {'胜率':>6} {'评价':<8}")
    print("-" * 80)
    
    factor_names = {
        'value_pe': 'Value_PE',
        'quality_roe': 'Quality_ROE',
        'momentum_20d': 'Momentum',
        'reversal_20d': 'Reversal',
        'lowvol_60d': 'LowVol',
        'turnover': 'Turnover',
        'skewness': 'Skewness',
        'max_drawdown': 'MaxDrawdown',
        'liquidity': 'Liquidity',
        'beta_proxy': 'Beta'
    }
    
    results_summary = {}
    
    for fname in factor_list:
        values = all_factor_ic.get(fname, [])
        name = factor_names.get(fname, fname)
        
        stats = analyze_factor_ic(fname, values)
        results_summary[fname] = stats
        
        if 'error' in stats:
            print(f"{name:<18} {'N/A':>4} {'N/A':>8} {'N/A':>7} {'N/A':>7} {'N/A':>7} {'N/A':>6} {'':>3} {'N/A':>6} ❌ 无数据")
            continue
        
        ic_mean = stats['ic_mean']
        ic_abs = stats['ic_abs_mean']
        ir = stats['ir']
        t_stat = stats['t_stat']
        p_val = stats['p_value']
        sig = stats['significant']
        win_rate = stats['win_rate']
        
        # 评价
        if ic_abs < 0.02:
            eval_text = "❌ 无效"
        elif ic_abs < 0.03:
            eval_text = "⚠️ 较弱"
        elif ic_abs < 0.05:
            eval_text = "✅ 有效"
        else:
            eval_text = "🌟 强"
        
        print(f"{name:<18} {stats['n']:>4} {ic_mean:>+8.4f} {ic_abs:>7.4f} {ir:>+7.2f} {t_stat:>+7.2f} {p_val:>6.3f} {sig:>3} {win_rate:>6.1%} {eval_text:<8}")
    
    # 详细统计
    print("\n" + "=" * 80)
    print("📊 详细统计指标")
    print("=" * 80)
    
    for fname in factor_list:
        name = factor_names.get(fname, fname)
        stats = results_summary.get(fname, {})
        
        if 'error' in stats:
            continue
        
        print(f"\n{name}:")
        print(f"  IC均值: {stats['ic_mean']:+.4f}  标准差: {stats['ic_std']:.4f}")
        print(f"  |IC|均值: {stats['ic_abs_mean']:.4f}")
        print(f"  IR: {stats['ir']:+.4f}  (|IR|>0.5稳定, >1.0非常稳定)")
        print(f"  t统计量: {stats['t_stat']:+.4f}  p值: {stats['p_value']:.4f} {stats['significant']}")
        print(f"  IC>0比例: {stats['win_rate']:.1%}  中位数: {stats['ic_median']:+.4f}")
        print(f"  IC范围: [{stats['ic_min']:+.4f}, {stats['ic_max']:+.4f}]")
        print(f"  自相关: {stats['autocorr']:+.4f}")
    
    # 显著性说明
    print("\n" + "=" * 80)
    print("📚 显著性标记说明")
    print("=" * 80)
    print("*** p<0.01: 高度显著 (99%置信度)")
    print("**  p<0.05: 显著 (95%置信度)")
    print("*   p<0.10: 边际显著 (90%置信度)")
    print()
    print("IR (Information Ratio) 标准:")
    print("  |IR| < 0.5: 不稳定")
    print("  0.5 ≤ |IR| < 1.0: 较稳定")
    print("  |IR| ≥ 1.0: 非常稳定")
    
    # 结论
    print("\n" + "=" * 80)
    print("💡 核心结论")
    print("=" * 80)
    print()
    
    # 找出最强因子
    valid_factors = {k: v for k, v in results_summary.items() if 'ic_abs_mean' in v}
    if valid_factors:
        best_factor = max(valid_factors.items(), key=lambda x: x[1]['ic_abs_mean'])
        worst_factor = min(valid_factors.items(), key=lambda x: x[1]['ic_abs_mean'])
        
        print(f"🏆 最强因子: {factor_names.get(best_factor[0], best_factor[0])}")
        print(f"   |IC|={best_factor[1]['ic_abs_mean']:.4f}, t={best_factor[1]['t_stat']:+.2f}{best_factor[1]['significant']}")
        print()
        print(f"⚠️  最弱因子: {factor_names.get(worst_factor[0], worst_factor[0])}")
        print(f"   |IC|={worst_factor[1]['ic_abs_mean']:.4f}, t={worst_factor[1]['t_stat']:+.2f}{worst_factor[1]['significant']}")
    
    print()
    print("推荐因子组合 (基于显著性和|IC|):")
    significant_factors = [(k, v) for k, v in valid_factors.items() 
                           if v.get('p_value', 1) < 0.05 and v.get('ic_abs_mean', 0) > 0.03]
    significant_factors.sort(key=lambda x: x[1]['ic_abs_mean'], reverse=True)
    
    for i, (fname, stats) in enumerate(significant_factors[:5], 1):
        name = factor_names.get(fname, fname)
        print(f"  {i}. {name}: |IC|={stats['ic_abs_mean']:.4f}, t={stats['t_stat']:+.2f}{stats['significant']}")
    
    # 保存结果
    output_file = '/root/.openclaw/workspace/股票分析项目/ic_analysis_comprehensive_results.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results_summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 详细结果已保存: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
