#!/usr/bin/env python3
"""
V12策略 分年度表现分析
"""
import json
import pandas as pd
from datetime import datetime

# 读取交易数据
with open('/root/.openclaw/workspace/股票分析项目/v12_v6_full_opt_trades.csv', 'r') as f:
    import csv
    trades = list(csv.DictReader(f))

# 按年份分组
yearly_stats = {}

for trade in trades:
    date = trade['entry_date'][:10]
    year = date[:4]
    
    if year not in yearly_stats:
        yearly_stats[year] = {'trades': [], 'returns': []}
    
    yearly_stats[year]['trades'].append(trade)
    yearly_stats[year]['returns'].append(float(trade['net_return']))

# 计算各年度统计
print("=" * 70)
print("📊 V12策略 V6 分年度表现分析")
print("=" * 70)

for year in sorted(yearly_stats.keys()):
    stats = yearly_stats[year]
    returns = stats['returns']
    trades_list = stats['trades']
    
    total_trades = len(returns)
    wins = len([r for r in returns if r > 0])
    win_rate = wins / total_trades * 100 if total_trades > 0 else 0
    avg_return = sum(returns) / len(returns) if returns else 0
    
    # 计算累计收益（对数法）
    import math
    log_returns = [math.log(1 + r/100) for r in returns if r > -100]
    total_log = sum(log_returns)
    cumulative = (math.exp(total_log) - 1) * 100
    
    # 最大回撤
    peak, max_dd = 0, 0
    running_log = 0
    for r in returns:
        if r > -100:
            running_log += math.log(1 + r/100)
            running_pct = (math.exp(running_log) - 1) * 100
            peak = max(peak, running_pct)
            max_dd = max(max_dd, peak - running_pct)
    
    # 止损统计
    stop_losses = len([t for t in trades_list if '止损' in t['exit_reason']])
    
    print(f"\n📅 {year}年:")
    print(f"  交易次数: {total_trades} 笔")
    print(f"  胜率: {win_rate:.1f}%")
    print(f"  平均收益: {avg_return:.2f}%")
    print(f"  累计收益: {cumulative:.2f}%")
    print(f"  最大回撤: {max_dd:.2f}%")
    print(f"  止损次数: {stop_losses} ({stop_losses/total_trades*100:.1f}%)")
    
    # 月度分布
    monthly_counts = {}
    for t in trades_list:
        month = t['entry_date'][:7]
        monthly_counts[month] = monthly_counts.get(month, 0) + 1
    
    print(f"  月均交易: {total_trades/len(monthly_counts):.1f} 笔")

print("\n" + "=" * 70)

# 季度分析
print("\n📈 分季度表现:")
quarterly = {}
for trade in trades:
    date = trade['entry_date'][:10]
    year = date[:4]
    month = int(date[5:7])
    quarter = f"{year}Q{(month-1)//3 + 1}"
    
    if quarter not in quarterly:
        quarterly[quarter] = {'returns': [], 'trades': 0}
    quarterly[quarter]['returns'].append(float(trade['net_return']))
    quarterly[quarter]['trades'] += 1

# 显示各季度
for q in sorted(quarterly.keys()):
    returns = quarterly[q]['returns']
    avg = sum(returns) / len(returns)
    wins = len([r for r in returns if r > 0])
    win_rate = wins / len(returns) * 100
    print(f"  {q}: {quarterly[q]['trades']:2d}笔 | 胜率{win_rate:.0f}% | 平均{avg:+.2f}%")
