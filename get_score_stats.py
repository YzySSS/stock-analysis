#!/usr/bin/env python3
import warnings
warnings.filterwarnings('ignore')

import jqdatasdk as jq
import numpy as np
from datetime import datetime, timedelta
import json

jq.auth('13929962527', 'Zy20001026')

test_dates = ['2025-01-15', '2025-02-17', '2025-03-17', '2025-04-16', '2025-05-15',
              '2025-06-16', '2025-07-16', '2025-08-15', '2025-09-16', '2025-10-16', 
              '2025-11-17', '2025-12-15']

stock_pool = ['000001.XSHE', '002594.XSHE', '300750.XSHE', '600519.XSHG', '601318.XSHG',
              '000938.XSHE', '002230.XSHE', '601012.XSHG', '000858.XSHE', '601888.XSHG']

all_scores = []

for date_str in test_dates:
    try:
        df_day = jq.get_price(stock_pool, count=1, end_date=date_str, frequency='daily')
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        df_prev = jq.get_price(stock_pool, count=1, end_date=prev_date, frequency='daily')
        
        for code in stock_pool:
            if code in df_day.index and code in df_prev.index:
                open_p = df_day.loc[code]['open']
                prev_c = df_prev.loc[code]['close']
                change = (open_p - prev_c) / prev_c * 100 if prev_c > 0 else 0
                
                score = 50
                if change > 3: score += 15
                elif change > 1: score += 8
                elif change > 0: score += 3
                if abs(change) < 3: score += 10
                elif abs(change) < 5: score += 5
                
                all_scores.append(score)
    except:
        pass

if all_scores:
    arr = np.array(all_scores)
    print('='*60)
    print('📊 2025年评分统计（按月抽样）')
    print('='*60)
    print(f'样本数量: {len(all_scores)}')
    print(f'平均评分: {np.mean(arr):.2f}')
    print(f'中位数: {np.median(arr):.2f}')
    print(f'标准差: {np.std(arr):.2f}')
    print(f'最低: {np.min(arr):.2f}, 最高: {np.max(arr):.2f}')
    print()
    print('分位数:')
    for p in [60, 65, 70, 75, 80, 85, 90]:
        print(f'  {p}分位: {np.percentile(arr, p):.1f}分')
    print()
    print('选中比例:')
    for th in [60, 65, 70]:
        pct = np.mean(arr >= th) * 100
        print(f'  ≥{th}分: {pct:.1f}%')
    print('='*60)
    
    # 保存
    stats = {
        '样本数': len(all_scores),
        '平均': float(np.mean(arr)),
        '中位数': float(np.median(arr)),
        '标准差': float(np.std(arr)),
        '分位数': {str(p): float(np.percentile(arr, p)) for p in [60,65,70,75,80,85,90]},
        '选中比例': {'60分': float(np.mean(arr>=60)*100), '65分': float(np.mean(arr>=65)*100), '70分': float(np.mean(arr>=70)*100)}
    }
    with open('score_stats_2025.json', 'w') as f:
        json.dump(stats, f, indent=2)
    print('✅ 已保存: score_stats_2025.json')
