#!/usr/bin/env python3
"""
V12策略 V8-快速评估版
====================
批量查询优化版本
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime
from collections import defaultdict
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


def run_quick_backtest(hold_days=1, start_date='2025-01-01', end_date='2025-03-31'):
    """快速回测 - 批量查询优化"""
    
    logger.info(f"快速回测: {start_date} ~ {end_date}, 持有{hold_days}天")
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 1. 获取所有交易日
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (start_date, end_date))
    days = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    logger.info(f"交易日: {len(days)}天")
    
    # 2. 批量获取所有价格数据
    logger.info("加载价格数据...")
    cursor.execute("""
        SELECT code, trade_date, open, close, turnover, pct_change 
        FROM stock_kline
        WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 30 DAY) AND %s
    """, (start_date, end_date))
    
    price_data = defaultdict(dict)
    for row in cursor.fetchall():
        code, date, open_p, close, turnover, pct = row
        price_data[code][date.strftime('%Y-%m-%d')] = {
            'open': float(open_p) if open_p else None,
            'close': float(close) if close else None,
            'turnover': float(turnover) if turnover else 0,
            'pct': float(pct) if pct else 0
        }
    
    # 3. 获取基本面数据
    logger.info("加载基本面数据...")
    cursor.execute("SELECT code, pe_score, roe_score FROM stock_basic WHERE pe_score IS NOT NULL AND roe_score IS NOT NULL")
    fundamental = {row[0]: {'pe': float(row[1]), 'roe': float(row[2])} for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    logger.info(f"股票数: {len(price_data)}, 有基本面数据: {len(fundamental)}")
    
    # 4. 选股并计算收益
    trades = []
    recent_picks = {}
    
    for i, date in enumerate(days):
        if i < 5:  # 跳过前几天
            continue
        
        if i % 10 == 0:
            logger.info(f"进度: {i}/{len(days)}")
        
        # 选股
        candidates = []
        for code, prices in price_data.items():
            if date not in prices:
                continue
            
            today = prices[date]
            if today['open'] is None or today['open'] < 5 or today['open'] > 150:
                continue
            if today['turnover'] < 0.5:
                continue
            if code not in fundamental:
                continue
            
            # 冷却期检查
            if code in recent_picks:
                last_date = datetime.strptime(recent_picks[code], '%Y-%m-%d')
                curr_date = datetime.strptime(date, '%Y-%m-%d')
                if (curr_date - last_date).days <= 5:
                    continue
            
            candidates.append({
                'code': code,
                'price': today['open'],
                'quality': fundamental[code]['roe'],
                'valuation': fundamental[code]['pe']
            })
        
        if len(candidates) < 10:
            continue
        
        # 简单评分（Z-score简化版）
        for key in ['quality', 'valuation']:
            values = [c[key] for c in candidates]
            mean, std = np.mean(values), np.std(values)
            for c in candidates:
                c[f'{key}_z'] = (c[key] - mean) / std if std > 0 else 0
        
        for c in candidates:
            c['score'] = 50 + (c['quality_z'] * 0.5 + c['valuation_z'] * 0.5) * 15
        
        candidates.sort(key=lambda x: x['score'], reverse=True)
        picks = [c for c in candidates if c['score'] >= 50][:5]
        
        # 计算收益
        for pick in picks:
            code = pick['code']
            entry_price = pick['price']
            
            # 找退出日
            future_days = days[i+1:i+hold_days+2]
            if len(future_days) < hold_days + 1:
                continue
            
            if hold_days == 1:
                exit_date = future_days[0]
                exit_price = price_data[code].get(exit_date, {}).get('open')
            else:
                exit_date = future_days[hold_days-1]
                exit_price = price_data[code].get(exit_date, {}).get('close')
            
            if exit_price is None:
                continue
            
            gross_return = (exit_price - entry_price) / entry_price
            cost = 0.0005 * 2 + 0.001 + 0.001 * 2  # 佣金+印花税+滑点
            net_return = gross_return - cost
            
            trades.append({
                'entry_date': date,
                'exit_date': exit_date,
                'code': code,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'gross_return': gross_return,
                'net_return': net_return,
                'score': pick['score']
            })
            
            recent_picks[code] = date
    
    # 统计
    if not trades:
        logger.warning("无交易记录")
        return {}
    
    net_returns = [t['net_return'] for t in trades]
    total_trades = len(trades)
    win_trades = sum(1 for r in net_returns if r > 0)
    win_rate = win_trades / total_trades
    avg_net = np.mean(net_returns)
    
    # 累计收益
    cum = 1.0
    for r in net_returns:
        cum *= (1 + r)
    total_return = cum - 1
    
    logger.info("=" * 50)
    logger.info(f"持有{hold_days}天 回测结果")
    logger.info("=" * 50)
    logger.info(f"总交易: {total_trades} | 胜率: {win_rate*100:.2f}%")
    logger.info(f"平均收益: {avg_net*100:.2f}% | 累计: {total_return*100:.2f}%")
    
    return {
        'hold_days': hold_days,
        'total_trades': total_trades,
        'win_rate': round(win_rate, 4),
        'avg_return': round(avg_net, 6),
        'total_return': round(total_return, 4),
        'trades': trades
    }


def main():
    results = {}
    
    for hold_days in [1, 3, 5]:
        result = run_quick_backtest(hold_days=hold_days)
        results[f'{hold_days}d'] = result
        
        output_file = f'v12_v8_fast_{hold_days}d.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n结果已保存: {output_file}\n")
    
    # 汇总
    logger.info("\n" + "=" * 60)
    logger.info("V8策略 收益率汇总")
    logger.info("=" * 60)
    for key, r in results.items():
        if r:
            logger.info(f"{key}: 胜率{r['win_rate']*100:.1f}% | 平均{r['avg_return']*100:.2f}% | 累计{r['total_return']*100:.2f}%")


if __name__ == '__main__':
    main()
