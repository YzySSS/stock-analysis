#!/usr/bin/env python3
"""
V12回测引擎 V10 - 极速版
======================
优化性能，快速获取结果
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


def get_price_history_batch(codes, end_date, days=65):
    """批量获取价格历史"""
    if not codes:
        return {}
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    placeholders = ','.join(['%s'] * len(codes))
    cursor.execute(f"""
        SELECT code, close, trade_date FROM stock_kline 
        WHERE code IN ({placeholders}) AND trade_date <= %s
        ORDER BY code, trade_date DESC
    """, tuple(codes) + (end_date,))
    
    result = defaultdict(list)
    for row in cursor.fetchall():
        code, close, date = row
        result[code].append(float(close))
    
    # 只保留最近days天，并反转顺序
    for code in result:
        result[code] = list(reversed(result[code][:days]))
    
    cursor.close()
    conn.close()
    return result


def calculate_factors_batch(stock_data, price_history):
    """批量计算因子"""
    results = []
    
    for stock in stock_data:
        code = stock['code']
        prices = price_history.get(code, [])
        
        if len(prices) < 21:
            continue
        
        # Quality: ROE或价格稳定性
        if stock.get('roe') is not None:
            quality = stock['roe']
        else:
            returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
            quality = 50 - np.std(returns) * 100
        
        # Value: -PE
        pe = stock.get('pe_fixed', 50)
        value = -pe if pe > 0 else -50
        
        # Reversal: -20日收益
        ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
        reversal = -ret_20d
        
        # LowVol: -波动率
        if len(prices) >= 61:
            returns_60d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-60, 0)]
            lowvol = -np.std(returns_60d) * 100
        elif len(prices) >= 21:
            returns_20d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
            lowvol = -np.std(returns_20d) * 100
        else:
            continue
        
        results.append({
            'code': code,
            'name': stock['name'],
            'industry': stock['industry'],
            'price': stock['price'],
            'quality': quality,
            'value': value,
            'reversal': reversal,
            'lowvol': lowvol
        })
    
    return results


def industry_neutralize(stocks):
    """行业中性化"""
    industries = defaultdict(list)
    for s in stocks:
        industries[s['industry']].append(s)
    
    for industry, industry_stocks in industries.items():
        if len(industry_stocks) < 3:
            continue
        
        for factor in ['quality', 'value', 'reversal', 'lowvol']:
            values = [s[factor] for s in industry_stocks]
            mean, std = np.mean(values), np.std(values)
            
            for s in industry_stocks:
                z = (s[factor] - mean) / std if std > 0 else 0
                s[f'{factor}_score'] = 50 + max(-3, min(3, z)) * 15
    
    return stocks


def run_backtest_fast(start_date, end_date):
    """快速回测"""
    logger.info("="*70)
    logger.info("V10极速回测")
    logger.info("="*70)
    
    # 配置
    initial_capital = 1000000
    capital = initial_capital
    weights = {'quality': 0.3, 'value': 0.3, 'reversal': 0.25, 'lowvol': 0.15}
    score_threshold = 60
    max_positions = 5
    hold_days = 10
    stop_loss = -0.08
    commission = 0.0003
    stamp_tax = 0.0005
    slippage = 0.002
    
    trades = []
    daily_values = []
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取交易日（每周一次）
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date
    """, (start_date, end_date))
    all_days = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    trade_days = all_days[::5]  # 每5天交易一次
    
    logger.info(f"交易日: {len(all_days)}天, 实际交易: {len(trade_days)}次")
    
    for i, date in enumerate(trade_days):
        if i < 5:
            continue
        
        logger.info(f"进度: {i+1}/{len(trade_days)} - {date}")
        
        # 获取前一交易日
        cursor.execute("SELECT MAX(trade_date) FROM stock_kline WHERE trade_date < %s", (date,))
        prev_date = cursor.fetchone()[0]
        
        # 获取股票数据
        cursor.execute("""
            SELECT b.code, b.name, b.industry, b.roe_clean, b.pe_fixed, k.open, k.turnover
            FROM stock_basic b
            JOIN stock_kline k ON b.code = k.code COLLATE utf8mb4_unicode_ci
            WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
            AND k.open BETWEEN 5 AND 150 AND k.turnover >= 1.0
        """, (date,))
        
        stock_data = []
        codes = []
        for row in cursor.fetchall():
            stock_data.append({
                'code': row[0], 'name': row[1], 'industry': row[2] or '其他',
                'roe': float(row[3]) if row[3] else None,
                'pe_fixed': float(row[4]) if row[4] else 50,
                'price': float(row[5]), 'turnover': float(row[6])
            })
            codes.append(row[0])
        
        if len(stock_data) < 10:
            continue
        
        # 批量获取价格历史
        price_history = get_price_history_batch(codes, date)
        
        # 批量计算因子
        stocks = calculate_factors_batch(stock_data, price_history)
        if len(stocks) < 10:
            continue
        
        # 行业中性化
        stocks = industry_neutralize(stocks)
        
        # 计算综合得分
        for s in stocks:
            s['total_score'] = (
                s.get('quality_score', 50) * weights['quality'] +
                s.get('value_score', 50) * weights['value'] +
                s.get('reversal_score', 50) * weights['reversal'] +
                s.get('lowvol_score', 50) * weights['lowvol']
            )
        
        # 选股
        stocks.sort(key=lambda x: x['total_score'], reverse=True)
        picks = [s for s in stocks if s['total_score'] >= score_threshold][:max_positions]
        
        logger.info(f"  候选: {len(stocks)}, 选中: {len(picks)}")
        
        # 模拟交易
        if picks:
            capital_per = capital * 0.9 / len(picks)
            
            for pick in picks:
                # 获取未来价格
                future_prices = get_price_history_batch([pick['code']], date, 15).get(pick['code'], [])
                if len(future_prices) >= 11:
                    entry = future_prices[0]
                    
                    # 检查止损
                    min_price = min(future_prices[:11])
                    stop_price = entry * (1 + stop_loss)
                    
                    if min_price <= stop_price:
                        exit_p = stop_price
                        reason = 'stop_loss'
                    else:
                        exit_p = future_prices[10]
                        reason = 'time_exit'
                    
                    # 计算收益
                    gross_return = (exit_p - entry) / entry
                    cost = commission * 2 + stamp_tax + slippage * 2
                    net_return = gross_return - cost
                    
                    shares = capital_per / entry
                    pnl = shares * entry * net_return
                    capital += pnl
                    
                    trades.append({
                        'date': date, 'code': pick['code'], 'return': net_return,
                        'pnl': pnl, 'reason': reason, 'score': pick['total_score']
                    })
        
        daily_values.append({'date': date, 'value': capital, 'positions': len(picks)})
    
    cursor.close()
    conn.close()
    
    # 生成报告
    if trades:
        total_return = (capital - initial_capital) / initial_capital
        win_trades = sum(1 for t in trades if t['return'] > 0)
        win_rate = win_trades / len(trades)
        avg_return = np.mean([t['return'] for t in trades])
        stop_loss_count = sum(1 for t in trades if t['reason'] == 'stop_loss')
        
        logger.info("="*70)
        logger.info("V10极速回测结果")
        logger.info("="*70)
        logger.info(f"初始资金: ¥{initial_capital:,.0f}")
        logger.info(f"最终资金: ¥{capital:,.0f}")
        logger.info(f"总收益: {total_return*100:.2f}%")
        logger.info(f"交易次数: {len(trades)}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_return*100:.2f}%")
        logger.info(f"止损次数: {stop_loss_count}")
        logger.info("="*70)
        
        return {
            'initial': initial_capital, 'final': capital,
            'total_return': total_return, 'trades': len(trades),
            'win_rate': win_rate, 'avg_return': avg_return,
            'stop_loss_count': stop_loss_count
        }
    
    return {}


if __name__ == '__main__':
    result = run_backtest_fast('2024-01-01', '2026-04-08')
    
    if result:
        with open('v10_fast_result.json', 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n✅ 结果已保存: v10_fast_result.json")
