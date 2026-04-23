#!/usr/bin/env python3
"""
V11_IC_Optimized 快速回测 (2024年Q1)
====================================
简化版本，减少查询次数
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
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
    'charset': 'utf8mb4'
}


def get_all_data(start_date, end_date):
    """一次性获取所有数据"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    sql = """
    SELECT code, trade_date, open, close, turnover, amount
    FROM stock_kline 
    WHERE trade_date BETWEEN %s AND %s
    AND amount >= 500000
    ORDER BY code, trade_date
    """
    cursor.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows)
    for col in ['open', 'close', 'turnover', 'amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    conn.close()
    return df


def calculate_factors(group):
    """计算因子"""
    if len(group) < 20:
        return None
    
    # Turnover因子 (低换手)
    avg_turnover = group['turnover'].tail(20).mean()
    turnover_score = max(0, min(100, 100 - (avg_turnover - 2) * 5))
    
    # LowVol因子 (低波动)
    returns = group['close'].pct_change().dropna()
    if len(returns) >= 20:
        volatility = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
        lowvol_score = max(0, min(100, 100 - volatility * 200))
    else:
        lowvol_score = 50
    
    # Reversal因子 (超跌)
    price_now = group['close'].iloc[-1]
    price_20d = group['close'].iloc[-21] if len(group) >= 21 else group['close'].iloc[0]
    ret_20d = (price_now - price_20d) / price_20d
    reversal_score = max(0, min(100, 50 - ret_20d * 150))
    
    # 加权总分
    total = turnover_score * 0.35 + lowvol_score * 0.35 + reversal_score * 0.30
    
    return {
        'code': group['code'].iloc[0],
        'date': group['trade_date'].iloc[-1],
        'close': price_now,
        'turnover': turnover_score,
        'lowvol': lowvol_score,
        'reversal': reversal_score,
        'total': total
    }


def run_quick_backtest():
    """快速回测主函数"""
    logger.info("开始快速回测 (2024年Q1)...")
    
    # 获取数据
    df = get_all_data('2024-01-01', '2024-04-30')
    logger.info(f"数据加载完成: {len(df)} 条记录")
    
    # 获取交易日
    trading_days = sorted(df['trade_date'].unique())
    logger.info(f"交易日数量: {len(trading_days)}")
    
    # 每3天选股一次
    selection_days = trading_days[::3]
    
    trades = []
    capital = 1000000
    
    for i, date in enumerate(selection_days[:-1]):
        logger.info(f"\n=== {date} ===")
        
        # 获取当日有数据的所有股票
        day_data = df[df['trade_date'] == date]
        
        candidates = []
        for code in day_data['code'].unique():
            stock_data = df[(df['code'] == code) & (df['trade_date'] <= date)].tail(70)
            if len(stock_data) >= 20:
                result = calculate_factors(stock_data)
                if result and result['total'] >= 45:
                    candidates.append(result)
        
        if not candidates:
            logger.info("无符合条件的股票")
            continue
        
        # 排序选前5
        candidates.sort(key=lambda x: x['total'], reverse=True)
        selected = candidates[:5]
        
        logger.info(f"选中 {len(selected)} 只:")
        for s in selected:
            logger.info(f"  {s['code']}: 总分={s['total']:.1f}")
        
        # 模拟3日后收益
        if i + 1 < len(selection_days):
            exit_date = selection_days[i + 1]
            
            for stock in selected:
                code = stock['code']
                entry_price = stock['close']
                
                # 查找3日后的价格
                exit_data = df[(df['code'] == code) & (df['trade_date'] == exit_date)]
                if len(exit_data) > 0:
                    exit_price = exit_data['close'].iloc[0]
                    ret = (exit_price - entry_price) / entry_price
                    
                    # 扣除成本
                    cost = 0.0003 + 0.0005 + 0.002  # 佣金+印花税+滑点
                    net_ret = ret - cost
                    
                    trades.append({
                        'entry_date': date,
                        'exit_date': exit_date,
                        'code': code,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'return': ret,
                        'net_return': net_ret
                    })
                    
                    logger.info(f"  -> {exit_date} 收益: {net_ret*100:.2f}%")
    
    # 统计结果
    if trades:
        df_trades = pd.DataFrame(trades)
        win_trades = len(df_trades[df_trades['net_return'] > 0])
        total_trades = len(trades)
        win_rate = win_trades / total_trades
        avg_return = df_trades['net_return'].mean()
        
        logger.info("\n" + "="*60)
        logger.info("回测结果汇总 (2024年Q1)")
        logger.info("="*60)
        logger.info(f"总交易次数: {total_trades}")
        logger.info(f"盈利次数: {win_trades}")
        logger.info(f"亏损次数: {total_trades - win_trades}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_return*100:.3f}%")
        logger.info(f"累计收益: {df_trades['net_return'].sum()*100:.2f}%")
        
        # 保存结果
        os.makedirs('backtest_results/v11_ic', exist_ok=True)
        df_trades.to_csv('backtest_results/v11_ic/trades_q1.csv', index=False)
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'trades': trades
        }
    
    return None


if __name__ == '__main__':
    result = run_quick_backtest()
