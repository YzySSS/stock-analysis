#!/usr/bin/env python3
"""
V12_MarketAdaptive 回测引擎
===========================
支持市场状态识别和动态因子权重
包含完整的价格记录
"""

import pandas as pd
import numpy as np
import pymysql
import json
import os
from datetime import datetime, timedelta
from v12_strategy_v12_market_adaptive import V12MarketAdaptiveStrategy, MarketStateClassifier

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}

# 成本模型
COMMISSION_RATE = 0.0003  # 佣金万3
STAMP_TAX_RATE = 0.0005   # 印花税千0.5
SLIPPAGE_RATE = 0.002     # 滑点千2


def run_backtest(start_date, end_date, run_id_prefix='V12_MA'):
    """运行回测"""
    
    print("="*70)
    print(f"V12_MarketAdaptive 回测: {start_date} ~ {end_date}")
    print("="*70)
    
    # 1. 连接数据库
    conn = pymysql.connect(**DB_CONFIG)
    
    # 2. 加载股票列表
    print("\n[1/4] 加载股票列表...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT code FROM stock_basic 
        WHERE is_etf = 0 AND is_st = 0 AND is_delisted = 0
    """)
    stock_codes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    print(f"  有效股票数: {len(stock_codes)}")
    
    # 3. 加载数据（扩大时间范围用于计算因子）
    print("\n[2/4] 加载市场数据...")
    
    # 计算数据开始日期（提前3个月用于因子计算）
    data_start = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y-%m-%d')
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT code, trade_date, close, turnover, amount
        FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        AND amount >= 500000
        ORDER BY code, trade_date
    """, (data_start, end_date))
    
    rows = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(rows, columns=['code', 'trade_date', 'close', 'turnover', 'amount'])
    
    # 过滤ETF和指数
    df = df[df['code'].isin(stock_codes)]
    df = df[~df['code'].str.startswith(('399', '899', 'sh', 'sz'))]
    
    # 数据类型转换
    for col in ['close', 'turnover', 'amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    print(f"  K线记录数: {len(df):,}")
    
    # 4. 获取交易日列表
    trading_days = sorted(df[df['trade_date'] >= start_date]['trade_date'].unique())
    selection_dates = trading_days[::3]  # 每3天选股一次
    print(f"  选股日数量: {len(selection_dates)}")
    
    # 5. 初始化策略
    strategy = V12MarketAdaptiveStrategy(score_threshold=45, max_positions=5)
    classifier = MarketStateClassifier()
    
    # 6. 运行回测
    print("\n[3/4] 运行回测...")
    
    trades = []
    market_state_stats = {'bull': 0, 'bear': 0, 'oscillation': 0}
    
    for i, sel_date in enumerate(selection_dates):
        date_str = sel_date.strftime('%Y-%m-%d')
        
        # 获取市场状态
        market_state = classifier.get_market_state(conn, date_str)
        market_state_stats[market_state] += 1
        
        # 筛选当日数据
        day_df = df[df['trade_date'] <= sel_date]
        
        # 遍历每只股票
        for code in day_df['code'].unique():
            stock_df = day_df[day_df['code'] == code].sort_values('trade_date')
            
            # 获取历史数据
            hist = stock_df.tail(70)
            if len(hist) < 20:
                continue
            
            # 计算因子
            factors = strategy.calculate_factors(hist)
            score, weights = strategy.calculate_weighted_score(factors, market_state)
            
            if score >= 45:
                # 获取买入价格（选股日收盘价）
                entry_price = hist['close'].iloc[-1]
                
                # 查找3日后的卖出价格
                future_df = df[(df['code'] == code) & (df['trade_date'] > sel_date)]
                if len(future_df) >= 3:
                    exit_price = future_df.iloc[2]['close']
                    
                    # 计算收益
                    gross_return = (exit_price - entry_price) / entry_price
                    
                    # 扣除成本
                    commission = COMMISSION_RATE * 2  # 买卖各一次
                    stamp_tax = STAMP_TAX_RATE  # 卖出时
                    slippage = SLIPPAGE_RATE * 2  # 买卖滑点
                    total_cost = commission + stamp_tax + slippage
                    
                    net_return = gross_return - total_cost
                    
                    trades.append({
                        'date': date_str,
                        'code': code,
                        'market_state': market_state,
                        'score': score,
                        'factors': factors,
                        'weights': weights,
                        'entry_price': float(entry_price),
                        'exit_price': float(exit_price),
                        'gross_return': float(gross_return),
                        'net_return': float(net_return),
                        'cost': float(total_cost)
                    })
        
        if (i + 1) % 10 == 0:
            print(f"    已处理 {i+1}/{len(selection_dates)} 个选股日, 累计候选 {len(trades)} 笔")
    
    print(f"\n  完成! 总候选交易: {len(trades)} 笔")
    
    # 7. 处理结果
    if not trades:
        print("❌ 无交易记录")
        conn.close()
        return
    
    df_trades = pd.DataFrame(trades)
    
    # 每天只取前5名
    df_trades = df_trades.sort_values('score', ascending=False).groupby('date').head(5).reset_index(drop=True)
    
    # 统计
    win_trades = len(df_trades[df_trades['net_return'] > 0])
    total_trades = len(df_trades)
    win_rate = win_trades / total_trades
    
    # 按市场状态统计
    state_stats = df_trades.groupby('market_state').agg({
        'net_return': ['count', 'sum', 'mean'],
        'code': 'count'
    }).round(4)
    
    print("\n" + "="*70)
    print("回测结果汇总")
    print("="*70)
    print(f"总交易次数: {total_trades}")
    print(f"盈利次数: {win_trades}")
    print(f"胜率: {win_rate*100:.2f}%")
    print(f"累计收益: {df_trades['net_return'].sum()*100:.2f}%")
    print(f"平均收益: {df_trades['net_return'].mean()*100:.3f}%")
    
    print("\n市场状态分布:")
    for state, count in market_state_stats.items():
        print(f"  {state}: {count}天 ({count/sum(market_state_stats.values())*100:.1f}%)")
    
    print("\n按市场状态表现:")
    for state in df_trades['market_state'].unique():
        state_df = df_trades[df_trades['market_state'] == state]
        state_win = len(state_df[state_df['net_return'] > 0])
        print(f"  {state}: 交易{len(state_df)}次, 胜率{state_win/len(state_df)*100:.1f}%, 收益{state_df['net_return'].sum()*100:.1f}%")
    
    # 8. 存入数据库
    print("\n[4/4] 存入数据库...")
    
    run_id = f'{run_id_prefix}_{start_date.replace("-", "")}_{end_date.replace("-", "")}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    
    cursor = conn.cursor()
    
    # 插入汇总
    cursor.execute('''
        INSERT INTO backtest_summary 
        (run_id, strategy_version, start_date, end_date, initial_capital,
         total_trades, win_trades, win_rate, total_return, avg_net_return, 
         factor_weights, remark, max_drawdown, sharpe_ratio)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        run_id, 'V12_MarketAdaptive', start_date, end_date, 1000000,
        total_trades, win_trades, win_rate,
        float(df_trades['net_return'].sum()),
        float(df_trades['net_return'].mean()),
        json.dumps(V12MarketAdaptiveStrategy.FACTOR_WEIGHTS),
        f'V12_MarketAdaptive 回测 市场状态动态调整',
        0.0, 0.0
    ))
    
    # 插入交易明细
    for _, row in df_trades.iterrows():
        # 计算卖出日期
        exit_date = (datetime.strptime(row['date'], '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            INSERT INTO backtest_trades 
            (run_id, strategy_version, code, select_date, select_score, factor_scores,
             entry_date, entry_price, exit_date, exit_price, 
             gross_return, net_return, total_cost, exit_reason, hold_days, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            run_id, 'V12_MarketAdaptive', row['code'], row['date'], row['score'],
            json.dumps({
                'factors': row['factors'],
                'weights': row['weights'],
                'market_state': row['market_state']
            }),
            row['date'], row['entry_price'], exit_date, row['exit_price'],
            row['gross_return'], row['net_return'], row['cost'],
            'time_exit', 3, f"market_state:{row['market_state']}"
        ))
    
    conn.commit()
    conn.close()
    
    # 保存CSV
    os.makedirs('backtest_results/v12_ma', exist_ok=True)
    df_trades.to_csv(f'backtest_results/v12_ma/trades_{run_id}.csv', index=False)
    
    print(f"✅ 已存入数据库 (run_id: {run_id})")
    print(f"✅ CSV已保存")
    
    return run_id, df_trades


if __name__ == '__main__':
    # 运行2024年Q1测试
    run_backtest('2024-01-01', '2024-03-31')
