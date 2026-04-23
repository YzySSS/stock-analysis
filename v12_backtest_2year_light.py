#!/usr/bin/env python3
"""
V12策略 2年回测 - 轻量版
========================
优化: 每5天采样一次，大幅加速
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import pymysql
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4'
}


def get_trading_days(start_date: str, end_date: str) -> list:
    """获取交易日列表"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (start_date, end_date))
    days = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return days


def get_stock_data_for_date(date: str) -> pd.DataFrame:
    """获取某日的股票数据"""
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取前一日
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(trade_date) FROM stock_kline WHERE trade_date < %s", (date,))
    prev_date = cursor.fetchone()[0]
    cursor.close()
    
    sql = """
    SELECT 
        k.code, k.close, k.turnover, k_prev.pct_change as prev_change,
        b.roe, b.pe_fixed, b.pb_ratio, b.name
    FROM stock_kline k
    LEFT JOIN stock_kline k_prev ON k.code = k_prev.code AND k_prev.trade_date = %s
    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
    WHERE k.trade_date = %s
    AND k.amount >= 500000
    AND b.is_delisted = 0 AND b.is_st = 0
    AND k.close BETWEEN 5 AND 200
    """
    
    df = pd.read_sql(sql, conn, params=(prev_date, date))
    conn.close()
    
    for col in ['close', 'turnover', 'prev_change', 'roe', 'pe_fixed', 'pb_ratio']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def get_historical_prices(code: str, date: str, days: int = 30) -> list:
    """获取历史价格"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT close FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT %s
    """, (code, date, days))
    prices = [float(row[0]) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    prices.reverse()
    return prices


def calculate_factors(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """计算因子"""
    results = []
    
    for _, row in df.iterrows():
        code = row['code']
        price = row['close']
        
        # 获取历史价格
        prices = get_historical_prices(code, date, 30)
        
        if len(prices) < 20:
            continue
        
        # MA20过滤
        ma20 = np.mean(prices[-20:])
        if price < ma20 * 0.90:
            continue
        
        # 趋势因子 (MA20斜率)
        if len(prices) >= 25:
            ma20_now = np.mean(prices[-20:])
            ma20_prev = np.mean(prices[-25:-5])
            trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100 if ma20_prev > 0 else 0
        else:
            trend = 0
        
        # 动量因子
        momentum = (price - prices[-20]) / prices[-20] * 100 if len(prices) >= 20 else 0
        
        # 质量因子
        roe = row['roe'] if pd.notna(row['roe']) else 0
        
        # 估值因子
        pe = row['pe_fixed'] if pd.notna(row['pe_fixed']) else 50
        
        # 情绪因子
        sentiment = row['prev_change'] if pd.notna(row['prev_change']) else 0
        
        results.append({
            'code': code,
            'name': row['name'],
            'price': price,
            'trend': trend,
            'momentum': momentum,
            'roe': roe,
            'pe': pe,
            'sentiment': sentiment
        })
    
    return pd.DataFrame(results)


def zscore_and_score(df: pd.DataFrame) -> pd.DataFrame:
    """Z-score标准化并计算得分"""
    if len(df) < 10:
        return df
    
    # Z-score
    for col in ['trend', 'momentum', 'roe', 'pe', 'sentiment']:
        mean = df[col].mean()
        std = df[col].std()
        if std > 0:
            df[f'{col}_z'] = ((df[col] - mean) / std).clip(-3, 3)
        else:
            df[f'{col}_z'] = 0
    
    # 加权得分
    weights = {'trend_z': 0.25, 'momentum_z': 0.15, 'roe_z': 0.20, 'sentiment_z': 0.20, 'pe_z': 0.20}
    df['score'] = 50 + sum(df[k] * v for k, v in weights.items()) * 15
    df['score'] = df['score'].clip(0, 100)
    
    return df


def run_backtest(start_date: str, end_date: str, threshold: int = 55):
    """运行回测"""
    logger.info("=" * 70)
    logger.info("V12策略 2年回测 - 轻量版 (每5日采样)")
    logger.info("=" * 70)
    
    # 获取交易日
    all_days = get_trading_days(start_date, end_date)
    trading_days = all_days[::5]  # 每5天采样一次
    logger.info(f"交易日: {len(all_days)}天，采样: {len(trading_days)}天")
    
    # 回测
    trades = []
    
    for i, date in enumerate(trading_days[:-1]):
        if i % 10 == 0:
            logger.info(f"进度: {i}/{len(trading_days)} ({i/len(trading_days)*100:.1f}%) - {date}")
        
        # 选股
        df = get_stock_data_for_date(date)
        if len(df) < 50:
            continue
        
        df = calculate_factors(df, date)
        if len(df) < 10:
            continue
        
        df = zscore_and_score(df)
        picks = df[df['score'] >= threshold].nlargest(5, 'score')
        
        if len(picks) == 0:
            continue
        
        # 次日收益
        next_date = trading_days[i + 1]
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for _, pick in picks.iterrows():
            cursor.execute("""
                SELECT close FROM stock_kline 
                WHERE code = %s AND trade_date = %s
            """, (pick['code'], next_date))
            row = cursor.fetchone()
            
            if row:
                exit_price = float(row[0])
                gross = (exit_price - pick['price']) / pick['price'] * 100
                net = gross - 0.28  # 成本
                
                trades.append({
                    'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)[:10],
                    'code': pick['code'],
                    'name': pick['name'],
                    'score': round(pick['score'], 1),
                    'entry': round(pick['price'], 2),
                    'exit': round(exit_price, 2),
                    'return': round(net, 2)
                })
        
        cursor.close()
        conn.close()
    
    return pd.DataFrame(trades)


def generate_report(df: pd.DataFrame) -> dict:
    """生成报告"""
    if len(df) == 0:
        return {}
    
    returns = df['return'].values
    wins = np.sum(returns > 0)
    
    # 复利
    cum = 1.0
    for r in returns:
        cum *= (1 + r / 100)
    
    trade_days = df['date'].nunique()
    years = trade_days / 252
    
    return {
        'version': 'V12_2Year_Light',
        'trades': len(df),
        'win_rate': round(wins / len(returns) * 100, 1),
        'avg_return': round(np.mean(returns), 2),
        'cumulative': round((cum - 1) * 100, 2),
        'annualized': round(((cum ** (1/max(years, 0.1))) - 1) * 100, 2),
        'max_return': round(np.max(returns), 2),
        'min_return': round(np.min(returns), 2)
    }


def main():
    df_trades = run_backtest('2024-01-01', '2025-12-31')
    
    if len(df_trades) > 0:
        report = generate_report(df_trades)
        
        # 保存
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_trades.to_csv(f'/root/.openclaw/workspace/股票分析项目/v12_2year_light_{ts}_trades.csv', index=False)
        
        with open(f'/root/.openclaw/workspace/股票分析项目/v12_2year_light_{ts}_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print("\n" + "=" * 70)
        print("📊 V12策略 2年回测报告 (轻量版)")
        print("=" * 70)
        for k, v in report.items():
            print(f"  {k}: {v}")
        print("=" * 70)
    else:
        logger.error("无交易记录")


if __name__ == '__main__':
    main()
