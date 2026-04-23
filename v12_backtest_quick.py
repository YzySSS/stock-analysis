#!/usr/bin/env python3
"""
V12策略30天回测 - 快速版
============================
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import json
import logging
import pymysql
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

from config import DB_CONFIG


def get_trade_dates(days=30):
    """获取最近N个交易日"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM stock_kline 
                WHERE trade_date <= CURDATE()
                ORDER BY trade_date DESC 
                LIMIT %s
            """, (days,))
            dates = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
            dates.reverse()
            return dates
    finally:
        conn.close()


def run_backtest_for_date(date, threshold=20):
    """单日回测"""
    conn = pymysql.connect(**DB_CONFIG)
    results = []
    
    try:
        with conn.cursor() as cursor:
            # 获取前一日
            cursor.execute("SELECT MAX(trade_date) FROM stock_kline WHERE trade_date < %s", (date,))
            prev_date = cursor.fetchone()[0]
            if not prev_date:
                return []
            
            # 获取ST/退市列表
            cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
            exclude = {row[0] for row in cursor.fetchall()}
            
            # 获取当天数据
            cursor.execute("""
                SELECT k.code, k.open, k.close, k.turnover, k_prev.pct_change
                FROM stock_kline k
                LEFT JOIN stock_kline k_prev ON k.code = k_prev.code AND k_prev.trade_date = %s
                WHERE k.trade_date = %s AND k.open > 0 AND k.turnover >= 1
            """, (prev_date, date))
            
            for row in cursor.fetchall():
                code = row[0]
                if code in exclude:
                    continue
                
                price = float(row[1])
                close = float(row[2])
                prev_change = float(row[4]) if row[4] else 0
                
                if price < 10 or price > 150:
                    continue
                
                # 获取历史价格（不含当日）
                cursor2 = conn.cursor()
                cursor2.execute("""
                    SELECT close FROM stock_kline 
                    WHERE code = %s AND trade_date < %s
                    ORDER BY trade_date DESC LIMIT 65
                """, (code, date))
                prices = [float(r[0]) for r in cursor2.fetchall()]
                prices.reverse()
                cursor2.close()
                
                if len(prices) < 21:
                    continue
                
                # MA20（不含当日）
                ma20 = sum(prices[-21:-1]) / 20
                if price < ma20 * 0.95:
                    continue
                
                # 评分
                factors = {}
                
                # 趋势
                trend = 12.5
                if len(prices) >= 61:
                    ma60 = sum(prices[-61:-1]) / 60
                    if ma20 > ma60:
                        trend += 12.5
                if price >= ma20:
                    trend += 5
                factors['trend'] = min(25, trend)
                
                # 动量（不含当日）
                momentum = 12.5
                ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
                if 0 <= ret_20d <= 40:
                    momentum += 12.5
                factors['momentum'] = min(25, momentum)
                
                # 质量
                quality = 10
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 for i in range(-20, 0)]
                vol = np.std(returns) if returns else 0
                if vol < 3:
                    quality += 10
                elif vol < 5:
                    quality += 5
                factors['quality'] = min(20, quality)
                
                # 情绪（前一日涨跌）
                sentiment = 10
                if prev_change >= 0:
                    sentiment += 10
                factors['sentiment'] = min(20, sentiment)
                
                factors['value'] = 5
                
                weights = {'trend': 0.25, 'momentum': 0.25, 'quality': 0.20, 
                          'sentiment': 0.20, 'value': 0.10}
                total = sum(factors[k] * weights[k] for k in factors)
                
                if total >= threshold:
                    ret = (close - price) / price * 100
                    results.append({
                        'code': code,
                        'score': round(total, 1),
                        'buy': round(price, 2),
                        'sell': round(close, 2),
                        'return': round(ret, 2),
                        'factors': factors
                    })
    finally:
        conn.close()
    
    # 排序取前3
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:3]


def main():
    dates = get_trade_dates(30)
    logger.info(f"回测区间: {dates[0]} 至 {dates[-1]} ({len(dates)}个交易日)")
    
    all_results = []
    
    for date in dates:
        picks = run_backtest_for_date(date, threshold=20)
        if picks:
            logger.info(f"【{date}】选中 {len(picks)} 只")
            for i, p in enumerate(picks):
                logger.info(f"  #{i+1} {p['code']} {p['score']:.1f}分 收益:{p['return']:+.2f}%")
                all_results.append({
                    'date': date,
                    'rank': i + 1,
                    **p
                })
        else:
            logger.info(f"【{date}】未选出股票")
    
    # 保存结果
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv('v12_backtest_30d.csv', index=False, encoding='utf-8-sig')
        
        returns = [r['return'] for r in all_results]
        wins = len([r for r in returns if r > 0])
        
        summary = {
            'period': f"{dates[0]} 至 {dates[-1]}",
            'total_trades': len(all_results),
            'win_rate': round(wins / len(returns) * 100, 1),
            'avg_return': round(sum(returns) / len(returns), 2),
            'max_return': round(max(returns), 2),
            'min_return': round(min(returns), 2)
        }
        
        with open('v12_backtest_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        print("\\n" + "="*60)
        print("📊 V12策略30天回测结果")
        print("="*60)
        print(f"总交易: {summary['total_trades']}笔")
        print(f"胜率: {summary['win_rate']}%")
        print(f"平均收益: {summary['avg_return']}%")
        print(f"最高: {summary['max_return']}% | 最低: {summary['min_return']}%")
        print("="*60)
    else:
        print("\\n⚠️ 30天内没有选出任何股票")
        print("可能原因: 市场环境差，或阈值设置过高")


if __name__ == '__main__':
    main()
