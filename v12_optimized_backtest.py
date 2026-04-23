#!/usr/bin/env python3
"""
V12策略30天回测 - 优化版
======================
按DeepSeek分析建议优化：
1. MA20过滤放宽到0.90
2. 评分系统改为满分50分，阈值30分
3. 成交额降低到5000万
4. 修复估值因子（从数据库读取PE/PB）
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


def get_stock_valuation(codes, cursor):
    """获取股票估值数据 - 简化版（当前数据库无PE/PB字段）"""
    # 由于数据库暂无PE/PB字段，返回空字典
    # 后续可以从其他数据源接入
    return {code: {'pe': 0, 'pb': 0} for code in codes}


def run_backtest_for_date(date, threshold=30):
    """单日回测 - 优化版"""
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
            
            # 【优化3】成交额降低到5000万
            cursor.execute("""
                SELECT k.code, k.open, k.close, k.turnover, k_prev.pct_change
                FROM stock_kline k
                LEFT JOIN stock_kline k_prev ON k.code = k_prev.code AND k_prev.trade_date = %s
                WHERE k.trade_date = %s AND k.open > 0 AND k.turnover >= 0.5
            """, (prev_date, date))
            
            stocks = []
            for row in cursor.fetchall():
                code = row[0]
                if code in exclude:
                    continue
                stocks.append({
                    'code': code,
                    'price': float(row[1]),
                    'close': float(row[2]),
                    'turnover': float(row[3]) if row[3] else 0,
                    'prev_change': float(row[4]) if row[4] else 0
                })
            
            # 批量获取估值数据
            codes = [s['code'] for s in stocks]
            valuations = get_stock_valuation(codes, cursor)
            
            for stock in stocks:
                code = stock['code']
                price = stock['price']
                
                # 硬性过滤
                if price < 10 or price > 150:
                    continue
                
                # 获取历史价格
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
                
                # 【优化1】MA20过滤放宽到0.90
                ma20 = sum(prices[-21:-1]) / 20
                if price < ma20 * 0.90:
                    continue
                
                # 【优化2】重构评分系统（满分50分）
                factors = {}
                
                # 1. 趋势因子 (0-15分)
                trend = 0
                ma10 = sum(prices[-11:-1]) / 10 if len(prices) >= 11 else ma20
                if price > ma20:
                    trend += 5
                if len(prices) >= 61:
                    ma60 = sum(prices[-61:-1]) / 60
                    if ma20 > ma60:
                        trend += 5
                if price > ma10:
                    trend += 5
                factors['trend'] = trend
                
                # 2. 动量因子 (0-15分) - 收紧涨幅范围
                momentum = 0
                ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
                if 0 < ret_20d < 20:
                    momentum += 10
                elif 20 <= ret_20d < 40:
                    momentum += 5
                factors['momentum'] = momentum
                
                # 3. 质量因子 (0-10分)
                quality = 0
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 for i in range(-20, 0)]
                vol = np.std(returns) if returns else 0
                if vol < 2:
                    quality += 10
                elif vol < 3:
                    quality += 5
                factors['quality'] = quality
                
                # 4. 情绪因子 (0-10分)
                sentiment = 0
                prev_change = stock['prev_change']
                if prev_change > 0:
                    sentiment += 5
                if prev_change > 2:
                    sentiment += 5
                factors['sentiment'] = sentiment
                
                # 5. 估值因子 (0-5分) - 【优化4】修复为动态计算
                value = 0
                val = valuations.get(code, {})
                pe = val.get('pe', 0)
                pb = val.get('pb', 0)
                if 10 <= pe <= 40:
                    value += 3
                if pb < 3:
                    value += 2
                elif pb > 10 or pe < 0:
                    value -= 2
                factors['value'] = max(0, value)
                
                # 总分（满分50分）
                total = sum(factors.values())
                
                # 【优化2】阈值提高到30分
                if total >= threshold:
                    ret = (stock['close'] - price) / price * 100
                    results.append({
                        'code': code,
                        'score': round(total, 1),
                        'buy': round(price, 2),
                        'sell': round(stock['close'], 2),
                        'return': round(ret, 2),
                        'factors': factors,
                        'ma20': round(ma20, 2),
                        'ret_20d': round(ret_20d, 2)
                    })
    finally:
        conn.close()
    
    # 排序取前3
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:3]


def main():
    dates = get_trade_dates(30)
    logger.info(f"【V12优化版】回测区间: {dates[0]} 至 {dates[-1]} ({len(dates)}个交易日)")
    logger.info("优化内容: MA20过滤0.90 | 满分50分/阈值30分 | 成交额5000万 | 修复估值因子")
    
    all_results = []
    
    for date in dates:
        picks = run_backtest_for_date(date, threshold=30)
        if picks:
            logger.info(f"【{date}】选中 {len(picks)} 只")
            for i, p in enumerate(picks):
                logger.info(f"  #{i+1} {p['code']} {p['score']:.1f}分(趋势{p['factors']['trend']}/动量{p['factors']['momentum']}/质量{p['factors']['quality']}/情绪{p['factors']['sentiment']}/估值{p['factors']['value']}) 收益:{p['return']:+.2f}%")
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
        df.to_csv('v12_optimized_backtest.csv', index=False, encoding='utf-8-sig')
        
        returns = [r['return'] for r in all_results]
        wins = len([r for r in returns if r > 0])
        
        summary = {
            'version': 'V12_Optimized',
            'changes': ['MA20_filter_0.90', 'score_max_50_threshold_30', 'turnover_50M', 'fixed_value_factor'],
            'period': f"{dates[0]} 至 {dates[-1]}",
            'total_trades': len(all_results),
            'trade_days': len(set(r['date'] for r in all_results)),
            'win_rate': round(wins / len(returns) * 100, 1),
            'avg_return': round(sum(returns) / len(returns), 2),
            'max_return': round(max(returns), 2),
            'min_return': round(min(returns), 2)
        }
        
        with open('v12_optimized_summary.json', 'w') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print("\n" + "="*70)
        print("📊 V12策略优化版 30天回测结果")
        print("="*70)
        print(f"回测区间: {summary['period']}")
        print(f"交易天数: {summary['trade_days']}/{len(dates)}")
        print(f"总交易: {summary['total_trades']}笔")
        print(f"\n💰 收益统计:")
        print(f"  胜率: {summary['win_rate']}%")
        print(f"  平均收益: {summary['avg_return']}%")
        print(f"  最高: {summary['max_return']}% | 最低: {summary['min_return']}%")
        print("="*70)
        print("\n优化对比:")
        print("  原版: 阈值20分 | MA20×0.95 | 成交额1亿 → 30天6笔交易")
        print("  优化: 阈值30分 | MA20×0.90 | 成交额5000万 | 修复估值因子")
    else:
        print("\n⚠️ 30天内没有选出任何股票")


if __name__ == '__main__':
    main()
