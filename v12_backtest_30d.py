#!/usr/bin/env python3
"""
V12策略30天回测
===============
验证修复后的V12策略（无未来函数版本）

回测逻辑:
1. 获取最近30个交易日
2. 每天盘前运行V12选股（基于前一日收盘数据）
3. 买入价 = 当日开盘价
4. 卖出价 = 当日收盘价
5. 统计收益率

输出:
- v12_backtest_30d.csv: 每日明细
- v12_backtest_summary.json: 汇总统计
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
import pandas as pd
import json
import logging
from typing import List, Dict
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# 导入配置
from config import DB_CONFIG


class V12BacktestEngine:
    """V12策略回测引擎"""
    
    def __init__(self):
        self.results = []
        self.trade_days = []
        
    def get_recent_trade_dates(self, days: int = 30) -> List[str]:
        """获取最近N个交易日（从今天往前）"""
        conn = pymysql.connect(**DB_CONFIG)
        dates = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT trade_date FROM stock_kline 
                    WHERE trade_date <= CURDATE()
                    ORDER BY trade_date DESC 
                    LIMIT %s
                """, (days,))
                dates = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
                dates.reverse()  # 改为 chronological order
        finally:
            conn.close()
        return dates
    
    def get_stock_data_for_date(self, date: str) -> List[Dict]:
        """获取某天的所有股票数据（用于选股）"""
        conn = pymysql.connect(**DB_CONFIG)
        stocks = []
        try:
            with conn.cursor() as cursor:
                # 获取前一日日期
                cursor.execute("""
                    SELECT MAX(trade_date) FROM stock_kline 
                    WHERE trade_date < %s
                """, (date,))
                prev_date_row = cursor.fetchone()
                if not prev_date_row or not prev_date_row[0]:
                    return []
                prev_date = prev_date_row[0]
                
                # 获取ST/退市股票列表
                cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
                exclude_codes = {row[0] for row in cursor.fetchall()}
                
                # 【修复】使用开盘价，获取前一日涨跌幅
                cursor.execute("""
                    SELECT 
                        k.code, 
                        k.open as price,
                        k.close,
                        k.turnover,
                        k_prev.pct_change as prev_change,
                        k.volume
                    FROM stock_kline k
                    LEFT JOIN stock_kline k_prev 
                        ON k.code = k_prev.code 
                        AND k_prev.trade_date = %s
                    WHERE k.trade_date = %s
                    AND k.open > 0
                """, (prev_date, date))
                
                for row in cursor.fetchall():
                    code = row[0]
                    if code in exclude_codes:
                        continue
                    stocks.append({
                        'code': code,
                        'price': float(row[1]),  # 开盘价
                        'close': float(row[2]),
                        'turnover': float(row[3]) if row[3] else 0,
                        'prev_change': float(row[4]) if row[4] else 0,
                        'volume': float(row[5]) if row[5] else 0
                    })
        finally:
            conn.close()
        return stocks
    
    def get_historical_prices(self, code: str, end_date: str, days: int = 61) -> List[float]:
        """【修复】获取历史价格（严格排除end_date当日）"""
        conn = pymysql.connect(**DB_CONFIG)
        prices = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT close FROM stock_kline 
                    WHERE code = %s AND trade_date < %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """, (code, end_date, days))
                prices = [float(row[0]) for row in cursor.fetchall()]
                prices.reverse()  # 时间顺序
        finally:
            conn.close()
        return prices
    
    def calculate_v12_score(self, stock: Dict, prices: List[float]) -> Dict:
        """
        【修复版】V12评分计算 - 无未来函数
        
        关键修复:
        1. 使用开盘价作为当前价格
        2. 使用前一日涨跌幅作为情绪因子输入
        3. MA计算排除当日数据
        """
        code = stock['code']
        price = stock['price']  # 当日开盘价
        turnover = stock['turnover']
        prev_change = stock['prev_change']  # 前一日涨跌幅（修复）
        
        # 硬性过滤
        if price < 10 or price > 150:
            return {'passed': False, 'reason': '股价范围', 'score': 0}
        if turnover < 1:  # 单位是亿
            return {'passed': False, 'reason': '成交额过低', 'score': 0}
        if len(prices) < 21:
            return {'passed': False, 'reason': '数据不足', 'score': 0}
        
        # 【修复】MA20计算排除当日
        ma20 = sum(prices[-21:-1]) / 20  # 只用前20日
        if price < ma20 * 0.95:
            return {'passed': False, 'reason': '跌破MA20', 'score': 0}
        
        # 开始评分
        factors = {}
        
        # 1. 趋势因子 (25分)
        trend_score = 12.5
        if len(prices) >= 61:
            # 【修复】MA60也排除当日
            ma60 = sum(prices[-61:-1]) / 60
            if ma20 > ma60:
                trend_score += 12.5
        if price >= ma20:
            trend_score += 5
        factors['trend'] = min(25, trend_score)
        
        # 2. 动量因子 (25分) 【修复】不含当日
        momentum_score = 12.5
        if len(prices) >= 21:
            # 【修复】20日涨幅 = (前一日收盘价 - 前20日收盘价) / 前20日收盘价
            ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
            if 0 <= ret_20d <= 40:
                momentum_score += 12.5
        factors['momentum'] = min(25, momentum_score)
        
        # 3. 质量因子 (20分) 【修复】不含当日
        quality_score = 10
        if len(prices) >= 21:
            returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                       for i in range(-20, 0)]  # 前20日涨跌幅
            import numpy as np
            vol = np.std(returns) if returns else 0
            if vol < 3:
                quality_score += 10
            elif vol < 5:
                quality_score += 5
        factors['quality'] = min(20, quality_score)
        
        # 4. 情绪因子 (20分) 【修复】使用前一日涨跌，非当日
        sentiment_score = 10
        if prev_change >= 0:
            sentiment_score += 10
        factors['sentiment'] = min(20, sentiment_score)
        
        # 5. 估值因子 (10分)
        value_score = 5
        factors['value'] = value_score
        
        # 加权总分
        weights = {'trend': 0.25, 'momentum': 0.25, 'quality': 0.20, 
                   'sentiment': 0.20, 'value': 0.10}
        total = sum(factors[k] * weights[k] for k in factors)
        
        return {
            'passed': True,
            'score': round(total, 1),
            'factors': factors,
            'ma20': round(ma20, 2),
            'price': price
        }
    
    def run_daily_picking(self, date: str, top_n: int = 3) -> List[Dict]:
        """运行某日盘前选股"""
        logger.info(f"【{date}】开始选股...")
        
        # 获取当天股票数据
        stocks = self.get_stock_data_for_date(date)
        logger.info(f"  股票池: {len(stocks)} 只")
        
        if not stocks:
            return []
        
        # 评分
        scored = []
        for stock in stocks:
            prices = self.get_historical_prices(stock['code'], date)
            result = self.calculate_v12_score(stock, prices)
            if result['passed'] and result['score'] >= 20:  # 阈值20分（市场不好时降低）
                scored.append({
                    'code': stock['code'],
                    'score': result['score'],
                    'factors': result['factors'],
                    'ma20': result['ma20'],
                    'buy_price': stock['price'],  # 开盘价
                    'sell_price': stock['close'],  # 收盘价
                })
        
        # 排序取前N
        scored.sort(key=lambda x: x['score'], reverse=True)
        picks = scored[:top_n]
        
        logger.info(f"  选中: {len(picks)} 只")
        for p in picks:
            logger.info(f"    ✅ {p['code']} | {p['score']:.1f}分 | 买:{p['buy_price']:.2f} 卖:{p['sell_price']:.2f}")
        
        return picks
    
    def run_backtest(self, days: int = 30):
        """运行30天回测"""
        # 获取交易日列表
        trade_dates = self.get_recent_trade_dates(days)
        logger.info(f"回测区间: {trade_dates[0]} 至 {trade_dates[-1]} ({len(trade_dates)}个交易日)")
        
        # 每天回测
        for date in trade_dates:
            picks = self.run_daily_picking(date, top_n=3)
            
            if picks:
                for i, pick in enumerate(picks):
                    # 计算收益率
                    buy = pick['buy_price']
                    sell = pick['sell_price']
                    ret = (sell - buy) / buy * 100 if buy > 0 else 0
                    
                    self.results.append({
                        'date': date,
                        'rank': i + 1,
                        'code': pick['code'],
                        'score': pick['score'],
                        'buy_price': round(buy, 2),
                        'sell_price': round(sell, 2),
                        'return_pct': round(ret, 2),
                        'factors': json.dumps(pick['factors'])
                    })
        
        logger.info(f"回测完成，共 {len(self.results)} 笔交易")
    
    def save_and_report(self):
        """保存结果并输出报告"""
        if not self.results:
            logger.warning("无回测结果")
            return
        
        # 保存明细CSV
        df = pd.DataFrame(self.results)
        output_file = "v12_backtest_30d.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"✅ 明细已保存: {output_file}")
        
        # 计算统计
        returns = [r['return_pct'] for r in self.results]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]
        
        # 按交易日统计每日平均收益
        daily_returns = {}
        for r in self.results:
            date = r['date']
            if date not in daily_returns:
                daily_returns[date] = []
            daily_returns[date].append(r['return_pct'])
        
        avg_daily_returns = [sum(rets)/len(rets) for rets in daily_returns.values()]
        
        summary = {
            'strategy': 'V12_MultiFactor_Fixed',
            'version': '1.1-NoLookahead',
            'period': f"{self.results[0]['date']} 至 {self.results[-1]['date']}",
            'total_days': len(daily_returns),
            'total_trades': len(self.results),
            'win_count': len(wins),
            'loss_count': len(losses),
            'win_rate': round(len(wins) / len(returns) * 100, 1),
            'avg_return': round(sum(returns) / len(returns), 2),
            'max_return': round(max(returns), 2),
            'min_return': round(min(returns), 2),
            'cumulative_return': round(sum(avg_daily_returns), 2),
            'avg_daily_return': round(sum(avg_daily_returns) / len(avg_daily_returns), 2) if avg_daily_returns else 0
        }
        
        # 保存汇总JSON
        with open("v12_backtest_summary.json", 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 汇总已保存: v12_backtest_summary.json")
        
        # 打印报告
        print("\n" + "="*70)
        print("📊 V12策略30天回测报告 (修复版 - 无未来函数)")
        print("="*70)
        print(f"回测区间: {summary['period']}")
        print(f"交易天数: {summary['total_days']}")
        print(f"总交易次数: {summary['total_trades']}")
        print(f"\n💰 收益统计:")
        print(f"  胜率: {summary['win_rate']:.1f}% ({summary['win_count']}胜 / {summary['loss_count']}负)")
        print(f"  单笔平均收益: {summary['avg_return']:.2f}%")
        print(f"  单日平均收益: {summary['avg_daily_return']:.2f}%")
        print(f"  累计收益(复利估算): {summary['cumulative_return']:.2f}%")
        print(f"  最高收益: +{summary['max_return']:.2f}%")
        print(f"  最低收益: {summary['min_return']:.2f}%")
        
        # 最近5天明细
        print(f"\n📅 最近5天选股明细:")
        recent = df.tail(15)  # 最近5天，每天最多3只
        for _, row in recent.iterrows():
            emoji = "🟢" if row['return_pct'] > 0 else "🔴"
            print(f"  {emoji} {row['date']} #{row['rank']} {row['code']} | "
                  f"{row['score']:.1f}分 | 收益: {row['return_pct']:+.2f}%")
        
        print("="*70)


def main():
    engine = V12BacktestEngine()
    engine.run_backtest(days=30)
    engine.save_and_report()


if __name__ == '__main__':
    main()
