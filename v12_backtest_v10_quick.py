#!/usr/bin/env python3
"""
V12回测引擎 V10 - 快速版
======================
优化内存使用，快速验证策略逻辑
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class V10QuickBacktest:
    """V10快速回测 - 简化版"""
    
    def __init__(self):
        # 配置
        self.initial_capital = 1000000
        self.capital = self.initial_capital
        self.score_threshold = 60.0
        self.max_positions = 5
        self.hold_days = 10
        self.stop_loss = -0.08
        
        # 因子权重
        self.weights = {'quality': 0.3, 'value': 0.3, 'reversal': 0.25, 'lowvol': 0.15}
        
        # 成本
        self.commission = 0.0003
        self.stamp_tax = 0.0005
        self.slippage = 0.002
        
        # 状态
        self.positions = []
        self.trades = []
        self.daily_values = []
        self.recent_picks = {}
        
    def get_price_history(self, code, end_date, days=65):
        """获取价格历史"""
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT close FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
        """, (code, end_date, days))
        prices = [float(row[0]) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return list(reversed(prices))
    
    def calculate_factors_for_stock(self, code, date, industry, roe, pe, price, turnover):
        """计算单只股票因子"""
        prices = self.get_price_history(code, date)
        if len(prices) < 21:
            return None
        
        # Quality: ROE或价格稳定性
        if roe is not None:
            quality = roe
        else:
            returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
            quality = 50 - np.std(returns) * 100
        
        # Value: -PE
        value = -(pe * 10) if pe and pe > 0 else 0
        
        # Reversal: -20日收益
        ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
        reversal = -ret_20d
        
        # LowVol: -波动率
        if len(prices) >= 61:
            returns_60d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-59, 0)]
            lowvol = -np.std(returns_60d) * 100
        elif len(prices) >= 21:
            returns_20d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-19, 0)]
            lowvol = -np.std(returns_20d) * 100
        else:
            return None
        
        return {
            'code': code, 'industry': industry, 'price': price, 'turnover': turnover,
            'quality': quality, 'value': value, 'reversal': reversal, 'lowvol': lowvol
        }
    
    def run_backtest(self, start_date, end_date):
        """运行简化回测"""
        logger.info(f"V10快速回测: {start_date} ~ {end_date}")
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 获取交易日
        cursor.execute("""
            SELECT DISTINCT trade_date FROM stock_kline 
            WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date
        """, (start_date, end_date))
        trading_days = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
        logger.info(f"交易日: {len(trading_days)}天")
        
        # 简化：每周交易一次（降低频率）
        trade_days = trading_days[::5]  # 每5天交易一次
        logger.info(f"实际交易次数: {len(trade_days)}次")
        
        for i, date in enumerate(trade_days):
            if i < 5:
                continue
            
            logger.info(f"进度: {i+1}/{len(trade_days)} - {date}")
            
            # 获取前一日
            cursor.execute("SELECT MAX(trade_date) FROM stock_kline WHERE trade_date < %s", (date,))
            prev_date = cursor.fetchone()[0]
            
            # 获取股票数据
            cursor.execute("""
                SELECT b.code, b.industry, b.roe_clean, b.pe_fixed, k.open, k.turnover
                FROM stock_basic b
                JOIN stock_kline k ON b.code = k.code COLLATE utf8mb4_unicode_ci
                WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
                AND k.open BETWEEN 5 AND 150 AND k.turnover >= 1.0
            """, (date,))
            
            stocks = []
            for row in cursor.fetchall():
                code, industry, roe, pe, price, turnover = row
                factors = self.calculate_factors_for_stock(
                    code, date, industry or '其他', 
                    float(roe) if roe else None,
                    float(pe) if pe else None,
                    float(price), float(turnover)
                )
                if factors:
                    stocks.append(factors)
            
            if len(stocks) < 10:
                continue
            
            # 行业中性化（简化版：行业内排名）
            industries = defaultdict(list)
            for s in stocks:
                industries[s['industry']].append(s)
            
            all_scores = []
            for industry, industry_stocks in industries.items():
                if len(industry_stocks) < 3:
                    continue
                
                for factor in ['quality', 'value', 'reversal', 'lowvol']:
                    values = [s[factor] for s in industry_stocks]
                    mean, std = np.mean(values), np.std(values)
                    for s in industry_stocks:
                        s[f'{factor}_z'] = (s[factor] - mean) / std if std > 0 else 0
                        s[f'{factor}_score'] = 50 + max(-3, min(3, s[f'{factor}_z'])) * 15
                
                for s in industry_stocks:
                    s['total_score'] = (
                        s['quality_score'] * self.weights['quality'] +
                        s['value_score'] * self.weights['value'] +
                        s['reversal_score'] * self.weights['reversal'] +
                        s['lowvol_score'] * self.weights['lowvol']
                    )
                    all_scores.append(s)
            
            # 选股
            all_scores.sort(key=lambda x: x['total_score'], reverse=True)
            picks = [s for s in all_scores if s['total_score'] >= self.score_threshold][:self.max_positions]
            
            logger.info(f"  候选: {len(all_scores)}, 选中: {len(picks)}")
            
            # 模拟交易（简化）
            if picks:
                # 每只分配资金
                capital_per = self.capital * 0.9 / len(picks)
                
                for pick in picks:
                    # 模拟10天后收益
                    future_prices = self.get_price_history(pick['code'], date, days=15)
                    if len(future_prices) >= 11:
                        entry = future_prices[0]
                        exit_p = future_prices[10]
                        
                        # 检查止损
                        min_price = min(future_prices[:11])
                        stop_price = entry * (1 + self.stop_loss)
                        
                        if min_price <= stop_price:
                            exit_p = stop_price
                            exit_reason = 'stop_loss'
                        else:
                            exit_reason = 'time_exit'
                        
                        # 计算收益
                        gross_return = (exit_p - entry) / entry
                        
                        # 扣除成本
                        cost = self.commission * 2 + self.stamp_tax + self.slippage * 2
                        net_return = gross_return - cost
                        
                        shares = capital_per / entry
                        pnl = shares * entry * net_return
                        
                        self.capital += pnl
                        
                        self.trades.append({
                            'date': date, 'code': pick['code'], 
                            'return': net_return, 'pnl': pnl,
                            'reason': exit_reason,
                            'score': pick['total_score']
                        })
            
            # 记录每日市值
            self.daily_values.append({
                'date': date, 'value': self.capital, 'positions': len(picks)
            })
        
        cursor.close()
        conn.close()
        
        return self._generate_report()
    
    def _generate_report(self):
        """生成报告"""
        if not self.trades:
            return {}
        
        total_return = (self.capital - self.initial_capital) / self.initial_capital
        
        win_trades = sum(1 for t in self.trades if t['return'] > 0)
        win_rate = win_trades / len(self.trades)
        avg_return = np.mean([t['return'] for t in self.trades])
        
        stop_loss_count = sum(1 for t in self.trades if t['reason'] == 'stop_loss')
        
        logger.info("=" * 50)
        logger.info("V10快速回测结果")
        logger.info("=" * 50)
        logger.info(f"初始资金: ¥{self.initial_capital:,.0f}")
        logger.info(f"最终资金: ¥{self.capital:,.0f}")
        logger.info(f"总收益: {total_return*100:.2f}%")
        logger.info(f"交易次数: {len(self.trades)}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_return*100:.2f}%")
        logger.info(f"止损次数: {stop_loss_count}")
        logger.info("=" * 50)
        
        return {
            'initial': self.initial_capital,
            'final': self.capital,
            'total_return': total_return,
            'trades': len(self.trades),
            'win_rate': win_rate,
            'avg_return': avg_return,
            'stop_loss_count': stop_loss_count
        }


def main():
    engine = V10QuickBacktest()
    result = engine.run_backtest('2024-01-01', '2026-04-08')
    
    with open('v10_quick_result.json', 'w') as f:
        json.dump(result, f, indent=2)
    
    print(f"\n结果已保存: v10_quick_result.json")


if __name__ == '__main__':
    main()
