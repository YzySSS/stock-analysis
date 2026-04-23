#!/usr/bin/env python3
"""
V11_IC_Regime_V1 回测脚本
==========================
测试第一阶段的改进效果
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from typing import List, Dict
from collections import defaultdict
import pymysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/.openclaw/workspace/股票分析项目/logs/v11_regime_v1_backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class V11RegimeV1Backtest:
    """V11改进版回测引擎"""
    
    def __init__(self, initial_capital: float = 1000000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = {}
        self.cooling = {}
        self.trades = []
        self.daily_regime = []  # 记录每日市场状态
        
    def connect_db(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
    
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        sql = """
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        self.cursor.execute(sql, (start_date, end_date))
        return [str(row['trade_date']) for row in self.cursor.fetchall()]
    
    def get_index_ma(self, date: str, ma_days: int) -> float:
        """获取上证指数MA值"""
        sql = """
        SELECT AVG(close) as ma FROM (
            SELECT close FROM stock_kline 
            WHERE code = '000001' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
        ) t
        """
        self.cursor.execute(sql, (date, ma_days))
        row = self.cursor.fetchone()
        return float(row['ma']) if row and row.get('ma') else None
    
    def is_bull_market(self, date: str) -> bool:
        """判断市场状态"""
        # 获取当日收盘价
        self.cursor.execute(
            "SELECT close FROM stock_kline WHERE code = '000001' AND trade_date = %s",
            (date,)
        )
        row = self.cursor.fetchone()
        if not row or not row.get('close'):
            return False
        
        current_price = float(row['close'])
        ma200 = self.get_index_ma(date, 200)
        ma20 = self.get_index_ma(date, 20)
        ma60 = self.get_index_ma(date, 60)
        
        if ma200 is None or ma20 is None or ma60 is None:
            return False
        
        return (current_price > ma200) and (ma20 > ma60)
    
    def get_params(self, is_bull: bool) -> Dict:
        """根据市场状态获取参数"""
        if is_bull:
            return {
                'score_threshold': 55,
                'max_positions': 5,
                'stop_loss': -0.08,
                'hold_days': 3,
                'cooling_days': 3,
                'weights': {'turnover': 0.35, 'lowvol': 0.35, 'reversal': 0.30}
            }
        else:
            return {
                'score_threshold': 60,  # 提高门槛
                'max_positions': 2,     # 减少持仓
                'stop_loss': -0.05,     # 收紧止损
                'hold_days': 3,
                'cooling_days': 5,      # 延长冷却
                'weights': {'turnover': 0.35, 'lowvol': 0.35, 'reversal': 0.30}
            }
    
    def select_stocks(self, date: str, params: Dict) -> List[Dict]:
        """选股"""
        # 获取候选股票
        sql = """
        SELECT k.code, k.close as price, k.turnover, k.amount,
               b.name, b.industry
        FROM stock_kline k
        LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
        WHERE k.trade_date = %s
        AND k.amount >= 500000
        AND b.is_delisted = 0 AND b.is_st = 0
        AND k.close BETWEEN 5 AND 200
        AND k.turnover >= 0.5
        """
        self.cursor.execute(sql, (date,))
        stocks = self.cursor.fetchall()
        
        if len(stocks) < 10:
            return []
        
        # 计算因子
        results = []
        for stock in stocks:
            code = stock['code']
            
            # 跳过冷却期股票
            if code in self.cooling and self.cooling[code] >= date:
                continue
            
            # 获取历史数据
            self.cursor.execute("""
                SELECT close, turnover FROM stock_kline 
                WHERE code = %s AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 25
            """, (code, date))
            history = self.cursor.fetchall()
            
            if len(history) < 20:
                continue
            
            closes = [float(row['close']) for row in history if row['close']]
            if len(closes) < 20:
                continue
            
            # 计算因子值
            turnover = float(stock['turnover'])
            vol_20 = np.std(closes[:20])
            ret_20 = (closes[0] - closes[19]) / closes[19] if closes[19] > 0 else 0
            
            results.append({
                'code': code,
                'name': stock['name'],
                'price': float(stock['price']),
                'turnover': turnover,
                'vol_20': vol_20,
                'ret_20': ret_20
            })
        
        if len(results) < 10:
            return []
        
        # 排名赋分
        df = pd.DataFrame(results)
        df['turnover_score'] = df['turnover'].rank(pct=True) * 100
        df['lowvol_score'] = (1 - df['vol_20'].rank(pct=True)) * 100
        df['reversal_score'] = (1 - df['ret_20'].rank(pct=True)) * 100
        
        # 加权总分
        w = params['weights']
        df['total_score'] = (
            df['turnover_score'] * w['turnover'] +
            df['lowvol_score'] * w['lowvol'] +
            df['reversal_score'] * w['reversal']
        )
        
        # 筛选达标股票
        picks = df[df['total_score'] >= params['score_threshold']].nlargest(
            params['max_positions'], 'total_score'
        )
        
        return picks.to_dict('records')
    
    def run_backtest(self, start_date: str, end_date: str, run_id: str):
        """运行回测"""
        logger.info("=" * 70)
        logger.info(f"V11_IC_Regime_V1 回测 [{run_id}]")
        logger.info(f"回测区间: {start_date} 至 {end_date}")
        logger.info("=" * 70)
        
        self.connect_db()
        trading_days = self.get_trading_days(start_date, end_date)
        
        bull_days = 0
        bear_days = 0
        
        for i, date in enumerate(trading_days):
            if i % 50 == 0:
                logger.info(f"进度: {i}/{len(trading_days)} ({i/len(trading_days)*100:.1f}%)")
            
            # 判断市场状态
            is_bull = self.is_bull_market(date)
            params = self.get_params(is_bull)
            
            if is_bull:
                bull_days += 1
            else:
                bear_days += 1
            
            self.daily_regime.append({
                'date': date,
                'is_bull': is_bull,
                'params': params
            })
            
            # 更新持仓（止损检查）
            exited = []
            for code, pos in self.positions.items():
                self.cursor.execute(
                    "SELECT close FROM stock_kline WHERE code = %s AND trade_date = %s",
                    (code, date)
                )
                row = self.cursor.fetchone()
                
                if not row or not row.get('close'):
                    continue
                
                current_price = float(row['close'])
                ret = (current_price - pos['entry_price']) / pos['entry_price']
                pos['hold_days'] += 1
                
                # 止损或持仓期满
                if ret <= params['stop_loss'] or pos['hold_days'] >= params['hold_days']:
                    reason = 'stop_loss' if ret <= params['stop_loss'] else 'time_exit'
                    exited.append((code, current_price, reason, ret, is_bull))
            
            # 处理退出
            for code, exit_price, reason, ret, regime in exited:
                pos = self.positions[code]
                
                # 计算成本
                cost = 0.0028  # 0.28%
                net_ret = ret - cost
                
                self.trades.append({
                    'entry_date': pos['entry_date'],
                    'exit_date': date,
                    'code': code,
                    'entry_price': pos['entry_price'],
                    'exit_price': exit_price,
                    'gross_return': ret,
                    'net_return': net_ret,
                    'exit_reason': reason,
                    'regime': 'bull' if regime else 'bear',
                    'hold_days': pos['hold_days']
                })
                
                # 加入冷却期
                if params['cooling_days'] > 0:
                    exit_idx = trading_days.index(date)
                    if exit_idx + params['cooling_days'] < len(trading_days):
                        self.cooling[code] = trading_days[exit_idx + params['cooling_days']]
                
                del self.positions[code]
            
            # 选股补充持仓
            if len(self.positions) < params['max_positions']:
                picks = self.select_stocks(date, params)
                
                for pick in picks:
                    code = pick['code']
                    if code not in self.positions and code not in self.cooling:
                        self.positions[code] = {
                            'entry_date': date,
                            'entry_price': pick['price'],
                            'hold_days': 0
                        }
        
        self.close_db()
        
        # 统计结果
        df_trades = pd.DataFrame(self.trades)
        
        results = {
            'run_id': run_id,
            'start_date': start_date,
            'end_date': end_date,
            'total_trades': len(self.trades),
            'bull_days': bull_days,
            'bear_days': bear_days,
            'trades': self.trades
        }
        
        if len(self.trades) > 0:
            returns = df_trades['net_return'].values
            wins = sum(1 for r in returns if r > 0)
            
            results['win_rate'] = wins / len(returns) * 100
            results['avg_return'] = np.mean(returns) * 100
            
            # 累计收益（复利计算）
            cum = 1.0
            for r in returns:
                cum *= (1 + r)
            results['total_return'] = (cum - 1) * 100
            
            # 按市场环境统计
            bull_trades = df_trades[df_trades['regime'] == 'bull']
            bear_trades = df_trades[df_trades['regime'] == 'bear']
            
            if len(bull_trades) > 0:
                results['bull_win_rate'] = sum(bull_trades['net_return'] > 0) / len(bull_trades) * 100
                results['bull_return'] = bull_trades['net_return'].sum() * 100
            
            if len(bear_trades) > 0:
                results['bear_win_rate'] = sum(bear_trades['net_return'] > 0) / len(bear_trades) * 100
                results['bear_return'] = bear_trades['net_return'].sum() * 100
        
        return results


def save_results(results, year):
    """保存回测结果到文件"""
    import os
    
    output_dir = '/root/.openclaw/workspace/股票分析项目/backtest_results'
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存JSON摘要
    summary = {k: v for k, v in results.items() if k != 'trades'}
    json_path = f"{output_dir}/V13_RegimeV1_{year}_summary.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"结果已保存: {json_path}")
    
    # 保存CSV交易明细
    if 'trades' in results and len(results['trades']) > 0:
        df = pd.DataFrame(results['trades'])
        csv_path = f"{output_dir}/V13_RegimeV1_{year}_trades.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"交易明细已保存: {csv_path}")

def main():
    """主函数"""
    backtest = V11RegimeV1Backtest()
    
    # 测试2024年
    results_2024 = backtest.run_backtest('2024-01-01', '2024-12-31', 'V13_RegimeV1_2024')
    
    print("\n" + "=" * 70)
    print("📊 V13_Regime_V1 2024年回测结果")
    print("=" * 70)
    print(f"总交易: {results_2024['total_trades']} 笔")
    print(f"牛市天数: {results_2024['bull_days']}")
    print(f"震荡/熊市天数: {results_2024['bear_days']}")
    if 'win_rate' in results_2024:
        print(f"胜率: {results_2024['win_rate']:.1f}%")
        print(f"累计收益: {results_2024['total_return']:.2f}%")
        if 'bull_win_rate' in results_2024:
            print(f"牛市胜率: {results_2024['bull_win_rate']:.1f}% | 收益: {results_2024['bull_return']:.2f}%")
        if 'bear_win_rate' in results_2024:
            print(f"震荡市胜率: {results_2024['bear_win_rate']:.1f}% | 收益: {results_2024['bear_return']:.2f}%")
    print("=" * 70)
    
    # 保存2024结果
    save_results(results_2024, '2024')
    
    # 测试2025年
    backtest = V11RegimeV1Backtest()  # 重置
    results_2025 = backtest.run_backtest('2025-01-01', '2025-12-31', 'V13_RegimeV1_2025')
    
    print("\n" + "=" * 70)
    print("📊 V13_Regime_V1 2025年回测结果")
    print("=" * 70)
    print(f"总交易: {results_2025['total_trades']} 笔")
    print(f"牛市天数: {results_2025['bull_days']}")
    print(f"震荡/熊市天数: {results_2025['bear_days']}")
    if 'win_rate' in results_2025:
        print(f"胜率: {results_2025['win_rate']:.1f}%")
        print(f"累计收益: {results_2025['total_return']:.2f}%")
        if 'bull_win_rate' in results_2025:
            print(f"牛市胜率: {results_2025['bull_win_rate']:.1f}% | 收益: {results_2025['bull_return']:.2f}%")
        if 'bear_win_rate' in results_2025:
            print(f"震荡市胜率: {results_2025['bear_win_rate']:.1f}% | 收益: {results_2025['bear_return']:.2f}%")
    print("=" * 70)
    
    # 保存2025结果
    save_results(results_2025, '2025')


if __name__ == '__main__':
    main()
