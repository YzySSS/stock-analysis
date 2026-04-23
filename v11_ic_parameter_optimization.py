#!/usr/bin/env python3
"""
V11_IC_Optimized 参数优化
==========================
网格搜索 + 交叉验证
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from itertools import product
import pymysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/.openclaw/workspace/股票分析项目/logs/v11_param_opt.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}

# 参数网格
PARAM_GRID = {
    'score_threshold': [50, 55, 60, 65],
    'stop_loss': [-0.05, -0.08, -0.10],
    'hold_days': [2, 3, 4],
    'cooling_days': [2, 3, 5],
    'max_positions': [3, 5, 7]
}

# 时间段划分
PERIODS = {
    'train': ('2024-01-01', '2024-06-30'),    # 训练集
    'valid': ('2024-07-01', '2024-12-31'),    # 验证集
    'test': ('2025-01-01', '2025-12-31'),      # 测试集
    'full': ('2024-01-01', '2025-12-31')       # 完整回测
}


class V11BacktestEngine:
    """V11_IC_Optimized 回测引擎"""
    
    def __init__(self, params):
        self.params = params
        self.trades = []
        self.positions = {}  # code -> {entry_date, entry_price, hold_days}
        self.cooling = {}    # code -> end_date
        
    def connect_db(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def get_trading_days(self, start_date, end_date):
        """获取交易日"""
        sql = """
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        self.cursor.execute(sql, (start_date, end_date))
        return [str(row['trade_date']) for row in self.cursor.fetchall()]
    
    def calculate_factors(self, date):
        """计算当日所有股票因子"""
        # 获取候选股票
        sql = """
        SELECT k.code, k.close as price, k.turnover, k.amount,
               b.name, b.industry
        FROM stock_kline k
        LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
        WHERE k.trade_date = %s
        AND k.amount >= %s
        AND b.is_delisted = 0 AND b.is_st = 0
        AND k.close BETWEEN 5 AND 200
        """
        self.cursor.execute(sql, (date, self.params['min_turnover_amount'] * 10000))
        stocks = self.cursor.fetchall()
        
        if not stocks:
            return []
        
        results = []
        for stock in stocks:
            code = stock['code']
            
            # 跳过冷却期股票
            if code in self.cooling and self.cooling[code] >= date:
                continue
            
            # 获取历史数据计算因子
            sql = """
            SELECT close, turnover FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 25
            """
            self.cursor.execute(sql, (code, date))
            history = self.cursor.fetchall()
            
            closes = [float(row['close']) for row in history if row['close'] is not None]
            turnovers = [float(row['turnover']) for row in history if row['turnover'] is not None]
            
            if len(closes) < 20 or len(turnovers) < 20:
                continue
            
            # LowVol: 20日波动率（标准差）
            vol_20 = np.std(closes[:20])
            
            # Reversal: 20日收益
            ret_20 = (closes[0] - closes[19]) / closes[19] if closes[19] > 0 else 0
            
            # Turnover: 当日换手率
            turnover = float(stock['turnover'])
            
            results.append({
                'code': code,
                'name': stock['name'],
                'industry': stock['industry'],
                'price': float(stock['price']),
                'turnover': turnover,
                'vol_20': vol_20,
                'ret_20': ret_20
            })
        
        return results
    
    def rank_score(self, stocks):
        """排名赋分"""
        if len(stocks) < 10:
            return []
        
        df = pd.DataFrame(stocks)
        
        # 排名赋分（高排名=高分）
        df['turnover_score'] = df['turnover'].rank(pct=True) * 100
        df['lowvol_score'] = (1 - df['vol_20'].rank(pct=True)) * 100  # 低波动=高分
        df['reversal_score'] = (1 - df['ret_20'].rank(pct=True)) * 100  # 超跌=高分
        
        # 加权总分
        df['total_score'] = (
            df['turnover_score'] * 0.35 +
            df['lowvol_score'] * 0.35 +
            df['reversal_score'] * 0.30
        )
        
        return df.to_dict('records')
    
    def run_backtest(self, start_date, end_date, period_name):
        """运行回测"""
        logger.info(f"[{period_name}] 回测: {start_date} 至 {end_date}")
        logger.info(f"参数: {self.params}")
        
        self.connect_db()
        trading_days = self.get_trading_days(start_date, end_date)
        
        self.trades = []
        self.positions = {}
        self.cooling = {}
        
        for i, date in enumerate(trading_days):
            if i % 50 == 0:
                logger.info(f"  进度: {i}/{len(trading_days)}")
            
            # 更新持仓（检查止损和持仓天数）
            exited = []
            for code, pos in self.positions.items():
                # 获取当日价格
                sql = "SELECT close FROM stock_kline WHERE code = %s AND trade_date = %s"
                self.cursor.execute(sql, (code, date))
                row = self.cursor.fetchone()
                
                if not row:
                    continue
                
                current_price = float(row['close'])
                ret = (current_price - pos['entry_price']) / pos['entry_price']
                
                # 止损检查
                if ret <= self.params['stop_loss']:
                    exited.append((code, current_price, 'stop_loss', ret))
                    continue
                
                # 持仓天数检查
                pos['hold_days'] += 1
                if pos['hold_days'] >= self.params['hold_days']:
                    exited.append((code, current_price, 'time_exit', ret))
            
            # 处理退出
            for code, exit_price, reason, ret in exited:
                pos = self.positions[code]
                
                # 计算成本
                commission = pos['entry_price'] * 0.0003 + exit_price * 0.0003
                stamp_tax = exit_price * 0.0005
                slippage = (pos['entry_price'] + exit_price) * 0.001
                total_cost = commission + stamp_tax + slippage
                
                net_ret = ret - total_cost / pos['entry_price']
                
                self.trades.append({
                    'entry_date': pos['entry_date'],
                    'exit_date': date,
                    'code': code,
                    'name': pos['name'],
                    'entry_price': pos['entry_price'],
                    'exit_price': exit_price,
                    'gross_return': ret,
                    'net_return': net_ret,
                    'exit_reason': reason,
                    'hold_days': pos['hold_days']
                })
                
                # 加入冷却期
                if self.params['cooling_days'] > 0:
                    exit_idx = trading_days.index(date)
                    if exit_idx + self.params['cooling_days'] < len(trading_days):
                        self.cooling[code] = trading_days[exit_idx + self.params['cooling_days']]
                
                del self.positions[code]
            
            # 选股（如果持仓不足）
            if len(self.positions) < self.params['max_positions']:
                stocks = self.calculate_factors(date)
                scored = self.rank_score(stocks)
                
                # 筛选达标的
                candidates = [s for s in scored if s['total_score'] >= self.params['score_threshold']]
                candidates.sort(key=lambda x: x['total_score'], reverse=True)
                
                # 补充持仓
                slots = self.params['max_positions'] - len(self.positions)
                for c in candidates[:slots]:
                    if c['code'] not in self.positions and c['code'] not in self.cooling:
                        self.positions[c['code']] = {
                            'entry_date': date,
                            'entry_price': c['price'],
                            'name': c['name'],
                            'hold_days': 0
                        }
        
        self.close_db()
        
        return self.calculate_metrics()
    
    def calculate_metrics(self):
        """计算回测指标"""
        if not self.trades:
            return {
                'total_trades': 0,
                'win_rate': 0,
                'avg_return': 0,
                'total_return': 0,
                'max_drawdown': 0,
                'sharpe': 0
            }
        
        df = pd.DataFrame(self.trades)
        returns = df['net_return'].values
        
        wins = sum(1 for r in returns if r > 0)
        win_rate = wins / len(returns) * 100
        avg_return = np.mean(returns) * 100
        
        # 累计收益（复利）
        cum = 1.0
        for r in returns:
            cum *= (1 + r)
        total_return = (cum - 1) * 100
        
        # 最大回撤
        peak = 1.0
        max_dd = 0
        running = 1.0
        for r in returns:
            running *= (1 + r)
            if running > peak:
                peak = running
            dd = (peak - running) / peak
            if dd > max_dd:
                max_dd = dd
        
        # 年化收益和夏普
        years = len(self.trades) / 252  # 假设每个交易日有交易
        if years > 0 and max_dd > 0:
            annual_ret = ((cum ** (1/years)) - 1) * 100
            sharpe = annual_ret / (max_dd * 100)
        else:
            annual_ret = 0
            sharpe = 0
        
        return {
            'total_trades': len(self.trades),
            'win_rate': win_rate,
            'avg_return': avg_return,
            'total_return': total_return,
            'annual_return': annual_ret,
            'max_drawdown': max_dd * 100,
            'sharpe': sharpe
        }


def run_parameter_search():
    """参数网格搜索"""
    logger.info("=" * 70)
    logger.info("V11_IC_Optimized 参数优化开始")
    logger.info("=" * 70)
    
    # 生成参数组合
    param_names = list(PARAM_GRID.keys())
    param_values = list(PARAM_GRID.values())
    combinations = list(product(*param_values))
    
    logger.info(f"参数组合数: {len(combinations)}")
    
    results = []
    
    for idx, combo in enumerate(combinations):
        params = dict(zip(param_names, combo))
        params['min_turnover_amount'] = 50  # 固定5000万
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[组合 {idx+1}/{len(combinations)}] {params}")
        logger.info(f"{'='*70}")
        
        # 训练集回测
        engine = V11BacktestEngine(params)
        train_metrics = engine.run_backtest(*PERIODS['train'], 'train')
        
        # 验证集回测
        engine = V11BacktestEngine(params)
        valid_metrics = engine.run_backtest(*PERIODS['valid'], 'valid')
        
        # 计算综合得分
        score = (
            train_metrics['total_return'] * 0.3 +
            train_metrics['win_rate'] * 0.25 +
            (30 - train_metrics['max_drawdown']) * 0.25 +  # 回撤越小越好
            train_metrics['sharpe'] * 10 * 0.2  # 夏普比率
        )
        
        result = {
            'params': params,
            'train': train_metrics,
            'valid': valid_metrics,
            'score': score
        }
        
        results.append(result)
        
        logger.info(f"训练集: 收益={train_metrics['total_return']:.2f}% 胜率={train_metrics['win_rate']:.1f}% 回撤={train_metrics['max_drawdown']:.2f}%")
        logger.info(f"验证集: 收益={valid_metrics['total_return']:.2f}% 胜率={valid_metrics['win_rate']:.1f}% 回撤={valid_metrics['max_drawdown']:.2f}%")
        logger.info(f"综合得分: {score:.2f}")
    
    # 排序找出最优
    results.sort(key=lambda x: x['score'], reverse=True)
    
    # 保存结果
    output_dir = '/root/.openclaw/workspace/股票分析项目/optimization_results'
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'{output_dir}/param_opt_{timestamp}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # 生成报告
    logger.info(f"\n{'='*70}")
    logger.info("📊 参数优化完成 - TOP 10 结果")
    logger.info(f"{'='*70}")
    
    for i, r in enumerate(results[:10]):
        logger.info(f"\n[排名 {i+1}] 得分: {r['score']:.2f}")
        logger.info(f"  参数: {r['params']}")
        logger.info(f"  训练集: 收益={r['train']['total_return']:.2f}% 胜率={r['train']['win_rate']:.1f}%")
        logger.info(f"  验证集: 收益={r['valid']['total_return']:.2f}% 胜率={r['valid']['win_rate']:.1f}%")
    
    # 最优参数完整回测
    best_params = results[0]['params']
    logger.info(f"\n{'='*70}")
    logger.info("🏆 最优参数完整回测")
    logger.info(f"{'='*70}")
    
    engine = V11BacktestEngine(best_params)
    full_metrics = engine.run_backtest(*PERIODS['full'], 'full')
    
    logger.info(f"完整2年回测结果:")
    logger.info(f"  总交易: {full_metrics['total_trades']} 笔")
    logger.info(f"  胜率: {full_metrics['win_rate']:.1f}%")
    logger.info(f"  累计收益: {full_metrics['total_return']:.2f}%")
    logger.info(f"  年化收益: {full_metrics['annual_return']:.2f}%")
    logger.info(f"  最大回撤: {full_metrics['max_drawdown']:.2f}%")
    logger.info(f"  夏普比率: {full_metrics['sharpe']:.2f}")
    
    # 保存最优参数
    with open(f'{output_dir}/best_params_{timestamp}.json', 'w') as f:
        json.dump({
            'params': best_params,
            'metrics': full_metrics,
            'timestamp': timestamp
        }, f, indent=2)
    
    logger.info(f"\n结果保存至: {output_dir}/")
    
    return results


if __name__ == '__main__':
    run_parameter_search()
