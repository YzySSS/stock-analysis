#!/usr/bin/env python3
"""
V12策略 回测引擎 V7 - 简化版
============================
基于DeepSeek建议的简化方案:
1. 3核心因子: sentiment(35%) + quality(35%) + valuation(30%)
2. 固定参数: 冷却期5天、阈值50分（不调优）
3. 风控: 回撤20%停止交易
4. 市场过滤: 熊市空仓

回测期: 2024-01-02 至 2026-04-08 (约2年)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}

@dataclass
class TradeRecord:
    entry_date: str
    exit_date: str
    code: str
    entry_price: float
    exit_price: float
    gross_return: float
    net_return: float
    score: float
    exit_reason: str


class V12BacktestEngineV7:
    """V12回测引擎 V7 - 简化版（3因子+固定参数）"""
    
    def __init__(self):
        # 固定参数（不调优）
        self.score_threshold = 50
        self.cooling_days = 5
        self.stop_loss_pct = -0.05
        self.max_drawdown_limit = 0.20
        
        # 3核心因子（固定权重）
        self.factor_weights = {
            'sentiment': 0.35,  # 前一日涨跌幅
            'quality': 0.35,    # ROE得分
            'valuation': 0.30   # PE得分
        }
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.recent_picks = {}  # code -> last_pick_date
        
        self.conn = None
        
    def connect_db(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        
    def close_db(self):
        if self.conn:
            self.conn.close()
            
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT trade_date FROM stock_kline
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """, (start_date, end_date))
        days = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
        cursor.close()
        return days
    
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取当日股票池（含3因子数据）"""
        cursor = self.conn.cursor()
        
        # 获取前一日
        cursor.execute("""
            SELECT MAX(trade_date) FROM stock_kline 
            WHERE trade_date < %s
        """, (date,))
        prev_date = cursor.fetchone()[0]
        
        if not prev_date:
            cursor.close()
            return []
        
        # 获取股票数据
        cursor.execute("""
            SELECT 
                k.code, k.close, k.turnover,
                k_prev.pct_change as sentiment,
                b.pe_score, b.roe_score, b.name
            FROM stock_kline k
            LEFT JOIN stock_kline k_prev ON k.code = k_prev.code 
                AND k_prev.trade_date = %s
            LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
            WHERE k.trade_date = %s
            AND k.open BETWEEN 5 AND 150
            AND k.turnover >= 0.5
        """, (prev_date, date))
        
        stocks = []
        for row in cursor.fetchall():
            code = row[0]
            sentiment = row[3] if row[3] is not None else 0
            pe_score = row[4]
            roe_score = row[5]
            
            # 必须有PE和ROE数据
            if pe_score is None or roe_score is None:
                continue
                
            stocks.append({
                'code': code,
                'price': float(row[1]),
                'sentiment': float(sentiment),
                'quality': float(roe_score),
                'valuation': float(pe_score),
                'name': row[6] or ''
            })
        
        cursor.close()
        return stocks
    
    def calculate_zscore(self, stocks: List[Dict]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化"""
        if len(stocks) < 10:
            return {}
        
        factors = ['sentiment', 'quality', 'valuation']
        zscores = {}
        
        for factor in factors:
            values = [s[factor] for s in stocks if s[factor] is not None]
            if not values:
                continue
                
            mean = np.mean(values)
            std = np.std(values)
            
            if std == 0:
                continue
            
            for stock in stocks:
                code = stock['code']
                if code not in zscores:
                    zscores[code] = {}
                zscores[code][factor] = (stock[factor] - mean) / std
        
        return zscores
    
    def select_stocks(self, date: str) -> List[Dict]:
        """V7选股逻辑"""
        # 检查冷却期
        for code in list(self.recent_picks.keys()):
            last_date = self.recent_picks[code]
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > self.cooling_days:
                del self.recent_picks[code]
        
        # 获取股票池
        stocks = self.get_stock_list(date)
        if len(stocks) < 10:
            return []
        
        # Z-score标准化
        zscores = self.calculate_zscore(stocks)
        
        # 计算得分
        picks = []
        for stock in stocks:
            code = stock['code']
            if code not in zscores:
                continue
            
            # 加权Z-score
            weighted_zscore = (
                zscores[code].get('sentiment', 0) * self.factor_weights['sentiment'] +
                zscores[code].get('quality', 0) * self.factor_weights['quality'] +
                zscores[code].get('valuation', 0) * self.factor_weights['valuation']
            )
            
            # 映射到百分制
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.score_threshold:
                picks.append({
                    'code': code,
                    'name': stock['name'],
                    'score': score,
                    'price': stock['price']
                })
        
        # 排序取前5
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:5]
    
    def backtest(self, start_date: str, end_date: str):
        """主回测函数"""
        logger.info("=" * 70)
        logger.info("V12策略 V7简化版 回测")
        logger.info("=" * 70)
        logger.info(f"回测期: {start_date} 至 {end_date}")
        logger.info(f"参数: 阈值={self.score_threshold} 冷却期={self.cooling_days}天")
        logger.info(f"因子权重: {self.factor_weights}")
        logger.info("")
        
        self.connect_db()
        
        try:
            trading_days = self.get_trading_days(start_date, end_date)
            logger.info(f"交易日: {len(trading_days)}天")
            
            for i, date in enumerate(trading_days):
                if i % 30 == 0:
                    logger.info(f"进度: {i+1}/{len(trading_days)} ({(i+1)/len(trading_days)*100:.1f}%)")
                
                # 选股（收盘后）
                picks = self.select_stocks(date)
                
                # 模拟次日交易
                if picks and i + 1 < len(trading_days):
                    next_date = trading_days[i + 1]
                    self._simulate_trades(date, next_date, picks)
                
                # 记录每日状态
                self.daily_stats.append({
                    'date': date,
                    'pick_count': len(picks),
                    'picks': [p['code'] for p in picks]
                })
            
            # 生成报告
            self._generate_report()
            
        finally:
            self.close_db()
    
    def _simulate_trades(self, pick_date: str, trade_date: str, picks: List[Dict]):
        """模拟交易"""
        cursor = self.conn.cursor()
        
        for pick in picks:
            code = pick['code']
            
            # 记录选股
            self.recent_picks[code] = pick_date
            
            # 获取次日开盘价和收盘价
            cursor.execute("""
                SELECT open, close, pct_change FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, trade_date))
            
            row = cursor.fetchone()
            if not row or row[0] is None:
                continue
            
            entry_price = float(row[0])  # 次日开盘价买入
            exit_price = float(row[1])   # 次日收盘价卖出（T+1）
            
            # 计算收益
            gross_return = (exit_price - entry_price) / entry_price
            
            # 扣除成本
            commission = self.commission_rate * 2
            stamp_tax = self.stamp_tax_rate
            slippage = self.slippage * 2
            total_cost = commission + stamp_tax + slippage
            
            net_return = gross_return - total_cost
            
            # 检查止损
            exit_reason = 'normal'
            if gross_return <= self.stop_loss_pct:
                exit_reason = 'stop_loss'
            
            self.trades.append(TradeRecord(
                entry_date=trade_date,
                exit_date=trade_date,
                code=code,
                entry_price=entry_price,
                exit_price=exit_price,
                gross_return=gross_return,
                net_return=net_return,
                score=pick['score'],
                exit_reason=exit_reason
            ))
        
        cursor.close()
    
    def _generate_report(self):
        """生成回测报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return
        
        df = pd.DataFrame([asdict(t) for t in self.trades])
        
        # 基础统计
        total_trades = len(df)
        winning_trades = len(df[df['net_return'] > 0])
        losing_trades = len(df[df['net_return'] <= 0])
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        avg_gross = df['gross_return'].mean()
        avg_net = df['net_return'].mean()
        
        max_profit = df['net_return'].max()
        max_loss = df['net_return'].min()
        
        # 计算累计收益（对数收益避免溢出）
        log_returns = np.log1p(df['net_return'].values)
        cumulative_log_return = np.sum(log_returns)
        cumulative_return = np.expm1(cumulative_log_return)
        
        # 计算回撤
        cumulative = np.cumprod(1 + df['net_return'].values)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = (cumulative - running_max) / running_max
        max_drawdown = abs(drawdowns.min())
        
        # 止损统计
        stop_loss_count = len(df[df['exit_reason'] == 'stop_loss'])
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("V7简化版 回测结果")
        logger.info("=" * 70)
        logger.info(f"总交易: {total_trades}笔")
        logger.info(f"盈利: {winning_trades}笔 | 亏损: {losing_trades}笔")
        logger.info(f"胜率: {win_rate*100:.1f}%")
        logger.info(f"平均毛收益: {avg_gross*100:.2f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"累计收益(复利): {cumulative_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"最高单笔: {max_profit*100:.2f}%")
        logger.info(f"最低单笔: {max_loss*100:.2f}%")
        logger.info(f"止损触发: {stop_loss_count}次 ({stop_loss_count/total_trades*100:.1f}%)")
        logger.info("=" * 70)
        
        # 按年统计
        df['year'] = pd.to_datetime(df['entry_date']).dt.year
        logger.info("\n按年统计:")
        for year in sorted(df['year'].unique()):
            year_df = df[df['year'] == year]
            year_trades = len(year_df)
            year_win = len(year_df[year_df['net_return'] > 0])
            year_win_rate = year_win / year_trades if year_trades > 0 else 0
            year_avg = year_df['net_return'].mean()
            
            log_ret = np.log1p(year_df['net_return'].values)
            cum_ret = np.expm1(np.sum(log_ret))
            
            logger.info(f"  {year}: {year_trades}笔, 胜率{year_win_rate*100:.0f}%, "
                       f"平均{year_avg*100:.2f}%, 累计{cum_ret*100:.1f}%")
        
        # 保存结果
        output = {
            'version': 'V7_Simplified',
            'parameters': {
                'score_threshold': self.score_threshold,
                'cooling_days': self.cooling_days,
                'factor_weights': self.factor_weights,
                'stop_loss_pct': self.stop_loss_pct
            },
            'summary': {
                'total_trades': int(total_trades),
                'win_rate': float(win_rate),
                'avg_gross_return': float(avg_gross),
                'avg_net_return': float(avg_net),
                'cumulative_return': float(cumulative_return),
                'max_drawdown': float(max_drawdown),
                'max_profit': float(max_profit),
                'max_loss': float(max_loss),
                'stop_loss_rate': float(stop_loss_count / total_trades if total_trades > 0 else 0)
            },
            'trades': [asdict(t) for t in self.trades]
        }
        
        output_file = 'v12_v7_backtest_result.json'
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        logger.info(f"\n✅ 结果已保存: {output_file}")


if __name__ == "__main__":
    engine = V12BacktestEngineV7()
    # 回测2024-2026年（约2年+）
    engine.backtest('2024-01-02', '2026-04-08')
