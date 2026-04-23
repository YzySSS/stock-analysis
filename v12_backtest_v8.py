#!/usr/bin/env python3
"""
V12策略 V8 - 优化版
==================
基于DeepSeek分析V7失败后的改进方案：
1. 5因子体系：quality + valuation + momentum + trend + reversal
2. 动态阈值：基于市场波动率调整
3. 市场环境判断：熊市减仓/空仓
4. 动态因子权重：根据市场状态调整
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
    market_status: str


class V12BacktestEngineV8:
    """V12回测引擎 V8 - 优化版（5因子+动态参数）"""
    
    def __init__(self):
        # 基础因子权重（会根据市场环境动态调整）
        self.base_weights = {
            'quality': 0.25,      # ROE质量
            'valuation': 0.20,    # PE估值
            'momentum': 0.20,     # 20日行业相对动量 ← 新增
            'trend': 0.15,        # MA20斜率 ← 新增
            'reversal': 0.20      # 前一日涨跌幅（负权重） ← 改反向
        }
        
        self.current_weights = self.base_weights.copy()
        
        # 动态参数
        self.base_threshold = 50
        self.current_threshold = 50
        self.cooling_days = 5
        self.stop_loss_pct = -0.05
        
        # 风控参数
        self.max_drawdown_limit = 0.20
        self.position_ratio = 1.0  # 会根据市场环境调整
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.recent_picks = {}
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
    
    def get_market_status(self, date: str) -> Tuple[str, float]:
        """
        判断市场环境
        返回: (market_status, volatility)
        market_status: 'bull', 'neutral', 'bear'
        """
        cursor = self.conn.cursor()
        
        # 获取沪深300最近20日数据
        cursor.execute("""
            SELECT close, pct_change FROM stock_kline
            WHERE code = '000300.SH' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 20
        """, (date,))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 20:
            return 'neutral', 0.2
        
        closes = [r[0] for r in reversed(rows)]
        changes = [r[1] for r in reversed(rows) if r[1] is not None]
        
        # 计算趋势（MA20斜率）
        ma20 = np.mean(closes)
        ma5 = np.mean(closes[-5:])
        
        # 计算波动率
        volatility = np.std(changes) * np.sqrt(252) if changes else 0.2
        
        # 判断市场状态
        if ma5 > ma20 * 1.02 and changes[-1] > 0:
            status = 'bull'
        elif ma5 < ma20 * 0.98 or changes[-1] < -2:
            status = 'bear'
        else:
            status = 'neutral'
        
        return status, volatility
    
    def adjust_parameters(self, market_status: str, volatility: float):
        """根据市场环境动态调整参数"""
        # 调整阈值
        if volatility > 0.3:  # 高波动
            self.current_threshold = min(65, self.base_threshold + 10)
        elif volatility < 0.15:  # 低波动
            self.current_threshold = max(40, self.base_threshold - 5)
        else:
            self.current_threshold = self.base_threshold
        
        # 调整仓位
        if market_status == 'bear':
            self.position_ratio = 0.2  # 熊市20%仓位
        elif market_status == 'bull':
            self.position_ratio = 1.0  # 牛市满仓
        else:
            self.position_ratio = 0.6  # 震荡市60%仓位
        
        # 调整因子权重
        if market_status == 'bull':
            # 牛市：增加动量权重
            self.current_weights = {
                'quality': 0.20, 'valuation': 0.15,
                'momentum': 0.30, 'trend': 0.20, 'reversal': 0.15
            }
        elif market_status == 'bear':
            # 熊市：增加质量和估值权重
            self.current_weights = {
                'quality': 0.35, 'valuation': 0.30,
                'momentum': 0.10, 'trend': 0.10, 'reversal': 0.15
            }
        else:
            self.current_weights = self.base_weights.copy()
    
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取当日股票池（含5因子数据）"""
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
        
        # 获取前20日用于计算趋势和动量
        cursor.execute("""
            SELECT MAX(trade_date) FROM stock_kline 
            WHERE trade_date <= %s
        """, (date,))
        current_date = cursor.fetchone()[0]
        
        # 获取股票数据（含历史价格计算趋势和动量）
        cursor.execute("""
            SELECT 
                k.code, k.close, k.turnover,
                k_prev.pct_change as reversal,
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
        codes = []
        for row in cursor.fetchall():
            code = row[0]
            pe_score = row[4]
            roe_score = row[5]
            
            if pe_score is None or roe_score is None:
                continue
            
            stocks.append({
                'code': code,
                'price': float(row[1]),
                'reversal': float(row[3]) if row[3] is not None else 0,
                'quality': float(roe_score),
                'valuation': float(pe_score),
                'name': row[6] or ''
            })
            codes.append(code)
        
        cursor.close()
        
        # 获取历史价格计算趋势和动量
        if codes:
            stocks = self._add_trend_momentum(stocks, codes, date)
        
        return stocks
    
    def _add_trend_momentum(self, stocks: List[Dict], codes: List[str], date: str) -> List[Dict]:
        """添加趋势和动量因子"""
        cursor = self.conn.cursor()
        
        placeholders = ','.join(['%s'] * len(codes))
        cursor.execute(f"""
            SELECT code, close, pct_change FROM stock_kline
            WHERE code IN ({placeholders})
            AND trade_date <= %s
            ORDER BY code, trade_date DESC
            LIMIT {len(codes) * 25}
        """, tuple(codes) + (date,))
        
        # 组织数据
        price_data = defaultdict(list)
        for row in cursor.fetchall():
            code, close, pct = row
            price_data[code].append({'close': close, 'pct': pct})
        
        cursor.close()
        
        # 计算趋势和动量
        for stock in stocks:
            code = stock['code']
            if code not in price_data or len(price_data[code]) < 20:
                stock['trend'] = 0
                stock['momentum'] = 0
                continue
            
            prices = price_data[code]
            
            # 趋势：MA20斜率（年化）
            if len(prices) >= 20:
                ma20_now = np.mean([p['close'] for p in prices[:20]])
                ma20_prev = np.mean([p['close'] for p in prices[5:25]]) if len(prices) >= 25 else ma20_now
                if ma20_prev > 0:
                    trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100  # 年化
                else:
                    trend = 0
            else:
                trend = 0
            
            # 动量：20日累计收益
            momentum = sum([p['pct'] for p in prices[:20] if p['pct'] is not None])
            
            stock['trend'] = trend
            stock['momentum'] = momentum
        
        return stocks
    
    def calculate_zscore(self, stocks: List[Dict]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化（5因子）"""
        if len(stocks) < 10:
            return {}
        
        factors = ['quality', 'valuation', 'momentum', 'trend', 'reversal']
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
    
    def select_stocks(self, date: str, market_status: str) -> List[Dict]:
        """V8选股逻辑"""
        # 检查冷却期
        for code in list(self.recent_picks.keys()):
            last_date = self.recent_picks[code]
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > self.cooling_days:
                del self.recent_picks[code]
        
        # 熊市且回撤超限 -> 空仓
        if market_status == 'bear' and self.position_ratio <= 0.2:
            return []
        
        # 获取股票池
        stocks = self.get_stock_list(date)
        if len(stocks) < 10:
            return []
        
        # 过滤冷却期
        stocks = [s for s in stocks if s['code'] not in self.recent_picks]
        
        # Z-score标准化
        zscores = self.calculate_zscore(stocks)
        
        # 计算得分（动态权重）
        picks = []
        for stock in stocks:
            code = stock['code']
            if code not in zscores:
                continue
            
            # 加权Z-score（reversal用负权重）
            weighted_zscore = (
                float(zscores[code].get('quality', 0)) * self.current_weights['quality'] +
                float(zscores[code].get('valuation', 0)) * self.current_weights['valuation'] +
                float(zscores[code].get('momentum', 0)) * self.current_weights['momentum'] +
                float(zscores[code].get('trend', 0)) * self.current_weights['trend'] -
                float(zscores[code].get('reversal', 0)) * self.current_weights['reversal']  # 负权重
            )
            
            # 映射到百分制
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.current_threshold:
                picks.append({
                    'code': code,
                    'name': stock['name'],
                    'score': score,
                    'price': stock['price']
                })
        
        # 排序取前N（根据仓位调整）
        picks.sort(key=lambda x: x['score'], reverse=True)
        max_picks = int(5 * self.position_ratio)  # 动态调整选股数量
        return picks[:max(1, max_picks)]
    
    def backtest(self, start_date: str, end_date: str):
        """主回测函数"""
        logger.info("=" * 70)
        logger.info("V12策略 V8优化版 回测")
        logger.info("=" * 70)
        logger.info(f"回测期: {start_date} 至 {end_date}")
        logger.info(f"基础参数: 阈值={self.base_threshold} 冷却期={self.cooling_days}天")
        logger.info(f"5因子权重: {self.base_weights}")
        logger.info("")
        
        self.connect_db()
        
        try:
            trading_days = self.get_trading_days(start_date, end_date)
            logger.info(f"交易日: {len(trading_days)}天")
            
            for i, date in enumerate(trading_days):
                if i % 10 == 0:
                    logger.info(f"进度: {i+1}/{len(trading_days)} ({(i+1)/len(trading_days)*100:.1f}%)")
                
                # 判断市场环境
                market_status, volatility = self.get_market_status(date)
                
                # 动态调整参数
                self.adjust_parameters(market_status, volatility)
                
                # 选股
                picks = self.select_stocks(date, market_status)
                
                # 模拟次日交易
                if picks and i + 1 < len(trading_days):
                    next_date = trading_days[i + 1]
                    self._simulate_trades(date, next_date, picks, market_status)
                
                # 记录每日状态
                self.daily_stats.append({
                    'date': date,
                    'pick_count': len(picks),
                    'market_status': market_status,
                    'threshold': self.current_threshold,
                    'position_ratio': self.position_ratio
                })
            
            self._generate_report()
            
        finally:
            self.close_db()
    
    def _simulate_trades(self, pick_date: str, trade_date: str, picks: List[Dict], market_status: str):
        """模拟交易"""
        cursor = self.conn.cursor()
        
        for pick in picks:
            code = pick['code']
            self.recent_picks[code] = pick_date
            
            cursor.execute("""
                SELECT open, close, pct_change FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, trade_date))
            
            row = cursor.fetchone()
            if not row or row[0] is None:
                continue
            
            entry_price = float(row[0])
            exit_price = float(row[1])
            
            gross_return = (exit_price - entry_price) / entry_price
            
            # 扣除成本
            total_cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
            net_return = gross_return - total_cost
            
            exit_reason = 'stop_loss' if gross_return <= self.stop_loss_pct else 'normal'
            
            self.trades.append(TradeRecord(
                entry_date=trade_date, exit_date=trade_date, code=code,
                entry_price=entry_price, exit_price=exit_price,
                gross_return=gross_return, net_return=net_return,
                score=pick['score'], exit_reason=exit_reason,
                market_status=market_status
            ))
        
        cursor.close()
    
    def _generate_report(self):
        """生成回测报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return
        
        df = pd.DataFrame([asdict(t) for t in self.trades])
        
        total_trades = len(df)
        winning = len(df[df['net_return'] > 0])
        win_rate = winning / total_trades
        avg_net = df['net_return'].mean()
        
        # 累计收益
        log_returns = np.log1p(df['net_return'].values)
        cumulative = np.expm1(np.sum(log_returns))
        
        # 最大回撤
        cum_series = np.cumprod(1 + df['net_return'].values)
        running_max = np.maximum.accumulate(cum_series)
        max_dd = abs(((cum_series - running_max) / running_max).min())
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("V8优化版 回测结果")
        logger.info("=" * 70)
        logger.info(f"总交易: {total_trades}笔")
        logger.info(f"胜率: {win_rate*100:.1f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"累计收益: {cumulative*100:.2f}%")
        logger.info(f"最大回撤: {max_dd*100:.2f}%")
        logger.info("=" * 70)
        
        # 按年统计
        df['year'] = pd.to_datetime(df['entry_date']).dt.year
        logger.info("\n按年统计:")
        for year in sorted(df['year'].unique()):
            ydf = df[df['year'] == year]
            yr = len(ydf[ydf['net_return'] > 0]) / len(ydf)
            ycum = np.expm1(np.sum(np.log1p(ydf['net_return'].values)))
            logger.info(f"  {year}: {len(ydf)}笔, 胜率{yr*100:.0f}%, 累计{ycum*100:.1f}%")
        
        # 按市场环境统计
        logger.info("\n按市场环境:")
        for status in ['bull', 'neutral', 'bear']:
            sdf = df[df['market_status'] == status]
            if len(sdf) > 0:
                sr = len(sdf[sdf['net_return'] > 0]) / len(sdf)
                scum = np.expm1(np.sum(np.log1p(sdf['net_return'].values)))
                logger.info(f"  {status}: {len(sdf)}笔, 胜率{sr*100:.0f}%, 累计{scum*100:.1f}%")
        
        # 保存
        output = {
            'version': 'V8_Optimized',
            'parameters': {'base_weights': self.base_weights, 'base_threshold': self.base_threshold},
            'summary': {
                'total_trades': int(total_trades), 'win_rate': float(win_rate),
                'avg_net_return': float(avg_net), 'cumulative_return': float(cumulative),
                'max_drawdown': float(max_dd)
            },
            'trades': [asdict(t) for t in self.trades]
        }
        
        with open('v12_v8_backtest_result.json', 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        logger.info(f"\n✅ 结果已保存: v12_v8_backtest_result.json")


if __name__ == "__main__":
    engine = V12BacktestEngineV8()
    engine.backtest('2024-01-02', '2026-04-08')
