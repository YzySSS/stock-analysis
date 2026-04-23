#!/usr/bin/env python3
"""
V12策略 V10 - P0风控完整版
==========================
核心改进:
1. 完整风控系统（最大回撤20%限制、连续亏损停止、周亏损限制）
2. 市场环境过滤（熊市自动空仓）
3. 个股仓位精细化管理
4. 整合V9的改进（止盈止损、多指标市场环境判断）

作者: DeepSeek建议 + 小X实现
版本: V10-P0
日期: 2026-04-10
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

# 导入风控模块
from strategies.v12_risk_control import RiskControlSystem, RiskMetrics
from strategies.v12_market_filter import MarketEnvironmentFilter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
    holding_days: int
    position_weight: float  # 个股仓位权重


class V12BacktestEngineV10:
    """
    V12回测引擎 V10 - P0风控完整版
    
    核心特性:
    - 风控系统: 回撤控制、连续亏损停止、周亏损限制
    - 市场过滤: 熊市自动空仓
    - 个股仓位: 基于波动率、流动性精细调整
    """
    
    def __init__(self):
        # 因子权重（4因子体系）
        self.base_weights = {
            'quality': 0.30,
            'valuation': 0.25,
            'combined_trend': 0.25,
            'reversal': 0.20
        }
        self.current_weights = self.base_weights.copy()
        
        # 动态参数
        self.base_threshold = 50
        self.current_threshold = 50
        self.base_cooling_days = 5
        
        # 交易参数
        self.stop_loss_pct = -0.05
        self.stop_profit_pct = 0.08
        self.max_holding_days = 10
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        self.total_cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
        
        # 🆕 P0核心: 风控系统
        self.risk_ctrl = RiskControlSystem(
            max_drawdown_limit=0.20,
            max_position_per_stock=0.20,
            max_consecutive_losses=5,
            max_weekly_loss=-0.10
        )
        
        # 🆕 P0核心: 市场环境过滤
        self.market_filter = MarketEnvironmentFilter()
        
        # 状态跟踪
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.recent_picks = {}
        self.active_positions = {}  # code -> {entry_date, entry_price, holding_days}
        self.account_value = 1.0     # 初始净值
        self.peak_value = 1.0
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
    
    def backtest(self, start_date: str, end_date: str):
        """主回测函数"""
        logger.info("=" * 80)
        logger.info("V12策略 V10 - P0风控完整版 回测")
        logger.info("=" * 80)
        logger.info(f"回测期: {start_date} 至 {end_date}")
        logger.info(f"风控配置: 最大回撤{self.risk_ctrl.max_drawdown_limit:.0%} | "
                   f"连续亏损停止{self.risk_ctrl.max_consecutive_losses}次 | "
                   f"周亏损限制{self.risk_ctrl.max_weekly_loss:.0%}")
        logger.info("")
        
        self.connect_db()
        
        try:
            trading_days = self.get_trading_days(start_date, end_date)
            logger.info(f"交易日: {len(trading_days)}天")
            logger.info("")
            
            for i, date in enumerate(trading_days):
                # 每10天输出进度
                if i % 10 == 0:
                    progress = (i + 1) / len(trading_days) * 100
                    logger.info(f"📊 进度: {i+1}/{len(trading_days)} ({progress:.1f}%) | "
                               f"净值:{self.account_value:.4f} | "
                               f"回撤:{self.risk_ctrl.current_drawdown:.1%}")
                
                # 🆕 P0核心: 风控检查 - 是否停止交易
                if self.risk_ctrl.should_stop_trading(self.account_value, date):
                    self.daily_stats.append({
                        'date': date,
                        'pick_count': 0,
                        'market_status': 'stop',
                        'account_value': self.account_value,
                        'drawdown': self.risk_ctrl.current_drawdown,
                        'reason': '风控停止'
                    })
                    continue
                
                # 🆕 P0核心: 判断市场环境
                market_status, composite_score = self.market_filter.get_market_status(
                    date, self.conn
                )
                
                # 🆕 P0核心: 熊市空仓
                if market_status == 'bear':
                    logger.info(f"  📉 熊市空仓: {date}")
                    self.daily_stats.append({
                        'date': date,
                        'pick_count': 0,
                        'market_status': 'bear',
                        'account_value': self.account_value,
                        'drawdown': self.risk_ctrl.current_drawdown,
                        'reason': '熊市空仓'
                    })
                    continue
                
                # 处理持仓（卖出检查）
                self._check_positions(date)
                
                # 选股
                picks = self.select_stocks(date, market_status)
                
                # 模拟买入
                if picks and i + 1 < len(trading_days):
                    next_date = trading_days[i + 1]
                    self._simulate_entries(date, next_date, picks, market_status)
                
                # 记录每日状态
                self.daily_stats.append({
                    'date': date,
                    'pick_count': len(picks),
                    'market_status': market_status,
                    'composite_score': round(composite_score, 3),
                    'account_value': self.account_value,
                    'drawdown': self.risk_ctrl.current_drawdown,
                    'position_ratio': self.risk_ctrl._calculate_position_scale()
                })
            
            # 强制平仓所有持仓
            if self.active_positions and trading_days:
                self._close_all_positions(trading_days[-1])
            
            self._generate_report()
            
        finally:
            self.close_db()
    
    def select_stocks(self, date: str, market_status: str) -> List[Dict]:
        """选股逻辑"""
        # 动态调整参数
        self._adjust_parameters(market_status)
        
        # 清理冷却期
        for code in list(self.recent_picks.keys()):
            last_date = self.recent_picks[code]
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > self.base_cooling_days:
                del self.recent_picks[code]
        
        # 获取股票池
        stocks = self._get_stock_list(date)
        if len(stocks) < 10:
            return []
        
        # 过滤冷却期
        stocks = [s for s in stocks if s['code'] not in self.recent_picks]
        
        # 过滤已有持仓
        stocks = [s for s in stocks if s['code'] not in self.active_positions]
        
        # Z-score评分
        picks = self._score_stocks(stocks)
        
        # 🆕 P0核心: 应用仓位限制
        picks = self._apply_position_limits(picks)
        
        return picks
    
    def _adjust_parameters(self, market_status: str):
        """根据市场环境调整参数"""
        if market_status == 'bull':
            self.current_weights = {
                'quality': 0.25, 'valuation': 0.20,
                'combined_trend': 0.35, 'reversal': 0.20
            }
        elif market_status == 'bear':
            self.current_weights = {
                'quality': 0.40, 'valuation': 0.35,
                'combined_trend': 0.10, 'reversal': 0.15
            }
        else:
            self.current_weights = self.base_weights.copy()
    
    def _apply_position_limits(self, picks: List[Dict]) -> List[Dict]:
        """🆕 P0核心: 应用仓位限制"""
        if not picks:
            return []
        
        # 根据回撤调整选股数量
        position_scale = self.risk_ctrl._calculate_position_scale()
        max_picks = max(1, int(5 * position_scale))
        
        return picks[:max_picks]
    
    def _get_stock_list(self, date: str) -> List[Dict]:
        """获取股票池"""
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
            pe_score = row[5]
            roe_score = row[6]
            
            if pe_score is None or roe_score is None:
                continue
            
            # 跳过没有基本面数据的
            if row[4] is None or row[5] is None:
                continue
                
            stocks.append({
                'code': row[0],
                'price': float(row[1]),
                'turnover': float(row[2]) if row[2] else 0,
                'reversal': float(row[3]) if row[3] is not None else 0,
                'quality': float(row[5]),  # roe_score
                'valuation': float(row[4]),  # pe_score
                'market_cap': None,
                'name': row[6] or ''
            })
            codes.append(row[0])
        
        cursor.close()
        
        # 添加趋势数据
        if codes:
            stocks = self._add_trend_data(stocks, codes, date)
        
        return stocks
    
    def _add_trend_data(self, stocks: List[Dict], codes: List[str], date: str) -> List[Dict]:
        """添加趋势数据"""
        cursor = self.conn.cursor()
        placeholders = ','.join(['%s'] * len(codes))
        
        cursor.execute(f"""
            SELECT code, close, pct_change FROM stock_kline
            WHERE code IN ({placeholders})
            AND trade_date <= %s
            ORDER BY code, trade_date DESC
            LIMIT {len(codes) * 25}
        """, tuple(codes) + (date,))
        
        price_data = defaultdict(list)
        for row in cursor.fetchall():
            price_data[row[0]].append({'close': row[1], 'pct': row[2]})
        
        cursor.close()
        
        for stock in stocks:
            code = stock['code']
            if code not in price_data or len(price_data[code]) < 20:
                stock['combined_trend'] = 0
                continue
            
            prices = price_data[code]
            
            # 合并趋势+动量
            ma20_now = np.mean([p['close'] for p in prices[:20]])
            ma20_prev = np.mean([p['close'] for p in prices[5:25]]) if len(prices) >= 25 else ma20_now
            
            if ma20_prev > 0:
                trend = float((ma20_now - ma20_prev) / ma20_prev * 252 * 100)
            else:
                trend = 0.0
            
            momentum = sum([float(p['pct']) for p in prices[:20] if p['pct'] is not None])
            
            # 合并为单一因子
            stock['combined_trend'] = trend * 0.4 + momentum * 0.6
        
        return stocks
    
    def _score_stocks(self, stocks: List[Dict]) -> List[Dict]:
        """股票评分"""
        if len(stocks) < 10:
            return []
        
        factors = ['quality', 'valuation', 'combined_trend', 'reversal']
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
        
        picks = []
        for stock in stocks:
            code = stock['code']
            if code not in zscores:
                continue
            
            # 加权得分（reversal用负权重）
            weighted_zscore = (
                zscores[code].get('quality', 0) * self.current_weights['quality'] +
                zscores[code].get('valuation', 0) * self.current_weights['valuation'] +
                zscores[code].get('combined_trend', 0) * self.current_weights['combined_trend'] -
                zscores[code].get('reversal', 0) * self.current_weights['reversal']
            )
            
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.current_threshold:
                picks.append({
                    'code': code,
                    'name': stock['name'],
                    'score': score,
                    'price': stock['price'],
                    'volatility': 0.2,  # 默认波动率
                    'turnover': stock['turnover'],
                    'market_cap': stock['market_cap']
                })
        
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:10]
    
    def _simulate_entries(self, pick_date: str, trade_date: str, picks: List[Dict], market_status: str):
        """模拟买入"""
        cursor = self.conn.cursor()
        
        for pick in picks:
            code = pick['code']
            self.recent_picks[code] = pick_date
            
            cursor.execute("""
                SELECT open FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, trade_date))
            
            row = cursor.fetchone()
            if not row or row[0] is None:
                continue
            
            entry_price = float(row[0])
            
            # 🆕 P0核心: 计算个股仓位权重
            position_weight = self.risk_ctrl.calculate_position_per_stock(
                score=pick['score'],
                volatility=pick['volatility'],
                liquidity=pick['turnover'],
                market_cap=pick.get('market_cap')
            )
            
            self.active_positions[code] = {
                'entry_date': trade_date,
                'entry_price': entry_price,
                'holding_days': 0,
                'score': pick['score'],
                'market_status': market_status,
                'position_weight': position_weight
            }
        
        cursor.close()
    
    def _check_positions(self, date: str):
        """检查持仓卖出条件"""
        if not self.active_positions:
            return
        
        cursor = self.conn.cursor()
        codes_to_remove = []
        
        for code, pos in self.active_positions.items():
            cursor.execute("""
                SELECT open, close, high, low FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, date))
            
            row = cursor.fetchone()
            if not row:
                continue
            
            open_price, close_price, high_price, low_price = [float(v) if v else 0 for v in row]
            entry_price = pos['entry_price']
            holding_days = pos['holding_days'] + 1
            
            # 计算收益
            current_return = (close_price - entry_price) / entry_price
            
            # 检查卖出条件
            exit_reason = None
            exit_price = close_price
            
            # 止损
            if current_return <= self.stop_loss_pct:
                exit_reason = 'stop_loss'
                exit_price = open_price if low_price <= entry_price * (1 + self.stop_loss_pct) else close_price
            
            # 止盈
            elif current_return >= self.stop_profit_pct:
                exit_reason = 'stop_profit'
                exit_price = open_price if high_price >= entry_price * (1 + self.stop_profit_pct) else close_price
            
            # 最长持有期
            elif holding_days >= self.max_holding_days:
                exit_reason = 'max_holding'
                exit_price = close_price
            
            if exit_reason:
                # 计算收益
                gross_return = (exit_price - entry_price) / entry_price
                net_return = gross_return - self.total_cost
                
                # 🆕 P0核心: 应用仓位权重
                weighted_return = net_return * pos['position_weight']
                
                # 更新账户净值
                self.account_value *= (1 + weighted_return)
                
                # 记录交易
                self.trades.append(TradeRecord(
                    entry_date=pos['entry_date'],
                    exit_date=date,
                    code=code,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    gross_return=gross_return,
                    net_return=net_return,
                    score=pos['score'],
                    exit_reason=exit_reason,
                    market_status=pos['market_status'],
                    holding_days=holding_days,
                    position_weight=pos['position_weight']
                ))
                
                # 🆕 P0核心: 记录交易结果到风控系统
                self.risk_ctrl.record_trade({
                    'date': date,
                    'return': weighted_return,
                    'code': code
                })
                
                codes_to_remove.append(code)
            else:
                # 更新持仓天数
                self.active_positions[code]['holding_days'] = holding_days
        
        # 移除已平仓
        for code in codes_to_remove:
            del self.active_positions[code]
        
        cursor.close()
    
    def _close_all_positions(self, date: str):
        """强制平仓所有持仓"""
        if not self.active_positions:
            return
        
        cursor = self.conn.cursor()
        
        for code, pos in list(self.active_positions.items()):
            cursor.execute("""
                SELECT close FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, date))
            
            row = cursor.fetchone()
            if not row or row[0] is None:
                continue
            
            exit_price = float(row[0])
            entry_price = pos['entry_price']
            
            gross_return = (exit_price - entry_price) / entry_price
            net_return = gross_return - self.total_cost
            weighted_return = net_return * pos['position_weight']
            
            self.account_value *= (1 + weighted_return)
            
            self.trades.append(TradeRecord(
                entry_date=pos['entry_date'],
                exit_date=date,
                code=code,
                entry_price=entry_price,
                exit_price=exit_price,
                gross_return=gross_return,
                net_return=net_return,
                score=pos['score'],
                exit_reason='end_of_backtest',
                market_status=pos['market_status'],
                holding_days=pos['holding_days'],
                position_weight=pos['position_weight']
            ))
        
        cursor.close()
        self.active_positions.clear()
    
    def _generate_report(self):
        """生成回测报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return
        
        df = pd.DataFrame([asdict(t) for t in self.trades])
        
        # 基础统计
        total_trades = len(df)
        winning = len(df[df['net_return'] > 0])
        win_rate = winning / total_trades
        avg_gross = df['gross_return'].mean()
        avg_net = df['net_return'].mean()
        
        # 累计收益
        final_value = self.account_value
        total_return = final_value - 1.0
        
        # 最大回撤
        max_dd = self.risk_ctrl.max_drawdown
        
        # 按市场状态统计
        by_market = df.groupby('market_status')['net_return'].agg(['count', 'mean', 'sum'])
        
        # 按退出原因统计
        by_reason = df.groupby('exit_reason')['net_return'].agg(['count', 'mean'])
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("V10 - P0风控完整版 回测结果")
        logger.info("=" * 80)
        logger.info(f"总交易: {total_trades}笔")
        logger.info(f"胜率: {win_rate*100:.1f}%")
        logger.info(f"平均毛收益: {avg_gross*100:.2f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"最终净值: {final_value:.4f}")
        logger.info(f"总收益: {total_return*100:.2f}%")
        logger.info(f"最大回撤: {max_dd*100:.2f}%")
        logger.info("")
        logger.info("按市场环境:")
        for status, row in by_market.iterrows():
            logger.info(f"  {status}: {row['count']}笔 | 平均{row['mean']*100:.2f}% | 总计{row['sum']*100:.2f}%")
        logger.info("")
        logger.info("按退出原因:")
        for reason, row in by_reason.iterrows():
            logger.info(f"  {reason}: {row['count']}笔 | 平均{row['mean']*100:.2f}%")
        logger.info("=" * 80)
        
        # 保存详细结果
        df.to_csv('v12_v10_p0_trades.csv', index=False)
        
        summary = {
            'version': 'V10-P0',
            'total_trades': total_trades,
            'win_rate': round(win_rate, 4),
            'avg_gross_return': round(avg_gross, 6),
            'avg_net_return': round(avg_net, 6),
            'final_value': round(final_value, 4),
            'total_return': round(total_return, 4),
            'max_drawdown': round(max_dd, 4),
            'risk_metrics': self.risk_ctrl.get_risk_report()
        }
        
        with open('v12_v10_p0_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info("结果已保存: v12_v10_p0_trades.csv, v12_v10_p0_summary.json")


# 主函数
if __name__ == '__main__':
    import sys
    
    # 默认回测2024-2026年
    start_date = sys.argv[1] if len(sys.argv) > 1 else '2024-01-02'
    end_date = sys.argv[2] if len(sys.argv) > 2 else '2026-04-08'
    
    engine = V12BacktestEngineV10()
    engine.backtest(start_date, end_date)