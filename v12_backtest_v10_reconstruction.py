#!/usr/bin/env python3
"""
V12回测引擎 V10 - 重构版
======================
基于4因子策略的真实回测
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict

from v12_strategy_v10_reconstruction import V12StrategyV10, TradeRecord

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DailyStats:
    """每日统计"""
    date: str
    total_value: float
    cash: float
    position_value: float
    position_count: int
    market_condition: str
    position_ratio: float


@dataclass
class Position:
    """持仓"""
    code: str
    name: str
    industry: str
    entry_date: str
    exit_date: str
    entry_price: float
    shares: float
    cost_basis: float
    stop_price: float  # 止损价
    
    # 因子得分
    quality_score: float
    value_score: float
    reversal_score: float
    lowvol_score: float
    total_score: float


class V12BacktestV10:
    """V10回测引擎"""
    
    def __init__(self, strategy: V12StrategyV10, initial_capital: float = 1000000):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.capital = initial_capital
        
        self.positions: List[Position] = []
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[DailyStats] = []
        
        self.cost_breakdown = {
            'total_commission': 0,
            'total_stamp_tax': 0,
            'total_slippage': 0,
            'total_transfer': 0,
            'total_cost': 0
        }
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        logger.info(f"="*70)
        logger.info(f"V10重构版回测: {start_date} ~ {end_date}")
        logger.info(f"初始资金: ¥{self.initial_capital:,.0f}")
        logger.info(f"="*70)
        
        # 连接数据库
        self.strategy.connect()
        
        try:
            # 获取交易日
            trading_days = self.strategy.get_trading_days(start_date, end_date)
            logger.info(f"交易日: {len(trading_days)}天")
            
            if len(trading_days) < 30:
                logger.error("交易日不足")
                return {}
            
            # 每日迭代
            for i, date in enumerate(trading_days):
                if i < 20:  # 跳过前20天（确保有足够历史数据）
                    continue
                
                if i % 50 == 0:
                    logger.info(f"进度: {i}/{len(trading_days)} ({i/len(trading_days)*100:.1f}%)")
                
                # 1. 检查市场环境
                market_condition = self.strategy.check_market_condition(date)
                position_ratio = self.strategy.calculate_position_ratio(market_condition)
                
                # 2. 检查持仓（止损、到期）
                self._check_positions(date)
                
                # 3. 选股并开仓
                self._open_positions(date, position_ratio)
                
                # 4. 记录每日统计
                position_value = self._calculate_position_value(date)
                total_value = self.capital + position_value
                
                self.daily_stats.append(DailyStats(
                    date=date,
                    total_value=total_value,
                    cash=self.capital,
                    position_value=position_value,
                    position_count=len(self.positions),
                    market_condition=market_condition,
                    position_ratio=position_ratio
                ))
            
            return self._generate_report()
            
        finally:
            self.strategy.close()
    
    def _check_positions(self, date: str):
        """检查持仓（止损、到期）"""
        for pos in self.positions[:]:
            current_price = self._get_price(pos.code, date, 'close')
            if not current_price:
                continue
            
            # 计算当前亏损
            loss_pct = (current_price - pos.entry_price) / pos.entry_price
            
            # 止损检查
            if loss_pct <= self.strategy.stop_loss:
                self._close_position(pos, date, current_price, 'stop_loss')
                continue
            
            # 持仓到期检查
            hold_days = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(pos.entry_date, '%Y-%m-%d')).days
            
            if hold_days >= self.strategy.hold_days:
                self._close_position(pos, date, current_price, 'time_exit')
    
    def _close_position(self, pos: Position, date: str, exit_price: float, reason: str):
        """平仓"""
        # 计算卖出金额
        sale_amount = pos.shares * exit_price
        
        # 计算卖出成本
        costs = self.strategy.calculate_costs(sale_amount, is_buy=False)
        net_amount = costs['net_amount']
        
        # 更新资金
        self.capital += net_amount
        
        # 记录成本
        self.cost_breakdown['total_commission'] += costs['commission']
        self.cost_breakdown['total_stamp_tax'] += costs['stamp_tax']
        self.cost_breakdown['total_slippage'] += costs['slippage']
        self.cost_breakdown['total_transfer'] += costs['transfer']
        self.cost_breakdown['total_cost'] += costs['total']
        
        # 计算收益
        buy_costs = pos.cost_basis - (pos.shares * pos.entry_price)
        total_cost = buy_costs + costs['total']
        
        gross_pnl = sale_amount - (pos.shares * pos.entry_price)
        net_pnl = net_amount - pos.cost_basis
        
        gross_return = gross_pnl / pos.cost_basis
        net_return = net_pnl / pos.cost_basis
        
        hold_days = (datetime.strptime(date, '%Y-%m-%d') - 
                    datetime.strptime(pos.entry_date, '%Y-%m-%d')).days
        
        # 记录交易
        self.trades.append(TradeRecord(
            entry_date=pos.entry_date,
            exit_date=date,
            code=pos.code,
            name=pos.name,
            industry=pos.industry,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            gross_pnl=gross_pnl,
            total_cost=total_cost,
            net_pnl=net_pnl,
            gross_return=gross_return,
            net_return=net_return,
            exit_reason=reason,
            hold_days=hold_days,
            quality_score=pos.quality_score,
            value_score=pos.value_score,
            reversal_score=pos.reversal_score,
            lowvol_score=pos.lowvol_score,
            total_score=pos.total_score
        ))
        
        # 移除持仓
        self.positions.remove(pos)
    
    def _open_positions(self, date: str, position_ratio: float):
        """开仓"""
        # 计算目标持仓数量
        current_count = len(self.positions)
        target_count = int(self.strategy.max_positions * position_ratio)
        
        if current_count >= target_count:
            return
        
        available_slots = target_count - current_count
        
        # 检查资金
        position_value = self._calculate_position_value(date)
        total_value = self.capital + position_value
        target_position_value = total_value * position_ratio
        available_for_new = target_position_value - position_value
        
        if available_for_new <= 0 or self.capital < self.initial_capital * 0.1:
            return
        
        # 选股
        df = self.strategy.get_stock_data(date)
        if df.empty:
            return
        
        codes = df['code'].tolist()
        price_history = self.strategy.get_historical_prices(codes, date)
        
        df_factors = self.strategy.calculate_factors(df, price_history, date)
        if df_factors.empty:
            return
        
        df_neutral = self.strategy.industry_neutralize(df_factors)
        df_scores = self.strategy.calculate_scores(df_neutral)
        
        picks = self.strategy.select_stocks(df_scores, date)
        
        if not picks:
            return
        
        # 计算每只股票分配资金
        capital_per_stock = min(
            available_for_new / available_slots,
            self.capital * 0.95
        )
        
        for pick in picks:
            if len(self.positions) >= target_count:
                break
            if self.capital < capital_per_stock * 0.5:
                break
            
            entry_price = pick['price']
            shares = capital_per_stock / entry_price
            buy_amount = shares * entry_price
            
            # 计算买入成本
            costs = self.strategy.calculate_costs(buy_amount, is_buy=True)
            
            if self.capital >= costs['net_amount']:
                # 扣除资金
                self.capital -= costs['net_amount']
                
                # 记录成本
                self.cost_breakdown['total_commission'] += costs['commission']
                self.cost_breakdown['total_slippage'] += costs['slippage']
                self.cost_breakdown['total_transfer'] += costs['transfer']
                self.cost_breakdown['total_cost'] += costs['total']
                
                # 计算止损价
                stop_price = entry_price * (1 + self.strategy.stop_loss)
                
                # 计算退出日期
                trading_days = self.strategy.get_trading_days(
                    (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'),
                    (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
                )
                exit_date = trading_days[min(self.strategy.hold_days - 1, len(trading_days) - 1)] if trading_days else date
                
                # 添加持仓
                self.positions.append(Position(
                    code=pick['code'],
                    name=pick['name'],
                    industry=pick['industry'],
                    entry_date=date,
                    exit_date=exit_date,
                    entry_price=entry_price,
                    shares=shares,
                    cost_basis=costs['net_amount'],
                    stop_price=stop_price,
                    quality_score=pick['quality_score'],
                    value_score=pick['value_score'],
                    reversal_score=pick['reversal_score'],
                    lowvol_score=pick['lowvol_score'],
                    total_score=pick['score']
                ))
                
                # 更新冷却期
                self.strategy.recent_picks[pick['code']] = date
    
    def _get_price(self, code: str, date: str, price_type: str = 'close') -> Optional[float]:
        """获取价格"""
        with self.strategy.conn.cursor() as cursor:
            cursor.execute("""
                SELECT open, close FROM stock_kline 
                WHERE code = %s AND trade_date = %s
            """, (code, date))
            row = cursor.fetchone()
            if row:
                return float(row[0]) if price_type == 'open' else float(row[1])
        return None
    
    def _calculate_position_value(self, date: str) -> float:
        """计算持仓市值"""
        total = 0
        for pos in self.positions:
            price = self._get_price(pos.code, date, 'close')
            if price:
                total += pos.shares * price
        return total
    
    def _generate_report(self) -> Dict:
        """生成报告"""
        if not self.daily_stats:
            return {}
        
        # 基本统计
        initial = self.initial_capital
        final = self.daily_stats[-1].total_value
        total_return = (final - initial) / initial
        
        days = len(self.daily_stats)
        years = days / 252
        annual_return = (1 + total_return) ** (1/years) - 1 if years > 0 else 0
        
        # 最大回撤
        values = [d.total_value for d in self.daily_stats]
        running_max = np.maximum.accumulate(values)
        drawdowns = [(v - m) / m for v, m in zip(values, running_max)]
        max_drawdown = abs(min(drawdowns))
        
        # 交易统计
        if self.trades:
            win_trades = sum(1 for t in self.trades if t.net_pnl > 0)
            total_trades = len(self.trades)
            win_rate = win_trades / total_trades
            avg_return = np.mean([t.net_return for t in self.trades])
            
            # 盈亏比
            avg_win = np.mean([t.net_return for t in self.trades if t.net_return > 0]) if win_trades > 0 else 0
            avg_loss = np.mean([t.net_return for t in self.trades if t.net_return <= 0]) if win_trades < total_trades else 0
            profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
            
            # 退出原因统计
            stop_loss_exits = sum(1 for t in self.trades if t.exit_reason == 'stop_loss')
            time_exits = sum(1 for t in self.trades if t.exit_reason == 'time_exit')
        else:
            win_rate = avg_return = profit_factor = 0
            total_trades = stop_loss_exits = time_exits = 0
        
        # 夏普比率
        if len(values) > 1:
            daily_returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
            sharpe = np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252) if np.std(daily_returns) > 0 else 0
        else:
            sharpe = 0
        
        # 年度统计
        yearly_stats = self._calculate_yearly_stats()
        
        report = {
            'config': {
                'initial_capital': self.initial_capital,
                'score_threshold': self.strategy.score_threshold,
                'hold_days': self.strategy.hold_days,
                'stop_loss': self.strategy.stop_loss,
                'base_position': self.strategy.base_position,
                'bear_position': self.strategy.bear_position,
                'factor_weights': self.strategy.factor_weights
            },
            'performance': {
                'initial_capital': initial,
                'final_value': final,
                'total_return': round(total_return, 4),
                'annual_return': round(annual_return, 4),
                'max_drawdown': round(max_drawdown, 4),
                'sharpe_ratio': round(sharpe, 4),
                'trading_days': days,
                'years': round(years, 2)
            },
            'trading': {
                'total_trades': total_trades,
                'win_rate': round(win_rate, 4),
                'avg_trade_return': round(avg_return, 6),
                'profit_factor': round(profit_factor, 4),
                'stop_loss_exits': stop_loss_exits,
                'time_exits': time_exits
            },
            'costs': {
                k: round(v, 2) for k, v in self.cost_breakdown.items()
            },
            'yearly_stats': yearly_stats,
            'daily_values': [asdict(d) for d in self.daily_stats],
            'trades': [asdict(t) for t in self.trades]
        }
        
        # 打印报告
        logger.info("=" * 70)
        logger.info("V10重构版回测结果")
        logger.info("=" * 70)
        logger.info(f"初始资金: ¥{initial:,.0f}")
        logger.info(f"最终市值: ¥{final:,.0f}")
        logger.info(f"总收益: {total_return*100:.2f}%")
        logger.info(f"年化收益: {annual_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"夏普比率: {sharpe:.2f}")
        logger.info("-" * 70)
        logger.info(f"总交易: {total_trades} | 胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_return*100:.2f}% | 盈亏比: {profit_factor:.2f}")
        logger.info(f"止损退出: {stop_loss_exits} | 到期退出: {time_exits}")
        logger.info("-" * 70)
        logger.info("成本分析:")
        logger.info(f"  总成本: ¥{self.cost_breakdown['total_cost']:,.0f}")
        logger.info(f"  佣金: ¥{self.cost_breakdown['total_commission']:,.0f}")
        logger.info(f"  印花税: ¥{self.cost_breakdown['total_stamp_tax']:,.0f}")
        logger.info(f"  滑点: ¥{self.cost_breakdown['total_slippage']:,.0f}")
        logger.info("=" * 70)
        
        return report
    
    def _calculate_yearly_stats(self) -> Dict:
        """计算年度统计"""
        yearly = defaultdict(lambda: {'trades': 0, 'wins': 0, 'returns': []})
        
        for trade in self.trades:
            year = trade.entry_date[:4]
            yearly[year]['trades'] += 1
            if trade.net_pnl > 0:
                yearly[year]['wins'] += 1
            yearly[year]['returns'].append(trade.net_return)
        
        result = {}
        for year, stats in sorted(yearly.items()):
            result[year] = {
                'trades': stats['trades'],
                'win_rate': round(stats['wins'] / stats['trades'], 4) if stats['trades'] > 0 else 0,
                'avg_return': round(np.mean(stats['returns']), 6) if stats['returns'] else 0
            }
        
        return result


def main():
    """主函数"""
    logger.info("\n" + "="*70)
    logger.info("V12策略 V10重构版 - 回测启动")
    logger.info("="*70 + "\n")
    
    # 创建策略
    strategy = V12StrategyV10(
        score_threshold=60.0,
        max_positions=8,
        hold_days=10,
        stop_loss=-0.08,
        base_position=0.60,
        bear_position=0.30
    )
    
    # 创建回测引擎
    engine = V12BacktestV10(strategy, initial_capital=1000000)
    
    # 运行回测
    report = engine.run_backtest('2024-01-01', '2026-04-08')
    
    # 保存结果
    if report:
        output_file = 'v12_v10_reconstruction_result.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"\n结果已保存: {output_file}")


if __name__ == '__main__':
    main()
