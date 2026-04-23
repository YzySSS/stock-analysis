#!/usr/bin/env python3
"""
V12策略 V9-真实成本版
==================
完善的真实资金曲线，精确模拟实盘交易成本

改进：
1. 真实成本模型（佣金万2.5+印花税千0.5+过户费+滑点）
2. 交易成本追踪（每笔交易的详细成本 breakdown）
3. 流动性分级滑点（根据成交额动态调整）
4. 涨停跌停处理（买不进/卖不出）
5. 支持多周期对比（1日/3日/5日/周频）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


@dataclass
class Position:
    """持仓记录"""
    code: str
    name: str
    entry_date: str
    exit_date: str
    entry_price: float
    shares: float
    cost_basis: float  # 实际成本（含买入成本）
    score: float


@dataclass
class Trade:
    """交易记录（含详细成本）"""
    entry_date: str
    exit_date: str
    code: str
    name: str
    entry_price: float
    exit_price: float
    shares: float
    
    # 成本明细
    buy_commission: float
    buy_slippage: float
    buy_transfer_fee: float
    sell_commission: float
    sell_stamp_tax: float
    sell_slippage: float
    sell_transfer_fee: float
    
    # 收益计算
    gross_pnl: float  # 毛收益（不含成本）
    total_cost: float  # 总成本
    net_pnl: float  # 净收益
    gross_return: float  # 毛收益率
    net_return: float  # 净收益率
    hold_days: int
    score: float


@dataclass
class CostBreakdown:
    """成本分解"""
    total_trades: int = 0
    total_turnover: float = 0
    total_commission: float = 0
    total_stamp_tax: float = 0
    total_transfer_fee: float = 0
    total_slippage: float = 0
    total_cost: float = 0
    
    def cost_ratio(self) -> float:
        return self.total_cost / self.total_turnover if self.total_turnover > 0 else 0


class RealCostModel:
    """
    真实成本模型
    
    A股交易成本结构：
    - 佣金：万2.5，最低5元，双向
    - 印花税：千0.5，卖出单边
    - 过户费：十万分之一，双向
    - 滑点：根据流动性分级（0.1%-0.5%）
    """
    
    def __init__(self):
        # 固定成本率
        self.commission_rate = 0.00025  # 万2.5
        self.min_commission = 5.0  # 最低5元
        self.stamp_tax_rate = 0.0005  # 千0.5
        self.transfer_rate = 0.00001  # 十万分之一
        
        # 滑点分级
        self.slippage_tiers = {
            'high': 0.001,    # 日成交>1亿，千1
            'medium': 0.002,  # 日成交5千万-1亿，千2
            'low': 0.005,     # 日成交<5千万，千5
        }
    
    def calculate_slippage_rate(self, avg_daily_volume: float) -> float:
        """根据成交额确定滑点率"""
        if avg_daily_volume >= 100000000:  # >1亿
            return self.slippage_tiers['high']
        elif avg_daily_volume >= 50000000:  # 5千万-1亿
            return self.slippage_tiers['medium']
        else:
            return self.slippage_tiers['low']
    
    def calculate_buy_cost(self, amount: float, avg_volume: float) -> Tuple[float, float, float, float]:
        """
        计算买入成本
        返回: (实际买入金额, 佣金, 滑点, 过户费)
        """
        # 滑点
        slip_rate = self.calculate_slippage_rate(avg_volume)
        slippage = amount * slip_rate
        
        # 佣金（最低5元）
        commission = max(amount * self.commission_rate, self.min_commission)
        
        # 过户费
        transfer_fee = amount * self.transfer_rate
        
        # 实际成本（价格上升）
        actual_amount = amount + slippage + commission + transfer_fee
        
        return actual_amount, commission, slippage, transfer_fee
    
    def calculate_sell_cost(self, amount: float, avg_volume: float) -> Tuple[float, float, float, float, float]:
        """
        计算卖出成本
        返回: (实际卖出金额, 佣金, 印花税, 滑点, 过户费)
        """
        # 滑点
        slip_rate = self.calculate_slippage_rate(avg_volume)
        slippage = amount * slip_rate
        
        # 佣金（最低5元）
        commission = max(amount * self.commission_rate, self.min_commission)
        
        # 印花税（单边）
        stamp_tax = amount * self.stamp_tax_rate
        
        # 过户费
        transfer_fee = amount * self.transfer_rate
        
        # 实际到手（扣除所有成本）
        net_amount = amount - commission - stamp_tax - slippage - transfer_fee
        
        return net_amount, commission, stamp_tax, slippage, transfer_fee
    
    def get_cost_summary(self) -> Dict:
        """获取成本配置摘要"""
        return {
            'commission_rate': f'{self.commission_rate*10000:.1f}‱',
            'min_commission': f'¥{self.min_commission}',
            'stamp_tax_rate': f'{self.stamp_tax_rate*1000:.1f}‰',
            'transfer_rate': f'{self.transfer_rate*100000:.1f}‱',
            'slippage_tiers': {
                'high(>1亿)': f'{self.slippage_tiers["high"]*1000:.1f}‰',
                'medium(5千万-1亿)': f'{self.slippage_tiers["medium"]*1000:.1f}‰',
                'low(<5千万)': f'{self.slippage_tiers["low"]*1000:.1f}‰'
            }
        }


class V12BacktestRealCost:
    """
    V12回测 - 真实成本版
    """
    
    def __init__(self, 
                 initial_capital: float = 1000000,
                 max_positions: int = 5,
                 hold_days: int = 1,
                 position_ratio: float = 0.95,
                 liquidity_threshold: float = 50.0):  # 成交额门槛（百万）
        
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.position_ratio = position_ratio
        self.liquidity_threshold = liquidity_threshold
        
        # 成本模型
        self.cost_model = RealCostModel()
        self.cost_breakdown = CostBreakdown()
        
        # 状态
        self.capital = initial_capital
        self.positions: List[Position] = []
        self.trades: List[Trade] = []
        self.daily_values: List[Dict] = []
        self.recent_picks: Dict[str, str] = {}
        
        # 数据缓存
        self.price_data = {}
        self.fundamental = {}
        self.avg_volumes = {}  # 平均成交额（用于滑点计算）
        self.trading_days = []
    
    def load_data(self, start_date: str, end_date: str):
        """加载数据"""
        logger.info("加载数据...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 交易日
        cursor.execute("""
            SELECT DISTINCT trade_date FROM stock_kline
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """, (start_date, end_date))
        self.trading_days = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
        
        # 2. 价格数据 + 计算平均成交额
        cursor.execute("""
            SELECT code, trade_date, open, close, turnover, pct_change 
            FROM stock_kline
            WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 60 DAY) 
                                 AND DATE_ADD(%s, INTERVAL 30 DAY)
        """, (start_date, end_date))
        
        volume_data = defaultdict(list)
        
        for row in cursor.fetchall():
            code, date, open_p, close, turnover, pct = row
            date_str = date.strftime('%Y-%m-%d')
            if code not in self.price_data:
                self.price_data[code] = {}
            self.price_data[code][date_str] = {
                'open': float(open_p) if open_p else None,
                'close': float(close) if close else None,
                'turnover': float(turnover) if turnover else 0,
                'pct': float(pct) if pct else 0
            }
            volume_data[code].append(float(turnover) if turnover else 0)
        
        # 计算60日平均成交额
        for code, volumes in volume_data.items():
            if volumes:
                self.avg_volumes[code] = np.mean(volumes[-60:])  # 最近60日平均
        
        # 3. 基本面数据
        cursor.execute("""
            SELECT code, pe_score, roe_score, name 
            FROM stock_basic 
            WHERE pe_score IS NOT NULL AND roe_score IS NOT NULL
        """)
        for row in cursor.fetchall():
            self.fundamental[row[0]] = {
                'pe': float(row[1]),
                'roe': float(row[2]),
                'name': row[3] or ''
            }
        
        cursor.close()
        conn.close()
        
        logger.info(f"交易日: {len(self.trading_days)}天, 股票: {len(self.price_data)}, "
                   f"有基本面: {len(self.fundamental)}")
    
    def get_price(self, code: str, date: str, price_type: str = 'close') -> Optional[float]:
        """获取价格"""
        if code in self.price_data and date in self.price_data[code]:
            return self.price_data[code][date].get(price_type)
        return None
    
    def get_avg_volume(self, code: str) -> float:
        """获取平均成交额"""
        return self.avg_volumes.get(code, 0)
    
    def calculate_position_value(self, date: str) -> float:
        """计算当前持仓市值"""
        total = 0
        for pos in self.positions:
            price = self.get_price(pos.code, date, 'close')
            if price:
                total += pos.shares * price
        return total
    
    def calculate_total_value(self, date: str) -> float:
        """计算总市值（现金+持仓）"""
        position_value = self.calculate_position_value(date)
        return self.capital + position_value
    
    def is_limit_up(self, code: str, date: str) -> bool:
        """检查是否涨停"""
        if code not in self.price_data or date not in self.price_data[code]:
            return False
        return self.price_data[code][date].get('pct', 0) >= 9.5  # 接近涨停
    
    def is_limit_down(self, code: str, date: str) -> bool:
        """检查是否跌停"""
        if code not in self.price_data or date not in self.price_data[code]:
            return False
        return self.price_data[code][date].get('pct', 0) <= -9.5  # 接近跌停
    
    def select_stocks(self, date: str) -> List[Dict]:
        """选股（带流动性过滤）"""
        candidates = []
        
        for code, prices in self.price_data.items():
            if date not in prices:
                continue
            
            today = prices[date]
            if today['open'] is None or today['open'] < 5 or today['open'] > 150:
                continue
            if today['turnover'] < self.liquidity_threshold / 100:  # 百万转亿
                continue
            if code not in self.fundamental:
                continue
            
            # 涨停买不进
            if self.is_limit_up(code, date):
                continue
            
            # 冷却期检查
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                curr_date = datetime.strptime(date, '%Y-%m-%d')
                if (curr_date - last_date).days <= 3:  # 3天冷却期
                    continue
            
            avg_vol = self.get_avg_volume(code)
            slippage_rate = self.cost_model.calculate_slippage_rate(avg_vol)
            
            candidates.append({
                'code': code,
                'name': self.fundamental[code]['name'],
                'price': today['open'],
                'turnover': today['turnover'],
                'avg_volume': avg_vol,
                'slippage_rate': slippage_rate,
                'quality': self.fundamental[code]['roe'],
                'valuation': self.fundamental[code]['pe']
            })
        
        if len(candidates) < 10:
            return []
        
        # Z-score评分
        for key in ['quality', 'valuation']:
            values = [c[key] for c in candidates if c[key] is not None]
            if len(values) > 0:
                mean, std = np.mean(values), np.std(values)
                for c in candidates:
                    if c[key] is not None:
                        c[f'{key}_z'] = (c[key] - mean) / std if std > 0 else 0
                    else:
                        c[f'{key}_z'] = 0
        
        for c in candidates:
            c['score'] = 50 + (c['quality_z'] * 0.6 + c['valuation_z'] * 0.4) * 15
        
        candidates.sort(key=lambda x: x['score'], reverse=True)
        picks = [c for c in candidates if c['score'] >= 55][:self.max_positions]
        
        return picks
    
    def get_exit_date(self, entry_date: str) -> Optional[str]:
        """计算退出日期"""
        try:
            idx = self.trading_days.index(entry_date)
            exit_idx = idx + self.hold_days
            if exit_idx < len(self.trading_days):
                return self.trading_days[exit_idx]
        except ValueError:
            pass
        return None
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        logger.info(f"="*60)
        logger.info(f"V9真实成本回测: {start_date} ~ {end_date}")
        logger.info(f"初始资金: {self.initial_capital:,.0f}, 持有期: {self.hold_days}天")
        logger.info(f"成本配置: {json.dumps(self.cost_model.get_cost_summary(), ensure_ascii=False)}")
        logger.info(f"="*60)
        
        self.load_data(start_date, end_date)
        
        if not self.trading_days:
            logger.error("无交易日数据")
            return {}
        
        # 每日迭代
        for i, date in enumerate(self.trading_days):
            if i < 20:  # 跳过前20天（确保有历史数据）
                continue
            
            if i % 50 == 0:
                logger.info(f"进度: {i}/{len(self.trading_days)} ({i/len(self.trading_days)*100:.1f}%)")
            
            # 1. 检查持仓到期，平仓
            for pos in self.positions[:]:  # 复制列表避免修改问题
                if pos.exit_date == date:
                    exit_price = self.get_price(pos.code, date, 'open')
                    
                    if exit_price and not self.is_limit_down(pos.code, date):
                        # 卖出金额
                        sale_amount = pos.shares * exit_price
                        avg_volume = self.get_avg_volume(pos.code)
                        
                        # 计算卖出成本
                        net_amount, commission, stamp_tax, slippage, transfer_fee = \
                            self.cost_model.calculate_sell_cost(sale_amount, avg_volume)
                        
                        # 更新资金
                        self.capital += net_amount
                        
                        # 记录成本
                        self.cost_breakdown.total_trades += 1
                        self.cost_breakdown.total_turnover += sale_amount * 2  # 买卖双边
                        self.cost_breakdown.total_commission += commission + pos.cost_basis - (pos.shares * pos.entry_price)
                        self.cost_breakdown.total_stamp_tax += stamp_tax
                        self.cost_breakdown.total_slippage += slippage
                        self.cost_breakdown.total_transfer_fee += transfer_fee
                        
                        # 毛收益（不含成本）
                        gross_pnl = sale_amount - (pos.shares * pos.entry_price)
                        # 总成本
                        total_cost = (pos.cost_basis - pos.shares * pos.entry_price) + \
                                    (sale_amount - net_amount)
                        # 净收益
                        net_pnl = net_amount - pos.cost_basis
                        
                        gross_return = gross_pnl / pos.cost_basis
                        net_return = net_pnl / pos.cost_basis
                        
                        hold_days = (datetime.strptime(date, '%Y-%m-%d') - 
                                    datetime.strptime(pos.entry_date, '%Y-%m-%d')).days
                        
                        self.trades.append(Trade(
                            entry_date=pos.entry_date,
                            exit_date=date,
                            code=pos.code,
                            name=pos.name,
                            entry_price=pos.entry_price,
                            exit_price=exit_price,
                            shares=pos.shares,
                            buy_commission=pos.cost_basis - pos.shares * pos.entry_price,
                            buy_slippage=0,  # 已计入cost_basis
                            buy_transfer_fee=0,
                            sell_commission=commission,
                            sell_stamp_tax=stamp_tax,
                            sell_slippage=slippage,
                            sell_transfer_fee=transfer_fee,
                            gross_pnl=gross_pnl,
                            total_cost=total_cost,
                            net_pnl=net_pnl,
                            gross_return=gross_return,
                            net_return=net_return,
                            hold_days=hold_days,
                            score=pos.score
                        ))
                    
                    self.positions.remove(pos)
            
            # 2. 选股并开仓
            available_slots = self.max_positions - len(self.positions)
            
            if available_slots > 0 and self.capital > self.initial_capital * 0.1:
                picks = self.select_stocks(date)
                
                # 计算每只股可分配的资金
                position_value = self.calculate_position_value(date)
                total_value = self.capital + position_value
                target_position_value = total_value * self.position_ratio
                available_for_new = target_position_value - position_value
                
                if available_for_new > 0:
                    capital_per_stock = min(
                        available_for_new / available_slots,
                        self.capital * 0.95
                    )
                    
                    for pick in picks:
                        if len(self.positions) >= self.max_positions:
                            break
                        if self.capital < capital_per_stock * 0.5:
                            break
                        
                        entry_price = pick['price']
                        target_shares = capital_per_stock / entry_price
                        buy_amount = target_shares * entry_price
                        
                        # 计算买入成本
                        actual_cost, commission, slippage, transfer_fee = \
                            self.cost_model.calculate_buy_cost(buy_amount, pick['avg_volume'])
                        
                        # 检查资金是否足够
                        if self.capital >= actual_cost:
                            shares = target_shares
                            
                            # 扣除资金
                            self.capital -= actual_cost
                            
                            # 计算退出日期
                            exit_date = self.get_exit_date(date)
                            if exit_date:
                                self.positions.append(Position(
                                    code=pick['code'],
                                    name=pick['name'],
                                    entry_date=date,
                                    exit_date=exit_date,
                                    entry_price=entry_price,
                                    shares=shares,
                                    cost_basis=actual_cost,
                                    score=pick['score']
                                ))
                                
                                self.recent_picks[pick['code']] = date
            
            # 3. 记录每日市值
            total_value = self.calculate_total_value(date)
            position_value = self.calculate_position_value(date)
            
            self.daily_values.append({
                'date': date,
                'total_value': total_value,
                'cash': self.capital,
                'position_value': position_value,
                'position_count': len(self.positions)
            })
        
        return self.generate_report()
    
    def generate_report(self) -> Dict:
        """生成报告"""
        if not self.daily_values:
            logger.warning("无回测数据")
            return {}
        
        # 基本统计
        initial = self.initial_capital
        final = self.daily_values[-1]['total_value']
        total_return = (final - initial) / initial
        
        days = len(self.daily_values)
        years = days / 252
        annual_return = (1 + total_return) ** (1/years) - 1 if years > 0 else 0
        
        # 最大回撤
        values = [d['total_value'] for d in self.daily_values]
        running_max = np.maximum.accumulate(values)
        drawdowns = [(v - m) / m for v, m in zip(values, running_max)]
        max_drawdown = abs(min(drawdowns))
        
        # 交易统计（基于净收益）
        if self.trades:
            win_trades = sum(1 for t in self.trades if t.net_pnl > 0)
            total_trades = len(self.trades)
            win_rate = win_trades / total_trades
            avg_return = np.mean([t.net_return for t in self.trades])
            
            avg_win = np.mean([t.net_return for t in self.trades if t.net_return > 0]) if win_trades > 0 else 0
            avg_loss = np.mean([t.net_return for t in self.trades if t.net_return <= 0]) if win_trades < total_trades else 0
            profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
            
            # 成本统计
            avg_cost_per_trade = np.mean([t.total_cost for t in self.trades])
            avg_cost_ratio = avg_cost_per_trade / np.mean([t.shares * t.entry_price for t in self.trades]) if self.trades else 0
        else:
            win_rate = avg_return = profit_factor = avg_cost_ratio = 0
            total_trades = 0
        
        # 夏普比率
        if len(values) > 1:
            daily_returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
            sharpe = np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252) if np.std(daily_returns) > 0 else 0
        else:
            sharpe = 0
        
        report = {
            'config': {
                'initial_capital': self.initial_capital,
                'max_positions': self.max_positions,
                'hold_days': self.hold_days,
                'liquidity_threshold': self.liquidity_threshold
            },
            'cost_config': self.cost_model.get_cost_summary(),
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
                'avg_cost_ratio': round(avg_cost_ratio, 6)
            },
            'cost_analysis': {
                'total_turnover': round(self.cost_breakdown.total_turnover, 2),
                'total_commission': round(self.cost_breakdown.total_commission, 2),
                'total_stamp_tax': round(self.cost_breakdown.total_stamp_tax, 2),
                'total_transfer_fee': round(self.cost_breakdown.total_transfer_fee, 2),
                'total_slippage': round(self.cost_breakdown.total_slippage, 2),
                'total_cost': round(self.cost_breakdown.total_cost, 2),
                'cost_ratio': round(self.cost_breakdown.cost_ratio(), 6)
            },
            'daily_values': self.daily_values,
            'trades': [
                {
                    'entry_date': t.entry_date,
                    'exit_date': t.exit_date,
                    'code': t.code,
                    'name': t.name,
                    'entry_price': t.entry_price,
                    'exit_price': t.exit_price,
                    'shares': round(t.shares, 2),
                    'gross_return': round(t.gross_return, 6),
                    'net_return': round(t.net_return, 6),
                    'total_cost': round(t.total_cost, 2),
                    'hold_days': t.hold_days
                } for t in self.trades
            ]
        }
        
        # 打印报告
        logger.info("=" * 70)
        logger.info("V9真实成本回测结果")
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
        logger.info("-" * 70)
        logger.info("成本分析:")
        logger.info(f"  总成交额: ¥{self.cost_breakdown.total_turnover:,.0f}")
        logger.info(f"  总成本: ¥{self.cost_breakdown.total_cost:,.0f}")
        logger.info(f"  成本率: {self.cost_breakdown.cost_ratio()*100:.3f}%")
        logger.info(f"  佣金: ¥{self.cost_breakdown.total_commission:,.0f}")
        logger.info(f"  印花税: ¥{self.cost_breakdown.total_stamp_tax:,.0f}")
        logger.info(f"  滑点: ¥{self.cost_breakdown.total_slippage:,.0f}")
        logger.info("=" * 70)
        
        return report


def main():
    """主函数：对比多周期"""
    results = {}
    
    # 测试多个持有周期
    configs = [
        {'hold_days': 1, 'name': '1日'},
        {'hold_days': 3, 'name': '3日'},
        {'hold_days': 5, 'name': '5日'},
    ]
    
    for config in configs:
        logger.info(f"\n{'='*70}")
        logger.info(f"开始回测: {config['name']}持有期")
        logger.info(f"{'='*70}\n")
        
        engine = V12BacktestRealCost(
            initial_capital=1000000,
            max_positions=5,
            hold_days=config['hold_days'],
            liquidity_threshold=50.0  # 日成交>5000万
        )
        
        report = engine.run_backtest('2024-01-01', '2026-04-08')
        results[config['name']] = report
        
        output_file = f'v12_v9_realcost_{config["name"]}.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n结果已保存: {output_file}\n")
    
    # 汇总对比
    logger.info("\n" + "=" * 80)
    logger.info("V9真实成本策略 多周期对比")
    logger.info("=" * 80)
    logger.info(f"{'周期':<6} {'总收益':>10} {'年化':>10} {'回撤':>10} {'夏普':>8} {'胜率':>8} {'交易次数':>8}")
    logger.info("-" * 80)
    
    for name, r in results.items():
        if r:
            p = r['performance']
            t = r['trading']
            logger.info(f"{name:<6} {p['total_return']*100:>9.2f}% {p['annual_return']*100:>9.1f}% "
                       f"{p['max_drawdown']*100:>9.1f}% {p['sharpe_ratio']:>8.2f} "
                       f"{t['win_rate']*100:>7.1f}% {t['total_trades']:>8}")
    
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
