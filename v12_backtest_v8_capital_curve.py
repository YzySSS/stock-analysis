#!/usr/bin/env python3
"""
V12策略 V8-资金曲线版
======================
构建真实的资金曲线，模拟实盘交易

特点：
1. 初始资金100万
2. 同时最多持有5只股票
3. 每只股分配等资金
4. 持仓到期后释放资金
5. 每日计算总市值
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
from typing import List, Dict, Optional
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
    cost: float
    score: float


@dataclass
class Trade:
    """交易记录"""
    entry_date: str
    exit_date: str
    code: str
    name: str
    entry_price: float
    exit_price: float
    shares: float
    pnl: float
    return_pct: float
    hold_days: int
    score: float


class V12BacktestCapitalCurve:
    """
    V12回测 - 真实资金曲线版
    """
    
    def __init__(self, 
                 initial_capital: float = 1000000,
                 max_positions: int = 5,
                 hold_days: int = 1,
                 position_ratio: float = 0.9):  # 仓位比例90%
        
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.position_ratio = position_ratio
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        # 状态
        self.capital = initial_capital  # 可用现金
        self.positions: List[Position] = []  # 当前持仓
        self.trades: List[Trade] = []  # 历史交易
        self.daily_values: List[Dict] = []  # 每日市值记录
        self.recent_picks: Dict[str, str] = {}  # 冷却期记录
        
        # 数据缓存
        self.price_data = {}
        self.fundamental = {}
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
        
        # 2. 价格数据（扩展日期范围用于计算持有期收益）
        cursor.execute("""
            SELECT code, trade_date, open, close, turnover, pct_change 
            FROM stock_kline
            WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 30 DAY) 
                                 AND DATE_ADD(%s, INTERVAL 30 DAY)
        """, (start_date, end_date))
        
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
        
        logger.info(f"交易日: {len(self.trading_days)}天, 股票: {len(self.price_data)}, 有基本面: {len(self.fundamental)}")
    
    def get_price(self, code: str, date: str, price_type: str = 'close') -> Optional[float]:
        """获取价格"""
        if code in self.price_data and date in self.price_data[code]:
            return self.price_data[code][date].get(price_type)
        return None
    
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
    
    def select_stocks(self, date: str) -> List[Dict]:
        """选股"""
        candidates = []
        
        for code, prices in self.price_data.items():
            if date not in prices:
                continue
            
            today = prices[date]
            if today['open'] is None or today['open'] < 5 or today['open'] > 150:
                continue
            if today['turnover'] < 0.5:
                continue
            if code not in self.fundamental:
                continue
            
            # 冷却期检查
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                curr_date = datetime.strptime(date, '%Y-%m-%d')
                if (curr_date - last_date).days <= 5:
                    continue
            
            candidates.append({
                'code': code,
                'name': self.fundamental[code]['name'],
                'price': today['open'],
                'quality': self.fundamental[code]['roe'],
                'valuation': self.fundamental[code]['pe']
            })
        
        if len(candidates) < 10:
            return []
        
        # Z-score评分
        for key in ['quality', 'valuation']:
            values = [c[key] for c in candidates]
            mean, std = np.mean(values), np.std(values)
            for c in candidates:
                c[f'{key}_z'] = (c[key] - mean) / std if std > 0 else 0
        
        for c in candidates:
            c['score'] = 50 + (c['quality_z'] * 0.5 + c['valuation_z'] * 0.5) * 15
        
        candidates.sort(key=lambda x: x['score'], reverse=True)
        picks = [c for c in candidates if c['score'] >= 50][:5]
        
        return picks
    
    def get_exit_date(self, entry_date: str) -> str:
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
        logger.info(f"V8资金曲线回测: {start_date} ~ {end_date}")
        logger.info(f"初始资金: {self.initial_capital:,.0f}, 持有期: {self.hold_days}天")
        logger.info(f"="*60)
        
        # 加载数据
        self.load_data(start_date, end_date)
        
        if not self.trading_days:
            logger.error("无交易日数据")
            return {}
        
        # 每日迭代
        for i, date in enumerate(self.trading_days):
            if i < 5:  # 跳过前几天
                continue
            
            if i % 50 == 0:
                logger.info(f"进度: {i}/{len(self.trading_days)} ({i/len(self.trading_days)*100:.1f}%)")
            
            # 1. 检查持仓到期，平仓
            for pos in self.positions[:]:
                if pos.exit_date == date:
                    # 卖出
                    exit_price = self.get_price(pos.code, date, 'open' if self.hold_days == 1 else 'close')
                    if exit_price:
                        # 扣除卖出成本
                        sale_value = pos.shares * exit_price
                        commission = sale_value * self.commission_rate
                        stamp_tax = sale_value * self.stamp_tax_rate
                        slippage = sale_value * self.slippage
                        
                        net_sale_value = sale_value - commission - stamp_tax - slippage
                        self.capital += net_sale_value
                        
                        # 记录交易
                        pnl = net_sale_value - (pos.shares * pos.entry_price + pos.cost)
                        return_pct = pnl / (pos.shares * pos.entry_price + pos.cost)
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
                            pnl=pnl,
                            return_pct=return_pct,
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
                        self.capital * 0.95  # 留5%现金
                    )
                    
                    for pick in picks:
                        if len(self.positions) >= self.max_positions:
                            break
                        if self.capital < capital_per_stock * 0.5:
                            break
                        
                        entry_price = pick['price']
                        shares = capital_per_stock / entry_price
                        
                        # 买入成本
                        buy_value = shares * entry_price
                        commission = buy_value * self.commission_rate
                        slippage = buy_value * self.slippage
                        total_cost = commission + slippage
                        
                        # 扣除资金和成本
                        self.capital -= (buy_value + total_cost)
                        
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
                                cost=total_cost,
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
        
        # 年化收益
        days = len(self.daily_values)
        years = days / 252
        annual_return = (1 + total_return) ** (1/years) - 1 if years > 0 else 0
        
        # 计算最大回撤
        values = [d['total_value'] for d in self.daily_values]
        running_max = np.maximum.accumulate(values)
        drawdowns = [(v - m) / m for v, m in zip(values, running_max)]
        max_drawdown = abs(min(drawdowns))
        
        # 交易统计
        if self.trades:
            win_trades = sum(1 for t in self.trades if t.pnl > 0)
            total_trades = len(self.trades)
            win_rate = win_trades / total_trades
            avg_return = np.mean([t.return_pct for t in self.trades])
            
            # 盈亏比
            avg_win = np.mean([t.return_pct for t in self.trades if t.return_pct > 0]) if win_trades > 0 else 0
            avg_loss = np.mean([t.return_pct for t in self.trades if t.return_pct <= 0]) if win_trades < total_trades else 0
            profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        else:
            win_rate = avg_return = profit_factor = 0
            total_trades = 0
        
        # 夏普比率（简化版）
        if len(values) > 1:
            daily_returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
            sharpe = np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252) if np.std(daily_returns) > 0 else 0
        else:
            sharpe = 0
        
        report = {
            'initial_capital': self.initial_capital,
            'final_value': final,
            'total_return': round(total_return, 4),
            'annual_return': round(annual_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'sharpe_ratio': round(sharpe, 4),
            'total_trades': total_trades,
            'win_rate': round(win_rate, 4),
            'avg_trade_return': round(avg_return, 6),
            'profit_factor': round(profit_factor, 4),
            'daily_values': self.daily_values,
            'trades': [
                {
                    'entry_date': t.entry_date,
                    'exit_date': t.exit_date,
                    'code': t.code,
                    'name': t.name,
                    'return_pct': round(t.return_pct, 6),
                    'pnl': round(t.pnl, 2),
                    'hold_days': t.hold_days
                } for t in self.trades
            ]
        }
        
        logger.info("=" * 60)
        logger.info("资金曲线回测结果")
        logger.info("=" * 60)
        logger.info(f"初始资金: {initial:,.0f}")
        logger.info(f"最终市值: {final:,.0f}")
        logger.info(f"总收益: {total_return*100:.2f}%")
        logger.info(f"年化收益: {annual_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"夏普比率: {sharpe:.2f}")
        logger.info("-" * 60)
        logger.info(f"总交易: {total_trades} | 胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_return*100:.2f}% | 盈亏比: {profit_factor:.2f}")
        logger.info("=" * 60)
        
        return report


def main():
    results = {}
    
    for hold_days in [1, 3, 5]:
        engine = V12BacktestCapitalCurve(
            initial_capital=1000000,
            max_positions=5,
            hold_days=hold_days
        )
        
        report = engine.run_backtest('2024-01-01', '2026-04-08')
        results[f'{hold_days}d'] = report
        
        output_file = f'v12_v8_capital_{hold_days}d.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n{hold_days}日持有结果: {output_file}\n")
    
    # 汇总
    logger.info("\n" + "=" * 70)
    logger.info("V8策略 资金曲线汇总")
    logger.info("=" * 70)
    for key, r in results.items():
        if r:
            logger.info(f"{key}: 收益{r['total_return']*100:.2f}% | 年化{r['annual_return']*100:.1f}% | 回撤{r['max_drawdown']*100:.1f}% | 夏普{r['sharpe_ratio']:.2f}")


if __name__ == '__main__':
    main()
