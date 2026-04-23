#!/usr/bin/env python3
"""
V12策略 V11_IC_Optimized 回测引擎
==================================
基于IC分析的3因子策略回测
- Turnover 35% + LowVol 35% + Reversal 30%
- 3日持仓周期
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict
import pymysql

from v12_strategy_v11_ic_optimized import V12StrategyV11ICOptimized, TradeRecord

# 设置日志
os.makedirs('backtest_results/v11_ic', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('backtest_results/v11_ic/backtest_2024_2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class V12BacktestV11:
    """V11回测引擎"""
    
    def __init__(self, strategy: V12StrategyV11ICOptimized, initial_capital: float = 1000000):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.capital = initial_capital
        
        # 持仓
        self.positions = {}  # code -> {entry_date, entry_price, shares, hold_days}
        self.cooling_stocks = {}  # code -> end_date
        
        # 记录
        self.trades = []
        self.daily_values = []
        
        # 成本
        self.total_commission = 0
        self.total_stamp_tax = 0
        self.total_slippage = 0
        
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def get_price(self, code: str, date: str, price_type: str = 'close') -> float:
        """获取指定日期的价格"""
        sql = """
        SELECT open, close, high, low FROM stock_kline 
        WHERE code = %s AND trade_date = %s
        """
        self.cursor.execute(sql, (code, date))
        row = self.cursor.fetchone()
        if row:
            return float(row[price_type])
        return None
    
    def get_next_trading_day(self, date: str, offset: int = 1) -> str:
        """获取下一个交易日"""
        sql = """
        SELECT trade_date FROM (
            SELECT DISTINCT trade_date FROM stock_kline 
            WHERE trade_date > %s 
            ORDER BY trade_date 
            LIMIT %s
        ) t ORDER BY trade_date DESC LIMIT 1
        """
        self.cursor.execute(sql, (date, offset))
        row = self.cursor.fetchone()
        if row:
            return str(row['trade_date'])
        return None
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        sql = """
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        self.cursor.execute(sql, (start_date, end_date))
        return [str(row['trade_date']) for row in self.cursor.fetchall()]
    
    def calculate_cost(self, amount: float, is_buy: bool) -> Dict:
        """计算交易成本"""
        commission = max(5, amount * self.strategy.commission_rate)
        stamp_tax = amount * self.strategy.stamp_tax_rate if not is_buy else 0
        slippage = amount * self.strategy.slippage_rate
        
        return {
            'commission': commission,
            'stamp_tax': stamp_tax,
            'slippage': slippage,
            'total': commission + stamp_tax + slippage
        }
    
    def update_cooling_list(self, current_date: str):
        """更新冷却期列表"""
        expired = []
        for code, end_date in self.cooling_stocks.items():
            if current_date > end_date:
                expired.append(code)
        for code in expired:
            del self.cooling_stocks[code]
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        logger.info(f"开始回测: {start_date} 至 {end_date}")
        logger.info(f"策略: V11_IC_Optimized (3日持仓)")
        logger.info(f"初始资金: {self.initial_capital:,.0f}")
        
        self.connect_db()
        self.strategy.connect_db()  # 策略也需要连接数据库
        trading_days = self.get_trading_days(start_date, end_date)
        logger.info(f"交易日数量: {len(trading_days)}")
        
        for i, date in enumerate(trading_days):
            # 更新冷却期
            self.update_cooling_list(date)
            
            # 处理持仓（止损/到期）
            self._check_positions(date)
            
            # 选股日：每3天选股一次（与持仓周期匹配）
            if i % self.strategy.hold_days == 0:
                cooling_list = list(self.cooling_stocks.keys())
                selected = self.strategy.select_stocks(date, cooling_list)
                
                if selected and len(self.positions) < self.strategy.max_positions:
                    self._open_positions(date, selected)
            
            # 记录每日净值
            self._record_daily_value(date)
        
        # 平仓所有持仓
        if trading_days:
            self._close_all_positions(trading_days[-1])
        
        self.close_db()
        self.strategy.close_db()  # 策略也需要关闭数据库
        
        return self._generate_report(start_date, end_date)
    
    def _check_positions(self, date: str):
        """检查持仓（止损/到期）"""
        to_close = []
        
        for code, pos in self.positions.items():
            current_price = self.get_price(code, date, 'close')
            if current_price is None:
                continue
            
            # 计算收益率
            ret = (current_price - pos['entry_price']) / pos['entry_price']
            hold_days = pos['hold_days'] + 1
            
            # 止损检查
            if ret <= self.strategy.stop_loss:
                to_close.append((code, 'stop_loss', current_price))
            # 时间到期
            elif hold_days >= self.strategy.hold_days:
                to_close.append((code, 'time_exit', current_price))
            else:
                self.positions[code]['hold_days'] = hold_days
        
        for code, reason, price in to_close:
            self._close_position(code, date, price, reason)
    
    def _open_positions(self, date: str, selected: List[Dict]):
        """开仓"""
        available_slots = self.strategy.max_positions - len(self.positions)
        if available_slots <= 0:
            return
        
        # 分配资金
        position_capital = self.capital * self.strategy.base_position / max(1, len(selected))
        
        for stock in selected[:available_slots]:
            code = stock['code']
            if code in self.positions:
                continue
            
            # 次日开盘价买入
            entry_date = self.get_next_trading_day(date)
            if not entry_date:
                continue
            
            entry_price = self.get_price(code, entry_date, 'open')
            if entry_price is None or entry_price <= 0:
                continue
            
            # 计算股数
            shares = int(position_capital / entry_price / 100) * 100
            if shares < 100:
                continue
            
            amount = shares * entry_price
            cost = self.calculate_cost(amount, is_buy=True)
            
            self.positions[code] = {
                'entry_date': entry_date,
                'entry_price': entry_price,
                'shares': shares,
                'hold_days': 0,
                'factors': stock
            }
            
            self.capital -= amount + cost['total']
            self.total_commission += cost['commission']
            self.total_slippage += cost['slippage']
            
            logger.info(f"[买入] {code} @ {entry_price:.2f}, 股数: {shares}, 得分: {stock['score']:.1f}")
    
    def _close_position(self, code: str, date: str, price: float, reason: str):
        """平仓"""
        if code not in self.positions:
            return
        
        pos = self.positions[code]
        amount = pos['shares'] * price
        cost = self.calculate_cost(amount, is_buy=False)
        
        # 计算收益
        entry_amount = pos['shares'] * pos['entry_price']
        gross_pnl = amount - entry_amount
        total_cost = cost['total'] + pos.get('entry_cost', 0)
        net_pnl = gross_pnl - total_cost
        
        gross_return = gross_pnl / entry_amount
        net_return = net_pnl / entry_amount
        
        # 记录交易
        trade = TradeRecord(
            entry_date=pos['entry_date'],
            exit_date=date,
            code=code,
            name=pos['factors'].get('name', code),
            industry=pos['factors'].get('industry', '未知'),
            entry_price=pos['entry_price'],
            exit_price=price,
            shares=pos['shares'],
            gross_pnl=gross_pnl,
            total_cost=total_cost,
            net_pnl=net_pnl,
            gross_return=gross_return,
            net_return=net_return,
            exit_reason=reason,
            hold_days=pos['hold_days'],
            turnover_score=pos['factors'].get('turnover_score', 0),
            lowvol_score=pos['factors'].get('lowvol_score', 0),
            reversal_score=pos['factors'].get('reversal_score', 0),
            total_score=pos['factors'].get('score', 0)
        )
        self.trades.append(trade)
        
        # 更新资金
        self.capital += amount - cost['total']
        self.total_commission += cost['commission']
        self.total_stamp_tax += cost['stamp_tax']
        self.total_slippage += cost['slippage']
        
        # 加入冷却期
        cooling_end = self.get_next_trading_day(date, self.strategy.cooling_days)
        if cooling_end:
            self.cooling_stocks[code] = cooling_end
        
        del self.positions[code]
        
        logger.info(f"[卖出] {code} @ {price:.2f}, 收益: {net_return*100:.2f}%, 原因: {reason}")
        
        # 定期输出进度
        if len(self.trades) % 50 == 0:
            win_trades = len([t for t in self.trades if t.net_return > 0])
            win_rate = win_trades / len(self.trades) if self.trades else 0
            logger.info(f"*** 进度: 已完成{len(self.trades)}笔交易, 当前胜率: {win_rate*100:.1f}% ***")
    
    def _close_all_positions(self, date: str):
        """平仓所有持仓"""
        for code in list(self.positions.keys()):
            price = self.get_price(code, date, 'close')
            if price:
                self._close_position(code, date, price, 'backtest_end')
    
    def _record_daily_value(self, date: str):
        """记录每日净值"""
        position_value = 0
        for code, pos in self.positions.items():
            price = self.get_price(code, date, 'close')
            if price:
                position_value += pos['shares'] * price
        
        total_value = self.capital + position_value
        self.daily_values.append({
            'date': date,
            'capital': self.capital,
            'position_value': position_value,
            'total_value': total_value
        })
    
    def _generate_report(self, start_date: str, end_date: str) -> Dict:
        """生成回测报告"""
        if not self.trades:
            logger.warning("没有交易记录")
            return {}
        
        trades_df = pd.DataFrame([{
            'entry_date': t.entry_date,
            'exit_date': t.exit_date,
            'code': t.code,
            'gross_return': t.gross_return,
            'net_return': t.net_return,
            'exit_reason': t.exit_reason,
            'hold_days': t.hold_days
        } for t in self.trades])
        
        # 基础统计
        total_trades = len(self.trades)
        win_trades = len([t for t in self.trades if t.net_return > 0])
        loss_trades = total_trades - win_trades
        win_rate = win_trades / total_trades if total_trades > 0 else 0
        
        gross_returns = trades_df['gross_return'].tolist()
        net_returns = trades_df['net_return'].tolist()
        
        avg_gross_return = np.mean(gross_returns)
        avg_net_return = np.mean(net_returns)
        
        # 计算最大回撤
        values = [v['total_value'] for v in self.daily_values]
        max_drawdown = 0
        peak = values[0] if values else self.initial_capital
        for v in values:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > max_drawdown:
                max_drawdown = dd
        
        # 计算年化收益
        final_value = values[-1] if values else self.initial_capital
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        days = len(self.daily_values)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0
        
        report = {
            'strategy': 'V11_IC_Optimized',
            'period': f"{start_date} to {end_date}",
            'trading_days': days,
            'initial_capital': self.initial_capital,
            'final_capital': final_value,
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'total_trades': total_trades,
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': win_rate,
            'avg_gross_return': avg_gross_return,
            'avg_net_return': avg_net_return,
            'avg_hold_days': np.mean([t.hold_days for t in self.trades]),
            'total_commission': self.total_commission,
            'total_stamp_tax': self.total_stamp_tax,
            'total_slippage': self.total_slippage,
            'total_cost': self.total_commission + self.total_stamp_tax + self.total_slippage,
            'trades': [{
                'entry_date': t.entry_date,
                'exit_date': t.exit_date,
                'code': t.code,
                'entry_price': t.entry_price,
                'exit_price': t.exit_price,
                'net_return': t.net_return,
                'exit_reason': t.exit_reason
            } for t in self.trades]
        }
        
        # 保存报告
        os.makedirs('backtest_results/v11_ic', exist_ok=True)
        
        with open('backtest_results/v11_ic/summary.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        trades_df.to_csv('backtest_results/v11_ic/trades.csv', index=False)
        
        # 打印摘要
        logger.info("\n" + "="*60)
        logger.info("V11_IC_Optimized 回测结果")
        logger.info("="*60)
        logger.info(f"回测周期: {start_date} ~ {end_date}")
        logger.info(f"初始资金: {self.initial_capital:,.0f}")
        logger.info(f"最终资金: {final_value:,.0f}")
        logger.info(f"总收益率: {total_return*100:.2f}%")
        logger.info(f"年化收益: {annual_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"交易次数: {total_trades}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益(毛): {avg_gross_return*100:.3f}%")
        logger.info(f"平均收益(净): {avg_net_return*100:.3f}%")
        logger.info(f"总成本: {report['total_cost']:,.2f}")
        logger.info("="*60)
        
        return report


if __name__ == '__main__':
    # 创建策略
    strategy = V12StrategyV11ICOptimized(
        score_threshold=45.0,  # 降低阈值
        max_positions=5,
        hold_days=3,
        stop_loss=-0.08
    )
    
    # 运行回测 (2024年Q1 - 3个月快速测试)
    backtest = V12BacktestV11(strategy, initial_capital=1000000)
    result = backtest.run_backtest('2024-01-02', '2024-03-31')
