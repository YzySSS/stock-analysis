#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测系统 - 验证技术指标策略的历史表现
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from technical_analysis import TechnicalAnalyzer
import json


@dataclass
class Trade:
    """单笔交易记录"""
    date: datetime
    action: str  # 'buy' 或 'sell'
    price: float
    shares: int
    value: float
    signal: str  # 触发信号


@dataclass
class BacktestResult:
    """回测结果"""
    # 基本收益
    total_return: float  # 总收益率
    annual_return: float  # 年化收益率
    
    # 风险指标
    max_drawdown: float  # 最大回撤
    volatility: float  # 波动率
    sharpe_ratio: float  # 夏普比率
    
    # 交易统计
    total_trades: int  # 总交易次数
    win_rate: float  # 胜率
    profit_factor: float  # 盈亏比
    
    # 持仓
    final_value: float  # 最终资产
    trades: List[Trade]  # 交易记录
    daily_values: pd.DataFrame  # 每日资产曲线
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_return': f"{self.total_return:.2%}",
            'annual_return': f"{self.annual_return:.2%}",
            'max_drawdown': f"{self.max_drawdown:.2%}",
            'volatility': f"{self.volatility:.2%}",
            'sharpe_ratio': f"{self.sharpe_ratio:.2f}",
            'total_trades': self.total_trades,
            'win_rate': f"{self.win_rate:.2%}",
            'profit_factor': f"{self.profit_factor:.2f}",
            'final_value': f"{self.final_value:.2f}"
        }


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital: float = 100000.0):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金，默认10万
        """
        self.initial_capital = initial_capital
        self.analyzer = TechnicalAnalyzer()
        
    def run_backtest(self, 
                     df: pd.DataFrame,
                     strategy: str = 'macd_cross',
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> BacktestResult:
        """
        运行回测
        
        Args:
            df: 股票数据 DataFrame
            strategy: 策略名称
                - 'macd_cross': MACD金叉买入，死叉卖出
                - 'ma_cross': 均线金叉买入，死叉卖出
                - 'rsi_extreme': RSI超卖买入，超买卖出
                - 'boll_break': 布林带下轨买入，上轨卖出
                - 'composite': 综合评分策略
            start_date: 回测开始日期 'YYYY-MM-DD'
            end_date: 回测结束日期 'YYYY-MM-DD'
        
        Returns:
            BacktestResult: 回测结果
        """
        # 数据预处理
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        # 日期过滤
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
        
        if len(df) < 60:
            raise ValueError("数据量不足，至少需要60个交易日")
        
        # 计算技术指标
        df = self._calculate_signals(df, strategy)
        
        # 执行回测
        return self._execute_trades(df, strategy)
    
    def _calculate_signals(self, df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """计算交易信号"""
        
        # 计算技术指标
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        
        # EMA12 和 EMA26
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = df['ema12'] - df['ema26']
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = 2 * (df['dif'] - df['dea'])
        
        # RSI6
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
        rs = gain / loss
        df['rsi6'] = 100 - (100 / (1 + rs))
        
        # 布林带
        df['ma20'] = df['close'].rolling(20).mean()
        std20 = df['close'].rolling(20).std()
        df['boll_upper'] = df['ma20'] + 2 * std20
        df['boll_lower'] = df['ma20'] - 2 * std20
        
        # MACD金叉死叉
        df['macd_golden_cross'] = (df['dif'] > df['dea']) & (df['dif'].shift(1) <= df['dea'].shift(1))
        df['macd_death_cross'] = (df['dif'] < df['dea']) & (df['dif'].shift(1) >= df['dea'].shift(1))
        
        # 均线金叉死叉
        df['ma_golden_cross'] = (df['ma5'] > df['ma10']) & (df['ma5'].shift(1) <= df['ma10'].shift(1))
        df['ma_death_cross'] = (df['ma5'] < df['ma10']) & (df['ma5'].shift(1) >= df['ma10'].shift(1))
        
        # 综合评分
        df['composite_score'] = 50  # 基础分
        df.loc[df['macd'] > 0, 'composite_score'] += 10
        df.loc[df['ma5'] > df['ma10'], 'composite_score'] += 10
        df.loc[df['rsi6'] > 50, 'composite_score'] += 10
        df.loc[df['close'] > df['ma20'], 'composite_score'] += 10
        
        # 计算信号
        df['signal'] = 0
        
        if strategy == 'macd_cross':
            df.loc[df['macd_golden_cross'] == True, 'signal'] = 1
            df.loc[df['macd_death_cross'] == True, 'signal'] = -1
            
        elif strategy == 'ma_cross':
            df.loc[df['ma_golden_cross'] == True, 'signal'] = 1
            df.loc[df['ma_death_cross'] == True, 'signal'] = -1
            
        elif strategy == 'rsi_extreme':
            df.loc[df['rsi6'] < 30, 'signal'] = 1
            df.loc[df['rsi6'] > 70, 'signal'] = -1
            
        elif strategy == 'boll_break':
            df.loc[df['close'] < df['boll_lower'], 'signal'] = 1
            df.loc[df['close'] > df['boll_upper'], 'signal'] = -1
            
        elif strategy == 'composite':
            df.loc[df['composite_score'] > 65, 'signal'] = 1
            df.loc[df['composite_score'] < 35, 'signal'] = -1
        
        return df
    
    def _execute_trades(self, df: pd.DataFrame, strategy: str) -> BacktestResult:
        """执行交易模拟"""
        
        cash = self.initial_capital
        position = 0  # 持仓股数
        trades = []
        daily_values = []
        
        for idx, row in df.iterrows():
            price = row['close']
            signal = row['signal']
            date = row['date']
            
            # 买入信号
            if signal == 1 and position == 0:
                # 全仓买入
                shares = int(cash / price)
                if shares > 0:
                    cost = shares * price
                    cash -= cost
                    position = shares
                    trades.append(Trade(
                        date=date,
                        action='buy',
                        price=price,
                        shares=shares,
                        value=cost,
                        signal=strategy
                    ))
            
            # 卖出信号
            elif signal == -1 and position > 0:
                # 全部卖出
                value = position * price
                cash += value
                trades.append(Trade(
                    date=date,
                    action='sell',
                    price=price,
                    shares=position,
                    value=value,
                    signal=strategy
                ))
                position = 0
            
            # 计算当日总资产
            total_value = cash + position * price
            daily_values.append({
                'date': date,
                'value': total_value,
                'price': price
            })
        
        # 最后一天平仓
        final_price = df.iloc[-1]['close']
        if position > 0:
            value = position * final_price
            cash += value
            trades.append(Trade(
                date=df.iloc[-1]['date'],
                action='sell',
                price=final_price,
                shares=position,
                value=value,
                signal='close_position'
            ))
            position = 0
        
        # 生成每日资产曲线
        daily_df = pd.DataFrame(daily_values)
        
        # 计算绩效指标
        return self._calculate_metrics(trades, daily_df)
    
    def _calculate_metrics(self, trades: List[Trade], daily_df: pd.DataFrame) -> BacktestResult:
        """计算回测绩效指标"""
        
        final_value = daily_df.iloc[-1]['value']
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        # 计算年化收益率
        days = (daily_df.iloc[-1]['date'] - daily_df.iloc[0]['date']).days
        years = max(days / 365, 0.01)
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 计算最大回撤
        daily_df['peak'] = daily_df['value'].cummax()
        daily_df['drawdown'] = (daily_df['value'] - daily_df['peak']) / daily_df['peak']
        max_drawdown = daily_df['drawdown'].min()
        
        # 计算波动率（年化）
        daily_returns = daily_df['value'].pct_change().dropna()
        volatility = daily_returns.std() * np.sqrt(252)
        
        # 计算夏普比率（假设无风险利率3%）
        risk_free_rate = 0.03
        if volatility > 0:
            sharpe_ratio = (annual_return - risk_free_rate) / volatility
        else:
            sharpe_ratio = 0
        
        # 交易统计
        total_trades = len([t for t in trades if t.action in ['buy', 'sell']])
        
        # 计算胜率和盈亏比
        buy_trades = [t for t in trades if t.action == 'buy']
        sell_trades = [t for t in trades if t.action == 'sell']
        
        wins = 0
        total_profit = 0
        total_loss = 0
        
        for i, buy in enumerate(buy_trades):
            if i < len(sell_trades):
                sell = sell_trades[i]
                profit = (sell.price - buy.price) * buy.shares
                if profit > 0:
                    wins += 1
                    total_profit += profit
                else:
                    total_loss += abs(profit)
        
        win_rate = wins / len(sell_trades) if sell_trades else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        return BacktestResult(
            total_return=total_return,
            annual_return=annual_return,
            max_drawdown=max_drawdown,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            final_value=final_value,
            trades=trades,
            daily_values=daily_df
        )
    
    def batch_backtest(self,
                      stock_data_dict: Dict[str, pd.DataFrame],
                      strategy: str = 'macd_cross',
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """
        批量回测多只股票
        
        Args:
            stock_data_dict: {股票代码: DataFrame} 字典
            strategy: 策略名称
            start_date/end_date: 日期范围
        
        Returns:
            DataFrame: 各股票回测结果汇总
        """
        results = []
        
        for code, df in stock_data_dict.items():
            try:
                result = self.run_backtest(df, strategy, start_date, end_date)
                results.append({
                    'code': code,
                    **result.to_dict()
                })
            except Exception as e:
                results.append({
                    'code': code,
                    'error': str(e)
                })
        
        return pd.DataFrame(results)
    
    def compare_strategies(self,
                         df: pd.DataFrame,
                         strategies: List[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> pd.DataFrame:
        """
        对比多种策略的表现
        
        Args:
            df: 股票数据
            strategies: 策略列表，默认全部
            start_date/end_date: 日期范围
        
        Returns:
            DataFrame: 各策略对比结果
        """
        if strategies is None:
            strategies = ['macd_cross', 'ma_cross', 'rsi_extreme', 'boll_break', 'composite']
        
        results = []
        
        for strategy in strategies:
            try:
                result = self.run_backtest(df, strategy, start_date, end_date)
                results.append({
                    'strategy': strategy,
                    **result.to_dict()
                })
            except Exception as e:
                results.append({
                    'strategy': strategy,
                    'error': str(e)
                })
        
        return pd.DataFrame(results)


def print_backtest_report(result: BacktestResult, stock_code: str = ""):
    """打印回测报告"""
    
    print("\n" + "="*60)
    print(f"📊 回测报告 {stock_code}")
    print("="*60)
    
    print("\n💰 收益指标:")
    print(f"  总收益率: {result.total_return:+.2%}")
    print(f"  年化收益率: {result.annual_return:+.2%}")
    print(f"  最终资产: {result.final_value:,.2f}")
    
    print("\n⚠️ 风险指标:")
    print(f"  最大回撤: {result.max_drawdown:.2%}")
    print(f"  波动率: {result.volatility:.2%}")
    print(f"  夏普比率: {result.sharpe_ratio:.2f}")
    
    print("\n📈 交易统计:")
    print(f"  总交易次数: {result.total_trades}")
    print(f"  胜率: {result.win_rate:.2%}")
    print(f"  盈亏比: {result.profit_factor:.2f}")
    
    print("\n📝 交易记录:")
    for trade in result.trades[:10]:  # 只显示前10笔
        action_emoji = "🟢" if trade.action == 'buy' else "🔴"
        print(f"  {action_emoji} {trade.date.strftime('%Y-%m-%d')} {trade.action.upper():4} "
              f"价格: {trade.price:.2f} 数量: {trade.shares} 金额: {trade.value:,.2f}")
    
    if len(result.trades) > 10:
        print(f"  ... 共 {len(result.trades)} 笔交易")
    
    print("="*60)


if __name__ == "__main__":
    # 测试代码
    print("🧪 回测系统测试")
    
    # 生成模拟数据
    np.random.seed(42)
    dates = pd.date_range(start='2023-01-01', end='2024-01-01', freq='B')
    n = len(dates)
    
    # 模拟价格走势
    price = 100
    prices = []
    for i in range(n):
        price *= (1 + np.random.normal(0.0005, 0.02))
        prices.append(price)
    
    df = pd.DataFrame({
        'date': dates,
        'open': prices,
        'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'close': prices,
        'volume': np.random.randint(1000000, 10000000, n)
    })
    
    # 创建回测引擎
    engine = BacktestEngine(initial_capital=100000)
    
    # 对比不同策略
    print("\n🔄 策略对比:")
    comparison = engine.compare_strategies(df)
    print(comparison.to_string())
    
    # 详细回测一个策略
    print("\n" + "="*60)
    print("📈 MACD策略详细回测")
    result = engine.run_backtest(df, strategy='macd_cross')
    print_backtest_report(result, "TEST")
