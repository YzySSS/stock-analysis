#!/usr/bin/env python3
"""
V12策略 V8 - 运行脚本（适配实际数据库）
======================================
修改：使用全市场中位数涨跌幅代替沪深300判断市场环境
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
    """V12回测引擎 V8 - 使用全市场中位数判断市场环境"""
    
    def __init__(self):
        # 基础因子权重（会根据市场环境动态调整）
        self.base_weights = {
            'quality': 0.25,      # ROE质量
            'valuation': 0.20,    # PE估值
            'momentum': 0.20,     # 20日行业相对动量
            'trend': 0.15,        # MA20斜率
            'reversal': 0.20      # 前一日涨跌幅（负权重）
        }
        
        self.current_weights = self.base_weights.copy()
        
        # 动态参数
        self.base_threshold = 50
        self.current_threshold = 50
        self.cooling_days = 5
        self.stop_loss_pct = -0.05
        
        # 风控参数
        self.max_drawdown_limit = 0.20
        self.position_ratio = 1.0
        
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
        判断市场环境 - 使用全市场中位数涨跌幅
        返回: (market_status, volatility)
        market_status: 'bull', 'neutral', 'bear'
        """
        cursor = self.conn.cursor()
        
        # 获取前5日所有股票的涨跌幅中位数
        cursor.execute("""
            SELECT pct_change FROM stock_kline
            WHERE trade_date <= %s AND trade_date >= DATE_SUB(%s, INTERVAL 5 DAY)
            AND pct_change IS NOT NULL
        """, (date, date))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 100:
            return 'neutral', 0.2
        
        changes = [r[0] for r in rows if r[0] is not None]
        
        # 计算波动率
        changes_float = [float(c) for c in changes]
        volatility = np.std(changes_float) * np.sqrt(252) if len(changes_float) > 10 else 0.2
        
        # 使用中位数判断市场状态
        median_change = np.median(changes_float)
        
        if median_change > 1.0:  # 中位数涨>1%
            status = 'bull'
        elif median_change < -1.0:  # 中位数跌>1%
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
            self.current_weights = {
                'quality': 0.20, 'valuation': 0.15,
                'momentum': 0.30, 'trend': 0.20, 'reversal': 0.15
            }
        elif market_status == 'bear':
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
        
        price_data = defaultdict(list)
        for row in cursor.fetchall():
            code, close, pct = row
            price_data[code].append({'close': close, 'pct': pct})
        
        cursor.close()
        
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
                    trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100
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
            values = [float(s[factor]) for s in stocks if s[factor] is not None]
            if not values:
                continue
            
            mean = float(np.mean(values))
            std = float(np.std(values))
            
            if std == 0:
                continue
            
            for stock in stocks:
                code = stock['code']
                if code not in zscores:
                    zscores[code] = {}
                zscores[code][factor] = (float(stock[factor]) - mean) / std
        
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
            
            score = 0
            for factor, weight in self.current_weights.items():
                if factor in zscores[code]:
                    score += float(zscores[code][factor]) * weight
            
            score = 50 + score * 15  # 映射到0-100
            
            picks.append({
                'code': code,
                'name': stock['name'],
                'price': stock['price'],
                'score': score,
                'zscores': zscores[code]
            })
        
        # 排序并选择
        picks.sort(key=lambda x: x['score'], reverse=True)
        
        # 达标即选（最多5只）
        selected = [p for p in picks if p['score'] >= self.current_threshold][:5]
        
        # 记录选股
        for pick in selected:
            self.recent_picks[pick['code']] = date
        
        return selected
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        logger.info(f"开始V8回测: {start_date} ~ {end_date}")
        
        self.connect_db()
        days = self.get_trading_days(start_date, end_date)
        
        if len(days) < 2:
            logger.error("交易日数量不足")
            self.close_db()
            return {}
        
        logger.info(f"共 {len(days)} 个交易日")
        
        positions = []  # 当前持仓
        daily_returns = []
        
        for i, date in enumerate(days):
            if i < 20:  # 跳过前20天（需要历史数据计算趋势）
                continue
            
            if i % 50 == 0:
                logger.info(f"处理进度: {i}/{len(days)} ({i/len(days)*100:.1f}%)")
            
            # 获取市场环境
            market_status, volatility = self.get_market_status(date)
            self.adjust_parameters(market_status, volatility)
            
            # 处理持仓（T+1卖出）
            exit_trades = []
            for pos in positions[:]:
                # 检查止损
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT close FROM stock_kline
                    WHERE code = %s AND trade_date = %s
                """, (pos['code'], date))
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    exit_price = float(row[0])
                    gross_return = (exit_price - pos['entry_price']) / pos['entry_price']
                    
                    # 交易成本
                    cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
                    net_return = gross_return - cost
                    
                    # 止损或持有期满
                    if gross_return <= self.stop_loss_pct:
                        exit_reason = 'stop_loss'
                    else:
                        exit_reason = 'time_exit'
                    
                    trade = TradeRecord(
                        entry_date=pos['entry_date'],
                        exit_date=date,
                        code=pos['code'],
                        entry_price=pos['entry_price'],
                        exit_price=exit_price,
                        gross_return=gross_return,
                        net_return=net_return,
                        score=pos['score'],
                        exit_reason=exit_reason,
                        market_status=market_status
                    )
                    exit_trades.append(trade)
                    positions.remove(pos)
            
            self.trades.extend(exit_trades)
            
            # 选股（买入）
            if len(positions) < 5:
                picks = self.select_stocks(date, market_status)
                for pick in picks[:5-len(positions)]:
                    positions.append({
                        'code': pick['code'],
                        'entry_date': date,
                        'entry_price': pick['price'],
                        'score': pick['score']
                    })
            
            # 记录每日统计
            self.daily_stats.append({
                'date': date,
                'market_status': market_status,
                'threshold': self.current_threshold,
                'position_ratio': self.position_ratio,
                'positions': len(positions),
                'volatility': volatility
            })
        
        self.close_db()
        
        # 生成报告
        return self.generate_report()
    
    def generate_report(self) -> Dict:
        """生成回测报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return {}
        
        trades_df = pd.DataFrame([asdict(t) for t in self.trades])
        
        # 基础统计
        total_trades = len(trades_df)
        win_trades = len(trades_df[trades_df['net_return'] > 0])
        loss_trades = len(trades_df[trades_df['net_return'] <= 0])
        win_rate = win_trades / total_trades if total_trades > 0 else 0
        
        avg_gross = trades_df['gross_return'].mean()
        avg_net = trades_df['net_return'].mean()
        
        # 计算累计收益
        cumulative = (1 + trades_df['net_return']).cumprod() - 1
        total_return = cumulative.iloc[-1]
        
        # 最大回撤
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / (1 + running_max)
        max_drawdown = abs(drawdown.min())
        
        report = {
            'version': 'V8',
            'total_trades': total_trades,
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': round(win_rate, 4),
            'avg_gross_return': round(avg_gross, 6),
            'avg_net_return': round(avg_net, 6),
            'total_return': round(total_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'trades': [asdict(t) for t in self.trades]
        }
        
        logger.info("=" * 50)
        logger.info("V8回测结果")
        logger.info("=" * 50)
        logger.info(f"总交易次数: {total_trades}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"累计收益: {total_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        
        return report


def main():
    """主函数"""
    engine = V12BacktestEngineV8()
    
    # 2年回测
    start_date = '2024-01-01'
    end_date = '2026-04-08'
    
    report = engine.run_backtest(start_date, end_date)
    
    # 保存结果
    output_file = f'v12_v8_backtest_{start_date}_{end_date}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n结果已保存: {output_file}")


if __name__ == '__main__':
    main()
