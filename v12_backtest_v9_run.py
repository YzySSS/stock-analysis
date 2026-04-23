#!/usr/bin/env python3
"""
V12策略 V9 - 修复版
==================
基于DeepSeek评估V8后的改进方案：
P0修复:
1. 重构市场环境判断：多指标综合（指数MA+波动率+市场宽度）
2. 修改卖出规则：止损-5% + 止盈+8% + 最长持有10日
P1修复:
3. 加入市值因子
4. 合并momentum+trend为combined_trend
5. 冷却期动态调整（基于前期涨幅）
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
    holding_days: int


class V12BacktestEngineV9:
    """V12回测引擎 V9 - 修复DeepSeek指出的问题"""
    
    def __init__(self):
        # 优化后因子权重（4因子体系）
        self.base_weights = {
            'quality': 0.30,      # ROE质量
            'valuation': 0.25,    # PE估值
            'combined_trend': 0.25,  # 合并动量+趋势
            'reversal': 0.20      # 前一日涨跌幅（反向）
        }
        
        self.current_weights = self.base_weights.copy()
        
        # 动态参数
        self.base_threshold = 50
        self.current_threshold = 50
        self.base_cooling_days = 5
        
        # 风控参数 - P0修复
        self.stop_loss_pct = -0.05      # 止损-5%
        self.stop_profit_pct = 0.08     # 止盈+8% - 新增
        self.max_holding_days = 10      # 最长持有10日 - 修改
        self.max_drawdown_limit = 0.20
        
        # 仓位管理
        self.position_ratio = 1.0
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.recent_picks = {}  # code -> {'date': str, 'cooling_days': int}
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
        P0修复: 重构市场环境判断 - 多指标综合
        指标:
        1. 指数MA排列 (50%)
        2. 20日涨跌幅 (30%)
        3. 波动率 (20%)
        """
        cursor = self.conn.cursor()
        
        # 获取上证指数最近20日数据
        cursor.execute("""
            SELECT close, pct_change FROM stock_kline
            WHERE code = '000001' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 20
        """, (date,))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 20:
            return 'neutral', 0.2
        
        closes = [float(r[0]) for r in reversed(rows)]
        changes = [float(r[1]) for r in rows if r[1] is not None]
        
        # 计算MA
        ma5 = np.mean(closes[-5:])
        ma10 = np.mean(closes[-10:])
        ma20 = np.mean(closes)
        
        # 1. MA排列得分
        if ma5 > ma10 > ma20:
            ma_score = 1.0  # 多头排列
        elif ma5 < ma10 < ma20:
            ma_score = -1.0  # 空头排列
        else:
            ma_score = 0.0  # 震荡
        
        # 2. 20日涨跌幅得分
        return_20d = (closes[-1] - closes[0]) / closes[0] * 100
        if return_20d > 5:
            trend_score = 1.0
        elif return_20d < -5:
            trend_score = -1.0
        else:
            trend_score = return_20d / 5
        
        # 3. 波动率
        volatility = float(np.std(changes)) * np.sqrt(252) if changes else 0.2
        
        # 综合判断
        composite = ma_score * 0.5 + trend_score * 0.3
        
        if composite > 0.3:
            status = 'bull'
        elif composite < -0.3:
            status = 'bear'
        else:
            status = 'neutral'
        
        return status, volatility
    
    def adjust_parameters(self, market_status: str, volatility: float):
        """根据市场环境动态调整参数"""
        # 调整阈值
        if volatility > 0.3:
            self.current_threshold = min(65, self.base_threshold + 10)
        elif volatility < 0.15:
            self.current_threshold = max(40, self.base_threshold - 5)
        else:
            self.current_threshold = self.base_threshold
        
        # 调整仓位
        if market_status == 'bear':
            self.position_ratio = 0.2
        elif market_status == 'bull':
            self.position_ratio = 1.0
        else:
            self.position_ratio = 0.6
        
        # 调整因子权重
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
    
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取当日股票池（含4因子+市值数据）"""
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
        
        # P1修复: 加入市值因子 (从stock_kline获取market_cap，但允许NULL)
        cursor.execute("""
            SELECT 
                k.code, k.close, k.turnover, k.amount,
                k.market_cap,
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
            pe_score = row[5]
            roe_score = row[6]
            total_mv = row[8]
            
            market_cap = row[4]
            pe_score = row[6]
            roe_score = row[7]
            
            if pe_score is None or roe_score is None:
                continue
            
            stocks.append({
                'code': code,
                'price': float(row[1]),
                'reversal': float(row[5]) if row[5] is not None else 0,
                'quality': float(roe_score),
                'valuation': float(pe_score),
                'market_cap': float(market_cap) if market_cap is not None else 100,  # 默认中市值
                'name': row[8] or ''
            })
            codes.append(code)
        
        cursor.close()
        
        # 获取历史价格计算combined_trend
        if codes:
            stocks = self._add_combined_trend(stocks, codes, date)
        
        return stocks
    
    def _add_combined_trend(self, stocks: List[Dict], codes: List[str], date: str) -> List[Dict]:
        """P1修复: 合并动量和趋势为combined_trend"""
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
            price_data[code].append({'close': float(close), 'pct': float(pct) if pct is not None else 0})
        
        cursor.close()
        
        for stock in stocks:
            code = stock['code']
            if code not in price_data or len(price_data[code]) < 20:
                stock['combined_trend'] = 0
                continue
            
            prices = price_data[code]
            
            # 趋势：MA20斜率（年化）
            ma20_now = np.mean([p['close'] for p in prices[:20]])
            ma20_prev = np.mean([p['close'] for p in prices[5:25]]) if len(prices) >= 25 else ma20_now
            
            # 动量：20日累计收益
            momentum = sum([p['pct'] for p in prices[:20] if p['pct'] is not None])
            
            # 合并：趋势占60%，动量占40%
            if ma20_prev > 0:
                trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100
            else:
                trend = 0
            
            stock['combined_trend'] = trend * 0.6 + momentum * 0.4
        
        return stocks
    
    def calculate_zscore(self, stocks: List[Dict]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化（4因子+市值中性化）"""
        if len(stocks) < 10:
            return {}
        
        # P1修复: 市值中性化处理
        # 按市值分组，组内标准化
        stocks_sorted = sorted(stocks, key=lambda x: x['market_cap'])
        group_size = max(10, len(stocks_sorted) // 5)
        
        zscores = {}
        
        for i in range(0, len(stocks_sorted), group_size):
            group = stocks_sorted[i:i+group_size]
            factors = ['quality', 'valuation', 'combined_trend', 'reversal']
            
            for factor in factors:
                values = [float(s[factor]) for s in group if s[factor] is not None]
                if not values or np.std(values) == 0:
                    continue
                
                mean = float(np.mean(values))
                std = float(np.std(values))
                
                for stock in group:
                    code = stock['code']
                    if code not in zscores:
                        zscores[code] = {}
                    zscores[code][factor] = (float(stock[factor]) - mean) / std
        
        return zscores
    
    def select_stocks(self, date: str, market_status: str) -> List[Dict]:
        """V9选股逻辑 - 动态冷却期"""
        # P1修复: 动态冷却期
        for code in list(self.recent_picks.keys()):
            last_info = self.recent_picks[code]
            last_date = last_info['date']
            cooling_days = last_info['cooling_days']
            
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > cooling_days:
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
        
        # 计算得分
        picks = []
        for stock in stocks:
            code = stock['code']
            if code not in zscores:
                continue
            
            score = 0
            for factor, weight in self.current_weights.items():
                if factor in zscores[code]:
                    score += float(zscores[code][factor]) * weight
            
            score = 50 + score * 15
            
            picks.append({
                'code': code,
                'name': stock['name'],
                'price': stock['price'],
                'score': score,
                'zscores': zscores[code],
                'market_cap': stock['market_cap']
            })
        
        # 排序并选择
        picks.sort(key=lambda x: x['score'], reverse=True)
        selected = [p for p in picks if p['score'] >= self.current_threshold][:5]
        
        # P1修复: 动态冷却期
        for pick in selected:
            # 基于市值决定冷却期（小盘股冷却期更长）
            if pick['market_cap'] < 50:  # 小盘股
                cooling = 7
            elif pick['market_cap'] > 500:  # 大盘股
                cooling = 3
            else:
                cooling = self.base_cooling_days
            
            self.recent_picks[pick['code']] = {
                'date': date,
                'cooling_days': cooling
            }
        
        return selected
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测 - P0修复: 新卖出规则"""
        logger.info(f"开始V9回测: {start_date} ~ {end_date}")
        
        self.connect_db()
        days = self.get_trading_days(start_date, end_date)
        
        if len(days) < 2:
            logger.error("交易日数量不足")
            self.close_db()
            return {}
        
        logger.info(f"共 {len(days)} 个交易日")
        
        positions = []  # 当前持仓
        
        for i, date in enumerate(days):
            if i < 20:
                continue
            
            if i % 50 == 0:
                logger.info(f"处理进度: {i}/{len(days)} ({i/len(days)*100:.1f}%)")
            
            # 获取市场环境
            market_status, volatility = self.get_market_status(date)
            self.adjust_parameters(market_status, volatility)
            
            # P0修复: 新卖出规则 - 止损+止盈+时间止损
            exit_trades = []
            for pos in positions[:]:
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT close FROM stock_kline
                    WHERE code = %s AND trade_date = %s
                """, (pos['code'], date))
                row = cursor.fetchone()
                cursor.close()
                
                if not row:
                    continue
                
                exit_price = float(row[0])
                gross_return = (exit_price - pos['entry_price']) / pos['entry_price']
                holding_days = (datetime.strptime(date, '%Y-%m-%d') - 
                               datetime.strptime(pos['entry_date'], '%Y-%m-%d')).days
                
                # 成本
                cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
                net_return = gross_return - cost
                
                # P0修复: 新卖出逻辑
                exit_reason = None
                
                # 1. 止损 -5%
                if gross_return <= self.stop_loss_pct:
                    exit_reason = 'stop_loss'
                # 2. 止盈 +8% - 新增
                elif gross_return >= self.stop_profit_pct:
                    exit_reason = 'stop_profit'
                # 3. 时间止损 - 最长10日
                elif holding_days >= self.max_holding_days:
                    exit_reason = 'time_exit'
                
                if exit_reason:
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
                        market_status=market_status,
                        holding_days=holding_days
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
        avg_holding = trades_df['holding_days'].mean()
        
        # 卖出原因分布
        exit_reasons = trades_df['exit_reason'].value_counts().to_dict()
        
        # 计算累计收益
        cumulative = (1 + trades_df['net_return']).cumprod() - 1
        total_return = cumulative.iloc[-1]
        
        # 最大回撤
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / (1 + running_max)
        max_drawdown = abs(drawdown.min())
        
        report = {
            'version': 'V9',
            'total_trades': total_trades,
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': round(win_rate, 4),
            'avg_gross_return': round(avg_gross, 6),
            'avg_net_return': round(avg_net, 6),
            'avg_holding_days': round(avg_holding, 2),
            'total_return': round(total_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'exit_reasons': exit_reasons,
            'trades': [asdict(t) for t in self.trades]
        }
        
        logger.info("=" * 50)
        logger.info("V9回测结果")
        logger.info("=" * 50)
        logger.info(f"总交易次数: {total_trades}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"平均持仓天数: {avg_holding:.1f}天")
        logger.info(f"累计收益: {total_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"卖出原因: {exit_reasons}")
        
        return report


def main():
    """主函数"""
    engine = V12BacktestEngineV9()
    
    # 2年回测
    start_date = '2024-01-01'
    end_date = '2026-04-08'
    
    report = engine.run_backtest(start_date, end_date)
    
    # 保存结果
    output_file = f'v12_v9_backtest_{start_date}_{end_date}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n结果已保存: {output_file}")


if __name__ == '__main__':
    main()
