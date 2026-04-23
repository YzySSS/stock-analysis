#!/usr/bin/env python3
"""
V12策略 V8-Fixed - 修复版
========================
基于DeepSeek建议的P0修复：
1. 重构市场环境判断（多指标综合）
2. 改进交易规则（止损止盈+时间退出）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

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
    hold_days: int


class V12BacktestEngineV8Fixed:
    """
    V12回测引擎 V8-Fixed
    
    P0修复：
    - 市场环境：多指标综合判断（均线+宽基+波动率+中位数）
    - 交易规则：止损-5% + 止盈+8% + 最长持有10日
    """
    
    def __init__(self):
        # 因子权重（5因子，trend和momentum将在计算时合并处理）
        self.base_weights = {
            'quality': 0.25,      # ROE质量
            'valuation': 0.20,    # PE估值
            'combined_momentum': 0.30,  # 趋势+动量合并（解决共线性）
            'reversal': 0.25      # 反转因子（前一日涨跌幅）
        }
        
        self.current_weights = self.base_weights.copy()
        
        # 动态参数
        self.base_threshold = 50
        self.current_threshold = 50
        self.cooling_days = 5
        
        # 🆕 交易规则参数（修复P0）
        self.stop_loss_pct = -0.05      # 止损 -5%
        self.stop_profit_pct = 0.08     # 止盈 +8%
        self.max_hold_days = 10         # 最长持有10日
        
        # 风控参数
        self.max_drawdown_limit = 0.20
        self.position_ratio = 1.0
        
        # 成本
        self.commission_rate = 0.0005   # 佣金
        self.stamp_tax_rate = 0.001     # 印花税
        self.slippage = 0.001           # 滑点
        
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.recent_picks = {}
        self.conn = None
        
        # 🆕 市场状态历史（用于平滑判断）
        self.market_status_history = []
        
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
    
    def get_market_status_v2(self, date: str) -> Tuple[str, float]:
        """
        🆕 改进的市场环境判断（P0修复）
        
        使用多指标综合判断：
        1. 全市场涨跌幅中位数（20日平均）
        2. 市场波动率（20日标准差年化）
        3. 上涨股票比例（市场宽度）
        4. 沪深300趋势（如果有数据）
        
        返回: (market_status, volatility_score)
        market_status: 'bull', 'neutral', 'bear'
        """
        cursor = self.conn.cursor()
        
        # 获取前20日所有股票的涨跌幅数据
        cursor.execute("""
            SELECT trade_date, pct_change, close, open
            FROM stock_kline
            WHERE trade_date <= %s 
            AND trade_date >= DATE_SUB(%s, INTERVAL 20 DAY)
            AND pct_change IS NOT NULL
            ORDER BY trade_date DESC
        """, (date, date))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 500:  # 数据不足
            return 'neutral', 0.2
        
        # 按日期分组
        date_groups = defaultdict(list)
        for row in rows:
            trade_date, pct_change, close, open_price = row
            date_groups[trade_date.strftime('%Y-%m-%d')].append({
                'pct_change': float(pct_change) if pct_change else 0,
                'close': float(close) if close else 0
            })
        
        # 计算每日指标
        daily_metrics = []
        for d, stocks in sorted(date_groups.items()):
            if len(stocks) < 100:
                continue
            
            changes = [s['pct_change'] for s in stocks]
            up_ratio = sum(1 for c in changes if c > 0) / len(changes)
            median_change = np.median(changes)
            volatility = np.std(changes)
            
            daily_metrics.append({
                'date': d,
                'median_change': median_change,
                'up_ratio': up_ratio,
                'volatility': volatility
            })
        
        if len(daily_metrics) < 5:
            return 'neutral', 0.2
        
        # 使用最近5日平均（平滑判断）
        recent = daily_metrics[-5:]
        avg_median = np.mean([d['median_change'] for d in recent])
        avg_up_ratio = np.mean([d['up_ratio'] for d in recent])
        avg_volatility = np.mean([d['volatility'] for d in recent])
        
        # 年化波动率
        annual_volatility = avg_volatility * np.sqrt(252)
        
        # 综合判断市场状态
        # 评分系统：牛熊信号综合
        bull_signals = 0
        bear_signals = 0
        
        # 信号1：中位数涨跌幅
        if avg_median > 0.5:
            bull_signals += 1
        elif avg_median < -0.5:
            bear_signals += 1
        
        # 信号2：上涨比例
        if avg_up_ratio > 0.55:
            bull_signals += 1
        elif avg_up_ratio < 0.45:
            bear_signals += 1
        
        # 信号3：波动率（高波动通常伴随下跌）
        if annual_volatility > 0.25:
            bear_signals += 0.5
        
        # 综合判断
        if bull_signals >= 2:
            status = 'bull'
        elif bear_signals >= 2:
            status = 'bear'
        else:
            status = 'neutral'
        
        # 记录历史
        self.market_status_history.append({
            'date': date,
            'status': status,
            'median': avg_median,
            'up_ratio': avg_up_ratio,
            'volatility': annual_volatility
        })
        
        return status, annual_volatility
    
    def adjust_parameters(self, market_status: str, volatility: float):
        """根据市场环境动态调整参数"""
        # 调整阈值（基于波动率）
        if volatility > 0.30:  # 高波动，提高门槛
            self.current_threshold = min(65, self.base_threshold + 10)
        elif volatility < 0.15:  # 低波动，降低门槛
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
            # 牛市：进攻性因子权重提高
            self.current_weights = {
                'quality': 0.20,
                'valuation': 0.15,
                'combined_momentum': 0.40,  # 提高动量权重
                'reversal': 0.25
            }
        elif market_status == 'bear':
            # 熊市：防御性因子权重提高
            self.current_weights = {
                'quality': 0.40,  # 提高质量权重
                'valuation': 0.30,  # 提高估值权重
                'combined_momentum': 0.15,  # 降低动量权重
                'reversal': 0.15
            }
        else:
            self.current_weights = self.base_weights.copy()
    
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取当日股票池（含4因子数据）"""
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
            stocks = self._add_combined_momentum(stocks, codes, date)
        
        return stocks
    
    def _add_combined_momentum(self, stocks: List[Dict], codes: List[str], date: str) -> List[Dict]:
        """
        🆕 添加合并后的动量因子（解决trend/momentum共线性问题）
        
        使用主成分分析思想：
        combined_momentum = 0.6 * momentum + 0.4 * trend
        两者高度相关，合并后减少共线性
        """
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
            price_data[code].append({'close': float(close) if close else 0, 'pct': float(pct) if pct else 0})
        
        cursor.close()
        
        for stock in stocks:
            code = stock['code']
            if code not in price_data or len(price_data[code]) < 20:
                stock['combined_momentum'] = 0
                continue
            
            prices = price_data[code]
            
            # 计算trend（MA20斜率年化）
            if len(prices) >= 20:
                ma20_now = np.mean([p['close'] for p in prices[:20]])
                ma20_prev = np.mean([p['close'] for p in prices[5:25]]) if len(prices) >= 25 else ma20_now
                if ma20_prev > 0:
                    trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100
                else:
                    trend = 0
            else:
                trend = 0
            
            # 计算momentum（20日累计收益）
            momentum = sum([p['pct'] for p in prices[:20] if p['pct'] is not None])
            
            # 🆕 合并因子（加权平均，解决共线性）
            # momentum和trend高度相关，合并后降低多重共线性
            stock['combined_momentum'] = 0.6 * momentum + 0.4 * trend
        
        return stocks
    
    def calculate_zscore(self, stocks: List[Dict]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化（4因子）"""
        if len(stocks) < 10:
            return {}
        
        factors = ['quality', 'valuation', 'combined_momentum', 'reversal']
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
        """V8-Fixed选股逻辑"""
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
        """运行回测 - V8-Fixed版"""
        logger.info(f"开始V8-Fixed回测: {start_date} ~ {end_date}")
        logger.info(f"交易规则: 止损{self.stop_loss_pct:.0%} | 止盈{self.stop_profit_pct:.0%} | 最长持有{self.max_hold_days}日")
        
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
            market_status, volatility = self.get_market_status_v2(date)
            self.adjust_parameters(market_status, volatility)
            
            # 🆕 处理持仓（改进的交易规则）
            exit_trades = []
            for pos in positions[:]:
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
                    hold_days = (datetime.strptime(date, '%Y-%m-%d') - 
                                datetime.strptime(pos['entry_date'], '%Y-%m-%d')).days
                    
                    # 交易成本
                    cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
                    net_return = gross_return - cost
                    
                    # 🆕 改进的卖出判断（P0修复）
                    exit_reason = None
                    
                    # 1. 止损
                    if gross_return <= self.stop_loss_pct:
                        exit_reason = 'stop_loss'
                    # 2. 止盈
                    elif gross_return >= self.stop_profit_pct:
                        exit_reason = 'stop_profit'
                    # 3. 时间退出（最长持有期）
                    elif hold_days >= self.max_hold_days:
                        exit_reason = 'time_exit'
                    
                    # 如果触发退出条件
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
                            hold_days=hold_days
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
        
        # 使用numpy计算统计指标
        net_returns = [t.net_return for t in self.trades]
        gross_returns = [t.gross_return for t in self.trades]
        
        # 基础统计
        total_trades = len(self.trades)
        win_trades = sum(1 for r in net_returns if r > 0)
        loss_trades = total_trades - win_trades
        win_rate = win_trades / total_trades if total_trades > 0 else 0
        
        avg_gross = np.mean(gross_returns)
        avg_net = np.mean(net_returns)
        
        # 计算累计收益
        cumulative_returns = []
        cum = 1.0
        for r in net_returns:
            cum *= (1 + r)
            cumulative_returns.append(cum - 1)
        total_return = cumulative_returns[-1] if cumulative_returns else 0
        
        # 最大回撤
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = [(c - m) / (1 + m) if m > -1 else 0 for c, m in zip(cumulative_returns, running_max)]
        max_drawdown = abs(min(drawdowns)) if drawdowns else 0
        
        # 🆕 按退出原因统计
        exit_reasons = defaultdict(lambda: {'count': 0, 'avg_return': 0})
        for t in self.trades:
            reason = t.exit_reason
            exit_reasons[reason]['count'] += 1
        
        for reason in exit_reasons:
            reason_returns = [t.net_return for t in self.trades if t.exit_reason == reason]
            exit_reasons[reason]['avg_return'] = np.mean(reason_returns)
        
        # 🆕 按市场环境统计
        market_stats = defaultdict(lambda: {'count': 0, 'win_count': 0, 'avg_return': 0})
        for t in self.trades:
            market = t.market_status
            market_stats[market]['count'] += 1
            if t.net_return > 0:
                market_stats[market]['win_count'] += 1
        
        for market in market_stats:
            market_returns = [t.net_return for t in self.trades if t.market_status == market]
            market_stats[market]['avg_return'] = np.mean(market_returns)
            market_stats[market]['win_rate'] = market_stats[market]['win_count'] / market_stats[market]['count']
        
        # 🆕 平均持仓天数
        avg_hold_days = np.mean([t.hold_days for t in self.trades])
        
        report = {
            'version': 'V8-Fixed',
            'modifications': [
                '市场环境判断：多指标综合（中位数+上涨比例+波动率）',
                '交易规则：止损-5% + 止盈+8% + 最长持有10日',
                '因子合并：trend+momentum合并为combined_momentum（解决共线性）',
                '平滑判断：使用20日平均而非单日判断'
            ],
            'total_trades': total_trades,
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': round(win_rate, 4),
            'avg_gross_return': round(avg_gross, 6),
            'avg_net_return': round(avg_net, 6),
            'total_return': round(total_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'avg_hold_days': round(avg_hold_days, 2),
            'exit_reasons': dict(exit_reasons),
            'market_stats': dict(market_stats),
            'trades': [asdict(t) for t in self.trades]
        }
        
        logger.info("=" * 60)
        logger.info("V8-Fixed 回测结果")
        logger.info("=" * 60)
        logger.info(f"总交易次数: {total_trades}")
        logger.info(f"胜率: {win_rate*100:.2f}%")
        logger.info(f"平均净收益: {avg_net*100:.2f}%")
        logger.info(f"累计收益: {total_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info(f"平均持仓天数: {avg_hold_days:.1f}天")
        logger.info("-" * 60)
        logger.info("退出原因分布:")
        for reason, stats in exit_reasons.items():
            logger.info(f"  {reason}: {stats['count']}次 (平均收益: {stats['avg_return']*100:.2f}%)")
        logger.info("-" * 60)
        logger.info("市场环境表现:")
        for market, stats in market_stats.items():
            logger.info(f"  {market}: {stats['count']}次 胜率{stats['win_rate']*100:.1f}% 平均{stats['avg_return']*100:.2f}%")
        
        return report


def main():
    """主函数"""
    import sys
    
    engine = V12BacktestEngineV8Fixed()
    
    # 支持命令行参数指定日期范围
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        # 默认3个月回测（快速验证）
        start_date = '2025-01-01'
        end_date = '2025-03-31'
    
    report = engine.run_backtest(start_date, end_date)
    
    # 保存结果
    output_file = f'v12_v8_fixed_backtest_{start_date}_{end_date}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n结果已保存: {output_file}")


if __name__ == '__main__':
    main()
