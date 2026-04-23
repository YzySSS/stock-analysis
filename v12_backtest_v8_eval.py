#!/usr/bin/env python3
"""
V12策略 V8-评估版
================
正确的选股策略回测：固定持有期，评估选股能力

收益率计算：
- 1日收益：选股日开盘价买入 → 次日开盘价卖出
- 3日收益：选股日开盘价买入 → 第3日收盘价卖出
- 5日收益：选股日开盘价买入 → 第5日收盘价卖出
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from collections import defaultdict
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class V12BacktestEngineV8Eval:
    """
    V12回测引擎 V8-评估版
    
    只做选股评估，固定持有期，无止损止盈
    """
    
    def __init__(self, hold_days=1):
        self.hold_days = hold_days  # 持有天数
        
        # 因子权重
        self.base_weights = {
            'quality': 0.25,
            'valuation': 0.20,
            'combined_momentum': 0.30,
            'reversal': 0.25
        }
        
        self.current_weights = self.base_weights.copy()
        self.current_threshold = 50
        self.cooling_days = 5
        
        # 成本
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        self.trades = []
        self.daily_picks = []
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
    
    def get_market_status(self, date: str) -> str:
        """判断市场环境"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT pct_change FROM stock_kline
            WHERE trade_date <= %s 
            AND trade_date >= DATE_SUB(%s, INTERVAL 5 DAY)
            AND pct_change IS NOT NULL
        """, (date, date))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 100:
            return 'neutral'
        
        changes = [float(r[0]) for r in rows if r[0] is not None]
        median_change = np.median(changes)
        
        if median_change > 1.0:
            return 'bull'
        elif median_change < -1.0:
            return 'bear'
        else:
            return 'neutral'
    
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取当日股票池"""
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
                k.code, k.open as price, k.turnover,
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
        
        # 添加合并动量因子
        if codes:
            stocks = self._add_combined_momentum(stocks, codes, date)
        
        return stocks
    
    def _add_combined_momentum(self, stocks, codes, date):
        """添加合并后的动量因子"""
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
            price_data[code].append({'close': float(close) if close else 0, 
                                      'pct': float(pct) if pct else 0})
        
        cursor.close()
        
        for stock in stocks:
            code = stock['code']
            if code not in price_data or len(price_data[code]) < 20:
                stock['combined_momentum'] = 0
                continue
            
            prices = price_data[code]
            
            # trend: MA20斜率
            ma20_now = np.mean([p['close'] for p in prices[:20]])
            ma20_prev = np.mean([p['close'] for p in prices[5:25]]) if len(prices) >= 25 else ma20_now
            trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100 if ma20_prev > 0 else 0
            
            # momentum: 20日累计收益
            momentum = sum([p['pct'] for p in prices[:20]])
            
            # 合并因子
            stock['combined_momentum'] = 0.6 * momentum + 0.4 * trend
        
        return stocks
    
    def calculate_zscore(self, stocks):
        """Z-score标准化"""
        if len(stocks) < 10:
            return {}
        
        factors = ['quality', 'valuation', 'combined_momentum', 'reversal']
        zscores = {}
        
        for factor in factors:
            values = [float(s[factor]) for s in stocks if s[factor] is not None]
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
                zscores[code][factor] = (float(stock[factor]) - mean) / std
        
        return zscores
    
    def select_stocks(self, date, market_status):
        """选股"""
        # 冷却期检查
        for code in list(self.recent_picks.keys()):
            last_date = self.recent_picks[code]
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > self.cooling_days:
                del self.recent_picks[code]
        
        stocks = self.get_stock_list(date)
        if len(stocks) < 10:
            return []
        
        # 过滤冷却期
        stocks = [s for s in stocks if s['code'] not in self.recent_picks]
        
        # Z-score评分
        zscores = self.calculate_zscore(stocks)
        
        picks = []
        for stock in stocks:
            code = stock['code']
            if code not in zscores:
                continue
            
            score = sum(zscores[code].get(f, 0) * w 
                       for f, w in self.current_weights.items())
            score = 50 + score * 15
            
            picks.append({
                'code': code,
                'name': stock['name'],
                'entry_price': stock['price'],
                'score': score
            })
        
        picks.sort(key=lambda x: x['score'], reverse=True)
        selected = [p for p in picks if p['score'] >= self.current_threshold][:5]
        
        for pick in selected:
            self.recent_picks[pick['code']] = date
        
        return selected
    
    def get_exit_price(self, code: str, entry_date: str, hold_days: int) -> Tuple[float, str]:
        """
        获取退出价格
        
        1日收益：次日开盘价
        3日/5日收益：第N日收盘价
        """
        cursor = self.conn.cursor()
        
        # 获取entry_date后的交易日列表
        cursor.execute("""
            SELECT trade_date FROM stock_kline
            WHERE trade_date > %s
            AND code = %s
            ORDER BY trade_date
            LIMIT %s
        """, (entry_date, code, hold_days + 1))
        
        future_dates = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
        
        if len(future_dates) < hold_days:
            cursor.close()
            return None, "数据不足"
        
        if hold_days == 1:
            # 1日收益：次日开盘价
            exit_date = future_dates[0]
            cursor.execute("""
                SELECT open FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, exit_date))
        else:
            # 3日/5日收益：第N日收盘价
            exit_date = future_dates[hold_days - 1]
            cursor.execute("""
                SELECT close FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, exit_date))
        
        row = cursor.fetchone()
        cursor.close()
        
        if row and row[0]:
            return float(row[0]), exit_date
        else:
            return None, "无价格数据"
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测 - 评估版"""
        logger.info(f"V8-评估版回测: {start_date} ~ {end_date}")
        logger.info(f"持有期: {self.hold_days}天 | 收益率计算: 开盘价→{'次日开盘' if self.hold_days==1 else f'第{self.hold_days}日收盘'}")
        
        self.connect_db()
        days = self.get_trading_days(start_date, end_date)
        
        if len(days) < 20:
            logger.error("交易日数量不足")
            self.close_db()
            return {}
        
        logger.info(f"共 {len(days)} 个交易日")
        
        # 只选股，记录entry信息
        for i, date in enumerate(days):
            if i < 20:
                continue
            
            if i % 50 == 0:
                logger.info(f"进度: {i}/{len(days)} ({i/len(days)*100:.1f}%)")
            
            market_status = self.get_market_status(date)
            picks = self.select_stocks(date, market_status)
            
            for pick in picks:
                exit_price, exit_date = self.get_exit_price(
                    pick['code'], date, self.hold_days
                )
                
                if exit_price is None:
                    continue
                
                entry_price = pick['entry_price']
                gross_return = (exit_price - entry_price) / entry_price
                
                # 扣除成本
                cost = self.commission_rate * 2 + self.stamp_tax_rate + self.slippage * 2
                net_return = gross_return - cost
                
                self.trades.append({
                    'entry_date': date,
                    'exit_date': exit_date,
                    'code': pick['code'],
                    'name': pick['name'],
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'gross_return': gross_return,
                    'net_return': net_return,
                    'score': pick['score'],
                    'market_status': market_status
                })
        
        self.close_db()
        return self.generate_report()
    
    def generate_report(self) -> Dict:
        """生成报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return {}
        
        net_returns = [t['net_return'] for t in self.trades]
        gross_returns = [t['gross_return'] for t in self.trades]
        
        total_trades = len(self.trades)
        win_trades = sum(1 for r in net_returns if r > 0)
        win_rate = win_trades / total_trades
        
        avg_gross = np.mean(gross_returns)
        avg_net = np.mean(net_returns)
        
        # 累计收益
        cum = 1.0
        for r in net_returns:
            cum *= (1 + r)
        total_return = cum - 1
        
        # 最大回撤
        cumulative = []
        cum = 1.0
        for r in net_returns:
            cum *= (1 + r)
            cumulative.append(cum - 1)
        
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = [(c - m) / (1 + m) for c, m in zip(cumulative, running_max)]
        max_drawdown = abs(min(drawdowns))
        
        # 按市场环境统计
        market_stats = defaultdict(lambda: {'count': 0, 'win': 0, 'avg': 0})
        for t in self.trades:
            m = t['market_status']
            market_stats[m]['count'] += 1
            if t['net_return'] > 0:
                market_stats[m]['win'] += 1
        
        for m in market_stats:
            returns = [t['net_return'] for t in self.trades if t['market_status'] == m]
            market_stats[m]['avg'] = np.mean(returns)
            market_stats[m]['win_rate'] = market_stats[m]['win'] / market_stats[m]['count']
        
        report = {
            'version': f'V8-Eval-{self.hold_days}D',
            'hold_days': self.hold_days,
            'total_trades': total_trades,
            'win_trades': win_trades,
            'win_rate': round(win_rate, 4),
            'avg_gross_return': round(avg_gross, 6),
            'avg_net_return': round(avg_net, 6),
            'total_return': round(total_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'market_stats': dict(market_stats),
            'trades': self.trades
        }
        
        logger.info("=" * 60)
        logger.info(f"V8-评估版 ({self.hold_days}日持有) 回测结果")
        logger.info("=" * 60)
        logger.info(f"总交易: {total_trades} | 胜率: {win_rate*100:.2f}%")
        logger.info(f"平均收益: {avg_net*100:.2f}% | 累计: {total_return*100:.2f}%")
        logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
        logger.info("-" * 60)
        logger.info("市场环境:")
        for m, s in market_stats.items():
            logger.info(f"  {m}: {s['count']}次 胜率{s['win_rate']*100:.1f}% 平均{s['avg']*100:.2f}%")
        
        return report


def main():
    import sys
    
    # 持有期：1日、3日、5日
    hold_days_list = [1, 3, 5]
    
    start_date = '2025-01-01'
    end_date = '2025-03-31'
    
    for hold_days in hold_days_list:
        engine = V12BacktestEngineV8Eval(hold_days=hold_days)
        report = engine.run_backtest(start_date, end_date)
        
        output_file = f'v12_v8_eval_{hold_days}d_{start_date}_{end_date}.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n{hold_days}日持有结果已保存: {output_file}\n")


if __name__ == '__main__':
    main()
