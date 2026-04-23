#!/usr/bin/env python3
"""
V12策略 - 选股能力评估回测引擎
==============================
核心目标: 评估选股策略的Alpha能力，而非交易执行

评估指标:
- 一日/三日收益率
- 因子IC值
- Top10 vs 市场平均超额收益
- 胜率
- 因子单调性

回测规则 (简化):
- T+1开盘价买入，固定持有1天或3天
- 等权重持仓 (不考虑仓位管理)
- 无止损、无风控 (纯选股评估)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

import pymysql

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
class PickResult:
    """选股结果"""
    date: str
    code: str
    name: str
    score: float
    entry_price: float
    exit_price_1d: float  # 1天后收盘价
    exit_price_3d: float  # 3天后收盘价
    return_1d: float      # 1日收益率
    return_3d: float      # 3日收益率
    market_return_1d: float  # 市场基准1日收益
    market_return_3d: float  # 市场基准3日收益


class StockPickerEvaluator:
    """
    选股能力评估器
    
    专注评估:
    1. 选股收益率 (1日/3日)
    2. 因子IC值
    3. Top10超额收益
    4. 胜率
    """
    
    def __init__(self, hold_days: int = 1):
        """
        Args:
            hold_days: 持仓天数 (1或3)，用于评估不同周期的选股能力
        """
        self.hold_days = hold_days
        
        # 因子权重
        self.factor_weights = {
            'quality': 0.30,
            'valuation': 0.25,
            'combined_trend': 0.25,
            'reversal': 0.20
        }
        
        # 选股参数
        self.score_threshold = 50
        self.max_picks = 10  # 每日最多选股数量
        self.cooling_days = 5
        
        # 成本 (双边0.2%)
        self.total_cost = 0.002
        
        # 结果存储
        self.pick_results: List[PickResult] = []
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
    
    def get_market_benchmark(self, date: str, days: int = 1) -> float:
        """获取市场基准收益率（沪深300）"""
        cursor = self.conn.cursor()
        
        # 获取当日收盘价
        cursor.execute("""
            SELECT close FROM stock_kline 
            WHERE code = '000300.SH' AND trade_date = %s
        """, (date,))
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return 0.0
        
        current_close = float(row[0])
        
        # 获取N日后收盘价
        cursor.execute("""
            SELECT close FROM stock_kline 
            WHERE code = '000300.SH' AND trade_date > %s
            ORDER BY trade_date LIMIT %s
        """, (date, days))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < days:
            return 0.0
        
        future_close = float(rows[-1][0])
        return (future_close - current_close) / current_close
    
    def evaluate(self, start_date: str, end_date: str):
        """主评估函数"""
        logger.info("=" * 80)
        logger.info(f"V12选股能力评估 | 持仓周期: {self.hold_days}天")
        logger.info("=" * 80)
        logger.info(f"评估期: {start_date} 至 {end_date}")
        logger.info(f"因子权重: {self.factor_weights}")
        logger.info("")
        
        self.connect_db()
        
        try:
            trading_days = self.get_trading_days(start_date, end_date)
            logger.info(f"交易日: {len(trading_days)}天")
            logger.info("")
            
            for i, date in enumerate(trading_days):
                if i % 20 == 0:
                    progress = (i + 1) / len(trading_days) * 100
                    logger.info(f"📊 进度: {i+1}/{len(trading_days)} ({progress:.1f}%)")
                
                # 选股
                picks = self.select_stocks(date)
                
                # 评估选股结果
                if picks and i + self.hold_days < len(trading_days):
                    self._evaluate_picks(date, picks, trading_days)
                
                # 记录每日统计
                self.daily_stats.append({
                    'date': date,
                    'pick_count': len(picks),
                    'avg_score': np.mean([p['score'] for p in picks]) if picks else 0
                })
            
            self._generate_report()
            
        finally:
            self.close_db()
    
    def select_stocks(self, date: str) -> List[Dict]:
        """选股逻辑"""
        # 清理冷却期
        for code in list(self.recent_picks.keys()):
            last_date = self.recent_picks[code]
            days_diff = (datetime.strptime(date, '%Y-%m-%d') - 
                        datetime.strptime(last_date, '%Y-%m-%d')).days
            if days_diff > self.cooling_days:
                del self.recent_picks[code]
        
        # 获取股票池
        stocks = self._get_stock_list(date)
        if len(stocks) < 10:
            return []
        
        # 过滤冷却期
        stocks = [s for s in stocks if s['code'] not in self.recent_picks]
        
        # Z-score评分
        picks = self._score_stocks(stocks)
        
        # 记录选股
        for pick in picks:
            self.recent_picks[pick['code']] = date
        
        return picks[:self.max_picks]
    
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
                k.code, k.open, k.close,
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
            pe_score = row[4]
            roe_score = row[5]
            
            if pe_score is None or roe_score is None:
                continue
            
            stocks.append({
                'code': row[0],
                'price': float(row[1]),
                'close': float(row[2]),
                'reversal': float(row[3]) if row[3] is not None else 0,
                'quality': float(roe_score),
                'valuation': float(pe_score),
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
                zscores[code].get('quality', 0) * self.factor_weights['quality'] +
                zscores[code].get('valuation', 0) * self.factor_weights['valuation'] +
                zscores[code].get('combined_trend', 0) * self.factor_weights['combined_trend'] -
                zscores[code].get('reversal', 0) * self.factor_weights['reversal']
            )
            
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.score_threshold:
                picks.append({
                    'code': code,
                    'name': stock['name'],
                    'score': score,
                    'price': stock['price']
                })
        
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks
    
    def _evaluate_picks(self, pick_date: str, picks: List[Dict], trading_days: List[str]):
        """评估选股结果"""
        cursor = self.conn.cursor()
        
        # 确定未来日期
        current_idx = trading_days.index(pick_date)
        future_idx_1d = min(current_idx + 1, len(trading_days) - 1)
        future_idx_3d = min(current_idx + 3, len(trading_days) - 1)
        
        date_1d = trading_days[future_idx_1d]
        date_3d = trading_days[future_idx_3d]
        
        # 获取市场基准
        market_1d = self.get_market_benchmark(pick_date, 1)
        market_3d = self.get_market_benchmark(pick_date, 3)
        
        for pick in picks:
            code = pick['code']
            
            # 获取买入价格（T+1开盘价）
            cursor.execute("""
                SELECT open, close FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, date_1d))
            
            row = cursor.fetchone()
            if not row:
                continue
            
            entry_price = float(row[0])
            
            # 获取1日后收盘价
            cursor.execute("""
                SELECT close FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, date_1d))
            row_1d = cursor.fetchone()
            exit_price_1d = float(row_1d[0]) if row_1d else entry_price
            
            # 获取3日后收盘价
            cursor.execute("""
                SELECT close FROM stock_kline
                WHERE code = %s AND trade_date = %s
            """, (code, date_3d))
            row_3d = cursor.fetchone()
            exit_price_3d = float(row_3d[0]) if row_3d else exit_price_1d
            
            # 计算收益率（扣除成本）
            return_1d = (exit_price_1d - entry_price) / entry_price - self.total_cost
            return_3d = (exit_price_3d - entry_price) / entry_price - self.total_cost
            
            self.pick_results.append(PickResult(
                date=pick_date,
                code=code,
                name=pick['name'],
                score=pick['score'],
                entry_price=entry_price,
                exit_price_1d=exit_price_1d,
                exit_price_3d=exit_price_3d,
                return_1d=return_1d,
                return_3d=return_3d,
                market_return_1d=market_1d,
                market_return_3d=market_3d
            ))
        
        cursor.close()
    
    def _calculate_ic(self) -> Dict[str, float]:
        """计算因子IC值"""
        if not self.pick_results:
            return {}
        
        df = pd.DataFrame([asdict(r) for r in self.pick_results])
        
        # 计算得分与收益率的相关性
        ic_1d = df['score'].corr(pd.Series(df['return_1d']))
        ic_3d = df['score'].corr(pd.Series(df['return_3d']))
        
        return {
            'ic_1d': round(ic_1d, 4) if not pd.isna(ic_1d) else 0,
            'ic_3d': round(ic_3d, 4) if not pd.isna(ic_3d) else 0
        }
    
    def _generate_report(self):
        """生成评估报告"""
        if not self.pick_results:
            logger.warning("无选股结果")
            return
        
        df = pd.DataFrame([asdict(r) for r in self.pick_results])
        
        # 基础统计
        total_picks = len(df)
        avg_score = df['score'].mean()
        
        # 一日收益统计
        winning_1d = len(df[df['return_1d'] > 0])
        win_rate_1d = winning_1d / total_picks
        avg_return_1d = df['return_1d'].mean()
        avg_excess_1d = (df['return_1d'] - df['market_return_1d']).mean()
        
        # 三日收益统计
        winning_3d = len(df[df['return_3d'] > 0])
        win_rate_3d = winning_3d / total_picks
        avg_return_3d = df['return_3d'].mean()
        avg_excess_3d = (df['return_3d'] - df['market_return_3d']).mean()
        
        # IC值
        ic = self._calculate_ic()
        
        # Top10超额收益
        top10 = df.nlargest(min(10, len(df)), 'score')
        top10_return_1d = top10['return_1d'].mean()
        top10_excess_1d = (top10['return_1d'] - top10['market_return_1d']).mean()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"V12选股能力评估报告 | 持仓: {self.hold_days}天")
        logger.info("=" * 80)
        logger.info(f"总选股次数: {total_picks}次")
        logger.info(f"平均选股得分: {avg_score:.1f}")
        logger.info("")
        logger.info("=== 一日收益率评估 ===")
        logger.info(f"胜率: {win_rate_1d*100:.1f}%")
        logger.info(f"平均收益: {avg_return_1d*100:.2f}%")
        logger.info(f"平均超额: {avg_excess_1d*100:.2f}%")
        logger.info(f"Top10平均收益: {top10_return_1d*100:.2f}%")
        logger.info(f"Top10超额收益: {top10_excess_1d*100:.2f}%")
        logger.info(f"IC值: {ic['ic_1d']:.4f}")
        logger.info("")
        logger.info("=== 三日收益率评估 ===")
        logger.info(f"胜率: {win_rate_3d*100:.1f}%")
        logger.info(f"平均收益: {avg_return_3d*100:.2f}%")
        logger.info(f"平均超额: {avg_excess_3d*100:.2f}%")
        logger.info(f"IC值: {ic['ic_3d']:.4f}")
        logger.info("=" * 80)
        
        # 保存详细结果
        suffix = f"{self.hold_days}d"
        df.to_csv(f'v12_stock_picker_{suffix}_results.csv', index=False)
        
        summary = {
            'version': 'V12-StockPicker',
            'hold_days': self.hold_days,
            'total_picks': total_picks,
            'avg_score': round(avg_score, 2),
            'win_rate_1d': round(win_rate_1d, 4),
            'avg_return_1d': round(avg_return_1d, 6),
            'avg_excess_1d': round(avg_excess_1d, 6),
            'top10_return_1d': round(top10_return_1d, 6),
            'top10_excess_1d': round(top10_excess_1d, 6),
            'ic_1d': ic['ic_1d'],
            'win_rate_3d': round(win_rate_3d, 4),
            'avg_return_3d': round(avg_return_3d, 6),
            'avg_excess_3d': round(avg_excess_3d, 6),
            'ic_3d': ic['ic_3d'],
            'factor_weights': self.factor_weights
        }
        
        with open(f'v12_stock_picker_{suffix}_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"结果已保存: v12_stock_picker_{suffix}_results.csv, v12_stock_picker_{suffix}_summary.json")


# 主函数
if __name__ == '__main__':
    import sys
    
    # 默认回测2024-2026年，持仓1天
    start_date = sys.argv[1] if len(sys.argv) > 1 else '2024-01-02'
    end_date = sys.argv[2] if len(sys.argv) > 2 else '2026-04-08'
    hold_days = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    
    evaluator = StockPickerEvaluator(hold_days=hold_days)
    evaluator.evaluate(start_date, end_date)
