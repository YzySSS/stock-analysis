#!/usr/bin/env python3
"""
V12策略 2年回测 (V4版本 - 接入清洗后估值因子)
===============================================

回测配置:
- 回测周期: 2024-01-01 至 2026-04-07 (约2年)
- 交易模型: T+1 (选股日收盘后运行，次日开盘买入，次日收盘卖出)
- 交易成本: 佣金0.05% + 印花税0.1% + 滑点0.1%
- 止损机制: -5%触发止损
- 选股规则: Z-score评分 ≥ 55分即入选，最多5只
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
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}


@dataclass
class TradeRecord:
    """交易记录"""
    entry_date: str
    exit_date: str
    code: str
    entry_price: float
    exit_price: float
    gross_return: float
    net_return: float
    score: float
    factors: Dict
    exit_reason: str


class V12BacktestEngineV4:
    """V12回测引擎 V4 - 2年回测版本"""
    
    def __init__(self, score_threshold=55):
        self.score_threshold = score_threshold
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        
        # 成本配置
        self.commission_rate = 0.0005  # 佣金0.05%
        self.stamp_tax_rate = 0.001    # 印花税0.1%
        self.slippage = 0.001          # 滑点0.1%
        
        # 因子权重（V4版本）
        self.weights = {
            'trend': 0.20,
            'momentum': 0.15,
            'quality': 0.20,
            'sentiment': 0.15,
            'valuation': 0.20,
            'liquidity': 0.10
        }
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT trade_date FROM stock_kline 
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                """, (start_date, end_date))
                return [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_historical_prices(self, code: str, end_date: str, days: int = 65) -> List[float]:
        """获取历史价格"""
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT close FROM stock_kline 
                    WHERE code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """, (code, end_date, days))
                prices = [float(row[0]) for row in cursor.fetchall()]
                prices.reverse()
                return prices
        finally:
            conn.close()
    
    def get_stock_data(self, date: str) -> List[Dict]:
        """获取某日的股票数据（包含清洗后的基本面）"""
        conn = pymysql.connect(**DB_CONFIG)
        stocks = []
        try:
            with conn.cursor() as cursor:
                # 获取前一日日期
                cursor.execute("""
                    SELECT MAX(trade_date) FROM stock_kline 
                    WHERE trade_date < %s
                """, (date,))
                prev_date = cursor.fetchone()[0]
                
                # 获取ST/退市列表
                cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
                exclude = {row[0] for row in cursor.fetchall()}
                
                # 【V4更新】获取当日数据 + 清洗后的基本面
                cursor.execute("""
                    SELECT 
                        k.code, 
                        k.open, 
                        k.turnover, 
                        k_prev.pct_change,
                        b.pe_clean,
                        b.pb_clean,
                        b.roe_clean,
                        b.pe_score,
                        b.roe_score,
                        b.name
                    FROM stock_kline k
                    LEFT JOIN stock_kline k_prev ON k.code = k_prev.code 
                        AND k_prev.trade_date = %s
                    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
                    WHERE k.trade_date = %s AND k.open > 0
                """, (prev_date, date))
                
                for row in cursor.fetchall():
                    code = row[0]
                    if code in exclude:
                        continue
                    
                    pe_clean = row[4]
                    if pe_clean is not None and (pe_clean < -1000 or pe_clean > 1000):
                        continue
                    
                    stocks.append({
                        'code': code,
                        'price': float(row[1]),
                        'turnover': float(row[2]) if row[2] else 0,
                        'prev_change': float(row[3]) if row[3] else 0,
                        'pe_clean': float(pe_clean) if pe_clean is not None else None,
                        'pb_clean': float(row[5]) if row[5] is not None else None,
                        'roe_clean': float(row[6]) if row[6] is not None else None,
                        'pe_score': float(row[7]) if row[7] is not None else None,
                        'roe_score': float(row[8]) if row[8] is not None else None,
                        'name': row[9] or ''
                    })
        finally:
            conn.close()
        
        return stocks
    
    def calculate_raw_factors(self, stock: Dict, prices: List[float]) -> Dict[str, float]:
        """计算原始因子"""
        factors = {}
        
        # 1. 趋势因子
        if len(prices) >= 25:
            ma20_now = sum(prices[-20:]) / 20
            ma20_prev = sum(prices[-25:-5]) / 20
            factors['trend'] = (ma20_now - ma20_prev) / ma20_prev * 252 if ma20_prev > 0 else 0
        else:
            factors['trend'] = 0
        
        # 2. 动量因子
        if len(prices) >= 21:
            factors['momentum'] = (prices[-1] - prices[-21]) / prices[-21] * 100
        else:
            factors['momentum'] = 0
        
        # 3. 质量因子 - 接入roe_clean
        roe_score = stock.get('roe_score')
        if roe_score is not None and not np.isnan(roe_score):
            factors['quality'] = roe_score
        else:
            if len(prices) >= 21:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                          for i in range(-20, 0)]
                volatility = np.std(returns) if returns else 10
                factors['quality'] = 20 - volatility * 0.5
            else:
                factors['quality'] = 0
        
        # 4. 情绪因子
        factors['sentiment'] = stock.get('prev_change', 0)
        
        # 5. 估值因子 - 接入pe_score
        pe_score = stock.get('pe_score')
        if pe_score is not None and not np.isnan(pe_score):
            factors['valuation'] = pe_score
        else:
            factors['valuation'] = -25  # 默认值
        
        # 6. 流动性因子
        turnover = stock.get('turnover', 0)
        factors['liquidity'] = np.log(turnover + 1) if turnover > 0 else 0
        
        return factors
    
    def calculate_zscore(self, all_factors: List[Tuple[str, Dict]]) -> Dict[str, Dict[str, float]]:
        """Z-score标准化"""
        if not all_factors:
            return {}
        
        factor_names = list(self.weights.keys())
        factor_values = {f: [] for f in factor_names}
        
        for code, factors in all_factors:
            for f in factor_names:
                v = factors.get(f, 0)
                if not np.isnan(v):
                    factor_values[f].append((code, v))
        
        zscores = {}
        for factor_name in factor_names:
            values = [v for _, v in factor_values[factor_name]]
            if len(values) < 2:
                continue
            
            mean = np.mean(values)
            std = np.std(values)
            if std < 1e-10:
                std = 1e-10
            
            for code, value in factor_values[factor_name]:
                zscore = (value - mean) / std
                zscores.setdefault(code, {})[factor_name] = np.clip(zscore, -3, 3)
        
        return zscores
    
    def hard_filter(self, stock: Dict, prices: List[float]) -> Tuple[bool, str]:
        """硬性过滤"""
        price = stock['price']
        turnover = stock['turnover']
        
        if price < 5 or price > 200:
            return False, '股价范围'
        
        if turnover < 0.5:
            return False, '成交额过低'
        
        if len(prices) < 21:
            return False, '数据不足'
        
        ma20 = sum(prices[-20:]) / 20
        if price < ma20 * 0.90:
            return False, '跌破MA20'
        
        return True, '通过'
    
    def run_daily_picking(self, date: str) -> List[Dict]:
        """单日选股"""
        stocks = self.get_stock_data(date)
        
        candidates = []
        for stock in stocks:
            prices = self.get_historical_prices(stock['code'], date)
            
            passed, reason = self.hard_filter(stock, prices)
            if not passed:
                continue
            
            raw_factors = self.calculate_raw_factors(stock, prices)
            candidates.append({
                'code': stock['code'],
                'stock': stock,
                'prices': prices,
                'raw_factors': raw_factors
            })
        
        if len(candidates) < 10:
            return []
        
        # Z-score标准化
        all_factors = [(c['code'], c['raw_factors']) for c in candidates]
        zscores = self.calculate_zscore(all_factors)
        
        # 选达标股票
        picks = []
        for candidate in candidates:
            code = candidate['code']
            if code not in zscores:
                continue
            
            weighted_zscore = sum(
                zscores[code].get(k, 0) * self.weights[k]
                for k in self.weights
            )
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.score_threshold:
                picks.append({
                    'code': code,
                    'score': score,
                    'zscores': zscores[code],
                    'stock': candidate['stock']
                })
        
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:5]
    
    def simulate_trade(self, date: str, picks: List[Dict], next_date: str) -> List[TradeRecord]:
        """模拟交易"""
        trades = []
        
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cursor:
                for pick in picks:
                    code = pick['code']
                    
                    cursor.execute("""
                        SELECT open, high, low, close FROM stock_kline 
                        WHERE code = %s AND trade_date = %s
                    """, (code, next_date))
                    row = cursor.fetchone()
                    if not row:
                        continue
                    
                    open_price = float(row[0])
                    high_price = float(row[1])
                    low_price = float(row[2])
                    close_price = float(row[3])
                    
                    # 买入价（滑点）
                    entry_price = open_price * (1 + self.slippage)
                    
                    # 止损检查
                    stop_loss_price = entry_price * 0.95
                    
                    if low_price <= stop_loss_price:
                        exit_price = stop_loss_price
                        exit_reason = '止损(-5%)'
                    else:
                        exit_price = close_price * (1 - self.slippage)
                        exit_reason = 'T+1平仓'
                    
                    # 计算收益
                    gross_return = (exit_price - entry_price) / entry_price * 100
                    cost = (self.commission_rate * 2 + self.stamp_tax_rate) * 100
                    net_return = gross_return - cost
                    
                    trades.append(TradeRecord(
                        entry_date=next_date,
                        exit_date=next_date,
                        code=code,
                        entry_price=round(entry_price, 2),
                        exit_price=round(exit_price, 2),
                        gross_return=round(gross_return, 2),
                        net_return=round(net_return, 2),
                        score=pick['score'],
                        factors=pick['zscores'],
                        exit_reason=exit_reason
                    ))
        finally:
            conn.close()
        
        return trades
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        all_dates = self.get_trade_dates(start_date, end_date)
        if len(all_dates) < 2:
            logger.error("交易日不足")
            return {}
        
        logger.info("=" * 70)
        logger.info("V12策略 2年回测 (V4版本 - 接入清洗后估值因子)")
        logger.info("=" * 70)
        logger.info(f"回测区间: {all_dates[0]} 至 {all_dates[-1]}")
        logger.info(f"交易日数: {len(all_dates)}天")
        logger.info(f"评分阈值: {self.score_threshold}")
        logger.info("=" * 70)
        
        # 逐日回测
        total_days = len(all_dates) - 1
        for i, date in enumerate(all_dates[:-1]):
            next_date = all_dates[i + 1]
            
            if i % 20 == 0:
                logger.info(f"进度: {i}/{total_days} ({i/total_days*100:.1f}%) - 当前: {date}")
            
            picks = self.run_daily_picking(date)
            
            if picks:
                trades = self.simulate_trade(date, picks, next_date)
                self.trades.extend(trades)
                
                if trades:
                    avg_return = sum(t.net_return for t in trades) / len(trades)
                    stop_count = len([t for t in trades if '止损' in t.exit_reason])
                    self.daily_stats.append({
                        'date': next_date,
                        'pick_count': len(picks),
                        'trade_count': len(trades),
                        'stop_count': stop_count,
                        'avg_return': round(avg_return, 2)
                    })
        
        return self.generate_report()
    
    def generate_report(self) -> Dict:
        """生成报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return {}
        
        net_returns = [t.net_return for t in self.trades]
        wins = len([r for r in net_returns if r > 0])
        
        # 复利计算
        cumulative = 1.0
        for r in net_returns:
            cumulative *= (1 + r / 100)
        cumulative_return = (cumulative - 1) * 100
        
        # 年化收益
        trade_days = len(self.daily_stats)
        years = trade_days / 252 if trade_days > 0 else 0
        annualized_return = ((cumulative ** (1/years)) - 1) * 100 if years > 0 else 0
        
        # 最大回撤
        daily_returns = [d['avg_return'] for d in self.daily_stats]
        peak = 0
        max_drawdown = 0
        running = 0
        for r in daily_returns:
            running += r
            if running > peak:
                peak = running
            drawdown = peak - running
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 止损统计
        stop_count = len([t for t in self.trades if '止损' in t.exit_reason])
        
        report = {
            'version': 'V4_2Year_Backtest',
            'config': {
                'score_threshold': self.score_threshold,
                'commission': self.commission_rate,
                'stamp_tax': self.stamp_tax_rate,
                'slippage': self.slippage,
                'stop_loss': 0.05
            },
            'summary': {
                'total_trades': len(self.trades),
                'trade_days': trade_days,
                'win_rate': round(wins / len(net_returns) * 100, 1),
                'avg_gross_return': round(sum([t.gross_return for t in self.trades]) / len(self.trades), 2),
                'avg_net_return': round(sum(net_returns) / len(net_returns), 2),
                'cumulative_return': round(cumulative_return, 2),
                'annualized_return': round(annualized_return, 2),
                'max_return': round(max(net_returns), 2),
                'min_return': round(min(net_returns), 2),
                'max_drawdown': round(max_drawdown, 2),
                'stop_loss_count': stop_count,
                'stop_loss_rate': round(stop_count / len(self.trades) * 100, 1),
                'sharpe_ratio': round((annualized_return / max_drawdown) if max_drawdown > 0 else 0, 2)
            },
            'trades': [asdict(t) for t in self.trades]
        }
        
        return report
    
    def save_results(self, report: Dict, prefix: str = 'v12_v4_2year'):
        """保存结果"""
        if not report:
            return
        
        # 保存交易明细
        df = pd.DataFrame(report['trades'])
        trades_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_trades.csv'
        df.to_csv(trades_path, index=False, encoding='utf-8-sig')
        
        # 保存汇总
        summary_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        s = report['summary']
        print("\n" + "=" * 70)
        print("📊 V12策略 2年回测报告 (V4 - 接入清洗后估值因子)")
        print("=" * 70)
        print(f"总交易: {s['total_trades']}笔 | 交易天数: {s['trade_days']}天")
        print(f"\n📈 收益统计:")
        print(f"  胜率: {s['win_rate']}%")
        print(f"  平均毛收益: {s['avg_gross_return']}%")
        print(f"  平均净收益: {s['avg_net_return']}%")
        print(f"  累计收益: {s['cumulative_return']}%")
        print(f"  年化收益: {s['annualized_return']}%")
        print(f"  夏普比率: {s['sharpe_ratio']}")
        print(f"\n📉 风险控制:")
        print(f"  最大回撤: {s['max_drawdown']}%")
        print(f"  止损次数: {s['stop_loss_count']}次 ({s['stop_loss_rate']}%)")
        print(f"  最大单笔: {s['max_return']}% | 最小单笔: {s['min_return']}%")
        print("=" * 70)
        print(f"\n✅ 结果已保存:")
        print(f"   交易明细: {trades_path}")
        print(f"   汇总报告: {summary_path}")
        print("=" * 70)


def main():
    """主函数"""
    engine = V12BacktestEngineV4(score_threshold=55)
    
    # 2年回测: 2024-01-01 至 2026-04-07
    report = engine.run_backtest('2024-01-01', '2026-04-07')
    
    if report:
        engine.save_results(report, prefix='v12_v4_2year')
    else:
        logger.error("回测失败")


if __name__ == '__main__':
    main()
