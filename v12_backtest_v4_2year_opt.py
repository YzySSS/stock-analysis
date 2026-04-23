#!/usr/bin/env python3
"""
V12策略 2年回测优化版 (V4 - 批量查询加速)
==========================================
优化点:
1. 批量获取历史价格数据（而非逐股查询）
2. 批量获取交易日OHLCV数据
3. 内存缓存避免重复查询
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


class V12BacktestEngineV4Optimized:
    """V12回测引擎 V4优化版 - 批量查询"""
    
    def __init__(self, score_threshold=55):
        self.score_threshold = score_threshold
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.slippage = 0.001
        
        self.weights = {
            'trend': 0.20, 'momentum': 0.15, 'quality': 0.20,
            'sentiment': 0.15, 'valuation': 0.20, 'liquidity': 0.10
        }
        
        self.conn = None
        self.cache = {}
    
    def connect(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM stock_kline 
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (start_date, end_date))
            return [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    
    def batch_get_historical_prices(self, codes: List[str], end_date: str, days: int = 65) -> Dict[str, List[float]]:
        """批量获取历史价格"""
        if not codes:
            return {}
        
        # 检查缓存
        cache_key = f"{end_date}_{days}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            result = {code: cached.get(code, []) for code in codes if code in cached}
            missing_codes = [code for code in codes if code not in cached]
            if not missing_codes:
                return result
            codes = missing_codes
        else:
            result = {}
            self.cache[cache_key] = {}
        
        with self.conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(codes))
            cursor.execute(f"""
                SELECT code, close FROM stock_kline 
                WHERE code IN ({placeholders})
                AND trade_date <= %s
                ORDER BY code, trade_date DESC
                LIMIT {len(codes) * days}
            """, tuple(codes) + (end_date,))
            
            for row in cursor.fetchall():
                code, close = row
                if code not in self.cache[cache_key]:
                    self.cache[cache_key][code] = []
                self.cache[cache_key][code].append(float(close))
            
            # 反转顺序（时间正序）并限制长度
            for code in codes:
                if code in self.cache[cache_key]:
                    prices = self.cache[cache_key][code][:days]
                    prices.reverse()
                    self.cache[cache_key][code] = prices
                    result[code] = prices
        
        return result
    
    def get_stock_data_batch(self, date: str, prev_date: str) -> pd.DataFrame:
        """批量获取股票数据"""
        with self.conn.cursor() as cursor:
            # 获取ST/退市列表
            cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
            exclude = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("""
                SELECT 
                    k.code, k.open, k.turnover, k_prev.pct_change,
                    b.pe_clean, b.pb_clean, b.roe_clean,
                    b.pe_score, b.roe_score, b.name
                FROM stock_kline k
                LEFT JOIN stock_kline k_prev ON k.code = k_prev.code 
                    AND k_prev.trade_date = %s
                LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
                WHERE k.trade_date = %s AND k.open > 0
                AND k.open BETWEEN 5 AND 150
                AND k.turnover >= 0.5
            """, (prev_date, date))
            
            data = []
            for row in cursor.fetchall():
                code = row[0]
                if code in exclude:
                    continue
                data.append({
                    'code': code,
                    'price': float(row[1]),
                    'turnover': float(row[2]) if row[2] else 0,
                    'prev_change': float(row[3]) if row[3] else 0,
                    'pe_clean': float(row[4]) if row[4] else None,
                    'pb_clean': float(row[5]) if row[5] else None,
                    'roe_clean': float(row[6]) if row[6] else None,
                    'pe_score': float(row[7]) if row[7] else None,
                    'roe_score': float(row[8]) if row[8] else None,
                    'name': row[9] or ''
                })
            
            return pd.DataFrame(data)
    
    def get_next_day_prices(self, date: str, codes: List[str]) -> Dict[str, Dict]:
        """批量获取次日价格数据"""
        if not codes:
            return {}
        
        with self.conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(codes))
            cursor.execute(f"""
                SELECT code, open, high, low, close 
                FROM stock_kline 
                WHERE trade_date = %s AND code IN ({placeholders})
            """, (date,) + tuple(codes))
            
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4])
                }
            return result
    
    def calculate_factors(self, df: pd.DataFrame, price_history: Dict[str, List[float]]) -> pd.DataFrame:
        """批量计算因子"""
        factors_list = []
        
        for _, row in df.iterrows():
            code = row['code']
            prices = price_history.get(code, [])
            
            if len(prices) < 21:
                continue
            
            # MA20过滤
            ma20 = sum(prices[-20:]) / 20
            if row['price'] < ma20 * 0.90:
                continue
            
            factors = {'code': code}
            
            # 趋势因子
            if len(prices) >= 25:
                ma20_now = sum(prices[-20:]) / 20
                ma20_prev = sum(prices[-25:-5]) / 20
                factors['trend'] = (ma20_now - ma20_prev) / ma20_prev * 252 if ma20_prev > 0 else 0
            else:
                factors['trend'] = 0
            
            # 动量因子
            factors['momentum'] = (prices[-1] - prices[-21]) / prices[-21] * 100
            
            # 质量因子
            roe_score = row.get('roe_score')
            if roe_score is not None and not np.isnan(roe_score):
                factors['quality'] = roe_score
            else:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 for i in range(-20, 0)]
                volatility = np.std(returns) if returns else 10
                factors['quality'] = 20 - volatility * 0.5
            
            # 情绪因子
            factors['sentiment'] = row['prev_change']
            
            # 估值因子
            pe_score = row.get('pe_score')
            factors['valuation'] = pe_score if pe_score is not None else -25
            
            # 流动性因子
            factors['liquidity'] = np.log(row['turnover'] + 1) if row['turnover'] > 0 else 0
            
            factors['name'] = row['name']
            factors['price'] = row['price']
            factors_list.append(factors)
        
        return pd.DataFrame(factors_list) if factors_list else pd.DataFrame()
    
    def calculate_zscore_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """向量化Z-score计算"""
        factor_cols = list(self.weights.keys())
        
        for col in factor_cols:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                if std > 1e-10:
                    df[f'{col}_z'] = ((df[col] - mean) / std).clip(-3, 3)
                else:
                    df[f'{col}_z'] = 0
        
        # 加权得分
        z_cols = [f'{c}_z' for c in factor_cols]
        df['weighted_z'] = sum(df[f'{c}_z'] * w for c, w in self.weights.items() if f'{c}_z' in df.columns)
        df['score'] = 50 + df['weighted_z'] * 15
        df['score'] = df['score'].clip(0, 100)
        
        return df
    
    def run_daily_picking(self, date: str, prev_date: str) -> pd.DataFrame:
        """单日选股 - 优化版"""
        # 批量获取股票数据
        df = self.get_stock_data_batch(date, prev_date)
        if df.empty:
            return pd.DataFrame()
        
        # 批量获取历史价格
        codes = df['code'].tolist()
        price_history = self.batch_get_historical_prices(codes, date)
        
        # 批量计算因子
        df_factors = self.calculate_factors(df, price_history)
        if df_factors.empty:
            return pd.DataFrame()
        
        # 向量化Z-score
        df_scored = self.calculate_zscore_vectorized(df_factors)
        
        # 筛选达标股票
        df_picks = df_scored[df_scored['score'] >= self.score_threshold].copy()
        df_picks = df_picks.sort_values('score', ascending=False).head(5)
        
        return df_picks
    
    def simulate_trades(self, date: str, picks: pd.DataFrame) -> List[TradeRecord]:
        """批量模拟交易"""
        if picks.empty:
            return []
        
        codes = picks['code'].tolist()
        next_day_prices = self.get_next_day_prices(date, codes)
        
        trades = []
        for _, pick in picks.iterrows():
            code = pick['code']
            if code not in next_day_prices:
                continue
            
            prices = next_day_prices[code]
            entry_price = prices['open'] * (1 + self.slippage)
            stop_loss_price = entry_price * 0.95
            
            if prices['low'] <= stop_loss_price:
                exit_price = stop_loss_price
                exit_reason = '止损(-5%)'
            else:
                exit_price = prices['close'] * (1 - self.slippage)
                exit_reason = 'T+1平仓'
            
            gross_return = (exit_price - entry_price) / entry_price * 100
            cost = (self.commission_rate * 2 + self.stamp_tax_rate) * 100
            net_return = gross_return - cost
            
            trades.append(TradeRecord(
                entry_date=date,
                exit_date=date,
                code=code,
                entry_price=round(entry_price, 2),
                exit_price=round(exit_price, 2),
                gross_return=round(gross_return, 2),
                net_return=round(net_return, 2),
                score=pick['score'],
                factors={k: pick.get(f'{k}_z', 0) for k in self.weights.keys()},
                exit_reason=exit_reason
            ))
        
        return trades
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """运行回测 - 优化版"""
        self.connect()
        
        try:
            all_dates = self.get_trade_dates(start_date, end_date)
            if len(all_dates) < 2:
                logger.error("交易日不足")
                return {}
            
            logger.info("=" * 70)
            logger.info("V12策略 2年回测 V4优化版 (批量查询)")
            logger.info("=" * 70)
            logger.info(f"回测区间: {all_dates[0]} 至 {all_dates[-1]}")
            logger.info(f"交易日数: {len(all_dates)}天")
            logger.info("=" * 70)
            
            total_days = len(all_dates) - 1
            for i in range(total_days):
                date = all_dates[i]
                next_date = all_dates[i + 1]
                
                if i % 30 == 0 or i == total_days - 1:
                    logger.info(f"进度: {i}/{total_days} ({i/total_days*100:.1f}%) - {date}")
                
                # 选股
                picks = self.run_daily_picking(date, next_date)
                
                if not picks.empty:
                    trades = self.simulate_trades(next_date, picks)
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
                
                # 定期清理缓存
                if i % 50 == 0:
                    self.cache.clear()
            
            return self.generate_report()
        finally:
            self.close()
    
    def generate_report(self) -> Dict:
        """生成报告"""
        if not self.trades:
            logger.warning("无交易记录")
            return {}
        
        net_returns = [t.net_return for t in self.trades]
        wins = len([r for r in net_returns if r > 0])
        
        cumulative = 1.0
        for r in net_returns:
            cumulative *= (1 + r / 100)
        cumulative_return = (cumulative - 1) * 100
        
        trade_days = len(self.daily_stats)
        years = trade_days / 252 if trade_days > 0 else 0
        annualized_return = ((cumulative ** (1/years)) - 1) * 100 if years > 0 else 0
        
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
        
        stop_count = len([t for t in self.trades if '止损' in t.exit_reason])
        
        return {
            'version': 'V4_2Year_Optimized',
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
    
    def save_results(self, report: Dict, prefix: str = 'v12_v4_2year_opt'):
        """保存结果"""
        if not report:
            return
        
        df = pd.DataFrame(report['trades'])
        trades_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_trades.csv'
        df.to_csv(trades_path, index=False, encoding='utf-8-sig')
        
        summary_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        s = report['summary']
        print("\n" + "=" * 70)
        print("📊 V12策略 2年回测报告 V4优化版")
        print("=" * 70)
        print(f"总交易: {s['total_trades']}笔 | 交易天数: {s['trade_days']}天")
        print(f"\n📈 收益统计:")
        print(f"  胜率: {s['win_rate']}%")
        print(f"  平均净收益: {s['avg_net_return']}%")
        print(f"  累计收益: {s['cumulative_return']}%")
        print(f"  年化收益: {s['annualized_return']}%")
        print(f"\n📉 风险控制:")
        print(f"  最大回撤: {s['max_drawdown']}%")
        print(f"  止损率: {s['stop_loss_rate']}%")
        print(f"  夏普比率: {s['sharpe_ratio']}")
        print("=" * 70)
        print(f"\n✅ 结果已保存:")
        print(f"   {trades_path}")
        print(f"   {summary_path}")


def main():
    engine = V12BacktestEngineV4Optimized(score_threshold=55)
    report = engine.run_backtest('2024-01-01', '2026-04-07')
    if report:
        engine.save_results(report)
    else:
        logger.error("回测失败")


if __name__ == '__main__':
    main()
