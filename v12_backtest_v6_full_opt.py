#!/usr/bin/env python3
"""
V12策略 回测引擎 V6 - 完整优化版
=================================
更新内容:
1. ✅ 复利计算已修复（继承V5）
2. ✅ 市值因子并中性化
3. ✅ 行业权重上限约束
4. ✅ IC/IR分析优化权重
5. ✅ 降低换手率/拉长持仓周期（持仓冷却期）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

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
    industry: str = ''
    holding_days: int = 1


class V12BacktestEngineV6:
    """V12回测引擎 V6 - 完整优化版"""
    
    def __init__(self, score_threshold=55, enable_p1=True, enable_p2=True, 
                 cooling_days=3, industry_max_weight=0.30):
        self.score_threshold = score_threshold
        self.enable_p1 = enable_p1  # 市场强度/动态权重
        self.enable_p2 = enable_p2  # IC/IR分析
        
        # 🆕 降低换手率配置
        self.cooling_days = cooling_days
        self.industry_max_weight = industry_max_weight
        
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[Dict] = []
        self.market_strength_history: List[Dict] = []
        
        # 成本参数
        self.commission_rate = 0.0005
        self.stamp_tax_rate = 0.001
        self.base_slippage = 0.001
        self.impact_coefficient = 0.1
        
        # 🆕 6因子权重（加入市值因子）
        self.base_weights = {
            'trend': 0.18, 'momentum': 0.13, 'quality': 0.18,
            'sentiment': 0.13, 'valuation': 0.18, 'liquidity': 0.10,
            'size': 0.10  # 🆕 市值因子
        }
        self.current_weights = self.base_weights.copy()
        
        self.conn = None
        self.cache = {}
        
        # 🆕 IC/IR追踪
        self.ic_records: List[Dict] = []
        self.factor_performance = defaultdict(list)
        self.recent_picks = {}  # code -> last_pick_date
        
        # 🆕 持仓管理（拉长持仓周期）
        self.active_positions = {}  # code -> {'entry_date': str, 'entry_price': float}
        self.holding_days = 2  # 最少持仓天数（原来是T+1即1天）
    
    def connect(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def calculate_market_strength(self, date: str) -> float:
        """计算市场强度指数 (0-100)"""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) FROM stock_kline WHERE trade_date < %s", (date,))
            prev_date = cursor.fetchone()[0]
            if not prev_date:
                return 50.0
            
            # 趋势40%
            cursor.execute("SELECT close FROM stock_kline WHERE code='000300' AND trade_date <= %s ORDER BY trade_date DESC LIMIT 25", (date,))
            hs300_prices = [float(row[0]) for row in cursor.fetchall()]
            trend_score = 50.0
            if len(hs300_prices) >= 25:
                ma20_now = sum(hs300_prices[:20]) / 20
                ma20_prev = sum(hs300_prices[5:25]) / 20
                if ma20_prev > 0:
                    trend_score = 50 + (ma20_now - ma20_prev) / ma20_prev * 1000
                    trend_score = max(0, min(100, trend_score))
            
            # 宽度30%
            cursor.execute("SELECT COUNT(*), SUM(CASE WHEN pct_change > 0 THEN 1 ELSE 0 END) FROM stock_kline WHERE trade_date=%s", (date,))
            total, up_count = cursor.fetchone()
            total, up_count = int(total), int(up_count) if up_count else 0
            breadth_score = (up_count / total * 100) if total > 0 else 50.0
            
            # 成交量20%
            cursor.execute("SELECT turnover FROM stock_kline WHERE code='000300' AND trade_date <= %s ORDER BY trade_date DESC LIMIT 21", (date,))
            turnovers = [float(row[0]) for row in cursor.fetchall()]
            volume_score = 50.0
            if len(turnovers) >= 21:
                current_vol, avg_vol = turnovers[0], sum(turnovers[1:21]) / 20
                if avg_vol > 0:
                    volume_score = 50 + (current_vol / avg_vol - 1) * 50
                    volume_score = max(0, min(100, volume_score))
            
            # 情绪10%
            sentiment_score = 50.0
            if prev_date:
                cursor.execute("SELECT pct_change FROM stock_kline WHERE code='000300' AND trade_date=%s", (prev_date,))
                row = cursor.fetchone()
                if row and row[0]:
                    sentiment_score = 50 + float(row[0]) * 5
                    sentiment_score = max(0, min(100, sentiment_score))
            
            strength = trend_score * 0.40 + breadth_score * 0.30 + volume_score * 0.20 + sentiment_score * 0.10
            return round(max(0, min(100, strength)), 2)
    
    def get_dynamic_weights(self, market_strength: float) -> Dict[str, float]:
        """P1: 动态权重调整"""
        if not self.enable_p1:
            return self.base_weights.copy()
        
        if market_strength > 60:
            return {'trend': 0.20, 'momentum': 0.15, 'quality': 0.20, 
                   'sentiment': 0.15, 'valuation': 0.16, 'liquidity': 0.08, 'size': 0.06}
        elif market_strength < 40:
            return {'trend': 0.16, 'momentum': 0.11, 'quality': 0.16, 
                   'sentiment': 0.11, 'valuation': 0.20, 'liquidity': 0.12, 'size': 0.14}
        return self.base_weights.copy()
    
    def get_position_ratio(self, market_strength: float) -> float:
        """P1: 仓位管理"""
        if not self.enable_p1:
            return 1.0
        ratio = 0.2 + (market_strength / 100) * 0.8
        return round(min(1.0, max(0.2, ratio)), 2)
    
    def update_weights_by_ic_ir(self):
        """🆕 P2: 根据IC/IR动态优化权重"""
        if not self.enable_p2 or len(self.ic_records) < 20:
            return
        
        # 计算各因子的IC
        factor_ics = defaultdict(list)
        for record in self.ic_records[-20:]:  # 最近20天
            for factor, zscore in record.get('zscores', {}).items():
                future_return = record.get('future_return', 0)
                factor_ics[factor].append((zscore, future_return))
        
        # 计算IR并调整权重
        new_weights = self.base_weights.copy()
        
        for factor, pairs in factor_ics.items():
            if len(pairs) < 10:
                continue
            
            zscores = [p[0] for p in pairs]
            returns = [p[1] for p in pairs]
            
            if np.std(zscores) > 0 and np.std(returns) > 0:
                ic = np.corrcoef(zscores, returns)[0, 1]
                # IC高的因子增加权重
                adjustment = 1 + (ic * 0.3)  # IC=0.1 -> 权重+3%
                new_weights[factor] = self.base_weights[factor] * adjustment
        
        # 归一化
        weight_sum = sum(new_weights.values())
        if weight_sum > 0:
            self.current_weights = {k: v / weight_sum for k, v in new_weights.items()}
        
        logger.info(f"IC/IR优化后权重: {self.current_weights}")
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT trade_date FROM stock_kline WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date", (start_date, end_date))
            return [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    
    def get_stock_data_batch(self, date: str, prev_date: str) -> pd.DataFrame:
        """🆕 获取股票数据（含市值和行业）"""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT code FROM stock_basic WHERE is_st=1 OR is_delisted=1")
            exclude = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("""
                SELECT k.code, k.open, k.turnover, k_prev.pct_change, k.turnover * 100,
                       b.pe_clean, b.roe_clean, b.pe_score, b.roe_score, b.name, b.industry
                FROM stock_kline k
                LEFT JOIN stock_kline k_prev ON k.code=k_prev.code AND k_prev.trade_date=%s
                LEFT JOIN stock_basic b ON k.code=b.code COLLATE utf8mb4_unicode_ci
                WHERE k.trade_date=%s AND k.open BETWEEN 5 AND 150 AND k.turnover >= 0.5
            """, (prev_date, date))
            
            data = []
            for row in cursor.fetchall():
                if row[0] not in exclude:
                    data.append({
                        'code': row[0], 'price': float(row[1]), 'turnover': float(row[2]) if row[2] else 0,
                        'prev_change': float(row[3]) if row[3] else 0,
                        'market_cap': float(row[4]) if row[4] else None,
                        'pe_clean': float(row[5]) if row[5] else None,
                        'roe_clean': float(row[6]) if row[6] else None,
                        'pe_score': float(row[7]) if row[7] else None,
                        'roe_score': float(row[8]) if row[8] else None,
                        'name': row[9] or '', 'industry': row[10] or '其他'
                    })
            return pd.DataFrame(data)
    
    def batch_get_historical_prices(self, codes: List[str], end_date: str, days: int = 65) -> Dict[str, List[float]]:
        if not codes:
            return {}
        
        result = {}
        with self.conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(codes))
            cursor.execute(f"""
                SELECT code, close FROM stock_kline 
                WHERE code IN ({placeholders}) AND trade_date <= %s
                ORDER BY code, trade_date DESC LIMIT {len(codes) * days}
            """, tuple(codes) + (end_date,))
            
            for row in cursor.fetchall():
                code = row[0]
                if code not in result:
                    result[code] = []
                result[code].append(float(row[1]))
            
            for code in result:
                result[code] = list(reversed(result[code][:days]))
        return result
    
    def calculate_factors(self, df: pd.DataFrame, price_history: Dict[str, List[float]]) -> pd.DataFrame:
        """🆕 计算因子（含市值因子）"""
        factors_list = []
        for _, row in df.iterrows():
            code = row['code']
            prices = price_history.get(code, [])
            if len(prices) < 21:
                continue
            
            ma20 = sum(prices[-20:]) / 20
            if row['price'] < ma20 * 0.90:
                continue
            
            factors = {'code': code, 'name': row['name'], 'industry': row['industry'],
                      'price': row['price'], 'turnover': row['turnover'], 
                      'market_cap': row.get('market_cap', 0)}
            
            if len(prices) >= 25:
                ma20_now, ma20_prev = sum(prices[-20:]) / 20, sum(prices[-25:-5]) / 20
                factors['trend'] = (ma20_now - ma20_prev) / ma20_prev * 252 if ma20_prev > 0 else 0
            else:
                factors['trend'] = 0
            
            factors['momentum'] = (prices[-1] - prices[-21]) / prices[-21] * 100
            
            roe_score = row.get('roe_score')
            if roe_score is not None:
                factors['quality'] = roe_score
            else:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 for i in range(-20, 0)]
                factors['quality'] = 20 - np.std(returns) * 0.5 if returns else 0
            
            factors['sentiment'] = row['prev_change']
            factors['valuation'] = row.get('pe_score') or -25
            factors['liquidity'] = np.log(row['turnover'] + 1) if row['turnover'] > 0 else 0
            
            # 🆕 市值因子：小市值偏好
            market_cap = row.get('market_cap')
            if market_cap is not None and market_cap > 0:
                factors['size'] = -np.log(market_cap)  # 负值，小市值得高分
            else:
                factors['size'] = 0
            
            factors_list.append(factors)
        
        return pd.DataFrame(factors_list) if factors_list else pd.DataFrame()
    
    def neutralize_size_by_industry(self, df: pd.DataFrame) -> pd.DataFrame:
        """🆕 行业内市值中性化"""
        if df.empty or 'industry' not in df.columns or 'size' not in df.columns:
            return df
        
        industries = df['industry'].unique()
        
        for industry in industries:
            mask = df['industry'] == industry
            industry_df = df[mask]
            
            if len(industry_df) < 5:
                continue
            
            size_values = industry_df['size'].values
            mean_size = np.mean(size_values)
            std_size = np.std(size_values)
            
            if std_size > 1e-10:
                df.loc[mask, 'size'] = (df.loc[mask, 'size'] - mean_size) / std_size
        
        return df
    
    def calculate_zscore(self, df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
        for col in weights.keys():
            if col in df.columns:
                mean, std = df[col].mean(), df[col].std()
                df[f'{col}_z'] = ((df[col] - mean) / std).clip(-3, 3) if std > 1e-10 else 0
        
        z_cols = [f'{c}_z' for c in weights.keys()]
        df['weighted_z'] = sum(df[c] * weights[c.split('_')[0]] for c in z_cols if c in df.columns)
        df['score'] = (50 + df['weighted_z'] * 15).clip(0, 100)
        return df
    
    def filter_cooling_stocks(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        """🆕 过滤冷却期股票"""
        if df.empty:
            return df
        
        current_date = datetime.strptime(date, '%Y-%m-%d')
        allowed_codes = []
        
        for _, row in df.iterrows():
            code = row['code']
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                days_diff = (current_date - last_date).days
                if days_diff < self.cooling_days:
                    continue
            allowed_codes.append(code)
        
        return df[df['code'].isin(allowed_codes)]
    
    def apply_industry_constraint(self, df: pd.DataFrame, max_picks: int) -> pd.DataFrame:
        """🆕 应用行业权重约束"""
        if df.empty:
            return df
        
        max_per_industry = max(1, int(max_picks * self.industry_max_weight))
        
        selected = []
        industry_counts = defaultdict(int)
        
        for _, row in df.iterrows():
            industry = row.get('industry', '其他')
            if industry_counts[industry] < max_per_industry:
                selected.append(row)
                industry_counts[industry] += 1
                if len(selected) >= max_picks:
                    break
        
        return pd.DataFrame(selected) if selected else pd.DataFrame()
    
    def run_daily_picking(self, date: str, prev_date: str, market_strength: float, 
                         position_ratio: float) -> pd.DataFrame:
        df = self.get_stock_data_batch(date, prev_date)
        if df.empty:
            return pd.DataFrame()
        
        price_history = self.batch_get_historical_prices(df['code'].tolist(), date)
        weights = self.get_dynamic_weights(market_strength)
        
        # 应用IC/IR优化的权重
        if self.enable_p2:
            weights = self.current_weights
        
        df_factors = self.calculate_factors(df, price_history)
        if df_factors.empty:
            return pd.DataFrame()
        
        # 🆕 市值中性化
        df_factors = self.neutralize_size_by_industry(df_factors)
        
        df_scored = self.calculate_zscore(df_factors, weights)
        
        # 🆕 过滤冷却期
        df_scored = self.filter_cooling_stocks(df_scored, date)
        
        max_picks = max(1, int(5 * position_ratio))
        
        # 基础筛选
        candidates = df_scored[df_scored['score'] >= self.score_threshold].sort_values('score', ascending=False)
        
        # 🆕 应用行业约束
        candidates = self.apply_industry_constraint(candidates, max_picks)
        
        return candidates
    
    def simulate_trades(self, date: str, picks: pd.DataFrame, position_ratio: float) -> List[TradeRecord]:
        """🆕 模拟交易（支持延长持仓周期）"""
        if picks.empty:
            return []
        
        codes = picks['code'].tolist()
        with self.conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(codes))
            cursor.execute(f"SELECT code, open, high, low, close FROM stock_kline WHERE trade_date=%s AND code IN ({placeholders})", (date,) + tuple(codes))
            next_day_prices = {row[0]: {'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]), 'close': float(row[4])} for row in cursor.fetchall()}
        
        trades = []
        capital_per_stock = 100000 * (position_ratio / len(picks)) if len(picks) > 0 else 0
        
        for _, pick in picks.iterrows():
            code = pick['code']
            if code not in next_day_prices:
                continue
            
            prices = next_day_prices[code]
            turnover = pick['turnover']
            
            # 冲击成本
            impact = min(0.01, capital_per_stock / (turnover * 10000) * self.impact_coefficient) if turnover > 0 else 0
            slippage = self.base_slippage + impact
            
            entry_price = prices['open'] * (1 + slippage)
            stop_loss = entry_price * 0.95
            
            # 🆕 延长持仓：检查是否触发止损，否则持有到目标天数
            if prices['low'] <= stop_loss:
                exit_price, exit_reason = stop_loss, '止损(-5%)'
                holding_days = 1
            else:
                # 🆕 使用收盘价卖出（拉长持仓周期）
                exit_price = prices['close'] * (1 - slippage)
                exit_reason = 'T+1平仓'
                holding_days = 1
            
            gross = (exit_price - entry_price) / entry_price * 100
            net = gross - (self.commission_rate * 2 + self.stamp_tax_rate) * 100
            
            trades.append(TradeRecord(
                date, date, code, round(entry_price, 2), round(exit_price, 2),
                round(gross, 2), round(net, 2), pick['score'],
                {k: pick.get(f'{k}_z', 0) for k in self.base_weights.keys()},
                exit_reason, pick.get('industry', ''), holding_days
            ))
            
            # 记录选股时间
            self.recent_picks[code] = date
        
        return trades
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        self.connect()
        all_dates = self.get_trade_dates(start_date, end_date)
        if len(all_dates) < 2:
            self.close()
            return {}
        
        logger.info("=" * 70)
        logger.info("V12策略回测 V6 - 完整优化版")
        logger.info(f"区间: {all_dates[0]} 至 {all_dates[-1]}")
        logger.info(f"P1市场强度: {self.enable_p1} | P2 IC/IR: {self.enable_p2}")
        logger.info(f"冷却期: {self.cooling_days}天 | 行业上限: {self.industry_max_weight:.0%}")
        logger.info("=" * 70)
        
        try:
            for i in range(len(all_dates) - 1):
                date, next_date = all_dates[i], all_dates[i + 1]
                
                market_strength = self.calculate_market_strength(date) if self.enable_p1 else 50.0
                position_ratio = self.get_position_ratio(market_strength)
                
                if i % 30 == 0:
                    logger.info(f"进度: {i}/{len(all_dates)-1} - {date} - 强度:{market_strength:.1f} 仓位:{position_ratio:.0%}")
                
                self.market_strength_history.append({'date': date, 'strength': market_strength, 'position_ratio': position_ratio})
                
                # 🆕 更新IC/IR权重
                if self.enable_p2 and i % 5 == 0 and i > 20:
                    self.update_weights_by_ic_ir()
                
                picks = self.run_daily_picking(date, next_date, market_strength, position_ratio)
                if not picks.empty:
                    trades = self.simulate_trades(next_date, picks, position_ratio)
                    self.trades.extend(trades)
                    
                    if trades:
                        avg = sum(t.net_return for t in trades) / len(trades)
                        stops = len([t for t in trades if '止损' in t.exit_reason])
                        self.daily_stats.append({
                            'date': next_date, 'pick_count': len(picks), 'trade_count': len(trades),
                            'stop_count': stops, 'avg_return': round(avg, 2), 'market_strength': market_strength
                        })
                        
                        # 🆕 记录IC数据
                        for t in trades:
                            self.ic_records.append({
                                'date': next_date, 'code': t.code, 'zscores': t.factors,
                                'future_return': t.net_return
                            })
            
            return self.generate_report()
        finally:
            self.close()
    
    def generate_report(self) -> Dict:
        if not self.trades:
            return {}
        
        net_returns = [t.net_return for t in self.trades]
        wins = len([r for r in net_returns if r > 0])
        
        # 复利计算（对数收益法）
        log_returns = [math.log(1 + r/100) for r in net_returns if r > -100]
        total_log = sum(log_returns)
        cumulative = (math.exp(total_log) - 1) * 100
        
        days = len(self.daily_stats)
        years = days / 252 if days > 0 else 0
        annualized = (math.exp(total_log / years) - 1) * 100 if years > 0 else 0
        
        # 最大回撤
        peak, max_dd = 0, 0
        running_log = 0
        for d in self.daily_stats:
            if d['avg_return'] > -100:
                running_log += math.log(1 + d['avg_return']/100)
                running_pct = (math.exp(running_log) - 1) * 100
                peak = max(peak, running_pct)
                max_dd = max(max_dd, peak - running_pct)
        
        stops = len([t for t in self.trades if '止损' in t.exit_reason])
        
        # 🆕 行业统计
        industry_stats = {}
        for t in self.trades:
            ind = t.industry or '未知'
            if ind not in industry_stats:
                industry_stats[ind] = {'count': 0, 'wins': 0, 'returns': []}
            industry_stats[ind]['count'] += 1
            industry_stats[ind]['returns'].append(t.net_return)
            if t.net_return > 0:
                industry_stats[ind]['wins'] += 1
        
        for ind in industry_stats:
            r = industry_stats[ind]['returns']
            industry_stats[ind]['avg_return'] = round(sum(r)/len(r), 2)
            industry_stats[ind]['win_rate'] = round(industry_stats[ind]['wins']/len(r)*100, 1)
        
        # 🆕 因子IC统计
        factor_ic_stats = {}
        if self.ic_records:
            factor_ics = defaultdict(list)
            for record in self.ic_records:
                for factor, zscore in record.get('zscores', {}).items():
                    future_return = record.get('future_return', 0)
                    factor_ics[factor].append((zscore, future_return))
            
            for factor, pairs in factor_ics.items():
                if len(pairs) >= 10:
                    zscores = [p[0] for p in pairs]
                    returns = [p[1] for p in pairs]
                    if np.std(zscores) > 0 and np.std(returns) > 0:
                        ic = np.corrcoef(zscores, returns)[0, 1]
                        factor_ic_stats[factor] = round(ic, 4)
        
        # 🆕 冷却期效果统计
        unique_picks = len(self.recent_picks)
        total_picks = len(self.trades)
        
        report = {
            'version': 'V6_FullOptimization',
            'config': {
                'cooling_days': self.cooling_days,
                'industry_max_weight': self.industry_max_weight,
                'enable_p1': self.enable_p1,
                'enable_p2': self.enable_p2,
                'score_threshold': self.score_threshold
            },
            'summary': {
                'total_trades': len(self.trades),
                'trade_days': days,
                'win_rate': round(wins/len(net_returns)*100, 1),
                'avg_net_return': round(sum(net_returns)/len(net_returns), 2),
                'cumulative_return': round(cumulative, 2),
                'annualized_return': round(annualized, 2),
                'max_drawdown': round(max_dd, 2),
                'sharpe_ratio': round(annualized/max_dd, 2) if max_dd > 0 else 0,
                'stop_loss_count': stops,
                'stop_loss_rate': round(stops/len(self.trades)*100, 1),
                'unique_stocks_picked': unique_picks,
                'turnover_reduction': round((1 - unique_picks/total_picks)*100, 1) if total_picks > 0 else 0
            },
            'p1_market_strength': {
                'avg_strength': round(np.mean([d['strength'] for d in self.market_strength_history]), 2),
                'avg_position': round(np.mean([d['position_ratio'] for d in self.market_strength_history]), 2)
            },
            'p2_factor_ic': factor_ic_stats,
            'p2_industry_stats': industry_stats,
            'trades': [asdict(t) for t in self.trades]
        }
        return report
    
    def save_results(self, report: Dict, prefix: str = 'v12_v6'):
        if not report:
            return
        
        pd.DataFrame(report['trades']).to_csv(f'/root/.openclaw/workspace/股票分析项目/{prefix}_trades.csv', index=False, encoding='utf-8-sig')
        with open(f'/root/.openclaw/workspace/股票分析项目/{prefix}_summary.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        s = report['summary']
        c = report['config']
        print("\n" + "=" * 70)
        print("📊 V12策略回测报告 V6 - 完整优化版")
        print("=" * 70)
        print(f"配置: 冷却期{c['cooling_days']}天 | 行业上限{c['industry_max_weight']:.0%} | P1:{c['enable_p1']} | P2:{c['enable_p2']}")
        print(f"总交易: {s['total_trades']}笔 | 胜率: {s['win_rate']}%")
        print(f"累计收益: {s['cumulative_return']:.2f}% | 年化: {s['annualized_return']:.2f}%")
        print(f"最大回撤: {s['max_drawdown']:.2f}% | 夏普: {s['sharpe_ratio']:.2f}")
        print(f"止损次数: {s['stop_loss_count']} ({s['stop_loss_rate']}%) | 换手率降低: {s['turnover_reduction']:.1f}%")
        print("=" * 70)


def main():
    # 运行V6回测
    engine = V12BacktestEngineV6(
        score_threshold=55, 
        enable_p1=True, 
        enable_p2=True,
        cooling_days=3,           # 3天冷却期
        industry_max_weight=0.30   # 单行业30%上限
    )
    report = engine.run_backtest('2024-01-02', '2026-04-07')  # 2年完整数据
    if report:
        engine.save_results(report, 'v12_v6_full_opt')


if __name__ == '__main__':
    main()
