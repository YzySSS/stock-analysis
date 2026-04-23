#!/usr/bin/env python3
"""
V12策略 V10 - 重构版 (4因子核心)
===============================
基于DeepSeek重构建议，全新4因子体系：
- Quality (30%): ROE-TTM行业稳定性
- Valuation (30%): PE/PB分位数负向
- Reversal (25%): -20日收益率（反转）
- LowVol (15%): -60日波动率

风控体系：
- -8%硬止损
- 10天持仓周期
- 60%基准仓位（熊市30%）
- 行业权重上限15%
- 市值中性化
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
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
    """交易记录"""
    entry_date: str
    exit_date: str
    code: str
    name: str
    industry: str
    entry_price: float
    exit_price: float
    shares: float
    
    # 收益
    gross_pnl: float  # 毛收益
    total_cost: float  # 总成本
    net_pnl: float  # 净收益
    gross_return: float  # 毛收益率
    net_return: float  # 净收益率
    
    # 退出原因
    exit_reason: str  # 'stop_loss', 'time_exit', 'signal_exit'
    hold_days: int
    
    # 因子得分
    quality_score: float
    value_score: float
    reversal_score: float
    lowvol_score: float
    total_score: float


class V12StrategyV10:
    """V12策略 V10 - 重构版"""
    
    def __init__(self, 
                 score_threshold: float = 60.0,  # 提高阈值
                 max_positions: int = 8,
                 min_positions: int = 5,
                 hold_days: int = 10,  # 延长持仓
                 stop_loss: float = -0.08,  # -8%硬止损
                 base_position: float = 0.60,  # 60%基准仓位
                 bear_position: float = 0.30,  # 熊市30%
                 industry_max: float = 0.15,  # 行业上限15%
                 cooling_days: int = 5,
                 min_turnover: float = 100.0):  # 日成交>1亿
        
        # 配置参数
        self.score_threshold = score_threshold
        self.max_positions = max_positions
        self.min_positions = min_positions
        self.hold_days = hold_days
        self.stop_loss = stop_loss
        self.base_position = base_position
        self.bear_position = bear_position
        self.industry_max = industry_max
        self.cooling_days = cooling_days
        self.min_turnover = min_turnover
        
        # 4因子权重
        self.factor_weights = {
            'quality': 0.30,
            'value': 0.30,
            'reversal': 0.25,
            'lowvol': 0.15
        }
        
        # 成本模型
        self.commission_rate = 0.0003  # 万三
        self.min_commission = 5.0
        self.stamp_tax_rate = 0.0005  # 千0.5
        self.transfer_rate = 0.00001  # 十万分之一
        self.slippage_rate = 0.002  # 千2滑点
        
        # 状态
        self.conn = None
        self.cache = {}
        self.recent_picks = {}  # 冷却期记录
        
        logger.info(f"V10策略初始化完成:")
        logger.info(f"  阈值: {score_threshold}, 持仓: {hold_days}天, 止损: {stop_loss*100:.0f}%")
        logger.info(f"  仓位: 基准{base_position*100:.0f}%, 熊市{bear_position*100:.0f}%")
        logger.info(f"  因子权重: {self.factor_weights}")
    
    def connect(self):
        """连接数据库"""
        self.conn = pymysql.connect(**DB_CONFIG)
        return self.conn
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM stock_kline 
                WHERE trade_date BETWEEN %s AND %s 
                ORDER BY trade_date
            """, (start_date, end_date))
            return [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    
    def get_historical_prices(self, codes: List[str], end_date: str, days: int = 65) -> Dict[str, List[float]]:
        """批量获取历史价格"""
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
    
    def get_industry_avg(self, date: str) -> Dict[str, float]:
        """获取各行业平均值（用于行业中性化）"""
        industry_stats = defaultdict(list)
        
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT b.code, b.industry, k.turnover, b.roe_clean, b.pe_clean
                FROM stock_basic b
                JOIN stock_kline k ON b.code = k.code
                WHERE k.trade_date = %s AND b.is_st = 0 AND b.is_delisted = 0
            """, (date,))
            
            for row in cursor.fetchall():
                code, industry, turnover, roe, pe = row
                industry_stats[industry].append({
                    'turnover': float(turnover) if turnover else 0,
                    'roe': float(roe) if roe else None,
                    'pe': float(pe) if pe else None
                })
        
        industry_avgs = {}
        for industry, stocks in industry_stats.items():
            if len(stocks) >= 3:
                industry_avgs[industry] = {
                    'avg_turnover': np.mean([s['turnover'] for s in stocks]),
                    'avg_roe': np.mean([s['roe'] for s in stocks if s['roe'] is not None]),
                    'avg_pe': np.mean([s['pe'] for s in stocks if s['pe'] is not None])
                }
        
        return industry_avgs
    
    def get_stock_data(self, date: str) -> pd.DataFrame:
        """获取股票数据"""
        with self.conn.cursor() as cursor:
            # 获取前一交易日
            cursor.execute("""
                SELECT MAX(trade_date) FROM stock_kline 
                WHERE trade_date < %s
            """, (date,))
            prev_date = cursor.fetchone()[0]
            
            # 获取股票数据
            cursor.execute("""
                SELECT b.code, b.name, b.industry, b.roe_clean, b.pe_fixed,
                       k.open, k.close, k.turnover, k_prev.close as prev_close
                FROM stock_basic b
                JOIN stock_kline k ON b.code = k.code COLLATE utf8mb4_unicode_ci
                LEFT JOIN stock_kline k_prev ON b.code = k_prev.code COLLATE utf8mb4_unicode_ci AND k_prev.trade_date = %s
                WHERE k.trade_date = %s 
                  AND b.is_st = 0 AND b.is_delisted = 0
                  AND k.open BETWEEN 5 AND 150
                  AND k.turnover >= %s
            """, (prev_date, date, self.min_turnover / 100))  # 百万转亿
            
            data = []
            for row in cursor.fetchall():
                data.append({
                    'code': row[0],
                    'name': row[1] or '',
                    'industry': row[2] or '其他',
                    'roe': float(row[3]) if row[3] else None,
                    'pe': float(row[4]) if row[4] else None,
                    'open': float(row[5]) if row[5] else None,
                    'close': float(row[6]) if row[6] else None,
                    'turnover': float(row[7]) if row[7] else 0,
                    'prev_close': float(row[8]) if row[8] else None
                })
            
            return pd.DataFrame(data)
    
    def calculate_factors(self, df: pd.DataFrame, price_history: Dict[str, List[float]], 
                         date: str) -> pd.DataFrame:
        """计算4因子"""
        results = []
        
        for _, row in df.iterrows():
            code = row['code']
            prices = price_history.get(code, [])
            
            if len(prices) < 21:
                continue
            
            # 基础数据
            result = {
                'code': code,
                'name': row['name'],
                'industry': row['industry'],
                'price': row['open'],
                'turnover': row['turnover']
            }
            
            # Factor 1: Quality (ROE稳定性)
            # 使用原始ROE，后续做行业中性化
            if row['roe'] is not None:
                result['quality_raw'] = row['roe']
            else:
                # 无ROE数据时，用价格稳定性代替
                if len(prices) >= 20:
                    returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
                    result['quality_raw'] = 50 - np.std(returns) * 100
                else:
                    continue
            
            # Factor 2: Value (估值)
            # 使用修复后的pe_fixed字段
            pe_value = row.get('pe_fixed')
            if pe_value is not None and pe_value > 0:
                result['value_raw'] = -pe_value  # 负值，越低PE得分越高
            else:
                result['value_raw'] = -50  # 默认值
            
            # Factor 3: Reversal (反转)
            # 过去20日收益率的负值（跌得多得分高）
            if len(prices) >= 21:
                ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
                result['reversal_raw'] = -ret_20d  # 负值，跌得多得分高
            else:
                continue
            
            # Factor 4: LowVol (低波动)
            # 过去60日波动率的负值
            if len(prices) >= 60:
                returns_60d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-60, 0)]
                volatility = np.std(returns_60d) * 100
                result['lowvol_raw'] = -volatility  # 负值，波动小得分高
            elif len(prices) >= 20:
                returns_20d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
                volatility = np.std(returns_20d) * 100
                result['lowvol_raw'] = -volatility
            else:
                continue
            
            results.append(result)
        
        return pd.DataFrame(results)
    
    def industry_neutralize(self, df: pd.DataFrame) -> pd.DataFrame:
        """行业中性化处理"""
        if df.empty:
            return df
        
        industries = df['industry'].unique()
        
        for factor in ['quality_raw', 'value_raw', 'reversal_raw', 'lowvol_raw']:
            df[f'{factor}_z'] = 0.0
            
            for industry in industries:
                mask = df['industry'] == industry
                industry_data = df.loc[mask, factor]
                
                if len(industry_data) >= 3:
                    mean = industry_data.mean()
                    std = industry_data.std()
                    if std > 0:
                        df.loc[mask, f'{factor}_z'] = (industry_data - mean) / std
                    else:
                        df.loc[mask, f'{factor}_z'] = 0
        
        return df
    
    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算综合得分"""
        if df.empty:
            return df
        
        # 映射到0-100分
        for factor in ['quality', 'value', 'reversal', 'lowvol']:
            z_col = f'{factor}_raw_z'
            if z_col in df.columns:
                # Z-score映射到50±15分
                df[f'{factor}_score'] = 50 + df[z_col].clip(-3, 3) * 15
            else:
                df[f'{factor}_score'] = 50
        
        # 加权综合得分
        df['total_score'] = (
            df['quality_score'] * self.factor_weights['quality'] +
            df['value_score'] * self.factor_weights['value'] +
            df['reversal_score'] * self.factor_weights['reversal'] +
            df['lowvol_score'] * self.factor_weights['lowvol']
        )
        
        return df
    
    def select_stocks(self, df: pd.DataFrame, date: str) -> List[Dict]:
        """选股"""
        if df.empty:
            return []
        
        # 冷却期过滤
        valid_stocks = []
        for _, row in df.iterrows():
            code = row['code']
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                curr_date = datetime.strptime(date, '%Y-%m-%d')
                if (curr_date - last_date).days <= self.cooling_days:
                    continue
            valid_stocks.append(row)
        
        if not valid_stocks:
            return []
        
        df_valid = pd.DataFrame(valid_stocks)
        
        # 按得分排序
        df_valid = df_valid.sort_values('total_score', ascending=False)
        
        # 选取高分股票
        candidates = df_valid[df_valid['total_score'] >= self.score_threshold].head(30)
        
        if candidates.empty:
            return []
        
        # 行业权重约束
        selected = []
        industry_weights = defaultdict(float)
        
        for _, row in candidates.iterrows():
            industry = row['industry']
            if industry_weights[industry] < self.industry_max:
                selected.append({
                    'code': row['code'],
                    'name': row['name'],
                    'industry': row['industry'],
                    'price': row['price'],
                    'score': row['total_score'],
                    'quality_score': row['quality_score'],
                    'value_score': row['value_score'],
                    'reversal_score': row['reversal_score'],
                    'lowvol_score': row['lowvol_score']
                })
                industry_weights[industry] += 1.0 / self.max_positions
                
                if len(selected) >= self.max_positions:
                    break
        
        return selected
    
    def check_market_condition(self, date: str) -> str:
        """检查市场环境"""
        with self.conn.cursor() as cursor:
            # 获取沪深300的120日和200日均线
            cursor.execute("""
                SELECT close FROM stock_kline 
                WHERE code = '000300' AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 200
            """, (date,))
            
            prices = [float(row[0]) for row in cursor.fetchall()]
            
            if len(prices) < 120:
                return 'neutral'
            
            current = prices[0]
            ma120 = np.mean(prices[:120])
            ma200 = np.mean(prices[:200]) if len(prices) >= 200 else ma120
            
            if current < ma200:
                return 'bear'
            elif current < ma120:
                return 'weak'
            else:
                return 'bull'
    
    def calculate_position_ratio(self, market_condition: str) -> float:
        """计算仓位比例"""
        if market_condition == 'bear':
            return self.bear_position
        elif market_condition == 'weak':
            return (self.base_position + self.bear_position) / 2
        else:
            return self.base_position
    
    def calculate_costs(self, amount: float, is_buy: bool = True) -> Dict:
        """计算交易成本"""
        if is_buy:
            commission = max(amount * self.commission_rate, self.min_commission)
            slippage = amount * self.slippage_rate
            transfer = amount * self.transfer_rate
            total_cost = commission + slippage + transfer
            return {
                'commission': commission,
                'slippage': slippage,
                'transfer': transfer,
                'total': total_cost,
                'net_amount': amount + total_cost
            }
        else:
            commission = max(amount * self.commission_rate, self.min_commission)
            stamp_tax = amount * self.stamp_tax_rate
            slippage = amount * self.slippage_rate
            transfer = amount * self.transfer_rate
            total_cost = commission + stamp_tax + slippage + transfer
            return {
                'commission': commission,
                'stamp_tax': stamp_tax,
                'slippage': slippage,
                'transfer': transfer,
                'total': total_cost,
                'net_amount': amount - total_cost
            }


if __name__ == '__main__':
    # 测试
    strategy = V12StrategyV10()
    print("V10策略初始化成功!")
    print(f"因子权重: {strategy.factor_weights}")
    print(f"持仓周期: {strategy.hold_days}天")
    print(f"止损线: {strategy.stop_loss*100:.0f}%")
