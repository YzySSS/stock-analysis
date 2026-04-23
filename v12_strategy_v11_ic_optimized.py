#!/usr/bin/env python3
"""
V12策略 V11_IC_Optimized - 基于IC分析的3因子优化版
========================================================
基于IC实证结果，3日持仓最优配置：
- Turnover (35%): 低换手率，IR最高(0.66)，最稳定
- LowVol (35%): 低波动，|IC|最高(0.145)
- Reversal (30%): 反转，IC=+0.082(显著)

剔除：Quality、Value、Momentum（IC不显著或与策略逻辑冲突）

逻辑：
Turnover+LowVol: 双重低波动筛选，找"机构控盘+低风险"股票
Reversal: 在低风险股票中，选超跌的，博3日反弹
3日持仓: 匹配IC最优周期，统计显著性最强
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
    gross_pnl: float
    total_cost: float
    net_pnl: float
    gross_return: float
    net_return: float
    
    # 退出原因
    exit_reason: str
    hold_days: int
    
    # 因子得分
    turnover_score: float
    lowvol_score: float
    reversal_score: float
    total_score: float


class V12StrategyV11ICOptimized:
    """V12策略 V11 - IC优化版 (3因子)"""
    
    def __init__(self,
                 score_threshold: float = 55.0,
                 max_positions: int = 5,
                 min_positions: int = 3,
                 hold_days: int = 3,  # 3日持仓，匹配IC最优周期
                 stop_loss: float = -0.08,
                 base_position: float = 0.80,
                 industry_max: float = 0.20,
                 cooling_days: int = 3,
                 min_turnover_amount: float = 50.0):  # 日成交额>5000万
        
        # 配置参数
        self.score_threshold = score_threshold
        self.max_positions = max_positions
        self.min_positions = min_positions
        self.hold_days = hold_days
        self.stop_loss = stop_loss
        self.base_position = base_position
        self.industry_max = industry_max
        self.cooling_days = cooling_days
        self.min_turnover_amount = min_turnover_amount
        
        # 因子权重 (基于IC分析)
        self.weights = {
            'turnover': 0.35,   # 低换手，IR最高
            'lowvol': 0.35,     # 低波动，|IC|最高
            'reversal': 0.30    # 反转，IC显著
        }
        
        # 成本模型
        self.commission_rate = 0.0003      # 万3佣金
        self.stamp_tax_rate = 0.0005       # 千0.5印花税(卖出)
        self.slippage_rate = 0.002         # 千2滑点
        
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """连接数据库"""
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def close_db(self):
        """关闭数据库"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def get_stock_list(self, date: str) -> List[Dict]:
        """获取股票列表"""
        sql = """
        SELECT DISTINCT code FROM stock_kline 
        WHERE trade_date = %s 
        AND turnover > 0
        AND amount >= %s
        """
        self.cursor.execute(sql, (date, self.min_turnover_amount * 10000))
        return [row['code'] for row in self.cursor.fetchall()]
    
    def get_stock_data(self, code: str, end_date: str, days: int = 30) -> pd.DataFrame:
        """获取股票历史数据"""
        sql = """
        SELECT * FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT %s
        """
        self.cursor.execute(sql, (code, end_date, days))
        rows = self.cursor.fetchall()
        if len(rows) < 20:  # 至少需要20天数据
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date')
        # 转换数值列为float
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turnover', 'pct_change']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    
    def calculate_turnover_factor(self, df: pd.DataFrame) -> float:
        """
        换手率因子 (Turnover)
        低换手率 = 机构控盘、筹码稳定
        IC最高且IR最稳定
        """
        if len(df) < 20:
            return 50.0
        
        # 使用20日平均换手率
        avg_turnover = df['turnover'].tail(20).mean()
        
        # 低换手得分：换手越低得分越高
        if avg_turnover <= 0 or pd.isna(avg_turnover):
            return 50.0
        
        # A股换手率通常在0.5%-30%之间
        # 换手率<2%为优秀(100分), >20%为差(0分)
        turnover_score = max(0, min(100, 100 - (avg_turnover - 2) * 5))
        
        return turnover_score
    
    def calculate_lowvol_factor(self, df: pd.DataFrame) -> float:
        """
        低波动因子 (LowVol)
        使用60日收益率标准差
        |IC|最高
        """
        if len(df) < 60:
            return 50.0
        
        # 计算60日收益率
        returns = df['close'].pct_change().dropna()
        if len(returns) < 20:
            return 50.0
        
        # 使用60日波动率
        volatility = returns.tail(60).std() * np.sqrt(252)  # 年化波动率
        
        # 波动率越低得分越高
        # 年化波动率20%为基准
        vol_score = max(0, min(100, 100 - volatility * 200))
        
        return vol_score
    
    def calculate_reversal_factor(self, df: pd.DataFrame) -> float:
        """
        反转因子 (Reversal)
        使用-20日收益率，超跌反弹
        IC=+0.082，显著
        """
        if len(df) < 25:
            return 50.0
        
        # 计算20日收益率
        price_now = df['close'].iloc[-1]
        price_20d_ago = df['close'].iloc[-21] if len(df) >= 21 else df['close'].iloc[0]
        
        if price_20d_ago <= 0:
            return 50.0
        
        ret_20d = (price_now - price_20d_ago) / price_20d_ago
        
        # 反转因子：跌得越多得分越高
        # 将收益率映射到得分：-30% -> 100分, +30% -> 0分
        reversal_score = max(0, min(100, 50 - ret_20d * 150))
        
        return reversal_score
    
    def calculate_factors(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算所有因子得分"""
        turnover_score = self.calculate_turnover_factor(df)
        lowvol_score = self.calculate_lowvol_factor(df)
        reversal_score = self.calculate_reversal_factor(df)
        
        # 加权总分
        total_score = (
            turnover_score * self.weights['turnover'] +
            lowvol_score * self.weights['lowvol'] +
            reversal_score * self.weights['reversal']
        )
        
        return {
            'turnover': turnover_score,
            'lowvol': lowvol_score,
            'reversal': reversal_score,
            'total': total_score
        }
    
    def select_stocks(self, date: str, cooling_list: List[str] = None) -> List[Dict]:
        """选股"""
        if cooling_list is None:
            cooling_list = []
            
        stock_codes = self.get_stock_list(date)
        logger.info(f"{date} 候选股票数: {len(stock_codes)}")
        
        candidates = []
        
        for code in stock_codes:
            # 排除冷却期股票
            if code in cooling_list:
                continue
                
            try:
                df = self.get_stock_data(code, date, days=70)
                if len(df) < 60:
                    continue
                
                # 获取最新数据
                latest = df.iloc[-1]
                
                # 计算因子
                factors = self.calculate_factors(df)
                
                # 达标才入选
                if factors['total'] >= self.score_threshold:
                    candidates.append({
                        'code': code,
                        'name': code,  # 简化处理
                        'industry': '未知',
                        'close': float(latest['close']),
                        'score': factors['total'],
                        'turnover_score': factors['turnover'],
                        'lowvol_score': factors['lowvol'],
                        'reversal_score': factors['reversal'],
                        'df': df
                    })
                    
            except Exception as e:
                continue
        
        # 按得分排序
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # 限制数量
        selected = candidates[:self.max_positions]
        
        logger.info(f"{date} 选中股票数: {len(selected)} (阈值: {self.score_threshold})")
        return selected


if __name__ == '__main__':
    strategy = V12StrategyV11ICOptimized()
    strategy.connect_db()
    
    # 测试选股
    test_date = '2025-03-10'
    stocks = strategy.select_stocks(test_date)
    
    print(f"\n=== V11_IC_Optimized 选股结果 ({test_date}) ===")
    for s in stocks[:5]:
        print(f"{s['code']}: 总分={s['score']:.1f}, "
              f"Turnover={s['turnover_score']:.1f}, "
              f"LowVol={s['lowvol_score']:.1f}, "
              f"Reversal={s['reversal_score']:.1f}")
    
    strategy.close_db()
