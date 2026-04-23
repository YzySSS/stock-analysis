#!/usr/bin/env python3
"""
V12_MarketAdaptive - 市场自适应多因子策略
=============================================
基于市场状态动态调整因子权重

市场状态定义:
- 牛市 (Bull): 沪深300在20日均线之上，且20日收益 > 5%
- 熊市 (Bear): 沪深300在20日均线之下，且20日收益 < -5%
- 震荡 (Oscillation): 其他情况

因子权重动态调整:
- 牛市: Turnover 40% + Reversal 30% + LowVol 30%
- 熊市: LowVol 50% + Quality 30% + Value 20%
- 震荡: Turnover 30% + LowVol 35% + Reversal 25% + Quality 10%
"""

import pandas as pd
import numpy as np
import pymysql
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}


class MarketStateClassifier:
    """市场状态分类器"""
    
    @staticmethod
    def get_market_state(conn, date_str):
        """
        获取指定日期的市场状态
        基于沪深300指数
        """
        # 获取沪深300最近40天数据
        cursor = conn.cursor()
        cursor.execute('''
            SELECT trade_date, close 
            FROM stock_kline 
            WHERE code = '000300' 
            AND trade_date <= %s
            ORDER BY trade_date DESC 
            LIMIT 40
        ''', (date_str,))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 20:
            return 'oscillation'  # 数据不足，默认震荡
        
        df = pd.DataFrame(rows, columns=['date', 'close'])
        df['close'] = pd.to_numeric(df['close'])
        
        current_price = df['close'].iloc[0]
        ma20 = df['close'].head(20).mean()
        
        # 计算20日收益率
        price_20d = df['close'].iloc[19] if len(df) >= 20 else df['close'].iloc[-1]
        ret_20d = (current_price - price_20d) / price_20d
        
        # 分类逻辑
        if current_price > ma20 and ret_20d > 0.05:
            return 'bull'
        elif current_price < ma20 and ret_20d < -0.05:
            return 'bear'
        else:
            return 'oscillation'


class V12MarketAdaptiveStrategy:
    """V12市场自适应策略"""
    
    # 因子权重配置
    FACTOR_WEIGHTS = {
        'bull': {
            'turnover': 0.40,
            'reversal': 0.30,
            'lowvol': 0.30,
            'quality': 0.0,
            'value': 0.0
        },
        'bear': {
            'turnover': 0.0,
            'reversal': 0.0,
            'lowvol': 0.50,
            'quality': 0.30,
            'value': 0.20
        },
        'oscillation': {
            'turnover': 0.30,
            'reversal': 0.25,
            'lowvol': 0.35,
            'quality': 0.10,
            'value': 0.0
        }
    }
    
    def __init__(self, score_threshold=45, max_positions=5):
        self.score_threshold = score_threshold
        self.max_positions = max_positions
        self.market_classifier = MarketStateClassifier()
    
    def calculate_factors(self, hist_df):
        """计算所有因子得分"""
        scores = {}
        
        # Turnover (低换手) - 20日平均换手率
        turnover_20d = hist_df['turnover'].tail(20).mean()
        scores['turnover'] = max(0, min(100, 100 - (turnover_20d - 2) * 5))
        
        # LowVol (低波动) - 60日波动率
        returns = hist_df['close'].pct_change().dropna()
        vol = returns.tail(60).std() * np.sqrt(252) if len(returns) >= 60 else returns.std() * np.sqrt(252)
        scores['lowvol'] = max(0, min(100, 100 - vol * 200))
        
        # Reversal (反转) - 20日收益率反向
        price_now = hist_df['close'].iloc[-1]
        price_20d = hist_df['close'].iloc[-21] if len(hist_df) >= 21 else hist_df['close'].iloc[0]
        ret_20d = (price_now - price_20d) / price_20d
        scores['reversal'] = max(0, min(100, 50 - ret_20d * 150))
        
        # Quality (质量) - ROE稳定性 (如果有数据)
        if 'roe' in hist_df.columns and not hist_df['roe'].isna().all():
            roe_mean = hist_df['roe'].tail(4).mean()
            scores['quality'] = max(0, min(100, roe_mean * 5)) if pd.notna(roe_mean) else 50
        else:
            scores['quality'] = 50  # 默认中值
        
        # Value (估值) - 低PE (如果有数据)
        if 'pe_ratio' in hist_df.columns and not hist_df['pe_ratio'].isna().all():
            pe = hist_df['pe_ratio'].iloc[-1]
            scores['value'] = max(0, min(100, 100 - pe)) if pd.notna(pe) and pe > 0 else 50
        else:
            scores['value'] = 50  # 默认中值
        
        return scores
    
    def calculate_weighted_score(self, factors, market_state):
        """根据市场状态计算加权得分"""
        weights = self.FACTOR_WEIGHTS.get(market_state, self.FACTOR_WEIGHTS['oscillation'])
        
        total_score = 0
        for factor, weight in weights.items():
            if weight > 0 and factor in factors:
                total_score += factors[factor] * weight
        
        return total_score, weights
    
    def select_stocks(self, df_all, date_str, conn):
        """
        选股主函数
        
        Args:
            df_all: 全市场数据DataFrame
            date_str: 选股日期
            conn: 数据库连接
        
        Returns:
            list: 选中的股票列表，每个元素包含详细信息
        """
        # 1. 判断市场状态
        market_state = self.market_classifier.get_market_state(conn, date_str)
        
        # 2. 选股
        selected = []
        
        for code in df_all['code'].unique():
            stock_df = df_all[df_all['code'] == code].sort_values('trade_date')
            
            # 获取历史数据（至少60天）
            hist = stock_df[stock_df['trade_date'] <= date_str].tail(70)
            if len(hist) < 20:
                continue
            
            # 计算因子
            factors = self.calculate_factors(hist)
            
            # 计算加权得分
            score, weights = self.calculate_weighted_score(factors, market_state)
            
            # 阈值过滤
            if score >= self.score_threshold:
                # 获取当前价格
                current_price = hist['close'].iloc[-1]
                
                selected.append({
                    'code': code,
                    'score': score,
                    'market_state': market_state,
                    'factors': factors,
                    'weights': weights,
                    'current_price': current_price,
                    'date': date_str
                })
        
        # 3. 按得分排序，取前N名
        selected.sort(key=lambda x: x['score'], reverse=True)
        return selected[:self.max_positions]


if __name__ == '__main__':
    # 测试
    print("V12_MarketAdaptive 策略加载完成")
    print("\n因子权重配置:")
    for state, weights in V12MarketAdaptiveStrategy.FACTOR_WEIGHTS.items():
        print(f"  {state}: {weights}")
