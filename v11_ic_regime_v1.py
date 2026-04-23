#!/usr/bin/env python3
"""
V11_IC_Optimized 第一阶段改进 - 市场环境判断 (Regime Switching)
================================================================
改进点：
1. 根据市场状态（趋势市/震荡市）切换策略参数
2. 震荡市降低仓位、收紧止损、提高选股门槛
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class MarketRegimeDetector:
    """市场环境检测器"""
    
    @staticmethod
    def get_index_ma(index_code: str, date: str, ma_days: int) -> Optional[float]:
        """获取指数MA值"""
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        sql = """
        SELECT AVG(close) FROM (
            SELECT close FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
        ) t
        """
        cursor.execute(sql, (index_code, date, ma_days))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return float(row[0]) if row and row[0] else None
    
    @staticmethod
    def is_bull_market(date: str) -> bool:
        """
        判断是否为趋势市（牛市）
        条件：上证指数 > MA200 且 MA20 > MA60
        """
        index_code = '000001'  # 上证指数
        
        # 获取当日收盘价
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT close FROM stock_kline WHERE code = %s AND trade_date = %s",
            (index_code, date)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row or not row[0]:
            return False
        
        current_price = float(row[0])
        
        # 获取各周期MA
        ma200 = MarketRegimeDetector.get_index_ma(index_code, date, 200)
        ma20 = MarketRegimeDetector.get_index_ma(index_code, date, 20)
        ma60 = MarketRegimeDetector.get_index_ma(index_code, date, 60)
        
        if ma200 is None or ma20 is None or ma60 is None:
            return False
        
        # 判断条件
        is_bull = (current_price > ma200) and (ma20 > ma60)
        
        logger.info(f"[{date}] 市场状态: {'牛市' if is_bull else '震荡/熊市'} | "
                   f"上证: {current_price:.2f} | MA200: {ma200:.2f} | MA20/MA60: {ma20:.2f}/{ma60:.2f}")
        
        return is_bull


class V11StrategyRegimeV1:
    """V11策略 - 市场环境判断版（第一阶段）"""
    
    def __init__(self):
        # 基础因子权重（不变）
        self.weights = {
            'turnover': 0.35,
            'lowvol': 0.35,
            'reversal': 0.30
        }
        
        # 趋势市参数（原版V11）
        self.bull_params = {
            'score_threshold': 55,
            'max_positions': 5,
            'stop_loss': -0.08,
            'hold_days': 3,
            'cooling_days': 3
        }
        
        # 震荡市参数（降低风险）
        self.bear_params = {
            'score_threshold': 60,  # 提高门槛
            'max_positions': 2,     # 减少持仓
            'stop_loss': -0.05,     # 收紧止损
            'hold_days': 3,
            'cooling_days': 5       # 延长冷却
        }
        
        self.min_turnover_amount = 50  # 5000万
        
        # 成本模型
        self.commission_rate = 0.0003
        self.stamp_tax_rate = 0.0005
        self.slippage_rate = 0.002
    
    def get_params(self, date: str) -> Dict:
        """根据市场状态获取参数"""
        is_bull = MarketRegimeDetector.is_bull_market(date)
        
        if is_bull:
            logger.info(f"[{date}] 使用趋势市参数")
            return self.bull_params
        else:
            logger.info(f"[{date}] 使用震荡市参数")
            return self.bear_params
    
    def select_stocks(self, date: str) -> List[Dict]:
        """选股主函数"""
        params = self.get_params(date)
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 获取候选股票
        sql = """
        SELECT k.code, k.close as price, k.turnover, k.amount,
               b.name, b.industry
        FROM stock_kline k
        LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
        WHERE k.trade_date = %s
        AND k.amount >= %s
        AND b.is_delisted = 0 AND b.is_st = 0
        AND k.close BETWEEN 5 AND 200
        """
        cursor.execute(sql, (date, self.min_turnover_amount * 10000))
        stocks = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not stocks:
            return []
        
        # 计算因子得分
        results = []
        for stock in stocks:
            # 获取历史数据
            factors = self.calculate_factors(stock['code'], date)
            if factors:
                total_score = (
                    factors['turnover_score'] * self.weights['turnover'] +
                    factors['lowvol_score'] * self.weights['lowvol'] +
                    factors['reversal_score'] * self.weights['reversal']
                )
                
                if total_score >= params['score_threshold']:
                    results.append({
                        'code': stock['code'],
                        'name': stock['name'],
                        'price': float(stock['price']),
                        'score': total_score,
                        **factors
                    })
        
        # 排序并返回前N个
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:params['max_positions']]
    
    def calculate_factors(self, code: str, date: str) -> Optional[Dict]:
        """计算个股因子"""
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        sql = """
        SELECT close, turnover FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT 25
        """
        cursor.execute(sql, (code, date))
        history = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if len(history) < 20:
            return None
        
        closes = [float(row['close']) for row in history if row['close']]
        turnovers = [float(row['turnover']) for row in history if row['turnover']]
        
        if len(closes) < 20 or len(turnovers) < 20:
            return None
        
        # Turnover得分：当日换手率排名（全市场）
        turnover_score = 100  # 已在SQL中筛选，这里简化
        
        # LowVol得分：20日波动率倒数排名
        vol_20 = np.std(closes[:20])
        
        # Reversal得分：20日收益倒数排名（超跌得分高）
        ret_20 = (closes[0] - closes[19]) / closes[19] if closes[19] > 0 else 0
        
        return {
            'turnover_score': turnover_score,
            'vol_20': vol_20,
            'ret_20': ret_20,
            'lowvol_score': 100,  # 简化，实际需要全市场排名
            'reversal_score': 100 if ret_20 < 0 else 50  # 简化
        }


def quick_test():
    """快速测试"""
    logger.info("=" * 70)
    logger.info("V11_IC_Regime_V1 快速测试")
    logger.info("=" * 70)
    
    # 测试市场环境判断
    test_dates = ['2024-02-01', '2024-06-01', '2025-03-01', '2025-09-01']
    
    for date in test_dates:
        is_bull = MarketRegimeDetector.is_bull_market(date)
        logger.info(f"{date}: {'牛市' if is_bull else '震荡/熊市'}")
    
    logger.info("=" * 70)


if __name__ == '__main__':
    quick_test()
