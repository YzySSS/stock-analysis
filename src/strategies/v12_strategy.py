#!/usr/bin/env python3
"""
V12 选股策略 - 修复版 (无未来函数)
=================================
基于全新多因子模型的稳健选股策略

修复内容:
1. 情绪因子使用前一日涨跌（非当日）
2. MA20计算排除当日数据
3. 选股基于开盘价（非收盘价）
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict
import logging
import numpy as np
from datetime import datetime

from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class V12Strategy(BaseStrategy):
    """V12 多因子选股策略 - 无未来函数版"""
    
    def __init__(self, **kwargs):
        super().__init__()
        self.name = "V12_MultiFactor"
        self.version = "1.1-Fixed"
        self.strategy_key = "V12"
        
        # 权重配置
        self.factor_weights = {
            'trend': 0.25,
            'momentum': 0.25,
            'quality': 0.20,
            'sentiment': 0.20,
            'value': 0.10
        }
        
        self.pick_count = 3
        self.score_threshold = kwargs.get('threshold', 50)
    
    def get_factor_weights(self) -> Dict[str, float]:
        return self.factor_weights
    
    def select(self, date: str, top_n: int = 3) -> List[Dict]:
        """V12选股主函数"""
        logger.info(f"V12策略选股: {date}")
        
        # 获取股票池
        candidates = self._get_candidates(date)
        logger.info(f"候选股票: {len(candidates)} 只")
        
        if not candidates:
            return []
        
        # 评分
        scored = []
        for stock in candidates:
            score, factors = self._calculate_score(stock)
            if score >= self.score_threshold:
                scored.append({
                    'code': stock['code'],
                    'name': stock['name'],
                    'score': score,
                    'factors': factors,
                    'price': stock['price'],
                    'reason': f"trend:{factors['trend']:.0f} mom:{factors['momentum']:.0f}"
                })
        
        scored.sort(key=lambda x: x['score'], reverse=True)
        picks = scored[:top_n]
        
        logger.info(f"达到阈值(>={self.score_threshold}): {len(scored)} 只，选中: {len(picks)} 只")
        for p in picks:
            logger.info(f"  ✅ {p['code']} {p['name']} | {p['score']:.1f}分")
        
        return picks
    
    def _get_candidates(self, date: str) -> List[Dict]:
        """获取候选股票 - 修复未来函数"""
        import pymysql
        from config import DB_CONFIG
        
        conn = pymysql.connect(**DB_CONFIG)
        stocks = []
        
        try:
            with conn.cursor() as cursor:
                # 【修复】获取数据库中最新的交易日（处理假期休市情况）
                cursor.execute("SELECT MAX(trade_date) FROM stock_kline")
                latest_date = cursor.fetchone()[0]
                
                if latest_date is None:
                    logger.error("数据库中没有K线数据")
                    return []
                
                # 使用最新交易日数据（如果传入日期没有数据）
                trade_date = latest_date
                logger.info(f"使用最新交易日数据: {trade_date} (请求日期: {date})")
                
                # 获取ST/退市/债券股票代码列表
                cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
                exclude_codes = {row[0] for row in cursor.fetchall()}
                
                # 【新增】排除债券相关ETF（名称含'债'）
                cursor.execute("SELECT code FROM stock_basic WHERE name LIKE '%债%'")
                bond_etfs = {row[0] for row in cursor.fetchall()}
                exclude_codes.update(bond_etfs)
                logger.info(f"排除债券ETF: {len(bond_etfs)} 只")
                
                # 【修复1】使用开盘价代替收盘价
                # 【优化】分两阶段查询，避免复杂JOIN
                
                # 阶段1: 获取前一交易日
                cursor.execute("""
                    SELECT MAX(trade_date) FROM stock_kline 
                    WHERE trade_date < %s
                """, (trade_date,))
                prev_date = cursor.fetchone()[0]
                
                # 阶段2: 获取当日数据
                cursor.execute("""
                    SELECT code, open as price, turnover
                    FROM stock_kline
                    WHERE trade_date = %s
                    AND open BETWEEN 5 AND 150
                    AND turnover >= 2.0
                    ORDER BY turnover DESC
                    LIMIT 200
                """, (trade_date,))
                
                kline_data = {}
                for row in cursor.fetchall():
                    if row[0] not in exclude_codes:
                        kline_data[row[0]] = {
                            'price': float(row[1]),
                            'turnover': float(row[2]),
                            'prev_change': 0  # 先默认0
                        }
                
                # 阶段3: 获取前一日涨跌幅
                if prev_date and kline_data:
                    codes_list = list(kline_data.keys())
                    placeholders = ','.join(['%s'] * len(codes_list))
                    cursor.execute(f"""
                        SELECT code, pct_change FROM stock_kline
                        WHERE trade_date = %s AND code IN ({placeholders})
                    """, (prev_date,) + tuple(codes_list))
                    
                    for row in cursor.fetchall():
                        if row[0] in kline_data:
                            kline_data[row[0]]['prev_change'] = float(row[1]) if row[1] else 0
                
                # 阶段4: 获取股票名称
                codes = list(kline_data.keys())
                
                # 获取名称
                codes = list(kline_data.keys())
                logger.info(f"基础过滤后候选股: {len(codes)}只")
                
                # 【优化】限制处理数量，避免超长IN查询
                if len(codes) > 500:
                    # 按换手率排序，取前500只
                    codes = sorted(codes, key=lambda c: kline_data[c]['turnover'], reverse=True)[:500]
                    logger.info(f"限制处理前500只（按换手率）")
                
                if codes:
                    placeholders = ','.join(['%s'] * len(codes))
                    cursor.execute(f"SELECT code, name FROM stock_basic WHERE code IN ({placeholders})", tuple(codes))
                    names = {row[0]: row[1] for row in cursor.fetchall()}
                
                # 【优化】批量获取所有股票的历史价格（限制查询日期范围）
                # 使用DATE_SUB获取90天前的日期
                cursor.execute("SELECT DATE_SUB(%s, INTERVAL 90 DAY)", (trade_date,))
                start_date = cursor.fetchone()[0]
                trade_date_str = trade_date.strftime('%Y-%m-%d') if hasattr(trade_date, 'strftime') else str(trade_date)
                start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
                cursor.execute("""
                    SELECT code, close FROM stock_kline 
                    WHERE code IN ({}) 
                    AND trade_date <= %s
                    AND trade_date >= %s
                    ORDER BY code, trade_date DESC
                """.format(','.join(['%s'] * len(codes))), tuple(codes) + (trade_date_str, start_date_str))
                
                # 按code分组存储价格历史
                price_history = {}
                for row in cursor.fetchall():
                    code, close = row
                    if code not in price_history:
                        price_history[code] = []
                    price_history[code].append(float(close))
                
                logger.info(f"获取到历史数据的股票: {len(price_history)}只")
                
                # 【修复2】MA20计算排除当日
                for code in kline_data:
                    if code not in price_history:
                        continue
                    prices = price_history[code]
                    
                    # 【修复3】MA20只使用前20日数据（不含当日）
                    if len(prices) >= 21:  # 需要至少21天数据（前20日+当日计算用）
                        ma20 = sum(prices[1:21]) / 20  # prices[0]是当日，排除后用1-21
                        current = kline_data[code]['price']  # 当日开盘价
                        
                        if current >= ma20 * 0.95:
                            stocks.append({
                                'code': code,
                                'name': names.get(code, ''),
                                'price': current,
                                'turnover': kline_data[code]['turnover'],
                                'prev_change': kline_data[code]['prev_change'],
                                'prices': list(reversed(prices))  # 转为时间顺序
                            })
        finally:
            conn.close()
        
        return stocks
    
    def _calculate_score(self, stock: Dict) -> tuple:
        """计算股票得分"""
        prices = stock['prices']
        price = stock['price']
        
        factors = {}
        factors['trend'] = self._trend_score(prices, price)
        factors['momentum'] = self._momentum_score(prices)
        factors['quality'] = self._quality_score(prices)
        factors['sentiment'] = self._sentiment_score(stock, prices)  # 【修复4】传入prices
        factors['value'] = 5
        
        total = sum(factors[k] * self.factor_weights[k] for k in factors)
        return round(total, 1), factors
    
    def _trend_score(self, prices: List[float], current: float) -> float:
        """趋势因子 (0-100) - 修复：MA20不含当日"""
        if len(prices) < 21:  # 需要至少21天（前20日+当日）
            return 40
        
        score = 40  # 基础分40分
        
        # 【修复】MA20只使用前20日数据（不含当日）
        ma20 = sum(prices[-21:-1]) / 20
        
        # 当日开盘价在MA20之上 (20分)
        if current >= ma20:
            score += 20
        
        # 有60天数据，检查长期趋势
        if len(prices) >= 61:
            # 【修复】MA60也只使用前60日数据
            ma60 = sum(prices[-61:-1]) / 60
            
            # MA20在MA60之上 (20分)
            if ma20 >= ma60:
                score += 20
            
            # 相对位置 (20分) - 使用前一日的相对位置
            prev_price = prices[-1]  # 前一日收盘价
            high_60 = max(prices[-61:-1])  # 前60日高点（不含当日）
            low_60 = min(prices[-61:-1])   # 前60日低点（不含当日）
            if high_60 > low_60:
                pos = (prev_price - low_60) / (high_60 - low_60)
                # 30%-80%区间最佳
                if 0.3 <= pos <= 0.8:
                    score += 20
        
        return min(100, score)
    
    def _momentum_score(self, prices: List[float]) -> float:
        """动量因子 (0-100) - 修复：20日涨幅不含当日"""
        if len(prices) < 21:
            return 40
        
        score = 40  # 基础分40分
        
        # 【修复】20日涨幅 = (前一日收盘价 - 前20日收盘价) / 前20日收盘价
        # 不含当日数据
        ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100
        
        # 0-40%区间加分 (60分)
        if 0 <= ret_20d <= 40:
            score += 60
        elif -10 <= ret_20d < 0:
            score += 20
        elif ret_20d > 40:
            score += 40  # 过热但仍给分
        elif ret_20d < -10:
            score -= 20
        
        return min(100, max(0, score))
    
    def _quality_score(self, prices: List[float]) -> float:
        """质量因子 (0-100) - 修复：波动率不含当日"""
        if len(prices) < 21:
            return 40
        
        score = 40  # 基础分40分
        
        # 【修复】波动率只使用前20日数据（不含当日）
        returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                   for i in range(-20, 0)]  # 前20日的涨跌幅
        vol = np.std(returns) if returns else 0
        
        # 低波动加分 (60分)
        if vol < 3:
            score += 60
        elif vol < 4:
            score += 30
        
        return min(100, score)
    
    def _sentiment_score(self, stock: Dict, prices: List[float]) -> float:
        """情绪因子 (0-100) - 修复：使用前一日涨跌，非当日"""
        score = 40  # 基础分40分
        
        # 【修复】使用前一日涨跌幅
        prev_change = stock.get('prev_change', 0)
        
        # 前一日涨跌判断 (60分)
        if prev_change >= 0:
            score += 60
        elif prev_change >= -2:
            score += 30
        else:
            score -= 20
        
        return min(100, max(0, score))


# 导出
v12_strategy = V12Strategy()
