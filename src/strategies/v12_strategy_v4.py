#!/usr/bin/env python3
"""
V12 选股策略 - 接入清洗后估值因子 (V4版本)
==========================================
更新内容:
1. 使用清洗后的 pe_clean, roe_clean 替代原始值
2. 估值因子使用行业中性化后的值
3. 保持Z-score标准化
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


class V12StrategyV4(BaseStrategy):
    """V12 多因子选股策略 - 接入清洗后估值因子"""
    
    def __init__(self, **kwargs):
        super().__init__()
        self.name = "V12_MultiFactor_V4"
        self.version = "1.4-ValuationEnhanced"
        self.strategy_key = "V12_V4"
        
        # 权重配置（接入估值因子）
        self.factor_weights = {
            'trend': 0.20,        # 趋势因子
            'momentum': 0.15,     # 动量因子
            'quality': 0.20,      # 质量因子（接入roe_clean）
            'sentiment': 0.15,    # 情绪因子
            'valuation': 0.20,    # 估值因子（接入pe_clean）
            'liquidity': 0.10     # 流动性因子
        }
        
        self.pick_count = kwargs.get('pick_count', 5)
        self.score_threshold = kwargs.get('threshold', 55)
        
        logger.info(f"V12 V4策略初始化 | 阈值:{self.score_threshold} | 最大选股:{self.pick_count}")
    
    def get_factor_weights(self) -> Dict[str, float]:
        return self.factor_weights
    
    def select(self, date: str, top_n: int = None) -> List[Dict]:
        """V12选股主函数 - 接入清洗后估值因子"""
        if top_n is None:
            top_n = self.pick_count
            
        logger.info(f"V12 V4策略选股: {date} | 阈值:{self.score_threshold}")
        
        # 获取股票池（包含清洗后的基本面数据）
        candidates = self._get_candidates(date)
        logger.info(f"候选股票: {len(candidates)} 只")
        
        if not candidates:
            return []
        
        # 使用Z-score评分
        picks = self._select_with_zscore(candidates, top_n)
        
        logger.info(f"选中: {len(picks)} 只")
        for p in picks[:3]:
            logger.info(f"  ✅ {p['code']} {p.get('name', '')} | {p['score']:.1f}分")
        
        return picks
    
    def _get_candidates(self, date: str) -> List[Dict]:
        """获取候选股票 - 接入清洗后估值因子"""
        import pymysql
        from config import DB_CONFIG
        
        conn = pymysql.connect(**DB_CONFIG)
        stocks = []
        
        try:
            with conn.cursor() as cursor:
                # 获取最新交易日
                cursor.execute("SELECT MAX(trade_date) FROM stock_kline")
                latest_date = cursor.fetchone()[0]
                
                if latest_date is None:
                    logger.error("数据库中没有K线数据")
                    return []
                
                trade_date = latest_date
                logger.info(f"使用最新交易日数据: {trade_date}")
                
                # 获取ST/退市列表
                cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
                exclude_codes = {row[0] for row in cursor.fetchall()}
                
                # 获取前一日日期
                cursor.execute("""
                    SELECT MAX(trade_date) FROM stock_kline 
                    WHERE trade_date < %s
                """, (trade_date,))
                prev_date = cursor.fetchone()[0]
                
                # 【关键更新】获取当日数据 + 清洗后的基本面数据
                cursor.execute("""
                    SELECT 
                        k.code, 
                        k.open as price, 
                        k.turnover,
                        k_prev.pct_change,
                        b.pe_clean,      -- 使用清洗后的PE
                        b.pb_clean,      -- 使用清洗后的PB
                        b.roe_clean,     -- 使用清洗后的ROE
                        b.pe_score,      -- PE得分（负值，越高越好）
                        b.roe_score,     -- ROE得分
                        b.name
                    FROM stock_kline k
                    LEFT JOIN stock_kline k_prev ON k.code = k_prev.code 
                        AND k_prev.trade_date = %s
                    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
                    WHERE k.trade_date = %s
                    AND k.open BETWEEN 5 AND 150
                    AND k.turnover >= 0.5
                """, (prev_date, trade_date))
                
                kline_data = {}
                for row in cursor.fetchall():
                    code = row[0]
                    if code in exclude_codes:
                        continue
                    
                    # 清洗后的基本面数据
                    pe_clean = row[4]
                    pb_clean = row[5]
                    roe_clean = row[6]
                    pe_score = row[7]
                    roe_score = row[8]
                    
                    # 过滤极端估值（清洗后的值应该已经处理过，但再保险一次）
                    if pe_clean is not None and (pe_clean < -1000 or pe_clean > 1000):
                        continue
                    
                    kline_data[code] = {
                        'code': code,
                        'price': float(row[1]),
                        'turnover': float(row[2]) if row[2] else 0,
                        'prev_change': float(row[3]) if row[3] else 0,
                        'pe_clean': float(pe_clean) if pe_clean is not None else None,
                        'pb_clean': float(pb_clean) if pb_clean is not None else None,
                        'roe_clean': float(roe_clean) if roe_clean is not None else None,
                        'pe_score': float(pe_score) if pe_score is not None else None,
                        'roe_score': float(roe_score) if roe_score is not None else None,
                        'name': row[9] or ''
                    }
                
                logger.info(f"基础过滤后候选股: {len(kline_data)}只")
                
                # 限制处理数量
                codes = list(kline_data.keys())
                if len(codes) > 500:
                    codes = sorted(codes, key=lambda c: kline_data[c]['turnover'], reverse=True)[:500]
                    logger.info(f"限制处理前500只（按换手率）")
                
                if not codes:
                    return []
                
                # 获取历史价格
                placeholders = ','.join(['%s'] * len(codes))
                cursor.execute(f"""
                    SELECT code, close FROM stock_kline 
                    WHERE code IN ({placeholders})
                    AND trade_date <= %s
                    ORDER BY code, trade_date DESC
                """, tuple(codes) + (trade_date,))
                
                price_history = {}
                for row in cursor.fetchall():
                    code, close = row
                    if code not in price_history:
                        price_history[code] = []
                    price_history[code].append(float(close))
                
                logger.info(f"获取到历史数据的股票: {len(price_history)}只")
                
                # MA20过滤
                for code in codes:
                    if code not in price_history:
                        continue
                    prices = price_history[code]
                    
                    if len(prices) >= 21:
                        ma20 = sum(prices[1:21]) / 20
                        current = kline_data[code]['price']
                        
                        if current >= ma20 * 0.90:
                            stocks.append({
                                **kline_data[code],
                                'prices': list(reversed(prices))
                            })
        finally:
            conn.close()
        
        return stocks
    
    def _select_with_zscore(self, candidates: List[Dict], top_n: int) -> List[Dict]:
        """使用Z-score标准化选股"""
        if len(candidates) < 10:
            return []
        
        # 计算原始因子
        raw_factors = []
        for stock in candidates:
            factors = self._calculate_raw_factors(stock)
            raw_factors.append((stock['code'], factors, stock))
        
        # Z-score标准化
        zscores = self._calculate_zscore(raw_factors)
        
        # 计算最终得分
        picks = []
        for code, _, stock in raw_factors:
            if code not in zscores:
                continue
            
            # 加权Z-score
            weighted_zscore = sum(
                zscores[code].get(k, 0) * self.factor_weights[k]
                for k in self.factor_weights
            )
            
            # 映射到百分制
            score = 50 + weighted_zscore * 15
            score = np.clip(score, 0, 100)
            
            if score >= self.score_threshold:
                picks.append({
                    'code': code,
                    'name': stock.get('name', ''),
                    'score': round(score, 1),
                    'price': stock['price'],
                    'zscores': zscores[code],
                    'raw_factors': {k: stock.get(k) for k in ['pe_clean', 'roe_clean', 'turnover']}
                })
        
        # 排序并限制数量
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:top_n]
    
    def _calculate_raw_factors(self, stock: Dict) -> Dict[str, float]:
        """计算原始因子值（用于Z-score）"""
        prices = stock['prices']
        
        factors = {}
        
        # 1. 趋势因子 - MA20斜率（年化）
        if len(prices) >= 25:
            ma20_now = sum(prices[-20:]) / 20
            ma20_prev = sum(prices[-25:-5]) / 20
            factors['trend'] = (ma20_now - ma20_prev) / ma20_prev * 252 if ma20_prev > 0 else 0
        else:
            factors['trend'] = 0
        
        # 2. 动量因子 - 20日涨幅
        if len(prices) >= 21:
            factors['momentum'] = (prices[-1] - prices[-21]) / prices[-21] * 100
        else:
            factors['momentum'] = 0
        
        # 3. 质量因子 - 接入清洗后的ROE
        roe_clean = stock.get('roe_clean', 0)
        if roe_clean is not None and not np.isnan(roe_clean):
            # ROE质量分（清洗后的值已经是行业内相对值）
            factors['quality'] = roe_clean
        else:
            # 无ROE数据时使用波动率替代
            if len(prices) >= 21:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                          for i in range(-20, 0)]
                volatility = np.std(returns) if returns else 10
                factors['quality'] = 20 - volatility  # 低波动=高质量
            else:
                factors['quality'] = 0
        
        # 4. 情绪因子
        factors['sentiment'] = stock.get('prev_change', 0)
        
        # 5. 估值因子 - 接入清洗后的PE
        pe_score = stock.get('pe_score')
        if pe_score is not None and not np.isnan(pe_score):
            # 使用清洗后的PE得分（负值，越高表示估值越低越好）
            factors['valuation'] = pe_score
        else:
            # 无PE数据时给个中等值
            factors['valuation'] = -25  # 中等估值
        
        # 6. 流动性因子
        turnover = stock.get('turnover', 0)
        factors['liquidity'] = np.log(turnover + 1) if turnover > 0 else 0
        
        return factors
    
    def _calculate_zscore(self, raw_factors: List[tuple]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化"""
        if not raw_factors:
            return {}
        
        factor_names = list(self.factor_weights.keys())
        
        # 收集每个因子的值
        factor_values = {f: [] for f in factor_names}
        for code, factors, _ in raw_factors:
            for f in factor_names:
                factor_values[f].append((code, factors.get(f, 0)))
        
        # 计算Z-score
        zscores = {}
        for factor_name in factor_names:
            values = [v for _, v in factor_values[factor_name] if not np.isnan(v)]
            
            if len(values) < 2:
                continue
            
            mean = np.mean(values)
            std = np.std(values)
            
            if std < 1e-10:
                std = 1e-10
            
            for code, value in factor_values[factor_name]:
                if np.isnan(value):
                    zscores.setdefault(code, {})[factor_name] = 0
                else:
                    zscore = (value - mean) / std
                    zscores.setdefault(code, {})[factor_name] = np.clip(zscore, -3, 3)
        
        return zscores


# 导出
v12_strategy_v4 = V12StrategyV4()
