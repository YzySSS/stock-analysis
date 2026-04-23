#!/usr/bin/env python3
"""
V12 选股策略 V7 - 简化版（防过拟合）
========================================
基于DeepSeek建议的简化方案：
1. 简化至3核心因子：sentiment + quality + valuation
2. 固定参数：冷却期5天、阈值50分（不调优）
3. 新增风控模块
4. 新增市场环境过滤
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Optional
import logging
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class V12StrategyV7(BaseStrategy):
    """V12 多因子选股策略 V7 - 简化版（防过拟合）"""
    
    def __init__(self, **kwargs):
        super().__init__()
        self.name = "V12_MultiFactor_V7_Simplified"
        self.version = "1.7-Simplified"
        self.strategy_key = "V12_V7_SIMPLIFIED"
        
        # 🆕 简化至3核心因子（固定权重，不调优）
        self.factor_weights = {
            'sentiment': 0.35,    # 情绪因子（IC最高0.70）
            'quality': 0.35,      # 质量因子（ROE）
            'valuation': 0.30     # 估值因子（PE）
        }
        
        # 🆕 固定参数（不调优，防过拟合）
        self.pick_count = 5
        self.score_threshold = 50      # 固定阈值（原55）
        self.cooling_days = 5          # 固定冷却期（原3）
        self.stop_loss_pct = -0.05     # 5%止损
        
        # 🆕 风控参数
        self.max_drawdown_limit = 0.20  # 最大回撤限制20%
        self.max_position_per_stock = 0.20  # 单股最大20%
        
        # 冷却期记录
        self.recent_picks = {}  # code -> last_pick_date
        
        logger.info(f"V12 V7简化版策略初始化 | 阈值:{self.score_threshold} | "
                   f"冷却期:{self.cooling_days}天 | 3核心因子")
    
    def get_factor_weights(self) -> Dict[str, float]:
        return self.factor_weights
    
    def select(self, date: str, market_status: str = 'neutral', 
               account_drawdown: float = 0.0) -> List[Dict]:
        """
        V12 V7选股主函数
        
        Args:
            date: 选股日期
            market_status: 市场状态 ('bull', 'neutral', 'bear')
            account_drawdown: 当前账户回撤
        """
        logger.info(f"V12 V7策略选股: {date} | 市场:{market_status} | 回撤:{account_drawdown:.1%}")
        
        # 🆕 市场环境过滤
        if self._should_stop_trading(market_status, account_drawdown):
            logger.warning(f"  ⚠️ 停止交易信号触发，返回空仓")
            return []
        
        # 获取股票池
        candidates = self._get_candidates(date)
        logger.info(f"候选股票: {len(candidates)} 只")
        
        if not candidates:
            return []
        
        # 过滤冷却期股票
        candidates = self._filter_cooling_stocks(candidates, date)
        logger.info(f"过滤冷却期后: {len(candidates)} 只")
        
        # Z-score评分（3因子）
        picks = self._select_with_zscore(candidates)
        
        # 应用仓位限制
        picks = self._apply_position_limits(picks, account_drawdown)
        
        # 记录选股时间
        for pick in picks:
            self.recent_picks[pick['code']] = date
        
        logger.info(f"最终选中: {len(picks)} 只")
        for p in picks[:3]:
            logger.info(f"  ✅ {p['code']} {p.get('name', '')} | {p['score']:.1f}分")
        
        return picks
    
    def _should_stop_trading(self, market_status: str, account_drawdown: float) -> bool:
        """
        🆕 停止交易条件
        """
        # 条件1: 熊市状态
        if market_status == 'bear':
            logger.info("  📉 熊市状态，停止交易")
            return True
        
        # 条件2: 回撤超限
        if account_drawdown > self.max_drawdown_limit:
            logger.warning(f"  🛑 回撤{account_drawdown:.1%}超过限制{self.max_drawdown_limit:.1%}")
            return True
        
        # 条件3: 回撤超过15%，减仓观察
        if account_drawdown > 0.15:
            logger.warning(f"  ⚠️ 回撤{account_drawdown:.1%}超过15%，谨慎交易")
        
        return False
    
    def _filter_cooling_stocks(self, candidates: List[Dict], date: str) -> List[Dict]:
        """过滤冷却期股票"""
        if not candidates:
            return candidates
        
        current_date = datetime.strptime(date, '%Y-%m-%d')
        filtered = []
        
        for stock in candidates:
            code = stock['code']
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                days_diff = (current_date - last_date).days
                if days_diff < self.cooling_days:
                    continue
            filtered.append(stock)
        
        return filtered
    
    def _apply_position_limits(self, picks: List[Dict], account_drawdown: float) -> List[Dict]:
        """
        🆕 应用仓位限制
        """
        if not picks:
            return []
        
        # 根据回撤调整仓位
        position_scale = 1.0
        if account_drawdown > 0.10:
            # 回撤10-20%：仓位降至50-0%
            position_scale = max(0, 1 - (account_drawdown - 0.10) / 0.10)
            logger.info(f"  📊 回撤{account_drawdown:.1%}，仓位缩放至{position_scale:.0%}")
        
        # 限制选股数量
        max_picks = max(1, int(self.pick_count * position_scale))
        
        return picks[:max_picks]
    
    def _get_candidates(self, date: str) -> List[Dict]:
        """获取候选股票 - V7简化版"""
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
                
                # 获取ST/退市列表
                cursor.execute("SELECT code FROM stock_basic WHERE is_st = 1 OR is_delisted = 1")
                exclude_codes = {row[0] for row in cursor.fetchall()}
                
                # 获取前一日日期
                cursor.execute("""
                    SELECT MAX(trade_date) FROM stock_kline 
                    WHERE trade_date < %s
                """, (trade_date,))
                prev_date = cursor.fetchone()[0]
                
                # 简化查询：只获取3因子所需数据
                cursor.execute("""
                    SELECT 
                        k.code, 
                        k.open as price, 
                        k.turnover,
                        k_prev.pct_change,
                        b.pe_score,      -- 估值因子
                        b.roe_score,     -- 质量因子
                        b.name
                    FROM stock_kline k
                    LEFT JOIN stock_kline k_prev ON k.code = k_prev.code 
                        AND k_prev.trade_date = %s
                    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
                    WHERE k.trade_date = %s
                    AND k.open BETWEEN 5 AND 150
                    AND k.turnover >= 0.5
                """, (prev_date, trade_date))
                
                for row in cursor.fetchall():
                    code = row[0]
                    if code in exclude_codes:
                        continue
                    
                    stocks.append({
                        'code': code,
                        'price': float(row[1]),
                        'turnover': float(row[2]) if row[2] else 0,
                        'prev_change': float(row[3]) if row[3] else 0,  # sentiment
                        'pe_score': float(row[4]) if row[4] is not None else None,  # valuation
                        'roe_score': float(row[5]) if row[5] is not None else None,  # quality
                        'name': row[6] or ''
                    })
                
                logger.info(f"基础过滤后候选股: {len(stocks)}只")
        finally:
            conn.close()
        
        return stocks
    
    def _select_with_zscore(self, candidates: List[Dict]) -> List[Dict]:
        """使用Z-score标准化选股 - V7简化版（3因子）"""
        if len(candidates) < 10:
            return []
        
        # 计算原始因子
        factor_data = []
        for stock in candidates:
            factors = self._calculate_factors(stock)
            if factors:  # 只保留有效数据
                factor_data.append((stock['code'], factors, stock))
        
        if len(factor_data) < 10:
            return []
        
        # Z-score标准化
        zscores = self._calculate_zscore(factor_data)
        
        # 计算最终得分
        picks = []
        for code, _, stock in factor_data:
            if code not in zscores:
                continue
            
            # 加权Z-score（3因子）
            weighted_zscore = (
                zscores[code].get('sentiment', 0) * self.factor_weights['sentiment'] +
                zscores[code].get('quality', 0) * self.factor_weights['quality'] +
                zscores[code].get('valuation', 0) * self.factor_weights['valuation']
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
                    'zscores': zscores[code]
                })
        
        # 排序
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:self.pick_count]
    
    def _calculate_factors(self, stock: Dict) -> Optional[Dict[str, float]]:
        """计算3因子值"""
        factors = {}
        
        # 1. Sentiment因子：前一日涨跌幅
        factors['sentiment'] = stock.get('prev_change', 0)
        
        # 2. Quality因子：ROE得分
        roe_score = stock.get('roe_score')
        if roe_score is not None and not np.isnan(roe_score):
            factors['quality'] = roe_score
        else:
            # 无ROE数据时跳过
            return None
        
        # 3. Valuation因子：PE得分
        pe_score = stock.get('pe_score')
        if pe_score is not None and not np.isnan(pe_score):
            factors['valuation'] = pe_score
        else:
            # 无PE数据时跳过
            return None
        
        return factors
    
    def _calculate_zscore(self, factor_data: List[tuple]) -> Dict[str, Dict[str, float]]:
        """计算Z-score标准化"""
        if not factor_data:
            return {}
        
        factor_names = list(self.factor_weights.keys())
        
        # 收集每个因子的值
        factor_values = {f: [] for f in factor_names}
        for code, factors, _ in factor_data:
            for f in factor_names:
                val = factors.get(f)
                if val is not None and not np.isnan(val):
                    factor_values[f].append((code, val))
        
        # 计算Z-score
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


# 导出
v12_strategy_v7 = V12StrategyV7()
