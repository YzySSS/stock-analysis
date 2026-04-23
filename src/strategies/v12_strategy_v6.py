#!/usr/bin/env python3
"""
V12 选股策略 V6 - 完整优化版
==========================
更新内容:
1. ✅ 复利计算已修复（继承V5）
2. 🆕 加入市值因子并中性化
3. 🆕 行业权重上限约束
4. 🆕 IC/IR分析优化权重
5. 🆕 降低换手率/拉长持仓周期（持仓冷却期）
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


class V12StrategyV6(BaseStrategy):
    """V12 多因子选股策略 V6 - 完整优化版"""
    
    def __init__(self, **kwargs):
        super().__init__()
        self.name = "V12_MultiFactor_V6"
        self.version = "1.6-FullOptimization"
        self.strategy_key = "V12_V6"
        
        # 基础权重配置（6因子）
        self.base_weights = {
            'trend': 0.18,        # 趋势因子
            'momentum': 0.13,     # 动量因子（降低，与趋势有共线性）
            'quality': 0.18,      # 质量因子
            'sentiment': 0.13,    # 情绪因子
            'valuation': 0.18,    # 估值因子
            'liquidity': 0.10,    # 流动性因子
            'size': 0.10          # 🆕 市值因子
        }
        
        self.factor_weights = self.base_weights.copy()
        
        # 配置参数
        self.pick_count = kwargs.get('pick_count', 5)
        self.score_threshold = kwargs.get('threshold', 55)
        
        # 🆕 行业权重约束
        self.industry_max_weight = kwargs.get('industry_max_weight', 0.30)  # 单行业最大30%
        
        # 🆕 持仓冷却期（降低换手率）
        self.cooling_days = kwargs.get('cooling_days', 3)  # 买入后3天内不重复买入
        self.recent_picks = {}  # code -> last_pick_date
        
        # 🆕 IC/IR记录（用于动态优化权重）
        self.ic_history = []
        self.ir_lookback = 20  # 20天滚动计算IR
        
        logger.info(f"V12 V6策略初始化 | 阈值:{self.score_threshold} | 最大选股:{self.pick_count} | "
                   f"行业上限:{self.industry_max_weight:.0%} | 冷却期:{self.cooling_days}天")
    
    def get_factor_weights(self) -> Dict[str, float]:
        return self.factor_weights
    
    def update_weights_by_ir(self, recent_returns: List[Dict]):
        """🆕 根据IC/IR动态优化权重"""
        if len(recent_returns) < self.ir_lookback:
            return
        
        # 计算各因子的IC
        factor_ics = defaultdict(list)
        for record in recent_returns[-self.ir_lookback:]:
            for factor, zscore in record.get('factor_zscores', {}).items():
                future_return = record.get('future_return', 0)
                factor_ics[factor].append((zscore, future_return))
        
        # 计算IR并调整权重
        new_weights = self.base_weights.copy()
        total_weight = sum(self.base_weights.values())
        
        for factor, pairs in factor_ics.items():
            if len(pairs) < 10:
                continue
            
            zscores = [p[0] for p in pairs]
            returns = [p[1] for p in pairs]
            
            # 计算IC
            if np.std(zscores) > 0 and np.std(returns) > 0:
                ic = np.corrcoef(zscores, returns)[0, 1]
                # 简单的权重调整：IC高的因子增加权重
                adjustment = 1 + (ic * 0.2)  # IC=0.1 -> 权重+2%
                new_weights[factor] = self.base_weights[factor] * adjustment
        
        # 归一化
        weight_sum = sum(new_weights.values())
        if weight_sum > 0:
            self.factor_weights = {k: v / weight_sum * total_weight for k, v in new_weights.items()}
        
        logger.info(f"权重更新: {self.factor_weights}")
    
    def select(self, date: str, top_n: int = None, 
               existing_positions: List[str] = None) -> List[Dict]:
        """V12 V6选股主函数"""
        if top_n is None:
            top_n = self.pick_count
        
        if existing_positions is None:
            existing_positions = []
        
        logger.info(f"V12 V6策略选股: {date} | 阈值:{self.score_threshold} | 现有持仓:{len(existing_positions)}")
        
        # 获取股票池
        candidates = self._get_candidates(date)
        logger.info(f"候选股票: {len(candidates)} 只")
        
        if not candidates:
            return []
        
        # 🆕 过滤冷却期股票（降低换手率）
        candidates = self._filter_cooling_stocks(candidates, date)
        logger.info(f"过滤冷却期后: {len(candidates)} 只")
        
        # Z-score评分
        picks = self._select_with_zscore(candidates, top_n)
        
        # 🆕 应用行业权重约束
        picks = self._apply_industry_constraint(picks, existing_positions)
        
        # 记录选股时间
        for pick in picks:
            self.recent_picks[pick['code']] = date
        
        logger.info(f"最终选中: {len(picks)} 只")
        for p in picks[:3]:
            logger.info(f"  ✅ {p['code']} {p.get('name', '')} | {p['score']:.1f}分 | {p.get('industry', '未知')}")
        
        return picks
    
    def _filter_cooling_stocks(self, candidates: List[Dict], date: str) -> List[Dict]:
        """🆕 过滤处于冷却期的股票"""
        current_date = datetime.strptime(date, '%Y-%m-%d')
        filtered = []
        
        for stock in candidates:
            code = stock['code']
            if code in self.recent_picks:
                last_date = datetime.strptime(self.recent_picks[code], '%Y-%m-%d')
                days_diff = (current_date - last_date).days
                if days_diff < self.cooling_days:
                    continue  # 仍在冷却期内，跳过
            filtered.append(stock)
        
        return filtered
    
    def _apply_industry_constraint(self, picks: List[Dict], existing_positions: List[str]) -> List[Dict]:
        """🆕 应用行业权重上限约束"""
        if not picks:
            return []
        
        # 统计现有持仓的行业分布
        industry_counts = defaultdict(int)
        for pos in existing_positions:
            # 这里简化处理，实际应该从持仓管理中获取行业信息
            pass
        
        # 计算每个行业的上限数量
        max_per_industry = max(1, int(self.pick_count * self.industry_max_weight))
        
        filtered_picks = []
        industry_selected = defaultdict(int)
        
        for pick in picks:
            industry = pick.get('industry', '未知')
            if industry_selected[industry] < max_per_industry:
                filtered_picks.append(pick)
                industry_selected[industry] += 1
            else:
                logger.info(f"  ⏭️ 跳过 {pick['code']} - {industry}行业已达上限")
        
        return filtered_picks
    
    def _get_candidates(self, date: str) -> List[Dict]:
        """获取候选股票 - V6加入市值数据"""
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
                
                # 🆕 获取当日数据 + 清洗后的基本面数据 + 市值数据
                cursor.execute("""
                    SELECT 
                        k.code, 
                        k.open as price, 
                        k.turnover,
                        k_prev.pct_change,
                        k.market_cap,           -- 🆕 市值
                        b.pe_clean,             -- 清洗后的PE
                        b.pb_clean,             -- 清洗后的PB
                        b.roe_clean,            -- 清洗后的ROE
                        b.pe_score,             -- PE得分
                        b.roe_score,            -- ROE得分
                        b.name,
                        b.industry              -- 🆕 行业
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
                    
                    market_cap = row[4]
                    
                    kline_data[code] = {
                        'code': code,
                        'price': float(row[1]),
                        'turnover': float(row[2]) if row[2] else 0,
                        'prev_change': float(row[3]) if row[3] else 0,
                        'market_cap': float(market_cap) if market_cap else None,  # 🆕 市值
                        'pe_clean': float(row[5]) if row[5] is not None else None,
                        'pb_clean': float(row[6]) if row[6] is not None else None,
                        'roe_clean': float(row[7]) if row[7] is not None else None,
                        'pe_score': float(row[8]) if row[8] is not None else None,
                        'roe_score': float(row[9]) if row[9] is not None else None,
                        'name': row[10] or '',
                        'industry': row[11] or '其他'  # 🆕 行业
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
        """使用Z-score标准化选股 - V6加入市值因子"""
        if len(candidates) < 10:
            return []
        
        # 计算原始因子
        raw_factors = []
        for stock in candidates:
            factors = self._calculate_raw_factors(stock)
            raw_factors.append((stock['code'], factors, stock))
        
        # Z-score标准化
        zscores = self._calculate_zscore(raw_factors)
        
        # 🆕 市值中性化：在行业内进行市值标准化
        zscores = self._neutralize_size(zscores, candidates)
        
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
                    'industry': stock.get('industry', '其他'),
                    'zscores': zscores[code],
                    'raw_factors': {k: stock.get(k) for k in ['pe_clean', 'roe_clean', 'turnover', 'market_cap']}
                })
        
        # 排序并限制数量
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:top_n]
    
    def _calculate_raw_factors(self, stock: Dict) -> Dict[str, float]:
        """计算原始因子值 - V6加入市值因子"""
        prices = stock['prices']
        
        factors = {}
        
        # 1. 趋势因子 - MA20斜率（年化）
        if len(prices) >= 25:
            ma20_now = sum(prices[-20:]) / 20
            ma20_prev = sum(prices[-25:-5]) / 20
            factors['trend'] = (ma20_now - ma20_prev) / ma20_prev * 252 if ma20_prev > 0 else 0
        else:
            factors['trend'] = 0
        
        # 2. 动量因子 - 20日涨幅（降低权重，与趋势有共线性）
        if len(prices) >= 21:
            factors['momentum'] = (prices[-1] - prices[-21]) / prices[-21] * 100
        else:
            factors['momentum'] = 0
        
        # 3. 质量因子 - ROE
        roe_clean = stock.get('roe_clean', 0)
        if roe_clean is not None and not np.isnan(roe_clean):
            factors['quality'] = roe_clean
        else:
            if len(prices) >= 21:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                          for i in range(-20, 0)]
                volatility = np.std(returns) if returns else 10
                factors['quality'] = 20 - volatility
            else:
                factors['quality'] = 0
        
        # 4. 情绪因子
        factors['sentiment'] = stock.get('prev_change', 0)
        
        # 5. 估值因子 - PE得分
        pe_score = stock.get('pe_score')
        if pe_score is not None and not np.isnan(pe_score):
            factors['valuation'] = pe_score
        else:
            factors['valuation'] = -25
        
        # 6. 流动性因子
        turnover = stock.get('turnover', 0)
        factors['liquidity'] = np.log(turnover + 1) if turnover > 0 else 0
        
        # 🆕 7. 市值因子 - 小市值偏好（A股小市值效应）
        market_cap = stock.get('market_cap')
        if market_cap is not None and market_cap > 0:
            # 使用对数市值，小市值得高分
            factors['size'] = -np.log(market_cap)  # 负值，市值越小分数越高
        else:
            factors['size'] = 0
        
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
    
    def _neutralize_size(self, zscores: Dict, candidates: List[Dict]) -> Dict[str, Dict[str, float]]:
        """🆕 市值中性化：在行业内对市值因子进行标准化"""
        # 按行业分组
        industry_groups = defaultdict(list)
        for stock in candidates:
            industry = stock.get('industry', '其他')
            industry_groups[industry].append(stock['code'])
        
        # 在每个行业内对size因子进行标准化
        for industry, codes in industry_groups.items():
            if len(codes) < 5:
                continue
            
            size_values = [zscores.get(c, {}).get('size', 0) for c in codes if c in zscores]
            if len(size_values) < 5:
                continue
            
            mean_size = np.mean(size_values)
            std_size = np.std(size_values)
            
            if std_size < 1e-10:
                std_size = 1e-10
            
            for code in codes:
                if code in zscores and 'size' in zscores[code]:
                    # 行业内标准化
                    raw_size = zscores[code]['size']
                    zscores[code]['size'] = (raw_size - mean_size) / std_size
        
        return zscores


# 导出
v12_strategy_v6 = V12StrategyV6()
