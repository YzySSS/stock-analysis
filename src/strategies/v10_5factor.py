#!/usr/bin/env python3
"""
V10 - 5因子固定权重策略
=======================
版本A：经典5因子评分，固定权重

因子权重:
- 技术面: 25%
- 情绪面: 20%
- 板块轮动: 30%
- 资金流向: 15%
- 风险因子: 10%
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict
import logging

from .base import BaseStrategy
from strategy_datasource import StrategyDataSource, SectorFactor, SentimentFactor, CapitalFactor, RiskFactor

logger = logging.getLogger(__name__)


class V10_5FactorStrategy(BaseStrategy):
    """V10 5因子固定权重策略"""
    
    def __init__(self, factor_weights: dict = None, pick_count: int = 3, **kwargs):
        super().__init__()
        self.name = "V10_5Factor"
        self.version = "1.0"
        self.strategy_key = "V10_5FACTOR"
        
        # 支持通过参数覆盖默认权重
        default_weights = {
            'technical': 0.25,   # 技术面
            'sentiment': 0.20,   # 情绪面
            'sector': 0.30,      # 板块轮动
            'capital': 0.15,     # 资金流向
            'risk': 0.10         # 风险因子
        }
        self.factor_weights = factor_weights or default_weights
        self.pick_count = pick_count
        
        # 其他配置参数
        self.config = kwargs
        
        # 初始化因子计算器（延迟加载）
        self._db = None
        self._sector_factor = None
        self._sentiment_factor = None
        self._capital_factor = None
        self._risk_factor = None

    @property
    def db(self):
        """数据源（延迟初始化）"""
        if self._db is None:
            self._db = StrategyDataSource()
        return self._db

    @property
    def sector_factor(self):
        """板块轮动因子"""
        if self._sector_factor is None:
            self._sector_factor = SectorFactor()
        return self._sector_factor

    @property
    def sentiment_factor(self):
        """情绪面因子"""
        if self._sentiment_factor is None:
            self._sentiment_factor = SentimentFactor()
        return self._sentiment_factor

    @property
    def capital_factor(self):
        """资金流向因子"""
        if self._capital_factor is None:
            self._capital_factor = CapitalFactor()
        return self._capital_factor

    @property
    def risk_factor(self):
        """风险因子"""
        if self._risk_factor is None:
            self._risk_factor = RiskFactor()
        return self._risk_factor

    def get_factor_weights(self) -> Dict[str, float]:
        """获取因子权重"""
        return self.factor_weights.copy()
    
    def select(self, date: str, top_n: int = 3) -> List[Dict]:
        """
        选股主方法
        
        Args:
            date: 选股日期 (YYYY-MM-DD)
            top_n: 选股数量
        
        Returns:
            选股结果列表
        """
        logger.info(f"V10策略选股: {date}")
        
        # 1. 获取全A股列表（非退市）
        all_stocks = self._get_stock_list()
        logger.info(f"全市场股票: {len(all_stocks)} 只")
        
        # 2. 计算每只股票的因子得分
        scored_stocks = []
        for code, name in all_stocks:
            try:
                score, factors = self._calculate_stock_score(code, name, date)
                if score > 0:
                    scored_stocks.append({
                        'code': code,
                        'name': name,
                        'score': score,
                        'factors': factors
                    })
            except Exception as e:
                logger.debug(f"计算 {code} 得分失败: {e}")
                continue
        
        logger.info(f"有效评分股票: {len(scored_stocks)} 只")
        
        # 3. 排序取Top N
        scored_stocks.sort(key=lambda x: x['score'], reverse=True)
        top_picks = scored_stocks[:top_n]
        
        # 4. 添加技术位分析
        for pick in top_picks:
            pick['tech_levels'] = self._calculate_tech_levels(pick['code'], date)
            pick['reason'] = self._generate_reason(pick)
        
        logger.info(f"选出 {len(top_picks)} 只股票: {[p['code'] for p in top_picks]}")
        
        return self.format_output(top_picks)
    
    def _get_stock_list(self) -> List[tuple]:
        """获取非退市股票列表"""
        try:
            # 从数据库获取
            import pymysql
            from config import DB_CONFIG
            
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT code, name FROM stock_basic 
                    WHERE is_delisted = 0
                    ORDER BY code
                ''')
                stocks = cursor.fetchall()
            conn.close()
            return stocks
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def _calculate_stock_score(self, code: str, name: str, date: str) -> tuple:
        """
        计算单只股票的综合得分
        
        Returns:
            (总分, 各因子得分字典)
        """
        factors = {}
        
        # 技术面得分 (0-100)
        try:
            tech_data = self.db.get_technical_indicators(code, date)
            factors['technical'] = self._calc_technical_score(tech_data)
        except:
            factors['technical'] = 50
        
        # 情绪面得分 (0-100)
        try:
            factors['sentiment'] = self.sentiment_factor.calculate(code, name, date)
        except:
            factors['sentiment'] = 50
        
        # 板块轮动得分 (0-100)
        try:
            factors['sector'] = self.sector_factor.calculate(code, date)
        except:
            factors['sector'] = 50
        
        # 资金流向得分 (0-100)
        try:
            factors['capital'] = self.capital_factor.calculate(code, date)
        except:
            factors['capital'] = 50
        
        # 风险因子得分 (0-100，越低越好，所以反转)
        try:
            risk_score = self.risk_factor.calculate(code, date)
            factors['risk'] = 100 - risk_score  # 反转，风险低得分高
        except:
            factors['risk'] = 50
        
        # 加权计算总分
        total_score = sum(
            factors[key] * weight 
            for key, weight in self.factor_weights.items()
        )
        
        return round(total_score, 2), factors
    
    def _calc_technical_score(self, tech_data: Dict) -> float:
        """计算技术面得分"""
        score = 50  # 基础分
        
        # 均线多头排列加分
        if tech_data.get('ma_trend') == 'up':
            score += 15
        
        # MACD金叉加分
        if tech_data.get('macd_signal') == 'golden_cross':
            score += 10
        
        # 放量上涨加分
        if tech_data.get('volume_ratio', 1) > 1.5:
            score += 10
        
        # RSI适中（不超买超卖）
        rsi = tech_data.get('rsi', 50)
        if 40 <= rsi <= 60:
            score += 5
        
        return min(100, score)
    
    def _calculate_tech_levels(self, code: str, date: str) -> Dict:
        """计算技术位（支撑/阻力/止损/目标价）"""
        try:
            hist = self.db.get_recent_prices(code, days=20)
            if not hist:
                return {}
            
            current_price = hist[-1]['close']
            
            # 计算支撑位（近期低点）
            recent_lows = [h['low'] for h in hist[-10:]]
            support_strong = min(recent_lows)
            support_weak = sum(sorted(recent_lows)[:3]) / 3
            
            # 计算阻力位（近期高点）
            recent_highs = [h['high'] for h in hist[-10:]]
            resistance_near = max(recent_highs[-5:])
            resistance_far = max(recent_highs)
            
            # 止损价（强支撑位下方3%）
            stop_loss = support_strong * 0.97
            
            # 目标价（近阻力位）
            target = resistance_near
            
            # 风险收益比
            risk = current_price - stop_loss
            reward = target - current_price
            risk_reward_ratio = reward / risk if risk > 0 else 0
            
            return {
                'current': round(current_price, 2),
                'support_strong': round(support_strong, 2),
                'support_weak': round(support_weak, 2),
                'resistance_near': round(resistance_near, 2),
                'resistance_far': round(resistance_far, 2),
                'stop_loss': round(stop_loss, 2),
                'target': round(target, 2),
                'risk_reward_ratio': round(risk_reward_ratio, 2)
            }
        except Exception as e:
            logger.debug(f"计算技术位失败 {code}: {e}")
            return {}
    
    def _generate_reason(self, pick: Dict) -> str:
        """生成选股理由"""
        factors = pick.get('factors', {})
        score = pick.get('score', 0)
        
        # 找出得分最高的因子
        max_factor = max(factors.items(), key=lambda x: x[1])
        
        reasons = []
        if score > 80:
            reasons.append("综合评分优秀")
        elif score > 70:
            reasons.append("综合评分良好")
        
        reasons.append(f"{max_factor[0]}因子突出({max_factor[1]:.0f}分)")
        
        return "; ".join(reasons)


if __name__ == "__main__":
    # 测试
    strategy = V10_5FactorStrategy()
    picks = strategy.select(date="2026-04-03")
    print(f"选股结果: {picks}")
