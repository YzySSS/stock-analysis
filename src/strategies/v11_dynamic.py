#!/usr/bin/env python3
"""
V11 - 动态权重策略
==================
版本B：根据市场强弱动态调整因子权重 + 技术位分析

基础权重:
- 技术面: 35%
- 情绪面: 20%
- 板块轮动: 30%
- 资金流向: 15%
- 风险因子: 10%

动态调整逻辑:
- 强势市场(>60): 进攻性因子×1.05, 风险因子×0.90
- 弱势市场(<40): 进攻性因子×0.95, 风险因子×1.20
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict
import logging

from .v10_5factor import V10_5FactorStrategy

logger = logging.getLogger(__name__)


class V11_DynamicStrategy(V10_5FactorStrategy):
    """V11 动态权重策略"""
    
    def __init__(self, base_weights: dict = None,
                 market_strength_threshold = None,
                 pick_count: int = 3, **kwargs):
        super().__init__()
        self.name = "V11_Dynamic"
        self.version = "2.1"  # 优化版
        self.strategy_key = "V11_DYNAMIC"

        # V11优化版: 基础权重调整
        # 板块从30%降至20%，风险从10%提升至25%
        default_weights = {
            'technical': 0.30,   # 技术面（优化: 30%）
            'sentiment': 0.15,   # 情绪面（优化: 15%）
            'sector': 0.20,      # 板块轮动（优化: 从30%降至20%）
            'capital': 0.10,     # 资金流向（优化: 从15%降至10%）
            'risk': 0.25         # 风险因子（优化: 从10%提升至25%）
        }
        self.base_weights = base_weights or default_weights
        self.factor_weights = self.base_weights.copy()

        # V11优化版: 调整市场强弱阈值
        # 进攻模式 >65分，防御模式 <35分
        if market_strength_threshold is None:
            self.weak_threshold, self.strong_threshold = 35, 65
        elif isinstance(market_strength_threshold, (list, tuple)):
            self.weak_threshold, self.strong_threshold = market_strength_threshold[0], market_strength_threshold[1]
        else:
            self.strong_threshold = market_strength_threshold
            self.weak_threshold = market_strength_threshold - 30
        
        # 选股数量
        self.pick_count = pick_count
        
        # 其他配置
        self.config = kwargs
        
        # V11优化版: 重新定义攻防因子
        # 进攻因子: 技术、情绪、资金
        # 防御因子: 板块、风险
        self.offensive_factors = ['technical', 'sentiment', 'capital']
        self.defensive_factors = ['sector', 'risk']
        
        # V11优化版: 攻防模式权重配置
        self.offensive_mode_weights = {
            'technical': 0.45,   # 进攻: 技术45%
            'sentiment': 0.25,   # 进攻: 情绪25%
            'sector': 0.15,      # 进攻: 板块15%
            'capital': 0.10,     # 进攻: 资金10%
            'risk': 0.05         # 进攻: 风险5%
        }
        
        self.defensive_mode_weights = {
            'technical': 0.15,   # 防御: 技术15%
            'sentiment': 0.10,   # 防御: 情绪10%
            'sector': 0.20,      # 防御: 板块20%
            'capital': 0.20,     # 防御: 资金20%
            'risk': 0.35         # 防御: 风险35%
        }
    
    def get_factor_weights(self) -> Dict[str, float]:
        """获取基础权重（实际使用时会动态调整）"""
        return self.base_weights.copy()
    
    def select(self, date: str, top_n: int = 3) -> List[Dict]:
        """
        选股主方法（带动态权重调整）
        """
        logger.info(f"V11策略选股: {date}")
        
        # 1. 计算市场强弱指数
        market_strength = self._calculate_market_strength(date)
        logger.info(f"市场强弱指数: {market_strength:.1f}")
        
        # 2. 动态调整因子权重
        adjusted_weights = self._adjust_weights(market_strength)
        logger.info(f"调整后权重: {adjusted_weights}")
        
        # 3. 临时更新权重
        original_weights = self.factor_weights.copy()
        self.factor_weights = adjusted_weights
        
        # 4. 调用父类选股逻辑
        picks = super().select(date, top_n)
        
        # 5. 恢复原始权重
        self.factor_weights = original_weights
        
        # 6. 添加市场状态信息
        for pick in picks:
            pick['market_strength'] = market_strength
            pick['market_status'] = self._get_market_status(market_strength)
            pick['adjusted_weights'] = adjusted_weights
        
        return picks
    
    def _calculate_market_strength(self, date: str) -> float:
        """
        V11优化版: 计算市场强弱指数 (0-100)
        
        优化后的四个维度:
        - 趋势 (40%): MA20趋势 + 多头排列
        - 宽度 (30%): 上涨家数占比
        - 成交量 (20%): 成交额对比20日均值
        - 情绪 (10%): 涨跌停家数比 + 波动率
        
        优化点:
        - 融入MA20判断长期趋势
        - 增加市场宽度指标
        - 引入波动率(VIX概念)
        """
        try:
            # 获取上证指数数据
            index_data = self.db.get_index_data('000001', date)
            if not index_data:
                return 50  # 默认中性
            
            strength = 0
            
            # 1. 趋势得分 (40%) - 融入MA20
            trend_score = 0
            close = index_data.get('close', 0)
            ma5 = index_data.get('ma5', 0)
            ma10 = index_data.get('ma10', 0)
            ma20 = index_data.get('ma20', 0)
            
            # MA20趋势判断
            if close > ma20:
                trend_score += 20  # 站上MA20
            if ma5 > ma10 > ma20:
                trend_score += 15  # 多头排列
            elif ma5 > ma10:
                trend_score += 5   # 短期多头
            
            # 2. 宽度得分 (30%) - 上涨家数占比
            breadth_score = 0
            try:
                # 从数据库获取市场宽度数据
                market_breadth = self.db.get_market_breadth(date)
                if market_breadth:
                    up_ratio = market_breadth.get('up_ratio', 0.5)
                    breadth_score = up_ratio * 30  # 上涨占比*30分
                else:
                    breadth_score = 15  # 默认中性
            except:
                breadth_score = 15
            
            # 3. 成交量得分 (20%)
            volume_score = 0
            volume_ratio = index_data.get('volume_ratio', 1)
            if volume_ratio > 1.5:
                volume_score = 20
            elif volume_ratio > 1.2:
                volume_score = 15
            elif volume_ratio > 0.8:
                volume_score = 10
            else:
                volume_score = 5
            
            # 4. 情绪得分 (10%) - 涨跌停比 + 波动率
            sentiment_score = 0
            try:
                # 涨跌停数据
                limit_data = self.db.get_limit_up_down(date)
                if limit_data:
                    limit_up = limit_data.get('limit_up', 50)
                    limit_down = limit_data.get('limit_down', 20)
                    if limit_down == 0:
                        limit_down = 1
                    sentiment_ratio = limit_up / limit_down
                    if sentiment_ratio > 3:
                        sentiment_score = 10
                    elif sentiment_ratio > 1:
                        sentiment_score = 7
                    else:
                        sentiment_score = 3
                else:
                    sentiment_score = 5
            except:
                sentiment_score = 5
            
            # 计算总分
            strength = trend_score + breadth_score + volume_score + sentiment_score
            
            # 波动率调整 - 高波动率降低得分
            try:
                atr_20 = index_data.get('atr_20', 0)
                if atr_20 > 2.0:  # 高波动
                    strength -= 5
                elif atr_20 < 1.0:  # 低波动
                    strength += 3
            except:
                pass
            
            return max(0, min(100, strength))
            
        except Exception as e:
            logger.warning(f"计算市场强弱失败: {e}")
            return 50
            return 50
    
    def _adjust_weights(self, market_strength: float) -> Dict[str, float]:
        """
        V11优化版: 根据市场强弱动态调整因子权重
        
        优化内容:
        - 调整阈值: 进攻>65分，防御<35分
        - 大幅调整权重，建立明确的"攻防模式"
        - 进攻模式: 技术45% 情绪25% 板块15% 资金10% 风险5%
        - 防御模式: 技术15% 情绪10% 板块20% 资金20% 风险35%
        - 震荡模式: 使用基础权重
        """
        if market_strength > self.strong_threshold:
            # V11优化版: 强势市场使用进攻模式权重
            logger.info(f"市场强势({market_strength:.0f}分)，切换为进攻模式")
            weights = self.offensive_mode_weights.copy()
                
        elif market_strength < self.weak_threshold:
            # V11优化版: 弱势市场使用防御模式权重
            logger.info(f"市场弱势({market_strength:.0f}分)，切换为防御模式")
            weights = self.defensive_mode_weights.copy()
        else:
            # 震荡市场：使用基础权重（已优化）
            logger.info(f"市场震荡({market_strength:.0f}分)，使用标准权重")
            weights = self.base_weights.copy()
        
        # 归一化，确保权重总和为1
        total = sum(weights.values())
        weights = {k: round(v / total, 3) for k, v in weights.items()}
        
        return weights
    
    def _get_market_status(self, market_strength: float) -> str:
        """获取市场状态描述"""
        if market_strength > 60:
            return "强势"
        elif market_strength < 40:
            return "弱势"
        else:
            return "震荡"
    
    def _generate_reason(self, pick: Dict) -> str:
        """生成选股理由（V11版本，包含市场状态）"""
        base_reason = super()._generate_reason(pick)
        
        market_status = pick.get('market_status', '震荡')
        
        return f"[{market_status}市场] {base_reason}"


if __name__ == "__main__":
    # 测试
    strategy = V11_DynamicStrategy()
    picks = strategy.select(date="2026-04-03")
    print(f"选股结果: {picks}")
