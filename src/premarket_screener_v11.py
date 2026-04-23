#!/usr/bin/env python3
"""
V11 简化版盘前选股策略
基于已有数据重构，不依赖外部接口
"""

import sys
sys.path.insert(0, 'src')

from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class PremarketFactors:
    """盘前因子数据结构"""
    technical: float = 0      # 技术因子(重构)
    sentiment: float = 0      # 情绪因子(8%)
    sector: float = 0         # 板块因子(33%)
    money_flow: float = 0     # 资金因子(简化)
    risk: float = 0           # 风险因子(15%)
    total: float = 0          # 总分


class PremarketScreenerV11:
    """
    V11简化版盘前选股器
    
    因子权重:
    - 技术: 20% (基于昨日数据)
    - 情绪: 8% (龙虎榜+分位数)
    - 板块: 33% (维持现有)
    - 资金: 14% (龙虎榜+放量)
    - 风险: 15% (维持现有)
    
    阈值: 50分(固定)
    """
    
    # 阈值
    THRESHOLD = 50
    
    def __init__(self):
        from main import StockScreenerV9
        self.base_screener = StockScreenerV9()
        logger.info("✅ V11简化版盘前选股器初始化完成")
    
    def calculate_technical_score(self, data: Dict, historical_prices: List[float] = None) -> float:
        """
        技术因子(20分) - 基于昨日数据
        
        修改:
        - 基础分: 8 → 12
        - 昨日涨幅替代当日涨幅
        - 昨日收盘确认突破/均线
        """
        price = data.get('price', 0)
        change_pct = data.get('change_pct', 0)  # 这是昨日涨幅(T-1)
        
        score = 12  # 提高基础分
        
        # 1. 昨日涨幅得分 (重构)
        # 筛选昨日涨幅3-9%的股票(已启动但未涨停)
        if 3 <= change_pct < 9.5:
            score += 4  # 最佳区间
        elif 1 <= change_pct < 3:
            score += 2  # 温和上涨
        elif 0 <= change_pct < 1:
            score += 1  # 小幅上涨
        elif change_pct >= 9.5:
            score -= 2  # 涨停股次日高开风险大
        elif change_pct < -3:
            score -= 4  # 昨日大跌
        
        # 2. 趋势确认 (基于昨日收盘)
        if historical_prices and len(historical_prices) >= 60:
            yesterday_close = historical_prices[-1]
            
            # 昨日收盘突破20日高点
            high_20 = max(historical_prices[-21:-1]) if len(historical_prices) >= 21 else 0
            if yesterday_close > high_20 * 0.99:  # 允许0.5%误差
                score += 4
            
            # 昨日收盘突破60日高点
            high_60 = max(historical_prices[-61:-1]) if len(historical_prices) >= 61 else 0
            if yesterday_close > high_60 * 0.99:
                score += 4
            
            # 多头排列 (MA5>MA10>MA20)
            ma5 = sum(historical_prices[-5:]) / 5
            ma10 = sum(historical_prices[-10:]) / 10
            ma20 = sum(historical_prices[-20:]) / 20
            if ma5 > ma10 > ma20:
                score += 4
            
            # RSI在55-75之间(动量健康)
            try:
                rsi = self._calculate_rsi(historical_prices, 14)
                if 55 <= rsi <= 75:
                    score += 3
                elif rsi > 80:  # 超买
                    score -= 3
            except:
                pass
            
            # 长上影线检测(昨日抛压)
            # 需要high/low数据，简化版跳过
        
        return min(20, max(0, score))
    
    def calculate_sentiment_score(self, data: Dict, historical_prices: List[float] = None) -> float:
        """
        情绪因子(8分) - 提升权重，增加龙虎榜
        
        修改:
        - 权重从3%提升到8%
        - 增加龙虎榜数据
        """
        score = 4  # 基础分
        change_pct = data.get('change_pct', 0)
        volume = data.get('volume', 0)
        code = data.get('code', '')
        
        # 1. 昨日涨幅分位数(简化版)
        # 假设涨幅4-6%为最佳情绪区间
        if 4 <= change_pct <= 6:
            score += 2
        elif 2 <= change_pct < 4:
            score += 1
        
        # 2. 龙虎榜加分(如有数据)
        # TODO: 接入龙虎榜API
        # 简化版：假设昨日涨停或大涨的股票可能有龙虎榜
        if change_pct >= 9:
            score += 1  # 可能有游资参与
        
        # 3. 成交量情绪(昨日放量)
        if historical_prices and len(historical_prices) >= 20:
            try:
                # 简化：检查volume是否大于前5日均量
                # 实际需要volume历史数据
                pass
            except:
                pass
        
        return min(8, max(0, score))
    
    def calculate_sector_score(self, data: Dict, sectors: List[Dict], sector_momentum: Dict = None) -> float:
        """
        板块因子(33分) - 维持现有逻辑
        
        直接使用V9的板块轮动评分
        """
        from main import MultiFactorAnalyzerV10
        analyzer = MultiFactorAnalyzerV10()
        
        # 调用现有板块评分逻辑
        sector_score = analyzer._calc_sector_rotation_score_v10(
            data, sectors, sector_momentum, 
            data.get('code', ''), data.get('name', '')
        )
        
        # V9返回0-35分，按比例缩放到0-33分
        return min(33, sector_score * 0.94)
    
    def calculate_money_flow_score(self, data: Dict, historical_prices: List[float] = None) -> float:
        """
        资金因子(14分) - 简化版
        
        修改:
        - 去掉实时主力流向
        - 保留龙虎榜+昨日放量
        """
        score = 6  # 基础分
        change_pct = data.get('change_pct', 0)
        volume = data.get('volume', 0)
        turnover = data.get('turnover', 0)
        code = data.get('code', '')
        
        # 1. 昨日成交额结构
        if code.startswith(('00', '60')):
            # 主板
            if turnover > 5e8:  # 5亿
                score += 4
            elif turnover > 2e8:  # 2亿
                score += 2
        else:
            # 中小创
            if turnover > 2e8:  # 2亿
                score += 4
            elif turnover > 8e7:  # 8000万
                score += 2
        
        # 2. 上涨+放量(昨日)
        if change_pct > 0:
            # 简化判断：涨幅>3%且成交额>1亿(主板)/3000万(中小创)
            if code.startswith(('00', '60')):
                if change_pct > 3 and turnover > 1e8:
                    score += 3
            else:
                if change_pct > 3 and turnover > 3e7:
                    score += 3
        
        # 3. 龙虎榜资金(简化)
        # TODO: 接入龙虎榜API后增加
        # 涨停股默认可能有资金关注
        if change_pct >= 9.5:
            score += 1
        
        return min(14, max(0, score))
    
    def calculate_risk_score(self, data: Dict, historical_prices: List[float] = None) -> float:
        """
        风险因子(15分) - 维持现有
        
        基于昨日数据的风险评估
        """
        change_pct = data.get('change_pct', 0)
        
        score = 10  # 基础分
        
        # 1. 昨日波动率
        if abs(change_pct) < 3:
            score += 3
        elif abs(change_pct) < 5:
            score += 1
        elif abs(change_pct) < 8:
            score -= 2
        else:
            score -= 4
        
        # 2. 20日最大回撤(基于历史数据)
        if historical_prices and len(historical_prices) >= 20:
            try:
                recent_high = max(historical_prices[-20:])
                current = historical_prices[-1]
                max_drawdown = (recent_high - current) / recent_high * 100 if recent_high > 0 else 0
                
                if max_drawdown > 15:
                    score -= 4
                elif max_drawdown > 10:
                    score -= 2
            except:
                pass
        
        # 3. 历史高位判断(80%分位)
        if historical_prices and len(historical_prices) >= 60:
            try:
                current = historical_prices[-1]
                sorted_prices = sorted(historical_prices[-60:])
                percentile_80 = sorted_prices[int(len(sorted_prices) * 0.8)]
                
                if current > percentile_80:
                    score -= 3  # 在80%分位以上，追高风险
            except:
                pass
        
        return min(15, max(0, score))
    
    def apply_filters(self, data: Dict, historical_prices: List[float] = None,
                      market_mode: str = 'neutral') -> tuple:
        """
        应用选股硬性过滤条件 - V11优化版2.0 (DeepSeek建议)
        
        新增基础过滤:
        - 收盘价 > MA20 (趋势向上)
        - 非ST股票
        - 上市 > 120天
        - MA20 > MA60 (中期趋势向上)
        
        原有过滤:
        - 成交额 > 2亿元
        - 股价 5-200元
        
        Args:
            data: 股票数据
            historical_prices: 历史价格
            market_mode: 市场模式 ('offensive'/'defensive'/'neutral')
            
        Returns:
            (是否通过, 过滤原因)
        """
        price = data.get('price', 0)
        turnover = data.get('turnover', 0)  # 成交额
        code = data.get('code', '')
        
        # 1. ST股票过滤
        name = data.get('name', '')
        if 'ST' in name or '*ST' in name:
            return False, 'ST股票'
        
        # 2. 上市时间过滤 (>120天)
        listing_days = data.get('listing_days', 999)
        if listing_days < 120:
            return False, f'上市{listing_days}天<120天'
        
        # 3. 成交额过滤: >2亿元
        if turnover < 2e8:
            return False, f'成交额{turnover/1e8:.1f}亿<2亿'
        
        # 4. 股价过滤: 5-200元
        if price < 5:
            return False, f'股价{price}<5元'
        if price > 200:
            return False, f'股价{price}>200元'
        
        # 5. 趋势过滤: 收盘价 > MA20 > MA60
        if historical_prices and len(historical_prices) >= 60:
            ma20 = sum(historical_prices[-20:]) / 20
            ma60 = sum(historical_prices[-60:]) / 60
            
            # 核心过滤: 必须在MA20之上
            if price < ma20:
                return False, f'股价{price:.2f}<MA20({ma20:.2f})'
            
            # 中期趋势: MA20 > MA60
            if ma20 < ma60:
                return False, f'MA20({ma20:.2f})<MA60({ma60:.2f})'
        else:
            return False, '历史数据不足60天'
        
        return True, '通过'
    
    # ==================== 新增阿尔法因子 (DeepSeek建议) ====================
    
    def calculate_momentum_score(self, data: Dict, historical_prices: List[float] = None) -> float:
        """
        新增: 价格动量因子 (15分)
        
        基于DeepSeek建议的新阿尔法因子:
        - 近20日涨幅 (RPS相对强度)
        - 近5日/10日/20日涨幅排名
        - 价格趋势强度
        """
        if not historical_prices or len(historical_prices) < 20:
            return 7.5  # 默认中分
        
        score = 7.5  # 基础分
        
        # 1. 近20日涨幅 (5分)
        try:
            price_20d_ago = historical_prices[-20]
            current_price = historical_prices[-1]
            return_20d = (current_price - price_20d_ago) / price_20d_ago * 100
            
            # 涨幅适中最佳 (5-25%)
            if 5 <= return_20d <= 25:
                score += 5
            elif 0 <= return_20d < 5:
                score += 2
            elif 25 < return_20d <= 40:
                score += 2
            else:
                score -= 3  # 涨幅过大或负收益
        except:
            pass
        
        # 2. 近5日/10日趋势 (5分)
        try:
            return_5d = (historical_prices[-1] - historical_prices[-5]) / historical_prices[-5] * 100
            return_10d = (historical_prices[-1] - historical_prices[-10]) / historical_prices[-10] * 100
            
            # 短期趋势向上
            if return_5d > 0 and return_10d > 0:
                score += 3
            if return_5d > return_10d * 0.5:  # 近期加速
                score += 2
            elif return_5d < 0:
                score -= 3
        except:
            pass
        
        return min(15, max(0, score))
    
    def calculate_analyst_score(self, data: Dict) -> float:
        """
        新增: 分析师预期因子 (10分)
        
        基于DeepSeek建议的新阿尔法因子:
        - 评级上调
        - 盈利预测修正
        - 目标价空间
        """
        score = 5  # 基础分
        
        # 1. 评级变化 (4分)
        rating_change = data.get('rating_change', 0)  # 1=上调, 0=不变, -1=下调
        if rating_change == 1:
            score += 4
        elif rating_change == -1:
            score -= 3
        
        # 2. 盈利预测修正 (3分)
        eps_revision = data.get('eps_revision', 0)  # 盈利预测调整比例
        if eps_revision > 5:  # 上调5%以上
            score += 3
        elif eps_revision > 0:
            score += 1
        elif eps_revision < -5:
            score -= 2
        
        # 3. 目标价空间 (3分)
        target_price = data.get('target_price', 0)
        current_price = data.get('price', 0)
        if target_price > 0 and current_price > 0:
            upside = (target_price - current_price) / current_price * 100
            if upside > 20:
                score += 3
            elif upside > 10:
                score += 2
            elif upside < 0:
                score -= 2
        
        return min(10, max(0, score))
    
    # ==================== 总分计算 (优化版2.0) ====================
    
    def calculate_total_score(self, data: Dict, sectors: List[Dict], 
                             sector_momentum: Dict = None,
                             historical_prices: List[float] = None,
                             market_mode: str = 'neutral') -> Dict:
        """
        计算总分 - V11优化版2.0 (DeepSeek建议)
        
        简化模型 - 稳健多因子通用模型:
        - 技术: 20% (原)
        - 动量: 15% (新增)
        - 情绪: 8% (原)
        - 板块: 20% (从33%降低)
        - 资金: 14% (原)
        - 分析师: 10% (新增)
        - 风险: 13% (从15%微调)
        
        阈值: 50分
        """
        # 应用过滤条件
        passed, reason = self.apply_filters(data, historical_prices, market_mode)
        if not passed:
            return {
                'total': 0,
                'technical': 0,
                'momentum': 0,
                'sentiment': 0,
                'sector': 0,
                'money_flow': 0,
                'analyst': 0,
                'risk': 0,
                'threshold': self.THRESHOLD,
                'filtered': True,
                'filter_reason': reason
            }
        
        # 计算各因子 (简化稳健模型)
        tech = self.calculate_technical_score(data, historical_prices)
        momentum = self.calculate_momentum_score(data, historical_prices)
        sentiment = self.calculate_sentiment_score(data, historical_prices)
        sector = self.calculate_sector_score(data, sectors, sector_momentum)
        money = self.calculate_money_flow_score(data, historical_prices)
        analyst = self.calculate_analyst_score(data)
        risk = self.calculate_risk_score(data, historical_prices)
        
        # 新权重配置 (简化稳健模型)
        weights = {
            'technical': 0.20,
            'momentum': 0.15,
            'sentiment': 0.08,
            'sector': 0.20,  # 从33%降低
            'money_flow': 0.14,
            'analyst': 0.10,  # 新增
            'risk': 0.13
        }
        
        # 加权总分
        total = (tech * weights['technical'] +
                momentum * weights['momentum'] +
                sentiment * weights['sentiment'] +
                sector * weights['sector'] +
                money * weights['money_flow'] +
                analyst * weights['analyst'] +
                risk * weights['risk'])
        
        return {
            'total': round(total, 1),
            'technical': round(tech, 1),
            'momentum': round(momentum, 1),
            'sentiment': round(sentiment, 1),
            'sector': round(sector, 1),
            'money_flow': round(money, 1),
            'analyst': round(analyst, 1),
            'risk': round(risk, 1),
            'threshold': self.THRESHOLD,
            'filtered': False,
            'filter_reason': ''
        }
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """计算RSI"""
        if len(prices) < period + 1:
            return 50
        
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0 for d in deltas[-period:]]
        
        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0.001
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi


# 导出
premarket_screener = PremarketScreenerV11()
