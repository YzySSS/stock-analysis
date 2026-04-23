#!/usr/bin/env python3
"""
V12 - 全新多因子选股策略
========================
基于IC分析和DeepSeek建议设计的稳健因子模型

设计原则:
1. 每个因子都有明确的逻辑和预测能力
2. 避免过度拟合，因子间低相关性
3. 权重分配基于回测效果而非主观
4. 强过滤条件前置，减少选股噪音

因子配置:
- 趋势因子: 25% (MA20/MA60趋势)
- 动量因子: 25% (20日涨幅排名)
- 质量因子: 20% (波动率、流动性)
- 情绪因子: 20% (量价配合)
- 估值因子: 10% (PE分位数)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from typing import Dict, List, Optional
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class V12FactorModel:
    """V12 多因子评分模型"""
    
    def __init__(self):
        self.name = "V12_MultiFactor"
        self.version = "1.0"
        
        # 阈值设置
        self.score_threshold = 60  # 提高阈值，确保质量
        
        # 权重配置 (总计100%)
        self.weights = {
            'trend': 0.25,      # 趋势因子
            'momentum': 0.25,   # 动量因子
            'quality': 0.20,    # 质量因子
            'sentiment': 0.20,  # 情绪因子
            'value': 0.10       # 估值因子
        }
    
    # ==================== 硬性过滤条件 ====================
    
    def hard_filters(self, data: Dict, prices: List[float]) -> tuple:
        """
        硬性过滤 - 不符合条件的直接剔除
        前置强过滤，减少噪音
        """
        code = data.get('code', '')
        name = data.get('name', '')
        price = data.get('price', 0)
        turnover = data.get('turnover', 0)
        
        # 1. ST/退市/停牌过滤
        if 'ST' in name or '*ST' in name or '退' in name:
            return False, 'ST/退市股'
        
        # 2. 上市时间 (>180天，约9个月)
        listing_days = data.get('listing_days', 999)
        if listing_days < 180:
            return False, f'上市{listing_days}天<180天'
        
        # 3. 流动性过滤 (日均成交额>3亿)
        if turnover < 3e8:
            return False, f'成交额{turnover/1e8:.1f}亿<3亿'
        
        # 4. 股价过滤 (10-150元)
        if price < 10:
            return False, f'股价{price}<10元'
        if price > 150:
            return False, f'股价{price}>150元'
        
        # 5. 趋势硬性条件 (必须在MA20之上)
        if len(prices) >= 20:
            ma20 = sum(prices[-20:]) / 20
            if price < ma20 * 0.98:  # 允许2%误差
                return False, f'股价{price:.2f}<MA20({ma20:.2f})'
        else:
            return False, '数据不足20天'
        
        # 6. 避免连续跌停股 (近5日跌幅<15%)
        if len(prices) >= 5:
            ret_5d = (prices[-1] - prices[-5]) / prices[-5] * 100
            if ret_5d < -15:
                return False, f'近5日跌幅{ret_5d:.1f}%<-15%'
        
        return True, '通过'
    
    # ==================== 阿尔法因子 ====================
    
    def trend_factor(self, data: Dict, prices: List[float]) -> float:
        """
        趋势因子 (0-25分)
        
        逻辑: 顺势而为，趋势向上的股票更容易继续上涨
        """
        if len(prices) < 60:
            return 12.5  # 默认中分
        
        score = 12.5  # 基础分
        current_price = prices[-1]
        
        # 1. MA20趋势 (5分)
        ma20 = sum(prices[-20:]) / 20
        ma20_prev = sum(prices[-21:-1]) / 20
        if ma20 > ma20_prev:  # MA20向上
            score += 5
        
        # 2. MA60趋势 (5分)
        ma60 = sum(prices[-60:]) / 60
        ma60_prev = sum(prices[-61:-1]) / 60
        if ma60 > ma60_prev:  # MA60向上
            score += 5
        
        # 3. 均线多头排列 (5分)
        ma5 = sum(prices[-5:]) / 5
        ma10 = sum(prices[-10:]) / 10
        if ma5 > ma10 > ma20 > ma60:
            score += 5  # 完美多头排列
        elif ma20 > ma60:
            score += 2  # 中期多头
        
        # 4. 价格相对位置 (2.5分)
        high_60 = max(prices[-60:])
        low_60 = min(prices[-60:])
        if high_60 > low_60:
            position = (current_price - low_60) / (high_60 - low_60)
            # 30%-70%区间最佳（脱离底部但未过热）
            if 0.3 <= position <= 0.7:
                score += 2.5
            elif position > 0.9:
                score -= 2.5  # 接近新高，追高风险
        
        return min(25, max(0, score))
    
    def momentum_factor(self, data: Dict, prices: List[float]) -> float:
        """
        动量因子 (0-25分)
        
        逻辑: 过去表现好的股票短期内更可能继续表现
        使用20日涨幅，但排除过度炒作(>40%)
        """
        if len(prices) < 20:
            return 12.5
        
        score = 12.5
        
        # 1. 20日涨幅 (15分)
        ret_20d = (prices[-1] - prices[-20]) / prices[-20] * 100
        
        # 最佳区间: 5% - 30%
        if 5 <= ret_20d <= 30:
            score += 15
        elif 0 <= ret_20d < 5:
            score += 5  # 有动量但较弱
        elif 30 < ret_20d <= 40:
            score += 8  # 动量强但可能过热
        elif ret_20d > 40:
            score -= 10  # 过度炒作，回调风险大
        elif -10 <= ret_20d < 0:
            score -= 5  # 弱势
        else:
            score -= 15  # 严重弱势
        
        # 2. 5日/10日加速度 (5分)
        if len(prices) >= 10:
            ret_5d = (prices[-1] - prices[-5]) / prices[-5] * 100
            ret_10d = (prices[-1] - prices[-10]) / prices[-10] * 100
            
            # 近期加速
            if ret_5d > ret_10d * 0.6:  # 5日涨幅接近10日涨幅的60%
                score += 5
            elif ret_5d < 0:
                score -= 5  # 近期回调
        
        # 3. 量价齐升 (5分)
        # TODO: 需要成交量数据
        # 简化版：涨幅适中时加分
        if 2 <= ret_20d <= 15:
            score += 5
        
        return min(25, max(0, score))
    
    def quality_factor(self, data: Dict, prices: List[float]) -> float:
        """
        质量因子 (0-20分)
        
        逻辑: 低波动、低回撤的股票质量更高
        """
        if len(prices) < 20:
            return 10
        
        score = 10
        
        # 1. 20日波动率 (10分)
        returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100 
                   for i in range(-19, 0)]
        volatility = np.std(returns) if returns else 0
        
        # 低波动加分 (年化<30%，即日波动<2%)
        if volatility < 2:
            score += 10
        elif volatility < 3:
            score += 5
        elif volatility > 5:
            score -= 5  # 高波动减分
        
        # 2. 最大回撤 (5分)
        recent_high = max(prices[-20:])
        current = prices[-1]
        drawdown = (recent_high - current) / recent_high * 100
        
        if drawdown < 5:
            score += 5
        elif drawdown < 10:
            score += 2
        elif drawdown > 20:
            score -= 5  # 深度回撤
        
        # 3. 连续上涨天数 (5分)
        up_days = sum(1 for r in returns if r > 0)
        if up_days >= 12:  # 60%以上交易日上涨
            score += 5
        elif up_days <= 6:  # 30%以下
            score -= 5
        
        return min(20, max(0, score))
    
    def sentiment_factor(self, data: Dict, prices: List[float]) -> float:
        """
        情绪因子 (0-20分)
        
        逻辑: 量价配合，资金关注度
        """
        score = 10
        
        # 1. 昨日涨幅与量能配合 (10分)
        change_pct = data.get('change_pct', 0)
        turnover = data.get('turnover', 0)
        
        # 温和上涨+放量 = 健康
        if 2 <= change_pct <= 7 and turnover > 5e8:
            score += 10
        elif 0 <= change_pct < 2 and turnover > 3e8:
            score += 5
        elif change_pct > 9.5:
            score -= 5  # 涨停次日风险
        elif change_pct < -3:
            score -= 10  # 大跌
        
        # 2. 近期人气 (5分)
        # 近5日在60日高位附近
        if len(prices) >= 60:
            high_5d = max(prices[-5:])
            high_60d = max(prices[-60:])
            if high_5d >= high_60d * 0.95:  # 近5日接近60日新高
                score += 5
        
        # 3. 资金流向 (5分) - 需要外部数据
        # 简化版：成交额相对自身历史
        # TODO: 接入主力资金数据
        
        return min(20, max(0, score))
    
    def value_factor(self, data: Dict) -> float:
        """
        估值因子 (0-10分)
        
        逻辑: 避免极端高估股票
        """
        score = 5
        
        # PE分位数 (如果数据可用)
        pe_ratio = data.get('pe_ratio', 0)
        pe_percentile = data.get('pe_percentile', 50)
        
        if pe_ratio > 0:
            # PE适中最佳 (10-40倍)
            if 10 <= pe_ratio <= 40:
                score += 5
            elif pe_ratio > 100:
                score -= 5  # 过高估值
            elif pe_ratio < 0:
                score -= 3  # 亏损股
        
        # PB分位数
        pb_ratio = data.get('pb_ratio', 0)
        if pb_ratio > 10:
            score -= 3
        
        return min(10, max(0, score))
    
    # ==================== 总分计算 ====================
    
    def calculate_score(self, data: Dict, historical_prices: List[float]) -> Dict:
        """
        计算综合评分
        
        Returns:
            {
                'total': 总分(0-100),
                'passed': 是否通过硬过滤,
                'filter_reason': 过滤原因,
                'factors': {各因子得分},
                'weights': {各因子权重}
            }
        """
        # 1. 硬性过滤
        passed, reason = self.hard_filters(data, historical_prices)
        if not passed:
            return {
                'total': 0,
                'passed': False,
                'filter_reason': reason,
                'factors': {},
                'weights': self.weights
            }
        
        # 2. 计算各因子得分
        factors = {
            'trend': self.trend_factor(data, historical_prices),
            'momentum': self.momentum_factor(data, historical_prices),
            'quality': self.quality_factor(data, historical_prices),
            'sentiment': self.sentiment_factor(data, historical_prices),
            'value': self.value_factor(data)
        }
        
        # 3. 加权总分
        total = sum(factors[k] * self.weights[k] for k in factors)
        
        return {
            'total': round(total, 1),
            'passed': True,
            'filter_reason': '',
            'factors': factors,
            'weights': self.weights
        }
    
    def select_stocks(self, stocks_data: List[Dict], top_n: int = 3) -> List[Dict]:
        """
        选股主函数
        
        Args:
            stocks_data: 全市场股票数据列表
            top_n: 选股数量
            
        Returns:
            选中的股票列表
        """
        scored_stocks = []
        
        for data in stocks_data:
            prices = data.get('historical_prices', [])
            if len(prices) < 60:
                continue
            
            result = self.calculate_score(data, prices)
            
            if result['passed'] and result['total'] >= self.score_threshold:
                scored_stocks.append({
                    'code': data['code'],
                    'name': data['name'],
                    'score': result['total'],
                    'factors': result['factors'],
                    'price': data['price']
                })
        
        # 按得分排序，取前N
        scored_stocks.sort(key=lambda x: x['score'], reverse=True)
        
        return scored_stocks[:top_n]


# 导出
v12_model = V12FactorModel()


if __name__ == "__main__":
    # 测试
    model = V12FactorModel()
    
    # 模拟数据
    test_data = {
        'code': '000001',
        'name': '平安银行',
        'price': 12.5,
        'turnover': 5e8,
        'change_pct': 3.2,
        'listing_days': 1000,
        'historical_prices': [10 + i * 0.05 + np.random.randn() * 0.1 for i in range(100)]
    }
    
    result = model.calculate_score(test_data, test_data['historical_prices'])
    
    print("V12因子评分测试结果:")
    print(f"通过过滤: {result['passed']}")
    print(f"总分: {result['total']}")
    if result['passed']:
        print("各因子得分:")
        for factor, score in result['factors'].items():
            weight = result['weights'][factor]
            weighted = score * weight
            print(f"  {factor}: {score:.1f} × {weight} = {weighted:.2f}")
    else:
        print(f"过滤原因: {result['filter_reason']}")
