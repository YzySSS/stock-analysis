#!/usr/bin/env python3
"""
策略基类
========
所有选股策略的抽象基类
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime


class BaseStrategy(ABC):
    """选股策略基类"""
    
    def __init__(self):
        self.name = "BaseStrategy"
        self.version = "1.0"
        self.factor_weights = {}
    
    @abstractmethod
    def select(self, date: str, top_n: int = 3) -> List[Dict]:
        """
        选股主方法
        
        Args:
            date: 选股日期 (YYYY-MM-DD)
            top_n: 选股数量（默认3只）
        
        Returns:
            选股结果列表 [{code, name, score, factors, tech_levels}]
        """
        pass
    
    @abstractmethod
    def get_factor_weights(self) -> Dict[str, float]:
        """获取因子权重配置"""
        pass
    
    def validate_picks(self, picks: List[Dict]) -> List[Dict]:
        """
        验证选股结果
        - 过滤ST/退市股
        - 过滤次新股（上市<60天）
        - 过滤北交所
        """
        validated = []
        for pick in picks:
            # 基础验证
            if not self._is_tradable(pick):
                continue
            validated.append(pick)
        return validated
    
    def _is_tradable(self, stock: Dict) -> bool:
        """检查股票是否可交易"""
        # 基础检查，子类可覆盖
        return True
    
    def calculate_score(self, stock_data: Dict) -> float:
        """
        计算股票综合得分
        子类需要实现具体的打分逻辑
        """
        return 0.0
    
    def format_output(self, picks: List[Dict]) -> List[Dict]:
        """
        格式化输出
        确保每个选股结果包含必要的字段
        """
        formatted = []
        for pick in picks:
            formatted_pick = {
                'code': pick.get('code', ''),
                'name': pick.get('name', ''),
                'score': pick.get('score', 0),
                'factors': pick.get('factors', {}),
                'tech_levels': pick.get('tech_levels', {}),
                'reason': pick.get('reason', '')
            }
            formatted.append(formatted_pick)
        return formatted
