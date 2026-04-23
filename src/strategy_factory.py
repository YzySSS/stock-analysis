#!/usr/bin/env python3
"""
策略工厂
========
统一管理所有选股策略的创建和配置

Usage:
    from strategy_factory import StrategyFactory
    
    # 通过配置创建策略
    strategy = StrategyFactory.create('V10_5FACTOR')
    
    # 列出所有可用策略
    strategies = StrategyFactory.list_strategies()
    
    # 从数据库配置创建
    strategy = StrategyFactory.create_from_db('V10_5FACTOR')
"""

import os
import sys
import json
import importlib
from typing import Dict, List, Optional, Type

# 添加src路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from strategies.base import BaseStrategy


class StrategyFactory:
    """策略工厂"""
    
    # 策略注册表
    _strategies = {}
    
    @classmethod
    def register(cls, key: str, strategy_class: Type[BaseStrategy]):
        """注册策略"""
        cls._strategies[key] = strategy_class
    
    @classmethod
    def create(cls, key: str, **kwargs) -> Optional[BaseStrategy]:
        """
        创建策略实例
        
        Args:
            key: 策略标识 (如 'V10_5FACTOR')
            **kwargs: 策略参数（覆盖默认配置）
        
        Returns:
            策略实例
        """
        # 动态导入策略模块（支持热插拔）
        if key not in cls._strategies:
            try:
                cls._load_strategy(key)
            except Exception as e:
                print(f"加载策略 {key} 失败: {e}")
                return None
        
        if key not in cls._strategies:
            raise ValueError(f"未知策略: {key}。可用策略: {list(cls._strategies.keys())}")
        
        strategy_class = cls._strategies[key]
        
        # 从数据库加载配置（如果有）
        db_config = cls._load_config_from_db(key)
        if db_config:
            kwargs.update(db_config)
        
        return strategy_class(**kwargs)
    
    @classmethod
    def _load_strategy(cls, key: str):
        """动态加载策略模块"""
        # 策略模块映射
        module_map = {
            'V10_5FACTOR': 'strategies.v10_5factor',
            'V11_DYNAMIC': 'strategies.v11_dynamic',
            'V12': 'strategies.v12_strategy'
        }
        
        if key not in module_map:
            return
        
        # 动态导入
        module_path = module_map[key]
        module = importlib.import_module(module_path)
        
        # 获取策略类
        # V10_5FACTOR -> V10_5FactorStrategy
        # V11_DYNAMIC -> V11_DynamicStrategy
        if key == 'V10_5FACTOR':
            class_name = 'V10_5FactorStrategy'
        elif key == 'V11_DYNAMIC':
            class_name = 'V11_DynamicStrategy'
        elif key == 'V12':
            class_name = 'V12Strategy'
        else:
            class_name = key.replace('_', '') + 'Strategy'
        
        if hasattr(module, class_name):
            strategy_class = getattr(module, class_name)
        else:
            # 尝试常见的类名
            for attr_name in dir(module):
                if not attr_name.startswith('_') and attr_name.endswith('Strategy'):
                    strategy_class = getattr(module, attr_name)
                    break
            else:
                raise ImportError(f"模块 {module_path} 中没有找到策略类 {class_name}")
        
        cls.register(key, strategy_class)
    
    @classmethod
    def _load_config_from_db(cls, key: str) -> Optional[Dict]:
        """从数据库加载策略配置"""
        try:
            import pymysql
            from config import DB_CONFIG
            
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT parameters FROM strategies 
                    WHERE strategy_key = %s AND is_active = 1
                ''', (key,))
                row = cursor.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
            conn.close()
        except Exception:
            pass
        return None
    
    @classmethod
    def list_strategies(cls) -> List[Dict]:
        """列出所有可用策略"""
        # 预置策略
        preset_strategies = [
            {
                'key': 'V10_5FACTOR',
                'name': 'V10 5因子固定权重',
                'version': '1.0',
                'description': '经典5因子策略，固定权重配置',
                'tags': ['稳定', '经典']
            },
            {
                'key': 'V11_DYNAMIC',
                'name': 'V11 动态权重版',
                'version': '2.0',
                'description': '根据市场强弱动态调整因子权重',
                'tags': ['智能', '动态调整']
            }
        ]
        
        # 合并已注册的策略
        for key in cls._strategies:
            if not any(s['key'] == key for s in preset_strategies):
                preset_strategies.append({
                    'key': key,
                    'name': key,
                    'version': 'unknown',
                    'description': '动态加载的策略',
                    'tags': ['custom']
                })
        
        return preset_strategies
    
    @classmethod
    def get_strategy_info(cls, key: str) -> Optional[Dict]:
        """获取策略详细信息"""
        strategies = cls.list_strategies()
        for s in strategies:
            if s['key'] == key:
                return s
        return None


# 便捷函数
def create_strategy(strategy_key: str, **kwargs) -> BaseStrategy:
    """创建策略的便捷函数"""
    return StrategyFactory.create(strategy_key, **kwargs)


def list_available_strategies() -> List[Dict]:
    """列出所有可用策略"""
    return StrategyFactory.list_strategies()


if __name__ == "__main__":
    # 测试
    print("可用策略列表:")
    for s in StrategyFactory.list_strategies():
        print(f"  • {s['key']}: {s['name']} ({s['version']})")
    
    print("\n测试创建策略:")
    strategy = StrategyFactory.create('V10_5FACTOR')
    print(f"  创建成功: {strategy.name}")
    print(f"  权重配置: {strategy.get_factor_weights()}")
