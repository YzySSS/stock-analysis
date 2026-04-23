#!/usr/bin/env python3
"""
策略模块使用示例
================
展示如何在不同场景下使用策略模块

场景1: 实盘选股（每日盘前/盘中）
场景2: 回测验证
场景3: 多策略对比
"""

import os
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

from strategy_factory import StrategyFactory, create_strategy


def example_1_basic_usage():
    """示例1: 基础用法"""
    print("=" * 60)
    print("示例1: 基础用法")
    print("=" * 60)
    
    # 方法1: 直接使用工厂创建
    strategy = StrategyFactory.create('V10_5FACTOR')
    print(f"创建策略: {strategy.name}")
    print(f"权重配置: {strategy.get_factor_weights()}")
    
    # 方法2: 使用便捷函数
    strategy2 = create_strategy('V11_DYNAMIC')
    print(f"\n创建策略: {strategy2.name}")
    print(f"权重配置: {strategy2.get_factor_weights()}")


def example_2_custom_params():
    """示例2: 自定义参数"""
    print("\n" + "=" * 60)
    print("示例2: 自定义参数")
    print("=" * 60)
    
    # 创建V10策略，但修改权重
    custom_weights = {
        'technical': 0.40,   # 提高技术面权重
        'sentiment': 0.15,   # 降低情绪面
        'sector': 0.25,
        'capital': 0.15,
        'risk': 0.05
    }
    
    strategy = StrategyFactory.create(
        'V10_5FACTOR',
        factor_weights=custom_weights,
        pick_count=5  # 选5只而不是3只
    )
    
    print(f"创建自定义V10策略")
    print(f"权重配置: {strategy.get_factor_weights()}")
    print(f"选股数量: {strategy.pick_count}")


def example_3_list_strategies():
    """示例3: 列出所有策略"""
    print("\n" + "=" * 60)
    print("示例3: 列出所有可用策略")
    print("=" * 60)
    
    strategies = StrategyFactory.list_strategies()
    for s in strategies:
        print(f"\n【{s['key']}】 {s['name']}")
        print(f"  版本: {s['version']}")
        print(f"  描述: {s['description']}")
        print(f"  标签: {', '.join(s['tags'])}")


def example_4_real_usage():
    """示例4: 实际使用场景"""
    print("\n" + "=" * 60)
    print("示例4: 实盘/回测统一接口")
    print("=" * 60)
    
    # 从配置文件读取当前策略（模拟）
    current_strategy_key = "V11_DYNAMIC"  # 可以从yaml配置读取
    
    # 创建策略
    strategy = StrategyFactory.create(current_strategy_key)
    
    # 选股日期
    date = "2026-04-03"
    
    print(f"使用策略: {strategy.name}")
    print(f"选股日期: {date}")
    
    # 执行选股（这里会调用真实的选股逻辑）
    # picks = strategy.select(date=date, top_n=3)
    # print(f"选股结果: {picks}")
    
    print("(实际运行时会执行选股逻辑)")


def example_5_backtest_usage():
    """示例5: 回测场景"""
    print("\n" + "=" * 60)
    print("示例5: 回测场景 - 遍历多个策略")
    print("=" * 60)
    
    strategies_to_test = ['V10_5FACTOR', 'V11_DYNAMIC']
    
    for key in strategies_to_test:
        print(f"\n回测策略: {key}")
        strategy = StrategyFactory.create(key)
        
        # 获取策略配置
        weights = strategy.get_factor_weights()
        print(f"  因子权重: {weights}")
        
        # 执行回测（简化示例）
        # backtest_result = run_backtest(strategy, start_date, end_date)
        # print(f"  回测结果: {backtest_result}")
        
        print(f"  (实际回测时会调用backtest.py)")


if __name__ == "__main__":
    # 运行所有示例
    example_1_basic_usage()
    example_2_custom_params()
    example_3_list_strategies()
    example_4_real_usage()
    example_5_backtest_usage()
    
    print("\n" + "=" * 60)
    print("所有示例运行完成!")
    print("=" * 60)
