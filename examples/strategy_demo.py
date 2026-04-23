#!/usr/bin/env python3
"""
策略模块使用演示
================
展示模块化策略设计的优势

特点:
1. 配置化切换策略
2. 统一接口
3. 支持自定义参数
4. 方便回测和实盘复用
"""

import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

# 导入策略工厂
from strategy_factory import StrategyFactory, create_strategy, list_available_strategies


def demo_1_list_strategies():
    """演示1: 列出所有可用策略"""
    print("=" * 60)
    print("📋 所有可用策略")
    print("=" * 60)
    
    strategies = list_available_strategies()
    for s in strategies:
        print(f"\n【{s['key']}】 {s['name']}")
        print(f"  版本: {s['version']}")
        print(f"  描述: {s['description']}")


def demo_2_create_strategy():
    """演示2: 创建策略实例"""
    print("\n" + "=" * 60)
    print("🔧 创建策略实例")
    print("=" * 60)
    
    # 方式1: 使用工厂
    strategy_a = StrategyFactory.create('V10_5FACTOR')
    print(f"\n✅ 创建 V10_5FACTOR")
    print(f"   名称: {strategy_a.name}")
    print(f"   权重: {strategy_a.get_factor_weights()}")
    
    # 方式2: 使用便捷函数
    strategy_b = create_strategy('V11_DYNAMIC')
    print(f"\n✅ 创建 V11_DYNAMIC")
    print(f"   名称: {strategy_b.name}")
    print(f"   权重: {strategy_b.get_factor_weights()}")


def demo_3_custom_params():
    """演示3: 自定义策略参数"""
    print("\n" + "=" * 60)
    print("⚙️  自定义策略参数")
    print("=" * 60)
    
    # 自定义权重
    custom_weights = {
        'technical': 0.40,
        'sentiment': 0.10,
        'sector': 0.30,
        'capital': 0.15,
        'risk': 0.05
    }
    
    strategy = StrategyFactory.create(
        'V10_5FACTOR',
        factor_weights=custom_weights,
        pick_count=5
    )
    
    print(f"\n✅ 创建自定义V10策略")
    print(f"   选股数量: {strategy.pick_count}")
    print(f"   自定义权重:")
    for factor, weight in strategy.get_factor_weights().items():
        print(f"     - {factor}: {weight*100:.0f}%")


def demo_4_usage_pattern():
    """演示4: 实际使用模式"""
    print("\n" + "=" * 60)
    print("🎯 实际使用模式")
    print("=" * 60)
    
    # 场景1: 实盘选股
    print("\n【场景1: 实盘选股】")
    current_strategy = "V11_DYNAMIC"  # 从配置文件读取
    
    strategy = create_strategy(current_strategy)
    print(f"   当前策略: {strategy.name}")
    print(f"   执行选股: strategy.select(date='2026-04-03')")
    # picks = strategy.select(date='2026-04-03')
    
    # 场景2: 回测
    print("\n【场景2: 回测验证】")
    strategies_to_test = ['V10_5FACTOR', 'V11_DYNAMIC']
    
    for key in strategies_to_test:
        s = create_strategy(key)
        print(f"   回测 {key}: {s.name}")
        # run_backtest(s, start='2026-03-01', end='2026-04-03')


def demo_5_config_driven():
    """演示5: 配置驱动"""
    print("\n" + "=" * 60)
    print("📄 配置驱动示例")
    print("=" * 60)
    
    # 模拟从YAML配置读取
    config = {
        'CURRENT_STRATEGY': 'V11_DYNAMIC',
        'STRATEGIES': {
            'V11_DYNAMIC': {
                'pick_count': 3,
                'base_weights': {
                    'technical': 0.35,
                    'sentiment': 0.20,
                    'sector': 0.30,
                    'capital': 0.15,
                    'risk': 0.10
                }
            }
        }
    }
    
    strategy_key = config['CURRENT_STRATEGY']
    strategy_config = config['STRATEGIES'][strategy_key]
    
    strategy = StrategyFactory.create(
        strategy_key,
        **strategy_config
    )
    
    print(f"\n✅ 从配置创建策略")
    print(f"   策略: {strategy_key}")
    print(f"   名称: {strategy.name}")
    print(f"   配置参数已应用")


if __name__ == "__main__":
    print("\n" + "🚀" * 30)
    print("  策略模块使用演示")
    print("🚀" * 30 + "\n")
    
    demo_1_list_strategies()
    demo_2_create_strategy()
    demo_3_custom_params()
    demo_4_usage_pattern()
    demo_5_config_driven()
    
    print("\n" + "=" * 60)
    print("✅ 所有演示完成!")
    print("=" * 60)
    print("\n💡 总结:")
    print("   • 通过 StrategyFactory 统一管理策略")
    print("   • 支持配置化切换，无需修改代码")
    print("   • 回测和实盘使用相同接口")
    print("   • 方便添加新策略（热插拔）")
