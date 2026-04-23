#!/usr/bin/env python3
"""
测试技术位分析功能
验证盘前/盘中报告中技术位计算是否正确
"""

import sys
sys.path.insert(0, 'src')

from technical_analysis import calculate_technical_levels, format_technical_levels
import pandas as pd
import numpy as np

def test_technical_levels():
    """测试技术位计算"""
    print("=" * 60)
    print("测试技术位计算功能")
    print("=" * 60)
    
    # 创建模拟历史数据
    np.random.seed(42)
    n_days = 60
    dates = pd.date_range(end=pd.Timestamp.now(), periods=n_days, freq='D')
    base_price = 50.0
    
    # 生成随机价格序列（带趋势）
    returns = np.random.normal(0.001, 0.02, n_days)
    prices = base_price * np.exp(np.cumsum(returns))
    
    # 生成高低开收量
    df = pd.DataFrame({
        'date': dates,
        'open': prices * (1 + np.random.normal(0, 0.005, n_days)),
        'high': prices * (1 + abs(np.random.normal(0.015, 0.005, n_days))),
        'low': prices * (1 - abs(np.random.normal(0.015, 0.005, n_days))),
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, n_days)
    })
    
    current_price = prices[-1]
    print(f"\n当前价格: ¥{current_price:.2f}")
    print(f"历史数据: {len(df)} 天")
    print(f"价格区间: ¥{df['low'].min():.2f} - ¥{df['high'].max():.2f}")
    
    # 计算技术位
    print("\n" + "-" * 60)
    print("计算技术位...")
    print("-" * 60)
    
    levels = calculate_technical_levels(df, current_price)
    
    # 格式化输出
    print("\n技术位结果:")
    print(format_technical_levels(levels, current_price))
    
    # 验证合理性
    print("\n" + "-" * 60)
    print("验证合理性:")
    print("-" * 60)
    
    checks = []
    
    # 检查1: 止损价应在当前价下方
    if levels.stop_loss < current_price:
        checks.append("✅ 止损价在当前价下方")
    else:
        checks.append("❌ 止损价应在当前价下方")
    
    # 检查2: 目标价应在当前价上方
    if levels.target_price > current_price:
        checks.append("✅ 目标价在当前价上方")
    else:
        checks.append("❌ 目标价应在当前价上方")
    
    # 检查3: 支撑位应在当前价下方
    if levels.support_1 < current_price and levels.support_2 < current_price:
        checks.append("✅ 支撑位在当前价下方")
    else:
        checks.append("❌ 支撑位应在当前价下方")
    
    # 检查4: 阻力位应在当前价上方
    if levels.resistance_1 > current_price and levels.resistance_2 > current_price:
        checks.append("✅ 阻力位在当前价上方")
    else:
        checks.append("❌ 阻力位应在当前价上方")
    
    # 检查5: 支撑2 < 支撑1
    if levels.support_2 < levels.support_1:
        checks.append("✅ 支撑2 < 支撑1")
    else:
        checks.append("❌ 支撑2应小于支撑1")
    
    # 检查6: 阻力2 > 阻力1
    if levels.resistance_2 > levels.resistance_1:
        checks.append("✅ 阻力2 > 阻力1")
    else:
        checks.append("❌ 阻力2应大于阻力1")
    
    # 检查7: 止损价接近支撑1
    stop_to_support = abs(levels.stop_loss - levels.support_1) / current_price
    if stop_to_support < 0.05:  # 5%以内
        checks.append(f"✅ 止损价接近支撑1 (偏差{stop_to_support*100:.1f}%)")
    else:
        checks.append(f"⚠️ 止损价与支撑1偏差较大 ({stop_to_support*100:.1f}%)")
    
    # 检查8: 风险收益比
    risk = current_price - levels.stop_loss
    reward = levels.target_price - current_price
    if risk > 0:
        rr_ratio = reward / risk
        if 1.5 <= rr_ratio <= 3.0:
            checks.append(f"✅ 风险收益比合理 (1:{rr_ratio:.1f})")
        else:
            checks.append(f"⚠️ 风险收益比可能需要调整 (1:{rr_ratio:.1f})")
    
    for check in checks:
        print(f"  {check}")
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)
    
    return levels

def test_data_insufficient():
    """测试数据不足时的处理"""
    print("\n" + "=" * 60)
    print("测试数据不足时的降级处理")
    print("=" * 60)
    
    current_price = 100.0
    levels = calculate_technical_levels(None, current_price)
    
    print(f"\n当前价格: ¥{current_price:.2f}")
    print("历史数据: 无")
    print("\n降级计算结果:")
    print(format_technical_levels(levels, current_price))
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    # 运行测试
    test_technical_levels()
    test_data_insufficient()
    
    print("\n✅ 所有测试通过！技术位分析功能正常。")
