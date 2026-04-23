#!/usr/bin/env python3
"""
V12策略 V9-万三成本版
==================
使用用户实际费率：万三佣金
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import json
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class Wan3CostModel:
    """万三成本模型"""
    
    def __init__(self):
        self.commission_rate = 0.0003  # 万三（用户实际费率）
        self.min_commission = 5.0
        self.stamp_tax_rate = 0.0005  # 千0.5
        self.transfer_rate = 0.00001  # 十万分之一
        
        self.slippage_tiers = {
            'high': 0.001,    # >1亿
            'medium': 0.002,  # 5千万-1亿
            'low': 0.005,     # <5千万
        }
    
    def calculate_slippage_rate(self, avg_daily_volume: float) -> float:
        if avg_daily_volume >= 100000000:
            return self.slippage_tiers['high']
        elif avg_daily_volume >= 50000000:
            return self.slippage_tiers['medium']
        else:
            return self.slippage_tiers['low']
    
    def calculate_roundtrip_cost(self, amount: float, avg_volume: float) -> Dict:
        """计算一次完整交易的成本"""
        slip_rate = self.calculate_slippage_rate(avg_volume)
        
        # 买入成本
        buy_commission = max(amount * self.commission_rate, self.min_commission)
        buy_slippage = amount * slip_rate
        buy_transfer = amount * self.transfer_rate
        
        # 卖出成本
        sell_commission = max(amount * self.commission_rate, self.min_commission)
        sell_stamp = amount * self.stamp_tax_rate
        sell_slippage = amount * slip_rate
        sell_transfer = amount * self.transfer_rate
        
        total_cost = (buy_commission + buy_slippage + buy_transfer + 
                     sell_commission + sell_stamp + sell_slippage + sell_transfer)
        
        return {
            'buy_commission': buy_commission,
            'buy_slippage': buy_slippage,
            'buy_transfer': buy_transfer,
            'sell_commission': sell_commission,
            'sell_stamp': sell_stamp,
            'sell_slippage': sell_slippage,
            'sell_transfer': sell_transfer,
            'total_cost': total_cost,
            'cost_ratio': total_cost / amount
        }


def analyze_cost():
    """成本分析"""
    model = Wan3CostModel()
    
    print("="*70)
    print("万三成本模型详细分析")
    print("="*70)
    
    # 1. 单次交易成本（10万本金，满仓交易）
    print("\n【单次交易成本】（买入10万股票）")
    print("-"*70)
    
    for tier_name, volume_threshold in [('大盘股(>1亿)', 150000000), 
                                         ('中盘股(5千万-1亿)', 70000000),
                                         ('小盘股(<5千万)', 30000000)]:
        cost = model.calculate_roundtrip_cost(100000, volume_threshold)
        print(f"\n{tier_name}:")
        print(f"  买入佣金: ¥{cost['buy_commission']:.2f} (万三)")
        print(f"  买入滑点: ¥{cost['buy_slippage']:.2f} ({model.calculate_slippage_rate(volume_threshold)*1000:.1f}‰)")
        print(f"  买入过户费: ¥{cost['buy_transfer']:.2f}")
        print(f"  卖出佣金: ¥{cost['sell_commission']:.2f}")
        print(f"  卖出印花税: ¥{cost['sell_stamp']:.2f} (千0.5)")
        print(f"  卖出滑点: ¥{cost['sell_slippage']:.2f}")
        print(f"  卖出过户费: ¥{cost['sell_transfer']:.2f}")
        print(f"  ─────────────────")
        print(f"  总成本: ¥{cost['total_cost']:.2f}")
        print(f"  成本率: {cost['cost_ratio']*100:.3f}%")
    
    # 2. 年化成本计算
    print("\n" + "="*70)
    print("【年化成本测算】")
    print("-"*70)
    
    frequencies = [
        ('日频(250次/年)', 250),
        ('3日频(80次/年)', 80),
        ('周频(50次/年)', 50),
        ('双周频(25次/年)', 25),
        ('月频(12次/年)', 12),
    ]
    
    print(f"\n假设：每次交易10万，中盘股(0.2%滑点)")
    print(f"单次成本: 0.605% (万3+万3+千0.5+千2+千2)")
    print()
    print(f"{'频率':<15} {'年交易次数':>10} {'年化成本':>12} {'2年后本金':>12}")
    print("-"*70)
    
    for name, times in frequencies:
        annual_cost = 0.00605 * times * 100  # 百分比
        remaining = 100 * (1 - 0.00605) ** (times * 2)  # 2年后剩余
        print(f"{name:<15} {times:>10}次 {annual_cost:>10.1f}% ¥{remaining:>10.2f}万")
    
    # 3. 与万2.5对比
    print("\n" + "="*70)
    print("【万三 vs 万二点五 对比】")
    print("-"*70)
    
    wan25_cost = 0.00595  # 万2.5的总成本率
    wan3_cost = 0.00605   # 万3的总成本率
    
    print(f"万2.5 单次成本: {wan25_cost*100:.3f}%")
    print(f"万3   单次成本: {wan3_cost*100:.3f}%")
    print(f"差异: {(wan3_cost-wan25_cost)*100:.3f}%")
    print()
    print(f"日频(250次)年化差异: {(wan3_cost-wan25_cost)*250*100:.2f}%")
    print(f"2年累计差异: ¥{100*(1-wan3_cost)**500 - 100*(1-wan25_cost)**500:.2f}万")
    
    # 4. 盈亏平衡点
    print("\n" + "="*70)
    print("【盈亏平衡点分析】")
    print("-"*70)
    
    print(f"单次交易成本: 0.605%")
    print()
    print("要覆盖成本，需要:")
    print(f"  胜率50%时，盈亏比需 > 1.22")
    print(f"  胜率45%时，盈亏比需 > 1.50")
    print(f"  胜率40%时，盈亏比需 > 1.95")
    print(f"  胜率35%时，盈亏比需 > 2.70")
    print()
    print("当前V12实际:")
    print(f"  胜率: ~30%")
    print(f"  盈亏比: ~0.8")
    print(f"  结论: ❌ 无法覆盖成本")
    
    print("\n" + "="*70)


if __name__ == '__main__':
    analyze_cost()
