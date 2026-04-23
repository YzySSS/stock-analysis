#!/usr/bin/env python3
"""
持仓配置 - 真实持仓（2026-03-17更新）
"""

from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Position:
    code: str  # 股票代码
    name: str  # 股票名称
    cost_price: float  # 成本价
    quantity: int = 0  # 持仓数量
    current_price: Optional[float] = None  # 现价（可选）

# 当前持仓（7只）- 2026-03-17更新
POSITIONS = [
    # 黄金ETF
    Position(code='159937', name='黄金9999', cost_price=10.877, quantity=5500, current_price=10.635),
    
    # 双创AI
    Position(code='159142', name='双创AI', cost_price=1.158, quantity=44800, current_price=1.061),
    
    # 银行ETF
    Position(code='159887', name='银行ETF', cost_price=1.276, quantity=30900, current_price=1.298),
    
    # 锂电池ETF
    Position(code='561160', name='锂电池ETF', cost_price=0.833, quantity=45000, current_price=0.881),
    
    # 顺丰控股
    Position(code='002352', name='顺丰控股', cost_price=37.633, quantity=1000, current_price=37.700),
    
    # 比亚迪
    Position(code='002594', name='比亚迪', cost_price=94.957, quantity=300, current_price=104.620),
    
    # 电力ETF
    Position(code='159611', name='电力ETF', cost_price=1.183, quantity=19000, current_price=1.147),
]


def get_all_positions() -> List[Position]:
    """获取所有持仓"""
    return POSITIONS


def get_position_by_code(code: str) -> Optional[Position]:
    """根据代码获取持仓"""
    for p in POSITIONS:
        if p.code == code:
            return p
    return None


def get_position_by_name(name: str) -> Optional[Position]:
    """根据名称获取持仓"""
    for p in POSITIONS:
        if p.name == name:
            return p
    return None


def calculate_total_profit():
    """计算总体盈亏"""
    total_cost = 0
    total_value = 0
    total_profit = 0
    
    for p in POSITIONS:
        cost = p.cost_price * p.quantity
        value = p.current_price * p.quantity if p.current_price else cost
        profit = value - cost
        
        total_cost += cost
        total_value += value
        total_profit += profit
    
    profit_pct = (total_profit / total_cost * 100) if total_cost > 0 else 0
    
    return {
        'total_cost': total_cost,
        'total_value': total_value,
        'total_profit': total_profit,
        'profit_pct': profit_pct
    }


if __name__ == "__main__":
    # 测试
    print("📊 当前持仓")
    print("=" * 70)
    for p in POSITIONS:
        profit = (p.current_price - p.cost_price) / p.cost_price * 100 if p.current_price else 0
        emoji = '🟢' if profit >= 0 else '🔴'
        print(f"{emoji} {p.name} ({p.code}): 成本¥{p.cost_price} → 现价¥{p.current_price} ({profit:+.2f}%)")
    
    print("\n" + "=" * 70)
    total = calculate_total_profit()
    print(f"总成本: ¥{total['total_cost']:,.2f}")
    print(f"总市值: ¥{total['total_value']:,.2f}")
    print(f"总盈亏: ¥{total['total_profit']:,.2f} ({total['profit_pct']:+.2f}%)")
