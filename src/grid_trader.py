#!/usr/bin/env python3
"""
ETF网格交易策略 V1.0
策略核心：在价格区间内低买高卖，赚取波动收益

适用标的：波动适中的ETF（银行ETF、电力ETF、沪深300ETF等）
不适合：趋势性强的个股或单边行情

参数设置：
- 价格区间：根据历史波动确定
- 网格数量：通常5-10格
- 每格买卖量：等分资金
- 每格价差：等差或等比
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
import os

logger = logging.getLogger(__name__)


@dataclass
class GridLevel:
    """网格档位"""
    level: int  # 档位序号（0为最低）
    buy_price: float  # 买入价
    sell_price: float  # 卖出价
    position: int  # 该档位持仓数量
    target_position: int  # 目标持仓数量


@dataclass
class GridStrategy:
    """网格策略配置"""
    code: str  # ETF代码
    name: str  # ETF名称
    base_price: float  # 基准价（当前价或成本价）
    lower_bound: float  # 下限价格
    upper_bound: float  # 上限价格
    grid_count: int  # 网格数量
    total_shares: int  # 总计划持仓数量
    grid_type: str = "等差"  # 等差/等比
    grid_levels: List[GridLevel] = field(default_factory=list)
    
    # 策略参数
    stop_loss_pct: float = 0.10  # 止损比例（跌破下限10%）
    take_profit_pct: float = 0.15  # 止盈比例（突破上限15%）


class ETFGridTrader:
    """
    ETF网格交易器
    
    核心逻辑：
    1. 在价格区间内划分多个网格
    2. 每跌一格买入一份
    3. 每涨一格卖出一份
    4. 严格遵守纪律，不追涨杀跌
    
    示例（银行ETF）：
    - 当前价：¥1.25
    - 区间：¥1.15 - ¥1.45
    - 网格：5格
    - 每格：3000股
    
    档位：
    5: ¥1.43 - 卖出价
    4: ¥1.37 - 买¥1.31/卖¥1.43
    3: ¥1.31 - 买¥1.25/卖¥1.37
    2: ¥1.25 - 买¥1.19/卖¥1.31  <- 当前档位
    1: ¥1.19 - 买¥1.13/卖¥1.25
    0: ¥1.13 - 买入价（下限）
    """
    
    # 预设策略配置
    DEFAULT_STRATEGIES = {
        '159887': {  # 银行ETF
            'name': '银行ETF',
            'lower_bound': 1.15,
            'upper_bound': 1.45,
            'grid_count': 6,
            'total_shares': 30900,
            'grid_type': '等差'
        },
        '159611': {  # 电力ETF
            'name': '电力ETF',
            'lower_bound': 1.05,
            'upper_bound': 1.35,
            'grid_count': 6,
            'total_shares': 19000,
            'grid_type': '等差'
        },
        '510300': {  # 沪深300ETF
            'name': '沪深300ETF',
            'lower_bound': 3.5,
            'upper_bound': 4.5,
            'grid_count': 8,
            'total_shares': 10000,
            'grid_type': '等差'
        },
        '510500': {  # 中证500ETF
            'name': '中证500ETF',
            'lower_bound': 5.5,
            'upper_bound': 7.5,
            'grid_count': 8,
            'total_shares': 8000,
            'grid_type': '等差'
        }
    }
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.expanduser("~/.clawdbot/stock_watcher/grid_strategies.json")
        self.strategies: Dict[str, GridStrategy] = {}
        self._load_strategies()
    
    def _load_strategies(self):
        """加载已保存的策略"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for code, config in data.items():
                        self.strategies[code] = self._create_strategy_from_config(code, config)
                logger.info(f"已加载 {len(self.strategies)} 个网格策略")
            except Exception as e:
                logger.warning(f"加载网格策略失败: {e}")
    
    def _save_strategies(self):
        """保存策略到文件"""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            data = {}
            for code, strategy in self.strategies.items():
                data[code] = {
                    'name': strategy.name,
                    'base_price': strategy.base_price,
                    'lower_bound': strategy.lower_bound,
                    'upper_bound': strategy.upper_bound,
                    'grid_count': strategy.grid_count,
                    'total_shares': strategy.total_shares,
                    'grid_type': strategy.grid_type
                }
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存网格策略失败: {e}")
    
    def _create_strategy_from_config(self, code: str, config: Dict) -> GridStrategy:
        """从配置创建策略"""
        strategy = GridStrategy(
            code=code,
            name=config.get('name', code),
            base_price=config.get('base_price', config.get('lower_bound', 1.0)),
            lower_bound=config.get('lower_bound', 1.0),
            upper_bound=config.get('upper_bound', 1.5),
            grid_count=config.get('grid_count', 5),
            total_shares=config.get('total_shares', 10000),
            grid_type=config.get('grid_type', '等差')
        )
        
        # 生成网格档位
        strategy.grid_levels = self._generate_grid_levels(strategy)
        return strategy
    
    def create_strategy(self, 
                        code: str,
                        name: str,
                        current_price: float,
                        lower_bound: float,
                        upper_bound: float,
                        grid_count: int = 6,
                        total_shares: int = 10000,
                        grid_type: str = '等差') -> GridStrategy:
        """
        创建新的网格策略
        
        Args:
            code: ETF代码
            name: ETF名称
            current_price: 当前价格
            lower_bound: 价格下限
            upper_bound: 价格上限
            grid_count: 网格数量（5-10）
            total_shares: 计划总持仓股数
            grid_type: 等差/等比
        """
        if lower_bound >= upper_bound:
            raise ValueError("下限必须小于上限")
        
        if current_price < lower_bound or current_price > upper_bound:
            logger.warning(f"当前价¥{current_price}不在区间[¥{lower_bound}, ¥{upper_bound}]内")
        
        strategy = GridStrategy(
            code=code,
            name=name,
            base_price=current_price,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            grid_count=grid_count,
            total_shares=total_shares,
            grid_type=grid_type
        )
        
        # 生成网格档位
        strategy.grid_levels = self._generate_grid_levels(strategy)
        
        # 确定当前档位
        current_level = self._get_current_level(strategy, current_price)
        
        # 初始化持仓分布
        self._init_position_distribution(strategy, current_level)
        
        # 保存策略
        self.strategies[code] = strategy
        self._save_strategies()
        
        logger.info(f"创建网格策略: {name}({code}) {grid_count}格 区间[¥{lower_bound}, ¥{upper_bound}]")
        
        return strategy
    
    def _generate_grid_levels(self, strategy: GridStrategy) -> List[GridLevel]:
        """生成网格档位"""
        levels = []
        
        if strategy.grid_type == '等差':
            # 等差网格
            step = (strategy.upper_bound - strategy.lower_bound) / strategy.grid_count
            
            for i in range(strategy.grid_count + 1):
                price = strategy.lower_bound + step * i
                
                if i == 0:
                    # 最低档，只有买入价
                    buy_price = round(price, 3)
                    sell_price = round(price + step, 3)
                elif i == strategy.grid_count:
                    # 最高档，只有卖出价
                    buy_price = round(price - step, 3)
                    sell_price = round(price, 3)
                else:
                    # 中间档
                    buy_price = round(price - step / 2, 3)
                    sell_price = round(price + step / 2, 3)
                
                levels.append(GridLevel(
                    level=i,
                    buy_price=buy_price,
                    sell_price=sell_price,
                    position=0,
                    target_position=0
                ))
        
        else:  # 等比网格
            ratio = (strategy.upper_bound / strategy.lower_bound) ** (1 / strategy.grid_count)
            
            for i in range(strategy.grid_count + 1):
                price = strategy.lower_bound * (ratio ** i)
                
                if i == 0:
                    buy_price = round(price, 3)
                    sell_price = round(price * ratio, 3)
                elif i == strategy.grid_count:
                    buy_price = round(price / ratio, 3)
                    sell_price = round(price, 3)
                else:
                    buy_price = round(price / (ratio ** 0.5), 3)
                    sell_price = round(price * (ratio ** 0.5), 3)
                
                levels.append(GridLevel(
                    level=i,
                    buy_price=buy_price,
                    sell_price=sell_price,
                    position=0,
                    target_position=0
                ))
        
        return levels
    
    def _get_current_level(self, strategy: GridStrategy, current_price: float) -> int:
        """确定当前价格所在档位"""
        for level in strategy.grid_levels:
            if current_price <= level.sell_price:
                return level.level
        return strategy.grid_count
    
    def _init_position_distribution(self, strategy: GridStrategy, current_level: int):
        """初始化持仓分布"""
        shares_per_grid = strategy.total_shares // (strategy.grid_count + 1)
        
        # 当前档位及以下，满仓
        for i in range(current_level + 1):
            strategy.grid_levels[i].target_position = shares_per_grid
            strategy.grid_levels[i].position = shares_per_grid
        
        # 当前档位以上，空仓
        for i in range(current_level + 1, strategy.grid_count + 1):
            strategy.grid_levels[i].target_position = shares_per_grid
            strategy.grid_levels[i].position = 0
    
    def check_trading_signals(self, code: str, current_price: float) -> List[Dict]:
        """
        检查交易信号
        
        Returns:
            交易信号列表 [{'action': 'buy'/'sell', 'price': float, 'shares': int, 'reason': str}]
        """
        if code not in self.strategies:
            logger.warning(f"未找到 {code} 的网格策略")
            return []
        
        strategy = self.strategies[code]
        signals = []
        
        # 检查是否触发止损
        if current_price < strategy.lower_bound * (1 - strategy.stop_loss_pct):
            # 跌破下限10%，触发止损
            total_position = sum(level.position for level in strategy.grid_levels)
            if total_position > 0:
                signals.append({
                    'action': 'sell',
                    'price': current_price,
                    'shares': total_position,
                    'reason': f'跌破网格下限{strategy.stop_loss_pct*100:.0f}%，止损清仓'
                })
            return signals
        
        # 检查是否触发止盈
        if current_price > strategy.upper_bound * (1 + strategy.take_profit_pct):
            # 突破上限15%，触发止盈
            total_position = sum(level.position for level in strategy.grid_levels)
            if total_position > 0:
                signals.append({
                    'action': 'sell',
                    'price': current_price,
                    'shares': total_position,
                    'reason': f'突破网格上限{strategy.take_profit_pct*100:.0f}%，止盈清仓'
                })
            return signals
        
        # 检查各档位交易机会
        for level in strategy.grid_levels:
            # 买入信号：价格低于买入价且该档位未满仓
            if current_price <= level.buy_price and level.position < level.target_position:
                buy_shares = level.target_position - level.position
                signals.append({
                    'action': 'buy',
                    'price': current_price,
                    'shares': buy_shares,
                    'reason': f'触发{level.level}档买入 (买入价¥{level.buy_price})'
                })
            
            # 卖出信号：价格高于卖出价且该档位有持仓
            if current_price >= level.sell_price and level.position > 0:
                sell_shares = level.position
                signals.append({
                    'action': 'sell',
                    'price': current_price,
                    'shares': sell_shares,
                    'reason': f'触发{level.level}档卖出 (卖出价¥{level.sell_price})'
                })
        
        return signals
    
    def update_position(self, code: str, level: int, shares_change: int):
        """更新持仓"""
        if code not in self.strategies:
            return
        
        strategy = self.strategies[code]
        if 0 <= level <= strategy.grid_count:
            strategy.grid_levels[level].position += shares_change
            logger.info(f"更新 {code} 第{level}档持仓: {shares_change:+d}股")
    
    def get_strategy_report(self, code: str, current_price: float) -> str:
        """生成策略报告"""
        if code not in self.strategies:
            return f"未找到 {code} 的网格策略"
        
        strategy = self.strategies[code]
        signals = self.check_trading_signals(code, current_price)
        
        lines = [
            f"\n{'='*60}",
            f"网格交易策略报告: {strategy.name}({code})",
            f"{'='*60}",
            f"价格区间: ¥{strategy.lower_bound} - ¥{strategy.upper_bound}",
            f"当前价格: ¥{current_price}",
            f"网格数量: {strategy.grid_count}格",
            f"总计划持仓: {strategy.total_shares}股",
            f"网格类型: {strategy.grid_type}",
            f"\n网格明细:",
            f"{'档位':<6}{'买入价':<10}{'卖出价':<10}{'持仓':<10}{'目标持仓':<10}",
            "-" * 50
        ]
        
        for level in strategy.grid_levels:
            marker = " <-- 当前" if level.buy_price <= current_price <= level.sell_price else ""
            lines.append(
                f"{level.level:<6}¥{level.buy_price:<9.3f}¥{level.sell_price:<9.3f}"
                f"{level.position:<10}{level.target_position:<10}{marker}"
            )
        
        # 交易信号
        if signals:
            lines.extend([
                f"\n{'='*60}",
                "交易信号:",
                "-" * 50
            ])
            for sig in signals:
                emoji = "🔴 买入" if sig['action'] == 'buy' else "🟢 卖出"
                lines.append(
                    f"{emoji} {sig['shares']}股 @ ¥{sig['price']:.3f}\n  理由: {sig['reason']}"
                )
        else:
            lines.append("\n当前无交易信号，继续观望")
        
        lines.append("="*60)
        return "\n".join(lines)
    
    def get_all_strategies_summary(self) -> str:
        """获取所有策略摘要"""
        if not self.strategies:
            return "暂无网格策略"
        
        lines = ["\n网格策略总览:", "="*60]
        for code, strategy in self.strategies.items():
            total_position = sum(level.position for level in strategy.grid_levels)
            lines.append(
                f"{strategy.name}({code}): 持仓{total_position}/{strategy.total_shares}股 "
                f"区间[¥{strategy.lower_bound}, ¥{strategy.upper_bound}]"
            )
        lines.append("="*60)
        return "\n".join(lines)


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("ETF网格交易策略测试")
    print("=" * 60)
    
    trader = ETFGridTrader()
    
    # 创建银行ETF网格策略
    strategy = trader.create_strategy(
        code='159887',
        name='银行ETF',
        current_price=1.258,
        lower_bound=1.15,
        upper_bound=1.45,
        grid_count=6,
        total_shares=30900
    )
    
    # 打印策略报告
    print(trader.get_strategy_report('159887', 1.258))
    
    # 测试不同价格的交易信号
    test_prices = [1.20, 1.25, 1.30, 1.35]
    print("\n不同价格的交易信号测试:")
    for price in test_prices:
        signals = trader.check_trading_signals('159887', price)
        print(f"\n价格 ¥{price}:")
        if signals:
            for sig in signals:
                print(f"  {sig['action'].upper()} {sig['shares']}股 - {sig['reason']}")
        else:
            print("  无信号")
    
    print("\n" + "=" * 60)
