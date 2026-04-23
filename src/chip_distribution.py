#!/usr/bin/env python3
"""
筹码分布分析模块 - 优化版
支持多数据源、熔断机制、fail-open设计
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

logger = logging.getLogger(__name__)


@dataclass
class ChipDistribution:
    """筹码分布数据结构"""
    price_levels: List[float]      # 价格档位
    chip_ratios: List[float]       # 各档位筹码占比
    avg_cost: float                # 平均成本
    concentration: float           # 筹码集中度 (90%集中度)
    profit_ratio: float            # 获利比例
    main_cost_zone: Tuple[float, float]  # 主力成本区间 (低, 高)
    source: str = "calculated"     # 数据来源


class ChipCircuitBreaker:
    """
    筹码分布接口熔断器
    防止重复调用失败的接口
    """
    
    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 300):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.is_open = False
    
    def record_success(self):
        """记录成功调用"""
        self.failure_count = 0
        self.is_open = False
    
    def record_failure(self):
        """记录失败调用"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.is_open = True
            logger.warning(f"[熔断] 筹码分布接口已熔断，{self.recovery_timeout}秒内不再尝试")
    
    def should_block(self) -> bool:
        """是否应该阻止调用"""
        if not self.is_open:
            return False
        
        # 检查是否过了恢复时间
        if self.last_failure_time:
            elapsed = (datetime.now() - self.last_failure_time).total_seconds()
            if elapsed > self.recovery_timeout:
                logger.info("[熔断] 恢复尝试")
                self.is_open = False
                self.failure_count = 0
                return False
        
        return True


# 全局熔断器实例
_chip_circuit_breaker = ChipCircuitBreaker()


class ChipDistributionAnalyzer:
    """
    筹码分布分析器 - 优化版
    
    支持:
    - 多数据源获取 (AkShare原生接口优先)
    - 计算方式兜底
    - 熔断机制
    - 失败降级
    """
    
    def __init__(self):
        self.enabled = True
        self.akshare_enabled = self._check_akshare()
    
    def _check_akshare(self) -> bool:
        """检查AkShare是否可用"""
        try:
            import akshare as ak
            return True
        except ImportError:
            logger.warning("AkShare未安装，筹码分析将使用计算方式")
            return False
    
    def calculate_chip_distribution(self, df: pd.DataFrame = None, 
                                    code: str = None) -> Optional[ChipDistribution]:
        """
        获取筹码分布（多数据源自动选择）
        
        优先级:
        1. AkShare原生接口 (如果可用且未熔断)
        2. 基于历史数据计算
        
        Args:
            df: 历史数据DataFrame
            code: 股票代码 (用于AkShare接口)
        
        Returns:
            ChipDistribution 筹码分布数据
        """
        # 检查功能是否被禁用
        if os.getenv('DISABLE_CHIP_DISTRIBUTION', 'false').lower() == 'true':
            logger.debug("[筹码分布] 功能已禁用")
            return None
        
        # 1. 尝试AkShare原生接口
        if code and self.akshare_enabled and not _chip_circuit_breaker.should_block():
            try:
                chip = self._get_from_akshare(code)
                if chip:
                    _chip_circuit_breaker.record_success()
                    logger.info(f"[筹码分布] 从AkShare获取成功: {code}")
                    return chip
            except Exception as e:
                _chip_circuit_breaker.record_failure()
                logger.warning(f"[筹码分布] AkShare接口失败: {e}")
        
        # 2. 使用计算方式兜底
        if df is not None and len(df) >= 60:
            try:
                chip = self._calculate_from_history(df)
                if chip:
                    logger.info("[筹码分布] 从历史数据计算成功")
                    return chip
            except Exception as e:
                logger.warning(f"[筹码分布] 计算失败: {e}")
        
        logger.warning("[筹码分布] 所有数据源均失败")
        return None
    
    def _get_from_akshare(self, code: str) -> Optional[ChipDistribution]:
        """从AkShare获取筹码分布"""
        try:
            import akshare as ak
            
            # 尝试多个接口
            candidates = [
                ('stock_chip_distribution_em', {'symbol': code}),
                ('stock_cyq_em', {'symbol': code}),
            ]
            
            for func_name, kwargs in candidates:
                try:
                    fn = getattr(ak, func_name, None)
                    if fn:
                        df = fn(**kwargs)
                        if df is not None and not df.empty:
                            latest = df.iloc[-1]
                            
                            return ChipDistribution(
                                price_levels=[],
                                chip_ratios=[],
                                avg_cost=float(latest.get('平均成本', latest.get('cost', 0))),
                                concentration=float(latest.get('90%集中度', latest.get('concentration', 0))),
                                profit_ratio=float(latest.get('获利比例', latest.get('profit_ratio', 0))),
                                main_cost_zone=(
                                    float(latest.get('主力成本下限', latest.get('main_cost_low', 0))),
                                    float(latest.get('主力成本上限', latest.get('main_cost_high', 0)))
                                ),
                                source='akshare'
                            )
                except Exception as e:
                    logger.debug(f"[筹码分布] 接口 {func_name} 失败: {e}")
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"[筹码分布] AkShare获取失败: {e}")
            return None
    
    def _calculate_from_history(self, df: pd.DataFrame) -> Optional[ChipDistribution]:
        """
        基于历史成交数据计算筹码分布
        
        参考: 东方财富筹码分布算法简化版
        """
        try:
            if df is None or len(df) < 60:
                logger.warning("数据不足,无法计算筹码分布")
                return None
            
            # 使用60日数据计算
            df = df.tail(60).copy()
            
            # 计算典型价格 (H+L+C)/3
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            
            # 计算价格区间
            min_price = df['low'].min()
            max_price = df['high'].max()
            
            if min_price == max_price or pd.isna(min_price) or pd.isna(max_price):
                return None
            
            # 分100个档位
            price_levels = np.linspace(min_price, max_price, 100)
            chip_ratios = []
            
            # 计算每个档位的筹码
            for i in range(len(price_levels) - 1):
                low_bound = price_levels[i]
                high_bound = price_levels[i + 1]
                
                # 筛选在这个价格区间的成交
                mask = (df['typical_price'] >= low_bound) & (df['typical_price'] < high_bound)
                volume_in_range = df[mask]['volume'].sum()
                
                chip_ratios.append(volume_in_range)
            
            # 归一化
            total_volume = sum(chip_ratios)
            if total_volume == 0:
                return None
            
            chip_ratios = [r / total_volume for r in chip_ratios]
            price_levels = price_levels[:-1]  # 去掉最后一个边界
            
            # 计算平均成本
            avg_cost = sum(p * r for p, r in zip(price_levels, chip_ratios))
            
            # 计算筹码集中度 (90%集中度)
            concentration = self._calculate_concentration(price_levels, chip_ratios)
            
            # 计算获利比例
            current_price = df['close'].iloc[-1]
            profit_ratio = self._calculate_profit_ratio(price_levels, chip_ratios, current_price)
            
            # 计算主力成本区间 (筹码最密集的区域,占40%)
            main_cost_zone = self._find_main_cost_zone(price_levels, chip_ratios)
            
            return ChipDistribution(
                price_levels=price_levels.tolist(),
                chip_ratios=chip_ratios,
                avg_cost=round(avg_cost, 2),
                concentration=round(concentration, 2),
                profit_ratio=round(profit_ratio, 2),
                main_cost_zone=main_cost_zone,
                source='calculated'
            )
            
        except Exception as e:
            logger.error(f"计算筹码分布失败: {e}")
            return None
    
    def _calculate_concentration(self, price_levels: List[float], 
                                  chip_ratios: List[float]) -> float:
        """
        计算筹码集中度 (90%集中度)
        公式: (90%筹码最高价 - 90%筹码最低价) / (90%筹码最高价 + 90%筹码最低价) * 100%
        """
        try:
            # 累计筹码
            cumulative = np.cumsum(chip_ratios)
            
            # 找到5%和95%分位点
            idx_5 = next(i for i, c in enumerate(cumulative) if c >= 0.05)
            idx_95 = next(i for i, c in enumerate(cumulative) if c >= 0.95)
            
            price_5 = price_levels[idx_5]
            price_95 = price_levels[idx_95]
            
            concentration = (price_95 - price_5) / (price_95 + price_5) * 100
            return concentration
            
        except Exception as e:
            logger.error(f"计算集中度失败: {e}")
            return 0.0
    
    def _calculate_profit_ratio(self, price_levels: List[float],
                                 chip_ratios: List[float],
                                 current_price: float) -> float:
        """
        计算获利比例
        成本低于当前价的筹码占比
        """
        try:
            profit_volume = 0
            for price, ratio in zip(price_levels, chip_ratios):
                if price < current_price:
                    profit_volume += ratio
            
            return profit_volume * 100
            
        except Exception as e:
            logger.error(f"计算获利比例失败: {e}")
            return 0.0
    
    def _find_main_cost_zone(self, price_levels: List[float],
                              chip_ratios: List[float],
                              ratio: float = 0.4) -> Tuple[float, float]:
        """
        找主力成本区间 (筹码最密集的区域)
        
        Args:
            ratio: 要包含的筹码比例 (默认40%)
        
        Returns:
            (最低价, 最高价)
        """
        try:
            # 滑动窗口找最密集的区域
            window_size = int(len(price_levels) * ratio)
            max_sum = 0
            max_idx = 0
            
            for i in range(len(price_levels) - window_size):
                window_sum = sum(chip_ratios[i:i+window_size])
                if window_sum > max_sum:
                    max_sum = window_sum
                    max_idx = i
            
            low = price_levels[max_idx]
            high = price_levels[max_idx + window_size]
            
            return (round(low, 2), round(high, 2))
            
        except Exception as e:
            logger.error(f"找主力成本区间失败: {e}")
            return (0.0, 0.0)
    
    def get_chip_signal(self, chip: ChipDistribution, current_price: float) -> Dict:
        """
        根据筹码分布生成交易信号
        
        Returns:
            {
                'signal': 'strong_buy'|'buy'|'hold'|'sell'|'strong_sell',
                'score': 0-100,
                'reason': '信号理由',
                'source': '数据来源'
            }
        """
        if chip is None:
            return {
                'signal': 'hold',
                'score': 50,
                'reason': '无筹码数据',
                'source': 'none'
            }
        
        signals = []
        score = 50
        
        # 1. 筹码集中度信号
        if chip.concentration < 10:
            signals.append("筹码高度集中,主力控盘度高")
            score += 15
        elif chip.concentration < 15:
            signals.append("筹码较集中")
            score += 10
        elif chip.concentration > 30:
            signals.append("筹码分散,注意风险")
            score -= 10
        
        # 2. 获利比例信号
        if chip.profit_ratio < 10:
            signals.append("套牢盘多,上涨压力大")
            score -= 15
        elif chip.profit_ratio < 30:
            signals.append("大部分筹码套牢")
            score -= 5
        elif chip.profit_ratio > 90:
            signals.append("获利盘过多,有回吐风险")
            score -= 10
        elif chip.profit_ratio > 70:
            signals.append("获利盘健康")
            score += 5
        
        # 3. 当前价格与主力成本关系
        main_cost_low, main_cost_high = chip.main_cost_zone
        if main_cost_low <= current_price <= main_cost_high:
            signals.append("价格在主力成本区,安全边际高")
            score += 10
        elif current_price < main_cost_low * 0.95:
            signals.append("价格低于主力成本,可能超跌")
            score += 5
        elif current_price > main_cost_high * 1.15:
            signals.append("价格远高于主力成本,注意回调")
            score -= 10
        
        # 4. 平均成本关系
        if current_price < chip.avg_cost * 0.95:
            signals.append("价格低于市场平均成本")
            score += 5
        elif current_price > chip.avg_cost * 1.10:
            signals.append("价格高于市场平均成本较多")
            score -= 5
        
        # 确定信号类型
        if score >= 75:
            signal = 'strong_buy'
        elif score >= 60:
            signal = 'buy'
        elif score >= 40:
            signal = 'hold'
        elif score >= 25:
            signal = 'sell'
        else:
            signal = 'strong_sell'
        
        return {
            'signal': signal,
            'score': max(0, min(100, score)),
            'reason': '; '.join(signals) if signals else '筹码分布中性',
            'source': chip.source
        }


def reset_circuit_breaker():
    """重置熔断器（手动恢复）"""
    global _chip_circuit_breaker
    _chip_circuit_breaker = ChipCircuitBreaker()
    logger.info("[熔断] 已手动重置")


if __name__ == "__main__":
    print("🧪 筹码分布分析模块测试 - 优化版")
    print("=" * 60)
    
    # 模拟测试数据
    import pandas as pd
    import numpy as np
    
    # 生成模拟K线数据
    np.random.seed(42)
    dates = pd.date_range(end=pd.Timestamp.now(), periods=60, freq='D')
    
    base_price = 10.0
    prices = []
    for i in range(60):
        change = np.random.normal(0, 0.02)
        base_price *= (1 + change)
        high = base_price * (1 + abs(np.random.normal(0, 0.01)))
        low = base_price * (1 - abs(np.random.normal(0, 0.01)))
        open_price = low + (high - low) * np.random.random()
        close = low + (high - low) * np.random.random()
        volume = int(np.random.randint(1000000, 10000000))
        
        prices.append({
            'date': dates[i],
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })
    
    df = pd.DataFrame(prices)
    
    # 测试计算
    analyzer = ChipDistributionAnalyzer()
    
    print("\n1. 测试计算方式...")
    chip = analyzer.calculate_chip_distribution(df=df, code="000001")
    
    if chip:
        print(f"✅ 计算成功! 来源: {chip.source}")
        print(f"   平均成本: ¥{chip.avg_cost}")
        print(f"   筹码集中度(90%): {chip.concentration}%")
        print(f"   获利比例: {chip.profit_ratio}%")
        print(f"   主力成本区间: ¥{chip.main_cost_zone[0]} - ¥{chip.main_cost_zone[1]}")
        
        # 测试信号
        current_price = df['close'].iloc[-1]
        signal = analyzer.get_chip_signal(chip, current_price)
        print(f"\n   📈 筹码信号:")
        print(f"   信号类型: {signal['signal']}")
        print(f"   评分: {signal['score']}/100")
        print(f"   理由: {signal['reason']}")
    else:
        print("❌ 计算失败")
    
    print("\n2. 测试熔断器...")
    cb = ChipCircuitBreaker(failure_threshold=2, recovery_timeout=5)
    print(f"   初始状态: 熔断={'开启' if cb.should_block() else '关闭'}")
    
    cb.record_failure()
    print(f"   失败1次: 熔断={'开启' if cb.should_block() else '关闭'}")
    
    cb.record_failure()
    print(f"   失败2次: 熔断={'开启' if cb.should_block() else '关闭'}")
    
    print("\n   等待5秒恢复...")
    import time
    time.sleep(6)
    print(f"   恢复后: 熔断={'开启' if cb.should_block() else '关闭'}")
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
