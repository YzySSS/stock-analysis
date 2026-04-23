#!/usr/bin/env python3
"""
V12 策略 P0 风控系统
====================
核心功能:
1. 组合层面风控（最大回撤控制、仓位限制）
2. 个股仓位精细化管理
3. 连续亏损监测
4. 策略失效预警

使用示例:
    risk_ctrl = RiskControlSystem(max_drawdown_limit=0.20)
    
    # 每日检查
    if risk_ctrl.should_stop_trading(account_value, date):
        return []  # 空仓
    
    # 个股仓位计算
    position = risk_ctrl.calculate_position_per_stock(
        score=75, volatility=0.25, liquidity=0.02
    )
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class RiskMetrics:
    """风险指标数据类"""
    current_drawdown: float          # 当前回撤
    max_drawdown: float              # 历史最大回撤
    consecutive_losses: int          # 连续亏损次数
    weekly_return: float             # 近一周收益
    monthly_return: float            # 近一月收益
    market_volatility: float         # 市场波动率
    is_stop_trading: bool            # 是否停止交易
    position_scale: float            # 仓位缩放比例


class RiskControlSystem:
    """
    V12策略风控系统
    
    P0级核心功能:
    - 回撤控制: 最大回撤限制20%
    - 仓位限制: 单股20%，单行业30%
    - 连续亏损: 5次连续亏损停止交易
    - 周亏损: 周亏损超10%停止交易
    """
    
    def __init__(
        self,
        max_drawdown_limit: float = 0.20,
        max_position_per_stock: float = 0.20,
        max_position_per_industry: float = 0.30,
        max_consecutive_losses: int = 5,
        max_weekly_loss: float = -0.10,
        drawdown_penalty_start: float = 0.10
    ):
        """
        初始化风控系统
        
        Args:
            max_drawdown_limit: 最大回撤限制（默认20%）
            max_position_per_stock: 单股最大仓位（默认20%）
            max_position_per_industry: 单行业最大仓位（默认30%）
            max_consecutive_losses: 最大连续亏损次数（默认5次）
            max_weekly_loss: 最大周亏损限制（默认-10%）
            drawdown_penalty_start: 回撤惩罚起始点（默认10%）
        """
        self.max_drawdown_limit = max_drawdown_limit
        self.max_position_per_stock = max_position_per_stock
        self.max_position_per_industry = max_position_per_industry
        self.max_consecutive_losses = max_consecutive_losses
        self.max_weekly_loss = max_weekly_loss
        self.drawdown_penalty_start = drawdown_penalty_start
        
        # 状态跟踪
        self.peak_value = 0.0            # 历史最高净值
        self.current_drawdown = 0.0      # 当前回撤
        self.max_drawdown = 0.0          # 历史最大回撤
        self.consecutive_losses = 0      # 连续亏损计数
        self.trade_history = deque(maxlen=100)  # 最近交易记录
        self.daily_returns = deque(maxlen=30)   # 最近30日收益
        
        # 停止交易状态
        self.stop_trading_until = None   # 停止交易截止日期
        self.stop_reason = None          # 停止原因
        
        logger.info(f"风控系统初始化 | 最大回撤限制:{max_drawdown_limit:.0%} | "
                   f"单股上限:{max_position_per_stock:.0%} | "
                   f"连续亏损停止:{max_consecutive_losses}次")
    
    def update_account_value(self, current_value: float, date: str) -> RiskMetrics:
        """
        更新账户净值，计算风险指标
        
        Args:
            current_value: 当前账户净值
            date: 当前日期
            
        Returns:
            RiskMetrics: 风险指标对象
        """
        # 更新峰值
        if current_value > self.peak_value:
            self.peak_value = current_value
            self.consecutive_losses = 0  # 创新高，重置连续亏损
        
        # 计算回撤
        if self.peak_value > 0:
            self.current_drawdown = (self.peak_value - current_value) / self.peak_value
            self.max_drawdown = max(self.max_drawdown, self.current_drawdown)
        
        # 计算仓位缩放比例
        position_scale = self._calculate_position_scale()
        
        # 检查是否停止交易
        is_stop = self._check_stop_conditions(date)
        
        # 计算近期收益
        weekly_return = self._calculate_period_return(5)
        monthly_return = self._calculate_period_return(20)
        
        metrics = RiskMetrics(
            current_drawdown=self.current_drawdown,
            max_drawdown=self.max_drawdown,
            consecutive_losses=self.consecutive_losses,
            weekly_return=weekly_return,
            monthly_return=monthly_return,
            market_volatility=self._estimate_volatility(),
            is_stop_trading=is_stop,
            position_scale=position_scale
        )
        
        # 记录日志
        if self.current_drawdown > 0.15:
            logger.warning(f"⚠️ 当前回撤 {self.current_drawdown:.1%} 超过15%")
        
        return metrics
    
    def should_stop_trading(self, current_value: float, date: str) -> bool:
        """
        判断是否应停止交易
        
        Args:
            current_value: 当前账户净值
            date: 当前日期
            
        Returns:
            bool: 是否停止交易
        """
        # 更新状态
        self.update_account_value(current_value, date)
        
        # 检查是否处于停止期
        if self.stop_trading_until:
            stop_until = datetime.strptime(self.stop_trading_until, '%Y-%m-%d')
            current = datetime.strptime(date, '%Y-%m-%d')
            if current <= stop_until:
                logger.warning(f"🛑 停止交易至 {self.stop_trading_until} | 原因: {self.stop_reason}")
                return True
            else:
                # 停止期结束，重置
                logger.info(f"✅ 停止交易期结束，恢复交易")
                self.stop_trading_until = None
                self.stop_reason = None
        
        # 检查停止条件
        return self._check_stop_conditions(date)
    
    def _check_stop_conditions(self, date: str) -> bool:
        """检查停止交易条件"""
        # 条件1: 回撤超限
        if self.current_drawdown > self.max_drawdown_limit:
            self._trigger_stop_trading(date, "回撤超限", days=5)
            return True
        
        # 条件2: 连续亏损超限
        if self.consecutive_losses >= self.max_consecutive_losses:
            self._trigger_stop_trading(date, f"连续亏损{self.consecutive_losses}次", days=3)
            return True
        
        # 条件3: 周亏损超限
        weekly_return = self._calculate_period_return(5)
        if weekly_return < self.max_weekly_loss:
            self._trigger_stop_trading(date, f"周亏损{weekly_return:.1%}", days=5)
            return True
        
        return False
    
    def _trigger_stop_trading(self, date: str, reason: str, days: int = 5):
        """触发停止交易"""
        stop_date = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=days)).strftime('%Y-%m-%d')
        self.stop_trading_until = stop_date
        self.stop_reason = reason
        logger.error(f"🛑 触发停止交易 | 原因: {reason} | 停止至: {stop_date}")
    
    def _calculate_position_scale(self) -> float:
        """
        根据回撤计算仓位缩放比例
        
        回撤<10%: 100%仓位
        回撤10-15%: 70%仓位
        回撤15-20%: 40%仓位
        回撤>20%: 停止交易
        """
        if self.current_drawdown < self.drawdown_penalty_start:
            return 1.0
        elif self.current_drawdown < 0.15:
            # 10-15%回撤: 线性降至70%
            penalty = (self.current_drawdown - self.drawdown_penalty_start) / 0.05
            return 1.0 - penalty * 0.3
        elif self.current_drawdown < self.max_drawdown_limit:
            # 15-20%回撤: 线性降至40%
            penalty = (self.current_drawdown - 0.15) / 0.05
            return 0.7 - penalty * 0.3
        else:
            return 0.0
    
    def calculate_position_per_stock(
        self,
        score: float,
        volatility: float,
        liquidity: float,
        market_cap: Optional[float] = None
    ) -> float:
        """
        计算个股仓位权重
        
        Args:
            score: 选股得分(0-100)
            volatility: 波动率(如0.25表示25%)
            liquidity: 换手率(如0.02表示2%)
            market_cap: 市值(亿元)，可选
            
        Returns:
            float: 个股仓位权重(0-0.20)
        """
        # 基础权重: 基于得分
        base_weight = score / 100 * self.max_position_per_stock
        
        # 波动率调整: 高波动降低仓位
        vol_adjust = 1.0
        if volatility > 0.40:
            vol_adjust = 0.5
        elif volatility > 0.30:
            vol_adjust = 0.7
        elif volatility > 0.20:
            vol_adjust = 0.85
        
        # 流动性调整: 低流动性降低仓位
        liq_adjust = 1.0
        if liquidity < 0.01:  # 换手率<1%
            liq_adjust = 0.3
        elif liquidity < 0.02:  # 换手率<2%
            liq_adjust = 0.6
        elif liquidity < 0.05:  # 换手率<5%
            liq_adjust = 0.8
        
        # 市值调整: 小市值降低仓位
        cap_adjust = 1.0
        if market_cap and market_cap < 50:  # 市值<50亿
            cap_adjust = 0.5
        elif market_cap and market_cap < 100:  # 市值<100亿
            cap_adjust = 0.7
        
        # 综合计算
        position = base_weight * vol_adjust * liq_adjust * cap_adjust
        
        return min(position, self.max_position_per_stock)
    
    def record_trade(self, trade_result: Dict):
        """
        记录交易结果
        
        Args:
            trade_result: 交易结果字典，包含:
                - date: 交易日期
                - return: 收益率
                - code: 股票代码
        """
        self.trade_history.append(trade_result)
        
        # 更新连续亏损
        if trade_result.get('return', 0) < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        
        # 更新日收益
        self.daily_returns.append({
            'date': trade_result.get('date'),
            'return': trade_result.get('return', 0)
        })
    
    def _calculate_period_return(self, days: int) -> float:
        """计算最近N日收益"""
        if len(self.daily_returns) < days:
            return 0.0
        
        recent = list(self.daily_returns)[-days:]
        total_return = 1.0
        for r in recent:
            total_return *= (1 + r['return'])
        return total_return - 1
    
    def _estimate_volatility(self) -> float:
        """估算市场波动率"""
        if len(self.daily_returns) < 10:
            return 0.2
        
        returns = [r['return'] for r in self.daily_returns]
        return float(np.std(returns) * np.sqrt(252))
    
    def get_risk_report(self) -> Dict:
        """获取风险报告"""
        return {
            'current_drawdown': round(self.current_drawdown, 4),
            'max_drawdown': round(self.max_drawdown, 4),
            'consecutive_losses': self.consecutive_losses,
            'peak_value': round(self.peak_value, 2),
            'is_stop_trading': self.stop_trading_until is not None,
            'stop_until': self.stop_trading_until,
            'stop_reason': self.stop_reason,
            'weekly_return': round(self._calculate_period_return(5), 4),
            'monthly_return': round(self._calculate_period_return(20), 4)
        }
    
    def reset(self):
        """重置风控状态（用于新回测）"""
        self.peak_value = 0.0
        self.current_drawdown = 0.0
        self.max_drawdown = 0.0
        self.consecutive_losses = 0
        self.trade_history.clear()
        self.daily_returns.clear()
        self.stop_trading_until = None
        self.stop_reason = None
        logger.info("风控系统状态已重置")


# 导出
risk_control_system = RiskControlSystem()