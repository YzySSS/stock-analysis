#!/usr/bin/env python3
"""
V12 策略市场环境过滤系统
==========================
核心功能:
1. 多指标综合判断市场环境（牛市/震荡/熊市）
2. 熊市自动空仓/减仓
3. 市场宽度分析
4. 波动率监测

使用示例:
    market_filter = MarketEnvironmentFilter()
    
    # 判断市场环境
    status, score = market_filter.get_market_status(date, db_connection)
    
    # 检查是否应空仓
    if market_filter.should_stop_strategy(status, strategy_performance):
        return []  # 空仓
"""

import logging
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class MarketIndicators:
    """市场指标数据类"""
    ma_trend: str                    # MA趋势: 'up', 'down', 'neutral'
    market_breadth: float            # 市场宽度(0-1)
    volatility: float                # 波动率
    liquidity: float                 # 流动性指标
    index_return_20d: float          # 20日指数收益
    composite_score: float           # 综合得分(-1到1)
    status: str                      # 市场环境: 'bull', 'neutral', 'bear'


class MarketEnvironmentFilter:
    """
    V12策略市场环境过滤系统
    
    P0级核心功能:
    - 多指标综合判断: MA排列 + 市场宽度 + 波动率 + 趋势
    - 熊市自动空仓: 当判断为熊市时停止交易
    - 动态仓位调整: 根据市场环境调整仓位比例
    """
    
    def __init__(
        self,
        bear_threshold: float = -0.3,
        bull_threshold: float = 0.3,
        volatility_high: float = 0.30,
        volatility_low: float = 0.15,
        breadth_threshold: float = 0.40
    ):
        """
        初始化市场环境过滤器
        
        Args:
            bear_threshold: 熊市判断阈值（默认-0.3）
            bull_threshold: 牛市判断阈值（默认0.3）
            volatility_high: 高波动率阈值（默认30%）
            volatility_low: 低波动率阈值（默认15%）
            breadth_threshold: 市场宽度阈值（默认40%）
        """
        self.bear_threshold = bear_threshold
        self.bull_threshold = bull_threshold
        self.volatility_high = volatility_high
        self.volatility_low = volatility_low
        self.breadth_threshold = breadth_threshold
        
        # 状态缓存
        self.last_status = None
        self.last_date = None
        
        logger.info(f"市场环境过滤系统初始化 | 牛市阈值:{bull_threshold} | "
                   f"熊市阈值:{bear_threshold}")
    
    def get_market_status(
        self,
        date: str,
        conn,
        index_code: str = '000001'  # 默认上证指数
    ) -> Tuple[str, float]:
        """
        判断市场环境
        
        Args:
            date: 当前日期
            conn: 数据库连接
            index_code: 指数代码（默认上证指数）
            
        Returns:
            Tuple[str, float]: (market_status, composite_score)
            market_status: 'bull', 'neutral', 'bear'
            composite_score: -1到1之间的综合得分
        """
        cursor = conn.cursor()
        
        try:
            # 获取指数最近20日数据
            cursor.execute("""
                SELECT close, pct_change, turnover
                FROM stock_kline
                WHERE code = %s AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 20
            """, (index_code, date))
            
            rows = cursor.fetchall()
            
            if len(rows) < 20:
                logger.warning(f"指数数据不足20天，默认返回neutral")
                return 'neutral', 0.0
            
            closes = [float(r[0]) for r in reversed(rows)]
            changes = [float(r[1]) for r in rows if r[1] is not None]
            turnovers = [float(r[2]) for r in rows if r[2] is not None]
            
            # 计算各项指标
            ma_score = self._calculate_ma_score(closes)
            trend_score = self._calculate_trend_score(closes)
            breadth_score = self._calculate_breadth(conn, date)
            vol_score = self._calculate_volatility_score(changes)
            liq_score = self._calculate_liquidity_score(turnovers)
            
            # 综合得分（加权）
            # MA排列 30% + 趋势 25% + 市场宽度 25% + 波动率 10% + 流动性 10%
            composite = (
                ma_score * 0.30 +
                trend_score * 0.25 +
                breadth_score * 0.25 +
                vol_score * 0.10 +
                liq_score * 0.10
            )
            
            # 判断市场状态
            if composite > self.bull_threshold:
                status = 'bull'
            elif composite < self.bear_threshold:
                status = 'bear'
            else:
                status = 'neutral'
            
            # 缓存结果
            self.last_status = status
            self.last_date = date
            
            logger.info(f"市场环境: {status.upper()} | 综合得分:{composite:+.2f} | "
                       f"MA:{ma_score:+.2f} 趋势:{trend_score:+.2f} 宽度:{breadth_score:+.2f}")
            
            return status, composite
            
        finally:
            cursor.close()
    
    def _calculate_ma_score(self, closes: List[float]) -> float:
        """
        计算MA排列得分
        
        Returns:
            float: -1到1之间
        """
        if len(closes) < 20:
            return 0.0
        
        ma5 = np.mean(closes[-5:])
        ma10 = np.mean(closes[-10:])
        ma20 = np.mean(closes)
        
        # 多头排列: MA5 > MA10 > MA20
        if ma5 > ma10 > ma20:
            return 1.0
        # 空头排列: MA5 < MA10 < MA20
        elif ma5 < ma10 < ma20:
            return -1.0
        # 金叉/死叉过渡
        elif ma5 > ma10 and ma10 <= ma20:
            return 0.5  # 潜在多头排列
        elif ma5 < ma10 and ma10 >= ma20:
            return -0.5  # 潜在空头排列
        else:
            return 0.0
    
    def _calculate_trend_score(self, closes: List[float]) -> float:
        """
        计算趋势得分（基于20日涨跌幅）
        
        Returns:
            float: -1到1之间
        """
        if len(closes) < 20:
            return 0.0
        
        return_20d = (closes[-1] - closes[0]) / closes[0] * 100
        
        if return_20d > 10:
            return 1.0
        elif return_20d > 5:
            return 0.5
        elif return_20d > 0:
            return 0.2
        elif return_20d > -5:
            return -0.2
        elif return_20d > -10:
            return -0.5
        else:
            return -1.0
    
    def _calculate_breadth(self, conn, date: str) -> float:
        """
        计算市场宽度（上涨股票比例）
        
        Returns:
            float: -1到1之间
        """
        cursor = conn.cursor()
        
        try:
            # 获取当日全市场涨跌分布
            cursor.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN pct_change > 0 THEN 1 ELSE 0 END) as up_count
                FROM stock_kline
                WHERE trade_date = %s
                AND code NOT LIKE '688%'  -- 排除科创板
                AND code NOT LIKE '8%'    -- 排除北交所
            """, (date,))
            
            row = cursor.fetchone()
            if not row or row[0] == 0:
                return 0.0
            
            total, up_count = row[0], row[1] or 0
            up_ratio = up_count / total
            
            # 映射到-1到1
            # 上涨比例>60%: 1.0, <40%: -1.0
            if up_ratio > 0.60:
                return 1.0
            elif up_ratio < 0.40:
                return -1.0
            else:
                return (up_ratio - 0.5) / 0.1
            
        except Exception as e:
            logger.error(f"计算市场宽度失败: {e}")
            return 0.0
        finally:
            cursor.close()
    
    def _calculate_volatility_score(self, changes: List[float]) -> float:
        """
        计算波动率得分
        
        Returns:
            float: -1到1之间，高波动为负分
        """
        if len(changes) < 10:
            return 0.0
        
        volatility = float(np.std(changes) * np.sqrt(252))
        
        # 高波动是负面信号
        if volatility > self.volatility_high:
            return -1.0
        elif volatility > 0.25:
            return -0.5
        elif volatility < self.volatility_low:
            return 0.5  # 低波动是正面信号
        else:
            return 0.0
    
    def _calculate_liquidity_score(self, turnovers: List[float]) -> float:
        """
        计算流动性得分
        
        Returns:
            float: -1到1之间
        """
        if len(turnovers) < 10:
            return 0.0
        
        avg_turnover = np.mean(turnovers)
        recent_turnover = np.mean(turnovers[-5:])
        
        # 成交额萎缩是负面信号
        if recent_turnover < avg_turnover * 0.5:
            return -1.0  # 严重萎缩
        elif recent_turnover < avg_turnover * 0.7:
            return -0.5
        elif recent_turnover > avg_turnover * 1.3:
            return 0.5  # 放量是正面信号
        else:
            return 0.0
    
    def should_stop_strategy(
        self,
        market_status: str,
        strategy_performance: Optional[Dict] = None
    ) -> bool:
        """
        判断是否应停止策略
        
        Args:
            market_status: 市场环境状态
            strategy_performance: 策略表现字典，包含:
                - recent_return: 近期收益
                - max_drawdown: 最大回撤
                
        Returns:
            bool: 是否停止策略
        """
        # 条件1: 熊市状态
        if market_status == 'bear':
            logger.warning(f"📉 熊市状态，建议空仓")
            return True
        
        # 条件2: 策略表现差（如果提供了数据）
        if strategy_performance:
            recent_return = strategy_performance.get('recent_return', 0)
            max_dd = strategy_performance.get('max_drawdown', 0)
            
            # 熊市+近期亏损>10%
            if market_status == 'bear' and recent_return < -0.10:
                logger.warning(f"🛑 熊市且策略近期亏损{recent_return:.1%}，停止交易")
                return True
            
            # 任何市场状态下回撤超20%
            if max_dd > 0.20:
                logger.warning(f"🛑 策略回撤{max_dd:.1%}超过20%，停止交易")
                return True
        
        return False
    
    def get_position_ratio(self, market_status: str, volatility: float) -> float:
        """
        根据市场环境获取建议仓位比例
        
        Args:
            market_status: 市场环境
            volatility: 波动率
            
        Returns:
            float: 仓位比例(0-1)
        """
        # 基础仓位
        base_ratio = {
            'bull': 1.0,
            'neutral': 0.6,
            'bear': 0.0
        }.get(market_status, 0.5)
        
        # 波动率调整
        vol_adjust = 1.0
        if volatility > self.volatility_high:
            vol_adjust = 0.5
        elif volatility > 0.25:
            vol_adjust = 0.7
        elif volatility < self.volatility_low:
            vol_adjust = 1.1
        
        return min(base_ratio * vol_adjust, 1.0)
    
    def get_market_summary(self, conn, date: str) -> Dict:
        """获取市场摘要报告"""
        status, score = self.get_market_status(date, conn)
        
        return {
            'date': date,
            'market_status': status,
            'composite_score': round(score, 3),
            'position_ratio': self.get_position_ratio(status, 0.2),
            'should_trade': status != 'bear',
            'description': {
                'bull': '牛市 - 积极做多',
                'neutral': '震荡市 - 谨慎操作',
                'bear': '熊市 - 空仓观望'
            }.get(status, 'unknown')
        }
    
    def reset(self):
        """重置状态"""
        self.last_status = None
        self.last_date = None
        logger.info("市场环境过滤系统已重置")


# 导出
market_environment_filter = MarketEnvironmentFilter()