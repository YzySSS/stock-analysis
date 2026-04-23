#!/usr/bin/env python3
"""
技术指标分析模块
支持常用技术分析指标计算和信号生成
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型"""
    BUY = "买入"
    SELL = "卖出"
    HOLD = "持有"
    STRONG_BUY = "强烈买入"
    STRONG_SELL = "强烈卖出"
    NEUTRAL = "中性"


@dataclass
class TechnicalIndicator:
    """技术指标数据结构"""
    name: str                    # 指标名称
    value: float                 # 当前值
    signal: SignalType           # 信号
    strength: float              # 信号强度 (0-100)
    description: str             # 描述
    details: Dict[str, Any]      # 详细信息


@dataclass
class StockAnalysis:
    """股票综合分析结果"""
    code: str
    name: str
    price: float
    change_percent: float
    
    # 技术指标
    ma_signal: Optional[TechnicalIndicator] = None      # 均线信号
    macd_signal: Optional[TechnicalIndicator] = None    # MACD信号
    rsi_signal: Optional[TechnicalIndicator] = None     # RSI信号
    volume_signal: Optional[TechnicalIndicator] = None  # 量能信号
    boll_signal: Optional[TechnicalIndicator] = None    # 布林带信号
    
    # 筹码分布 (新增)
    chip_distribution: Optional[Any] = None             # 筹码分布数据
    chip_signal: Optional[Dict] = None                  # 筹码信号
    
    # 基本面数据 (新增)
    fundamental: Optional[Any] = None                   # 基本面数据
    industry_comparison: Optional[Dict] = None          # 行业对比
    risk_alerts: List[str] = None                       # 基本面风险
    
    # 综合评分
    total_score: int = 0
    recommendation: str = ""
    risk_level: str = "中等"


class TechnicalAnalyzer:
    """
    技术分析器
    计算各种技术指标并生成交易信号
    """
    
    def __init__(self):
        self.indicators_cache = {}
    
    def analyze_stock(self, code: str, name: str, price: float,
                     change_percent: float, history_df: Any = None,
                     enable_chip: bool = True,
                     enable_fundamental: bool = True) -> StockAnalysis:
        """
        综合分析单只股票

        Args:
            code: 股票代码
            name: 股票名称
            price: 当前价格
            change_percent: 涨跌幅
            history_df: 历史数据DataFrame (包含 open, high, low, close, volume)
            enable_chip: 是否启用筹码分析
            enable_fundamental: 是否启用基本面分析

        Returns:
            StockAnalysis: 综合分析结果
        """
        analysis = StockAnalysis(
            code=code,
            name=name,
            price=price,
            change_percent=change_percent,
            risk_alerts=[]
        )

        # 如果有历史数据，计算技术指标
        if history_df is not None and len(history_df) >= 20:
            # 计算各指标
            analysis.ma_signal = self._calculate_ma_signals(history_df)
            analysis.macd_signal = self._calculate_macd(history_df)
            analysis.rsi_signal = self._calculate_rsi(history_df)
            analysis.volume_signal = self._calculate_volume_signals(history_df)
            analysis.boll_signal = self._calculate_bollinger(history_df)

            # 筹码分布分析 (新增)
            if enable_chip:
                try:
                    from chip_distribution import ChipDistributionAnalyzer
                    chip_analyzer = ChipDistributionAnalyzer()
                    analysis.chip_distribution = chip_analyzer.calculate_chip_distribution(history_df)
                    if analysis.chip_distribution:
                        analysis.chip_signal = chip_analyzer.get_chip_signal(analysis.chip_distribution, price)
                except Exception as e:
                    logger.warning(f"筹码分析失败: {e}")

        # 基本面分析 (新增)
        if enable_fundamental:
            try:
                from fundamental_adapter import fundamental_adapter
                analysis.fundamental = fundamental_adapter.get_fundamental_data(code[:6])
                if analysis.fundamental:
                    analysis.industry_comparison = fundamental_adapter.get_industry_comparison(code[:6])
                    analysis.risk_alerts = fundamental_adapter.get_risk_alerts(code[:6])
            except Exception as e:
                logger.warning(f"基本面分析失败: {e}")

        # 综合评分
        analysis.total_score = self._calculate_total_score(analysis)
        analysis.recommendation = self._generate_recommendation(analysis)
        analysis.risk_level = self._assess_risk_level(analysis)
        
        return analysis
    
    def _analyze_without_history(self, code: str, name: str, price: float, change_percent: float) -> StockAnalysis:
        """无历史数据时的基础分析"""
        logger.warning(f"{code} 历史数据不足，仅进行基础分析")
        analysis = StockAnalysis(
            code=code,
            name=name,
            price=price,
            change_percent=change_percent,
            total_score=50,
            recommendation="数据不足，建议观望",
            risk_level="未知"
        )
        
        return analysis
    
    def _calculate_ma_signals(self, df: Any) -> TechnicalIndicator:
        """
        计算均线信号
        判断MA5/MA10/MA20的多头/空头排列
        """
        try:
            # 计算均线
            df['MA5'] = df['close'].rolling(window=5).mean()
            df['MA10'] = df['close'].rolling(window=10).mean()
            df['MA20'] = df['close'].rolling(window=20).mean()
            df['MA60'] = df['close'].rolling(window=60).mean()
            
            latest = df.iloc[-1]
            ma5, ma10, ma20, ma60 = latest['MA5'], latest['MA10'], latest['MA20'], latest['MA60']
            close = latest['close']
            
            # 判断排列
            bullish_arrangement = ma5 > ma10 > ma20 > ma60  # 多头排列
            bearish_arrangement = ma5 < ma10 < ma20 < ma60  # 空头排列
            
            # 判断金叉死叉
            prev = df.iloc[-2]
            golden_cross = (df['MA5'].iloc[-2] <= df['MA10'].iloc[-2]) and (ma5 > ma10)  # MA5上穿MA10
            death_cross = (df['MA5'].iloc[-2] >= df['MA10'].iloc[-2]) and (ma5 < ma10)   # MA5下穿MA10
            
            # 计算偏离率
            bias_5 = (close - ma5) / ma5 * 100
            bias_20 = (close - ma20) / ma20 * 100
            
            # 生成信号
            if golden_cross or bullish_arrangement:
                signal = SignalType.BUY if golden_cross else SignalType.STRONG_BUY
                strength = 80 if golden_cross else 90
                desc = f"{'金叉形成' if golden_cross else '多头排列'}, 偏离率MA5={bias_5:.2f}%"
            elif death_cross or bearish_arrangement:
                signal = SignalType.SELL if death_cross else SignalType.STRONG_SELL
                strength = 80 if death_cross else 90
                desc = f"{'死叉形成' if death_cross else '空头排列'}, 偏离率MA5={bias_5:.2f}%"
            else:
                signal = SignalType.HOLD
                strength = 50
                desc = f"均线纠缠, 偏离率MA5={bias_5:.2f}%"
            
            return TechnicalIndicator(
                name="均线系统",
                value=ma5,
                signal=signal,
                strength=strength,
                description=desc,
                details={
                    "MA5": round(ma5, 2),
                    "MA10": round(ma10, 2),
                    "MA20": round(ma20, 2),
                    "MA60": round(ma60, 2),
                    "偏离率_MA5": round(bias_5, 2),
                    "偏离率_MA20": round(bias_20, 2),
                    "多头排列": bullish_arrangement,
                    "空头排列": bearish_arrangement
                }
            )
            
        except Exception as e:
            logger.error(f"计算均线信号失败: {e}")
            return TechnicalIndicator(
                name="均线系统",
                value=0,
                signal=SignalType.NEUTRAL,
                strength=0,
                description="计算失败",
                details={}
            )
    
    def _calculate_macd(self, df: Any) -> TechnicalIndicator:
        """
        计算MACD指标
        DIF = EMA(12) - EMA(26)
        DEA = EMA(DIF, 9)
        MACD = (DIF - DEA) * 2
        """
        try:
            # 计算EMA
            ema12 = df['close'].ewm(span=12, adjust=False).mean()
            ema26 = df['close'].ewm(span=26, adjust=False).mean()
            
            # 计算MACD
            df['DIF'] = ema12 - ema26
            df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
            df['MACD'] = (df['DIF'] - df['DEA']) * 2
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            dif, dea, macd = latest['DIF'], latest['DEA'], latest['MACD']
            
            # 判断金叉死叉
            golden_cross = (prev['DIF'] <= prev['DEA']) and (dif > dea)
            death_cross = (prev['DIF'] >= prev['DEA']) and (dif < dea)
            
            # 判断背离
            bullish_divergence = dif > 0 and macd > 0
            bearish_divergence = dif < 0 and macd < 0
            
            # 生成信号
            if golden_cross:
                signal = SignalType.BUY
                strength = 85
                desc = "MACD金叉形成，做多信号"
            elif death_cross:
                signal = SignalType.SELL
                strength = 85
                desc = "MACD死叉形成，做空信号"
            elif bullish_divergence and macd > prev['MACD']:
                signal = SignalType.BUY
                strength = 70
                desc = "MACD红柱放大，多头延续"
            elif bearish_divergence and macd < prev['MACD']:
                signal = SignalType.SELL
                strength = 70
                desc = "MACD绿柱放大，空头延续"
            else:
                signal = SignalType.HOLD
                strength = 50
                desc = "MACD走平，观望"
            
            return TechnicalIndicator(
                name="MACD",
                value=macd,
                signal=signal,
                strength=strength,
                description=desc,
                details={
                    "DIF": round(dif, 3),
                    "DEA": round(dea, 3),
                    "MACD": round(macd, 3),
                    "金叉": golden_cross,
                    "死叉": death_cross
                }
            )
            
        except Exception as e:
            logger.error(f"计算MACD失败: {e}")
            return TechnicalIndicator(
                name="MACD",
                value=0,
                signal=SignalType.NEUTRAL,
                strength=0,
                description="计算失败",
                details={}
            )
    
    def _calculate_rsi(self, df: Any, period: int = 14) -> TechnicalIndicator:
        """
        计算RSI相对强弱指标
        RSI > 70: 超买
        RSI < 30: 超卖
        """
        try:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            latest_rsi = rsi.iloc[-1]
            
            # 生成信号
            if latest_rsi > 80:
                signal = SignalType.STRONG_SELL
                strength = 90
                desc = f"RSI极度超买 ({latest_rsi:.1f})，强烈卖出"
            elif latest_rsi > 70:
                signal = SignalType.SELL
                strength = 75
                desc = f"RSI超买 ({latest_rsi:.1f})，考虑卖出"
            elif latest_rsi < 20:
                signal = SignalType.STRONG_BUY
                strength = 90
                desc = f"RSI极度超卖 ({latest_rsi:.1f})，强烈买入"
            elif latest_rsi < 30:
                signal = SignalType.BUY
                strength = 75
                desc = f"RSI超卖 ({latest_rsi:.1f})，考虑买入"
            else:
                signal = SignalType.HOLD
                strength = 50
                desc = f"RSI正常区间 ({latest_rsi:.1f})"
            
            return TechnicalIndicator(
                name="RSI",
                value=latest_rsi,
                signal=signal,
                strength=strength,
                description=desc,
                details={
                    "RSI": round(latest_rsi, 2),
                    "超买阈值": 70,
                    "超卖阈值": 30,
                    "周期": period
                }
            )
            
        except Exception as e:
            logger.error(f"计算RSI失败: {e}")
            return TechnicalIndicator(
                name="RSI",
                value=0,
                signal=SignalType.NEUTRAL,
                strength=0,
                description="计算失败",
                details={}
            )
    
    def _calculate_volume_signals(self, df: Any) -> TechnicalIndicator:
        """
        计算量能信号
        量比、放量/缩量判断
        """
        try:
            # 计算5日均量
            df['VOL_MA5'] = df['volume'].rolling(window=5).mean()
            df['VOL_MA20'] = df['volume'].rolling(window=20).mean()
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            current_vol = latest['volume']
            vol_ma5 = latest['VOL_MA5']
            vol_ma20 = latest['VOL_MA20']
            
            # 量比 (当前量/5日均量)
            volume_ratio = current_vol / vol_ma5 if vol_ma5 > 0 else 1
            
            # 放量/缩量判断
            volume_expanding = current_vol > vol_ma5 * 1.5  # 放量50%以上
            volume_shrinking = current_vol < vol_ma5 * 0.7   # 缩量30%以上
            
            # 价量配合
            price_up = latest['close'] > prev['close']
            price_down = latest['close'] < prev['close']
            
            # 生成信号
            if volume_expanding and price_up:
                signal = SignalType.BUY
                strength = 85
                desc = f"放量上涨，量比{volume_ratio:.2f}，量价配合良好"
            elif volume_expanding and price_down:
                signal = SignalType.SELL
                strength = 85
                desc = f"放量下跌，量比{volume_ratio:.2f}，注意风险"
            elif volume_shrinking and price_up:
                signal = SignalType.HOLD
                strength = 60
                desc = f"缩量上涨，量比{volume_ratio:.2f}，上涨动能减弱"
            elif volume_shrinking and price_down:
                signal = SignalType.BUY
                strength = 70
                desc = f"缩量下跌，量比{volume_ratio:.2f}，可能企稳"
            else:
                signal = SignalType.HOLD
                strength = 50
                desc = f"量能正常，量比{volume_ratio:.2f}"
            
            return TechnicalIndicator(
                name="量能分析",
                value=volume_ratio,
                signal=signal,
                strength=strength,
                description=desc,
                details={
                    "量比": round(volume_ratio, 2),
                    "当前量": int(current_vol),
                    "5日均量": int(vol_ma5),
                    "20日均量": int(vol_ma20),
                    "放量": volume_expanding,
                    "缩量": volume_shrinking
                }
            )
            
        except Exception as e:
            logger.error(f"计算量能信号失败: {e}")
            return TechnicalIndicator(
                name="量能分析",
                value=0,
                signal=SignalType.NEUTRAL,
                strength=0,
                description="计算失败",
                details={}
            )
    
    def _calculate_bollinger(self, df: Any, period: int = 20, std_dev: float = 2.0) -> TechnicalIndicator:
        """
        计算布林带指标
        上轨 = MA20 + 2*STD
        下轨 = MA20 - 2*STD
        """
        try:
            df['BOLL_MID'] = df['close'].rolling(window=period).mean()
            df['BOLL_STD'] = df['close'].rolling(window=period).std()
            df['BOLL_UP'] = df['BOLL_MID'] + std_dev * df['BOLL_STD']
            df['BOLL_DOWN'] = df['BOLL_MID'] - std_dev * df['BOLL_STD']
            
            latest = df.iloc[-1]
            close = latest['close']
            mid = latest['BOLL_MID']
            upper = latest['BOLL_UP']
            lower = latest['BOLL_DOWN']
            
            # 判断位置
            percent_b = (close - lower) / (upper - lower) * 100 if upper != lower else 50
            
            # 带宽 (波动率)
            bandwidth = (upper - lower) / mid * 100
            
            # 生成信号
            if close > upper:
                signal = SignalType.SELL
                strength = 80
                desc = f"股价突破布林上轨，超买，%B={percent_b:.1f}"
            elif close < lower:
                signal = SignalType.BUY
                strength = 80
                desc = f"股价跌破布林下轨，超卖，%B={percent_b:.1f}"
            elif percent_b > 80:
                signal = SignalType.HOLD
                strength = 65
                desc = f"接近上轨，偏强，%B={percent_b:.1f}"
            elif percent_b < 20:
                signal = SignalType.HOLD
                strength = 65
                desc = f"接近下轨，偏弱，%B={percent_b:.1f}"
            else:
                signal = SignalType.HOLD
                strength = 50
                desc = f"布林带中轨附近，震荡，%B={percent_b:.1f}"
            
            return TechnicalIndicator(
                name="布林带",
                value=close,
                signal=signal,
                strength=strength,
                description=desc,
                details={
                    "上轨": round(upper, 2),
                    "中轨": round(mid, 2),
                    "下轨": round(lower, 2),
                    "%B": round(percent_b, 2),
                    "带宽": round(bandwidth, 2)
                }
            )
            
        except Exception as e:
            logger.error(f"计算布林带失败: {e}")
            return TechnicalIndicator(
                name="布林带",
                value=0,
                signal=SignalType.NEUTRAL,
                strength=0,
                description="计算失败",
                details={}
            )
    
    def _calculate_total_score(self, analysis: StockAnalysis) -> int:
        """
        计算综合评分 (0-100)
        整合技术指标、筹码分布、基本面数据
        """
        scores = []
        weights = {
            'ma': 0.20,
            'macd': 0.20,
            'rsi': 0.15,
            'volume': 0.15,
            'boll': 0.10,
            'chip': 0.10,      # 筹码分布权重
            'fundamental': 0.10  # 基本面权重
        }

        # 技术指标评分
        if analysis.ma_signal:
            score = self._signal_to_score(analysis.ma_signal.signal) * analysis.ma_signal.strength / 100
            scores.append(score * weights['ma'])

        if analysis.macd_signal:
            score = self._signal_to_score(analysis.macd_signal.signal) * analysis.macd_signal.strength / 100
            scores.append(score * weights['macd'])

        if analysis.rsi_signal:
            score = self._signal_to_score(analysis.rsi_signal.signal) * analysis.rsi_signal.strength / 100
            scores.append(score * weights['rsi'])

        if analysis.volume_signal:
            score = self._signal_to_score(analysis.volume_signal.signal) * analysis.volume_signal.strength / 100
            scores.append(score * weights['volume'])

        if analysis.boll_signal:
            score = self._signal_to_score(analysis.boll_signal.signal) * analysis.boll_signal.strength / 100
            scores.append(score * weights['boll'])

        # 筹码分布评分 (新增)
        if analysis.chip_signal:
            chip_score = analysis.chip_signal.get('score', 50)
            scores.append(chip_score * weights['chip'])

        # 基本面评分 (新增)
        fundamental_score = self._calculate_fundamental_score(analysis)
        if fundamental_score > 0:
            scores.append(fundamental_score * weights['fundamental'])

        total = sum(scores) if scores else 50
        return min(max(int(total), 0), 100)

    def _calculate_fundamental_score(self, analysis: StockAnalysis) -> int:
        """
        计算基本面评分 (0-100)
        """
        if not analysis.fundamental:
            return 0

        score = 50
        f = analysis.fundamental

        # 估值评分
        if f.valuation.pe_ttm:
            if f.valuation.pe_ttm < 0:
                score -= 20  # 亏损
            elif f.valuation.pe_ttm < 20:
                score += 15  # 低估
            elif f.valuation.pe_ttm < 40:
                score += 5   # 合理
            else:
                score -= 10  # 高估

        # 成长评分
        if f.growth.profit_growth_yoy:
            if f.growth.profit_growth_yoy > 50:
                score += 15
            elif f.growth.profit_growth_yoy > 30:
                score += 10
            elif f.growth.profit_growth_yoy > 0:
                score += 5
            else:
                score -= 10

        # 盈利评分
        if f.profitability.roe:
            if f.profitability.roe > 20:
                score += 15
            elif f.profitability.roe > 15:
                score += 10
            elif f.profitability.roe > 10:
                score += 5
            else:
                score -= 5

        # 机构持仓评分
        if f.institution.fund_holdings:
            if f.institution.fund_holdings > 10:
                score += 10
            elif f.institution.fund_holdings > 5:
                score += 5

        # 风险扣分
        if analysis.risk_alerts:
            score -= len(analysis.risk_alerts) * 5

        return min(max(score, 0), 100)
    
    def _signal_to_score(self, signal: SignalType) -> int:
        """信号转换为分数"""
        mapping = {
            SignalType.STRONG_BUY: 95,
            SignalType.BUY: 80,
            SignalType.HOLD: 50,
            SignalType.SELL: 20,
            SignalType.STRONG_SELL: 5,
            SignalType.NEUTRAL: 50
        }
        return mapping.get(signal, 50)
    
    def _generate_recommendation(self, analysis: StockAnalysis) -> str:
        """生成投资建议"""
        score = analysis.total_score
        
        if score >= 85:
            return "强烈关注 - 多指标共振向上"
        elif score >= 70:
            return "积极关注 - 趋势向好"
        elif score >= 55:
            return "中性观望 - 等待明确信号"
        elif score >= 40:
            return "谨慎观望 - 趋势偏弱"
        else:
            return "回避 - 多指标走弱"
    
    def _assess_risk_level(self, analysis: StockAnalysis) -> str:
        """评估风险等级"""
        # 基于波动率、偏离度等评估风险
        risk_score = 0
        
        if analysis.boll_signal:
            bandwidth = analysis.boll_signal.details.get('带宽', 0)
            if bandwidth > 10:
                risk_score += 2  # 高波动
            elif bandwidth < 5:
                risk_score += 0  # 低波动
        
        if analysis.ma_signal:
            bias = analysis.ma_signal.details.get('偏离率_MA5', 0)
            if abs(bias) > 8:
                risk_score += 2  # 偏离过大
            elif abs(bias) > 5:
                risk_score += 1
        
        if analysis.rsi_signal:
            rsi = analysis.rsi_signal.value
            if rsi > 80 or rsi < 20:
                risk_score += 1  # 极端状态
        
        # 风险等级
        if risk_score >= 4:
            return "高风险"
        elif risk_score >= 2:
            return "中等风险"
        else:
            return "低风险"


class BatchAnalyzer:
    """批量分析器"""
    
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
    
    def analyze_batch(self, stocks_data: List[Dict], 
                     history_provider: callable = None) -> List[StockAnalysis]:
        """
        批量分析股票
        
        Args:
            stocks_data: 股票基础数据列表
            history_provider: 获取历史数据的函数 (code) -> DataFrame
        
        Returns:
            List[StockAnalysis]: 分析结果列表
        """
        results = []
        
        for stock in stocks_data:
            code = stock.get('code')
            name = stock.get('name')
            price = stock.get('price', 0)
            change = stock.get('change_percent', 0)
            
            # 获取历史数据
            history_df = None
            if history_provider:
                try:
                    history_df = history_provider(code)
                except Exception as e:
                    logger.warning(f"获取 {code} 历史数据失败: {e}")
            
            # 分析
            analysis = self.analyzer.analyze_stock(code, name, price, change, history_df)
            results.append(analysis)
        
        # 按评分排序
        results.sort(key=lambda x: x.total_score, reverse=True)
        return results
    
    def get_top_picks(self, analyses: List[StockAnalysis], 
                     min_score: int = 70, max_count: int = 10) -> List[StockAnalysis]:
        """
        获取精选标的
        
        Args:
            analyses: 分析结果列表
            min_score: 最低评分
            max_count: 最大数量
        
        Returns:
            List[StockAnalysis]: 精选标的
        """
        filtered = [a for a in analyses if a.total_score >= min_score]
        return filtered[:max_count]


# 便捷函数
def analyze_single_stock(code: str, name: str, price: float, 
                        change: float, history_df: Any = None) -> StockAnalysis:
    """便捷函数：分析单只股票"""
    analyzer = TechnicalAnalyzer()
    return analyzer.analyze_stock(code, name, price, change, history_df)


def quick_screen(stocks: List[Dict], min_score: int = 70) -> List[StockAnalysis]:
    """便捷函数：快速筛选"""
    batch = BatchAnalyzer()
    analyses = batch.analyze_batch(stocks)
    return batch.get_top_picks(analyses, min_score)


if __name__ == "__main__":
    """测试技术指标模块"""
    import pandas as pd
    
    print("=" * 60)
    print("技术指标分析模块测试")
    print("=" * 60)
    
    # 创建模拟历史数据
    print("\n1. 创建模拟历史数据...")
    np.random.seed(42)
    n_days = 60
    
    dates = pd.date_range(end=datetime.now(), periods=n_days, freq='D')
    base_price = 100
    
    # 生成随机价格序列
    returns = np.random.normal(0.001, 0.02, n_days)
    prices = base_price * np.exp(np.cumsum(returns))
    
    # 生成高低开收量
    df = pd.DataFrame({
        'date': dates,
        'open': prices * (1 + np.random.normal(0, 0.005, n_days)),
        'high': prices * (1 + abs(np.random.normal(0.01, 0.005, n_days))),
        'low': prices * (1 - abs(np.random.normal(0.01, 0.005, n_days))),
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, n_days)
    })
    
    print(f"   生成 {len(df)} 天历史数据")
    print(f"   价格区间: ¥{df['close'].min():.2f} - ¥{df['close'].max():.2f}")
    
    # 测试分析器
    print("\n2. 测试技术分析...")
    analyzer = TechnicalAnalyzer()
    
    analysis = analyzer.analyze_stock(
        code="000001",
        name="平安银行",
        price=df['close'].iloc[-1],
        change_percent=2.5,
        history_df=df
    )
    
    # 输出结果
    print(f"\n   股票: {analysis.code} {analysis.name}")
    print(f"   现价: ¥{analysis.price:.2f} ({analysis.change_percent:+.2f}%)")
    print(f"   综合评分: {analysis.total_score}/100")
    print(f"   投资建议: {analysis.recommendation}")
    print(f"   风险等级: {analysis.risk_level}")
    
    print("\n3. 技术指标详情:")
    
    if analysis.ma_signal:
        print(f"\n   [均线系统]")
        print(f"   信号: {analysis.ma_signal.signal.value} (强度: {analysis.ma_signal.strength}%)")
        print(f"   描述: {analysis.ma_signal.description}")
        print(f"   MA5: {analysis.ma_signal.details.get('MA5')} | MA20: {analysis.ma_signal.details.get('MA20')}")
    
    if analysis.macd_signal:
        print(f"\n   [MACD]")
        print(f"   信号: {analysis.macd_signal.signal.value} (强度: {analysis.macd_signal.strength}%)")
        print(f"   描述: {analysis.macd_signal.description}")
        print(f"   DIF: {analysis.macd_signal.details.get('DIF')} | MACD: {analysis.macd_signal.details.get('MACD')}")
    
    if analysis.rsi_signal:
        print(f"\n   [RSI]")
        print(f"   信号: {analysis.rsi_signal.signal.value} (强度: {analysis.rsi_signal.strength}%)")
        print(f"   描述: {analysis.rsi_signal.description}")
        print(f"   RSI值: {analysis.rsi_signal.details.get('RSI')}")
    
    if analysis.volume_signal:
        print(f"\n   [量能]")
        print(f"   信号: {analysis.volume_signal.signal.value} (强度: {analysis.volume_signal.strength}%)")
        print(f"   描述: {analysis.volume_signal.description}")
        print(f"   量比: {analysis.volume_signal.details.get('量比')}")
    
    if analysis.boll_signal:
        print(f"\n   [布林带]")
        print(f"   信号: {analysis.boll_signal.signal.value} (强度: {analysis.boll_signal.strength}%)")
        print(f"   描述: {analysis.boll_signal.description}")
        print(f"   上轨: {analysis.boll_signal.details.get('上轨')} | 下轨: {analysis.boll_signal.details.get('下轨')}")
    
    # 测试批量分析
    print("\n4. 测试批量分析...")
    batch = BatchAnalyzer()
    
    test_stocks = [
        {'code': '000001', 'name': '平安银行', 'price': 12.5, 'change_percent': 2.3},
        {'code': '000002', 'name': '万科A', 'price': 15.8, 'change_percent': -1.2},
        {'code': '600519', 'name': '贵州茅台', 'price': 1650, 'change_percent': 0.8},
    ]
    
    # 使用相同的模拟数据
    def mock_history_provider(code):
        return df
    
    results = batch.analyze_batch(test_stocks, mock_history_provider)
    
    print(f"\n   分析完成: {len(results)} 只股票")
    print("\n   评分排名:")
    for i, r in enumerate(results, 1):
        emoji = "🟢" if r.total_score >= 70 else "🟡" if r.total_score >= 50 else "🔴"
        print(f"   {i}. {r.code} {r.name}: {r.total_score}分 {emoji}")
    
    top_picks = batch.get_top_picks(results, min_score=60)
    print(f"\n   精选标的 (>=60分): {len(top_picks)} 只")
    
    print("\n" + "=" * 60)
    print("测试完成 ✅")
    print("=" * 60)


# ==================== 辅助函数 ====================

def get_signal_emoji(signal: str) -> str:
    """获取信号对应的表情"""
    emoji_map = {
        '强烈买入': '🔥',
        '买入': '🟢',
        '强烈卖出': '💔',
        '卖出': '🔴',
        '持有': '⚪',
        '中性': '⚪'
    }
    return emoji_map.get(signal, '⚪')


def get_risk_emoji(risk: str) -> str:
    """获取风险等级对应的表情"""
    emoji_map = {
        '高风险': '🔴',
        '中高风险': '🟠',
        '中等风险': '🟡',
        '中低风险': '🔵',
        '低风险': '🟢'
    }
    return emoji_map.get(risk, '⚪')


# ==================== 技术位分析（新增）====================

@dataclass
class TechnicalLevels:
    """技术位数据结构 - 支撑/阻力/止损/目标"""
    support_1: float          # 第一支撑位（强支撑）
    support_2: float          # 第二支撑位（弱支撑）
    resistance_1: float       # 第一阻力位（近阻力）
    resistance_2: float       # 第二阻力位（远阻力）
    stop_loss: float          # 止损价
    target_price: float       # 目标价
    
    # 计算依据
    support_basis: str = ""   # 支撑计算依据
    resistance_basis: str = "" # 阻力计算依据
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'support_1': round(self.support_1, 2),
            'support_2': round(self.support_2, 2),
            'resistance_1': round(self.resistance_1, 2),
            'resistance_2': round(self.resistance_2, 2),
            'stop_loss': round(self.stop_loss, 2),
            'target_price': round(self.target_price, 2),
            'support_basis': self.support_basis,
            'resistance_basis': self.resistance_basis
        }


def calculate_technical_levels(df: Any, current_price: float, 
                               volatility: float = None) -> TechnicalLevels:
    """
    计算股票的关键技术位
    
    计算逻辑：
    - 支撑1: 最近20日低点 或 MA20
    - 支撑2: 最近60日低点
    - 阻力1: 最近20日高点 或 前高
    - 阻力2: 最近60日高点 或 布林上轨
    - 止损: 支撑1下方3-5%（根据波动率调整）
    - 目标: 阻力1上方，或按风险收益比1:2计算
    
    Args:
        df: 历史数据DataFrame (包含 high, low, close) 或价格列表 List[float]
        current_price: 当前价格
        volatility: 波动率（ATR或历史波动率，可选）
    
    Returns:
        TechnicalLevels: 技术位数据
    """
    try:
        # 处理价格列表输入（从数据库获取的简单列表）
        if isinstance(df, list):
            if len(df) < 20:
                return _estimate_simple_levels(current_price)
            # 从价格列表创建DataFrame（假设波动率为2%来估算high/low）
            prices = df
            df = pd.DataFrame({
                'close': prices,
                'high': [p * (1 + abs(np.random.normal(0, 0.015))) for p in prices],
                'low': [p * (1 - abs(np.random.normal(0, 0.015))) for p in prices],
                'volume': [1000000] * len(prices)
            })
        
        if df is None or len(df) < 20:
            # 数据不足时使用简单估算
            return _estimate_simple_levels(current_price)
        
        # 计算基础指标
        df['MA5'] = df['close'].rolling(window=5).mean()
        df['MA10'] = df['close'].rolling(window=10).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        df['MA60'] = df['close'].rolling(window=60).mean()
        
        # 布林带
        df['BOLL_MID'] = df['close'].rolling(window=20).mean()
        df['BOLL_STD'] = df['close'].rolling(window=20).std()
        df['BOLL_UP'] = df['BOLL_MID'] + 2 * df['BOLL_STD']
        
        # 近期高低点
        recent_20 = df.tail(20)
        recent_60 = df.tail(60) if len(df) >= 60 else df
        
        high_20 = recent_20['high'].max()
        low_20 = recent_20['low'].min()
        high_60 = recent_60['high'].max()
        low_60 = recent_60['low'].min()
        
        latest = df.iloc[-1]
        ma20 = latest['MA20']
        ma60 = latest['MA60'] if not pd.isna(latest['MA60']) else ma20
        boll_up = latest['BOLL_UP']
        
        # 计算ATR（平均真实波幅）用于动态止损
        if volatility is None:
            atr = _calculate_atr(df)
        else:
            atr = volatility
        
        # ========== 支撑位计算 ==========
        # 支撑1：MA20和最近20日低点的较大值（强支撑）
        support_1 = max(ma20 * 0.98, low_20 * 1.02)  # 给予MA20稍微上浮，低点稍微下浮
        support_1 = min(support_1, current_price * 0.95)  # 确保在当前价下方
        
        # 支撑2：MA60或最近60日低点（弱支撑/防守位）
        support_2 = min(ma60 * 0.95, low_60 * 1.01)
        support_2 = min(support_2, support_1 * 0.95)  # 确保比支撑1更低
        
        # 支撑依据
        if abs(support_1 - ma20) / ma20 < 0.02:
            support_basis = f"MA20支撑(¥{ma20:.2f})"
        else:
            support_basis = f"20日低点(¥{low_20:.2f})"
        
        # ========== 阻力位计算 ==========
        # 阻力1：最近20日高点或前高
        resistance_1 = high_20 * 1.01  # 给予突破空间
        resistance_1 = max(resistance_1, current_price * 1.03)  # 确保在当前价上方
        
        # 阻力2：最近60日高点或布林上轨
        resistance_2 = max(high_60 * 1.02, boll_up * 1.01)
        resistance_2 = max(resistance_2, resistance_1 * 1.05)  # 确保比阻力1更高
        
        # 阻力依据
        if abs(resistance_1 - high_20) / high_20 < 0.02:
            resistance_basis = f"20日高点(¥{high_20:.2f})"
        else:
            resistance_basis = "前高压力"
        
        # ========== 止损价计算 ==========
        # 止损设在支撑1下方，根据ATR动态调整
        # 高波动股票给更宽松的止损
        if atr > 0:
            stop_distance = max(atr * 2, current_price * 0.03)  # 至少3%或2倍ATR
        else:
            stop_distance = current_price * 0.05  # 默认5%
        
        stop_loss = support_1 - stop_distance
        stop_loss = max(stop_loss, support_2 * 0.98)  # 不低于支撑2下方太多
        
        # ========== 目标价计算 ==========
        # 目标按风险收益比1:2计算
        risk = current_price - stop_loss
        if risk > 0:
            target_price = current_price + risk * 2  # 1:2 风险收益比
        else:
            target_price = current_price * 1.08  # 默认8%涨幅
        
        # 目标不超过阻力2
        target_price = min(target_price, resistance_2 * 0.98)
        # 目标至少要到阻力1
        target_price = max(target_price, resistance_1 * 1.02)
        
        return TechnicalLevels(
            support_1=support_1,
            support_2=support_2,
            resistance_1=resistance_1,
            resistance_2=resistance_2,
            stop_loss=stop_loss,
            target_price=target_price,
            support_basis=support_basis,
            resistance_basis=resistance_basis
        )
        
    except Exception as e:
        logger.error(f"计算技术位失败: {e}")
        return _estimate_simple_levels(current_price)


def _calculate_atr(df: Any, period: int = 14) -> float:
    """计算ATR（平均真实波幅）"""
    try:
        df['H_L'] = df['high'] - df['low']
        df['H_PC'] = abs(df['high'] - df['close'].shift(1))
        df['L_PC'] = abs(df['low'] - df['close'].shift(1))
        df['TR'] = df[['H_L', 'H_PC', 'L_PC']].max(axis=1)
        df['ATR'] = df['TR'].rolling(window=period).mean()
        return df['ATR'].iloc[-1]
    except:
        return 0


def _estimate_simple_levels(current_price: float) -> TechnicalLevels:
    """数据不足时的简单估算"""
    stop_loss = current_price * 0.95  # 默认5%止损
    target_price = current_price * 1.08  # 默认8%目标
    
    return TechnicalLevels(
        support_1=current_price * 0.97,
        support_2=current_price * 0.93,
        resistance_1=current_price * 1.05,
        resistance_2=current_price * 1.10,
        stop_loss=stop_loss,
        target_price=target_price,
        support_basis="估算（数据不足）",
        resistance_basis="估算（数据不足）"
    )


def format_technical_levels(levels: TechnicalLevels, current_price: float = None) -> str:
    """
    格式化技术位为可读字符串
    
    Args:
        levels: TechnicalLevels对象
        current_price: 当前价格（用于计算距离）
    
    Returns:
        str: 格式化后的技术位描述
    """
    lines = []
    
    # 支撑
    lines.append(f"📉 支撑位: ¥{levels.support_1:.2f}(强) / ¥{levels.support_2:.2f}(弱)")
    if levels.support_basis:
        lines.append(f"   依据: {levels.support_basis}")
    
    # 阻力
    lines.append(f"📈 阻力位: ¥{levels.resistance_1:.2f}(近) / ¥{levels.resistance_2:.2f}(远)")
    if levels.resistance_basis:
        lines.append(f"   依据: {levels.resistance_basis}")
    
    # 止损和目标
    lines.append(f"🛡️ 止损价: ¥{levels.stop_loss:.2f}")
    lines.append(f"🎯 目标价: ¥{levels.target_price:.2f}")
    
    # 风险收益比
    if current_price and current_price > 0:
        risk = current_price - levels.stop_loss
        reward = levels.target_price - current_price
        if risk > 0:
            rr_ratio = reward / risk
            lines.append(f"⚖️ 风险收益比: 1:{rr_ratio:.1f}")
    
    return "\n".join(lines)