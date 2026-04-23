#!/usr/bin/env python3
"""
报告生成器模块
支持多种报告格式：完整版、简洁版、Brief模式
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ReportType(Enum):
    """报告类型"""
    FULL = "full"       # 完整版：详细分析
    SIMPLE = "simple"   # 简洁版：关键信息
    BRIEF = "brief"     # Brief模式：3-5句话概括


@dataclass
class BriefStockSummary:
    """Brief模式股票摘要"""
    code: str
    name: str
    signal_emoji: str      # 信号表情
    operation: str         # 操作建议
    score: int             # 评分
    one_sentence: str      # 一句话结论
    key_price: str = ""    # 关键价位
    risk_note: str = ""    # 风险提示


class ReportGenerator:
    """
    报告生成器
    
    支持:
    - FULL: 完整详细报告（所有指标、新闻、筹码等）
    - SIMPLE: 简洁报告（关键信息）
    - BRIEF: 3-5句话概括（适合移动端/快速浏览）
    """
    
    def __init__(self, report_type: ReportType = ReportType.FULL):
        self.report_type = report_type
    
    def generate_report(self, results: List[Any], 
                       market_summary: Dict = None,
                       report_date: str = None) -> str:
        """
        生成报告
        
        Args:
            results: 分析结果列表
            market_summary: 市场概况
            report_date: 报告日期
        
        Returns:
            Markdown格式报告
        """
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        
        if self.report_type == ReportType.BRIEF:
            return self._generate_brief_report(results, report_date)
        elif self.report_type == ReportType.SIMPLE:
            return self._generate_simple_report(results, market_summary, report_date)
        else:
            return self._generate_full_report(results, market_summary, report_date)
    
    def _generate_brief_report(self, results: List[Any], report_date: str) -> str:
        """
        生成Brief模式报告 - 3-5句话概括
        
        格式示例:
        # 03/16 决策简报
        
        > 5只 | 🟢2 🟡2 🔴1
        
        **平安银行(000001)** 🟢 买入 | 评分72
        均线多头排列，MACD金叉，股价突破布林上轨，建议关注。理想买入价12.5，止损12.0。
        
        **贵州茅台(600519)** 🟡 持有 | 评分58  
        高位震荡，获利盘过多有回吐风险，建议观望。当前价格高于主力成本15%。
        """
        if not results:
            return f"# {report_date} 决策简报\n\n无分析结果"
        
        # 统计
        sorted_results = sorted(results, key=lambda x: getattr(x, 'sentiment_score', 0), reverse=True)
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = len(results) - buy_count - sell_count
        
        lines = [
            f"# 📋 {report_date} 决策简报",
            "",
            f"> **{len(results)}只** | 🟢买入{buy_count} 🟡观望{hold_count} 🔴回避{sell_count}",
            ""
        ]
        
        # 生成每只股票摘要
        for r in sorted_results[:5]:  # 最多显示5只
            summary = self._create_stock_brief(r)
            lines.extend([
                f"**{summary.name}({summary.code})** {summary.signal_emoji} {summary.operation} | 评分{summary.score}",
                f"{summary.one_sentence}",
                ""
            ])
        
        lines.append(f"⏰ *{datetime.now().strftime('%m/%d %H:%M')}*")
        
        return "\n".join(lines)
    
    def _create_stock_brief(self, result: Any) -> BriefStockSummary:
        """
        创建单只股票的Brief摘要 - 核心逻辑
        
        3-5句话包含:
        1. 技术面一句话总结
        2. 操作建议 + 理由
        3. 关键价位（如果有）
        4. 风险提示（如果有）
        """
        code = result.code
        name = result.name if result.name and not result.name.startswith('股票') else f'股票{code}'
        score = getattr(result, 'sentiment_score', 50)
        operation = getattr(result, 'operation_advice', '观望')
        
        # 信号表情
        signal_emoji = self._get_signal_emoji(score, operation)
        
        # 构建一句话结论
        sentences = []
        
        # 1. 技术面状态
        tech_summary = self._summarize_technical(result)
        if tech_summary:
            sentences.append(tech_summary)
        
        # 2. 基本面/筹码补充
        fundamental_summary = self._summarize_fundamental_chip(result)
        if fundamental_summary:
            sentences.append(fundamental_summary)
        
        # 3. 操作建议
        advice = self._summarize_advice(result)
        if advice:
            sentences.append(advice)
        
        # 4. 关键价位
        key_price = self._summarize_key_price(result)
        
        # 5. 风险提示
        risk = self._summarize_risk(result)
        
        # 组合成3-5句话
        one_sentence = "；".join(sentences)
        if key_price:
            one_sentence += f" {key_price}"
        if risk:
            one_sentence += f" {risk}"
        
        # 截断到合适长度
        if len(one_sentence) > 120:
            one_sentence = one_sentence[:117] + "..."
        
        return BriefStockSummary(
            code=code,
            name=name,
            signal_emoji=signal_emoji,
            operation=operation,
            score=score,
            one_sentence=one_sentence,
            key_price=key_price,
            risk_note=risk
        )
    
    def _get_signal_emoji(self, score: int, operation: str) -> str:
        """获取信号表情"""
        if score >= 70 or '买入' in operation:
            return "🟢"
        elif score <= 40 or '卖出' in operation:
            return "🔴"
        else:
            return "🟡"
    
    def _summarize_technical(self, result: Any) -> str:
        """总结技术面 - 一句话"""
        parts = []
        
        # 检查各种技术信号
        if hasattr(result, 'ma_signal') and result.ma_signal:
            ma = result.ma_signal
            if '多头排列' in str(ma.description):
                parts.append("均线多头排列")
            elif '空头排列' in str(ma.description):
                parts.append("均线空头排列")
            
            # 乖离率
            if hasattr(ma, 'details') and ma.details:
                bias = ma.details.get('偏离率_MA5', 0)
                if abs(bias) > 5:
                    parts.append(f"偏离MA5达{bias:.1f}%")
        
        if hasattr(result, 'macd_signal') and result.macd_signal:
            macd = result.macd_signal
            if '金叉' in str(macd.description):
                parts.append("MACD金叉")
            elif '死叉' in str(macd.description):
                parts.append("MACD死叉")
        
        if hasattr(result, 'rsi_signal') and result.rsi_signal:
            rsi = result.rsi_signal
            rsi_val = getattr(rsi, 'value', 50)
            if rsi_val > 70:
                parts.append(f"RSI超买({rsi_val:.0f})")
            elif rsi_val < 30:
                parts.append(f"RSI超卖({rsi_val:.0f})")
        
        if hasattr(result, 'boll_signal') and result.boll_signal:
            boll = result.boll_signal
            if '突破上轨' in str(boll.description):
                parts.append("突破布林上轨")
            elif '跌破下轨' in str(boll.description):
                parts.append("跌破布林下轨")
        
        if hasattr(result, 'volume_signal') and result.volume_signal:
            vol = result.volume_signal
            if '放量上涨' in str(vol.description):
                parts.append("放量上涨")
            elif '放量下跌' in str(vol.description):
                parts.append("放量下跌")
        
        return "，".join(parts) if parts else ""
    
    def _summarize_fundamental_chip(self, result: Any) -> str:
        """总结基本面和筹码 - 一句话"""
        parts = []
        
        # 筹码分布
        if hasattr(result, 'chip_distribution') and result.chip_distribution:
            chip = result.chip_distribution
            if chip.concentration < 10:
                parts.append("筹码高度集中")
            
            if chip.profit_ratio > 90:
                parts.append("获利盘过多")
            elif chip.profit_ratio < 10:
                parts.append("套牢盘居多")
        
        # 基本面
        if hasattr(result, 'fundamental') and result.fundamental:
            f = result.fundamental
            if f.valuation.pe_ttm:
                if f.valuation.pe_ttm < 10:
                    parts.append("估值较低")
                elif f.valuation.pe_ttm > 100:
                    parts.append("估值偏高")
            
            if f.growth.profit_growth_yoy and f.growth.profit_growth_yoy > 30:
                parts.append("业绩高增长")
            elif f.growth.profit_growth_yoy and f.growth.profit_growth_yoy < -20:
                parts.append("业绩下滑")
        
        return "，".join(parts) if parts else ""
    
    def _summarize_advice(self, result: Any) -> str:
        """总结操作建议"""
        operation = getattr(result, 'operation_advice', '')
        confidence = getattr(result, 'confidence_level', '中')
        
        if '买入' in operation:
            return f"建议{'积极' if confidence == '高' else '关注'}买入"
        elif '卖出' in operation:
            return "建议减仓回避"
        else:
            return "建议观望等待"
    
    def _summarize_key_price(self, result: Any) -> str:
        """总结关键价位"""
        prices = []
        
        if hasattr(result, 'dashboard') and result.dashboard:
            dash = result.dashboard
            if hasattr(dash, 'battle_plan') and dash.battle_plan:
                battle = dash.battle_plan
                if hasattr(battle, 'sniper_points') and battle.sniper_points:
                    sp = battle.sniper_points
                    if hasattr(sp, 'ideal_buy') and sp.ideal_buy:
                        prices.append(f"理想买入{sp.ideal_buy}")
                    if hasattr(sp, 'stop_loss') and sp.stop_loss:
                        prices.append(f"止损{sp.stop_loss}")
        
        if hasattr(result, 'chip_distribution') and result.chip_distribution:
            chip = result.chip_distribution
            low, high = chip.main_cost_zone
            if low > 0 and high > 0:
                prices.append(f"主力成本{low}-{high}")
        
        return "，".join(prices) if prices else ""
    
    def _summarize_risk(self, result: Any) -> str:
        """总结风险"""
        risks = []
        
        # 技术风险
        if hasattr(result, 'rsi_signal') and result.rsi_signal:
            rsi_val = getattr(result.rsi_signal, 'value', 50)
            if rsi_val > 80:
                risks.append("注意回调风险")
        
        # 筹码风险
        if hasattr(result, 'chip_distribution') and result.chip_distribution:
            chip = result.chip_distribution
            if chip.concentration > 30:
                risks.append("筹码分散")
        
        # 基本面风险
        if hasattr(result, 'risk_alerts') and result.risk_alerts:
            if len(result.risk_alerts) > 0:
                risks.append("存在基本面风险")
        
        return f"【风险】{'，'.join(risks)}" if risks else ""
    
    def _generate_simple_report(self, results: List[Any], 
                                market_summary: Dict,
                                report_date: str) -> str:
        """生成简洁报告"""
        lines = [
            f"# 📊 股票分析报告 - {report_date}",
            "",
            "## 🎯 精选推荐",
            ""
        ]
        
        sorted_results = sorted(results, 
                              key=lambda x: getattr(x, 'sentiment_score', 0), 
                              reverse=True)
        
        for i, r in enumerate(sorted_results[:5], 1):
            score = getattr(r, 'sentiment_score', 0)
            emoji = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
            
            lines.append(f"### {i}. {emoji} {r.name} ({r.code})")
            lines.append(f"- **评分**: {score}/100")
            lines.append(f"- **建议**: {getattr(r, 'operation_advice', '观望')}")
            
            # 核心结论
            if hasattr(r, 'dashboard') and r.dashboard and hasattr(r.dashboard, 'core_conclusion'):
                core = r.dashboard.core_conclusion
                if hasattr(core, 'one_sentence') and core.one_sentence:
                    lines.append(f"- **结论**: {core.one_sentence}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_full_report(self, results: List[Any],
                              market_summary: Dict,
                              report_date: str) -> str:
        """生成完整报告（保持原有格式）"""
        # 这里复用现有的报告生成逻辑
        # 实际项目中可以整合到main.py的_generate_report中
        return self._generate_simple_report(results, market_summary, report_date)


# 便捷函数
def generate_brief_report(results: List[Any], report_date: str = None) -> str:
    """便捷函数：生成Brief报告"""
    generator = ReportGenerator(ReportType.BRIEF)
    return generator.generate_report(results, report_date=report_date)


def generate_simple_report(results: List[Any], 
                           market_summary: Dict = None,
                           report_date: str = None) -> str:
    """便捷函数：生成简洁报告"""
    generator = ReportGenerator(ReportType.SIMPLE)
    return generator.generate_report(results, market_summary, report_date)


if __name__ == "__main__":
    print("🧪 报告生成器测试")
    print("=" * 60)
    
    # 创建模拟数据
    class MockResult:
        def __init__(self, code, name, score, operation, decision_type='hold'):
            self.code = code
            self.name = name
            self.sentiment_score = score
            self.operation_advice = operation
            self.decision_type = decision_type
            self.dashboard = None
            self.chip_distribution = None
            self.fundamental = None
            self.risk_alerts = []
    
    mock_results = [
        MockResult('000001', '平安银行', 72, '买入', 'buy'),
        MockResult('600519', '贵州茅台', 58, '观望', 'hold'),
        MockResult('000002', '万科A', 45, '观望', 'hold'),
        MockResult('300750', '宁德时代', 35, '回避', 'sell'),
    ]
    
    print("\n1. 测试 Brief 模式")
    print("-" * 40)
    brief = generate_brief_report(mock_results)
    print(brief)
    
    print("\n2. 测试 Simple 模式")
    print("-" * 40)
    simple = generate_simple_report(mock_results)
    print(simple[:500] + "...")
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
