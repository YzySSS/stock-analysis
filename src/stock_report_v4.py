#!/usr/bin/env python3
"""
股票报告 V4.0 - AI增强版
整合：技术分析 + 舆情分析 + AI决策
复用 daily_stock_analysis 项目架构
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from stock_report_v3 import StockReportV3
from ai_sentiment_analyzer import AIStockAnalyzer, StrategyLoader
from technical_analysis import TechnicalAnalyzer
from backtest import BacktestEngine
from notification import NotificationManager

import pandas as pd
from datetime import datetime
from typing import Dict, List


class StockReportV4:
    """
    V4版本 - AI增强型股票报告
    整合传统技术指标 + AI舆情分析 + 智能决策
    """
    
    def __init__(self, notification_config: Dict = None):
        self.v3 = StockReportV3(notification_config)
        self.ai_analyzer = AIStockAnalyzer()
        self.technical_analyzer = TechnicalAnalyzer()
        self.backtest_engine = BacktestEngine()
        self.notifier = self.v3.notifier
    
    def generate_ai_report(self,
                          stock_codes: List[str],
                          strategy: str = "ma_golden_cross",
                          send_notification: bool = True) -> Dict:
        """
        生成AI增强版报告
        
        Args:
            stock_codes: 股票代码列表
            strategy: AI策略名称
            send_notification: 是否推送
        """
        print("🚀 生成AI增强版报告 V4.0")
        print("="*70)
        
        report = {
            "version": "4.0",
            "date": datetime.now().isoformat(),
            "strategy": strategy,
            "market_summary": self.v3._get_market_summary(),
            "ai_analysis": [],
            "recommendations": []
        }
        
        # 获取策略定义
        strategy_def = self.ai_analyzer.strategy_loader.get_strategy(strategy)
        if strategy_def:
            print(f"📋 使用策略: {strategy_def['display_name']}")
            print(f"   {strategy_def['description']}")
        
        # 分析每只股票
        for code in stock_codes:
            try:
                analysis = self._analyze_stock_ai(code, strategy)
                if analysis:
                    report["ai_analysis"].append(analysis)
                    print(f"  ✅ {code} AI分析完成")
            except Exception as e:
                print(f"  ⚠️ {code} 分析失败: {e}")
        
        # 生成推荐
        report["recommendations"] = self._generate_ai_recommendations(
            report["ai_analysis"]
        )
        
        # 发送推送
        if send_notification and self.notifier:
            self._send_ai_report(report)
        
        return report
    
    def _analyze_stock_ai(self, code: str, strategy: str) -> Dict:
        """AI分析单只股票"""
        
        # 1. 获取历史数据
        df = self.v3.data_source.get_stock_history(code, days=365) if self.v3.data_source else None
        
        # 2. 技术分析（如果有数据）
        technical = None
        if df is not None and len(df) > 60:
            indicators = self.technical_analyzer.analyze(df)
            latest = df.iloc[-1]
            
            technical = {
                "price": latest["close"],
                "change": (latest["close"] - latest["open"]) / latest["open"] * 100,
                "composite_score": indicators["composite_score"],
                "signal": indicators["signal"],
                "ma": indicators.get("ma", {}),
                "macd": indicators.get("macd", {}),
                "rsi": indicators.get("rsi", {})
            }
            
            # 回测
            try:
                bt = self.backtest_engine.run_backtest(df, strategy="composite")
                technical["backtest"] = {
                    "total_return": f"{bt.total_return:.2%}",
                    "win_rate": f"{bt.win_rate:.2%}"
                }
            except:
                pass
        
        # 3. AI综合分析
        # 从代码提取名称（简化处理）
        name = code.replace(".SZ", "").replace(".SH", "")
        
        ai_result = self.ai_analyzer.analyze(
            stock_code=code,
            stock_name=name,
            strategy=strategy,
            technical_data=technical or {}
        )
        
        return ai_result
    
    def _generate_ai_recommendations(self, analyses: List[Dict]) -> List[Dict]:
        """生成AI推荐"""
        
        # 按置信度排序
        sorted_stocks = sorted(
            analyses,
            key=lambda x: x.get("decision", {}).get("confidence", 0),
            reverse=True
        )
        
        recommendations = []
        for stock in sorted_stocks[:5]:
            decision = stock.get("decision", {})
            sentiment = stock.get("sentiment", {})
            
            rec = {
                "code": stock["code"],
                "name": stock["name"],
                "action": decision.get("action", "hold"),
                "confidence": decision.get("confidence", 0),
                "reason": decision.get("reason", ""),
                "entry_price": decision.get("entry_price"),
                "stop_loss": decision.get("stop_loss"),
                "target_price": decision.get("target_price"),
                "risk_level": decision.get("risk_level", "medium"),
                "sentiment_score": sentiment.get("score", 50),
                "sentiment_summary": sentiment.get("summary", ""),
                "checklist": decision.get("checklist", []),
                "data_source": "AI分析 + AkShare",
                "analysis_method": "技术指标 + 舆情分析 + LLM决策"
            }
            
            recommendations.append(rec)
        
        return recommendations
    
    def _send_ai_report(self, report: Dict):
        """发送AI报告"""
        if not self.notifier:
            return
        
        try:
            # 构建增强版消息
            title = f"📊 AI选股报告 V4 - {datetime.now().strftime('%m/%d')}"
            
            # 生成Markdown消息
            lines = [
                f"# {title}",
                f"",
                f"**策略**: {report['strategy']}",
                f"**时间**: {report['date']}",
                f"",
                "---",
                "",
                "## 🎯 AI精选推荐",
                ""
            ]
            
            for i, rec in enumerate(report["recommendations"], 1):
                action_emoji = {
                    "buy": "🟢", "sell": "🔴", "hold": "⚪"
                }.get(rec["action"], "⚪")
                
                lines.extend([
                    f"### {i}. {action_emoji} {rec['name']} ({rec['code']})",
                    f"- **操作建议**: {rec['action'].upper()} (置信度: {rec['confidence']}%)",
                    f"- **推荐理由**: {rec['reason']}",
                    f"- **舆情评分**: {rec['sentiment_score']}/100 - {rec['sentiment_summary']}",
                ])
                
                if rec.get("entry_price"):
                    lines.append(f"- **买入区间**: {rec['entry_price']}")
                if rec.get("stop_loss"):
                    lines.append(f"- **止损价位**: {rec['stop_loss']}")
                if rec.get("target_price"):
                    lines.append(f"- **目标价位**: {rec['target_price']}")
                
                # 检查清单
                if rec.get("checklist"):
                    lines.append("- **检查清单**:")
                    for item in rec["checklist"]:
                        lines.append(f"  - {item}")
                
                lines.append("")
            
            # 添加说明
            lines.extend([
                "---",
                "",
                "## 📋 分析方法",
                "",
                "- **技术分析**: MA/MACD/RSI/布林带",
                "- **舆情分析**: 新闻搜索 + AI情感分析",
                "- **决策引擎**: LLM综合评估",
                "",
                "⚠️ **免责声明**: 本报告仅供参考，不构成投资建议",
                f"⏰ **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            ])
            
            message = "\n".join(lines)
            
            # 发送到飞书
            self.notifier._send_feishu(title, message)
            print("  ✅ AI报告已推送")
            
        except Exception as e:
            print(f"  ❌ 推送失败: {e}")


def main():
    """测试入口"""
    print("🚀 AI增强版股票报告 V4.0")
    print("="*70)
    
    # 检查API配置
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️ 未配置 OPENAI_API_KEY，AI功能将不可用")
        print("   请设置环境变量: export OPENAI_API_KEY=your_key")
    
    if not os.getenv("TAVILY_API_KEY") and not os.getenv("SERPAPI_KEY"):
        print("⚠️ 未配置新闻搜索API，舆情分析将跳过")
        print("   可选: export TAVILY_API_KEY=your_key")
    
    print("\n使用方式:")
    print("  1. 配置API Key")
    print("  2. from stock_report_v4 import StockReportV4")
    print("  3. report = StockReportV4().generate_ai_report(['000001.SZ'])")
    
    # 列出可用策略
    loader = StrategyLoader()
    print("\n📋 可用AI策略:")
    for s in loader.list_strategies():
        strategy = loader.get_strategy(s)
        print(f"  - {s}: {strategy['display_name']}")


if __name__ == "__main__":
    main()
