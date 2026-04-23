#!/usr/bin/env python3
"""
股票AI分析器 - 决策仪表盘版
参考 daily_stock_analysis 项目架构
整合：技术面 + 舆情面 + AI决策仪表盘
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import requests

# 加载 .env 文件
try:
    from dotenv import load_dotenv
    # 尝试加载 workspace 目录的 .env
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        # 尝试加载当前工作目录的 .env
        load_dotenv()
except ImportError:
    pass  # python-dotenv 未安装

from news_provider import NewsAggregator
from news_credibility import NewsCredibilityChecker, NewsWithCredibility

logger = logging.getLogger(__name__)


@dataclass
class SniperPoints:
    """狙击点位"""
    ideal_buy: str = ""  # 理想买入点
    secondary_buy: str = ""  # 次优买入点
    stop_loss: str = ""  # 止损位
    take_profit: str = ""  # 目标位


@dataclass
class BattlePlan:
    """作战计划"""
    sniper_points: SniperPoints = None
    position_strategy: str = ""  # 仓位策略
    action_checklist: List[str] = None  # 检查清单


@dataclass
class Intelligence:
    """舆情情报"""
    latest_news: str = ""  # 最新消息
    risk_alerts: List[str] = None  # 风险警报
    positive_catalysts: List[str] = None  # 利好催化
    sentiment_summary: str = ""  # 情绪总结


@dataclass
class DataPerspective:
    """数据透视"""
    trend_status: Dict[str, Any] = None  # 趋势状态
    price_position: Dict[str, Any] = None  # 价格位置
    volume_analysis: Dict[str, Any] = None  # 量能分析


@dataclass
class CoreConclusion:
    """核心结论"""
    one_sentence: str = ""  # 一句话结论
    signal_type: str = ""  # 信号类型
    time_sensitivity: str = ""  # 时间敏感性
    position_advice_no_position: str = ""  # 空仓者建议
    position_advice_has_position: str = ""  # 持仓者建议


@dataclass
class Dashboard:
    """决策仪表盘"""
    core_conclusion: CoreConclusion = None
    data_perspective: DataPerspective = None
    intelligence: Intelligence = None
    battle_plan: BattlePlan = None


@dataclass
class AnalysisResult:
    """AI分析结果"""
    code: str
    name: str
    sentiment_score: int = 50  # 0-100
    trend_prediction: str = ""  # 强烈看多/看多/震荡/看空/强烈看空
    operation_advice: str = ""  # 买入/加仓/持有/减仓/卖出/观望
    decision_type: str = "hold"  # buy/hold/sell
    confidence_level: str = "中"  # 高/中/低
    dashboard: Dashboard = None
    analysis_summary: str = ""
    risk_warning: str = ""
    news_credibility_summary: str = ""  # 新闻可信度汇总
    news_count: int = 0
    news_details: List[Dict] = None  # 新闻详情列表（含链接和可信度）
    success: bool = True
    error_message: str = ""


class StockAIAnalyzer:
    """
    股票AI分析器 - 决策仪表盘版
    
    7大核心交易理念：
    1. 严进策略（乖离率>5%直接观望）
    2. 趋势交易（MA5>MA10>MA20）
    3. 效率优先（筹码集中度<15%）
    4. 买点偏好（缩量回踩MA5）
    5. 风险排查（减持/业绩/监管）
    6. 估值关注（PE合理性）
    7. 强势趋势股放宽
    """
    
    SYSTEM_PROMPT = """你是一位专注于趋势交易的A股投资分析师，负责生成专业的【决策仪表盘】分析报告。

## 核心交易理念（必须严格遵守）

### 1. 严进策略（不追高）
- **绝对不追高**：当股价偏离MA5超过5%时，坚决不买入
- **乖离率公式**：(现价 - MA5) / MA5 × 100%
- 乖离率 < 2%：最佳买点区间
- 乖离率 2-5%：可小仓介入
- 乖离率 > 5%：严禁追高！直接判定为"观望"

### 2. 趋势交易（顺势而为）
- **多头排列必须条件**：MA5 > MA10 > MA20
- 只做多头排列的股票，空头排列坚决不碰
- 均线发散上行优于均线粘合

### 3. 效率优先（筹码结构）
- 关注筹码集中度：90%集中度 < 15% 表示筹码集中
- 获利比例分析：70-90%获利盘时需警惕获利回吐

### 4. 买点偏好（回踩支撑）
- **最佳买点**：缩量回踩MA5获得支撑
- **次优买点**：回踩MA10获得支撑

### 5. 风险排查重点
- 减持公告、业绩预亏、监管处罚、行业政策利空

### 6. 估值关注（PE/PB）
- PE明显偏高时需在风险点中说明

### 7. 强势趋势股放宽
- 强势趋势股可适当放宽乖离率要求，但仍需设置止损

## 输出格式：决策仪表盘 JSON

请严格按照以下JSON格式输出：

```json
{
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "confidence_level": "高/中/低",
    
    "dashboard": {
        "core_conclusion": {
            "one_sentence": "一句话核心结论（30字以内）",
            "signal_type": "🟢买入信号/🟡持有观望/🔴卖出信号",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice_no_position": "空仓者建议",
            "position_advice_has_position": "持仓者建议"
        },
        
        "data_perspective": {
            "trend_status": {
                "ma_alignment": "均线排列状态",
                "is_bullish": true/false,
                "trend_score": 0-100
            },
            "price_position": {
                "current_price": 当前价格,
                "ma5": MA5数值,
                "bias_ma5": 乖离率,
                "bias_status": "安全/警戒/危险"
            },
            "volume_analysis": {
                "volume_ratio": 量比,
                "volume_status": "放量/缩量/平量"
            }
        },
        
        "intelligence": {
            "latest_news": "【最新消息】摘要",
            "risk_alerts": ["风险点1", "风险点2"],
            "positive_catalysts": ["利好1", "利好2"],
            "sentiment_summary": "舆情情绪一句话总结"
        },
        
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "理想买入点：XX元",
                "secondary_buy": "次优买入点：XX元",
                "stop_loss": "止损位：XX元",
                "take_profit": "目标位：XX元"
            },
            "position_strategy": "建议仓位和建仓策略",
            "action_checklist": [
                "✅ 多头排列",
                "⚠️ 乖离率合理",
                "✅ 量能配合",
                "❌ 无重大利空"
            ]
        }
    },
    
    "analysis_summary": "100字综合分析摘要",
    "risk_warning": "风险提示"
}
```

## 评分标准

- **强烈买入（80-100分）**：多头排列+低乖离率+缩量回调+利好
- **买入（60-79分）**：多头排列+乖离率<5%+量能正常
- **观望（40-59分）**：乖离率>5% 或 均线缠绕 或 有风险
- **卖出（0-39分）**：空头排列 或 跌破MA20 或 重大利空"""

    def __init__(self):
        # 优先使用 OpenAI，如果没有则尝试 DeepSeek
        self.api_key = os.getenv("OPENAI_API_KEY") or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("DEEPSEEK_BASE_URL", "https://api.openai.com/v1")
        self.model = os.getenv("OPENAI_MODEL") or os.getenv("AI_MODEL", "gpt-3.5-turbo")
        self.news_aggregator = NewsAggregator()
    
    def analyze(self, 
                code: str, 
                name: str,
                technical_data: Dict = None) -> AnalysisResult:
        """
        分析股票并生成决策仪表盘
        
        Args:
            code: 股票代码
            name: 股票名称
            technical_data: 技术分析数据
        
        Returns:
            AnalysisResult 分析结果
        """
        logger.info(f"开始AI分析: {name}({code})")
        
        try:
            # 1. 获取新闻
            news = self.news_aggregator.get_stock_news(code, name)
            
            # 2. 评估新闻可信度
            news_with_credibility = self._assess_news_credibility(news)
            news_context = self._format_news_with_credibility(news_with_credibility)
            
            # 3. 构建Prompt
            prompt = self._build_prompt(code, name, technical_data, news_context)
            
            # 4. 调用LLM
            response = self._call_llm(prompt)
            
            # 5. 解析结果
            result = self._parse_response(code, name, response, len(news))

            # 6. 附加新闻可信度信息
            result.news_credibility_summary = self._get_credibility_summary(news_with_credibility)
            result.news_details = [n.to_dict() for n in news_with_credibility]

            return result
            
        except Exception as e:
            logger.error(f"AI分析失败: {e}")
            return AnalysisResult(
                code=code,
                name=name,
                success=False,
                error_message=str(e)
            )
    
    def _assess_news_credibility(self, news: List[Dict]) -> List[NewsWithCredibility]:
        """评估新闻可信度"""
        return [NewsWithCredibility(n) for n in news]
    
    def _format_news_with_credibility(self, news_with_credibility: List[NewsWithCredibility]) -> str:
        """格式化新闻为上下文（含可信度）"""
        if not news_with_credibility:
            return "暂无相关新闻"
        
        # 按可信度排序（高可信度优先）
        sorted_news = sorted(news_with_credibility, 
                           key=lambda x: x.credibility.score, 
                           reverse=True)
        
        lines = [f"找到 {len(sorted_news)} 条相关新闻（按可信度排序）："]
        lines.append("\n【新闻可信度说明：S级=官方权威 A级=主流财经 B级=财经媒体 C级=自媒体 D级=可疑来源】\n")
        
        for i, n in enumerate(sorted_news[:5], 1):
            lines.append(f"\n{i}. {n.credibility_color} [{n.credibility.level}级] {n.credibility_emoji}")
            lines.append(f"   标题: {n.title}")
            lines.append(f"   来源: {n.source} (可信度: {n.credibility.score}/100)")
            lines.append(f"   评级: {n.credibility.reason}")
            lines.append(f"   链接: {n.url}")
            lines.append(f"   摘要: {n.content[:150]}...")
        
        # 添加可信度统计
        if sorted_news:
            high_cred = len([n for n in sorted_news if n.credibility.level in ['S', 'A']])
            lines.append(f"\n【可信度统计：高可信度(S/A级) {high_cred}/{len(sorted_news)} 条】")
        
        return "\n".join(lines)
    
    def _get_credibility_summary(self, news_with_credibility: List[NewsWithCredibility]) -> str:
        """获取可信度汇总"""
        if not news_with_credibility:
            return "无新闻数据"
        
        level_count = {'S': 0, 'A': 0, 'B': 0, 'C': 0, 'D': 0}
        for n in news_with_credibility:
            level_count[n.credibility.level] = level_count.get(n.credibility.level, 0) + 1
        
        avg_score = sum(n.credibility.score for n in news_with_credibility) / len(news_with_credibility)
        
        parts = []
        if level_count['S'] > 0:
            parts.append(f"⭐官方权威(S级) {level_count['S']}条")
        if level_count['A'] > 0:
            parts.append(f"主流财经(A级) {level_count['A']}条")
        if level_count['B'] > 0:
            parts.append(f"财经媒体(B级) {level_count['B']}条")
        if level_count['C'] > 0:
            parts.append(f"自媒体(C级) {level_count['C']}条")
        
        return f"共{len(news_with_credibility)}条，平均可信度{avg_score:.0f}/100 | " + " | ".join(parts)
    
    def _build_prompt(self, code: str, name: str, 
                     technical: Dict, news_context: str) -> str:
        """构建分析Prompt"""
        
        tech_str = json.dumps(technical, ensure_ascii=False, indent=2) if technical else "暂无技术数据"
        
        return f"""请分析股票 {name}({code})

**技术分析数据**:
{tech_str}

**新闻舆情**:
{news_context}

请按照系统提示词的格式，输出决策仪表盘JSON。"""

    def _call_llm(self, prompt: str) -> str:
        """调用LLM"""
        if not self.api_key:
            raise ValueError("未配置 AI API Key (需要 OPENAI_API_KEY 或 DEEPSEEK_API_KEY)")
        
        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3
            },
            timeout=120
        )
        
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]
    
    def _parse_response(self, code: str, name: str, 
                       response: str, news_count: int) -> AnalysisResult:
        """解析LLM响应"""
        
        # 提取JSON
        try:
            # 尝试直接解析
            data = json.loads(response)
        except:
            # 从文本中提取JSON
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
            else:
                raise ValueError("无法从响应中提取JSON")
        
        # 解析dashboard
        dashboard_data = data.get("dashboard", {})
        
        core = CoreConclusion(
            one_sentence=dashboard_data.get("core_conclusion", {}).get("one_sentence", ""),
            signal_type=dashboard_data.get("core_conclusion", {}).get("signal_type", ""),
            time_sensitivity=dashboard_data.get("core_conclusion", {}).get("time_sensitivity", ""),
            position_advice_no_position=dashboard_data.get("core_conclusion", {}).get("position_advice_no_position", ""),
            position_advice_has_position=dashboard_data.get("core_conclusion", {}).get("position_advice_has_position", "")
        )
        
        intel = Intelligence(
            latest_news=dashboard_data.get("intelligence", {}).get("latest_news", ""),
            risk_alerts=dashboard_data.get("intelligence", {}).get("risk_alerts", []),
            positive_catalysts=dashboard_data.get("intelligence", {}).get("positive_catalysts", []),
            sentiment_summary=dashboard_data.get("intelligence", {}).get("sentiment_summary", "")
        )
        
        sniper = SniperPoints(**dashboard_data.get("battle_plan", {}).get("sniper_points", {}))
        
        battle = BattlePlan(
            sniper_points=sniper,
            position_strategy=dashboard_data.get("battle_plan", {}).get("position_strategy", ""),
            action_checklist=dashboard_data.get("battle_plan", {}).get("action_checklist", [])
        )
        
        dashboard = Dashboard(
            core_conclusion=core,
            intelligence=intel,
            battle_plan=battle
        )
        
        return AnalysisResult(
            code=code,
            name=name,
            sentiment_score=data.get("sentiment_score", 50),
            trend_prediction=data.get("trend_prediction", ""),
            operation_advice=data.get("operation_advice", ""),
            decision_type=data.get("decision_type", "hold"),
            confidence_level=data.get("confidence_level", "中"),
            dashboard=dashboard,
            analysis_summary=data.get("analysis_summary", ""),
            risk_warning=data.get("risk_warning", ""),
            news_count=news_count,
            success=True
        )


if __name__ == "__main__":
    print("🧪 决策仪表盘AI分析器测试")
    print("="*60)
    
    analyzer = StockAIAnalyzer()
    
    # 测试数据
    test_data = {
        "current_price": 10.85,
        "ma5": 10.72,
        "ma10": 10.65,
        "ma20": 10.58,
        "bias_ma5": 1.21,
        "volume_ratio": 1.15,
        "trend": "多头排列"
    }
    
    print("\n📊 测试分析平安银行...")
    result = analyzer.analyze("000001.SZ", "平安银行", test_data)
    
    if result.success:
        print(f"\n✅ 分析成功!")
        print(f"评分: {result.sentiment_score}/100")
        print(f"建议: {result.operation_advice}")
        if result.dashboard and result.dashboard.core_conclusion:
            print(f"结论: {result.dashboard.core_conclusion.one_sentence}")
    else:
        print(f"❌ 分析失败: {result.error_message}")
