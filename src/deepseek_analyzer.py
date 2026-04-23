#!/usr/bin/env python3
"""
DeepSeek AI 持仓分析模块
========================
调用 DeepSeek API 分析持仓股票，给出智能卖出点和持仓建议
"""

import os
import json
import requests
from typing import List, Dict, Optional
from datetime import datetime


class DeepSeekAnalyzer:
    """DeepSeek AI 持仓分析器"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.api_base = "https://api.deepseek.com/v1"
        self.model = "deepseek-chat"
        
        if not self.api_key:
            print("⚠️ DeepSeek API Key 未配置")
    
    def analyze_positions(self, positions: List[Dict], market_context: Dict = None) -> Dict:
        """
        分析持仓股票，给出卖出点和持仓建议
        
        Args:
            positions: 持仓列表
            market_context: 市场环境信息
            
        Returns:
            AI分析结果
        """
        if not self.api_key:
            return self._fallback_analysis(positions)
        
        try:
            # 构建提示词
            prompt = self._build_prompt(positions, market_context)
            
            # 调用 DeepSeek API
            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "你是一位专业的股票分析师，擅长技术分析和风险控制。请基于提供的数据给出具体的卖出点和持仓建议。"
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.3,
                    "max_tokens": 2000
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                analysis = result['choices'][0]['message']['content']
                return {
                    'success': True,
                    'analysis': analysis,
                    'source': 'deepseek'
                }
            else:
                print(f"⚠️ DeepSeek API 错误: {response.status_code} - {response.text}")
                return self._fallback_analysis(positions)
                
        except Exception as e:
            print(f"⚠️ DeepSeek 分析失败: {e}")
            return self._fallback_analysis(positions)
    
    def _build_prompt(self, positions: List[Dict], market_context: Dict = None) -> str:
        """构建分析提示词"""
        prompt = "请分析以下持仓股票，给出具体的卖出点和持仓建议：\n\n"
        
        # 添加市场环境
        if market_context:
            prompt += f"【市场环境】\n"
            prompt += f"- 大盘涨跌: {market_context.get('avg_change', 0):+.2f}%\n"
            prompt += f"- 上涨家数: {market_context.get('up_count', 0)}\n"
            prompt += f"- 下跌家数: {market_context.get('down_count', 0)}\n\n"
        
        # 添加持仓详情
        prompt += "【持仓详情】\n"
        for i, p in enumerate(positions, 1):
            prompt += f"\n{i}. {p.get('name')} ({p.get('code')})\n"
            prompt += f"   - 买入价: ¥{p.get('buy_price')}\n"
            prompt += f"   - 当前价: ¥{p.get('current_price')}\n"
            prompt += f"   - 收益率: {p.get('current_return', 0):+.2f}%\n"
            prompt += f"   - 持股数: {p.get('shares')}\n"
            prompt += f"   - 买入日期: {p.get('buy_date')}\n"
            
            if p.get('stop_loss'):
                prompt += f"   - 止损价: ¥{p.get('stop_loss')}\n"
            if p.get('target_price'):
                prompt += f"   - 目标价: ¥{p.get('target_price')}\n"
        
        prompt += "\n【分析要求】\n"
        prompt += "请对每只股票给出以下分析：\n"
        prompt += "1. **操作建议**: 持有/减仓/清仓\n"
        prompt += "2. **目标卖出价**: 具体价格\n"
        prompt += "3. **止损调整**: 是否需要调整止损价\n"
        prompt += "4. **理由**: 基于技术面/基本面的分析\n"
        prompt += "5. **风险提示**: 主要风险点\n"
        prompt += "\n请以 Markdown 格式输出，便于阅读。"
        
        return prompt
    
    def _fallback_analysis(self, positions: List[Dict]) -> Dict:
        """备用分析（当API不可用时）"""
        lines = ["## 🤖 AI 持仓分析\n"]
        lines.append("*使用内置策略分析（DeepSeek API 暂不可用）*\n")
        
        for p in positions:
            code = p.get('code')
            name = p.get('name')
            buy_price = p.get('buy_price', 0)
            current_price = p.get('current_price', 0)
            return_pct = p.get('current_return', 0)
            stop_loss = p.get('stop_loss', 0)
            target_price = p.get('target_price', 0)
            
            lines.append(f"### {name} ({code})\n")
            
            # 基于规则的简单分析
            if return_pct >= 10:
                action = "🟡 考虑减仓"
                sell_price = round(target_price * 0.95, 2)
                reason = "收益率已超过10%，建议分批止盈，锁定部分利润。"
            elif return_pct >= 5:
                action = "🟢 持有观望"
                sell_price = target_price
                reason = "收益率良好，继续持有至目标价。"
            elif return_pct > -3:
                action = "🟢 持有"
                sell_price = target_price
                reason = "小幅波动，保持原有策略。"
            elif current_price <= stop_loss:
                action = "🔴 建议止损"
                sell_price = round(current_price * 0.98, 2)
                reason = f"已触发止损价¥{stop_loss}，建议严格执行止损。"
            else:
                action = "🟡 密切关注"
                sell_price = stop_loss
                reason = "处于亏损状态，关注是否触发止损。"
            
            lines.append(f"**操作建议**: {action}\n")
            lines.append(f"**建议卖出价**: ¥{sell_price}\n")
            lines.append(f"**分析理由**: {reason}\n")
            
            # 风险提示
            if return_pct > 0:
                lines.append(f"**风险提示**: 注意回调风险，可考虑分批止盈。\n")
            else:
                lines.append(f"**风险提示**: 跌破止损价应果断卖出，避免深套。\n")
            
            lines.append("---\n")
        
        return {
            'success': True,
            'analysis': '\n'.join(lines),
            'source': 'fallback'
        }
    
    def quick_suggestion(self, code: str, name: str, buy_price: float, 
                         current_price: float, market_trend: str = "震荡") -> Dict:
        """快速获取单只股票建议"""
        position = {
            'code': code,
            'name': name,
            'buy_price': buy_price,
            'current_price': current_price,
            'current_return': round((current_price - buy_price) / buy_price * 100, 2),
            'shares': 100
        }
        
        return self.analyze_positions([position], {
            'avg_change': 0 if market_trend == "震荡" else 1 if market_trend == "上涨" else -1,
            'up_count': 2000,
            'down_count': 1500
        })


# 全局分析器实例
deepseek_analyzer = DeepSeekAnalyzer()


class StockPickerAnalyzer:
    """选股分析器 - 根据V9因子得分生成详细的选股说明"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.api_base = "https://api.deepseek.com/v1"
        self.model = "deepseek-chat"

    def analyze_pick(self, pick: Dict, market_context: str = "") -> str:
        """
        分析单只选股，生成详细说明

        Args:
            pick: 选股结果字典
            market_context: 市场环境描述

        Returns:
            选股分析说明
        """
        if not self.api_key:
            return self._generate_fallback_analysis(pick)

        try:
            prompt = self._build_pick_prompt(pick, market_context)

            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "你是一位专业的量化选股分析师，擅长根据多因子评分模型分析股票。请基于V9选股模型的因子得分，给出简洁但专业的选股理由。"
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.3,
                    "max_tokens": 300
                },
                timeout=15
            )

            if response.status_code == 200:
                result = response.json()
                analysis = result['choices'][0]['message']['content'].strip()
                return analysis
            else:
                print(f"⚠️ DeepSeek API 错误: {response.status_code}")
                return self._generate_fallback_analysis(pick)

        except Exception as e:
            print(f"⚠️ 选股分析失败: {e}")
            return self._generate_fallback_analysis(pick)

    def _build_pick_prompt(self, pick: Dict, market_context: str) -> str:
        """构建选股分析提示词"""
        factors = pick.get('factors', {})

        prompt = f"""请分析以下V9选股模型选出的股票：

【股票信息】
- 名称: {pick.get('name')} ({pick.get('code')})
- 当前价: ¥{pick.get('price', 0):.2f} ({pick.get('change_pct', 0):+.2f}%)
- 所属板块: {pick.get('sector', '其他')}
- 板块龙头: {'是' if pick.get('is_sector_leader') else '否'}

【V9因子得分】(满分100)
- 技术因子(25分): {factors.get('technical', 'N/A')}分 - 衡量价格趋势和突破强度
- 情绪因子(20分): {factors.get('sentiment', 'N/A')}分 - 衡量市场情绪和资金流向
- 板块因子(30分): {factors.get('sector', 'N/A')}分 - 衡量板块轮动强度
- 资金因子(15分): {factors.get('money_flow', 'N/A')}分 - 衡量资金流入情况
- 风险因子(10分): {factors.get('risk', 'N/A')}分 - 衡量波动率和风险
- 综合评分: {pick.get('total_score', 0)}分

【市场环境】
{market_context if market_context else '当前市场震荡'}

【分析要求】
请用2-3句话给出选股理由，要求：
1. 指出该股票的核心优势（基于高分因子）
2. 说明适合的投资者类型（激进/稳健）
3. 提及需要注意的风险点
4. 语言简洁专业，控制在80字以内

请直接给出选股理由，不要分点。"""

        return prompt

    def _generate_fallback_analysis(self, pick: Dict) -> str:
        """备用分析（当API不可用时）"""
        factors = pick.get('factors', {})
        total_score = pick.get('total_score', 0)
        sector = pick.get('sector', '其他')
        is_leader = pick.get('is_sector_leader', False)

        # 找出最高分因子
        factor_scores = {
            '技术面': factors.get('technical', 0),
            '情绪面': factors.get('sentiment', 0),
            '板块轮动': factors.get('sector', 0),
            '资金面': factors.get('money_flow', 0),
            '风控': factors.get('risk', 0)
        }
        best_factor = max(factor_scores, key=factor_scores.get)
        best_score = factor_scores[best_factor]

        # 生成分析
        parts = []

        # 核心优势
        if best_score >= 20:
            parts.append(f"{best_factor}表现优异({best_score:.0f}分)")
        elif total_score >= 70:
            parts.append("各因子均衡，综合评分优秀")
        else:
            parts.append("综合评分良好，符合选股标准")

        # 板块因素
        if is_leader:
            parts.append(f"，为{sector}板块龙头")
        elif sector != '其他':
            parts.append(f"，受益于{sector}板块轮动")

        # 投资者类型和风险
        if total_score >= 75:
            parts.append("。适合激进型投资者，建议关注开盘表现，注意控制仓位风险。")
        elif total_score >= 70:
            parts.append("。适合稳健型投资者，可关注低吸机会，设好止损位。")
        else:
            parts.append("。适合保守型投资者，建议观察后再决策，注意市场波动风险。")

        return "".join(parts)


    def analyze_position(self, position: Dict) -> str:
        """
        分析单只持仓，生成专业建议

        Args:
            position: 持仓信息字典

        Returns:
            持仓分析说明
        """
        if not self.api_key:
            return self._generate_position_fallback_analysis(position)

        try:
            prompt = self._build_position_prompt(position)

            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "你是一位专业的持仓管理顾问，擅长根据持仓盈亏状态给出具体的操作建议。请用简洁专业的语言给出分析。"
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.3,
                    "max_tokens": 200
                },
                timeout=10
            )

            if response.status_code == 200:
                result = response.json()
                analysis = result['choices'][0]['message']['content'].strip()
                return analysis
            else:
                return self._generate_position_fallback_analysis(position)

        except Exception as e:
            print(f"⚠️ 持仓分析失败: {e}")
            return self._generate_position_fallback_analysis(position)

    def _build_position_prompt(self, position: Dict) -> str:
        """构建持仓分析提示词"""
        profit_pct = position.get('profit_pct', 0)
        profit = position.get('profit', 0)
        buy_price = position.get('buy_price', 0)
        current_price = position.get('current_price', 0)
        stop_loss = position.get('stop_loss', 0)
        target_price = position.get('target_price', 0)

        # 确定盈亏状态
        if profit_pct >= 5:
            status = "盈利较好"
            status_desc = f"盈利{profit_pct:.1f}%"
        elif profit_pct >= 0:
            status = "小幅盈利"
            status_desc = f"盈利{profit_pct:.1f}%"
        elif profit_pct >= -5:
            status = "小幅亏损"
            status_desc = f"亏损{profit_pct:.1f}%"
        else:
            status = "亏损较大"
            status_desc = f"亏损{profit_pct:.1f}%"

        prompt = f"""请分析以下持仓股票并给出操作建议：

【股票信息】
- 名称: {position.get('name')} ({position.get('code')})
- 买入价: ¥{buy_price:.3f}
- 当前价: ¥{current_price:.3f}
- 持仓: {position.get('shares'):,}股
- 盈亏状态: {status_desc}
- 止损价: ¥{stop_loss:.3f}
- 目标价: ¥{target_price:.3f}

【分析要求】
请用1-2句话给出专业建议，要求：
1. 基于当前盈亏状态给出操作方向
2. 提及关键价位（止损价/目标价）
3. 提示主要风险点
4. 语言简洁专业，控制在60字以内

请直接给出分析建议。"""

        return prompt

    def _generate_position_fallback_analysis(self, position: Dict) -> str:
        """生成备用持仓分析"""
        profit_pct = position.get('profit_pct', 0)
        stop_loss = position.get('stop_loss', 0)
        target_price = position.get('target_price', 0)
        current_price = position.get('current_price', 0)

        parts = []

        if profit_pct >= 5:
            parts.append(f"盈利{profit_pct:.1f}%，趋势良好，可继续持有")
            parts.append(f"，建议关注¥{target_price:.2f}目标价，可考虑分批止盈")
        elif profit_pct >= 0:
            parts.append(f"小幅盈利{profit_pct:.1f}%，正常波动")
            parts.append(f"，建议设好止损位¥{stop_loss:.2f}，等待趋势明朗")
        elif profit_pct >= -5:
            parts.append(f"小幅亏损{profit_pct:.1f}%，关注支撑位")
            if current_price <= stop_loss * 1.02:
                parts.append(f"，已接近止损价¥{stop_loss:.2f}，如跌破建议果断减仓")
            else:
                parts.append(f"，如跌破¥{stop_loss:.2f}止损价建议减仓")
        else:
            parts.append(f"亏损{profit_pct:.1f}%，已触发关注阈值")
            if current_price <= stop_loss:
                parts.append(f"，已跌破止损价¥{stop_loss:.2f}，建议严格执行止损避免深套")
            else:
                parts.append(f"，关注是否跌破止损价¥{stop_loss:.2f}，准备减仓或止损")

        return "".join(parts)


# 全局选股分析器实例
stock_picker_analyzer = StockPickerAnalyzer()


if __name__ == "__main__":
    print("🧪 DeepSeek 持仓分析测试")
    print("="*60)
    
    # 测试数据
    test_positions = [
        {
            'code': '000001',
            'name': '平安银行',
            'buy_price': 10.5,
            'current_price': 11.2,
            'current_return': 6.67,
            'shares': 1000,
            'buy_date': '2026-03-15',
            'stop_loss': 9.5,
            'target_price': 12.0
        },
        {
            'code': '600519',
            'name': '贵州茅台',
            'buy_price': 1500.0,
            'current_price': 1450.0,
            'current_return': -3.33,
            'shares': 100,
            'buy_date': '2026-03-10',
            'stop_loss': 1400.0,
            'target_price': 1700.0
        }
    ]
    
    analyzer = DeepSeekAnalyzer()
    result = analyzer.analyze_positions(test_positions, {
        'avg_change': 0.5,
        'up_count': 2500,
        'down_count': 2000
    })
    
    print("\n📊 分析结果:")
    print(f"来源: {result.get('source', 'unknown')}")
    print("\n" + result.get('analysis', '无分析结果'))
