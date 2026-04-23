#!/usr/bin/env python3
"""
生成示例盘前报告（含技术位分析）
用于展示新功能效果
"""

import sys
sys.path.insert(0, 'src')

from datetime import datetime

def generate_sample_report():
    """生成示例报告"""
    
    # 模拟选股结果
    sample_picks = [
        {
            'name': '平安银行',
            'code': '000001',
            'price': 12.85,
            'change_pct': 2.35,
            'total_score': 78.5,
            'sector': '银行',
            'factors': {
                'technical': 16.5,
                'sentiment': 2.8,
                'sector': 28.0,
                'money_flow': 15.5,
                'risk': 12.0,
                'news_sentiment': 3.7
            },
            'technical_levels': {
                'support_1': 12.45,
                'support_2': 11.90,
                'resistance_1': 13.20,
                'resistance_2': 13.85,
                'stop_loss': 12.15,
                'target_price': 13.55,
                'support_basis': 'MA20支撑(¥12.48)',
                'resistance_basis': '20日高点(¥13.15)'
            }
        },
        {
            'name': '宁德时代',
            'code': '300750',
            'price': 185.60,
            'change_pct': 4.12,
            'total_score': 82.3,
            'sector': '新能源',
            'is_sector_leader': True,
            'factors': {
                'technical': 18.2,
                'sentiment': 2.5,
                'sector': 30.5,
                'money_flow': 17.0,
                'risk': 10.5,
                'news_sentiment': 3.6
            },
            'technical_levels': {
                'support_1': 178.50,
                'support_2': 172.30,
                'resistance_1': 192.00,
                'resistance_2': 205.50,
                'stop_loss': 175.00,
                'target_price': 200.00,
                'support_basis': '20日低点(¥178.20)',
                'resistance_basis': '前高压力'
            }
        },
        {
            'name': '贵州茅台',
            'code': '600519',
            'price': 1588.00,
            'change_pct': 0.85,
            'total_score': 75.2,
            'sector': '白酒',
            'factors': {
                'technical': 15.0,
                'sentiment': 2.2,
                'sector': 25.0,
                'money_flow': 16.5,
                'risk': 14.0,
                'news_sentiment': 2.5
            },
            'technical_levels': {
                'support_1': 1545.00,
                'support_2': 1498.00,
                'resistance_1': 1620.00,
                'resistance_2': 1680.00,
                'stop_loss': 1520.00,
                'target_price': 1650.00,
                'support_basis': 'MA20支撑(¥1548.50)',
                'resistance_basis': '20日高点(¥1615.00)'
            }
        }
    ]
    
    # 生成报告
    lines = [
        "# 🌅 盘前选股报告 1.0（含技术位分析）",
        "",
        f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "**选股引擎**: V9板块轮动增强",
        "**报告引擎**: V10+（技术位增强版）",
        f"**股票数量**: {len(sample_picks)} 只",
        "",
        "======================================================================",
        "",
        "## 🎯 V10板块轮动策略 - TOP 推荐",
        "======================================================================",
        "因子权重: 技术20% | 情绪3% | 板块33% | 资金19% | 风险15% | 舆情7%",
        "**新增**: 关键支撑/阻力位、止损价、目标价、风险收益比",
        "----------------------------------------------------------------------",
        "",
        "### 📊 板块分布",
        "",
        "- **银行**: 1只",
        "- **新能源**: 1只 🌟",
        "- **白酒**: 1只",
        "",
        "---",
        "",
    ]
    
    for i, pick in enumerate(sample_picks, 1):
        score = pick['total_score']
        change_pct = pick['change_pct']
        price = pick['price']
        levels = pick.get('technical_levels', {})
        
        if score >= 75:
            suggestion = "⭐ 强烈推荐"
        elif score >= 70:
            suggestion = "✅ 推荐关注"
        else:
            suggestion = "📈 可关注"
        
        leader_tag = " [板块龙头]" if pick.get('is_sector_leader') else ""
        
        lines.extend([
            f"{i}. ⭐ **{pick['name']}** ({pick['code']}){leader_tag}",
            f"   - 当前价: ¥{price:.2f} ({change_pct:+.2f}%)",
            f"   - 综合评分: **{score:.1f}**",
            f"   - 因子得分: 技术{pick['factors']['technical']} | 情绪{pick['factors']['sentiment']} | 板块{pick['factors']['sector']} | 资金{pick['factors']['money_flow']} | 风险{pick['factors']['risk']} | 舆情{pick['factors']['news_sentiment']}",
            f"   - 所属板块: {pick['sector']}",
            f"   - 选股说明: **{suggestion}**，技术面突破+板块轮动强势，资金持续流入",
            f"   - 技术位分析:",
            f"      📉 支撑位: ¥{levels.get('support_1', 'N/A'):.2f}(强) / ¥{levels.get('support_2', 'N/A'):.2f}(弱)",
            f"         依据: {levels.get('support_basis', 'N/A')}",
            f"      📈 阻力位: ¥{levels.get('resistance_1', 'N/A'):.2f}(近) / ¥{levels.get('resistance_2', 'N/A'):.2f}(远)",
            f"         依据: {levels.get('resistance_basis', 'N/A')}",
            f"      🛡️ 止损价: ¥{levels.get('stop_loss', 'N/A'):.2f}",
            f"      🎯 目标价: ¥{levels.get('target_price', 'N/A'):.2f}",
        ])
        
        # 计算风险收益比
        stop_loss = levels.get('stop_loss', 0)
        target = levels.get('target_price', 0)
        if stop_loss > 0 and target > 0 and price > 0:
            risk = price - stop_loss
            reward = target - price
            if risk > 0:
                rr_ratio = reward / risk
                lines.append(f"      ⚖️ 风险收益比: 1:{rr_ratio:.1f}")
        
        lines.append("")
    
    lines.extend([
        "======================================================================",
        "",
        "### 📋 操作建议",
        "",
        "| 股票 | 建议 | 入场区间 | 止损 | 目标 | 仓位 |",
        "|------|------|----------|------|------|------|",
    ])
    
    for pick in sample_picks:
        levels = pick.get('technical_levels', {})
        price = pick['price']
        sup1 = levels.get('support_1', price * 0.98)
        lines.append(f"| {pick['name']} | 关注 | ¥{sup1:.2f}-{price:.2f} | ¥{levels.get('stop_loss', 'N/A'):.2f} | ¥{levels.get('target_price', 'N/A'):.2f} | 20% |")
    
    lines.extend([
        "",
        "### 💡 使用说明",
        "",
        "**支撑位**: 股价下跌时可能获得支撑的价格区间",
        "- 强支撑（支撑1）：首次回调可考虑加仓",
        "- 弱支撑（支撑2）：跌破需警惕，考虑减仓",
        "",
        "**阻力位**: 股价上涨时可能遇到压力的价格区间",
        "- 近阻力（阻力1）：首次触及可考虑部分止盈",
        "- 远阻力（阻力2）：突破后打开上涨空间",
        "",
        "**止损价**: 跌破此价位建议离场，控制亏损",
        "**目标价**: 达到此价位建议分批止盈",
        "**风险收益比**: 建议 >= 1:1.5，数值越高越划算",
        "",
        "======================================================================",
        "",
        "⚠️ **风险提示**: 以上分析仅供参考，不构成投资建议。",
        "",
        "---",
        "*股票分析项目 1.4 | V10-P2 | 技术位增强版*"
    ])
    
    return "\n".join(lines)


if __name__ == "__main__":
    report = generate_sample_report()
    print(report)
