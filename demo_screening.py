#!/usr/bin/env python3
"""
股票筛选器演示 - 展示筛选逻辑
"""

print("="*70)
print("股票筛选器 - 功能演示")
print("="*70)
print()

# 模拟筛选结果（实际运行时会从市场获取）
mock_results = [
    {
        "name": "平安银行",
        "code": "000001.SZ",
        "price": 10.85,
        "change_pct": 2.35,
        "score": 78,
        "reason": "今日涨幅2.35%，均线金叉形成，资金关注度高"
    },
    {
        "name": "贵州茅台", 
        "code": "600519.SH",
        "price": 1652.00,
        "change_pct": 1.82,
        "score": 72,
        "reason": "品牌护城河深，基本面稳健，今日放量上涨"
    },
    {
        "name": "万科A",
        "code": "000002.SZ", 
        "price": 15.68,
        "change_pct": -0.52,
        "score": 45,
        "reason": "债务压力仍存，但部分债务获展期，观察"
    },
]

print("筛选策略: 综合筛选（涨幅 + 量能 + 技术指标）")
print("-"*70)

for i, r in enumerate(mock_results, 1):
    emoji = "G" if r["change_pct"] > 5 else "Y" if r["change_pct"] > 0 else "R"
    print(f"{i}. [{emoji}] {r['name']} ({r['code']})")
    print(f"   价格: {r['price']:.2f} | 涨跌: {r['change_pct']:+.2f}% | AI评分: {r['score']}")
    print(f"   入选理由: {r['reason']}")
    print()

print("-"*70)
print("✅ 筛选逻辑:")
print("   1. 获取今日全部A股行情")
print("   2. 按涨幅排序（排除ST股）")
print("   3. 筛选量比>2的放量股")
print("   4. 技术指标分析（金叉/多头排列）")
print("   5. 综合打分，取Top N")
print()
print("="*70)
print("使用方法:")
print()
print("# 模式1: AI自动选股（推荐）")
print("python3 main.py --auto-screen")
print("python3 main.py --auto-screen --screen-count 20")
print()
print("# 模式2: 分析指定股票")
print("python3 main.py --stocks 000001.SZ,000002.SZ,600519.SH")
print()
print("注意: 交易时间（9:30-15:00）数据源更稳定")
print("="*70)
