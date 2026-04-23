#!/usr/bin/env python3
"""
推送盘中报告到飞书
"""

import requests

FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

# 读取报告
report_path = "/workspace/projects/workspace/股票分析项目/daily_reports/intraday/20260325_113843_intraday_v10.md"

with open(report_path, 'r', encoding='utf-8') as f:
    report_content = f.read()

# 简化内容（提取关键部分）
lines = report_content.split('\n')
key_content = []
in_recommendation = False

for line in lines:
    if '## 🎯 下午选股分析' in line:
        in_recommendation = True
    if in_recommendation:
        key_content.append(line)
    if in_recommendation and line.startswith('---'):
        break

# 也包含市场概况
market_section = []
for line in lines:
    if '## 📈 上午收盘总结' in line:
        capture = True
    if '## 🎯 下午选股分析' in line:
        break
    if 'capture' in dir() and capture:
        market_section.append(line)

content = '\n'.join(market_section[:30] + ['\n---\n'] + key_content[:80])

# 构建飞书消息
payload = {
    "msg_type": "text",
    "content": {
        "text": f"📊 午间简报 1.2（3月25日）\n\n{content[:3500]}"
    }
}

print("正在发送盘中报告到飞书...")
try:
    response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
    result = response.json()
    if result.get('code') == 0:
        print("✅ 盘中报告推送成功")
    else:
        print(f"❌ 推送失败: {result}")
except Exception as e:
    print(f"❌ 发送失败: {e}")
