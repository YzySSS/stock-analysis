#!/usr/bin/env python3
"""
手动推送今天的盘前报告到飞书 - 简化版
"""

import requests
import json

# 飞书 Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

# 读取报告
report_path = "/workspace/projects/workspace/股票分析项目/daily_reports/premarket/20260325_112849_premarket_v10.md"

with open(report_path, 'r', encoding='utf-8') as f:
    report_content = f.read()

# 简化内容（提取关键部分）
lines = report_content.split('\n')
key_content = []
in_recommendation = False

for line in lines:
    # 跳过持仓分析，只保留推荐部分
    if '## 🎯 V10板块轮动策略' in line:
        in_recommendation = True
    if in_recommendation:
        key_content.append(line)
    if in_recommendation and line.startswith('---'):
        break

content = '\n'.join(key_content[:100])  # 限制长度

# 构建飞书消息
payload = {
    "msg_type": "text",
    "content": {
        "text": f"🌅 盘前选股报告 1.0（3月25日 补发）\n\n{content[:3500]}"
    }
}

print("正在发送报告到飞书...")
try:
    response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
    result = response.json()
    if result.get('code') == 0:
        print("✅ 报告推送成功")
    else:
        print(f"❌ 推送失败: {result}")
except Exception as e:
    print(f"❌ 发送失败: {e}")
