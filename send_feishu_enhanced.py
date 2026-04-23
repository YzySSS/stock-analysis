#!/usr/bin/env python3
"""
推送增强版盘前报告到飞书
"""

import requests

FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

# 读取报告
report_path = "/workspace/projects/workspace/股票分析项目/daily_reports/premarket/20260325_114421_premarket_v10.md"

with open(report_path, 'r', encoding='utf-8') as f:
    report_content = f.read()

# 简化内容
lines = report_content.split('\n')
key_content = []

# 包含市场概况和推荐股
sections_to_include = ['📊 市场概况', '🎯 V10板块轮动策略']
for line in lines:
    if any(s in line for s in sections_to_include):
        key_content.append(line)
    elif line.startswith('## '):
        if any(s in line for s in sections_to_include):
            capture = True
        else:
            capture = False
    if 'capture' in dir() and capture:
        key_content.append(line)

# 如果提取失败，使用原始内容
if len(key_content) < 10:
    key_content = lines[:150]

content = '\n'.join(key_content[:120])

# 构建飞书消息
payload = {
    "msg_type": "text",
    "content": {
        "text": f"🌅 盘前选股报告 1.0（3月25日 增强版）\n\n{content[:3500]}\n\n...（完整报告请查看文件）"
    }
}

print("正在发送增强版报告到飞书...")
try:
    response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
    result = response.json()
    if result.get('code') == 0:
        print("✅ 增强版报告推送成功")
    else:
        print(f"❌ 推送失败: {result}")
except Exception as e:
    print(f"❌ 发送失败: {e}")
