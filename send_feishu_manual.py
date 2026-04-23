#!/usr/bin/env python3
"""
手动推送今天的盘前报告到飞书
"""

import requests
import json
import os

# 飞书 Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

# 读取报告
report_path = "/workspace/projects/workspace/股票分析项目/daily_reports/premarket/20260325_112849_premarket_v10.md"

with open(report_path, 'r', encoding='utf-8') as f:
    report_content = f.read()

# 构建飞书消息
payload = {
    "msg_type": "post",
    "content": {
        "post": {
            "zh_cn": {
                "title": "🌅 盘前选股报告 1.0（3月25日 补发）",
                "content": [
                    [
                        {
                            "tag": "text",
                            "text": report_content[:3000]  # 飞书限制，截取前3000字符
                        }
                    ]
                ]
            }
        }
    }
}

# 如果内容太长，分段发送
if len(report_content) > 3000:
    # 分段处理
    chunks = []
    lines = report_content.split('\n')
    current_chunk = []
    current_len = 0
    
    for line in lines:
        if current_len + len(line) > 2800:
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_len = len(line)
        else:
            current_chunk.append(line)
            current_len += len(line) + 1
    
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    print(f"报告内容较长，将分 {len(chunks)} 段发送")
    
    # 发送第一段（带标题）
    payload["content"]["post"]["zh_cn"]["content"][0][0]["text"] = chunks[0]
    
    try:
        response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
        result = response.json()
        if result.get('code') == 0:
            print(f"✅ 第一段发送成功")
        else:
            print(f"❌ 第一段发送失败: {result}")
    except Exception as e:
        print(f"❌ 发送失败: {e}")
    
    # 发送剩余段落
    for i, chunk in enumerate(chunks[1:], 2):
        payload["msg_type"] = "text"
        payload["content"] = {"text": f"（续{i-1}）\n{chunk[:3000]}"}
        
        try:
            response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
            result = response.json()
            if result.get('code') == 0:
                print(f"✅ 第{i}段发送成功")
            else:
                print(f"❌ 第{i}段发送失败: {result}")
        except Exception as e:
            print(f"❌ 第{i}段发送失败: {e}")
else:
    # 一次性发送
    try:
        response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
        result = response.json()
        if result.get('code') == 0:
            print("✅ 报告推送成功")
        else:
            print(f"❌ 推送失败: {result}")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

print("\n推送完成")
