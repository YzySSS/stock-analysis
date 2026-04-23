#!/usr/bin/env python3
"""搜索股票因子相关文章"""

import os
import json
import urllib.request

TAVILY_URL = "https://api.tavily.com/search"
API_KEY = "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"

def search(query, max_results=5):
    payload = {
        "api_key": API_KEY,
        "query": query,
        "max_results": max_results,
        "search_depth": "advanced",
        "include_answer": True,
        "include_images": False,
        "include_raw_content": False,
    }

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        TAVILY_URL,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode('utf-8', errors='replace')
        return json.loads(body)

# 搜索A股因子策略
print("=" * 70)
print("🔍 搜索1: A股多因子选股策略 有效因子")
print("=" * 70)
result1 = search("A股多因子选股策略 有效因子 IC值 量化投资  Barra", 5)
print(json.dumps(result1, indent=2, ensure_ascii=False))

print("\n" + "=" * 70)
print("🔍 搜索2: 量化投资 因子挖掘")
print("=" * 70)
result2 = search("量化投资 因子挖掘 动量 反转 价值 质量 波动率", 5)
print(json.dumps(result2, indent=2, ensure_ascii=False))
