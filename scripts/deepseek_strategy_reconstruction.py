#!/usr/bin/env python3
"""
调用DeepSeek API分析V12策略重构方案
"""

import os
import json
import requests

# 读取分析请求文档
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.dirname(script_dir)
request_file = os.path.join(workspace_dir, 'docs', 'V12_STRATEGY_RECONSTRUCTION_REQUEST.md')

with open(request_file, 'r', encoding='utf-8') as f:
    analysis_request = f.read()

# API配置
api_key = os.getenv('DEEPSEEK_API_KEY', 'sk-51f77975d58f4f5691e98e110bebdc4c')
base_url = 'https://api.deepseek.com/v1'

# 请求体
payload = {
    "model": "deepseek-reasoner",
    "messages": [
        {
            "role": "system",
            "content": "你是一位资深的量化策略研究员，专精于多因子选股策略设计和优化。请基于提供的策略信息，给出专业、系统、可操作的重构建议。重点关注因子有效性、胜率提升、回撤控制和策略简化。"
        },
        {
            "role": "user",
            "content": f"""请对以下量化选股策略进行全面诊断，并给出重构方案。

{analysis_request}

请深入分析当前策略的问题，设计新的因子组合，给出具体的改进路线图。目标是：胜率从45.5%提升到55%+，回撤从84.6%控制到30%以内。"""
        }
    ],
    "temperature": 0.7,
    "max_tokens": 8000
}

# 发送请求
print("🔄 正在调用DeepSeek进行策略重构分析...")
print(f"   模型: deepseek-reasoner")
print()

try:
    response = requests.post(
        f"{base_url}/chat/completions",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        },
        json=payload,
        timeout=300
    )
    
    if response.status_code == 200:
        result = response.json()
        analysis = result['choices'][0]['message']['content']
        
        # 保存分析结果
        output_file = os.path.join(workspace_dir, 'docs', 'V12_STRATEGY_RECONSTRUCTION_DEEPSEEK.md')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(analysis)
        
        print("✅ 分析完成！")
        print(f"   输出文件: {output_file}")
        print()
        print("="*70)
        print("DEEPSEEK策略重构分析结果：")
        print("="*70)
        print(analysis)
        
    else:
        print(f"❌ API请求失败: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ 错误: {e}")
