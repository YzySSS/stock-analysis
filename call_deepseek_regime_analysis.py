#!/usr/bin/env python3
"""调用DeepSeek分析市场环境判断方案"""

import os
import requests
import sys

def main():
    # 读取API Key
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        # 尝试从配置文件读取
        try:
            with open('/root/.openclaw/workspace/股票分析项目/config/env.sh', 'r') as f:
                for line in f:
                    if 'DEEPSEEK_API_KEY' in line:
                        api_key = line.split('=')[1].strip().strip('"').strip("'")
                        break
        except:
            pass
    
    if not api_key:
        print("Error: DEEPSEEK_API_KEY not found")
        sys.exit(1)
    
    # 读取分析请求文档
    with open('market_regime_analysis_request.md', 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("正在调用DeepSeek API进行分析...")
    print("=" * 70)
    
    response = requests.post(
        'https://api.deepseek.com/v1/chat/completions',
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        },
        json={
            'model': 'deepseek-chat',
            'messages': [
                {'role': 'system', 'content': '你是量化投资策略分析专家，擅长市场状态判断和策略切换系统设计。请提供详细、专业的分析，包含具体的数据支持和可执行的建议。'},
                {'role': 'user', 'content': content}
            ],
            'temperature': 0.3,
            'max_tokens': 4000
        },
        timeout=120
    )
    
    if response.status_code == 200:
        result = response.json()
        analysis = result['choices'][0]['message']['content']
        
        # 保存分析结果
        output_file = 'docs/market_regime_analysis_deepseek.md'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# 市场环境判断方案 - DeepSeek分析\n\n")
            f.write(f"**分析时间**: 2026-04-15\n\n")
            f.write("---\n\n")
            f.write(analysis)
        
        print(analysis)
        print("\n" + "=" * 70)
        print(f"分析结果已保存: {output_file}")
    else:
        print(f"Error: API返回状态码 {response.status_code}")
        print(response.text)
        sys.exit(1)

if __name__ == '__main__':
    main()
