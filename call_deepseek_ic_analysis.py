#!/usr/bin/env python3
"""
调用DeepSeek分析IC分析结果和回测结果
"""

import os
import sys
import json
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def call_deepseek_api(prompt: str) -> str:
    """调用DeepSeek API"""
    
    # 获取API Key
    api_key = os.environ.get('DEEPSEEK_API_KEY', '')
    if not api_key:
        # 尝试从config/env.sh读取
        try:
            with open(os.path.join(os.path.dirname(__file__), 'config/env.sh'), 'r') as f:
                for line in f:
                    if 'DEEPSEEK_API_KEY' in line and '=' in line:
                        api_key = line.split('=')[1].strip().strip('"').strip("'")
                        break
        except:
            pass
    
    if not api_key:
        return "错误: 未找到DeepSeek API Key"
    
    try:
        req = urllib.request.Request(
            'https://api.deepseek.com/v1/chat/completions',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {api_key}'
            },
            data=json.dumps({
                'model': 'deepseek-chat',
                'messages': [
                    {'role': 'system', 'content': '你是量化投资策略分析专家，擅长多因子选股策略设计和回测分析。请用中文回答，提供具体、可操作的建议。'},
                    {'role': 'user', 'content': prompt}
                ],
                'temperature': 0.5,
                'max_tokens': 8000
            }).encode('utf-8')
        )
        
        with urllib.request.urlopen(req, timeout=180) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            return result['choices'][0]['message']['content']
    
    except Exception as e:
        return f"API调用失败: {str(e)}"

def main():
    """主函数"""
    print("="*70)
    print("🔍 DeepSeek V12策略深度分析")
    print("="*70)
    
    # 读取分析报告
    with open(os.path.join(os.path.dirname(__file__), 'docs/V12_DEEPSEEK_ANALYSIS_REQUEST_2026-04-13.md'), 'r', encoding='utf-8') as f:
        report = f.read()
    
    # 构建提示词
    prompt = f"""请对以下量化选股策略进行全面深度分析。

## 你的任务
1. 诊断V10策略失败的**根本原因**
2. 基于IC分析结果，设计**最优的3日持仓策略**
3. 提供**具体的因子权重和选股规则**
4. 预测新策略的**预期胜率/收益**
5. 给出**下一步具体实施建议**

## 分析资料

{report}

## 输出要求

### 1. 失败原因诊断 (20%)
- V10策略为什么胜率只有15.58%？
- 是因子选择错误、权重错误、还是持仓周期错误？
- 哪个因素是主要问题？

### 2. 最优策略设计 (40%)
基于IC分析，推荐：
- 持仓周期: ___日
- 因子组合: ___ (列出因子及IC值)
- 权重分配: ___% + ___% + ___%
- 选股阈值: ___分
- 风控设置: 止损___%，仓位___%

### 3. 预期效果预测 (20%)
- 预期胜率: ___%
- 预期年化收益: ___%
- 预期最大回撤: ___%
- 理由说明

### 4. 实施路线图 (20%)
- P0 (本周必须完成): ___
- P1 (下周完成): ___
- P2 (后续优化): ___

请用中文回答，数据驱动，给出具体可操作的建议。"""
    
    print("\n📋 提示词长度:", len(prompt), "字符")
    print("\n🚀 调用DeepSeek API...")
    print("-"*70)
    
    # 调用API
    response = call_deepseek_api(prompt)
    
    print(response)
    
    # 保存结果
    output_dir = os.path.join(os.path.dirname(__file__), 'docs')
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"V12_IC_ANALYSIS_DEEPSEEK_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# V12策略IC分析与优化建议 - DeepSeek\n\n")
        f.write(f"**分析时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")
        f.write(response)
    
    print("\n" + "="*70)
    print(f"✅ 分析结果已保存: {output_file}")
    print("="*70)
    
    return output_file

if __name__ == "__main__":
    main()
