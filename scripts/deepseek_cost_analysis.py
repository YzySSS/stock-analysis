#!/usr/bin/env python3
"""
调用DeepSeek API分析V12策略真实资金曲线可行性
"""

import os
import json
import requests

# 读取分析请求文档
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.dirname(script_dir)
request_file = os.path.join(workspace_dir, 'docs', 'V12_REAL_COST_ANALYSIS_REQUEST.md')

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
            "content": "你是一位专业的量化交易顾问，专注于策略回测和交易成本分析。请基于提供的策略信息，给出专业、客观、可操作的分析建议。重点关注：1)成本模型的完整性和准确性 2)高频策略的成本困境 3)实际可行的改进方案。"
        },
        {
            "role": "user",
            "content": f"""请分析以下量化策略的真实资金曲线可行性。

{analysis_request}

请按以下格式输出详细分析：

# DeepSeek 分析：V12真实资金曲线可行性评估

## 总体结论
[用一句话总结策略是否可行]

## 1. 成本模型评估
### 1.1 成本项目完整性
- 佣金印花税：...
- 滑点估算：...
- 其他成本：...

### 1.2 费率合理性
[详细分析]

## 2. 高频策略成本困境分析
### 2.1 成本结构冲突
[详细分析]

### 2.2 盈亏平衡点计算
- 理论胜率要求：...
- 理论收益要求：...

## 3. 各方案可行性评估
| 方案 | 年化成本 | 可行性 | 风险 | 建议 |
|------|----------|--------|------|------|
| A:降频至3天 | ... | ... | ... | ... |
| B:提高门槛 | ... | ... | ... | ... |
| C:分笔拆单 | ... | ... | ... | ... |

## 4. 真实资金曲线预测
### 4.1 修正后收益预估
| 年份 | 理论收益 | 扣除成本后 | 变化 |
|------|----------|------------|------|
| 2024 | -43.5% | ... | ... |
| 2025 | -40.7% | ... | ... |
| 2026 | +673% | ... | ... |

### 4.2 关键指标变化
- 累计收益：... → ...
- 最大回撤：... → ...
- 夏普比率：... → ...

## 5. 最终建议
### P0 (必须执行)
- [ ]
- [ ]

### P1 (强烈建议)
- [ ]
- [ ]

### P2 (可选优化)
- [ ]

## 6. 风险提示
[重要提醒]
"""
        }
    ],
    "temperature": 0.7,
    "max_tokens": 8000
}

# 发送请求
print("🔄 正在调用DeepSeek API进行分析...")
print(f"   API Key: {api_key[:10]}...{api_key[-4:]}")
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
        output_file = os.path.join(workspace_dir, 'docs', 'V12_REAL_COST_DEEPSEEK_ANALYSIS.md')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(analysis)
        
        print("✅ 分析完成！")
        print(f"   输出文件: {output_file}")
        print()
        print("="*60)
        print("DEEPSEEK分析结果预览：")
        print("="*60)
        print(analysis[:2000])
        print("...")
        print(f"\n完整内容已保存到: {output_file}")
        
    else:
        print(f"❌ API请求失败: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ 错误: {e}")
