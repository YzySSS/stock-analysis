#!/usr/bin/env python3
"""
调用DeepSeek API分析：是否需要加入真实资金曲线
"""

import os
import json
import requests

# API配置
api_key = os.getenv('DEEPSEEK_API_KEY', 'sk-51f77975d58f4f5691e98e110bebdc4c')
base_url = 'https://api.deepseek.com/v1'

# 请求体
payload = {
    "model": "deepseek-reasoner",
    "messages": [
        {
            "role": "system",
            "content": "你是一位专业的量化交易顾问，专注于策略回测和资金曲线分析。请从专业角度回答用户关于真实资金曲线的问题。"
        },
        {
            "role": "user",
            "content": """请从专业量化交易角度回答：V12策略是否需要加入真实资金曲线计算？

## 背景信息

V12策略现状：
- 回测周期：2024-01-02 至 2026-04-07（约2年3个月）
- 总交易次数：233笔
- 交易频率：日频满仓换股
- 理论收益（未扣除成本）：2年累计+159%
- 胜率：45.5%
- 最大回撤：84.6%

成本分析（已计算）：
- 单次交易成本：~0.5%（佣金+印花税+滑点）
- 年化交易成本：~125.5%（250次交易）
- 扣除成本后收益：约-92%（巨亏）

当前回测状态：
- 使用的简化成本模型（未真实扣除）
- 显示2024-2025年熊市大幅亏损
- 显示2026年牛市暴利（可能过拟合）

## 问题

请回答以下问题：

1. **是否必须加入真实资金曲线？** 从专业角度分析必要性

2. **当前阶段合适吗？** 策略还在研发/回测阶段，现在加入是否过早？

3. **实施路径建议**：
   - 立即加入完整成本模型？
   - 先降频优化，再加入成本？
   - 分阶段实施？

4. **如果不加入的后果**：
   - 对策略评估的影响？
   - 对实盘决策的风险？

5. **替代方案**：
   - 是否可以用理论成本率估算？
   - 是否可以先跑模拟盘？

## 期望输出格式

请按以下格式输出：

# DeepSeek建议：是否加入真实资金曲线

## 一句话结论
[直接回答：必须加入/建议加入/可以暂缓/不建议加入]

## 1. 必要性分析（为什么必须做/可以不做）
### 1.1 专业角度
...

### 1.2 当前策略状态
...

### 1.3 风险考量
...

## 2. 时机评估（现在做是否合适）
### 2.1 当前阶段特点
...

### 2.2 加入成本的影响
...

### 2.3 最佳时机建议
...

## 3. 实施路径对比
| 方案 | 描述 | 优点 | 缺点 | 推荐指数 |
|------|------|------|------|----------|
| 方案A：立即完整实施 | ... | ... | ... | ⭐⭐⭐ |
| 方案B：先降频再实施 | ... | ... | ... | ⭐⭐⭐⭐⭐ |
| 方案C：分阶段实施 | ... | ... | ... | ⭐⭐⭐⭐ |
| 方案D：暂不实施 | ... | ... | ... | ⭐⭐ |

## 4. 如果不加入的后果
### 4.1 短期影响
...

### 4.2 长期风险
...

### 4.3 决策误导风险
...

## 5. 最终建议
### 推荐方案
[详细描述推荐的实施路径]

### 优先级
- P0：...
- P1：...
- P2：...

### 风险提示
[重要提醒]

## 6. 补充建议
[其他相关建议]"""
        }
    ],
    "temperature": 0.7,
    "max_tokens": 8000
}

# 发送请求
print("🔄 正在询问DeepSeek专业意见...")
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
        script_dir = os.path.dirname(os.path.abspath(__file__))
        workspace_dir = os.path.dirname(script_dir)
        output_file = os.path.join(workspace_dir, 'docs', 'V12_COST_DECISION_DEEPSEEK.md')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(analysis)
        
        print("✅ 分析完成！")
        print(f"   输出文件: {output_file}")
        print()
        print("="*60)
        print("DEEPSEEK分析结果：")
        print("="*60)
        print(analysis)
        
    else:
        print(f"❌ API请求失败: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ 错误: {e}")
