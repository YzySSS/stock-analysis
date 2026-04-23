#!/usr/bin/env python3
"""
调用DeepSeek API分析V12策略
"""
import requests
import os

# 读取分析报告
with open('/root/.openclaw/workspace/股票分析项目/docs/V12_V6_DEEPSEEK_ANALYSIS_REQUEST.md', 'r') as f:
    analysis_content = f.read()

# DeepSeek API配置
api_key = "sk-51f77975d58f4f5691e98e110bebdc4c"
base_url = "https://api.deepseek.com/v1"

# 构建请求
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

payload = {
    "model": "deepseek-chat",
    "messages": [
        {
            "role": "system",
            "content": "你是一位专业的量化交易策略分析师，擅长策略回测分析、风险控制和因子优化。请给出具体、可执行的改进建议。"
        },
        {
            "role": "user", 
            "content": f"""请分析以下量化选股策略，并给出具体的改进建议：

{analysis_content}

请针对以下6个问题给出详细的分析和建议：
1. 风控层面：如何将最大回撤控制在20%以内？需要增加哪些机制？
2. 市场环境过滤：基于哪些指标可以在2024年初就判断出策略会失效？
3. 因子优化：trend和momentum共线性如何解决？sentiment因子IC 0.70是否应该加大权重？
4. 仓位管理：当前市场强度→仓位的映射是否合理？弱势市场是否应该直接空仓？
5. 样本外验证：2026年高收益是否可信？如何判断策略是否已经失效？
6. 实盘可行性：当前状态下，如果要实盘，初始规模和风险控制应该如何设置？

请给出具体的代码级别的修改建议。"""
        }
    ],
    "temperature": 0.7,
    "max_tokens": 4000
}

print("正在调用DeepSeek API...")
try:
    response = requests.post(
        f"{base_url}/chat/completions",
        headers=headers,
        json=payload,
        timeout=180
    )
    
    if response.status_code == 200:
        result = response.json()
        analysis = result['choices'][0]['message']['content']
        
        # 保存分析结果
        output_file = '/root/.openclaw/workspace/股票分析项目/docs/V12_V6_DEEPSEEK_ADVICE.md'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# DeepSeek对V12策略V6的分析建议\n\n")
            f.write(analysis)
        
        print(f"✅ 分析完成！结果已保存到: {output_file}")
        print("\n" + "="*70)
        print(analysis)
        print("="*70)
    else:
        print(f"❌ API调用失败: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
