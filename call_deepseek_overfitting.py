#!/usr/bin/env python3
"""
调用DeepSeek API - 过拟合分析
"""
import requests

# 读取分析报告
with open('/root/.openclaw/workspace/股票分析项目/docs/V12_OVERFITTING_ANALYSIS.md', 'r') as f:
    analysis_content = f.read()

# DeepSeek API配置
api_key = "sk-51f77975d58f4f5691e98e110bebdc4c"
base_url = "https://api.deepseek.com/v1"

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

payload = {
    "model": "deepseek-chat",
    "messages": [
        {
            "role": "system",
            "content": "你是一位专业的量化交易研究员，专注于过拟合检测、因子挖掘和策略稳健性分析。请给出具体、可操作的建议。"
        },
        {
            "role": "user", 
            "content": f"""请分析以下量化策略是否过拟合，并给出因子层面的修改建议：

{analysis_content}

请重点回答：
1. 基于2024-2026年的极端收益反差，策略是否过拟合？如何判断？
2. 当前的7个因子（trend/momentum/quality/sentiment/valuation/liquidity/size）应该如何调整？
   - 哪些应该删除/合并？
   - 应该增加哪些新因子？
   - sentiment因子IC高达0.70，是否应该提升权重？
3. 参数稳健性：冷却期3天、阈值55分、行业上限30%等参数如何验证不是过拟合？
4. 除了回测2018-2023年，还有什么方法可以验证策略稳健性？
5. 如果确认过拟合，具体如何修正？（正则化、简化、增加约束等）

请给出具体的代码级别的修改建议。"""
        }
    ],
    "temperature": 0.7,
    "max_tokens": 4000
}

print("正在调用DeepSeek API分析过拟合问题...")
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
        output_file = '/root/.openclaw/workspace/股票分析项目/docs/V12_OVERFITTING_ADVICE.md'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# DeepSeek对V12策略过拟合的分析与建议\n\n")
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
