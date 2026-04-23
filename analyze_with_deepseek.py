#!/usr/bin/env python3
"""
DeepSeek API 分析工具
用于分析盘前选股策略表现
"""

import os
import sys
import json
import urllib.request
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def load_premarket_picks(date_str: str) -> list:
    """加载指定日期的盘前选股记录"""
    db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'stock_history.db')
    
    # 从报告目录读取
    report_dirs = [
        os.path.join(os.path.dirname(__file__), 'daily_reports_version_b', 'premarket'),
        os.path.join(os.path.dirname(__file__), 'daily_reports', 'premarket'),
    ]
    
    picks = []
    for report_dir in report_dirs:
        if os.path.exists(report_dir):
            for filename in os.listdir(report_dir):
                if date_str.replace('-', '') in filename and 'premarket' in filename:
                    filepath = os.path.join(report_dir, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                            # 解析报告内容提取选股
                            # 这里简化处理，实际应该解析MD文件
                            picks.append({
                                'file': filename,
                                'content': content[:500]  # 前500字符
                            })
                    except:
                        pass
    
    return picks

def get_stock_close_price(code: str, date_str: str) -> float:
    """获取股票指定日期的收盘价"""
    try:
        import baostock as bs
        result = bs.login()
        if result.error_code != '0':
            return None
        
        bs_code = f"sz.{code}" if code.startswith(('00', '30')) else f"sh.{code}"
        rs = bs.query_history_k_data_plus(
            bs_code, 'date,close',
            start_date=date_str, end_date=date_str,
            frequency='d', adjustflag='2'
        )
        
        close_price = None
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            close_price = float(row[1]) if row[1] else None
        
        bs.logout()
        return close_price
    except:
        return None

def generate_historical_data() -> str:
    """生成历史选股表现数据文本"""
    
    # 历史数据（硬编码已知的）
    historical_text = """
### 📅 2026-03-23 (周一)
选股5只，评分均为71.0分
- 珠海中富(000659): 推荐时+4.13% → 收盘-2.38% | 评分:71.0 | ❌
- 模塑科技(000700): 推荐时+4.12% → 收盘-9.23% | 评分:71.0 | ❌  
- 湖南发展(000722): 推荐时+4.56% → 收盘-3.44% | 评分:71.0 | ❌
- 冀东装备(000856): 推荐时+3.66% → 收盘+0.98% | 评分:71.0 | ✅
- 蓝焰控股(000968): 推荐时+4.99% → 收盘+6.80% | 评分:71.0 | ✅
**统计**: 准确率40%，平均收益-1.46%

### 📅 2026-03-26 (周四)  
选股3只
- 五粮液(000858): 推荐时+1.10% → 收盘-1.14% | 评分:65.2 | ❌
- 贵州茅台(600519): 推荐时+0.21% → 收盘-0.64% | 评分:59.5 | ❌
- 中国平安(601318): 推荐时+1.15% → 收盘-3.38% | 评分:57.6 | ❌
**统计**: 准确率0%，平均收益-1.72% (全错!)

### 📅 2026-03-27 (周五)
选股3只
- 宁德时代(300750): 推荐时+1.18% → 收盘+3.40% | 评分:63.3 | ✅
- 工商银行(601398): 推荐时+1.09% → 收盘-0.27% | 评分:60.5 | ❌
- 平安银行(000001): 推荐时+0.00% → 收盘+0.73% | 评分:59.5 | ✅
**统计**: 准确率66.7%，平均收益+1.29%

### 📅 整体统计 (3个交易日)
- 总选股数: 11只
- 正确次数: 4次
- 准确率: 36.4%
- 平均收益: -0.78%
- 最大盈利: +6.80% (蓝焰控股)
- 最大亏损: -9.23% (模塑科技)
"""
    
    return historical_text

def call_deepseek_api(prompt: str) -> str:
    """调用DeepSeek API"""
    
    # 获取API Key
    api_key = os.environ.get('DEEPSEEK_API_KEY', '')
    if not api_key:
        # 尝试从.env文件读取
        try:
            with open(os.path.join(os.path.dirname(__file__), '.env'), 'r') as f:
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
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0.7,
                'max_tokens': 4000
            }).encode('utf-8')
        )
        
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            return result['choices'][0]['message']['content']
    
    except Exception as e:
        return f"API调用失败: {str(e)}"

def main():
    """主函数"""
    print("="*70)
    print("🔍 DeepSeek 盘前选股策略分析")
    print("="*70)
    
    # 导入提示词模板
    from deepseek_prompt import generate_prompt
    
    # 生成历史数据
    historical_data = generate_historical_data()
    
    # 生成完整提示词
    prompt = generate_prompt(historical_data)
    
    print("\n📋 提示词已生成，长度:", len(prompt), "字符")
    print("\n🚀 调用DeepSeek API...")
    print("-"*70)
    
    # 调用API
    response = call_deepseek_api(prompt)
    
    print(response)
    
    # 保存结果
    output_dir = os.path.join(os.path.dirname(__file__), 'daily_reports', 'analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"deepseek_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# DeepSeek 盘前选股策略分析报告\n\n")
        f.write(f"**分析时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")
        f.write(response)
    
    print("\n" + "="*70)
    print(f"✅ 分析结果已保存: {output_file}")
    print("="*70)

if __name__ == "__main__":
    main()
