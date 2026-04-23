#!/usr/bin/env python3
"""
手动推送版本A报告到飞书
"""

import sys
sys.path.insert(0, 'src')

import os
from notification import NotificationManager

# 读取版本A的最新报告
import glob
report_dir = "/workspace/projects/workspace/股票分析项目/daily_reports_version_a/premarket/"
reports = glob.glob(report_dir + "*.md")
report_path = max(reports, key=os.path.getmtime)  # 取最新报告

with open(report_path, 'r', encoding='utf-8') as f:
    report = f.read()

# 去掉报告中原有的标题（避免重复）
# 原有标题格式: # 🌅 盘前选股报告 1.0
lines = report.split('\n')
if lines and '# 🌅' in lines[0]:
    # 找到第一个空行后的内容
    for i, line in enumerate(lines):
        if line.strip() == '':
            report = '\n'.join(lines[i+1:])
            break

# 添加统一格式的标题
report_with_title = f"""═══════════════════════════════════════════════
🌅 盘前选股报告 1.0 - 版本A(5因子)
═══════════════════════════════════════════════

{report}"""

# 推送飞书
config = {
    'feishu': {
        'webhook': 'https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067'
    }
}

notifier = NotificationManager(config)
result = notifier.send_in_parts("", report_with_title)  # 标题已在内容中

if result.get('success'):
    print(f"✅ 版本A报告推送成功！发送了 {result.get('parts_sent', 0)} 段")
else:
    print(f"❌ 推送失败: {result.get('error', '未知错误')}")
