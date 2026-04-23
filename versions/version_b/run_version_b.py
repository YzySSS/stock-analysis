#!/usr/bin/env python3
"""
版本B入口 (V11最新版本)
包含: 技术位分析、动态阈值、舆情因子、ETF数据等
"""

import sys
import os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 将项目根目录加入路径
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# 修改报告输出目录
os.environ['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_version_b')
os.environ['VERSION_NAME'] = '版本B(V11)'

# 执行版本B (使用项目根目录的最新main.py)
exec(open(os.path.join(project_root, 'main.py')).read())
