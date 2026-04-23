#!/usr/bin/env python3
"""
V10版本选股入口 (2026-03-23版本)
保留原有多因子评分逻辑，不包含最新优化
"""

import sys
import os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 将项目根目录加入路径
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# 修改报告输出目录
os.environ['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v10')

# 导入V10版本main.py
exec(open(os.path.join(os.path.dirname(__file__), 'main_v10.py')).read())
