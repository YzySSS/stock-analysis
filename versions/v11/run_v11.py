#!/usr/bin/env python3
"""
V11版本选股入口 (当前最新版本)
包含所有优化：技术位分析、动态阈值、ETF数据等
"""

import sys
import os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 将项目根目录加入路径
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# 修改报告输出目录
os.environ['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v11')

# 导入V11版本main.py (项目根目录的main.py)
exec(open(os.path.join(project_root, 'main.py')).read())
