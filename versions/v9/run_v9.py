#!/usr/bin/env python3
"""
V9版本选股入口 (基础版本)
核心功能：多因子评分，板块轮动，不含P1/P2优化和舆情因子
"""

import sys
import os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 将项目根目录加入路径
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# 修改报告输出目录
os.environ['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v9')

# 导入V9版本 (使用V10的main_v10.py作为V9基础，去除高级功能)
os.environ['V9_MODE'] = '1'  # 设置V9模式标志

# 执行V9版本逻辑
exec(open(os.path.join(os.path.dirname(__file__), 'main_v9_base.py')).read())
