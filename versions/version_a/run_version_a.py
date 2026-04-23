#!/usr/bin/env python3
"""
版本A入口 (早期5因子模型 - 选出珠海中富的版本)
因子权重: 技术25% | 情绪20% | 板块30% | 资金15% | 风险10%
"""

import sys
import os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 将项目根目录加入路径
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# 修改报告输出目录
os.environ['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_version_a')
os.environ['VERSION_NAME'] = '版本A(5因子)'

# 执行版本A
exec(open(os.path.join(os.path.dirname(__file__), 'main_version_a.py')).read())
