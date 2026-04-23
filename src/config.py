#!/usr/bin/env python3
"""
配置文件
========
数据库配置和其他全局配置
"""

import os

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}

# 飞书配置
FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK', '')

# 其他配置
DEFAULT_PICK_COUNT = 3
DEFAULT_STRATEGY = 'V11_DYNAMIC'
