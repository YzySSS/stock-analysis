#!/bin/bash
# 2018-2023数据回填 - 分批处理脚本
# 每批处理200只股票，避免超时

cd /root/.openclaw/workspace/股票分析项目

# 获取当前进度
python3 -c "
import pymysql
DB_CONFIG = {'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user', 'password': 'open@2026', 'database': 'stock'}
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute('SELECT COUNT(DISTINCT code) FROM stock_kline WHERE trade_date BETWEEN \"2018-01-01\" AND \"2018-12-31\"')
completed = cursor.fetchone()[0]
print(f'已完成: {completed} 只')
conn.close()
"

# 运行回填（每批50只，限速）
python3 backfill_2018_2023.py
