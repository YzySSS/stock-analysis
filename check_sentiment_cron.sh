#!/bin/bash
# 舆情数据更新定时任务配置
# =============================
# 
# 已配置 OpenClaw Cron 任务:
#   任务名: sentiment_data_update
#   频率: 每30分钟
#   命令: python3 update_sentiment_data.py
#
# 或者手动添加到 crontab:
#   */30 * * * * cd /workspace/projects/workspace/股票分析项目 && python3 update_sentiment_data.py >> /tmp/sentiment_update.log 2>&1

echo "舆情数据定时任务状态:"
cron list | grep sentiment_data_update || echo "任务未找到"

echo ""
echo "数据库状态:"
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('src/data_cache/sentiment_cache.db')
cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache")
total = cursor.fetchone()[0]
cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache WHERE date=date('now')")
today = cursor.fetchone()[0]
print(f"  总记录数: {total}")
print(f"  今日记录: {today}")
conn.close()
EOF

echo ""
echo "下次更新时间:"
date -d "$(date +%H):30:00" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "每小时的:00和:30"
