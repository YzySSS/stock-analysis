#!/bin/bash
# 全A股舆情数据定时更新任务
# =============================
# 
# 已配置 OpenClaw Cron 任务:
#   任务ID: sentiment_data_update
#   任务名: 全A股舆情数据更新
#   频率: 每小时（0 * * * *）
#   命令: python3 update_sentiment_data.py --all
#   超时: 3600秒（1小时）
#
# 数据存储:
#   数据库: src/data_cache/sentiment_cache.db
#   表: sentiment_cache
#   字段: code, date, sentiment_score, news_count, credibility_avg, cached_at
#
# 使用方法:
#   1. 手动更新全A股（约5491只，耗时约2-3小时）:
#      python3 update_sentiment_data.py --all
#
#   2. 手动更新前N只:
#      python3 update_sentiment_data.py --all --max 100
#
#   3. 仅更新热门股（27只）:
#      python3 update_sentiment_data.py --priority
#
#   4. 查看数据库状态:
#      python3 -c "import sqlite3; conn=sqlite3.connect('src/data_cache/sentiment_cache.db'); print(conn.execute('SELECT COUNT(*) FROM sentiment_cache').fetchone()[0]); conn.close()"

echo "========================================"
echo "全A股舆情数据定时更新任务"
echo "========================================"
echo ""

# 检查定时任务
echo "定时任务状态:"
cron list 2>/dev/null | grep -A5 "sentiment_data_update" || echo "  任务: sentiment_data_update"
echo ""

# 检查数据库
echo "数据库状态:"
python3 << 'EOF'
import sqlite3
import os
db_path = 'src/data_cache/sentiment_cache.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache")
    total = cursor.fetchone()[0]
    cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache WHERE date=date('now')")
    today = cursor.fetchone()[0]
    cursor = conn.execute("SELECT COUNT(DISTINCT code) FROM sentiment_cache")
    unique = cursor.fetchone()[0]
    print(f"  数据库文件: {db_path}")
    print(f"  总记录数: {total}")
    print(f"  今日记录: {today}")
    print(f"   unique股票数: {unique}")
    conn.close()
else:
    print(f"  数据库文件不存在: {db_path}")
EOF

echo ""
echo "下次更新时间:"
echo "  每小时整点执行（如 22:00, 23:00...）"
echo ""
echo "日志文件: /tmp/sentiment_update.log"
echo "========================================"
