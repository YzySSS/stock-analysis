#!/bin/bash
# 舆情数据更新监控脚本
# =====================

echo "舆情数据更新监控"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 检查进程
PID=$(pgrep -f "force_update_sentiment.py" | head -1)
if [ -n "$PID" ]; then
    echo "✅ 更新进程运行中 (PID: $PID)"
else
    echo "⚠️ 更新进程未运行"
fi

# 检查日志
echo ""
echo "最近10行日志:"
tail -10 /tmp/sentiment_full_update.log 2>/dev/null || echo "日志文件不存在"

# 数据库统计
echo ""
echo "数据库统计:"
python3 << 'EOF'
import sqlite3
import os
db_path = '/workspace/projects/workspace/股票分析项目/src/data_cache/sentiment_cache.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    
    # 按日期统计
    cursor = conn.execute("SELECT date, COUNT(*) FROM sentiment_cache GROUP BY date ORDER BY date DESC")
    print("日期分布:")
    for row in cursor:
        print(f"  {row[0]}: {row[1]} 只")
    
    # 总数
    cursor = conn.execute("SELECT COUNT(DISTINCT code) FROM sentiment_cache")
    unique = cursor.fetchone()[0]
    print(f"\n覆盖股票数: {unique} / 5491 ({unique/5491*100:.1f}%)")
    
    conn.close()
else:
    print("数据库文件不存在")
EOF

echo ""
echo "========================================"
echo "监控命令: watch -n 30 bash check_update_progress.sh"
