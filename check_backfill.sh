#!/bin/bash
# 监控数据回填进度

echo "数据回填进度监控"
echo "===================="
echo ""

# 检查进程
PID=$(pgrep -f "backfill_2018_2023.py")
if [ -n "$PID" ]; then
    echo "回填进程运行中 (PID: $PID)"
    echo ""
    
    # 显示最近日志
    LOG_FILE=$(ls -t logs/backfill_2018_2023_*.log | head -1)
    echo "最近日志: $LOG_FILE"
    echo ""
    echo "最后20行日志:"
    tail -20 "$LOG_FILE"
    echo ""
    
    # 统计日志中的成功记录
    SUCCESS=$(grep -c "成功" "$LOG_FILE" 2>/dev/null || echo "0")
    echo "本批次已处理: $SUCCESS 只股票"
else
    echo "回填进程未运行"
    echo ""
    echo "查看历史日志:"
    ls -lt logs/backfill_2018_2023_*.log 2>/dev/null | head -5
fi
