#!/bin/bash
# 历史数据填充进度监控

echo "=== 历史数据填充进度监控 ==="
echo ""

# 检查进程
PID=$(ps aux | grep fill_history_mysql | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
    echo "✅ 进程运行中: PID $PID"
    echo "   运行时间: $(ps -o etime= -p $PID)"
    echo ""
else
    echo "⚠️ 进程未运行"
fi

# 查看最新日志
LOGFILE=$(ls -t logs/fill_history_*.log 2>/dev/null | head -1)
if [ -n "$LOGFILE" ]; then
    echo "📄 日志文件: $LOGFILE"
    echo ""
    echo "📊 最新进度:"
    tail -10 "$LOGFILE" | grep -E "(进度|批次|完成)" | tail -5
    echo ""
    echo "📈 统计:"
    grep -E "成功.*失败" "$LOGFILE" | tail -1
fi

# 数据库统计
echo ""
echo "🗄️  数据库统计:"
mysql -h$DB_HOST -P$DB_PORT -u$DB_USER -p$DB_PASSWORD $DB_NAME -e "
    SELECT 
        COUNT(DISTINCT code) as stock_count,
        COUNT(*) as total_records,
        MIN(trade_date) as min_date,
        MAX(trade_date) as max_date
    FROM stock_kline;
" 2>/dev/null || echo "   需要配置数据库连接"
