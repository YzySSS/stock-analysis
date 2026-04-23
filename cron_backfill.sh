#!/bin/bash
# 定时数据回填脚本
# 每小时检查并启动回填

cd /root/.openclaw/workspace/股票分析项目

# 检查是否已在运行
PID=$(pgrep -f "backfill_2018_2023.py")
if [ -n "$PID" ]; then
    echo "$(date): 回填已在运行 (PID: $PID)，跳过"
    exit 0
fi

# 检查2024/2025数据是否已完成
RECORDS_2024=$(mysql -h10.0.4.8 -uopenclaw_user -p'open@2026' stock -N -e "
    SELECT COUNT(*) FROM stock_kline 
    WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31'
" 2>/dev/null)

if [ "$RECORDS_2024" -gt "800000" ]; then
    echo "$(date): 2024年数据已完成 ($RECORDS_2024条)，停止回填"
    exit 0
fi

# 启动回填
echo "$(date): 启动数据回填..."
nohup python3 backfill_2018_2023.py > logs/backfill_cron_$(date +%Y%m%d_%H%M).log 2>&1 &
echo "$(date): 回填已启动 (PID: $!)"
