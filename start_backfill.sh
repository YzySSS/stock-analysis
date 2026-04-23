#!/bin/bash
# 启动2018-2023历史数据回填

cd /root/.openclaw/workspace/股票分析项目

# 日志目录
mkdir -p logs

# 后台启动回填脚本
nohup python3 backfill_2018_2023.py > logs/backfill_2018_2023_$(date +%Y%m%d_%H%M).log 2>&1 &

echo "✅ 数据回填已启动 (PID: $!)"
echo "日志: logs/backfill_2018_2023_$(date +%Y%m%d_%H%M).log"
echo ""
echo "使用以下命令监控进度:"
echo "  tail -f logs/backfill_2018_2023_*.log"
