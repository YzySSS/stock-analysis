#!/bin/bash
# 股票分析项目 - 定时任务脚本
# 添加到crontab: crontab -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# 加载环境变量
source config/env.sh

# 记录日志
echo "$(date): 开始执行股票分析..." >> logs/cron.log

# 执行分析
python3 main.py --stocks "$DEFAULT_STOCKS" >> logs/cron.log 2>&1

# 记录完成
echo "$(date): 分析完成" >> logs/cron.log
