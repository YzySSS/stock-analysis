#!/bin/bash
# 股票历史数据每日更新定时任务
# =============================
# 
# 添加到 crontab 的方法:
#   crontab -e
# 然后添加下面这行（每天 19:00 运行）:
#   0 19 * * * cd /workspace/projects/workspace/股票分析项目 && python3 daily_update.py >> /tmp/stock_daily_update.log 2>&1
#
# 或者使用此脚本:
#   bash setup_cron.sh

echo "设置股票数据每日更新定时任务..."

# 检查是否已有定时任务
if crontab -l 2>/dev/null | grep -q "daily_update.py"; then
    echo "✅ 定时任务已存在"
    crontab -l | grep "daily_update.py"
else
    # 添加定时任务（每天 19:00 运行）
    (crontab -l 2>/dev/null; echo "0 19 * * * cd /workspace/projects/workspace/股票分析项目 && python3 daily_update.py >> /tmp/stock_daily_update.log 2>&1") | crontab -
    echo "✅ 定时任务已添加"
    echo "   执行时间: 每天 19:00"
    echo "   日志文件: /tmp/stock_daily_update.log"
fi

echo ""
echo "当前 crontab 内容:"
crontab -l | grep -E "(daily_update|stock)" || echo "   (无相关任务)"
