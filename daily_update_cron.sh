#!/bin/bash
# 股票数据每日更新 wrapper 脚本
# =============================
# 此脚本由 cron 调用，执行实际的 Python 更新脚本

# 加载环境变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env_config.sh"

cd "${PROJECT_DIR}"

# 日志文件
LOG_FILE="/tmp/stock_daily_update_$(date +%Y%m%d).log"

echo "==============================================" >> $LOG_FILE
echo "股票数据每日更新 - $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE
echo "==============================================" >> $LOG_FILE

# 执行更新
python3 daily_update.py >> $LOG_FILE 2>&1

EXIT_CODE=$?

# 发送结果通知
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ 更新成功完成" >> $LOG_FILE
    # 可选：发送成功通知到飞书
    if [ ! -z "$FEISHU_WEBHOOK" ]; then
        curl -s -X POST "$FEISHU_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"🌅 股票数据更新完成\\n📅 $(date '+%Y-%m-%d')\\n✅ 状态: 成功\"}}" > /dev/null 2>&1
    fi
else
    echo "❌ 更新失败 (退出码: $EXIT_CODE)" >> $LOG_FILE
    # 可选：发送失败通知到飞书
    if [ ! -z "$FEISHU_WEBHOOK" ]; then
        curl -s -X POST "$FEISHU_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"🌅 股票数据更新完成\\n📅 $(date '+%Y-%m-%d')\\n❌ 状态: 失败\\n请检查日志: $LOG_FILE\"}}" > /dev/null 2>&1
    fi
fi

echo "==============================================" >> $LOG_FILE
echo "" >> $LOG_FILE

exit $EXIT_CODE
