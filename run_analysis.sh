#!/bin/bash
# 股票分析定时任务启动脚本
# =========================
# 统一入口，加载环境变量后执行分析

# 加载环境变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env_config.sh"

# 加载飞书配置 (优先使用config/env.sh中的配置)
if [ -f "${SCRIPT_DIR}/config/env.sh" ]; then
    source "${SCRIPT_DIR}/config/env.sh"
fi

# 切换到项目目录
cd "${PROJECT_DIR}"

# 日志目录
LOG_DIR="/tmp/stock_analysis"
mkdir -p "${LOG_DIR}"

# 获取当前时间
DATE=$(date +%Y%m%d)
TIME=$(date +%H%M)

# 根据参数执行不同模式
case "$1" in
    premarket|盘前)
        echo "🌅 执行盘前分析..."
        LOG_FILE="${LOG_DIR}/premarket_${DATE}_${TIME}.log"
        python3 main.py --mode premarket >> "${LOG_FILE}" 2>&1
        ;;
    intraday|盘中)
        echo "☀️ 执行盘中简报..."
        LOG_FILE="${LOG_DIR}/intraday_${DATE}_${TIME}.log"
        python3 main.py --mode noon >> "${LOG_FILE}" 2>&1
        ;;
    postmarket|盘后)
        echo "🌙 执行盘后复盘..."
        LOG_FILE="${LOG_DIR}/postmarket_${DATE}_${TIME}.log"
        python3 main.py --mode postmarket >> "${LOG_FILE}" 2>&1
        ;;
    daily|收盘)
        echo "📊 执行收盘数据更新..."
        LOG_FILE="${LOG_DIR}/daily_update_${DATE}_${TIME}.log"
        python3 daily_update.py >> "${LOG_FILE}" 2>&1
        ;;
    *)
        echo "用法: $0 [premarket|intraday|postmarket|daily]"
        echo "  premarket  - 盘前分析 (8:50)"
        echo "  intraday   - 盘中简报 (12:30)"
        echo "  postmarket - 盘后复盘 (15:50)"
        echo "  daily      - 收盘更新 (15:35)"
        exit 1
        ;;
esac

EXIT_CODE=$?

# 发送通知
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ 执行成功"
else
    echo "❌ 执行失败 (退出码: $EXIT_CODE)"
    echo "   日志: ${LOG_FILE}"
fi

exit $EXIT_CODE
