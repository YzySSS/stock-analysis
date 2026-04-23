#!/bin/bash
# 股票分析定时任务启动脚本 - V12版本
# =========================

# 加载环境变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env_config.sh"

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
        echo "🌅 执行V12盘前分析..."
        LOG_FILE="${LOG_DIR}/premarket_${DATE}_${TIME}.log"
        python3 main.py --mode premarket >> "${LOG_FILE}" 2>&1
        ;;
    intraday|盘中)
        echo "☀️ 执行V12盘中简报..."
        LOG_FILE="${LOG_DIR}/intraday_${DATE}_${TIME}.log"
        # 使用V12策略进行盘中选股
        python3 -c "
import sys
sys.path.insert(0, 'src')
from strategies.v12_strategy import V12Strategy
from datetime import datetime

v12 = V12Strategy()
today = datetime.now().strftime('%Y-%m-%d')
picks = v12.select(date=today, top_n=3)

if picks:
    print('✅ V12盘中选股完成')
    for p in picks:
        print(f\"  - {p['code']} {p.get('name', '')}: 评分{p.get('total_score', 0):.1f}\")
else:
    print('❌ V12选股无结果')
" >> "${LOG_FILE}" 2>&1
        ;;
    postmarket|盘后)
        echo "🌙 执行V12盘后复盘..."
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
        exit 1
        ;;
esac

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ 执行成功"
else
    echo "❌ 执行失败 (退出码: $EXIT_CODE)"
fi

exit $EXIT_CODE
