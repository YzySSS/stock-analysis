#!/bin/bash
# 股票分析项目环境变量配置
# =========================
# 在定时任务或其他脚本中引用此文件

# 项目目录
export PROJECT_DIR="/root/.openclaw/workspace/股票分析项目"

# 代理配置（云服务器必须使用代理访问东方财富）
export USE_PROXY="true"
export BRD_PROXY_HOST="brd.superproxy.io"
export BRD_PROXY_PORT="33335"
export BRD_PROXY_USER="brd-customer-hl_8abbb7fa-zone-isp_proxy1"
export BRD_PROXY_PASS="1chayfaf4h24"

# 飞书推送
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"

# 报告目录
export STOCK_REPORTS_DIR="/root/.openclaw/workspace/股票分析项目/daily_reports"

# Python 路径
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"

echo "✅ 环境变量已加载"
echo "   USE_PROXY: ${USE_PROXY}"
echo "   PROJECT_DIR: ${PROJECT_DIR}"
