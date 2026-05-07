#!/bin/bash
# 股票分析收盘数据更新
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"
export DEEPSEEK_API_KEY="${DEEPSEEK_API_KEY:-}"
export TAVILY_API_KEY="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"
export STOCK_REPORTS_DIR="/workspace/projects/workspace/股票分析项目/daily_reports"
export STOCK_LIST_FILE="/workspace/projects/workspace/股票分析项目/data/all_a_stocks.txt"

cd /root/.openclaw/workspace/股票分析项目
python3 daily_update_mysql.py 2>&1
