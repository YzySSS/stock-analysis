#!/bin/bash
# 盘前选股脚本

export FEISHU_WEBHOOK="${FEISHU_WEBHOOK:-}"
export DEEPSEEK_API_KEY="${DEEPSEEK_API_KEY:-}"
export TAVILY_API_KEY="${TAVILY_API_KEY:-}"
export STOCK_REPORTS_DIR="/workspace/projects/workspace/股票分析项目/daily_reports"
export STOCK_LIST_FILE="/workspace/projects/workspace/股票分析项目/data/all_a_stocks.txt"

cd /root/.openclaw/workspace/股票分析项目
python3 main.py --mode premarket 2>&1
