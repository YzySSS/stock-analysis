#!/bin/bash
# 股票筛选器 V8 全A股版 - 一键启动

cd "$(dirname "$0")"

echo "=================================="
echo "🚀 股票筛选器 V8 - 全A股版"
echo "=================================="
echo "股票池: 4914只 (沪深主板+创业板)"
echo "数据源: 新浪财经 + 聚宽"
echo ""

python3 scripts/screen_sector_v8.py
