#!/bin/bash
# 股票筛选器启动脚本 - V8全A股版

cd /workspace/projects/workspace/股票分析项目

echo "=================================="
echo "🚀 启动股票筛选器 V8 - 全A股版"
echo "=================================="
echo ""

python3 scripts/screen_sector_v8.py "$@"
