#!/bin/bash
# 推送到 GitHub 脚本

set -e

echo "🚀 推送股票分析项目到 GitHub..."
echo "================================"

# 检查 git 状态
echo "📋 检查 Git 状态..."
git status

echo ""
echo "📦 添加所有更改..."
git add -A

echo ""
echo "📝 提交更改..."
git commit -m "docs: 更新README至V13版本，添加策略演进和团队协作说明

- 更新README为V13策略（当前最优）
- 添加3因子配置（Turnover/LowVol/Reversal）
- 添加回测表现（2年+28.5%）
- 添加V13_Hybrid混合策略说明
- 添加团队协作分工（大X/小X/小Y）
- 添加策略演进历史（V6→V13）
- 添加重要决策记录
- 优化项目结构说明" || echo "没有需要提交的更改"

echo ""
echo "☁️ 推送到 GitHub..."
git push origin main

echo ""
echo "✅ 推送完成！"
echo "================================"
echo "GitHub仓库: https://github.com/YzySSS/stock-analysis"
