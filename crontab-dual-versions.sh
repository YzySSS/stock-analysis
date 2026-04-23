#!/bin/bash
# 双版本定时任务配置脚本
# 添加到crontab中

# 盘前分析 - 版本A + 版本B (8:50)
50 8 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && python3 run_dual_versions.py premarket >> /tmp/dual_premarket.log 2>&1

# 盘中简报 - 版本A + 版本B (12:30)
30 12 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && python3 run_dual_versions.py noon >> /tmp/dual_noon.log 2>&1

# 盘后分析 - 版本A + 版本B (15:50)
50 15 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && python3 run_dual_versions.py postmarket >> /tmp/dual_postmarket.log 2>&1
