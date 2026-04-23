#!/bin/bash
# 双版本定时任务配置
# 
# 使用说明:
# 1. 盘前：双版本并行（版本A 5因子 + 版本B V11）
# 2. 盘中：版本B V11
# 3. 盘后：版本B V11

# 盘前分析 - 双版本并行（8:50）
50 8 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && python3 run_dual_versions.py premarket >> /tmp/dual_premarket.log 2>&1

# 盘中简报 - 版本B V11（12:30）
30 12 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && STOCK_REPORTS_DIR=/workspace/projects/workspace/股票分析项目/daily_reports_version_b python3 versions/version_b/run_version_b.py --mode noon >> /tmp/v11_noon.log 2>&1

# 盘后分析 - 版本B V11（15:50）
50 15 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && STOCK_REPORTS_DIR=/workspace/projects/workspace/股票分析项目/daily_reports_version_b python3 versions/version_b/run_version_b.py --mode postmarket >> /tmp/v11_postmarket.log 2>&1
