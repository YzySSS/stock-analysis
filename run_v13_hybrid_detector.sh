#!/bin/bash
# V13 Hybrid 市场环境检测器运行脚本

cd /root/.openclaw/workspace/股票分析项目

echo "========================================"
echo "V13 Hybrid 市场环境检测器"
echo "========================================"
echo ""

# 检查并创建日志目录
mkdir -p logs
mkdir -p backtest_results

# 显示菜单
echo "请选择操作:"
echo "1. 测试单个日期检测"
echo "2. 批量检测并保存结果"
echo "3. 优化权重和阈值参数"
echo "4. 分析历史市场状态分布"
echo ""
read -p "请输入选项 (1-4): " choice

case $choice in
    1)
        echo ""
        echo "运行单日期测试..."
        python3 v13_hybrid_market_detector.py
        ;;
    2)
        echo ""
        read -p "请输入开始日期 (YYYY-MM-DD): " start_date
        read -p "请输入结束日期 (YYYY-MM-DD): " end_date
        echo ""
        echo "运行批量检测: $start_date 至 $end_date"
        python3 -c "
from v13_hybrid_market_detector import MarketEnvironmentDetector
detector = MarketEnvironmentDetector()
df = detector.batch_detect('$start_date', '$end_date')
output_file = '/root/.openclaw/workspace/股票分析项目/backtest_results/v13_hybrid_regime_${start_date}_${end_date}.csv'
detector.save_results(df, output_file)
print(f'\n结果已保存: {output_file}')
"
        ;;
    3)
        echo ""
        echo "运行参数优化 (这可能需要10-30分钟)..."
        echo "优化目标: 最大化卡尔玛比率"
        echo ""
        python3 v13_hybrid_optimizer.py
        ;;
    4)
        echo ""
        read -p "请输入开始年份 (如: 2024): " start_year
        read -p "请输入结束年份 (如: 2025): " end_year
        echo ""
        echo "分析 ${start_year}-${end_year} 年市场状态分布..."
        python3 -c "
from v13_hybrid_optimizer import V13HybridOptimizer
optimizer = V13HybridOptimizer()
optimizer.analyze_historical_regimes('${start_year}-01-01', '${end_year}-12-31')
"
        ;;
    *)
        echo "无效选项"
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "完成!"
echo "========================================"
