#!/bin/bash
# 分批次补充历史舆情数据
# ======================
# 同时处理 2026-03-24 和 2026-03-23

set -e

cd /workspace/projects/workspace/股票分析项目

TARGET_DATES=("2026-03-24" "2026-03-23")
BATCH_SIZE=500

echo "========================================"
echo "历史舆情数据分批次补充"
echo "开始时间: $(date '+%H:%M:%S')"
echo "========================================"

for TARGET_DATE in "${TARGET_DATES[@]}"; do
    echo ""
    echo "========================================"
    echo "处理日期: $TARGET_DATE"
    echo "========================================"
    
    # 检查当前数量
    CURRENT=$(python3 -c "
import sqlite3
conn = sqlite3.connect('src/data_cache/sentiment_cache.db')
cursor = conn.execute('SELECT COUNT(*) FROM sentiment_cache WHERE date=\"$TARGET_DATE\"')
print(cursor.fetchone()[0])
conn.close()
" 2>/dev/null || echo "0")
    
    echo "当前已有: $CURRENT 只"
    
    if [ "$CURRENT" -ge 5491 ]; then
        echo "✅ $TARGET_DATE 数据已完整，跳过"
        continue
    fi
    
    # 分批次处理（每批500只，共11批）
    for START in 0 500 1000 1500 2000 2500 3000 3500 4000 4500 5000; do
        echo ""
        echo "批次: $START - $((START+BATCH_SIZE))"
        
        python3 << EOF
import sys
sys.path.insert(0, 'src')
from sentiment_factor import get_sentiment_calculator
import sqlite3
from datetime import datetime

calc = get_sentiment_calculator()
target_date = "$TARGET_DATE"
start = $START
batch_size = $BATCH_SIZE

# 加载股票列表
with open('data/all_a_stocks.txt', 'r') as f:
    codes = [line.strip() for line in f if line.strip()]

end = min(start + batch_size, len(codes))
stock_list = [(codes[i], f"股票{codes[i]}") for i in range(start, end)]

print(f"处理 {len(stock_list)} 只股票...")

updated = 0
for code, name in stock_list:
    try:
        # 检查是否已存在
        existing = calc.get_cached_sentiment(code, target_date)
        if existing:
            continue
        
        # 计算并保存
        result = calc.calculate_sentiment_factor(code, name, target_date)
        updated += 1
    except Exception as e:
        pass

print(f"✅ 本批次完成: 新增 {updated} 只")
EOF
        
        # 批次间休息3秒
        sleep 3
    done
    
    echo ""
    echo "✅ $TARGET_DATE 处理完成"
done

echo ""
echo "========================================"
echo "全部完成！"
echo "结束时间: $(date '+%H:%M:%S')"
echo "========================================"

# 最终统计
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('src/data_cache/sentiment_cache.db')

print("\n最终统计:")
cursor = conn.execute('SELECT date, COUNT(*) FROM sentiment_cache GROUP BY date ORDER BY date DESC')
for row in cursor:
    print(f"  {row[0]}: {row[1]}只")

conn.close()
EOF
