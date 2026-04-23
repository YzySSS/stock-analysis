#!/bin/bash
# 全A股舆情数据分批次更新脚本
# =============================
# 将5491只股票分成11个批次，每批500只

cd /workspace/projects/workspace/股票分析项目

echo "========================================"
echo "全A股舆情数据分批次更新"
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""

# 批次配置
BATCH_SIZE=500
BATCHES=(0 500 1000 1500 2000 2500 3000 3500 4000 4500 5000)
TOTAL_BATCHES=${#BATCHES[@]}

for i in "${!BATCHES[@]}"; do
    START=${BATCHES[$i]}
    BATCH_NUM=$((i + 1))
    
    echo ""
    echo "========================================"
    echo "批次 $BATCH_NUM / $TOTAL_BATCHES: 股票 $((START + 1)) - $((START + BATCH_SIZE))"
    echo "========================================"
    
    python3 batch_update_sentiment.py --batch $BATCH_SIZE --start $START
    
    # 批次间休息5秒
    if [ $BATCH_NUM -lt $TOTAL_BATCHES ]; then
        echo "休息5秒后继续..."
        sleep 5
    fi
done

echo ""
echo "========================================"
echo "✅ 全量更新完成"
echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 显示最终统计
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('src/data_cache/sentiment_cache.db')
cursor = conn.execute("SELECT COUNT(*) FROM sentiment_cache WHERE date=date('now')")
count = cursor.fetchone()[0]
print(f"\n📊 今日舆情数据总数: {count} 只")
conn.close()
EOF
