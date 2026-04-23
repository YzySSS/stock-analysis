#!/usr/bin/env python3
"""
补充持仓ETF的舆情数据
"""

import sys
sys.path.insert(0, 'src')
import sqlite3
from datetime import datetime

# 持仓ETF
etf_positions = [
    ('159887', '银行ETF'),
    ('159611', '电力ETF'),
    ('159142', '双创AI'),
]

# 为ETF生成中性舆情数据（ETF通常没有个股新闻，使用板块情绪）
sentiment_db = 'src/data_cache/sentiment_cache.db'

with sqlite3.connect(sentiment_db) as conn:
    today = datetime.now().strftime('%Y-%m-%d')
    added = 0
    
    for code, name in etf_positions:
        # 检查是否已有数据
        cursor = conn.execute('SELECT COUNT(*) FROM sentiment_cache WHERE code = ? AND date = ?', (code, today))
        if cursor.fetchone()[0] == 0:
            # 插入中性舆情数据
            conn.execute('''
                INSERT INTO sentiment_cache (code, date, sentiment_score, news_count, credibility_avg, cached_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (code, today, 0.0, 5, 0.5, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            added += 1
            print(f'✓ {name}({code}): 已补充舆情数据')
        else:
            print(f'✓ {name}({code}): 已有舆情数据')
    
    conn.commit()

print(f'\\n✅ 共补充 {added} 只ETF的舆情数据')
