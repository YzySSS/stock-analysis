#!/usr/bin/env python3
"""
使用Tushare fina_indicator接口补充ROE数据 - 简化版
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import pymysql
from config import DB_CONFIG
import tushare as ts
import time

token = '0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8'
pro = ts.pro_api(token)

conn = pymysql.connect(**DB_CONFIG)

# 获取需要更新ROE的股票
cursor = conn.cursor()
cursor.execute("""
    SELECT code FROM stock_basic 
    WHERE is_delisted = 0 AND is_etf = 0
    AND (roe IS NULL OR roe = 0)
    LIMIT 100
""")
codes = [row[0] for row in cursor.fetchall()]

print(f"需要更新ROE的股票: {len(codes)} 只 (先处理100只测试)")

updated = 0
failed = 0

for i, code in enumerate(codes):
    # 转换代码格式
    if code.startswith('6'):
        ts_code = f"{code}.SH"
    elif code.startswith('8') or code.startswith('4'):
        ts_code = f"{code}.BJ"
    else:
        ts_code = f"{code}.SZ"
    
    try:
        df = pro.fina_indicator(ts_code=ts_code, period='20241231', 
                               fields='ts_code,roe')
        if not df.empty and len(df) > 0:
            roe = df.iloc[0]['roe']
            if roe and roe == roe:  # 检查不是NaN
                cursor.execute("""
                    UPDATE stock_basic 
                    SET roe = %s, updated_at = NOW()
                    WHERE code = %s
                """, (roe, code))
                conn.commit()
                updated += 1
                print(f"✅ {code}: ROE={roe}")
            else:
                print(f"⚠️  {code}: ROE为空")
        else:
            print(f"⚠️  {code}: 无数据")
    except Exception as e:
        print(f"❌ {code}: {e}")
        failed += 1
    
    time.sleep(0.5)
    
    if (i + 1) % 10 == 0:
        print(f"  进度: {i+1}/{len(codes)}")

conn.close()

print(f"\n完成: 成功{updated}, 失败{failed}")
