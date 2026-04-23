#!/usr/bin/env python3
"""
使用Tushare fina_indicator接口补充ROE数据 - 智能版
排除ETF和指数，使用多period查询
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
cursor = conn.cursor()

# 只获取正常股票（排除ETF、指数、退市股）
cursor.execute("""
    SELECT code FROM stock_basic 
    WHERE is_delisted = 0 AND is_etf = 0
    AND (name NOT LIKE '%指数%')
    AND (roe IS NULL OR roe = 0)
    LIMIT 200
""")
codes = [row[0] for row in cursor.fetchall()]

print(f"需要更新ROE的正常股票: {len(codes)} 只")

# 尝试的财报期（从最新到较早）
periods = ['20241231', '20240930', '20240630', '20240331', '20231231']

updated = 0
no_data = 0
failed = 0

for i, code in enumerate(codes):
    # 转换代码格式
    if code.startswith('6'):
        ts_code = f"{code}.SH"
    elif code.startswith('8') or code.startswith('4'):
        ts_code = f"{code}.BJ"
    else:
        ts_code = f"{code}.SZ"
    
    roe_found = False
    
    # 尝试多个财报期
    for period in periods:
        try:
            df = pro.fina_indicator(ts_code=ts_code, period=period, 
                                   fields='ts_code,roe')
            if not df.empty and len(df) > 0:
                roe = df.iloc[0]['roe']
                if roe and roe == roe and roe != 0:  # 有效ROE
                    cursor.execute("""
                        UPDATE stock_basic 
                        SET roe = %s, updated_at = NOW()
                        WHERE code = %s
                    """, (float(roe), code))
                    conn.commit()
                    updated += 1
                    print(f"✅ {code} ({period}): ROE={roe}")
                    roe_found = True
                    break
        except Exception as e:
            if '最多访问' in str(e):
                print(f"⏳ 触发限速，等待30秒...")
                time.sleep(30)
                continue
            # 其他错误忽略
    
    if not roe_found:
        print(f"⚠️  {code}: 无ROE数据")
        no_data += 1
    
    time.sleep(0.5)
    
    if (i + 1) % 20 == 0:
        print(f"  进度: {i+1}/{len(codes)} (成功{updated})")

conn.close()

print(f"\n完成: 成功{updated}, 无数据{no_data}, 失败{failed}")
