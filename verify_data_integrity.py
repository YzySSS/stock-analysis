#!/usr/bin/env python3
"""
验证历史数据完整性
检查：
1. 每只股票的交易日数量是否合理
2. 日期连续性（是否有缺失交易日）
3. 数据字段完整性（OHLCV是否有缺失）
4. 异常值检测
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import pymysql
from config import DB_CONFIG
from datetime import datetime
import pandas as pd

def verify_data_integrity():
    conn = pymysql.connect(**DB_CONFIG)
    
    print("=" * 70)
    print("📊 历史数据完整性验证报告")
    print("=" * 70)
    
    # 1. 基础统计
    print("\n1️⃣ 基础统计")
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(DISTINCT code) FROM stock_kline")
        total_stocks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stock_kline")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM stock_kline")
        min_date, max_date = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM stock_kline")
        trade_days = cursor.fetchone()[0]
        
    print(f"   股票总数: {total_stocks}")
    print(f"   总记录数: {total_records:,}")
    print(f"   日期范围: {min_date} ~ {max_date}")
    print(f"   交易日数: {trade_days}")
    
    # 2. 每只股票的记录数统计
    print("\n2️⃣ 单只股票记录数检查")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code, COUNT(*) as cnt 
            FROM stock_kline 
            GROUP BY code 
            ORDER BY cnt DESC
        """)
        results = cursor.fetchall()
        
        counts = [r[1] for r in results]
        avg_count = sum(counts) / len(counts)
        max_count = max(counts)
        min_count = min(counts)
        
    print(f"   平均记录数: {avg_count:.0f}")
    print(f"   最多记录: {max_count} 条")
    print(f"   最少记录: {min_count} 条")
    
    # 找出记录数异常的股票
    low_count_stocks = [(code, cnt) for code, cnt in results if cnt < 500]
    if low_count_stocks:
        print(f"   ⚠️  记录数<500的股票: {len(low_count_stocks)} 只（可能是新股或数据缺失）")
        for code, cnt in low_count_stocks[:5]:
            print(f"      - {code}: {cnt} 条")
    else:
        print("   ✅ 所有股票记录数正常")
    
    # 3. 数据字段完整性检查
    print("\n3️⃣ 数据字段完整性检查")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
                SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) as null_high,
                SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) as null_low,
                SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
            FROM stock_kline
        """)
        null_counts = cursor.fetchone()
        
    fields = ['open', 'high', 'low', 'close', 'volume']
    all_ok = True
    for field, count in zip(fields, null_counts):
        if count > 0:
            print(f"   ⚠️  {field} 字段缺失: {count} 条")
            all_ok = False
    if all_ok:
        print("   ✅ 所有字段无缺失值")
    
    # 4. 价格异常值检查
    print("\n4️⃣ 价格异常值检查")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code, trade_date, close
            FROM stock_kline
            WHERE close <= 0 OR close > 10000
            LIMIT 10
        """)
        anomalies = cursor.fetchall()
        
    if anomalies:
        print(f"   ⚠️  发现 {len(anomalies)} 条异常价格数据")
        for code, date, close in anomalies[:5]:
            print(f"      - {code} @ {date}: {close}")
    else:
        print("   ✅ 价格数据正常")
    
    # 5. 成交量异常检查
    print("\n5️⃣ 成交量异常检查")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code, trade_date, volume
            FROM stock_kline
            WHERE volume < 0
            LIMIT 5
        """)
        vol_anomalies = cursor.fetchall()
        
    if vol_anomalies:
        print(f"   ⚠️  发现 {len(vol_anomalies)} 条异常成交量数据")
    else:
        print("   ✅ 成交量数据正常")
    
    # 6. 日期连续性检查（抽样）
    print("\n6️⃣ 日期连续性检查（抽样10只）")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT code FROM stock_kline 
            ORDER BY RAND() LIMIT 10
        """)
        sample_codes = [r[0] for r in cursor.fetchall()]
        
        for code in sample_codes:
            cursor.execute("""
                SELECT trade_date FROM stock_kline 
                WHERE code = %s 
                ORDER BY trade_date
            """, (code,))
            dates = [r[0] for r in cursor.fetchall()]
            
            # 检查是否有明显缺失（相邻日期差>7天）
            gaps = []
            for i in range(1, len(dates)):
                delta = (dates[i] - dates[i-1]).days
                if delta > 7:  # 超过7天可能是长假，但连续缺失需检查
                    gaps.append((dates[i-1], dates[i], delta))
            
            if gaps:
                print(f"   ⚠️  {code}: 发现 {len(gaps)} 个日期间隔>7天")
            else:
                print(f"   ✅ {code}: 日期连续 ({len(dates)} 条)")
    
    # 7. 与stock_basic表的一致性检查
    print("\n7️⃣ 与 stock_basic 表一致性检查")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT code) 
            FROM stock_kline 
            WHERE code NOT IN (SELECT code FROM stock_basic)
        """)
        missing_in_basic = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM stock_basic 
            WHERE code NOT IN (SELECT DISTINCT code FROM stock_kline)
        """)
        no_kline_data = cursor.fetchone()[0]
        
    if missing_in_basic > 0:
        print(f"   ⚠️  {missing_in_basic} 只股票在kline中有但basic中无")
    else:
        print("   ✅ kline数据的股票都在basic表中")
        
    if no_kline_data > 0:
        print(f"   ⚠️  {no_kline_data} 只股票在basic中有但无kline数据")
    else:
        print("   ✅ 所有basic表股票都有kline数据")
    
    # 8. 回测数据可用性检查
    print("\n8️⃣ 回测数据可用性检查 (2024-2025年)")
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT code) as stock_count,
                COUNT(*) as record_count,
                MIN(trade_date) as min_date,
                MAX(trade_date) as max_date
            FROM stock_kline 
            WHERE trade_date >= '2024-01-01'
        """)
        result = cursor.fetchone()
        
    print(f"   2024年以来股票数: {result[0]}")
    print(f"   2024年以来记录数: {result[1]:,}")
    print(f"   日期范围: {result[2]} ~ {result[3]}")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("✅ 数据完整性验证完成")
    print("=" * 70)

if __name__ == '__main__':
    verify_data_integrity()
