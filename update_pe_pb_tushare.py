#!/usr/bin/env python3
"""
从Tushare获取PE/PB数据并更新数据库
"""

import os
import sys
import time
import pymysql
import tushare as ts
from datetime import datetime

# Tushare Token（从MEMORY.md获取）
TUSHARE_TOKEN = "0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8"

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}

def update_pe_pb():
    """更新PE/PB数据"""
    print("="*70)
    print("Tushare PE/PB数据更新")
    print("="*70)
    
    # 初始化Tushare
    pro = ts.pro_api(TUSHARE_TOKEN)
    print(f"\n✅ Tushare API初始化成功")
    
    # 连接数据库
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取所有股票代码
    cursor.execute("SELECT code, name FROM stock_basic WHERE is_st=0 AND is_delisted=0")
    stocks = cursor.fetchall()
    print(f"\n📊 需要更新 {len(stocks)} 只股票")
    
    # 获取交易日
    today = datetime.now().strftime('%Y%m%d')
    print(f"\n📅 数据日期: {today}")
    
    # 批量获取daily_basic数据
    updated = 0
    failed = 0
    
    print("\n⏳ 正在获取数据...")
    try:
        # 获取全市场daily_basic数据
        df = pro.daily_basic(trade_date=today, fields='ts_code,pe,pb')
        
        if df.empty:
            # 如果今天没有数据，尝试昨天
            from datetime import timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            print(f"  今天无数据，尝试昨天: {yesterday}")
            df = pro.daily_basic(trade_date=yesterday, fields='ts_code,pe,pb')
        
        print(f"\n✅ 获取到 {len(df)} 条数据")
        
        # 转换为字典方便查询
        pe_pb_map = {}
        for _, row in df.iterrows():
            code = row['ts_code'][:6]  # 提取股票代码
            pe = row['pe']
            pb = row['pb']
            pe_pb_map[code] = {'pe': pe, 'pb': pb}
        
        # 更新数据库
        print("\n⏳ 正在更新数据库...")
        for code, name in stocks:
            if code in pe_pb_map:
                pe = pe_pb_map[code]['pe']
                pb = pe_pb_map[code]['pb']
                
                # 清理异常值
                if pe and (pe < 0 or pe > 1000):
                    pe = None
                if pb and (pb < 0 or pb > 100):
                    pb = None
                
                cursor.execute("""
                    UPDATE stock_basic 
                    SET pe_tushare = %s, pb_tushare = %s, updated_at = NOW()
                    WHERE code = %s
                """, (pe, pb, code))
                updated += 1
            else:
                failed += 1
            
            if (updated + failed) % 500 == 0:
                print(f"  进度: {updated + failed}/{len(stocks)} (成功:{updated}, 失败:{failed})")
                conn.commit()
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ 错误: {e}")
    
    # 统计结果
    print("\n" + "="*70)
    print("更新结果")
    print("="*70)
    print(f"  成功更新: {updated}只")
    print(f"  更新失败: {failed}只")
    
    # 检查更新后的数据
    cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE pe_tushare IS NOT NULL")
    total_with_pe = cursor.fetchone()[0]
    print(f"  数据库中现在有PE数据: {total_with_pe}只")
    
    cursor.close()
    conn.close()
    
    print("\n✅ 更新完成!")


def check_existing_data():
    """检查现有数据"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print("现有PE/PB数据统计")
    print("="*70)
    
    # 检查是否有pe_tushare字段
    cursor.execute("DESCRIBE stock_basic")
    columns = [row[0] for row in cursor.fetchall()]
    
    if 'pe_tushare' not in columns:
        print("\n⏳ 添加pe_tushare和pb_tushare字段...")
        cursor.execute("ALTER TABLE stock_basic ADD COLUMN pe_tushare FLOAT")
        cursor.execute("ALTER TABLE stock_basic ADD COLUMN pb_tushare FLOAT")
        conn.commit()
        print("✅ 字段添加完成")
    
    # 统计
    cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE pe_tushare IS NOT NULL")
    tushare_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE pe_ratio IS NOT NULL")
    original_count = cursor.fetchone()[0]
    
    print(f"\n  Tushare PE数据: {tushare_count}只")
    print(f"  原始PE数据: {original_count}只")
    
    cursor.close()
    conn.close()
    
    return tushare_count == 0  # 返回是否需要更新


if __name__ == '__main__':
    need_update = check_existing_data()
    
    if need_update:
        update_pe_pb()
    else:
        print("\n✅ Tushare数据已存在，跳过更新")
        print("  如需强制更新，请删除pe_tushare字段后重新运行")
