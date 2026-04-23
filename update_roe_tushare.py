#!/usr/bin/env python3
"""
使用Tushare fina_indicator接口补充ROE数据
获取更全面的财务指标
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import pymysql
from config import DB_CONFIG
import tushare as ts
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def update_roe_from_tushare():
    """从Tushare fina_indicator获取ROE并更新到数据库"""
    token = '0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8'
    pro = ts.pro_api(token)
    
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取需要更新ROE的股票（ROE为空的）
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code FROM stock_basic 
            WHERE is_delisted = 0 AND is_etf = 0
            AND (roe IS NULL OR roe = 0)
        """)
        codes = [row[0] for row in cursor.fetchall()]
    
    print(f"📝 需要更新ROE的股票: {len(codes)} 只")
    
    # 获取最近的财报期
    current_year = datetime.now().year
    periods = [f"{current_year}1231", f"{current_year-1}1231", f"{current_year-1}0930"]
    
    updated = 0
    failed = 0
    not_found = 0
    
    for i, code in enumerate(codes):
        # 转换为Tushare格式
        if code.startswith('6'):
            ts_code = f"{code}.SH"
        elif code.startswith('8') or code.startswith('4'):
            ts_code = f"{code}.BJ"
        else:
            ts_code = f"{code}.SZ"
        
        # 尝试多个财报期
        roe_value = None
        for period in periods:
            try:
                df = pro.fina_indicator(ts_code=ts_code, period=period, 
                                       fields='ts_code,roe,roe_waa,roe_dt,grossprofit_margin,netprofit_margin')
                if not df.empty and len(df) > 0:
                    roe_value = df.iloc[0]['roe']
                    if pd.notna(roe_value) and roe_value != 0:
                        break
            except Exception as e:
                logger.debug(f"查询 {ts_code} {period} 失败: {e}")
                continue
            time.sleep(0.3)  # 限速
        
        if roe_value is None or (isinstance(roe_value, float) and pd.isna(roe_value)):
            not_found += 1
            continue
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE stock_basic 
                    SET roe = %s, updated_at = NOW()
                    WHERE code = %s
                """, (roe_value, code))
            conn.commit()
            updated += 1
            
            if (i + 1) % 500 == 0:
                print(f"  进度: {i+1}/{len(codes)} (已更新 {updated})")
                
        except Exception as e:
            logger.error(f"更新 {code} 失败: {e}")
            failed += 1
    
    conn.close()
    
    print(f"\n✅ 完成:")
    print(f"   - 成功更新: {updated} 只")
    print(f"   - 未找到数据: {not_found} 只")
    print(f"   - 更新失败: {failed} 只")
    
    # 显示统计
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE roe IS NOT NULL AND roe != 0")
        roe_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM stock_basic")
        total = cursor.fetchone()[0]
        print(f"\n📊 ROE数据覆盖率: {roe_count}/{total} ({roe_count/total*100:.1f}%)")
    conn.close()

if __name__ == '__main__':
    import pandas as pd
    update_roe_from_tushare()
