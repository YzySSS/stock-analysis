#!/usr/bin/env python3
"""
使用BaoStock获取基本面数据 + Bright Data代理获取东方财富PE/PB
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import baostock as bs
import pymysql
from config import DB_CONFIG
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def update_from_baostock():
    """从BaoStock获取财务数据"""
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取所有股票代码
    with conn.cursor() as cursor:
        cursor.execute("SELECT code FROM stock_basic WHERE is_delisted = 0")
        codes = [row[0] for row in cursor.fetchall()]
    
    print(f"总共{len(codes)}只股票需要更新")
    
    # 登录BaoStock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"BaoStock登录失败: {lg.error_msg}")
        return
    
    updated = 0
    failed = 0
    
    try:
        for i, code in enumerate(codes):
            if i % 100 == 0:
                print(f"进度: {i}/{len(codes)}, 成功:{updated}, 失败:{failed}")
            
            # 转换代码格式
            if code.startswith('6'):
                bs_code = f"sh.{code}"
            elif code.startswith('0') or code.startswith('3'):
                bs_code = f"sz.{code}"
            else:
                continue
            
            try:
                # 查询杜邦分析数据（包含ROE）
                rs = bs.query_dupont_data(code=bs_code, year=2024, quarter=4)
                if rs.error_code == '0' and rs.next():
                    roe = rs.get_row_data()[3]  # dupontROE
                    if roe and roe != '':
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                UPDATE stock_basic 
                                SET roe = %s, updated_at = NOW()
                                WHERE code = %s
                            """, (float(roe), code))
                        conn.commit()
                        updated += 1
            except Exception as e:
                failed += 1
                if failed % 50 == 1:
                    print(f"  错误示例 {code}: {e}")
    finally:
        bs.logout()
        conn.close()
    
    print(f"\n完成: 成功{updated}只, 失败{failed}只")

if __name__ == '__main__':
    update_from_baostock()
