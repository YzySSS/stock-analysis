#!/usr/bin/env python3
"""
使用AkShare获取实时行情（包含PE/PB）
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import pymysql
from config import DB_CONFIG
import akshare as ak
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def update_from_akshare():
    """使用AkShare获取实时行情（含PE/PB）"""
    conn = pymysql.connect(**DB_CONFIG)
    
    try:
        # 获取所有A股实时行情
        print("正在从AkShare获取实时行情...")
        df = ak.stock_zh_a_spot_em()
        print(f"获取到{len(df)}条数据")
        
        # 选择需要的列
        if '代码' in df.columns and '市盈率-动态' in df.columns:
            data = df[['代码', '名称', '市盈率-动态', '市净率', 'ROE']].copy()
            data.columns = ['code', 'name', 'pe', 'pb', 'roe']
            
            # 清理数据
            data = data[data['code'].str.match(r'^\d{6}$')]  # 只保留6位数字代码
            data['pe'] = pd.to_numeric(data['pe'], errors='coerce')
            data['pb'] = pd.to_numeric(data['pb'], errors='coerce')
            data['roe'] = pd.to_numeric(data['roe'], errors='coerce')
            
            # 过滤无效数据
            data = data[(data['pe'] > 0) & (data['pe'] < 1000)]
            data = data[(data['pb'] > 0) & (data['pb'] < 50)]
            
            print(f"有效数据: {len(data)}条")
            
            # 更新数据库
            updated = 0
            with conn.cursor() as cursor:
                for _, row in data.iterrows():
                    try:
                        cursor.execute("""
                            UPDATE stock_basic 
                            SET pe_ratio = %s, pb_ratio = %s, roe = %s, updated_at = NOW()
                            WHERE code = %s
                        """, (row['pe'], row['pb'], row['roe'], row['code']))
                        if cursor.rowcount > 0:
                            updated += 1
                    except Exception as e:
                        logging.warning(f"更新{row['code']}失败: {e}")
                
                conn.commit()
            
            print(f"成功更新{updated}只股票")
        else:
            print(f"数据列不匹配: {df.columns.tolist()}")
            
    except Exception as e:
        print(f"获取失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    import pandas as pd
    update_from_akshare()
