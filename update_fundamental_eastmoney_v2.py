#!/usr/bin/env python3
"""
使用Bright Data代理重新获取东方财富PE/PB数据 (带重试版本)
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')

import pymysql
from config import DB_CONFIG
import requests
import time
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Bright Data代理配置
PROXY_URL = "http://brd-customer-hl_8abbb7fa-zone-isp_proxy1:1chayfaf4h24@brd.superproxy.io:33335"

def get_proxy():
    return {
        'http': PROXY_URL,
        'https': PROXY_URL
    }

def fetch_eastmoney_fundamental(code, max_retries=3):
    """从东方财富获取单只股票基本面数据（带重试）"""
    # 转换代码格式
    if code.startswith('6'):
        secid = f"1.{code}"
    elif code.startswith('0') or code.startswith('3'):
        secid = f"0.{code}"
    else:
        return None
    
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        'secid': secid,
        'fields': 'f57,f58,f162,f163,f164',  # 代码,名称,PE(静),PE(TTM),PB
        '_': int(time.time() * 1000)
    }
    
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, proxies=get_proxy(), timeout=30)
            data = resp.json()
            
            if data.get('data'):
                d = data['data']
                pe_static = d.get('f162')  # 静态PE
                pe_ttm = d.get('f163')     # 动态PE(TTM)
                pb = d.get('f164')         # PB
                
                result = {}
                if pe_ttm and pe_ttm != '-' and pe_ttm != '511':
                    result['pe'] = float(pe_ttm)
                elif pe_static and pe_static != '-' and pe_static != '511':
                    result['pe'] = float(pe_static)
                
                if pb and pb != '-' and float(pb) > 0 and pb != '511':
                    result['pb'] = float(pb)
                
                return result if result else None
                
        except requests.exceptions.Timeout:
            logging.warning(f"{code}: 超时，重试 {attempt+1}/{max_retries}")
            time.sleep(2)
        except Exception as e:
            logging.warning(f"{code}: 失败 - {e}")
            time.sleep(1)
    
    return None

def batch_update(limit=500):
    """批量更新"""
    conn = pymysql.connect(**DB_CONFIG)
    
    # 获取需要更新的股票（缺少PB的优先）
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code FROM stock_basic 
            WHERE is_delisted = 0 
            AND pb_ratio IS NULL
            AND (code LIKE '6%%' OR code LIKE '0%%' OR code LIKE '3%%')
            LIMIT %s
        """ % limit)
        codes = [row[0] for row in cursor.fetchall()]
    
    print(f"需要更新{len(codes)}只股票")
    
    updated = 0
    failed = 0
    
    for i, code in enumerate(codes):
        if i % 50 == 0:
            print(f"进度: {i}/{len(codes)}, 成功:{updated}, 失败:{failed}")
        
        data = fetch_eastmoney_fundamental(code)
        if data:
            try:
                with conn.cursor() as cursor:
                    if 'pe' in data and 'pb' in data:
                        cursor.execute("""
                            UPDATE stock_basic 
                            SET pe_ratio = %s, pb_ratio = %s, updated_at = NOW()
                            WHERE code = %s
                        """, (data['pe'], data['pb'], code))
                    elif 'pb' in data:
                        cursor.execute("""
                            UPDATE stock_basic 
                            SET pb_ratio = %s, updated_at = NOW()
                            WHERE code = %s
                        """, (data['pb'], code))
                conn.commit()
                updated += 1
            except Exception as e:
                failed += 1
                print(f"更新{code}失败: {e}")
        else:
            failed += 1
        
        time.sleep(0.5)  # 控制请求频率
    
    conn.close()
    print(f"\n完成: 成功{updated}只, 失败{failed}只")
    
    # 显示更新后的统计
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE pe_ratio IS NOT NULL")
        pe_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE pb_ratio IS NOT NULL")
        pb_count = cursor.fetchone()[0]
        print(f"更新后: PE={pe_count}只, PB={pb_count}只")
    conn.close()

if __name__ == '__main__':
    batch_update(limit=500)
