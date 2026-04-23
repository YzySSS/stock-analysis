#!/usr/bin/env python3
"""
补充股票行业数据
===============
从 AkShare 获取股票所属行业并更新到 stock_basic 表
"""

import os
import sys
import pymysql
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


def update_industry_data():
    """更新行业数据"""
    try:
        import akshare as ak
        
        logger.info("从AkShare获取股票行业数据...")
        
        # 获取A股列表（包含行业信息）
        df = ak.stock_zh_a_spot_em()
        
        logger.info(f"获取到 {len(df)} 只股票数据")
        
        # 处理数据
        industry_map = {}
        for _, row in df.iterrows():
            code = str(row.get('代码', '')).strip()
            industry = str(row.get('行业', '')).strip()
            
            if code and industry and industry != 'nan':
                industry_map[code] = industry
        
        logger.info(f"解析出 {len(industry_map)} 只股票的行业信息")
        
        # 更新数据库
        conn = pymysql.connect(**DB_CONFIG)
        updated = 0
        
        with conn.cursor() as cursor:
            for code, industry in industry_map.items():
                try:
                    cursor.execute('''
                        UPDATE stock_basic 
                        SET industry = %s, updated_at = NOW()
                        WHERE code = %s
                    ''', (industry, code))
                    if cursor.rowcount > 0:
                        updated += 1
                except Exception as e:
                    logger.warning(f"更新失败 {code}: {e}")
            
            conn.commit()
        
        conn.close()
        logger.info(f"成功更新 {updated} 只股票的行业信息")
        return True
        
    except ImportError:
        logger.error("请先安装 akshare: pip install akshare")
        return False
    except Exception as e:
        logger.error(f"获取行业数据失败: {e}")
        return False


if __name__ == "__main__":
    update_industry_data()
