#!/usr/bin/env python3
"""
补充ETF历史数据
==============
为持仓中的ETF补充历史数据到数据库
"""

import sys
sys.path.insert(0, 'src')

import logging
import baostock as bs
from stock_history_db import StockHistoryDB
from fill_history_db import fetch_stock_history_baostock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_etf_history(code: str, days: int = 60):
    """获取ETF历史数据"""
    try:
        # 转换代码格式
        if code.startswith(('00', '30', '15', '16', '18')):
            bs_code = f"sz.{code}"
        else:
            bs_code = f"sh.{code}"
        
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            bs_code,
            'date,close,volume',
            start_date=start_date,
            end_date=end_date,
            frequency='d',
            adjustflag='2'  # 前复权
        )
        
        if rs.error_code != '0':
            logger.warning(f"{code} Baostock错误: {rs.error_msg}")
            return None
        
        prices_data = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[1] and float(row[1]) > 0:  # close价格有效
                prices_data.append({
                    'date': row[0],
                    'close': float(row[1]),
                    'volume': int(row[2]) if row[2] else 0
                })
        
        return prices_data[-days:] if len(prices_data) > days else prices_data
        
    except Exception as e:
        logger.error(f"获取 {code} 失败: {e}")
        return None

# 当前持仓的ETF列表
ETF_CODES = [
    '159887',  # 银行ETF
    '159611',  # 电力ETF
    '159142',  # 双创AI
    '159937',  # 黄金9999
    '510050',  # 上证50ETF
    '510300',  # 沪深300ETF
    '510500',  # 中证500ETF
    '512000',  # 券商ETF
    '512880',  # 证券ETF
    '515790',  # 光伏ETF
    '515030',  # 新能源车ETF
    '512690',  # 酒ETF
    '512170',  # 医疗ETF
    '159915',  # 创业板ETF
    '159952',  # 创业板ETF易方达
]

def main():
    db = StockHistoryDB()
    
    print("="*60)
    print("补充ETF历史数据")
    print("="*60)
    
    # 登录Baostock
    logger.info("登录Baostock...")
    bs.login()
    
    success_count = 0
    fail_count = 0
    
    for code in ETF_CODES:
        # 检查是否已有数据
        existing = db.get_prices(code, days=1)
        if existing:
            logger.info(f"✅ {code}: 已有数据，跳过")
            success_count += 1
            continue
        
        logger.info(f"📊 获取 {code} 历史数据...")
        try:
            # 获取60天历史数据
            prices_data = fetch_etf_history(code, days=60)
            
            if prices_data and len(prices_data) > 0:
                # 保存到数据库
                db.save_prices(code, prices_data)
                logger.info(f"✅ {code}: 成功保存 {len(prices_data)} 天数据")
                success_count += 1
            else:
                logger.warning(f"⚠️ {code}: 未获取到数据")
                fail_count += 1
                
        except Exception as e:
            logger.error(f"❌ {code}: 获取失败 - {e}")
            fail_count += 1
    
    # 登出Baostock
    bs.logout()
    
    print("\n" + "="*60)
    print(f"完成: 成功 {success_count}, 失败 {fail_count}")
    print("="*60)

if __name__ == "__main__":
    main()
