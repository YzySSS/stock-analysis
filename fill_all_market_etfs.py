#!/usr/bin/env python3
"""
获取全市场所有ETF并补充历史数据
"""

import sys
sys.path.insert(0, 'src')

import baostock as bs
import logging
from stock_history_db import StockHistoryDB
from typing import List
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_all_etfs_from_baostock() -> List[str]:
    """从Baostock获取所有ETF代码"""
    etf_codes = []
    
    # ETF代码特征：
    # 深圳：159xxx, 160xxx
    # 上海：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 518xxx, 560xxx-569xxx, 580xxx-589xxx
    
    # 通过查询所有股票，筛选ETF
    logger.info("查询Baostock所有股票...")
    
    # 上海ETF
    for prefix in ['510', '511', '512', '513', '515', '518', '560', '561', '562', '563', '564', '565', '566', '567', '568', '569', '580', '581', '582', '583', '584', '585', '586', '587', '588', '589']:
        try:
            rs = bs.query_stock_basic(code_name="", code=f"sh.{prefix}")
            if rs.error_code == '0':
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    code = row[0].split('.')[1]
                    code_name = row[1]
                    # 确认是ETF或LOF
                    if 'ETF' in code_name or 'LOF' in code_name or '基金' in code_name:
                        etf_codes.append(code)
        except Exception as e:
            logger.warning(f"获取 sh.{prefix} 失败: {e}")
    
    # 深圳ETF
    for prefix in ['159', '160']:
        try:
            rs = bs.query_stock_basic(code_name="", code=f"sz.{prefix}")
            if rs.error_code == '0':
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    code = row[0].split('.')[1]
                    code_name = row[1]
                    if 'ETF' in code_name or 'LOF' in code_name or '基金' in code_name:
                        etf_codes.append(code)
        except Exception as e:
            logger.warning(f"获取 sz.{prefix} 失败: {e}")
    
    return list(set(etf_codes))

def fetch_etf_history(code: str, days: int = 60):
    """获取ETF历史数据"""
    try:
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
            adjustflag='2'
        )
        
        if rs.error_code != '0':
            return None
        
        prices_data = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[1] and float(row[1]) > 0:
                prices_data.append({
                    'date': row[0],
                    'close': float(row[1]),
                    'volume': int(row[2]) if row[2] else 0
                })
        
        return prices_data[-days:] if len(prices_data) > days else prices_data
        
    except Exception as e:
        return None

def main():
    db = StockHistoryDB()
    
    print("="*60)
    print("获取全市场所有ETF并补充历史数据")
    print("="*60)
    
    # 登录Baostock
    logger.info("登录Baostock...")
    bs.login()
    
    # 获取所有ETF
    logger.info("获取全市场ETF列表...")
    all_etfs = get_all_etfs_from_baostock()
    logger.info(f"发现 {len(all_etfs)} 只ETF")
    
    # 如果没有获取到，使用预设列表
    if len(all_etfs) < 50:
        logger.warning("从Baostock获取ETF列表失败，使用预设列表")
        # 全市场ETF代码段
        all_etfs = []
        # 上海ETF: 510xxx-589xxx
        for i in range(510000, 590000):
            all_etfs.append(str(i))
        # 深圳ETF: 159xxx-160xxx
        for i in range(159000, 161000):
            all_etfs.append(str(i))
    
    # 去重并排序
    all_etfs = sorted(list(set(all_etfs)))
    
    logger.info(f"计划处理 {len(all_etfs)} 只ETF")
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for i, code in enumerate(all_etfs, 1):
        # 检查是否已有数据
        existing = db.get_prices(code, days=1)
        if existing:
            skip_count += 1
            if i % 100 == 0:
                logger.info(f"[{i}/{len(all_etfs)}] 已存在: {code} 等")
            continue
        
        try:
            prices_data = fetch_etf_history(code, days=60)
            
            if prices_data and len(prices_data) > 0:
                db.save_prices(code, prices_data)
                success_count += 1
                if success_count % 10 == 0:
                    logger.info(f"[{i}/{len(all_etfs)}] ✅ 新增: {code} ({len(prices_data)}天)")
            else:
                fail_count += 1
                if fail_count % 10 == 0:
                    logger.info(f"[{i}/{len(all_etfs)}] ⚠️ 无数据: {code}")
                    
        except Exception as e:
            fail_count += 1
        
        # 每10个暂停一下，避免频率限制
        if i % 10 == 0:
            time.sleep(0.1)
    
    bs.logout()
    
    print("\n" + "="*60)
    print(f"完成: 成功 {success_count}, 跳过 {skip_count}, 失败 {fail_count}")
    print("="*60)
    
    # 更新统计
    stats = db.get_stats()
    print(f"\n数据库统计:")
    print(f"  总股票数: {stats.get('stock_count', 'N/A')}")
    print(f"  总记录数: {stats.get('record_count', 'N/A')}")

if __name__ == "__main__":
    main()
