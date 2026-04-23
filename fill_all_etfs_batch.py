#!/usr/bin/env python3
"""
获取全市场ETF历史数据 - 批量版
通过遍历ETF代码段，批量获取有数据的ETF
"""

import sys
sys.path.insert(0, 'src')

import baostock as bs
import logging
from stock_history_db import StockHistoryDB
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_etf_history(code: str, days: int = 60):
    """获取ETF历史数据"""
    try:
        if code.startswith(('00', '30', '15', '16', '18')):
            bs_code = f"sz.{code}"
        else:
            bs_code = f"sh.{code}"
        
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

def batch_fetch_etfs():
    """批量获取ETF"""
    db = StockHistoryDB()
    
    print("="*60)
    print("批量获取全市场ETF历史数据")
    print("="*60)
    
    # 登录Baostock
    logger.info("登录Baostock...")
    bs.login()
    
    # ETF代码段
    etf_ranges = [
        # 深圳ETF
        ('159', 0, 1000),      # 159000-159999
        ('160', 0, 200),       # 160000-160199 (LOF)
        # 上海ETF
        ('510', 0, 1000),      # 510000-510999
        ('511', 0, 1000),      # 511000-511999
        ('512', 0, 1000),      # 512000-512999
        ('513', 0, 1000),      # 513000-513999
        ('515', 0, 1000),      # 515000-515999
        ('516', 0, 1000),      # 516000-516999
        ('517', 0, 200),       # 517000-517199
        ('518', 0, 200),       # 518000-518199 (黄金)
        ('560', 0, 1000),      # 560000-560999
        ('561', 0, 1000),      # 561000-561999
        ('562', 0, 1000),      # 562000-562999
        ('563', 0, 1000),      # 563000-563999
        ('564', 0, 200),       # 564000-564199
        ('565', 0, 200),       # 565000-565199
        ('566', 0, 200),       # 566000-566199
        ('567', 0, 200),       # 567000-567199
        ('568', 0, 200),       # 568000-568199
        ('569', 0, 200),       # 569000-569199
        ('580', 0, 100),       # 580000-580099
        ('588', 0, 200),       # 588000-588199 (科创)
        ('589', 0, 200),       # 589000-589199
    ]
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    total_codes = sum(count for _, _, count in etf_ranges)
    processed = 0
    
    for prefix, start, count in etf_ranges:
        logger.info(f"处理 {prefix} 段 ({start}-{start+count})...")
        
        for i in range(start, start + count):
            code = f"{prefix}{i:03d}"
            processed += 1
            
            # 检查是否已有数据
            existing = db.get_prices(code, days=1)
            if existing:
                skip_count += 1
                continue
            
            # 获取数据
            prices_data = fetch_etf_history(code, days=60)
            
            if prices_data and len(prices_data) > 0:
                db.save_prices(code, prices_data)
                success_count += 1
                logger.info(f"✅ {code}: {len(prices_data)}天")
            else:
                fail_count += 1
            
            # 每100个暂停一下
            if processed % 100 == 0:
                logger.info(f"进度: {processed}/{total_codes} (成功{success_count}, 跳过{skip_count}, 失败{fail_count})")
                time.sleep(0.5)
    
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
    batch_fetch_etfs()
