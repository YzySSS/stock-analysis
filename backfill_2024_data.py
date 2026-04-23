#!/usr/bin/env python3
"""
历史数据回补脚本 - 2024年数据回填
==================================

用途: 回补2024-01-01 至 2025-12-22的历史K线数据
数据源: BaoStock
策略: 分批回补，每批处理500只股票，避免超时

用法:
  python3 backfill_2024_data.py --start 2024-01-01 --end 2025-12-22
  python3 backfill_2024_data.py --batch-size 300 --start 2024-06-01  # 从指定日期开始
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict

import pymysql
import baostock as bs
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class Backfill2024Data:
    """2024年历史数据回填器"""
    
    def __init__(self, start_date: str, end_date: str, batch_size: int = 500):
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size
        self.conn = None
        self.total_inserted = 0
        self.total_updated = 0
        self.total_errors = 0
        
    def connect_db(self) -> bool:
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def login_baostock(self) -> bool:
        try:
            result = bs.login()
            if result.error_code == '0':
                logger.info("✅ BaoStock 登录成功")
                return True
            else:
                logger.error(f"❌ BaoStock 登录失败: {result.error_msg}")
                return False
        except Exception as e:
            logger.error(f"❌ BaoStock 登录异常: {e}")
            return False
    
    def logout_baostock(self):
        try:
            bs.logout()
            logger.info("✅ BaoStock 已登出")
        except:
            pass
    
    def get_stocks_to_fill(self) -> List[Dict]:
        """获取需要回填数据的股票列表"""
        stocks = []
        try:
            with self.conn.cursor() as cursor:
                # 获取所有非退市、非ETF的有效股票
                sql = '''
                    SELECT code, market, name 
                    FROM stock_basic 
                    WHERE is_delisted = 0 
                    AND (is_etf = 0 OR is_etf IS NULL)
                    ORDER BY code
                '''
                cursor.execute(sql)
                
                for row in cursor.fetchall():
                    code, market, name = row
                    # 转换market格式为BaoStock格式
                    bs_code = f"{market}.{code}"
                    stocks.append({
                        'code': code,
                        'bs_code': bs_code,
                        'name': name
                    })
                
                logger.info(f"📊 获取到 {len(stocks)} 只需要回填的股票")
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
        
        return stocks
    
    def check_existing_data(self, code: str) -> set:
        """检查某只股票已有的数据日期"""
        existing = set()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT trade_date FROM stock_kline WHERE code = %s",
                    (code,)
                )
                for row in cursor.fetchall():
                    existing.add(row[0])
        except Exception as e:
            logger.warning(f"⚠️ 检查 {code} 现有数据失败: {e}")
        return existing
    
    def fetch_kline_from_baostock(self, bs_code: str, code: str) -> List[Dict]:
        """从BaoStock获取K线数据"""
        data = []
        try:
            # 获取数据（不复权，保持原始价格）
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=self.start_date,
                end_date=self.end_date,
                frequency="d",
                adjustflag="3"  # 3=不复权
            )
            
            if rs.error_code != '0':
                logger.error(f"❌ {code} 获取数据失败: {rs.error_msg}")
                return []
            
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                try:
                    # 跳过无效数据
                    if not row[2] or float(row[2]) <= 0:  # open
                        continue
                    
                    data.append({
                        'code': code,
                        'trade_date': row[0],
                        'open': float(row[2]) if row[2] else None,
                        'high': float(row[3]) if row[3] else None,
                        'low': float(row[4]) if row[4] else None,
                        'close': float(row[5]) if row[5] else None,
                        'volume': int(float(row[7])) if row[7] else 0,
                        'amount': float(row[8]) if row[8] else 0,
                        'turnover': float(row[9]) if row[9] else 0,
                        'pct_change': float(row[10]) if row[10] else 0
                    })
                except Exception as e:
                    logger.debug(f"⚠️ {code} 解析数据行失败: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ {code} 获取数据异常: {e}")
        
        return data
    
    def save_to_database(self, records: List[Dict], existing_dates: set) -> tuple:
        """保存数据到数据库"""
        inserted = 0
        updated = 0
        errors = 0
        
        if not records:
            return inserted, updated, errors
        
        try:
            with self.conn.cursor() as cursor:
                # 准备SQL
                insert_sql = '''
                    INSERT INTO stock_kline 
                    (code, trade_date, open, high, low, close, 
                     volume, amount, turnover, pct_change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open=VALUES(open), high=VALUES(high), low=VALUES(low),
                    close=VALUES(close),
                    volume=VALUES(volume), amount=VALUES(amount),
                    turnover=VALUES(turnover), pct_change=VALUES(pct_change)
                '''
                
                batch = []
                for record in records:
                    trade_date = record['trade_date']
                    
                    if trade_date in existing_dates:
                        updated += 1
                    else:
                        inserted += 1
                    
                    batch.append((
                        record['code'], trade_date,
                        record['open'], record['high'], record['low'],
                        record['close'],
                        record['volume'], record['amount'],
                        record['turnover'], record['pct_change']
                    ))
                    
                    # 批量提交
                    if len(batch) >= 500:
                        cursor.executemany(insert_sql, batch)
                        self.conn.commit()
                        batch = []
                
                # 提交剩余数据
                if batch:
                    cursor.executemany(insert_sql, batch)
                    self.conn.commit()
                    
        except Exception as e:
            logger.error(f"❌ 保存数据失败: {e}")
            errors = len(records)
        
        return inserted, updated, errors
    
    def run_backfill(self):
        """执行数据回填"""
        logger.info("=" * 70)
        logger.info("🚀 启动2024年历史数据回填")
        logger.info("=" * 70)
        logger.info(f"回填区间: {self.start_date} 至 {self.end_date}")
        logger.info(f"批次大小: {self.batch_size} 只股票/批")
        logger.info("=" * 70)
        
        # 获取股票列表
        stocks = self.get_stocks_to_fill()
        if not stocks:
            logger.error("❌ 没有需要回填的股票")
            return
        
        total_stocks = len(stocks)
        
        # 分批处理
        for batch_idx in range(0, total_stocks, self.batch_size):
            batch = stocks[batch_idx:batch_idx + self.batch_size]
            batch_num = batch_idx // self.batch_size + 1
            total_batches = (total_stocks + self.batch_size - 1) // self.batch_size
            
            logger.info(f"\n📦 批次 {batch_num}/{total_batches} ({len(batch)}只股票)")
            
            batch_inserted = 0
            batch_updated = 0
            
            for stock in batch:
                code = stock['code']
                bs_code = stock['bs_code']
                
                # 检查现有数据
                existing = self.check_existing_data(code)
                
                # 获取BaoStock数据
                records = self.fetch_kline_from_baostock(bs_code, code)
                
                if records:
                    # 保存数据
                    inserted, updated, errors = self.save_to_database(records, existing)
                    batch_inserted += inserted
                    batch_updated += updated
                    self.total_errors += errors
                    
                    logger.debug(f"  ✅ {code}: +{inserted}条新数据, ~{updated}条更新")
                else:
                    logger.debug(f"  ⚠️ {code}: 无数据")
            
            self.total_inserted += batch_inserted
            self.total_updated += batch_updated
            
            logger.info(f"✅ 批次 {batch_num} 完成: +{batch_inserted}条新数据, ~{batch_updated}条更新")
            logger.info(f"📊 累计进度: {min(batch_idx + self.batch_size, total_stocks)}/{total_stocks} 只股票")
            logger.info(f"📊 累计数据: +{self.total_inserted}条新, ~{self.total_updated}条更新")
        
        # 总结
        logger.info("\n" + "=" * 70)
        logger.info("🎉 数据回填完成!")
        logger.info("=" * 70)
        logger.info(f"总股票数: {total_stocks}")
        logger.info(f"新增记录: {self.total_inserted:,} 条")
        logger.info(f"更新记录: {self.total_updated:,} 条")
        logger.info(f"错误记录: {self.total_errors} 条")
        logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='2024年历史数据回填')
    parser.add_argument('--start', default='2024-01-01', help='开始日期 (默认: 2024-01-01)')
    parser.add_argument('--end', default='2025-12-22', help='结束日期 (默认: 2025-12-22)')
    parser.add_argument('--batch-size', type=int, default=500, help='每批处理股票数 (默认: 500)')
    
    args = parser.parse_args()
    
    # 初始化
    backfill = Backfill2024Data(
        start_date=args.start,
        end_date=args.end,
        batch_size=args.batch_size
    )
    
    # 连接数据库
    if not backfill.connect_db():
        sys.exit(1)
    
    # 登录BaoStock
    if not backfill.login_baostock():
        sys.exit(1)
    
    try:
        # 执行回填
        backfill.run_backfill()
    finally:
        backfill.logout_baostock()
        if backfill.conn:
            backfill.conn.close()


if __name__ == '__main__':
    main()
