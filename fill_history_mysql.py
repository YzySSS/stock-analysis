#!/usr/bin/env python3
"""
历史数据填充脚本
================
从 stock_basic 获取非退市股票列表，从 BaoStock 获取60天历史K线数据

用法:
  python3 fill_history_mysql.py           # 增量填充（跳过已有数据）
  python3 fill_history_mysql.py --full    # 全量填充（覆盖已有数据）
  python3 fill_history_mysql.py --days 90 # 获取90天数据
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple

import pymysql
import baostock as bs
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库连接配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class HistoryDataFiller:
    """历史数据填充器"""
    
    def __init__(self):
        self.conn = None
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.start_date = (datetime.now() - timedelta(days=70)).strftime('%Y-%m-%d')  # 获取70天，留余量
        
    def connect_db(self) -> bool:
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def connect_baostock(self) -> bool:
        """登录 BaoStock"""
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
        """登出 BaoStock"""
        try:
            bs.logout()
            logger.info("✅ BaoStock 已登出")
        except:
            pass
    
    def get_stocks_to_update(self, force_full: bool = False, include_etf: bool = False) -> List[Dict]:
        """
        获取需要更新历史数据的股票列表
        
        Args:
            force_full: 是否全量更新（无视已有数据）
            include_etf: 是否包含ETF
            
        Returns:
            [{'code': '000001', 'market': 'sz'}, ...]
        """
        stocks = []
        
        try:
            with self.conn.cursor() as cursor:
                # 基础查询：非退市股票
                sql = '''
                    SELECT code, market 
                    FROM stock_basic 
                    WHERE is_delisted = 0
                '''
                
                # 如果不包含ETF，排除5开头的
                if not include_etf:
                    sql += " AND is_etf = 0"
                
                if not force_full:
                    # 增量更新：获取已有数据的股票
                    existing_codes = self.get_existing_codes()
                    if existing_codes:
                        # 排除已有数据的股票
                        placeholders = ','.join(['%s'] * len(existing_codes))
                        sql += f' AND code NOT IN ({placeholders})'
                        cursor.execute(sql, tuple(existing_codes))
                    else:
                        cursor.execute(sql)
                else:
                    cursor.execute(sql)
                
                for row in cursor.fetchall():
                    stocks.append({
                        'code': row[0],
                        'market': row[1]
                    })
                
                etf_msg = '包含ETF' if include_etf else '不含ETF'
                logger.info(f"✅ 获取到 {len(stocks)} 只需要更新的股票 ({etf_msg})")
                return stocks
                
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return []
    
    def get_existing_codes(self) -> Set[str]:
        """获取已有数据的股票代码（用于增量更新）"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('SELECT DISTINCT code FROM stock_kline WHERE trade_date >= %s', (self.start_date,))
                return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            logger.warning(f"获取已有数据失败: {e}")
            return set()
    
    def fetch_history_baostock(self, code: str, market: str, days: int = 60) -> Optional[List[Dict]]:
        """
        从 BaoStock 获取单只股票历史数据
        
        Args:
            code: 股票代码
            market: 市场 (sh/sz)
            days: 获取天数
            
        Returns:
            [{'trade_date': '...', 'open': ..., 'close': ...}, ...] or None
        """
        try:
            # BaoStock 代码格式
            bs_code = f"{market}.{code}"
            
            # 计算日期范围
            end_date = self.today
            start_date = (datetime.now() - timedelta(days=days+10)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount,pctChg,turn',
                start_date=start_date,
                end_date=end_date,
                frequency='d',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code != '0':
                logger.debug(f"查询失败 {code}: {rs.error_msg}")
                return None
            
            data_list = []
            is_first_day = True  # 标记是否为上市首日
            
            while rs.next():
                row_data = rs.get_row_data()
                trade_date = row_data[0]
                pct_change = float(row_data[7]) if row_data[7] else None
                
                # 跳过新股上市首日数据（涨跌幅可能超过1000%）
                if is_first_day and pct_change and abs(pct_change) > 500:
                    logger.debug(f"🆕 {code}: 跳过上市首日数据 {trade_date} (涨跌幅: {pct_change}%)")
                    is_first_day = False
                    continue
                
                is_first_day = False
                
                # 限制异常涨跌幅
                if pct_change:
                    if pct_change > 999.99:
                        pct_change = 999.99
                    elif pct_change < -999.99:
                        pct_change = -999.99
                
                data_list.append({
                    'trade_date': trade_date,
                    'open': float(row_data[1]) if row_data[1] else None,
                    'high': float(row_data[2]) if row_data[2] else None,
                    'low': float(row_data[3]) if row_data[3] else None,
                    'close': float(row_data[4]) if row_data[4] else None,
                    'volume': int(row_data[5]) if row_data[5] else 0,
                    'amount': float(row_data[6]) if row_data[6] else None,
                    'pct_change': pct_change,
                    'turnover': float(row_data[8]) if row_data[8] else None
                })
            
            # 只返回最近N天
            if len(data_list) > days:
                data_list = data_list[-days:]
            
            return data_list if data_list else None
            
        except Exception as e:
            logger.debug(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def save_kline_data(self, code: str, data_list: List[Dict]) -> int:
        """
        保存K线数据到数据库
        
        Args:
            code: 股票代码
            data_list: K线数据列表
            
        Returns:
            保存成功的条数
        """
        if not data_list:
            return 0
        
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO stock_kline 
                    (code, trade_date, open, high, low, close, volume, amount, 
                     pct_change, turnover, created_at)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    amount = VALUES(amount),
                    pct_change = VALUES(pct_change),
                    turnover = VALUES(turnover),
                    updated_at = NOW()
                '''
                
                saved = 0
                for data in data_list:
                    cursor.execute(sql, (
                        code,
                        data['trade_date'],
                        data['open'],
                        data['high'],
                        data['low'],
                        data['close'],
                        data['volume'],
                        data['amount'],
                        data['pct_change'],
                        data['turnover']
                    ))
                    saved += 1
                
                self.conn.commit()
                return saved
                
        except Exception as e:
            logger.warning(f"❌ 保存 {code} 数据失败: {e}")
            return 0
    
    def log_update(self, update_type: str, total: int, success: int, failed: int, 
                   skip: int = 0, message: str = '', status: str = 'success'):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO update_log 
                    (update_type, start_time, end_time, total_stocks, success_count, 
                     fail_count, skip_count, status, message, data_start_date, data_end_date)
                    VALUES (%s, NOW(), NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (update_type, total, success, failed, skip, status, message, 
                                   self.start_date, self.today))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"记录日志失败: {e}")
    
    def get_db_stats(self) -> Dict:
        """获取数据库统计"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT 
                        COUNT(DISTINCT code) as stock_count,
                        COUNT(*) as total_records,
                        MIN(trade_date) as min_date,
                        MAX(trade_date) as max_date
                    FROM stock_kline
                ''')
                row = cursor.fetchone()
                return {
                    'stock_count': row[0] or 0,
                    'total_records': row[1] or 0,
                    'min_date': row[2],
                    'max_date': row[3]
                }
        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return {}
    
    def run(self, force_full: bool = False, include_etf: bool = False, 
            days: int = 60, batch_size: int = 100):
        """
        执行历史数据填充
        
        Args:
            force_full: 是否全量更新
            include_etf: 是否包含ETF
            days: 获取天数
            batch_size: 每批处理数量
        """
        logger.info("=" * 60)
        logger.info("🚀 历史数据填充")
        logger.info("=" * 60)
        logger.info(f"📅 数据范围: {days}天 ({self.start_date} ~ {self.today})")
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 获取需要更新的股票
        stocks = self.get_stocks_to_update(force_full=force_full, include_etf=include_etf)
        if not stocks:
            logger.info("✅ 所有股票数据已是最新，无需更新")
            return True
        
        # 3. 登录 BaoStock
        if not self.connect_baostock():
            return False
        
        try:
            # 4. 分批处理
            total = len(stocks)
            success_stocks = 0
            failed_stocks = 0
            total_records = 0
            
            for i in range(0, total, batch_size):
                batch = stocks[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (total - 1) // batch_size + 1
                
                logger.info(f"\n📊 批次 {batch_num}/{total_batches}: 处理 {len(batch)} 只股票")
                
                batch_success = 0
                batch_failed = 0
                batch_records = 0
                
                for stock in batch:
                    try:
                        # 获取历史数据
                        data_list = self.fetch_history_baostock(
                            stock['code'], stock['market'], days=days
                        )
                        
                        if data_list and len(data_list) >= 20:  # 至少20天数据
                            # 保存到数据库
                            saved = self.save_kline_data(stock['code'], data_list)
                            if saved > 0:
                                batch_success += 1
                                batch_records += saved
                            else:
                                batch_failed += 1
                        else:
                            # 数据不足（可能是新股或停牌）
                            batch_failed += 1
                            logger.debug(f"⏭️ {stock['code']}: 数据不足 ({len(data_list) if data_list else 0} 条)")
                        
                    except Exception as e:
                        batch_failed += 1
                        logger.debug(f"❌ {stock['code']}: {str(e)[:50]}")
                
                # 批次统计
                success_stocks += batch_success
                failed_stocks += batch_failed
                total_records += batch_records
                
                progress = min(100, (i + len(batch)) / total * 100)
                logger.info(f"⏱️  进度: {progress:.1f}% | 本批次: {batch_success}成功/{batch_failed}失败 | 累计: {success_stocks}成功/{failed_stocks}失败")
            
            # 5. 显示最终统计
            stats = self.get_db_stats()
            logger.info("\n" + "=" * 60)
            logger.info("📊 填充完成统计:")
            logger.info(f"   本次处理: {success_stocks} 只成功, {failed_stocks} 只失败")
            logger.info(f"   本次新增: {total_records} 条记录")
            logger.info(f"   数据库总计: {stats.get('stock_count', 0)} 只股票, {stats.get('total_records', 0)} 条记录")
            logger.info(f"   数据日期: {stats.get('min_date')} ~ {stats.get('max_date')}")
            logger.info("=" * 60)
            
            # 6. 记录日志
            self.log_update('fill_history', total, success_stocks, failed_stocks, 
                          message=f'Days:{days}, Records:{total_records}')
            
            return True
            
        finally:
            self.logout_baostock()
            if self.conn:
                self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='历史数据填充')
    parser.add_argument('--full', action='store_true', 
                       help='强制全量更新（覆盖已有数据）')
    parser.add_argument('--include-etf', action='store_true',
                       help='包含ETF/基金（默认只填充个股）')
    parser.add_argument('--days', type=int, default=60,
                       help='获取历史数据天数（默认60）')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='每批处理的股票数量（默认100）')
    
    args = parser.parse_args()
    
    filler = HistoryDataFiller()
    success = filler.run(
        force_full=args.full,
        include_etf=args.include_etf,
        days=args.days,
        batch_size=args.batch_size
    )
    
    if success:
        logger.info("✅ 历史数据填充完成!")
        sys.exit(0)
    else:
        logger.error("❌ 历史数据填充失败!")
        sys.exit(1)


if __name__ == "__main__":
    main()
