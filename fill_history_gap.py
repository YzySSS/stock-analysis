#!/usr/bin/env python3
"""
历史数据补充脚本
================
补充缺失的15个交易日历史数据（用于补足60个交易日）

用法:
  python3 fill_history_gap.py           # 补充缺失的15个交易日
  python3 fill_history_gap.py --days 15 # 补充指定天数
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional

import pymysql
import baostock as bs

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


class HistoryGapFiller:
    """历史数据缺口填充器"""
    
    def __init__(self):
        self.conn = None
        # 计算需要补充的日期范围
        # 当前脚本获取的是 2026-01-22 开始的数据
        # 需要往前再推约15个交易日
        self.end_date = (datetime.now() - timedelta(days=75)).strftime('%Y-%m-%d')  # 2026-01-21左右
        self.start_date = (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')  # 2025-12-25左右
        
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
    
    def get_stocks_to_fill(self) -> List[Dict]:
        """获取需要补充数据的股票列表（已有数据但缺少早期数据的股票）"""
        try:
            with self.conn.cursor() as cursor:
                # 先获取已有kline数据的股票代码列表
                cursor.execute('SELECT DISTINCT code FROM stock_kline')
                codes_with_data = [row[0] for row in cursor.fetchall()]
                
                if not codes_with_data:
                    logger.info("⚠️ 没有已填充数据的股票")
                    return []
                
                # 检查这些股票是否缺少早期数据
                placeholders = ','.join(['%s'] * len(codes_with_data))
                cursor.execute(f'''
                    SELECT sb.code, sb.market 
                    FROM stock_basic sb
                    WHERE sb.code IN ({placeholders})
                    AND sb.is_delisted = 0
                    AND sb.is_etf = 0
                ''', tuple(codes_with_data))
                
                stocks = []
                for row in cursor.fetchall():
                    stocks.append({
                        'code': row[0],
                        'market': row[1]
                    })
                
                logger.info(f"✅ 获取到 {len(stocks)} 只需要补充历史数据的股票")
                return stocks
                
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return []
    
    def fetch_history_baostock(self, code: str, market: str) -> Optional[List[Dict]]:
        """从 BaoStock 获取历史数据"""
        try:
            bs_code = f"{market}.{code}"
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount,pctChg,turn',
                start_date=self.start_date,
                end_date=self.end_date,
                frequency='d',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code != '0':
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
            
            return data_list if data_list else None
            
        except Exception as e:
            logger.debug(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def save_kline_data(self, code: str, data_list: List[Dict]) -> int:
        """保存K线数据到数据库"""
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
    
    def log_update(self, total: int, success: int, failed: int, records: int):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO update_log 
                    (update_type, start_time, end_time, total_stocks, success_count, 
                     fail_count, status, message, data_start_date, data_end_date)
                    VALUES (%s, NOW(), NOW(), %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, ('fill_gap', total, success, failed, 'success', 
                                   f'Records:{records}', self.start_date, self.end_date))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"记录日志失败: {e}")
    
    def run(self, batch_size: int = 300):
        """执行历史数据缺口填充"""
        logger.info("=" * 60)
        logger.info("🚀 历史数据缺口填充")
        logger.info("=" * 60)
        logger.info(f"📅 补充日期范围: {self.start_date} ~ {self.end_date}")
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 获取需要补充的股票
        stocks = self.get_stocks_to_fill()
        if not stocks:
            logger.info("✅ 没有需要补充的股票，数据已完整")
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
                        data_list = self.fetch_history_baostock(stock['code'], stock['market'])
                        
                        if data_list:
                            saved = self.save_kline_data(stock['code'], data_list)
                            if saved > 0:
                                batch_success += 1
                                batch_records += saved
                            else:
                                batch_failed += 1
                        else:
                            batch_failed += 1
                        
                    except Exception as e:
                        batch_failed += 1
                        logger.debug(f"❌ {stock['code']}: {str(e)[:50]}")
                
                success_stocks += batch_success
                failed_stocks += batch_failed
                total_records += batch_records
                
                progress = min(100, (i + len(batch)) / total * 100)
                logger.info(f"⏱️  进度: {progress:.1f}% | 本批次: {batch_success}成功/{batch_failed}失败 | 累计: {success_stocks}成功/{failed_stocks}失败")
            
            # 5. 完成统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 缺口填充完成:")
            logger.info(f"   本次处理: {success_stocks} 只成功, {failed_stocks} 只失败")
            logger.info(f"   本次新增: {total_records} 条记录")
            logger.info("=" * 60)
            
            self.log_update(total, success_stocks, failed_stocks, total_records)
            return True
            
        finally:
            self.logout_baostock()
            if self.conn:
                self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='历史数据缺口填充')
    parser.add_argument('--batch-size', type=int, default=300,
                       help='每批处理的股票数量（默认300）')
    
    args = parser.parse_args()
    
    filler = HistoryGapFiller()
    success = filler.run(batch_size=args.batch_size)
    
    if success:
        logger.info("✅ 历史数据缺口填充完成!")
        sys.exit(0)
    else:
        logger.error("❌ 历史数据缺口填充失败!")
        sys.exit(1)


if __name__ == "__main__":
    main()
