#!/usr/bin/env python3
"""
第一阶段失败股票补充脚本
==========================
补充第一阶段填充失败的股票（主要是pct_change超限的新股/ETF）

用法:
  python3 fill_history_retry.py           # 补充失败的股票
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pymysql
import baostock as bs

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


class HistoryRetryFiller:
    """失败股票补充器"""
    
    def __init__(self):
        self.conn = None
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.start_date = (datetime.now() - timedelta(days=70)).strftime('%Y-%m-%d')
        
    def connect_db(self) -> bool:
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def connect_baostock(self) -> bool:
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
    
    def get_failed_stocks(self) -> List[Dict]:
        """获取填充失败的股票（缺少近期数据的）"""
        try:
            with self.conn.cursor() as cursor:
                # 获取已有数据的股票
                cursor.execute('SELECT DISTINCT code FROM stock_kline WHERE trade_date >= %s', (self.start_date,))
                has_data = set(row[0] for row in cursor.fetchall())
                
                # 获取所有应填充的股票
                cursor.execute('SELECT code, market, name FROM stock_basic WHERE is_delisted = 0 AND is_etf = 0')
                all_stocks = cursor.fetchall()
                
                # 找出缺失的
                failed = []
                for row in all_stocks:
                    if row[0] not in has_data:
                        failed.append({
                            'code': row[0],
                            'market': row[1],
                            'name': row[2]
                        })
                
                logger.info(f"✅ 发现 {len(failed)} 只需要补充的股票")
                return failed
                
        except Exception as e:
            logger.error(f"❌ 获取失败股票列表错误: {e}")
            return []
    
    def fetch_history_baostock(self, code: str, market: str) -> Optional[List[Dict]]:
        """获取历史数据"""
        try:
            bs_code = f"{market}.{code}"
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount,pctChg,turn',
                start_date=self.start_date,
                end_date=self.today,
                frequency='d',
                adjustflag='2'
            )
            
            if rs.error_code != '0':
                return None
            
            data_list = []
            while rs.next():
                row_data = rs.get_row_data()
                
                # 处理 pct_change 可能超限的问题
                pct_change = row_data[7]
                if pct_change:
                    try:
                        pct_val = float(pct_change)
                        # 限制在合理范围内 (-1000% 到 1000%)
                        if pct_val > 999.99:
                            pct_change = 999.99
                        elif pct_val < -999.99:
                            pct_change = -999.99
                    except:
                        pct_change = None
                
                data_list.append({
                    'trade_date': row_data[0],
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
            logger.debug(f"获取 {code} 失败: {e}")
            return None
    
    def save_kline_data(self, code: str, data_list: List[Dict]) -> int:
        """保存数据"""
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
                    try:
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
                    except Exception as e:
                        logger.warning(f"保存 {code} {data['trade_date']} 失败: {e}")
                        continue
                
                self.conn.commit()
                return saved
                
        except Exception as e:
            logger.warning(f"❌ 保存 {code} 数据失败: {e}")
            return 0
    
    def run(self):
        """执行补充"""
        logger.info("=" * 60)
        logger.info("🚀 第一阶段失败股票补充")
        logger.info("=" * 60)
        
        if not self.connect_db():
            return False
        
        stocks = self.get_failed_stocks()
        if not stocks:
            logger.info("✅ 没有需要补充的股票")
            return True
        
        if not self.connect_baostock():
            return False
        
        try:
            total = len(stocks)
            success = 0
            failed = 0
            
            for i, stock in enumerate(stocks):
                try:
                    logger.info(f"[{i+1}/{total}] 处理 {stock['code']} {stock['name']}...")
                    
                    data_list = self.fetch_history_baostock(stock['code'], stock['market'])
                    
                    if data_list:
                        saved = self.save_kline_data(stock['code'], data_list)
                        if saved > 0:
                            success += 1
                            logger.info(f"   ✅ 成功保存 {saved} 条记录")
                        else:
                            failed += 1
                            logger.warning(f"   ❌ 保存失败")
                    else:
                        failed += 1
                        logger.warning(f"   ❌ 无数据")
                    
                except Exception as e:
                    failed += 1
                    logger.error(f"   ❌ 处理失败: {e}")
            
            logger.info("\n" + "=" * 60)
            logger.info("📊 补充完成:")
            logger.info(f"   成功: {success} 只")
            logger.info(f"   失败: {failed} 只")
            logger.info("=" * 60)
            
            return True
            
        finally:
            self.logout_baostock()
            if self.conn:
                self.conn.close()


def main():
    filler = HistoryRetryFiller()
    success = filler.run()
    
    if success:
        logger.info("✅ 补充完成!")
        sys.exit(0)
    else:
        logger.error("❌ 补充失败!")
        sys.exit(1)


if __name__ == "__main__":
    main()
