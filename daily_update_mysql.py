#!/usr/bin/env python3
"""
每日K线数据更新脚本
==================
每天 20:00 运行（收盘后）
- 检查今天是否为交易日
- 如果是，获取全A股今天的K线数据并存入数据库
- 非交易日自动跳过
- 运行结束后输出结构化报告

用法:
  python3 daily_update_mysql.py           # 更新今天的数据
  python3 daily_update_mysql.py --date 20250403  # 更新指定日期
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/daily_update_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
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


class DailyUpdater:
    """每日数据更新器"""
    
    def __init__(self):
        self.conn = None
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.is_trading_day = False
        self.start_time = None
        self.end_time = None
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'new_records': 0
        }
        self.report_lines = []
        
    def connect_db(self) -> bool:
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            self.report_lines.append("✅ 数据库连接：成功")
            return True
        except Exception as e:
            self.report_lines.append(f"❌ 数据库连接：失败 - {e}")
            return False
    
    def connect_baostock(self) -> bool:
        """登录 BaoStock"""
        try:
            result = bs.login()
            if result.error_code == '0':
                self.report_lines.append("✅ BaoStock登录：成功")
                return True
            else:
                self.report_lines.append(f"❌ BaoStock登录：失败 - {result.error_msg}")
                return False
        except Exception as e:
            self.report_lines.append(f"❌ BaoStock登录异常：{e}")
            return False
    
    def logout_baostock(self):
        """登出 BaoStock"""
        try:
            bs.logout()
        except:
            pass
    
    def check_trading_day(self, date_str: str = None) -> bool:
        """检查指定日期是否为交易日"""
        check_date = date_str or self.today
        
        try:
            rs = bs.query_history_k_data_plus(
                'sh.000001',
                'date,open,close',
                start_date=check_date,
                end_date=check_date,
                frequency='d'
            )
            
            if rs.error_code != '0':
                return False
            
            if rs.next():
                row_data = rs.get_row_data()
                if row_data[1] and row_data[2] and float(row_data[1]) > 0:
                    self.is_trading_day = True
                    return True
            
            self.is_trading_day = False
            return False
            
        except Exception as e:
            return False
    
    def get_all_stocks(self) -> List[Dict]:
        """从 stock_basic 获取全A股列表（非退市）"""
        stocks = []
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    SELECT code, market 
                    FROM stock_basic 
                    WHERE is_delisted = 0
                '''
                cursor.execute(sql)
                
                for row in cursor.fetchall():
                    stocks.append({
                        'code': row[0],
                        'market': row[1]
                    })
                
                return stocks
                
        except Exception as e:
            return []
    
    def get_existing_codes(self, date_str: str) -> Set[str]:
        """获取指定日期已有数据的股票代码"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    'SELECT DISTINCT code FROM stock_kline WHERE trade_date = %s',
                    (date_str,)
                )
                return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            return set()
    
    def fetch_today_data(self, code: str, market: str, date_str: str) -> Optional[Dict]:
        """获取单只股票指定日期的K线数据"""
        try:
            bs_code = f"{market}.{code}"
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount,pctChg,turn',
                start_date=date_str,
                end_date=date_str,
                frequency='d',
                adjustflag='2'
            )
            
            if rs.error_code != '0':
                return None
            
            if rs.next():
                row_data = rs.get_row_data()
                close_price = row_data[4]
                if not close_price or float(close_price) <= 0:
                    return None
                
                pct_change = float(row_data[7]) if row_data[7] else None
                if pct_change:
                    if pct_change > 999.99:
                        pct_change = 999.99
                    elif pct_change < -999.99:
                        pct_change = -999.99
                
                return {
                    'trade_date': row_data[0],
                    'open': float(row_data[1]) if row_data[1] else None,
                    'high': float(row_data[2]) if row_data[2] else None,
                    'low': float(row_data[3]) if row_data[3] else None,
                    'close': float(close_price),
                    'volume': int(row_data[5]) if row_data[5] else 0,
                    'amount': float(row_data[6]) if row_data[6] else None,
                    'pct_change': pct_change,
                    'turnover': float(row_data[8]) if row_data[8] else None
                }
            
            return None
            
        except Exception as e:
            return None
    
    def save_kline_data(self, code: str, data: Dict) -> bool:
        """保存K线数据到数据库"""
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
                
                self.conn.commit()
                return True
                
        except Exception as e:
            return False
    
    def log_update(self, update_type: str, status: str = 'success', message: str = ''):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO update_log 
                    (update_type, start_time, end_time, total_stocks, success_count, 
                     fail_count, skip_count, status, message, data_end_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (
                    update_type, 
                    self.start_time,
                    self.end_time,
                    self.stats['total'], 
                    self.stats['success'], 
                    self.stats['failed'],
                    self.stats['skipped'],
                    status, 
                    message,
                    self.today
                ))
                self.conn.commit()
        except Exception as e:
            pass
    
    def get_db_stats(self) -> Dict:
        """获取数据库统计"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT 
                        COUNT(DISTINCT code) as stock_count,
                        COUNT(*) as total_records,
                        MAX(trade_date) as max_date
                    FROM stock_kline
                ''')
                row = cursor.fetchone()
                return {
                    'stock_count': row[0] or 0,
                    'total_records': row[1] or 0,
                    'max_date': row[2]
                }
        except Exception as e:
            return {'stock_count': 0, 'total_records': 0, 'max_date': '-'}
    
    def print_report(self):
        """输出结构化报告"""
        elapsed = (self.end_time - self.start_time).total_seconds()
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print("\n" + "=" * 60)
        print("📊 每日K线数据更新报告")
        print("=" * 60)
        print(f"📅 更新日期：{self.today}")
        print(f"⏰ 执行时间：{self.start_time.strftime('%H:%M:%S')} - {self.end_time.strftime('%H:%M:%S')}（约{minutes}分{seconds}秒）")
        print("-" * 60)
        
        if not self.is_trading_day:
            print("📈 任务状态：⏭️ 非交易日，已跳过")
            print("\n📋 执行摘要：")
            print(f"  • {self.today} 不是交易日，无需更新数据")
        else:
            print(f"📈 任务状态：{'✅ 已完成' if self.stats['failed'] == 0 else '⚠️ 部分完成'}")
            print("\n📋 处理进度：")
            print(f"  • 计划更新：{self.stats['total']} 只股票（约{self.stats['total']//300 + 1}批次）")
            print(f"  • 实际完成：{self.stats['success']} 只股票")
            print(f"  • 跳过：{self.stats['skipped']} 只（已有数据/停牌）")
            print(f"  • 失败：{self.stats['failed']} 只")
            success_rate = (self.stats['success'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
            print(f"  • 完成比例：{success_rate:.1f}%")
            print(f"  • 新增记录：{self.stats['new_records']} 条")
        
        print("\n🔌 连接状态：")
        for line in self.report_lines:
            print(f"  • {line}")
        
        db_stats = self.get_db_stats()
        print("\n💾 数据库状态：")
        print(f"  • 股票总数：{db_stats['stock_count']} 只")
        print(f"  • 记录总数：{db_stats['total_records']:,} 条")
        print(f"  • 最新日期：{db_stats['max_date']}")
        
        print("=" * 60)
    
    def run(self, date_str: str = None, batch_size: int = 300):
        """执行每日更新"""
        target_date = date_str or self.today
        self.start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("🚀 每日K线数据更新")
        logger.info("=" * 60)
        logger.info(f"📅 目标日期: {target_date}")
        
        # 1. 连接数据库
        if not self.connect_db():
            self.end_time = datetime.now()
            self.print_report()
            return False
        
        # 2. 登录 BaoStock
        if not self.connect_baostock():
            self.end_time = datetime.now()
            self.print_report()
            return False
        
        try:
            # 3. 检查是否为交易日
            if not self.check_trading_day(target_date):
                self.end_time = datetime.now()
                self.log_update('daily_update', status='skipped', message='非交易日')
                self.print_report()
                return True
            
            # 4. 获取全A股列表
            stocks = self.get_all_stocks()
            if not stocks:
                self.end_time = datetime.now()
                self.print_report()
                return False
            
            self.stats['total'] = len(stocks)
            
            # 5. 获取已有数据的股票
            existing_codes = self.get_existing_codes(target_date)
            
            # 6. 分批处理
            for i in range(0, len(stocks), batch_size):
                batch = stocks[i:i+batch_size]
                
                for stock in batch:
                    code = stock['code']
                    
                    # 检查是否已有数据
                    if code in existing_codes:
                        self.stats['skipped'] += 1
                        continue
                    
                    # 获取当天数据
                    data = self.fetch_today_data(code, stock['market'], target_date)
                    
                    if data:
                        if self.save_kline_data(code, data):
                            self.stats['success'] += 1
                            self.stats['new_records'] += 1
                        else:
                            self.stats['failed'] += 1
                    else:
                        self.stats['skipped'] += 1
            
            # 7. 完成统计
            self.end_time = datetime.now()
            
            # 8. 记录日志
            self.log_update(
                'daily_update', 
                status='success' if self.stats['failed'] == 0 else 'partial',
                message=f'新增{self.stats["new_records"]}条记录'
            )
            
            # 9. 输出报告
            self.print_report()
            
            return True
            
        finally:
            self.logout_baostock()
            if self.conn:
                self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='每日K线数据更新')
    parser.add_argument('--date', type=str, default=None,
                       help='指定日期 (格式: YYYY-MM-DD)，默认今天')
    parser.add_argument('--batch-size', type=int, default=300,
                       help='每批处理的股票数量（默认300）')
    
    args = parser.parse_args()
    
    updater = DailyUpdater()
    success = updater.run(
        date_str=args.date,
        batch_size=args.batch_size
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
