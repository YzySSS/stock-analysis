#!/usr/bin/env python3
"""
股票基础信息初始化脚本
========================
从 BaoStock 获取全A股列表，自动标记 ST/*ST/退市股

用法:
  python3 init_stock_basic.py           # 初始化所有股票基础信息
  python3 init_stock_basic.py --force   # 强制全量更新（清空后重建）
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


class StockBasicInitializer:
    """股票基础信息初始化器"""
    
    def __init__(self):
        self.conn = None
        self.bs = None
        self.today = datetime.now().strftime('%Y-%m-%d')
        
    def connect_db(self):
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
    
    def get_all_stocks_from_baostock(self) -> List[Dict]:
        """
        从 BaoStock 获取所有股票列表
        
        Returns:
            [{'code': '000001', 'name': '平安银行', 'market': 'sz', ...}, ...]
        """
        stocks = []
        
        try:
            # 获取所有股票
            rs = bs.query_all_stock(day=self.today)
            
            if rs.error_code != '0':
                logger.error(f"❌ 获取股票列表失败: {rs.error_msg}")
                return stocks
            
            while rs.next():
                data = rs.get_row_data()
                if len(data) > 0:
                    full_code = data[0]  # 如 'sz.000001'
                    code = full_code.replace('sh.', '').replace('sz.', '').replace('bj.', '')
                    market = 'sh' if full_code.startswith('sh.') else ('bj' if full_code.startswith('bj.') else 'sz')
                    
                    stocks.append({
                        'code': code,
                        'market': market,
                        'full_code': full_code
                    })
            
            logger.info(f"✅ 从 BaoStock 获取到 {len(stocks)} 只股票")
            return stocks
            
        except Exception as e:
            logger.error(f"❌ 获取股票列表异常: {e}")
            return stocks
    
    def get_all_stock_basic_batch(self) -> Dict[str, Dict]:
        """
        批量获取所有股票基础信息（使用 query_stock_basic 不带 code 参数）
        
        Returns:
            {'000001': {'name': '...', 'ipo_date': '...'}, ...}
        """
        stock_info = {}
        
        try:
            # 查询所有股票基本信息（批量）
            rs = bs.query_stock_basic()
            
            if rs.error_code != '0':
                logger.error(f"❌ 批量获取股票信息失败: {rs.error_msg}")
                return stock_info
            
            while rs.next():
                data = rs.get_row_data()
                # BaoStock 返回: [code, name, ipo_date, out_date, type, status]
                full_code = data[0] if len(data) > 0 else ''
                code = full_code.replace('sh.', '').replace('sz.', '').replace('bj.', '')
                
                stock_info[code] = {
                    'name': data[1] if len(data) > 1 else '',
                    'ipo_date': data[2] if len(data) > 2 and data[2] else None,
                    'out_date': data[3] if len(data) > 3 and data[3] else None,
                    'stock_type': data[4] if len(data) > 4 else '',
                    'status_code': data[5] if len(data) > 5 else ''
                }
            
            logger.info(f"✅ 批量获取到 {len(stock_info)} 只股票详细信息")
            return stock_info
            
        except Exception as e:
            logger.error(f"❌ 批量获取股票信息异常: {e}")
            return stock_info
    
    def detect_st_status(self, name: str) -> Dict:
        """
        根据股票名称检测 ST/*ST 状态
        
        Args:
            name: 股票名称
            
        Returns:
            {'is_st': 0/1, 'is_star_st': 0/1, 'status': 1/2/3}
        """
        if not name:
            return {'is_st': 0, 'is_star_st': 0, 'status': 1}
        
        # 检测 *ST (退市风险警示)
        if name.startswith('*ST') or name.startswith('＊ST'):
            return {'is_st': 1, 'is_star_st': 1, 'status': 3}
        
        # 检测 ST (其他风险警示)
        if name.startswith('ST'):
            return {'is_st': 1, 'is_star_st': 0, 'status': 2}
        
        return {'is_st': 0, 'is_star_st': 0, 'status': 1}
    
    def detect_delisted_status(self, out_date: str) -> Dict:
        """
        根据退市日期检测是否已退市
        
        Args:
            out_date: 退市日期 (YYYY-MM-DD) 或空
            
        Returns:
            {'is_delisted': 0/1, 'status': 1/4}
        """
        if out_date and out_date != '' and out_date < self.today:
            return {'is_delisted': 1, 'status': 4}
        return {'is_delisted': 0, 'status': 1}
    
    def detect_etf(self, code: str) -> int:
        """
        检测是否ETF/基金（5开头）
        
        Args:
            code: 股票代码
            
        Returns:
            0/1
        """
        return 1 if code.startswith('5') else 0
    
    def detect_new_stock(self, list_date: str) -> int:
        """
        检测是否新股（上市不满1年）
        
        Args:
            list_date: 上市日期 (YYYY-MM-DD)
            
        Returns:
            0/1
        """
        if not list_date or list_date == '':
            return 0
        
        try:
            list_dt = datetime.strptime(list_date, '%Y-%m-%d')
            one_year_ago = datetime.now() - timedelta(days=365)
            return 1 if list_dt > one_year_ago else 0
        except:
            return 0
    
    def save_stock_basic(self, stock: Dict) -> bool:
        """
        保存单只股票基础信息到数据库
        
        Args:
            stock: {'code': ..., 'name': ..., 'market': ..., ...}
            
        Returns:
            bool: 是否成功
        """
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO stock_basic 
                    (code, name, market, industry, list_date, status, 
                     is_st, is_star_st, is_delisted, is_new_stock, is_etf, risk_level)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    market = VALUES(market),
                    industry = VALUES(industry),
                    list_date = VALUES(list_date),
                    status = VALUES(status),
                    is_st = VALUES(is_st),
                    is_star_st = VALUES(is_star_st),
                    is_delisted = VALUES(is_delisted),
                    is_new_stock = VALUES(is_new_stock),
                    is_etf = VALUES(is_etf),
                    risk_level = VALUES(risk_level),
                    updated_at = NOW()
                '''
                
                # 风险等级计算
                risk_level = 3 if stock.get('is_star_st') else (2 if stock.get('is_st') else 1)
                
                cursor.execute(sql, (
                    stock['code'],
                    stock.get('name', ''),
                    stock.get('market', ''),
                    stock.get('industry', ''),
                    stock.get('list_date'),
                    stock.get('status', 1),
                    stock.get('is_st', 0),
                    stock.get('is_star_st', 0),
                    stock.get('is_delisted', 0),
                    stock.get('is_new_stock', 0),
                    stock.get('is_etf', 0),
                    risk_level
                ))
                
                self.conn.commit()
                return True
                
        except Exception as e:
            logger.warning(f"❌ 保存 {stock['code']} 失败: {e}")
            return False
    
    def log_update(self, update_type: str, total: int, success: int, failed: int, 
                   message: str = '', status: str = 'success'):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO update_log 
                    (update_type, start_time, end_time, total_stocks, success_count, 
                     fail_count, status, message)
                    VALUES (%s, NOW(), NOW(), %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (update_type, total, success, failed, status, message))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"记录日志失败: {e}")
    
    def clear_existing_data(self):
        """清空现有数据（用于强制全量更新）"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE stock_basic")
                self.conn.commit()
                logger.info("✅ 已清空 stock_basic 表")
        except Exception as e:
            logger.error(f"❌ 清空数据失败: {e}")
    
    def get_db_stats(self) -> Dict:
        """获取数据库统计"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total,
                        SUM(is_st) as st_count,
                        SUM(is_star_st) as star_st_count,
                        SUM(is_delisted) as delisted_count,
                        SUM(is_new_stock) as new_count
                    FROM stock_basic
                ''')
                row = cursor.fetchone()
                return {
                    'total': row[0] or 0,
                    'st': row[1] or 0,
                    'star_st': row[2] or 0,
                    'delisted': row[3] or 0,
                    'new': row[4] or 0
                }
        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return {}
    
    def run(self, force_full: bool = False, batch_size: int = 100):
        """
        执行初始化
        
        Args:
            force_full: 是否强制全量更新
            batch_size: 每批处理数量
        """
        logger.info("=" * 60)
        logger.info("🚀 股票基础信息初始化")
        logger.info("=" * 60)
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 如果需要，清空现有数据
        if force_full:
            self.clear_existing_data()
        
        # 3. 登录 BaoStock
        if not self.connect_baostock():
            return False
        
        try:
            # 4. 获取所有股票列表
            stocks = self.get_all_stocks_from_baostock()
            if not stocks:
                logger.error("❌ 未获取到股票列表")
                return False
            
            logger.info(f"📝 开始处理 {len(stocks)} 只股票...")
            
            # 5. 批量获取所有股票详细信息（一次查询）
            logger.info("📥 批量获取股票详细信息...")
            all_stock_info = self.get_all_stock_basic_batch()
            
            # 6. 分批处理
            total = len(stocks)
            success = 0
            failed = 0
            
            for i, stock in enumerate(stocks):
                try:
                    code = stock['code']
                    
                    # 从批量获取的信息中提取
                    basic_info = all_stock_info.get(code)
                    
                    if basic_info:
                        stock['name'] = basic_info['name']
                        stock['list_date'] = basic_info['ipo_date']
                        
                        # 检测各种状态
                        st_status = self.detect_st_status(basic_info['name'])
                        delist_status = self.detect_delisted_status(basic_info['out_date'])
                        is_new = self.detect_new_stock(basic_info['ipo_date'])
                        is_etf = self.detect_etf(code)
                        
                        stock.update(st_status)
                        stock.update(delist_status)
                        stock['is_new_stock'] = is_new
                        stock['is_etf'] = is_etf
                    else:
                        # 无详细信息，使用默认值
                        stock['name'] = ''
                        stock['list_date'] = None
                        stock['is_st'] = 0
                        stock['is_star_st'] = 0
                        stock['is_delisted'] = 0
                        stock['is_new_stock'] = 0
                        stock['is_etf'] = self.detect_etf(code)
                        stock['status'] = 1
                    
                    # 保存到数据库
                    if self.save_stock_basic(stock):
                        success += 1
                    else:
                        failed += 1
                    
                    # 进度显示
                    if (i + 1) % batch_size == 0 or i == total - 1:
                        progress = (i + 1) / total * 100
                        logger.info(f"⏱️  进度: {progress:.1f}% | 成功:{success} | 失败:{failed}")
                    
                except Exception as e:
                    failed += 1
                    logger.debug(f"处理 {stock['code']} 失败: {e}")
            
            # 6. 显示统计
            stats = self.get_db_stats()
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成统计:")
            logger.info(f"   总股票数: {stats.get('total', 0)}")
            logger.info(f"   ST股: {stats.get('st', 0)}")
            logger.info(f"   *ST股: {stats.get('star_st', 0)}")
            logger.info(f"   退市股: {stats.get('delisted', 0)}")
            logger.info(f"   新股: {stats.get('new', 0)}")
            logger.info("=" * 60)
            
            # 7. 记录日志
            self.log_update('init', total, success, failed, 
                          f'Total:{total}, Success:{success}, Failed:{failed}')
            
            return True
            
        finally:
            self.logout_baostock()
            if self.conn:
                self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='股票基础信息初始化')
    parser.add_argument('--force', action='store_true', 
                       help='强制全量更新（清空后重建）')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='每批处理数量（默认100）')
    
    args = parser.parse_args()
    
    initializer = StockBasicInitializer()
    success = initializer.run(force_full=args.force, batch_size=args.batch_size)
    
    if success:
        logger.info("✅ 初始化完成!")
        sys.exit(0)
    else:
        logger.error("❌ 初始化失败!")
        sys.exit(1)


if __name__ == "__main__":
    main()
