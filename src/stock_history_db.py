#!/usr/bin/env python3
"""
股票历史数据数据库管理器
============================
使用 SQLite 本地存储历史数据，减少API调用

表结构:
- stock_prices: 存储每日收盘价
- last_update: 记录每只股票最后更新日期

使用方式:
1. 首次运行：从API获取60天数据存入数据库
2. 每日更新：只获取昨天数据追加到数据库
3. 盘前选股：从数据库读取60天数据（无需API调用）
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# 单例模式：全局共享的数据库实例
_db_instance = None

def get_stock_history_db(db_path: str = None):
    """获取历史数据库单例实例"""
    global _db_instance
    if _db_instance is None:
        _db_instance = StockHistoryDB(db_path)
    return _db_instance

class StockHistoryDB:
    """股票历史数据数据库管理器（单例模式）"""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(os.path.dirname(__file__), 'data_cache', 'stock_history.db')
        
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self._init_db()
        logger.debug(f"历史数据库初始化: {db_path}")
    
    def _init_db(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 股票历史价格表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_prices (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    close_price REAL NOT NULL,
                    volume INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (code, date)
                )
            ''')
            
            # 创建索引加速查询
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_code_date 
                ON stock_prices(code, date)
            ''')
            
            # 最后更新记录表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS last_update (
                    code TEXT PRIMARY KEY,
                    last_date TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    def get_prices(self, code: str, days: int = 60) -> List[float]:
        """
        获取股票历史收盘价（从数据库）
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            收盘价列表（oldest first）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT close_price FROM stock_prices 
                WHERE code = ? 
                ORDER BY date DESC 
                LIMIT ?
            ''', (code, days))
            
            prices = [row[0] for row in cursor.fetchall()]
            # 返回 oldest first
            return prices[::-1]
    
    def save_prices(self, code: str, prices_data: List[Dict]):
        """
        批量保存股票历史数据
        
        Args:
            code: 股票代码
            prices_data: [{'date': '2024-01-01', 'close': 10.5, 'volume': 10000}, ...]
        """
        if not prices_data:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            for data in prices_data:
                conn.execute('''
                    INSERT OR REPLACE INTO stock_prices (code, date, close_price, volume)
                    VALUES (?, ?, ?, ?)
                ''', (
                    code, 
                    data['date'], 
                    data.get('close', 0),
                    data.get('volume', 0)
                ))
            
            # 更新最后更新日期
            last_date = max(p['date'] for p in prices_data)
            conn.execute('''
                INSERT OR REPLACE INTO last_update (code, last_date)
                VALUES (?, ?)
            ''', (code, last_date))
            
            conn.commit()
        
        logger.debug(f"✅ 保存 {code} 历史数据: {len(prices_data)} 条")
    
    def get_last_update_date(self, code: str) -> Optional[str]:
        """获取股票最后更新日期"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT last_date FROM last_update WHERE code = ?
            ''', (code,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_stocks_need_update(self, all_codes: List[str]) -> List[str]:
        """
        获取需要更新的股票列表
        
        需要更新的情况：
        1. 数据库中没有该股票
        2. 最后更新日期不是昨天/今天
        
        Returns:
            需要更新的股票代码列表
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        
        need_update = []
        
        with sqlite3.connect(self.db_path) as conn:
            for code in all_codes:
                cursor = conn.execute('''
                    SELECT last_date FROM last_update WHERE code = ?
                ''', (code,))
                row = cursor.fetchone()
                
                if not row:
                    # 数据库中没有该股票
                    need_update.append(code)
                elif row[0] not in [yesterday, today]:
                    # 数据不是最新的
                    need_update.append(code)
        
        return need_update
    
    def get_stats(self) -> Dict:
        """获取数据库统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            # 总股票数
            cursor = conn.execute('SELECT COUNT(DISTINCT code) FROM stock_prices')
            stock_count = cursor.fetchone()[0]
            
            # 总记录数
            cursor = conn.execute('SELECT COUNT(*) FROM stock_prices')
            record_count = cursor.fetchone()[0]
            
            # 最早/最晚日期
            cursor = conn.execute('SELECT MIN(date), MAX(date) FROM stock_prices')
            min_date, max_date = cursor.fetchone()
            
            return {
                'stock_count': stock_count,
                'record_count': record_count,
                'date_range': f"{min_date} ~ {max_date}" if min_date else "无数据"
            }
    
    def cleanup_old_data(self, keep_days: int = 90):
        """清理过期数据（保留最近N天）"""
        cutoff_date = (datetime.now() - timedelta(days=keep_days)).strftime('%Y-%m-%d')
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                DELETE FROM stock_prices WHERE date < ?
            ''', (cutoff_date,))
            deleted = cursor.rowcount
            conn.commit()
        
        logger.info(f"🧹 清理过期数据: 删除 {deleted} 条记录（{cutoff_date}之前）")
        return deleted


# ============================================================================
# 数据同步工具
# ============================================================================

class StockHistorySync:
    """股票历史数据同步工具"""
    
    def __init__(self):
        self.db = get_stock_history_db()
    
    def sync_all_stocks(self, stock_codes: List[str], force_full: bool = False):
        """
        同步所有股票的历史数据
        
        Args:
            stock_codes: 股票代码列表
            force_full: 是否强制全量更新（首次使用）
        """
        if force_full:
            need_update = stock_codes
            logger.info(f"🔄 强制全量更新: {len(need_update)} 只股票")
        else:
            need_update = self.db.get_stocks_need_update(stock_codes)
            logger.info(f"🔄 需要更新: {len(need_update)}/{len(stock_codes)} 只股票")
        
        if not need_update:
            logger.info("✅ 所有股票数据已是最新，无需更新")
            return
        
        # 分批获取，避免API限制
        batch_size = 100
        total_updated = 0
        
        for i in range(0, len(need_update), batch_size):
            batch = need_update[i:i+batch_size]
            logger.info(f"📊 处理批次 {i//batch_size + 1}/{(len(need_update)-1)//batch_size + 1}: {len(batch)} 只")
            
            for code in batch:
                try:
                    self._sync_single_stock(code)
                    total_updated += 1
                except Exception as e:
                    logger.warning(f"❌ 同步 {code} 失败: {e}")
            
            import time
            time.sleep(0.5)  # 避免请求过快
        
        logger.info(f"✅ 同步完成: 更新 {total_updated} 只股票")
    
    def _sync_single_stock(self, code: str):
        """同步单只股票的历史数据（使用Baostock）"""
        try:
            import baostock as bs
            
            # 登录Baostock
            result = bs.login()
            if result.error_code != '0':
                logger.warning(f"Baostock登录失败: {result.error_msg}")
                return
            
            # 转换代码格式
            if code.startswith(('00', '30')):
                bs_code = f"sz.{code}"
            else:
                bs_code = f"sh.{code}"
            
            # 获取60天数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=70)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,close,volume',
                start_date=start_date,
                end_date=end_date,
                frequency='d',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code == '0':
                prices_data = []
                while rs.next():
                    row_data = rs.get_row_data()
                    date_str = row_data[0]
                    close_price = row_data[1]
                    volume = row_data[2] if len(row_data) > 2 else '0'
                    
                    if close_price:
                        prices_data.append({
                            'date': date_str,
                            'close': float(close_price),
                            'volume': int(volume) if volume else 0
                        })
                
                if prices_data:
                    self.db.save_prices(code, prices_data)
                    logger.debug(f"✅ 保存 {code}: {len(prices_data)} 条记录")
            
            bs.logout()
            
        except Exception as e:
            logger.warning(f"❌ 同步 {code} 失败: {e}")


# ============================================================================
# 使用示例
# ============================================================================

if __name__ == "__main__":
    # 示例：初始化数据库并同步数据（单例模式）
    db = get_stock_history_db()
    
    # 查看统计
    stats = db.get_stats()
    print(f"数据库统计: {stats}")
    
    # 示例：获取某只股票历史数据
    prices = db.get_prices('000001', days=30)
    print(f"平安银行最近30天收盘价: {prices}")
