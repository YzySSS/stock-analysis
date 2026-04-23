#!/usr/bin/env python3
"""
每日历史数据增量更新脚本
========================
每天收盘后运行（15:35），获取当天收盘价添加到数据库

用法:
  python3 daily_update.py           # 更新所有股票当天的数据
  python3 daily_update.py --date 20240324  # 更新指定日期的数据
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
import time
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_all_stocks(db_path: str = None) -> List[str]:
    """加载全A股列表 + 数据库中已有的ETF"""
    stocks = []
    
    # 1. 从文件加载个股（尝试多个路径）
    possible_paths = [
        os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt"),
        os.path.join(os.path.dirname(__file__), 'data', 'all_a_stocks.txt'),
    ]
    
    all_a_stocks_file = None
    for path in possible_paths:
        if os.path.exists(path):
            all_a_stocks_file = path
            break
    
    if all_a_stocks_file:
        try:
            with open(all_a_stocks_file, 'r', encoding='utf-8') as f:
                stocks = [line.strip() for line in f if line.strip()]
            logger.info(f"✅ 从文件加载个股: {len(stocks)} 只 ({all_a_stocks_file})")
        except Exception as e:
            logger.error(f"❌ 读取股票列表失败: {e}")
    
    # 2. 从数据库加载ETF（15/51/56/58开头）
    if db_path and os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute('''
                    SELECT DISTINCT code FROM stock_prices 
                    WHERE code LIKE '15%' OR code LIKE '51%' OR code LIKE '56%' OR code LIKE '58%'
                ''')
                etf_codes = [row[0] for row in cursor.fetchall()]
                logger.info(f"✅ 从数据库加载ETF: {len(etf_codes)} 只")
                stocks.extend(etf_codes)
        except Exception as e:
            logger.warning(f"⚠️ 从数据库加载ETF失败: {e}")
    
    if not stocks:
        logger.warning("⚠️ 使用默认核心股票")
        return ['000001', '000002', '000858', '002594', '300750', '600519', '601318', '601398']
    
    # 去重
    stocks = list(set(stocks))
    logger.info(f"✅ 总计加载: {len(stocks)} 只股票（个股+ETF）")
    return stocks


def get_db_existing_dates(db_path: str) -> Dict[str, str]:
    """获取数据库中每只股票的最后更新日期"""
    existing_dates = {}
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute('SELECT code, MAX(date) FROM stock_prices GROUP BY code')
            for row in cursor.fetchall():
                existing_dates[row[0]] = row[1]
    except Exception as e:
        logger.warning(f"获取现有日期失败: {e}")
    return existing_dates


def fetch_yesterday_data_eastmoney(code: str, trade_date: str) -> Optional[Dict]:
    """
    使用东方财富获取单只股票/ETF指定日期的数据
    
    Args:
        code: 股票代码
        trade_date: 交易日期 (YYYY-MM-DD)
    
    Returns:
        {'date': '2024-03-24', 'close': 10.5, 'volume': 10000} 或 None
    """
    try:
        from eastmoney_datasource import get_stock_history
        
        # 获取历史数据（获取多一点天数以确保能拿到目标日期）
        history = get_stock_history(code, days=30)
        
        if not history:
            return None
        
        # 查找目标日期的数据
        for item in history:
            if item['date'] == trade_date:
                return {
                    'date': item['date'],
                    'close': float(item['close']),
                    'volume': int(item['volume']) if item.get('volume') else 0
                }
        
        # 如果没找到目标日期，返回最新的一条（假设是最新的交易日）
        latest = history[-1]
        return {
            'date': latest['date'],
            'close': float(latest['close']),
            'volume': int(latest['volume']) if latest.get('volume') else 0
        }
        
    except Exception as e:
        logger.debug(f"获取 {code} {trade_date} 数据失败: {e}")
        return None


def update_database(db_path: str, code: str, data: Dict):
    """更新单只股票数据到数据库"""
    try:
        with sqlite3.connect(db_path) as conn:
            # 插入价格数据
            conn.execute('''
                INSERT OR REPLACE INTO stock_prices (code, date, close_price, volume)
                VALUES (?, ?, ?, ?)
            ''', (code, data['date'], data['close'], data['volume']))
            
            # 更新最后更新日期
            conn.execute('''
                INSERT OR REPLACE INTO last_update (code, last_date)
                VALUES (?, ?)
            ''', (code, data['date']))
            
            conn.commit()
        return True
    except Exception as e:
        logger.warning(f"保存 {code} 数据失败: {e}")
        return False


def daily_update(db_path: str, all_stocks: List[str], target_date: str = None, batch_size: int = 200):
    """
    每日增量更新
    
    Args:
        db_path: 数据库路径
        all_stocks: 所有股票代码列表
        target_date: 目标日期 (YYYY-MM-DD)，默认今天（收盘后运行）
        batch_size: 每批处理数量
    """
    # 使用东方财富数据源（备用：如果可用akshare则使用akshare）
    
    # 确定目标日期 - 收盘后运行，默认获取当天收盘价
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')
        # 如果日期在未来（测试环境），使用数据库最新日期
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute('SELECT MAX(date) FROM stock_prices')
                db_max_date = cursor.fetchone()[0]
                if db_max_date and target_date > db_max_date:
                    logger.warning(f"⚠️ 目标日期 {target_date} 在未来，使用数据库最新日期 {db_max_date}")
                    target_date = db_max_date
        except Exception as e:
            logger.warning(f"检查数据库日期失败: {e}")
    
    logger.info(f"📅 更新日期: {target_date} (收盘后更新当天数据)")
    
    # 获取数据库中已有数据的股票及其最后日期
    existing_dates = get_db_existing_dates(db_path)
    logger.info(f"📊 数据库中已有 {len(existing_dates)} 只股票的历史数据")
    
    # 过滤出需要更新的股票（只更新已有数据的股票，保持数据连续性）
    stocks_to_update = [s for s in all_stocks if s in existing_dates]
    logger.info(f"🔄 需要更新: {len(stocks_to_update)} 只股票")
    
    if not stocks_to_update:
        logger.info("✅ 没有需要更新的股票")
        return
    
    # 分批处理
    total_updated = 0
    total_failed = 0
    total_skipped = 0  # 已是最新或该日期无数据
    start_time = time.time()
    
    for i in range(0, len(stocks_to_update), batch_size):
        batch = stocks_to_update[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(stocks_to_update)-1)//batch_size + 1
        
        logger.info(f"\n📊 处理批次 {batch_num}/{total_batches}: {len(batch)} 只股票")
        
        # 东方财富不需要登录
        
        batch_updated = 0
        batch_failed = 0
        batch_skipped = 0
        
        for code in batch:
            try:
                # 检查是否已更新到目标日期
                last_date = existing_dates.get(code)
                if last_date and last_date >= target_date:
                    batch_skipped += 1
                    logger.debug(f"⏭️ {code}: 已是最新 ({last_date})")
                    continue
                
                # 获取昨天数据
                data = fetch_yesterday_data_eastmoney(code, target_date)
                
                if data:
                    if update_database(db_path, code, data):
                        batch_updated += 1
                        # 更新内存中的日期记录
                        existing_dates[code] = target_date
                        logger.debug(f"✅ {code}: {data['close']}")
                    else:
                        batch_failed += 1
                else:
                    # 该日期无数据（可能是非交易日或股票停牌）
                    batch_skipped += 1
                    logger.debug(f"⏭️ {code}: 无数据")
                
                # 短暂延迟
                time.sleep(0.02)
                
            except Exception as e:
                batch_failed += 1
                logger.debug(f"❌ {code}: {str(e)[:50]}")
        
        # 东方财富不需要登出
        
        # 累加统计
        total_updated += batch_updated
        total_failed += batch_failed
        total_skipped += batch_skipped
        
        # 显示进度
        elapsed = time.time() - start_time
        progress = min(100, (i + len(batch)) / len(stocks_to_update) * 100)
        logger.info(f"⏱️  进度: {progress:.1f}% | "
                   f"更新{batch_updated}/跳过{batch_skipped}/失败{batch_failed} | "
                   f"总计: 更新{total_updated}/跳过{total_skipped}/失败{total_failed} | "
                   f"用时: {elapsed:.1f}s")
    
    # 显示最终统计
    total_time = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ 每日更新完成!")
    logger.info(f"📅 更新日期: {target_date}")
    logger.info(f"📊 成功更新: {total_updated} 只")
    logger.info(f"📊 跳过: {total_skipped} 只（已最新或无数据）")
    logger.info(f"📊 失败: {total_failed} 只")
    logger.info(f"⏱️  总用时: {total_time:.1f} 秒")
    logger.info(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='每日历史数据增量更新')
    parser.add_argument('--date', type=str, default=None,
                       help='指定更新日期 (格式: YYYY-MM-DD)，默认昨天')
    parser.add_argument('--batch-size', type=int, default=200,
                       help='每批处理的股票数量（默认200）')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("🚀 每日历史数据增量更新")
    logger.info("="*60)
    
    # 数据库路径
    db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'stock_history.db')
    
    # 加载全A股列表（个股+ETF）
    all_stocks = load_all_stocks(db_path)
    
    # 执行更新
    daily_update(db_path, all_stocks, target_date=args.date, batch_size=args.batch_size)
    
    # 显示更新后统计
    logger.info("\n📊 更新后数据库状态:")
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute('SELECT COUNT(DISTINCT code), COUNT(*), MAX(date) FROM stock_prices')
            stock_count, record_count, max_date = cursor.fetchone()
            logger.info(f"   股票数: {stock_count} 只")
            logger.info(f"   记录数: {record_count} 条")
            logger.info(f"   最新日期: {max_date}")
    except Exception as e:
        logger.error(f"获取统计失败: {e}")
    
    logger.info("\n" + "="*60)
    logger.info("✅ 全部完成!")
    logger.info("="*60)


if __name__ == "__main__":
    main()
