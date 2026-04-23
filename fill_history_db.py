#!/usr/bin/env python3
"""
全A股历史数据补全脚本
======================
使用 Baostock 获取所有股票的60天历史数据，存入本地数据库

用法:
  python3 fill_history_db.py           # 只补充缺失的股票
  python3 fill_history_db.py --full    # 重新获取所有股票（全量）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_all_stocks() -> List[str]:
    """加载全A股列表"""
    all_a_stocks_file = os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt")
    
    if os.path.exists(all_a_stocks_file):
        try:
            with open(all_a_stocks_file, 'r', encoding='utf-8') as f:
                stocks = [line.strip() for line in f if line.strip()]
            logger.info(f"✅ 从文件加载全A股列表: {len(stocks)} 只")
            return stocks
        except Exception as e:
            logger.error(f"❌ 读取股票列表失败: {e}")
    
    # 默认核心股票
    logger.warning("⚠️ 使用默认核心股票")
    return ['000001', '000002', '000858', '002594', '300750', '600519', '601318', '601398']


def get_existing_stocks(db_path: str) -> set:
    """获取数据库中已存在的股票代码"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute('SELECT DISTINCT code FROM stock_prices')
            return set(row[0] for row in cursor.fetchall())
    except:
        return set()


def fetch_stock_history_baostock(code: str, days: int = 60, bs=None) -> List[Dict]:
    """
    使用Baostock获取单只股票历史数据
    
    Args:
        code: 股票代码
        days: 获取天数
        bs: baostock模块（已登录）
    
    Returns:
        [{'date': '2024-01-01', 'close': 10.5, 'volume': 10000}, ...]
    """
    try:
        # 转换代码格式（支持股票和ETF）
        # ETF: 15/16/18开头为深圳，50/51开头为上海
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
            adjustflag='2'  # 前复权
        )
        
        if rs.error_code != '0':
            logger.warning(f"Baostock查询失败 {code}: {rs.error_msg}")
            return []
        
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
        
        return prices_data[-days:] if len(prices_data) > days else prices_data
        
    except Exception as e:
        logger.warning(f"❌ 获取 {code} 历史数据失败: {e}")
        return []


def batch_update_database(db_path: str, all_stocks: List[str], batch_size: int = 100, force_full: bool = False):
    """
    批量更新数据库（每批次独立登录，避免会话过期）
    
    Args:
        db_path: 数据库路径
        all_stocks: 所有股票代码列表
        batch_size: 每批处理数量
        force_full: 是否强制全量更新
    """
    import baostock as bs
    
    # 确定需要更新的股票
    if force_full:
        stocks_to_update = all_stocks
        logger.info(f"🔄 强制全量更新: {len(stocks_to_update)} 只股票")
    else:
        existing = get_existing_stocks(db_path)
        stocks_to_update = [s for s in all_stocks if s not in existing]
        logger.info(f"🔄 增量更新: 需要补充 {len(stocks_to_update)} 只股票")
    
    if not stocks_to_update:
        logger.info("✅ 所有股票数据已是最新，无需更新")
        return
    
    # 分批处理
    total_updated = 0
    total_failed = 0
    start_time = time.time()
    
    for i in range(0, len(stocks_to_update), batch_size):
        batch = stocks_to_update[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(stocks_to_update)-1)//batch_size + 1
        
        logger.info(f"\n📊 处理批次 {batch_num}/{total_batches}: {len(batch)} 只股票")
        
        # 每批次重新登录Baostock（避免会话过期）
        result = bs.login()
        if result.error_code != '0':
            logger.error(f"❌ Baostock登录失败: {result.error_msg}")
            continue
        
        batch_updated = 0
        batch_failed = 0
        
        for code in batch:
            try:
                prices_data = fetch_stock_history_baostock(code, days=60, bs=bs)
                
                if prices_data and len(prices_data) >= 20:  # 至少20天数据
                    # 保存到数据库
                    with sqlite3.connect(db_path) as conn:
                        for data in prices_data:
                            conn.execute('''
                                INSERT OR REPLACE INTO stock_prices (code, date, close_price, volume)
                                VALUES (?, ?, ?, ?)
                            ''', (code, data['date'], data['close'], data['volume']))
                        
                        # 更新最后更新日期
                        last_date = max(p['date'] for p in prices_data)
                        conn.execute('''
                            INSERT OR REPLACE INTO last_update (code, last_date)
                            VALUES (?, ?)
                        ''', (code, last_date))
                        
                        conn.commit()
                    
                    batch_updated += 1
                    logger.debug(f"✅ {code}: 保存 {len(prices_data)} 条记录")
                else:
                    batch_failed += 1
                    logger.debug(f"⚠️ {code}: 数据不足 ({len(prices_data) if prices_data else 0} 条)")
                
                # 短暂延迟，避免请求过快
                time.sleep(0.02)
                
            except Exception as e:
                batch_failed += 1
                logger.warning(f"❌ {code}: {str(e)[:50]}")
        
        # 批次结束，登出Baostock
        bs.logout()
        
        # 累加统计
        total_updated += batch_updated
        total_failed += batch_failed
        
        # 每批次后显示进度
        elapsed = time.time() - start_time
        progress = min(100, (i + len(batch)) / len(stocks_to_update) * 100)
        logger.info(f"⏱️  进度: {progress:.1f}% | 本批次: 成功{batch_updated}/失败{batch_failed} | 总计: 成功{total_updated}/失败{total_failed} | 用时: {elapsed:.1f}s")
    
    # 显示最终统计
    total_time = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ 更新完成!")
    logger.info(f"📊 成功: {total_updated} 只")
    logger.info(f"📊 失败: {total_failed} 只")
    logger.info(f"⏱️  总用时: {total_time:.1f} 秒")
    logger.info(f"📈 平均: {total_time/max(total_updated,1):.2f} 秒/只")
    logger.info(f"{'='*60}")


def show_db_stats(db_path: str):
    """显示数据库统计"""
    try:
        with sqlite3.connect(db_path) as conn:
            # 总股票数
            cursor = conn.execute('SELECT COUNT(DISTINCT code) FROM stock_prices')
            stock_count = cursor.fetchone()[0]
            
            # 总记录数
            cursor = conn.execute('SELECT COUNT(*) FROM stock_prices')
            record_count = cursor.fetchone()[0]
            
            # 最早/最晚日期
            cursor = conn.execute('SELECT MIN(date), MAX(date) FROM stock_prices')
            min_date, max_date = cursor.fetchone()
            
            logger.info(f"📊 数据库统计:")
            logger.info(f"   股票数: {stock_count} 只")
            logger.info(f"   记录数: {record_count} 条")
            logger.info(f"   日期范围: {min_date} ~ {max_date}")
            
    except Exception as e:
        logger.error(f"❌ 获取统计失败: {e}")


def main():
    parser = argparse.ArgumentParser(description='全A股历史数据补全')
    parser.add_argument('--full', action='store_true', 
                       help='强制全量更新（重新获取所有股票）')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='每批处理的股票数量（默认100）')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("🚀 全A股历史数据补全")
    logger.info("="*60)
    
    # 数据库路径
    db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'stock_history.db')
    
    # 显示更新前统计
    logger.info("\n📊 更新前数据库状态:")
    show_db_stats(db_path)
    
    # 加载全A股列表
    all_stocks = load_all_stocks()
    
    # 执行更新
    logger.info(f"\n🔄 开始更新...")
    batch_update_database(db_path, all_stocks, batch_size=args.batch_size, force_full=args.full)
    
    # 显示更新后统计
    logger.info("\n📊 更新后数据库状态:")
    show_db_stats(db_path)
    
    logger.info("\n" + "="*60)
    logger.info("✅ 全部完成!")
    logger.info("="*60)


if __name__ == "__main__":
    main()
