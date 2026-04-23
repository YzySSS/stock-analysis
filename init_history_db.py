#!/usr/bin/env python3
"""
股票历史数据库初始化脚本
========================
首次使用或需要重建数据库时运行

用法:
  python3 init_history_db.py           # 全量初始化（获取所有股票60天数据）
  python3 init_history_db.py --update  # 增量更新（只获取最新数据）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_sector_stocks() -> list:
    """加载板块成分股（约200只，而非全A股5000+只）"""
    # 核心板块成分股
    sector_map = {
        # 科技成长
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661'],
        '新能源': ['002594', '300750', '601012', '603659', '300014', '600438'],
        '光伏': ['601012', '600438', '002129', '300274', '688599'],
        '储能': ['300274', '002594', '300014', '300207', '688063'],
        '5G通信': ['000063', '600498', '300502', '002281', '300136'],
        '云计算': ['000938', '300017', '600845', '300454', '603881'],
        
        # 大消费
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589'],
        '医药': ['600276', '000538', '300760', '603259', '600436', '300122'],
        '医疗器械': ['300760', '603658', '300003', '688617'],
        '创新药': ['600276', '000661', '300122', '688180', '688235'],
        '食品饮料': ['000895', '600887', '603288', '600519', '000568'],
        '家电': ['000333', '000651', '600690', '002032'],
        '汽车': ['002594', '601633', '601127', '000625', '600660'],
        
        # 大金融
        '银行': ['000001', '600036', '601398', '601318', '601288'],
        '券商': ['600030', '300059', '601688', '000776', '601211'],
        '保险': ['601318', '601628', '601601'],
        
        # 周期资源
        '有色金属': ['601899', '002460', '600547', '603993', '000878'],
        '煤炭': ['601088', '601225', '600188', '601699'],
        '钢铁': ['600019', '000932', '600507'],
        '化工': ['002092', '600309', '601216', '600352'],
        '石油': ['601857', '600028', '601808'],
        
        # 基建地产
        '房地产': ['000002', '600048', '001979', '600606'],
        '建筑': ['601668', '601390', '601669', '601800'],
        '建材': ['000786', '600585', '002271', '600801'],
        
        # 其他
        '军工': ['600893', '000768', '600760', '600372'],
        '传媒': ['002027', '300413', '600637', '002555'],
        '电力': ['600900', '600011', '600795', '601985'],
        '交运': ['601006', '600009', '601111', '601919'],
    }
    
    # 去重合并所有板块股票
    all_stocks = set()
    for sector, codes in sector_map.items():
        all_stocks.update(codes)
    
    stocks = sorted(list(all_stocks))
    logger.info(f"✅ 加载板块成分股: {len(stocks)} 只（来自 {len(sector_map)} 个板块）")
    return stocks


def init_database(force_full: bool = False):
    """初始化数据库"""
    from stock_history_db import StockHistoryDB, StockHistorySync
    
    # 加载板块成分股（约200只）
    stocks = load_sector_stocks()
    
    # 查看当前数据库状态（单例模式）
    from stock_history_db import get_stock_history_db
    db = get_stock_history_db()
    stats = db.get_stats()
    logger.info(f"📊 当前数据库状态: {stats}")
    
    # 同步数据
    sync = StockHistorySync()
    sync.sync_all_stocks(stocks, force_full=force_full)
    
    # 查看更新后状态
    stats = db.get_stats()
    logger.info(f"📊 更新后数据库状态: {stats}")


def main():
    parser = argparse.ArgumentParser(description='股票历史数据库初始化')
    parser.add_argument('--full', action='store_true', 
                       help='强制全量更新（重新获取所有股票60天数据）')
    parser.add_argument('--update', action='store_true',
                       help='增量更新（只获取缺失的最新数据）')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("🚀 股票历史数据库初始化")
    logger.info("="*60)
    
    if args.full:
        logger.info("📥 模式: 全量更新")
        init_database(force_full=True)
    else:
        logger.info("📥 模式: 增量更新")
        init_database(force_full=False)
    
    logger.info("="*60)
    logger.info("✅ 初始化完成")
    logger.info("="*60)


if __name__ == "__main__":
    main()
