#!/usr/bin/env python3
"""
舆情数据定时更新脚本
========================
定期更新热门股票的舆情数据到数据库

用法:
  python3 update_sentiment_data.py           # 更新一次
  python3 update_sentiment_data.py --all     # 更新全市场（较慢）
  
建议定时任务（每30分钟运行一次）:
  */30 * * * * cd /workspace/projects/workspace/股票分析项目 && python3 update_sentiment_data.py >> /tmp/sentiment_update.log 2>&1
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# 全A股列表文件
ALL_A_STOCKS_FILE = os.path.join(os.path.dirname(__file__), 'data', 'all_a_stocks.txt')

# 热门板块成分股（优先更新）
PRIORITY_STOCKS = [
    # 银行
    ('000001', '平安银行'), ('600036', '招商银行'), ('601398', '工商银行'),
    # 新能源
    ('002594', '比亚迪'), ('300750', '宁德时代'), ('601012', '隆基绿能'),
    # 白酒
    ('600519', '贵州茅台'), ('000858', '五粮液'), ('000568', '泸州老窖'),
    # 医药
    ('600276', '恒瑞医药'), ('603259', '药明康德'), ('300760', '迈瑞医疗'),
    # 科技
    ('000938', '中芯国际'), ('002230', '科大讯飞'), ('002415', '海康威视'),
    # 有色
    ('601899', '紫金矿业'), ('002460', '赣锋锂业'), ('603993', '洛阳钼业'),
    # 券商
    ('600030', '中信证券'), ('300059', '东方财富'), ('601688', '华泰证券'),
    # 其他蓝筹
    ('601318', '中国平安'), ('601888', '中国中免'), ('603288', '海天味业'),
    ('600887', '伊利股份'), ('000333', '美的集团'), ('000651', '格力电器'),
]

def load_all_stocks() -> list:
    """加载全A股列表"""
    if os.path.exists(ALL_A_STOCKS_FILE):
        try:
            with open(ALL_A_STOCKS_FILE, 'r', encoding='utf-8') as f:
                codes = [line.strip() for line in f if line.strip()]
            # 返回 (code, name) 格式，name用code代替
            return [(code, f"股票{code}") for code in codes]
        except Exception as e:
            logger.error(f"读取全A股列表失败: {e}")
    return []


def update_sentiment_data(batch_size: int = 50, update_all: bool = True, max_stocks: int = None):
    """
    更新舆情数据到数据库
    
    Args:
        batch_size: 每批处理数量
        update_all: 是否更新全市场
        max_stocks: 最大更新数量（None表示全部）
    """
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    today = datetime.now().strftime('%Y-%m-%d')
    
    if update_all:
        # 更新全市场
        stock_list = load_all_stocks()
        if not stock_list:
            logger.error("无法加载全A股列表，使用优先股票列表")
            stock_list = PRIORITY_STOCKS
        else:
            if max_stocks:
                stock_list = stock_list[:max_stocks]
            logger.info(f"全市场模式：更新{len(stock_list)}只股票舆情")
    else:
        # 仅更新优先股票
        stock_list = PRIORITY_STOCKS
        logger.info(f"优先模式：更新{len(stock_list)}只热门股票舆情")
    
    total = len(stock_list)
    updated = 0
    failed = 0
    skipped = 0
    start_time = time.time()
    
    logger.info(f"开始更新舆情数据: {today}, 总计{total}只")
    
    for i in range(0, total, batch_size):
        batch = stock_list[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total - 1) // batch_size + 1
        
        logger.info(f"批次 {batch_num}/{total_batches}: 处理 {len(batch)} 只股票")
        
        for code, name in batch:
            try:
                # 检查是否已存在今日数据
                existing = calc.get_cached_sentiment(code, today)
                if existing:
                    skipped += 1
                    logger.debug(f"{code} 今日数据已存在，跳过")
                    continue
                
                # 强制重新计算（不读缓存）
                result = calc.calculate_sentiment_factor(code, name, today)
                updated += 1
                logger.debug(f"{code} {name}: 得分={result['score']}, 新闻={result['news_count']}")
            except Exception as e:
                logger.warning(f"{code} {name} 更新失败: {e}")
                failed += 1
        
        # 批次间短暂休息，避免请求过快
        if i + batch_size < total:
            time.sleep(1)
        
        # 每10批次输出一次进度
        if batch_num % 10 == 0:
            elapsed = time.time() - start_time
            avg_speed = (i + len(batch)) / elapsed if elapsed > 0 else 0
            remaining = (total - i - len(batch)) / avg_speed if avg_speed > 0 else 0
            logger.info(f"进度: {i+len(batch)}/{total}, 速度: {avg_speed:.1f}只/秒, 预计剩余: {remaining/60:.1f}分钟")
    
    elapsed = time.time() - start_time
    logger.info(f"✅ 舆情数据更新完成: 成功{updated}, 跳过{skipped}, 失败{failed}, 耗时{elapsed:.1f}秒")


def main():
    parser = argparse.ArgumentParser(description='舆情数据定时更新')
    parser.add_argument('--all', action='store_true', help='更新全市场（默认）')
    parser.add_argument('--priority', action='store_true', help='仅更新热门股')
    parser.add_argument('--batch-size', type=int, default=50, help='每批处理数量')
    parser.add_argument('--max', type=int, default=None, help='最大更新数量')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("舆情数据定时更新 - 全A股模式")
    logger.info("="*60)
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 默认更新全市场
    update_all = not args.priority
    update_sentiment_data(batch_size=args.batch_size, update_all=update_all, max_stocks=args.max)
    
    logger.info("="*60)
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
