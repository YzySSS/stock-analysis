#!/usr/bin/env python3
"""
每日更新选股收盘价
定时任务：每天收盘后运行（15:35）
自动更新飞书多维表格中 "进行中" 状态的选股记录
支持多策略配置
"""
import os
import sys

# 添加 src 到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
from feishu_bitable_tracker import get_tracker, get_strategy_config

# 尝试导入 logger
try:
    from utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)


def get_close_price(code: str) -> float:
    """获取股票收盘价"""
    try:
        # 尝试使用 data_source
        try:
            from data_source import data_manager
            source = data_manager.get_source()
            
            if hasattr(source, 'get_realtime_quotes'):
                quotes = source.get_realtime_quotes([code])
                if code in quotes:
                    return quotes[code].get('price', 0)
        except Exception as e:
            logger.debug(f"data_source 获取失败: {e}")
        
        # 备选：使用 AkShare
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == code]
        if not row.empty:
            return float(row.iloc[0]['最新价'])
        
    except Exception as e:
        logger.error(f"获取 {code} 收盘价失败: {e}")
    
    return 0


def update_pending_records():
    """更新所有进行中的记录"""
    tracker = get_tracker()
    if not tracker.client:
        logger.error("飞书客户端未初始化，请检查环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return
    
    # 获取策略配置
    strategy_config = get_strategy_config()
    enabled_strategies = [s for s, info in strategy_config.items() if info["enabled"]]
    
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    
    logger.info("=" * 60)
    logger.info(f"开始更新收盘价 {today_str}")
    logger.info(f"启用的策略: {', '.join(enabled_strategies)}")
    logger.info("=" * 60)
    
    for strategy in enabled_strategies:
        logger.info(f"\n📊 检查策略: {strategy}")
        
        pending = tracker.get_pending_records(strategy)
        if not pending:
            logger.info(f"  没有进行中的记录")
            continue
        
        logger.info(f"  找到 {len(pending)} 条进行中记录")
        
        for record in pending:
            code = record['code']
            name = record['name']
            pick_date = record['pick_date']
            pick_price = record['pick_price']
            day1_close = record.get('day1_close')
            day2_close = record.get('day2_close')
            
            # 计算已过去的天数
            pick_dt = datetime.strptime(pick_date, '%Y-%m-%d')
            days_passed = (today - pick_dt).days
            
            logger.info(f"  📈 {name}({code}) 选股日:{pick_date} 已过{days_passed}天")
            
            # 获取今日收盘价
            close_price = get_close_price(code)
            if close_price <= 0:
                logger.warning(f"    ⚠️ 无法获取收盘价，跳过")
                continue
            
            # 根据已过天数更新对应字段
            if days_passed == 1 and day1_close is None:
                # 第1天收盘
                tracker.update_prices(strategy, pick_date, code, day1=close_price)
                change_pct = round((close_price - pick_price) / pick_price * 100, 2)
                logger.info(f"    ✅ 更新第1天收盘价: {close_price} (涨幅:{change_pct}%)")
                
            elif days_passed == 2 and day2_close is None:
                # 第2天收盘
                tracker.update_prices(strategy, pick_date, code, day2=close_price)
                change_pct = round((close_price - pick_price) / pick_price * 100, 2)
                logger.info(f"    ✅ 更新第2天收盘价: {close_price} (涨幅:{change_pct}%)")
                
            elif days_passed >= 3:
                # 第3天收盘，完成后标记
                tracker.update_prices(strategy, pick_date, code, day3=close_price)
                change_pct = round((close_price - pick_price) / pick_price * 100, 2)
                logger.info(f"    ✅ 更新第3天收盘价: {close_price} (涨幅:{change_pct}%) - 已完成")
    
    logger.info("\n" + "=" * 60)
    logger.info("更新完成！")
    logger.info("=" * 60)


if __name__ == '__main__':
    update_pending_records()
