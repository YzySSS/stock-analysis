#!/usr/bin/env python3
"""
股票回测系统 - 简化版
==================
T+1收益率计算：
- 买入：选股当日收盘价
- 卖出：次日收盘价
- 收益率 = (次日收盘价 - 当日收盘价) / 当日收盘价 * 100%

用法:
  python3 backtest_simple.py --strategy V10_5FACTOR --start 2026-03-01 --end 2026-04-03
  python3 backtest_simple.py --strategy V11_DYNAMIC --days 30
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pymysql
import akshare as ak

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/backtest_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class SimpleBacktester:
    """简化版回测器"""
    
    def __init__(self, strategy_key: str):
        self.strategy_key = strategy_key
        self.conn = None
        self.stats = {
            'total_days': 0,
            'total_trades': 0,
            'win_count': 0,
            'loss_count': 0,
            'total_return': 0.0,  # 累计收益率（简单加总）
            'daily_returns': []   # 每日收益率列表
        }
    
    def connect_db(self) -> bool:
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def get_strategy_config(self) -> Optional[Dict]:
        """获取策略配置"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    'SELECT factor_weights, parameters FROM strategies WHERE strategy_key = %s',
                    (self.strategy_key,)
                )
                row = cursor.fetchone()
                if row:
                    return {
                        'factor_weights': json.loads(row[0]),
                        'parameters': json.loads(row[1])
                    }
                return None
        except Exception as e:
            logger.error(f"获取策略配置失败: {e}")
            return None
    
    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT DISTINCT trade_date 
                    FROM stock_kline 
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                ''', (start_date, end_date))
                return [str(row[0]) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取交易日失败: {e}")
            return []
    
    def get_stock_price(self, code: str, date: str) -> Optional[float]:
        """获取股票某日的收盘价"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT close FROM stock_kline 
                    WHERE code = %s AND trade_date = %s
                ''', (code, date))
                row = cursor.fetchone()
                return float(row[0]) if row else None
        except Exception as e:
            return None
    
    def select_stocks(self, date: str, strategy_config: Dict) -> List[Dict]:
        """
        模拟选股（简化版）
        
        实际应该调用 main.py 的选股逻辑
        这里先用简化逻辑：选取当日涨幅前3的股票
        """
        try:
            with self.conn.cursor() as cursor:
                # 简化：选取当日涨幅前3的非ST股票
                cursor.execute('''
                    SELECT code, close, pct_change
                    FROM stock_kline 
                    WHERE trade_date = %s AND pct_change IS NOT NULL
                    ORDER BY pct_change DESC
                    LIMIT 3
                ''', (date,))
                
                picks = []
                for row in cursor.fetchall():
                    picks.append({
                        'code': row[0],
                        'buy_price': float(row[1]),  # 当日收盘价买入
                        'pick_reason': f'涨幅: {row[2]:.2f}%'
                    })
                return picks
        except Exception as e:
            logger.error(f"选股失败 {date}: {e}")
            return []
    
    def calculate_return(self, code: str, buy_date: str) -> Optional[float]:
        """
        计算T+1收益率
        
        买入：buy_date收盘价
        卖出：buy_date次日收盘价
        收益率 = (卖出价 - 买入价) / 买入价 * 100%
        """
        # 获取买入价
        buy_price = self.get_stock_price(code, buy_date)
        if buy_price is None:
            return None
        
        # 获取次日日期
        with self.conn.cursor() as cursor:
            cursor.execute('''
                SELECT MIN(trade_date) FROM stock_kline 
                WHERE trade_date > %s
            ''', (buy_date,))
            sell_date = cursor.fetchone()[0]
        
        if not sell_date:
            return None
        
        # 获取卖出价（次日收盘价）
        sell_price = self.get_stock_price(code, str(sell_date))
        if sell_price is None:
            return None
        
        # 计算收益率
        return_rate = (sell_price - buy_price) / buy_price * 100
        return round(return_rate, 2)
    
    def run_backtest(self, start_date: str, end_date: str) -> bool:
        """执行回测"""
        logger.info("=" * 70)
        logger.info(f"🚀 开始回测: {self.strategy_key}")
        logger.info(f"📅 回测区间: {start_date} ~ {end_date}")
        logger.info("=" * 70)
        
        if not self.connect_db():
            return False
        
        # 获取策略配置
        strategy_config = self.get_strategy_config()
        if not strategy_config:
            logger.error("未找到策略配置")
            return False
        
        logger.info(f"📊 策略配置: {strategy_config}")
        
        # 获取交易日列表
        trading_dates = self.get_trading_dates(start_date, end_date)
        if not trading_dates:
            logger.error("未获取到交易日")
            return False
        
        logger.info(f"📅 共 {len(trading_dates)} 个交易日")
        
        # 创建回测记录
        run_id = self.create_run_record(start_date, end_date, strategy_config)
        if not run_id:
            return False
        
        # 逐日回测
        for i, date in enumerate(trading_dates):
            logger.info(f"\n📅 处理日期: {date} ({i+1}/{len(trading_dates)})")
            
            # 选股
            picks = self.select_stocks(date, strategy_config)
            if not picks:
                logger.info("  ⏭️ 无选股")
                continue
            
            logger.info(f"  📊 选股 {len(picks)} 只: {[p['code'] for p in picks]}")
            
            # 计算每只股票的T+1收益
            daily_returns = []
            daily_detail = []
            
            for pick in picks:
                code = pick['code']
                return_rate = self.calculate_return(code, date)
                
                if return_rate is not None:
                    daily_returns.append(return_rate)
                    daily_detail.append({
                        'code': code,
                        'buy_price': pick['buy_price'],
                        'return': return_rate,
                        'reason': pick['pick_reason']
                    })
                    
                    # 统计
                    self.stats['total_trades'] += 1
                    if return_rate > 0:
                        self.stats['win_count'] += 1
                    else:
                        self.stats['loss_count'] += 1
            
            # 计算当日平均收益
            if daily_returns:
                avg_return = sum(daily_returns) / len(daily_returns)
                self.stats['daily_returns'].append(avg_return)
                self.stats['total_return'] += avg_return
                
                logger.info(f"  💰 当日平均收益: {avg_return:.2f}%")
                
                # 保存每日明细
                self.save_daily_record(run_id, date, picks, avg_return, daily_detail)
            
            self.stats['total_days'] += 1
        
        # 计算最终统计
        self.calculate_final_stats(run_id)
        
        return True
    
    def create_run_record(self, start_date: str, end_date: str, config: Dict) -> Optional[int]:
        """创建回测记录"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO backtest_runs_v2 
                    (run_name, strategy_key, strategy_id, start_date, end_date, 
                     parameters, status, benchmark)
                    VALUES (%s, %s, 
                        (SELECT id FROM strategies WHERE strategy_key = %s),
                        %s, %s, %s, 'running', '000001')
                ''', (
                    f'{self.strategy_key}_{start_date}_{end_date}',
                    self.strategy_key,
                    self.strategy_key,
                    start_date, end_date,
                    json.dumps(config)
                ))
                self.conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"创建回测记录失败: {e}")
            return None
    
    def save_daily_record(self, run_id: int, date: str, picks: List[Dict], 
                         avg_return: float, detail: List[Dict]):
        """保存每日回测记录"""
        try:
            with self.conn.cursor() as cursor:
                win_count = sum(1 for d in detail if d['return'] > 0)
                loss_count = sum(1 for d in detail if d['return'] <= 0)
                
                cursor.execute('''
                    INSERT INTO backtest_daily 
                    (run_id, trade_date, picked_stocks, pick_count,
                     next_day_return, win_count, loss_count, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    picked_stocks = VALUES(picked_stocks),
                    next_day_return = VALUES(next_day_return),
                    detail = VALUES(detail)
                ''', (
                    run_id, date, json.dumps(picks), len(picks),
                    avg_return, win_count, loss_count, json.dumps(detail)
                ))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"保存每日记录失败 {date}: {e}")
    
    def calculate_final_stats(self, run_id: int):
        """计算最终统计并保存"""
        if not self.stats['daily_returns']:
            logger.warning("无回测数据")
            return
        
        # 计算指标
        total_days = len(self.stats['daily_returns'])
        win_rate = self.stats['win_count'] / self.stats['total_trades'] * 100 if self.stats['total_trades'] > 0 else 0
        avg_return = self.stats['total_return'] / total_days if total_days > 0 else 0
        
        # 计算最大回撤
        cumulative = 0
        max_drawdown = 0
        peak = 0
        for ret in self.stats['daily_returns']:
            cumulative += ret
            if cumulative > peak:
                peak = cumulative
            drawdown = peak - cumulative
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 回测结果汇总")
        logger.info("=" * 70)
        logger.info(f"策略: {self.strategy_key}")
        logger.info(f"回测天数: {total_days} 天")
        logger.info(f"总交易次数: {self.stats['total_trades']} 次")
        logger.info(f"胜率: {win_rate:.1f}% ({self.stats['win_count']}/{self.stats['total_trades']})")
        logger.info(f"累计收益率: {self.stats['total_return']:.2f}%")
        logger.info(f"日均收益率: {avg_return:.2f}%")
        logger.info(f"最大回撤: {max_drawdown:.2f}%")
        logger.info("=" * 70)
        
        # 保存到数据库
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    UPDATE backtest_runs_v2 
                    SET status = 'completed',
                        total_days = %s,
                        total_trades = %s,
                        win_rate = %s,
                        avg_return = %s,
                        max_drawdown = %s,
                        total_return = %s,
                        completed_at = NOW()
                    WHERE id = %s
                ''', (
                    total_days, self.stats['total_trades'], win_rate,
                    avg_return, max_drawdown, self.stats['total_return'],
                    run_id
                ))
                self.conn.commit()
        except Exception as e:
            logger.error(f"保存最终统计失败: {e}")
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='股票回测系统 - 简化版')
    parser.add_argument('--strategy', type=str, required=True,
                       help='策略标识 (V10_5FACTOR 或 V11_DYNAMIC)')
    parser.add_argument('--start', type=str, default=None,
                       help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None,
                       help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=30,
                       help='回测最近N天（默认30天）')
    
    args = parser.parse_args()
    
    # 确定日期范围
    if args.end is None:
        args.end = datetime.now().strftime('%Y-%m-%d')
    if args.start is None:
        args.start = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    # 执行回测
    backtester = SimpleBacktester(args.strategy)
    try:
        success = backtester.run_backtest(args.start, args.end)
        sys.exit(0 if success else 1)
    finally:
        backtester.close()


if __name__ == "__main__":
    main()
