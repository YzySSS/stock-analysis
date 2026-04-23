#!/usr/bin/env python3
"""
股票回测系统 - 真实策略版
========================
支持V10_5FACTOR和V11_DYNAMIC策略

收益率计算口径:
- 一日收益率: 当日开盘价买入 → 次日开盘价卖出
- 三日收益率: 当日开盘价买入 → 第3日收盘价卖出

用法:
  python3 backtest.py --strategy V10_5FACTOR --days 30
  python3 backtest.py --strategy V11_DYNAMIC --start 2026-03-01 --end 2026-04-03
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pymysql

# 添加src路径
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')
from strategy_factory import StrategyFactory

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


class Backtester:
    """回测器 - 支持分批止盈/止损"""
    
    def __init__(self, strategy_key: str):
        self.strategy_key = strategy_key
        # 使用策略工厂创建策略实例
        self.strategy = StrategyFactory.create(strategy_key)
        if not self.strategy:
            raise ValueError(f"无法创建策略: {strategy_key}")
        self.conn = None
        
        self.stats = {
            'total_days': 0,
            'total_trades': 0,
            'day1': {'returns': [], 'win': 0, 'loss': 0},
            'day3': {'returns': [], 'win': 0, 'loss': 0},
            'batch_exit': {'returns': [], 'win': 0, 'loss': 0, 'details': []}  # 新增分批止盈止损统计
        }
        
        # 分批止盈/止损配置 - V11优化版
        self.batch_exit_config = {
            # 修改1: 单一 -5% 硬性止损（废除-2.5%/-5%/-8%三档）
            'stop_loss': -5.0,  # 硬性止损线
            # 修改3: 回撤止盈从3%改为5%
            'drawback_stop': 5.0,  # 从最高点回撤5%触发全部卖出
            # 修改4: 时间止损 - 5日涨幅<2%强制平仓
            'time_stop_days': 5,  # 持有天数
            'time_stop_threshold': 2.0,  # 涨幅阈值
            # 保留止盈档位但降低权重（逐步废除）
            'profit_levels': [5.0, 10.0],  # 简化止盈档位
            'sell_ratios': [0.5, 1.0],  # 卖出比例
            'loss_levels': [-5.0],  # 单一止损档位
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
    
    def get_stock_price(self, code: str, date: str, price_type: str = 'close') -> Optional[float]:
        """
        获取股票价格
        
        Args:
            code: 股票代码
            date: 日期
            price_type: 'open'开盘价, 'close'收盘价
        """
        try:
            with self.conn.cursor() as cursor:
                column = 'open' if price_type == 'open' else 'close'
                cursor.execute(f"""
                    SELECT {column} FROM stock_kline
                    WHERE code = %s AND trade_date = %s
                """, (code, date))
                row = cursor.fetchone()
                return float(row[0]) if row and row[0] else None
        except Exception as e:
            return None
    
    def get_nth_trading_day(self, start_date: str, n: int) -> Optional[str]:
        """获取第N个交易日（N=1是次日，N=3是第3日）"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT trade_date FROM stock_kline 
                    WHERE trade_date > %s
                    ORDER BY trade_date
                    LIMIT %s, 1
                ''', (start_date, n-1))
                row = cursor.fetchone()
                return str(row[0]) if row else None
        except Exception as e:
            return None
    
    def get_price_series(self, code: str, start_date: str, days: int) -> List[Dict]:
        """获取股票连续多天的价格数据"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT trade_date, open, high, low, close
                    FROM stock_kline 
                    WHERE code = %s AND trade_date >= %s
                    ORDER BY trade_date
                    LIMIT %s
                ''', (code, start_date, days))
                rows = cursor.fetchall()
                return [
                    {
                        'date': str(row[0]),
                        'open': float(row[1]),
                        'high': float(row[2]),
                        'low': float(row[3]),
                        'close': float(row[4])
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.debug(f"获取价格序列失败 {code}: {e}")
            return []
    
    def calculate_batch_exit(self, pick: Dict, buy_date: str) -> Optional[Dict]:
        """
        计算分批止盈/止损收益率 - V11优化版
        
        优化后规则:
        - 硬性止损: -5%无条件全部卖出
        - 回撤止盈: 从最高点回撤5%全部卖出
        - 时间止损: 持有5日涨幅<2%强制平仓
        - 简化固定止盈: 5%卖50%, 10%全部卖出
        
        Returns:
            {
                'code', 'name', 'buy_price', 'final_return', 
                'exit_details': [...], 'exit_type': 'profit'/'loss'/'drawback'/'timestop'
            }
        """
        code = pick['code']
        name = pick.get('name', '')
        
        # 获取买入价
        buy_price = self.get_stock_price(code, buy_date, 'open')
        if not buy_price:
            return None
        
        # 获取后续10个交易日的数据（用于模拟持有期）
        price_series = self.get_price_series(code, buy_date, 15)
        if len(price_series) < 2:
            return None
        
        # 持仓状态
        position = {
            'hold_ratio': 1.0,  # 当前持仓比例
            'cost_price': buy_price,
            'max_price': buy_price,  # 最高价（用于回撤止盈）
            'triggers': set()  # 已触发的档位
        }
        
        exit_details = []
        exit_type = None
        
        # 遍历每个交易日（从第2天开始，第1天是买入日）
        day_count = 0
        for day_data in price_series[1:]:
            if position['hold_ratio'] <= 0:
                break
            
            day_count += 1
            
            current_price = day_data['high']  # 用最高价检查是否触发止盈
            current_low = day_data['low']  # 用最低价检查是否触发止损
            
            # 更新最高价
            if current_price > position['max_price']:
                position['max_price'] = current_price
            
            # 计算当前涨跌幅
            return_pct = (current_price - position['cost_price']) / position['cost_price'] * 100
            return_pct_low = (current_low - position['cost_price']) / position['cost_price'] * 100
            
            # 回撤止盈已禁用 - 导致亏损扩大
            # drawdown = (position['max_price'] - current_low) / position['max_price'] * 100
            # if drawdown >= self.batch_exit_config['drawback_stop'] and position['hold_ratio'] > 0:
            #     ...
            
            # 修改1: 单一 -5% 硬性止损（全部卖出）
            stop_loss = self.batch_exit_config['stop_loss']
            if return_pct_low <= stop_loss and position['hold_ratio'] > 0:
                sell_ratio = position['hold_ratio']
                position['hold_ratio'] = 0
                exit_type = 'loss'
                exit_details.append({
                    'date': day_data['date'],
                    'price': current_low,
                    'sell_ratio': sell_ratio,
                    'reason': f'硬性止损({stop_loss}%)',
                    'return_pct': return_pct_low
                })
                break
            
            # 检查简化止盈档位 - 仅在盈利时触发
            for i, level in enumerate(reversed(self.batch_exit_config['profit_levels'])):
                if return_pct >= level and f'profit_{level}' not in position['triggers']:
                    idx = len(self.batch_exit_config['profit_levels']) - 1 - i
                    sell_ratio = position['hold_ratio'] * self.batch_exit_config['sell_ratios'][idx]
                    position['hold_ratio'] -= sell_ratio
                    position['triggers'].add(f'profit_{level}')
                    exit_type = 'profit'
                    exit_details.append({
                        'date': day_data['date'],
                        'price': current_price,
                        'sell_ratio': sell_ratio,
                        'reason': f'止盈{level}%',
                        'return_pct': return_pct
                    })
                    if position['hold_ratio'] <= 0:
                        break
            
            # 修改4: 时间止损 - 持有5日涨幅<2%强制平仓
            time_stop_days = self.batch_exit_config.get('time_stop_days', 5)
            time_stop_threshold = self.batch_exit_config.get('time_stop_threshold', 2.0)
            if day_count >= time_stop_days and position['hold_ratio'] > 0:
                current_return = return_pct if return_pct is not None else 0
                if current_return < time_stop_threshold:
                    sell_ratio = position['hold_ratio']
                    position['hold_ratio'] = 0
                    exit_type = 'timestop'
                    exit_details.append({
                        'date': day_data['date'],
                        'price': current_low,
                        'sell_ratio': sell_ratio,
                        'reason': f'时间止损({day_count}日涨幅{current_return:.1f}%<{time_stop_threshold}%)',
                        'return_pct': current_return
                    })
                    break
        
        # 如果还有剩余仓位，按最后一天收盘价全部卖出
        if position['hold_ratio'] > 0 and price_series:
            last_day = price_series[-1]
            final_price = last_day['close']
            final_return = (final_price - buy_price) / buy_price * 100
            
            exit_details.append({
                'date': last_day['date'],
                'price': final_price,
                'sell_ratio': position['hold_ratio'],
                'reason': '到期卖出',
                'return_pct': final_return
            })
            position['hold_ratio'] = 0
        
        # 计算加权平均收益率
        if exit_details:
            total_weighted_return = sum(
                d['sell_ratio'] * d['return_pct'] for d in exit_details
            )
            avg_return = total_weighted_return  # 相对于初始仓位的收益率
        else:
            avg_return = 0
        
        return {
            'code': code,
            'name': name,
            'buy_price': buy_price,
            'final_return': round(avg_return, 2),
            'exit_type': exit_type or 'hold',
            'exit_details': exit_details
        }
    
    def calculate_returns(self, picks: List[Dict], buy_date: str) -> tuple:
        """
        计算收益率（一日、三日、分批止盈止损）
        
        Returns:
            (day1_returns, day3_returns, batch_returns)
        """
        day1_returns = []
        day3_returns = []
        batch_returns = []
        
        # 获取次日和第3日日期
        day1_sell_date = self.get_nth_trading_day(buy_date, 1)
        day3_sell_date = self.get_nth_trading_day(buy_date, 3)
        
        for pick in picks:
            code = pick['code']
            
            # 买入价：当日开盘价
            buy_price = self.get_stock_price(code, buy_date, 'open')
            if not buy_price:
                logger.debug(f"{code} {buy_date} 开盘价缺失，跳过")
                continue
            
            result = {
                'code': code,
                'name': pick.get('name', ''),
                'buy_price': buy_price
            }
            
            # 一日收益率：次日开盘价卖出
            if day1_sell_date:
                day1_sell_price = self.get_stock_price(code, day1_sell_date, 'open')
                if day1_sell_price:
                    day1_return = (day1_sell_price - buy_price) / buy_price * 100
                    day1_returns.append({
                        **result,
                        'sell_price': day1_sell_price,
                        'sell_date': day1_sell_date,
                        'return': round(day1_return, 2)
                    })
            
            # 三日收益率：第3日收盘价卖出
            if day3_sell_date:
                day3_sell_price = self.get_stock_price(code, day3_sell_date, 'close')
                if day3_sell_price:
                    day3_return = (day3_sell_price - buy_price) / buy_price * 100
                    day3_returns.append({
                        **result,
                        'sell_price': day3_sell_price,
                        'sell_date': day3_sell_date,
                        'return': round(day3_return, 2)
                    })
            
            # 分批止盈止损收益率
            batch_result = self.calculate_batch_exit(pick, buy_date)
            if batch_result:
                batch_returns.append(batch_result)
        
        return day1_returns, day3_returns, batch_returns
    
    def run_backtest(self, start_date: str, end_date: str):
        """执行回测"""
        logger.info("=" * 70)
        logger.info(f"🚀 开始回测: {self.strategy_key}")
        logger.info(f"📅 回测区间: {start_date} ~ {end_date}")
        logger.info("=" * 70)
        
        if not self.connect_db():
            return False
        
        # 获取交易日列表
        trading_dates = self.get_trading_dates(start_date, end_date)
        if not trading_dates:
            logger.error("未获取到交易日")
            return False
        
        logger.info(f"📅 共 {len(trading_dates)} 个交易日")
        
        # 创建回测记录
        run_id = self.create_run_record(start_date, end_date)
        if not run_id:
            return False
        
        # 逐日回测
        for i, date in enumerate(trading_dates):
            logger.info(f"\n📅 [{i+1}/{len(trading_dates)}] {date}")
            
            # 选股
            try:
                picks = self.strategy.select(date=date, top_n=3)
            except Exception as e:
                logger.error(f"选股失败 {date}: {e}")
                continue
            
            if not picks:
                logger.info("  ⏭️ 无选股")
                continue
            
            logger.info(f"  📊 选股: {[p['code'] for p in picks]}")
            
            # 计算收益率
            day1_returns, day3_returns, batch_returns = self.calculate_returns(picks, date)
            
            # 统计一日收益
            if day1_returns:
                day1_avg = sum(r['return'] for r in day1_returns) / len(day1_returns)
                day1_win = sum(1 for r in day1_returns if r['return'] > 0)
                day1_loss = len(day1_returns) - day1_win
                
                self.stats['day1']['returns'].append(day1_avg)
                self.stats['day1']['win'] += day1_win
                self.stats['day1']['loss'] += day1_loss
                
                logger.info(f"  💰 一日收益: {day1_avg:.2f}% ({day1_win}胜/{day1_loss}负)")
            
            # 统计三日收益
            if day3_returns:
                day3_avg = sum(r['return'] for r in day3_returns) / len(day3_returns)
                day3_win = sum(1 for r in day3_returns if r['return'] > 0)
                day3_loss = len(day3_returns) - day3_win
                
                self.stats['day3']['returns'].append(day3_avg)
                self.stats['day3']['win'] += day3_win
                self.stats['day3']['loss'] += day3_loss
                
                logger.info(f"  💰 三日收益: {day3_avg:.2f}% ({day3_win}胜/{day3_loss}负)")
            
            # 统计分批止盈止损收益
            if batch_returns:
                batch_avg = sum(r['final_return'] for r in batch_returns) / len(batch_returns)
                batch_win = sum(1 for r in batch_returns if r['final_return'] > 0)
                batch_loss = len(batch_returns) - batch_win
                
                self.stats['batch_exit']['returns'].append(batch_avg)
                self.stats['batch_exit']['win'] += batch_win
                self.stats['batch_exit']['loss'] += batch_loss
                self.stats['batch_exit']['details'].extend(batch_returns)
                
                logger.info(f"  📊 分批止盈止损: {batch_avg:.2f}% ({batch_win}胜/{batch_loss}负)")
                # 显示每只股票的退出详情
                for br in batch_returns:
                    exit_info = ', '.join([f"{d['reason']}({d['date']})" for d in br['exit_details'][:2]])
                    logger.info(f"    {br['code']}: {br['final_return']:.2f}% - {exit_info}")
            
            # 保存每日记录
            self.save_daily_record(run_id, date, picks, day1_returns, day3_returns, batch_returns)
            
            self.stats['total_days'] += 1
            self.stats['total_trades'] += len(picks)
        
        # 计算最终统计
        self.calculate_final_stats(run_id)
        
        return True
    
    def create_run_record(self, start_date: str, end_date: str) -> Optional[int]:
        """创建回测记录"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO backtest_runs_v2 
                    (run_name, strategy_key, strategy_id, start_date, end_date, status)
                    VALUES (%s, %s, 
                        (SELECT id FROM strategies WHERE strategy_key = %s),
                        %s, %s, 'running')
                ''', (
                    f'{self.strategy_key}_{start_date}_{end_date}',
                    self.strategy_key,
                    self.strategy_key,
                    start_date, end_date
                ))
                self.conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"创建回测记录失败: {e}")
            return None
    
    def save_daily_record(self, run_id: int, date: str, picks: List[Dict],
                         day1_returns: List[Dict], day3_returns: List[Dict], batch_returns: List[Dict] = None):
        """保存每日回测记录"""
        try:
            with self.conn.cursor() as cursor:
                day1_avg = sum(r['return'] for r in day1_returns) / len(day1_returns) if day1_returns else 0
                day3_avg = sum(r['return'] for r in day3_returns) / len(day3_returns) if day3_returns else 0
                batch_avg = sum(r['final_return'] for r in batch_returns) / len(batch_returns) if batch_returns else 0
                
                cursor.execute('''
                    INSERT INTO backtest_daily 
                    (run_id, trade_date, picked_stocks, pick_count,
                     day1_return, day1_detail, day1_win_count, day1_loss_count,
                     day3_return, day3_detail, day3_win_count, day3_loss_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    picked_stocks = VALUES(picked_stocks),
                    day1_return = VALUES(day1_return),
                    day1_detail = VALUES(day1_detail),
                    day3_return = VALUES(day3_return),
                    day3_detail = VALUES(day3_detail)
                ''', (
                    run_id, date, json.dumps(picks), len(picks),
                    day1_avg, json.dumps(day1_returns),
                    sum(1 for r in day1_returns if r['return'] > 0),
                    sum(1 for r in day1_returns if r['return'] <= 0),
                    day3_avg, json.dumps(day3_returns),
                    sum(1 for r in day3_returns if r['return'] > 0),
                    sum(1 for r in day3_returns if r['return'] <= 0)
                ))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"保存每日记录失败 {date}: {e}")
    
    def calculate_final_stats(self, run_id: int):
        """计算并输出最终统计"""
        logger.info("\n" + "=" * 70)
        logger.info("📊 回测结果汇总")
        logger.info("=" * 70)
        
        # 一日收益统计
        if self.stats['day1']['returns']:
            day1_total = sum(self.stats['day1']['returns'])
            day1_avg = day1_total / len(self.stats['day1']['returns'])
            day1_win_rate = self.stats['day1']['win'] / (self.stats['day1']['win'] + self.stats['day1']['loss']) * 100
            
            logger.info(f"\n【一日收益率】(当日开盘买入 → 次日开盘卖出)")
            logger.info(f"  回测天数: {len(self.stats['day1']['returns'])} 天")
            logger.info(f"  累计收益率: {day1_total:.2f}%")
            logger.info(f"  日均收益率: {day1_avg:.2f}%")
            logger.info(f"  胜率: {day1_win_rate:.1f}% ({self.stats['day1']['win']}胜/{self.stats['day1']['loss']}负)")
        
        # 三日收益统计
        if self.stats['day3']['returns']:
            day3_total = sum(self.stats['day3']['returns'])
            day3_avg = day3_total / len(self.stats['day3']['returns'])
            day3_win_rate = self.stats['day3']['win'] / (self.stats['day3']['win'] + self.stats['day3']['loss']) * 100
            
            logger.info(f"\n【三日收益率】(当日开盘买入 → 第3日收盘卖出)")
            logger.info(f"  回测天数: {len(self.stats['day3']['returns'])} 天")
            logger.info(f"  累计收益率: {day3_total:.2f}%")
            logger.info(f"  日均收益率: {day3_avg:.2f}%")
            logger.info(f"  胜率: {day3_win_rate:.1f}% ({self.stats['day3']['win']}胜/{self.stats['day3']['loss']}负)")
        
        # 分批止盈止损收益统计
        if self.stats['batch_exit']['returns']:
            batch_total = sum(self.stats['batch_exit']['returns'])
            batch_avg = batch_total / len(self.stats['batch_exit']['returns'])
            batch_win_rate = self.stats['batch_exit']['win'] / (self.stats['batch_exit']['win'] + self.stats['batch_exit']['loss']) * 100
            
            logger.info(f"\n【分批止盈止损收益率】(智能止盈止损)")
            logger.info(f"  止盈档位: +2.5% / +5% / +10%")
            logger.info(f"  止损档位: -2.5% / -5% / -8%")
            logger.info(f"  回撤止盈: 从最高点回撤3%全卖")
            logger.info(f"  回测天数: {len(self.stats['batch_exit']['returns'])} 天")
            logger.info(f"  累计收益率: {batch_total:.2f}%")
            logger.info(f"  日均收益率: {batch_avg:.2f}%")
            logger.info(f"  胜率: {batch_win_rate:.1f}% ({self.stats['batch_exit']['win']}胜/{self.stats['batch_exit']['loss']}负)")
            
            # 统计退出类型分布
            exit_types = {'profit': 0, 'loss': 0, 'drawback': 0, 'timestop': 0, 'hold': 0}
            for detail in self.stats['batch_exit']['details']:
                exit_types[detail.get('exit_type', 'hold')] += 1
            
            logger.info(f"\n  退出类型分布:")
            logger.info(f"    止盈退出: {exit_types['profit']} 只")
            logger.info(f"    止损退出: {exit_types['loss']} 只")
            logger.info(f"    回撤止盈: {exit_types['drawback']} 只")
            logger.info(f"    持有到期: {exit_types['hold']} 只")
        
        logger.info("=" * 70)
        
        # 保存到数据库
        try:
            with self.conn.cursor() as cursor:
                # 优先使用分批止盈止损的收益率，如果没有则使用一日收益率
                if self.stats['batch_exit']['returns']:
                    total_ret = sum(self.stats['batch_exit']['returns'])
                    avg_ret = total_ret / len(self.stats['batch_exit']['returns'])
                elif self.stats['day1']['returns']:
                    total_ret = sum(self.stats['day1']['returns'])
                    avg_ret = total_ret / len(self.stats['day1']['returns'])
                else:
                    total_ret = 0
                    avg_ret = 0
                
                cursor.execute('''
                    UPDATE backtest_runs_v2 
                    SET status = 'completed',
                        total_days = %s,
                        total_trades = %s,
                        total_return = %s,
                        avg_return = %s,
                        completed_at = NOW()
                    WHERE id = %s
                ''', (
                    self.stats['total_days'],
                    self.stats['total_trades'],
                    total_ret,
                    avg_ret,
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
    parser = argparse.ArgumentParser(description='股票回测系统')
    parser.add_argument('--strategy', type=str, required=True,
                       choices=['V10_5FACTOR', 'V11_DYNAMIC', 'V12'],
                       help='策略选择')
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
    backtester = Backtester(args.strategy)
    try:
        success = backtester.run_backtest(args.start, args.end)
        sys.exit(0 if success else 1)
    finally:
        backtester.close()


if __name__ == "__main__":
    main()
