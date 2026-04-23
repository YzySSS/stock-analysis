#!/usr/bin/env python3
"""
盘前选股策略回测
================
时间范围: 2024-12-13 至 2025-01-13 (测试期)
最终范围: 2024-12-13 至 2025-12-13 (完整期)

回测逻辑:
1. 每天盘前获取历史数据（前60日）
2. 运行选股策略，选出TOP 3
3. 记录当天开盘价（作为买入价）
4. 记录当天收盘价（作为卖出价）
5. 计算每只股票的日收益率
6. 汇总统计

输出:
- backtest_result_YYYYMMDD.csv: 每日选股及盈亏明细
- backtest_summary.json: 汇总统计
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
import pandas as pd
import json
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class BacktestEngine:
    """回测引擎"""

    def __init__(self, start_date: str, end_date: str):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.results = []

        # 初始化数据源
        self._init_data_providers()

    def _init_data_providers(self):
        """初始化数据提供者"""
        try:
            import baostock as bs
            result = bs.login()
            if result.error_code == '0':
                self.baostock_available = True
                logger.info("✅ Baostock登录成功")
            else:
                self.baostock_available = False
        except:
            self.baostock_available = False

        try:
            import jqdatasdk as jq
            jq.auth('13929962527', 'Zy20001026')
            self.jq_available = True
            logger.info("✅ 聚宽登录成功")
        except:
            self.jq_available = False

    def get_stock_data(self, code: str, date: str) -> Dict:
        """
        获取某只股票在某天的数据
        返回: {'open': float, 'close': float, 'high': float, 'low': float, 'volume': float}
        """
        # 尝试Baostock
        if self.baostock_available:
            try:
                import baostock as bs
                if code.startswith(('00', '30')):
                    bs_code = f"sz.{code}"
                else:
                    bs_code = f"sh.{code}"

                rs = bs.query_history_k_data_plus(
                    bs_code,
                    'date,open,high,low,close,volume',
                    start_date=date,
                    end_date=date,
                    frequency='d',
                    adjustflag='2'
                )

                if rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    return {
                        'date': row[0],
                        'open': float(row[1]) if row[1] else 0,
                        'high': float(row[2]) if row[2] else 0,
                        'low': float(row[3]) if row[3] else 0,
                        'close': float(row[4]) if row[4] else 0,
                        'volume': float(row[5]) if row[5] else 0
                    }
            except Exception as e:
                logger.warning(f"Baostock获取失败 {code} {date}: {e}")

        # 尝试聚宽
        if self.jq_available:
            try:
                import jqdatasdk as jq
                if code.startswith(('00', '30', '39')):
                    jq_code = f"{code}.XSHE"
                else:
                    jq_code = f"{code}.XSHG"

                df = jq.get_price(jq_code, count=1, end_date=date, frequency='daily')
                if not df.empty:
                    return {
                        'date': date,
                        'open': float(df['open'].iloc[-1]),
                        'high': float(df['high'].iloc[-1]),
                        'low': float(df['low'].iloc[-1]),
                        'close': float(df['close'].iloc[-1]),
                        'volume': float(df['volume'].iloc[-1])
                    }
            except Exception as e:
                logger.warning(f"聚宽获取失败 {code} {date}: {e}")

        return None

    def get_history_for_screener(self, code: str, end_date: str, days: int = 60) -> List[float]:
        """获取选股需要的历史价格数据"""
        prices = []

        # 尝试Baostock
        if self.baostock_available:
            try:
                import baostock as bs
                if code.startswith(('00', '30')):
                    bs_code = f"sz.{code}"
                else:
                    bs_code = f"sh.{code}"

                start = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days*2)).strftime('%Y-%m-%d')

                rs = bs.query_history_k_data_plus(
                    bs_code, 'date,close', start_date=start, end_date=end_date,
                    frequency='d', adjustflag='2'
                )

                while rs.next():
                    close = rs.get_row_data()[1]
                    if close:
                        prices.append(float(close))

                if prices:
                    return prices[-days:]
            except:
                pass

        # 尝试聚宽
        if self.jq_available:
            try:
                import jqdatasdk as jq
                if code.startswith(('00', '30', '39')):
                    jq_code = f"{code}.XSHE"
                else:
                    jq_code = f"{code}.XSHG"

                df = jq.get_price(jq_code, count=days, end_date=end_date, frequency='daily')
                if not df.empty:
                    return df['close'].tolist()
            except:
                pass

        return prices

    def simulate_stock_picking(self, date: str, stock_pool: List[str]) -> List[Dict]:
        """
        模拟盘前选股
        简化版：基于前一天数据计算评分
        """
        picks = []

        # 获取前一天日期
        prev_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

        for code in stock_pool[:100]:  # 限制数量加快测试
            try:
                # 获取历史数据用于计算指标
                hist_prices = self.get_history_for_screener(code, prev_date, 60)
                if len(hist_prices) < 20:
                    continue

                # 获取当天数据（开盘价作为买入价）
                today_data = self.get_stock_data(code, date)
                if not today_data or today_data['open'] == 0:
                    continue

                # 简化评分计算
                current_price = today_data['open']
                prev_price = hist_prices[-1] if hist_prices else current_price
                change_pct = (current_price - prev_price) / prev_price * 100 if prev_price > 0 else 0

                # 计算技术指标
                ma20 = sum(hist_prices[-20:]) / 20 if len(hist_prices) >= 20 else current_price
                ma60 = sum(hist_prices[-60:]) / 60 if len(hist_prices) >= 60 else current_price

                # 突破判断
                high_20 = max(hist_prices[-20:]) if len(hist_prices) >= 20 else current_price
                is_break_high = current_price > high_20

                # 简化评分（0-100）
                score = 50

                # 技术分（0-20）
                if is_break_high:
                    score += 10
                if current_price > ma20 > ma60:
                    score += 8
                if change_pct > 2:
                    score += 2

                # 趋势分（0-30）
                if change_pct > 5:
                    score += 15
                elif change_pct > 3:
                    score += 10
                elif change_pct > 0:
                    score += 5

                # 风险分（0-15）
                if abs(change_pct) < 3:
                    score += 10
                elif abs(change_pct) < 5:
                    score += 5

                if score >= 65:  # 阈值
                    picks.append({
                        'code': code,
                        'score': score,
                        'buy_price': today_data['open'],
                        'sell_price': today_data['close'],
                        'change_pct': change_pct,
                        'is_break_high': is_break_high
                    })

            except Exception as e:
                continue

        # 排序选TOP 3
        picks.sort(key=lambda x: x['score'], reverse=True)
        return picks[:3]

    def run_backtest(self, stock_pool: List[str]):
        """运行回测"""
        logger.info(f"开始回测: {self.start_date.date()} 至 {self.end_date.date()}")

        current_date = self.start_date
        trade_days = 0

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')

            # 跳过周末
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            logger.info(f"处理日期: {date_str}")

            # 选股
            picks = self.simulate_stock_picking(date_str, stock_pool)

            if picks:
                trade_days += 1
                for i, pick in enumerate(picks):
                    # 计算收益率
                    buy_price = pick['buy_price']
                    sell_price = pick['sell_price']
                    return_pct = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0

                    self.results.append({
                        'date': date_str,
                        'rank': i + 1,
                        'code': pick['code'],
                        'score': pick['score'],
                        'buy_price': round(buy_price, 2),
                        'sell_price': round(sell_price, 2),
                        'return_pct': round(return_pct, 2),
                        'is_break_high': pick['is_break_high']
                    })

                logger.info(f"  选出 {len(picks)} 只，最佳收益率: {self.results[-1]['return_pct']:.2f}%")
            else:
                logger.info(f"  未选出股票")

            current_date += timedelta(days=1)

        logger.info(f"回测完成，共 {trade_days} 个交易日")

    def save_results(self):
        """保存回测结果"""
        if not self.results:
            logger.warning("没有回测结果")
            return

        # 保存明细CSV
        df = pd.DataFrame(self.results)
        output_file = f"backtest_result_{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"✅ 明细已保存: {output_file}")

        # 计算汇总统计
        summary = {
            'start_date': self.start_date.strftime('%Y-%m-%d'),
            'end_date': self.end_date.strftime('%Y-%m-%d'),
            'total_trades': len(self.results),
            'win_trades': len([r for r in self.results if r['return_pct'] > 0]),
            'loss_trades': len([r for r in self.results if r['return_pct'] <= 0]),
            'avg_return': round(sum(r['return_pct'] for r in self.results) / len(self.results), 2),
            'max_return': round(max(r['return_pct'] for r in self.results), 2),
            'min_return': round(min(r['return_pct'] for r in self.results), 2),
            'total_days': len(set(r['date'] for r in self.results))
        }

        # 保存汇总JSON
        summary_file = f"backtest_summary_{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ 汇总已保存: {summary_file}")

        # 打印汇总
        print("\n" + "="*60)
        print("📊 回测汇总")
        print("="*60)
        print(f"回测期间: {summary['start_date']} 至 {summary['end_date']}")
        print(f"交易天数: {summary['total_days']}")
        print(f"总交易次数: {summary['total_trades']}")
        print(f"盈利次数: {summary['win_trades']} ({summary['win_trades']/summary['total_trades']*100:.1f}%)")
        print(f"亏损次数: {summary['loss_trades']} ({summary['loss_trades']/summary['total_trades']*100:.1f}%)")
        print(f"平均收益率: {summary['avg_return']:.2f}%")
        print(f"最高收益: {summary['max_return']:.2f}%")
        print(f"最低收益: {summary['min_return']:.2f}%")
        print("="*60)


def main():
    # 测试期: 2024-12-13 至 2025-01-13
    start_date = '2024-12-13'
    end_date = '2025-01-13'

    # 股票池（从文件加载或手动指定）
    stock_pool = [
        '000001', '000002', '000858', '002594', '300750',  # 原默认股票
        '600519', '601318', '601398', '601888', '603288',
        '000938', '002230', '002415', '300033', '300418',  # 科技
        '002371', '300782', '603501', '688981', '688012',  # 芯片
        '601012', '603659', '300014',  # 新能源
    ]

    # 创建回测引擎
    engine = BacktestEngine(start_date, end_date)

    # 运行回测
    engine.run_backtest(stock_pool)

    # 保存结果
    engine.save_results()


if __name__ == '__main__':
    main()
