#!/usr/bin/env python3
"""
2025年完整回测 - 后台运行版
结果写入文件，无需等待输出
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import logging

# 设置日志写入文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.FileHandler('backtest_2025.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def calculate_score(open_price, prev_close):
    """简化评分计算"""
    change_pct = (open_price - prev_close) / prev_close * 100 if prev_close > 0 else 0
    
    score = 50
    if change_pct > 3: score += 15
    elif change_pct > 1: score += 8
    elif change_pct > 0: score += 3
    if abs(change_pct) < 3: score += 10
    elif abs(change_pct) < 5: score += 5
    
    return score


def run_backtest():
    """运行完整回测"""
    start_date = '2025-01-13'
    end_date = '2025-12-13'
    
    try:
        import jqdatasdk as jq
        jq.auth('13929962527', 'Zy20001026')
        logger.info("✅ 聚宽登录成功")
    except Exception as e:
        logger.error(f"聚宽登录失败: {e}")
        return
    
    stock_pool = [
        '000001.XSHE', '000002.XSHE', '000858.XSHE', '002594.XSHE', '300750.XSHE',
        '600519.XSHG', '601318.XSHG', '601398.XSHG', '601888.XSHG', '603288.XSHG',
        '000938.XSHE', '002230.XSHE', '002415.XSHE', '603501.XSHG', '688981.XSHG',
        '002371.XSHE', '300782.XSHE', '688012.XSHG', '601012.XSHG', '300014.XSHE',
        '600276.XSHG', '000538.XSHE', '300760.XSHE', '603259.XSHG', '600900.XSHG'
    ]
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    results = []
    all_scores = []
    trade_days = 0
    
    current = start
    
    logger.info(f"开始回测: {start_date} 至 {end_date}")
    logger.info(f"股票池: {len(stock_pool)} 只")
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        try:
            df_test = jq.get_price('000001.XSHE', count=1, end_date=date_str, frequency='daily')
            if df_test.empty:
                current += timedelta(days=1)
                continue
        except:
            current += timedelta(days=1)
            continue
        
        trade_days += 1
        
        try:
            df_day = jq.get_price(stock_pool, count=1, end_date=date_str, frequency='daily')
            prev_date = (current - timedelta(days=1)).strftime('%Y-%m-%d')
            df_prev = jq.get_price(stock_pool, count=1, end_date=prev_date, frequency='daily')
            
            day_picks = []
            
            for code in stock_pool:
                try:
                    if code not in df_day.index or code not in df_prev.index:
                        continue
                    
                    today = df_day.loc[code]
                    prev_close = df_prev.loc[code]['close']
                    open_price = today['open']
                    close_price = today['close']
                    
                    score = calculate_score(open_price, prev_close)
                    all_scores.append(score)
                    
                    if score >= 60:
                        day_return = (close_price - open_price) / open_price * 100 if open_price > 0 else 0
                        
                        day_picks.append({
                            'date': date_str,
                            'code': code.replace('.XSHE', '').replace('.XSHG', ''),
                            'score': round(score, 1),
                            'buy_price': round(open_price, 2),
                            'sell_price': round(close_price, 2),
                            'return_pct': round(day_return, 2)
                        })
                except:
                    continue
            
            day_picks.sort(key=lambda x: x['score'], reverse=True)
            top_picks = day_picks[:3]
            
            if top_picks:
                results.extend(top_picks)
                if trade_days % 20 == 0:
                    avg_ret = sum(p['return_pct'] for p in top_picks) / len(top_picks)
                    logger.info(f"{date_str}: 选出 {len(top_picks)} 只，平均收益 {avg_ret:.2f}%")
            
        except Exception as e:
            logger.warning(f"{date_str} 处理失败: {e}")
        
        current += timedelta(days=1)
    
    logger.info(f"回测完成，共 {trade_days} 个交易日，{len(results)} 笔交易")
    
    if results:
        df = pd.DataFrame(results)
        csv_file = f'backtest_2025_{start_date}_{end_date}.csv'
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        logger.info(f"✅ 明细已保存: {csv_file}")
        
        if all_scores:
            arr = np.array(all_scores)
            score_stats = {
                '样本数': len(all_scores),
                '平均': float(np.mean(arr)),
                '中位数': float(np.median(arr)),
                '标准差': float(np.std(arr)),
                '最低': float(np.min(arr)),
                '最高': float(np.max(arr)),
                '分位数': {str(p): float(np.percentile(arr, p)) for p in [60,70,75,80,85,90]}
            }
        else:
            score_stats = {}
        
        returns = [r['return_pct'] for r in results]
        wins = len([r for r in results if r['return_pct'] > 0])
        
        summary = {
            '回测期间': f'{start_date} 至 {end_date}',
            '交易日数': trade_days,
            '总选股次数': len(results),
            '胜率': f"{wins/len(results)*100:.1f}%",
            '平均收益率': f"{sum(returns)/len(returns):.2f}%",
            '累计收益率': f"{sum(returns):.2f}%",
            '评分统计': score_stats
        }
        
        json_file = f'backtest_2025_summary_{start_date}_{end_date}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 汇总已保存: {json_file}")
        
        with open('BACKTEST_DONE.txt', 'w') as f:
            f.write(f"回测完成\n时间: {datetime.now()}\n文件: {csv_file}, {json_file}")


if __name__ == '__main__':
    run_backtest()
