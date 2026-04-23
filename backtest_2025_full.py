#!/usr/bin/env python3
"""
2025年完整回测 + 平均评分统计 (全A股版)
==========================
时间范围: 2025-01-13 至 2025-12-13
目标: 
1. 运行盘前选股回测（全A股股票池）
2. 统计平均股票评分
3. 为阈值调整提供数据支持
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_indicators(prices: list) -> dict:
    """计算技术指标"""
    if len(prices) < 20:
        return {'ma20': 0, 'ma60': 0, 'rsi': 50, 'high_20': 0, 'is_break': False}
    
    current = prices[-1]
    ma20 = sum(prices[-20:]) / 20
    ma60 = sum(prices[-60:]) / 60 if len(prices) >= 60 else ma20
    high_20 = max(prices[-20:])
    
    # RSI简化计算
    gains = [prices[i] - prices[i-1] for i in range(1, len(prices)) if prices[i] > prices[i-1]]
    losses = [prices[i-1] - prices[i] for i in range(1, len(prices)) if prices[i] < prices[i-1]]
    avg_gain = sum(gains[-14:]) / 14 if gains else 0
    avg_loss = sum(losses[-14:]) / 14 if losses else 0
    rs = avg_gain / avg_loss if avg_loss > 0 else 1
    rsi = 100 - (100 / (1 + rs))
    
    return {
        'ma20': ma20,
        'ma60': ma60,
        'rsi': rsi,
        'high_20': high_20,
        'is_break': current > high_20
    }


def calculate_score(open_price: float, prev_close: float, hist_prices: list) -> float:
    """计算股票评分（完整版6因子）"""
    if not hist_prices or len(hist_prices) < 20:
        return 0
    
    change_pct = (open_price - prev_close) / prev_close * 100 if prev_close > 0 else 0
    indicators = calculate_indicators(hist_prices)
    
    score = 0
    
    # 1. 技术因子 (0-20)
    tech_score = 8
    if change_pct > 3:
        tech_score += 3
    elif change_pct > 1:
        tech_score += 2
    elif change_pct > 0:
        tech_score += 1
    
    if indicators['is_break']:
        tech_score += 5
    if open_price > indicators['ma20'] > indicators['ma60']:
        tech_score += 4
    if 50 <= indicators['rsi'] <= 80:
        tech_score += 3
    
    score += min(20, tech_score)
    
    # 2. 情绪因子 (0-10)
    sentiment_score = 5
    if change_pct > 5:
        sentiment_score += 2
    score += min(10, sentiment_score)
    
    # 3. 板块/趋势因子 (0-35)
    trend_score = 10
    if change_pct > 5:
        trend_score += 15
    elif change_pct > 3:
        trend_score += 10
    elif change_pct > 1:
        trend_score += 5
    score += min(35, trend_score)
    
    # 4. 资金流因子 (0-20)
    money_score = 6
    if change_pct > 0:
        money_score += 4
    score += min(20, money_score)
    
    # 5. 风险因子 (0-15)
    risk_score = 10
    if abs(change_pct) < 3:
        risk_score += 2
    elif abs(change_pct) < 5:
        risk_score += 0
    else:
        risk_score -= 3
    score += min(15, max(0, risk_score))
    
    return score


def get_all_a_stocks(jq, date_str):
    """获取全A股股票列表（排除ST、退市、新股、北交所）"""
    try:
        # 获取所有股票
        all_stocks = jq.get_all_securities(types=['stock'], date=date_str)
        
        # 过滤条件
        # 1. 排除北交所 (BJ)
        # 2. 排除ST
        # 3. 排除退市
        # 4. 排除上市不足60天的新股
        
        filtered = []
        for code, info in all_stocks.iterrows():
            # 排除北交所
            if code.startswith('8') or code.startswith('4'):
                continue
            # 排除ST
            if 'ST' in info['display_name'] or '*ST' in info['display_name']:
                continue
            # 排除退市
            if '退' in info['display_name']:
                continue
            # 排除新股（上市不足60天）
            start_date = info['start_date']
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            check_date = datetime.strptime(date_str, '%Y-%m-%d')
            if (check_date - start_date).days < 60:
                continue
            filtered.append(code)
        
        return filtered
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return []


def run_backtest_2025():
    """运行2025年完整回测（全A股）"""
    
    start_date = '2025-01-13'
    end_date = '2025-12-13'
    
    # 登录聚宽
    try:
        import jqdatasdk as jq
        jq.auth('13929962527', 'Zy20001026')
        logger.info("✅ 聚宽登录成功")
    except Exception as e:
        logger.error(f"聚宽登录失败: {e}")
        return
    
    # 获取初始股票池（基于开始日期）
    print("📊 正在获取全A股股票池...")
    stock_pool = get_all_a_stocks(jq, start_date)
    print(f"✅ 初始股票池: {len(stock_pool)} 只")
    
    # 限制股票池大小（聚宽API限制，分批处理）
    MAX_STOCKS_PER_BATCH = 100  # 每批最多处理100只
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    results = []
    all_scores = []  # 记录所有股票的评分用于统计
    trade_days = 0
    
    current = start
    
    print(f"\n🔄 回测期间: {start_date} 至 {end_date}")
    print(f"📊 股票池: 全A股（约{len(stock_pool)}只）")
    print(f"⚙️ 每批处理: {MAX_STOCKS_PER_BATCH} 只")
    print("="*70)
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        
        # 跳过周末
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        try:
            # 检查是否为交易日
            df_test = jq.get_price('000001.XSHE', count=1, end_date=date_str, frequency='daily')
            if df_test.empty:
                current += timedelta(days=1)
                continue
        except:
            current += timedelta(days=1)
            continue
        
        trade_days += 1
        if trade_days % 10 == 0:
            print(f"📅 已处理 {trade_days} 个交易日，当前: {date_str}")
        
        try:
            # 更新股票池（每日更新，排除新ST/退市/新股）
            if trade_days % 5 == 0:  # 每5天更新一次股票池
                stock_pool = get_all_a_stocks(jq, date_str)
            
            # 分批获取当天数据
            prev_date = (current - timedelta(days=1)).strftime('%Y-%m-%d')
            
            day_picks = []
            day_scores = []
            
            # 分批处理股票
            for i in range(0, len(stock_pool), MAX_STOCKS_PER_BATCH):
                batch = stock_pool[i:i + MAX_STOCKS_PER_BATCH]
                
                try:
                    # 获取当天数据
                    df_day = jq.get_price(batch, count=1, end_date=date_str, frequency='daily', 
                                          fields=['open', 'close', 'high', 'low', 'volume'])
                    
                    # 获取前1日数据
                    df_prev = jq.get_price(batch, count=1, end_date=prev_date, frequency='daily',
                                          fields=['close'])
                    
                    if df_day.empty or df_prev.empty:
                        continue
                    
                    # 获取前60日历史数据（用于计算指标）
                    for code in batch:
                        try:
                            if code not in df_day.index or code not in df_prev.index:
                                continue
                            
                            today = df_day.loc[code]
                            prev_close = df_prev.loc[code]['close']
                            open_price = today['open']
                            close_price = today['close']
                            
                            # 获取历史数据
                            df_hist = jq.get_price(code, count=60, end_date=prev_date, frequency='daily',
                                                   fields=['close'])
                            if df_hist.empty or len(df_hist) < 20:
                                continue
                            
                            hist_prices = df_hist['close'].tolist()
                            
                            # 计算评分
                            score = calculate_score(open_price, prev_close, hist_prices)
                            
                            day_scores.append(score)
                            
                            if score >= 65:  # 使用65分阈值进行回测
                                day_return = (close_price - open_price) / open_price * 100 if open_price > 0 else 0
                                
                                day_picks.append({
                                    'date': date_str,
                                    'code': code.replace('.XSHE', '').replace('.XSHG', ''),
                                    'score': round(score, 1),
                                    'open': round(open_price, 2),
                                    'close': round(close_price, 2),
                                    'return_pct': round(day_return, 2),
                                    'change_pct': round((open_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
                                })
                        except:
                            continue
                            
                except Exception as e:
                    logger.debug(f"批次处理失败: {e}")
                    continue
            
            # 记录当天所有评分
            if day_scores:
                all_scores.extend(day_scores)
            
            # 排序选TOP 3
            day_picks.sort(key=lambda x: x['score'], reverse=True)
            top_picks = day_picks[:3]
            
            if top_picks:
                results.extend(top_picks)
                if trade_days % 20 == 0:
                    avg_ret = sum(p['return_pct'] for p in top_picks) / len(top_picks)
                    print(f"   ✅ {date_str}: 选出 {len(top_picks)} 只，平均收益 {avg_ret:.2f}%")
            
        except Exception as e:
            logger.warning(f"{date_str} 处理失败: {e}")
        
        current += timedelta(days=1)
    
    # 保存结果
    print("\n" + "="*70)
    print("📊 生成回测报告...")
    
    if results:
        # 保存明细
        df_results = pd.DataFrame(results)
        csv_file = f'backtest_2025_{start_date}_{end_date}.csv'
        df_results.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        # 评分统计
        scores_array = np.array(all_scores)
        score_stats = {
            '总评分样本数': len(all_scores),
            '平均评分': round(float(np.mean(scores_array)), 2),
            '中位数评分': round(float(np.median(scores_array)), 2),
            '标准差': round(float(np.std(scores_array)), 2),
            '最高分': round(float(np.max(scores_array)), 2),
            '最低分': round(float(np.min(scores_array)), 2),
            '75分位数': round(float(np.percentile(scores_array, 75)), 2),
            '90分位数': round(float(np.percentile(scores_array, 90)), 2),
            '95分位数': round(float(np.percentile(scores_array, 95)), 2),
        }
        
        # 收益统计
        total = len(results)
        wins = len([r for r in results if r['return_pct'] > 0])
        losses = total - wins
        returns = [r['return_pct'] for r in results]
        
        summary = {
            '回测期间': f'{start_date} 至 {end_date}',
            '交易日数': trade_days,
            '总选股次数': total,
            '有选股天数': len(set(r['date'] for r in results)),
            '盈利次数': wins,
            '亏损次数': losses,
            '胜率': f'{wins/total*100:.1f}%' if total > 0 else '0%',
            '平均收益率': f'{sum(returns)/len(returns):.2f}%' if returns else '0%',
            '最高单日收益': f'{max(returns):.2f}%' if returns else '0%',
            '最低单日收益': f'{min(returns):.2f}%' if returns else '0%',
            '累计收益率': f'{sum(returns):.2f}%' if returns else '0%',
            '评分统计': score_stats
        }
        
        # 保存JSON
        json_file = f'backtest_2025_summary_{start_date}_{end_date}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # 打印汇总
        print("\n" + "="*70)
        print("📊 2025年回测汇总（全A股）")
        print("="*70)
        print(f"回测期间: {summary['回测期间']}")
        print(f"交易日数: {trade_days}")
        print(f"总选股次数: {total}")
        print(f"有选股天数: {summary['有选股天数']}")
        print(f"\n📈 收益统计:")
        print(f"  胜率: {summary['胜率']}")
        print(f"  平均收益率: {summary['平均收益率']}")
        print(f"  累计收益率: {summary['累计收益率']}")
        print(f"  最高单日: {summary['最高单日收益']}")
        print(f"  最低单日: {summary['最低单日收益']}")
        print(f"\n📊 评分统计 (基于{score_stats['总评分样本数']}个样本):")
        print(f"  平均评分: {score_stats['平均评分']}")
        print(f"  中位数: {score_stats['中位数评分']}")
        print(f"  标准差: {score_stats['标准差']}")
        print(f"  75分位: {score_stats['75分位数']}")
        print(f"  90分位: {score_stats['90分位数']}")
        print(f"  95分位: {score_stats['95分位数']}")
        print(f"\n💡 阈值建议:")
        print(f"  当前阈值50分约位于 {(50-score_stats['平均评分'])/score_stats['标准差']:.1f} 个标准差处")
        print(f"  建议阈值范围: {score_stats['75分位数']:.0f} - {score_stats['90分位数']:.0f} 分")
        print("="*70)
        print(f"✅ 明细: {csv_file}")
        print(f"✅ 汇总: {json_file}")
        
    else:
        print("❌ 未生成交易记录")


if __name__ == '__main__':
    run_backtest_2025()

    run_backtest_2025()
