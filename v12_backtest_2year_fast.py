#!/usr/bin/env python3
"""
V12策略 2年回测 - 快速版
========================
优化: 一次性加载所有数据，内存中计算
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import pymysql
import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4'
}


def load_all_data(start_date: str, end_date: str) -> pd.DataFrame:
    """一次性加载所有K线数据"""
    logger.info("[1/4] 加载K线数据...")
    conn = pymysql.connect(**DB_CONFIG)
    
    sql = """
    SELECT k.code, k.trade_date, k.open, k.close, k.turnover, k.pct_change,
           b.roe, b.pe_fixed, b.pb_ratio, b.name
    FROM stock_kline k
    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
    WHERE k.trade_date BETWEEN %s AND %s
    AND k.amount >= 500000
    AND b.is_delisted = 0 AND b.is_st = 0
    ORDER BY k.code, k.trade_date
    """
    
    df = pd.read_sql(sql, conn, params=(start_date, end_date))
    conn.close()
    
    # 数据类型转换
    for col in ['open', 'close', 'turnover', 'pct_change', 'roe', 'pe_fixed', 'pb_ratio']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    logger.info(f"  加载完成: {len(df)} 条记录, {df['code'].nunique()} 只股票")
    return df


def calculate_factors_for_stock(group: pd.DataFrame) -> pd.DataFrame:
    """为单只股票计算所有因子"""
    group = group.sort_values('trade_date').reset_index(drop=True)
    
    if len(group) < 30:
        return pd.DataFrame()
    
    results = []
    
    for i in range(30, len(group)):
        row = group.iloc[i]
        hist = group.iloc[:i]
        
        # 硬性过滤
        price = row['close']
        turnover = row['turnover']
        
        if price < 5 or price > 200 or turnover < 0.5:
            continue
        
        # MA20过滤
        if len(hist) >= 20:
            ma20 = hist['close'].tail(20).mean()
            if price < ma20 * 0.90:
                continue
        
        # 趋势因子 (MA20斜率)
        if len(hist) >= 25:
            ma20_now = hist['close'].tail(20).mean()
            ma20_prev = hist['close'].iloc[-25:-5].mean()
            trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100 if ma20_prev > 0 else 0
        else:
            trend = 0
        
        # 动量因子 (20日收益)
        if len(hist) >= 21:
            momentum = (price - hist['close'].iloc[-21]) / hist['close'].iloc[-21] * 100
        else:
            momentum = 0
        
        # 质量因子 (ROE)
        roe = row['roe'] if pd.notna(row['roe']) else None
        
        # 估值因子 (PE)
        pe = row['pe_fixed'] if pd.notna(row['pe_fixed']) else None
        
        # 情绪因子 (前一日涨跌)
        sentiment = hist['pct_change'].iloc[-1] if len(hist) > 0 else 0
        
        # 流动性因子
        liquidity = np.log(turnover + 1)
        
        results.append({
            'code': row['code'],
            'name': row['name'],
            'date': row['trade_date'],
            'price': price,
            'trend': trend,
            'momentum': momentum,
            'roe': roe,
            'pe': pe,
            'pb': row['pb_ratio'],
            'sentiment': sentiment,
            'liquidity': liquidity,
            'ma20': ma20 if len(hist) >= 20 else None
        })
    
    return pd.DataFrame(results)


def zscore_normalize(df: pd.DataFrame, factor_cols: List[str]) -> pd.DataFrame:
    """Z-score标准化"""
    for col in factor_cols:
        if col in df.columns and df[col].notna().sum() > 0:
            mean = df[col].mean()
            std = df[col].std()
            if std > 0:
                df[f'{col}_z'] = ((df[col] - mean) / std).clip(-3, 3)
            else:
                df[f'{col}_z'] = 0
        else:
            df[f'{col}_z'] = 0
    return df


def calculate_score(row: pd.Series) -> float:
    """计算加权得分"""
    # V12权重配置
    weights = {
        'trend_z': 0.25,
        'momentum_z': 0.15,
        'roe_z': 0.20,
        'sentiment_z': 0.20,
        'pe_z': 0.20
    }
    
    weighted_sum = sum(row.get(k, 0) * v for k, v in weights.items())
    score = 50 + weighted_sum * 15
    return np.clip(score, 0, 100)


def run_backtest(df: pd.DataFrame, score_threshold: int = 55) -> Tuple[pd.DataFrame, Dict]:
    """运行回测"""
    logger.info("[2/4] 计算因子...")
    
    # 为每只股票计算因子
    all_factors = []
    for code, group in df.groupby('code'):
        factors_df = calculate_factors_for_stock(group)
        if len(factors_df) > 0:
            all_factors.append(factors_df)
    
    if not all_factors:
        logger.error("没有有效的因子数据")
        return pd.DataFrame(), {}
    
    factors_all = pd.concat(all_factors, ignore_index=True)
    logger.info(f"  因子计算完成: {len(factors_all)} 条记录")
    
    # 获取交易日列表
    trading_days = sorted(factors_all['date'].unique())
    logger.info(f"  交易日: {len(trading_days)} 天")
    
    # 逐日选股和回测
    logger.info("[3/4] 回测模拟...")
    trades = []
    
    for i, date in enumerate(trading_days[:-1]):
        if i % 50 == 0:
            logger.info(f"  进度: {i}/{len(trading_days)} ({i/len(trading_days)*100:.1f}%)")
        
        # 当日选股
        day_data = factors_all[factors_all['date'] == date].copy()
        
        if len(day_data) < 10:
            continue
        
        # Z-score标准化
        day_data = zscore_normalize(day_data, ['trend', 'momentum', 'roe', 'sentiment', 'pe'])
        
        # 计算得分
        day_data['score'] = day_data.apply(calculate_score, axis=1)
        
        # 选达标股票 (最多5只)
        picks = day_data[day_data['score'] >= score_threshold].nlargest(5, 'score')
        
        if len(picks) == 0:
            continue
        
        # 次日价格
        next_date = trading_days[i + 1]
        next_day = df[df['trade_date'] == next_date][['code', 'open', 'close']].rename(
            columns={'open': 'next_open', 'close': 'next_close'}
        )
        
        for _, pick in picks.iterrows():
            code = pick['code']
            entry_price = pick['price']  # 收盘价买入
            
            # 查找次日价格
            next_price = next_day[next_day['code'] == code]
            if len(next_price) == 0:
                continue
            
            exit_price = next_price['next_close'].values[0]
            
            # 计算收益
            gross_return = (exit_price - entry_price) / entry_price * 100
            
            # 扣除成本 (佣金0.03% + 印花税0.05% + 滑点0.2% = 0.28%)
            cost = 0.28
            net_return = gross_return - cost
            
            trades.append({
                'entry_date': date.strftime('%Y-%m-%d'),
                'exit_date': next_date.strftime('%Y-%m-%d'),
                'code': code,
                'name': pick['name'],
                'entry_price': round(entry_price, 2),
                'exit_price': round(exit_price, 2),
                'score': round(pick['score'], 1),
                'gross_return': round(gross_return, 2),
                'net_return': round(net_return, 2)
            })
    
    df_trades = pd.DataFrame(trades)
    logger.info(f"  回测完成: {len(df_trades)} 笔交易")
    
    return df_trades, {}


def generate_report(df_trades: pd.DataFrame) -> Dict:
    """生成回测报告"""
    if len(df_trades) == 0:
        return {}
    
    net_returns = df_trades['net_return'].values
    wins = np.sum(net_returns > 0)
    
    # 复利计算
    cumulative = 1.0
    for r in net_returns:
        cumulative *= (1 + r / 100)
    cumulative_return = (cumulative - 1) * 100
    
    # 交易天数
    trade_days = df_trades['entry_date'].nunique()
    years = trade_days / 252 if trade_days > 0 else 0
    annualized = ((cumulative ** (1/years)) - 1) * 100 if years > 0 else 0
    
    # 按日统计
    daily_returns = df_trades.groupby('entry_date')['net_return'].mean().values
    
    # 最大回撤
    peak = 0
    max_drawdown = 0
    running = 0
    for r in daily_returns:
        running += r
        if running > peak:
            peak = running
        drawdown = peak - running
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    
    report = {
        '回测版本': 'V12_2Year_Fast',
        '回测区间': f"{df_trades['entry_date'].min()} 至 {df_trades['exit_date'].max()}",
        '总交易数': int(len(df_trades)),
        '交易天数': int(trade_days),
        '胜率': round(wins / len(net_returns) * 100, 1),
        '平均收益': round(np.mean(net_returns), 2),
        '累计收益': round(cumulative_return, 2),
        '年化收益': round(annualized, 2),
        '最大单笔': round(np.max(net_returns), 2),
        '最小单笔': round(np.min(net_returns), 2),
        '最大回撤': round(max_drawdown, 2),
        '夏普比率': round(annualized / max_drawdown, 2) if max_drawdown > 0 else 0
    }
    
    return report


def save_results(df_trades: pd.DataFrame, report: Dict):
    """保存结果"""
    logger.info("[4/4] 保存结果...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix = f'v12_2year_fast_{timestamp}'
    
    # 保存交易明细
    trades_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_trades.csv'
    df_trades.to_csv(trades_path, index=False, encoding='utf-8-sig')
    
    # 保存报告
    report_path = f'/root/.openclaw/workspace/股票分析项目/{prefix}_report.json'
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"  交易明细: {trades_path}")
    logger.info(f"  报告文件: {report_path}")
    
    # 打印报告
    print("\n" + "=" * 70)
    print("📊 V12策略 2年回测报告 (快速版)")
    print("=" * 70)
    for k, v in report.items():
        print(f"  {k}: {v}")
    print("=" * 70)


def main():
    """主函数"""
    logger.info("=" * 70)
    logger.info("V12策略 2年回测 - 快速版")
    logger.info("=" * 70)
    
    # 加载数据
    df = load_all_data('2023-11-01', '2026-04-13')
    
    # 运行回测
    df_trades, _ = run_backtest(df, score_threshold=55)
    
    if len(df_trades) > 0:
        # 生成报告
        report = generate_report(df_trades)
        # 保存结果
        save_results(df_trades, report)
    else:
        logger.error("回测失败：无交易记录")


if __name__ == '__main__':
    main()
