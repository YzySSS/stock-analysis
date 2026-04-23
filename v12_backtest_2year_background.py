#!/usr/bin/env python3
"""
V12策略 2年回测 - 后台运行版
=============================
优化: 批量处理，定期保存进度
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
from collections import defaultdict

# 配置日志
log_file = '/root/.openclaw/workspace/股票分析项目/logs/v12_backtest_2year.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4'
}

RUN_ID = f'V12_2Year_{datetime.now().strftime("%Y%m%d_%H%M%S")}'


def log_progress(current, total, message=""):
    """记录进度"""
    pct = current / total * 100
    logger.info(f"[{current}/{total} {pct:.1f}%] {message}")
    
    # 保存进度到文件
    progress_file = '/root/.openclaw/workspace/股票分析项目/logs/backtest_progress.json'
    with open(progress_file, 'w') as f:
        json.dump({
            'run_id': RUN_ID,
            'current': current,
            'total': total,
            'percent': pct,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }, f)


def load_kline_data(start_date, end_date):
    """分批加载K线数据"""
    logger.info("=" * 70)
    logger.info(f"[{RUN_ID}] 开始加载K线数据")
    logger.info("=" * 70)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取交易日列表
    cursor.execute("""
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (start_date, end_date))
    
    trading_days = [row[0] for row in cursor.fetchall()]
    logger.info(f"交易日数量: {len(trading_days)}")
    
    # 获取股票代码列表
    cursor.execute("""
        SELECT DISTINCT code FROM stock_kline 
        WHERE trade_date BETWEEN %s AND %s
        AND code NOT LIKE '399%%' AND code NOT LIKE '899%%'
    """, (start_date, end_date))
    
    codes = [row[0] for row in cursor.fetchall()]
    logger.info(f"股票数量: {len(codes)}")
    
    cursor.close()
    conn.close()
    
    return trading_days, codes


def get_stock_batch_data(codes, start_date, end_date):
    """批量获取股票数据"""
    conn = pymysql.connect(**DB_CONFIG)
    
    codes_str = ','.join([f"'{c}'" for c in codes])
    
    sql = f"""
    SELECT 
        k.code, k.trade_date, k.open, k.close, k.turnover, k.pct_change,
        b.roe, b.pe_fixed, b.pb_ratio, b.name, b.industry
    FROM stock_kline k
    LEFT JOIN stock_basic b ON k.code = b.code COLLATE utf8mb4_unicode_ci
    WHERE k.code IN ({codes_str})
    AND k.trade_date BETWEEN %s AND %s
    AND k.amount >= 500000
    AND (b.is_delisted = 0 OR b.is_delisted IS NULL)
    AND (b.is_st = 0 OR b.is_st IS NULL)
    ORDER BY k.code, k.trade_date
    """
    
    df = pd.read_sql(sql, conn, params=(start_date, end_date))
    conn.close()
    
    # 数据类型转换
    for col in ['open', 'close', 'turnover', 'pct_change', 'roe', 'pe_fixed', 'pb_ratio']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    return df


def calculate_factors_batch(df):
    """批量计算因子"""
    results = []
    
    for code, group in df.groupby('code'):
        group = group.sort_values('trade_date').reset_index(drop=True)
        
        if len(group) < 30:
            continue
        
        for i in range(30, len(group)):
            row = group.iloc[i]
            hist = group.iloc[:i]
            
            price = row['close']
            turnover = row['turnover']
            
            # 硬性过滤
            if price < 5 or price > 200 or turnover < 0.5:
                continue
            
            # MA20过滤
            if len(hist) >= 20:
                ma20 = hist['close'].tail(20).mean()
                if price < ma20 * 0.90:
                    continue
            
            # 趋势因子
            if len(hist) >= 25:
                ma20_now = hist['close'].tail(20).mean()
                ma20_prev = hist['close'].iloc[-25:-5].mean()
                trend = (ma20_now - ma20_prev) / ma20_prev * 252 * 100 if ma20_prev > 0 else 0
            else:
                trend = 0
            
            # 动量因子
            if len(hist) >= 21:
                momentum = (price - hist['close'].iloc[-21]) / hist['close'].iloc[-21] * 100
            else:
                momentum = 0
            
            # 质量因子 (ROE)
            roe = row['roe'] if pd.notna(row['roe']) else None
            
            # 估值因子 (PE)
            pe = row['pe_fixed'] if pd.notna(row['pe_fixed']) else None
            
            # 情绪因子
            sentiment = hist['pct_change'].iloc[-1] if len(hist) > 0 else 0
            
            # 流动性
            liquidity = np.log(turnover + 1)
            
            results.append({
                'code': code,
                'name': row['name'],
                'date': row['trade_date'],
                'price': price,
                'trend': trend,
                'momentum': momentum,
                'roe': roe,
                'pe': pe,
                'pb': row['pb_ratio'],
                'sentiment': sentiment,
                'liquidity': liquidity
            })
    
    return pd.DataFrame(results)


def zscore_normalize(df, factor_cols):
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


def calculate_score(row):
    """计算加权得分"""
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


def run_backtest(trading_days, df_kline, score_threshold=55):
    """运行回测"""
    logger.info("=" * 70)
    logger.info("开始计算因子和回测")
    logger.info("=" * 70)
    
    # 计算所有因子
    df_factors = calculate_factors_batch(df_kline)
    logger.info(f"因子计算完成: {len(df_factors)} 条记录")
    
    if len(df_factors) == 0:
        logger.error("没有有效的因子数据")
        return pd.DataFrame()
    
    # 逐日选股和回测
    trades = []
    total_days = len(trading_days) - 1
    
    for i, date in enumerate(trading_days[:-1]):
        if i % 20 == 0:
            log_progress(i, total_days, f"回测日期: {date}")
        
        # 当日选股
        day_data = df_factors[df_factors['date'] == pd.Timestamp(date)].copy()
        
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
        next_day_data = df_kline[df_kline['trade_date'] == pd.Timestamp(next_date)]
        
        for _, pick in picks.iterrows():
            code = pick['code']
            entry_price = pick['price']
            
            # 查找次日收盘价
            next_row = next_day_data[next_day_data['code'] == code]
            if len(next_row) == 0:
                continue
            
            exit_price = next_row['close'].values[0]
            
            # 计算收益
            gross_return = (exit_price - entry_price) / entry_price * 100
            
            # 扣除成本 (佣金0.03% + 印花税0.05% + 滑点0.2% = 0.28%)
            cost = 0.28
            net_return = gross_return - cost
            
            trades.append({
                'entry_date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                'exit_date': next_date.strftime('%Y-%m-%d') if hasattr(next_date, 'strftime') else str(next_date),
                'code': code,
                'name': pick['name'],
                'score': round(pick['score'], 1),
                'entry_price': round(entry_price, 2),
                'exit_price': round(exit_price, 2),
                'gross_return': round(gross_return, 2),
                'net_return': round(net_return, 2)
            })
    
    return pd.DataFrame(trades)


def save_to_database(df_trades, report):
    """保存结果到数据库"""
    logger.info("保存结果到数据库...")
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 保存汇总
        cursor.execute('''
            INSERT INTO backtest_summary 
            (run_id, strategy_version, start_date, end_date, initial_capital,
             total_trades, win_trades, loss_trades, win_rate, total_return, 
             annual_return, max_drawdown, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            RUN_ID, 'V12_2Year_Backtest', 
            report['start_date'], report['end_date'],
            1000000,
            report['total_trades'],
            report['win_trades'],
            report['total_trades'] - report['win_trades'],
            report['win_rate'] / 100,
            report['total_return'] / 100,
            report['annualized'] / 100 if 'annualized' in report else None,
            report.get('max_drawdown', 0) / 100,
            'V12策略2年完整回测'
        ))
        
        # 保存交易明细
        for _, row in df_trades.iterrows():
            cursor.execute('''
                INSERT INTO backtest_trades 
                (run_id, strategy_version, code, name, select_date, select_score,
                 entry_price, exit_price, gross_return, net_return, exit_reason, hold_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                RUN_ID, 'V12_2Year_Backtest',
                row['code'], row['name'], row['entry_date'], row['score'],
                row['entry_price'], row['exit_price'], 
                row['gross_return'] / 100, row['net_return'] / 100,
                'time_exit', 1
            ))
        
        conn.commit()
        logger.info(f"已保存到数据库 (run_id: {RUN_ID})")
        
    except Exception as e:
        logger.error(f"保存数据库失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def generate_report(df_trades):
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
    annualized = ((cumulative ** (1/max(years, 0.1))) - 1) * 100 if years > 0 else 0
    
    # 按日统计回撤
    daily_returns = df_trades.groupby('entry_date')['net_return'].sum().values
    
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
    
    return {
        'run_id': RUN_ID,
        'start_date': df_trades['entry_date'].min(),
        'end_date': df_trades['exit_date'].max(),
        'total_trades': int(len(df_trades)),
        'win_trades': int(wins),
        'win_rate': round(wins / len(net_returns) * 100, 1),
        'avg_return': round(np.mean(net_returns), 2),
        'total_return': round(cumulative_return, 2),
        'annualized': round(annualized, 2),
        'max_return': round(np.max(net_returns), 2),
        'min_return': round(np.min(net_returns), 2),
        'max_drawdown': round(max_drawdown, 2),
        'sharpe': round(annualized / max_drawdown, 2) if max_drawdown > 0 else 0
    }


def save_files(df_trades, report):
    """保存到文件"""
    output_dir = '/root/.openclaw/workspace/股票分析项目/backtest_results'
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存交易明细
    trades_file = f'{output_dir}/{RUN_ID}_trades.csv'
    df_trades.to_csv(trades_file, index=False, encoding='utf-8-sig')
    logger.info(f"交易明细: {trades_file}")
    
    # 保存报告
    report_file = f'{output_dir}/{RUN_ID}_report.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"报告文件: {report_file}")
    
    # 打印报告
    logger.info("\n" + "=" * 70)
    logger.info("📊 V12策略 2年回测报告")
    logger.info("=" * 70)
    for k, v in report.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 70)


def main():
    """主函数"""
    logger.info("\n" + "=" * 70)
    logger.info(f"V12策略 2年回测启动 [{RUN_ID}]")
    logger.info("=" * 70)
    
    try:
        # 加载交易日和股票列表
        trading_days, codes = load_kline_data('2024-01-01', '2025-12-31')
        
        # 分批处理股票（每批500只）
        batch_size = 500
        all_factors = []
        
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i+batch_size]
            log_progress(i, len(codes), f"处理股票批次 {i//batch_size + 1}")
            
            # 获取数据
            df_kline = get_stock_batch_data(batch_codes, '2023-11-01', '2026-01-31')
            
            if len(df_kline) > 0:
                all_factors.append(df_kline)
        
        # 合并所有数据
        if all_factors:
            df_all = pd.concat(all_factors, ignore_index=True)
            logger.info(f"总数据量: {len(df_all)} 条记录")
            
            # 运行回测
            df_trades = run_backtest(trading_days, df_all, score_threshold=55)
            
            if len(df_trades) > 0:
                # 生成报告
                report = generate_report(df_trades)
                
                # 保存结果
                save_files(df_trades, report)
                save_to_database(df_trades, report)
                
                logger.info("✅ 回测完成!")
            else:
                logger.error("回测失败：无交易记录")
        else:
            logger.error("没有获取到数据")
            
    except Exception as e:
        logger.error(f"回测出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
