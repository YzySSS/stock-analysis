#!/usr/bin/env python3
"""
IC分析工具 (Information Coefficient Analysis)
=============================================
用于评估选股因子的预测能力

IC = 因子值与下期收益的相关系数
- IC > 0: 因子与收益正相关
- IC < 0: 因子与收益负相关
- |IC| > 0.03: 有预测能力
- |IC| > 0.05: 较强预测能力

用法:
    python3 /root/.openclaw/workspace/股票分析项目/ic_analysis.py
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pymysql
# from scipy.stats import spearmanr  # 如果scipy可用，否则使用pandas

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class ICAnalyzer:
    """IC因子分析器"""
    
    def __init__(self):
        self.conn = None
        
    def connect_db(self):
        """连接数据库"""
        self.conn = pymysql.connect(**DB_CONFIG)
        
    def get_factor_data(self, date: str) -> pd.DataFrame:
        """获取因子数据"""
        query = """
        SELECT 
            b.code,
            b.name,
            k.open, k.close, k.high, k.low, k.volume, k.turnover,
            k.pct_change
        FROM stock_basic b
        JOIN stock_kline k ON b.code = k.code
        WHERE k.trade_date = %s
        AND b.is_delisted = 0
        """
        
        df = pd.read_sql(query, self.conn, params=(date,))
        
        # 计算因子
        df['momentum'] = df['pct_change']  # 动量因子
        df['turnover_norm'] = df['turnover'] / df['turnover'].median()  # 成交额标准化
        df['volume_norm'] = df['volume'] / df['volume'].median()  # 成交量标准化
        
        return df
    
    def get_next_day_return(self, codes: List[str], today: str) -> Dict:
        """获取次日收益"""
        # 获取下一个交易日
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date FROM stock_kline 
                WHERE trade_date > %s 
                GROUP BY trade_date 
                ORDER BY trade_date 
                LIMIT 1
            """, (today,))
            row = cursor.fetchone()
            if not row:
                return {}
            next_day = row[0]
        
        # 获取次日开盘价和今日收盘价
        placeholders = ','.join(['%s'] * len(codes))
        query = f"""
        SELECT 
            k1.code,
            k1.close as today_close,
            k2.open as next_open
        FROM stock_kline k1
        JOIN stock_kline k2 ON k1.code = k2.code
        WHERE k1.code IN ({placeholders}) 
        AND k1.trade_date = %s
        AND k2.trade_date = %s
        """
        
        df = pd.read_sql(query, self.conn, params=codes + [today, next_day])
        
        returns = {}
        for _, row in df.iterrows():
            if row['today_close'] and row['next_open'] and row['today_close'] > 0:
                ret = (row['next_open'] - row['today_close']) / row['today_close'] * 100
                returns[row['code']] = ret
        
        return returns
    
    def analyze_factor(self, factor_name: str, dates: List[str]) -> Dict:
        """分析单个因子"""
        ic_values = []
        
        for date in dates:
            try:
                # 获取因子数据
                df = self.get_factor_data(date)
                if len(df) < 10:
                    continue
                
                # 获取次日收益
                returns = self.get_next_day_return(df['code'].tolist(), date)
                df['forward_return'] = df['code'].map(returns)
                df = df.dropna(subset=['forward_return'])
                
                if len(df) < 10:
                    continue
                
                # 获取因子值
                if factor_name == 'momentum':
                    factor_values = df['momentum']
                elif factor_name == 'turnover':
                    factor_values = df['turnover_norm']
                elif factor_name == 'volume':
                    factor_values = df['volume_norm']
                else:
                    continue
                
                # 计算IC (Spearman秩相关系数)
                # 使用pandas的corr方法
                temp_df = pd.DataFrame({
                    'factor': factor_values,
                    'return': df['forward_return']
                })
                # 转换为排名后计算pearson相关系数 = spearman相关系数
                temp_df['factor_rank'] = temp_df['factor'].rank()
                temp_df['return_rank'] = temp_df['return'].rank()
                ic = temp_df['factor_rank'].corr(temp_df['return_rank'])
                
                if not np.isnan(ic):
                    ic_values.append(ic)
                    
            except Exception as e:
                print(f"  分析 {date} 失败: {e}")
                continue
        
        if not ic_values:
            return {'factor': factor_name, 'error': '无有效IC值'}
        
        ic_series = pd.Series(ic_values)
        
        return {
            'factor': factor_name,
            'ic_mean': round(ic_series.mean(), 4),
            'ic_std': round(ic_series.std(), 4),
            'icir': round(ic_series.mean() / ic_series.std(), 4) if ic_series.std() > 0 else 0,
            'ic_positive_ratio': round((ic_series > 0).sum() / len(ic_series), 4),
            'ic_abs_mean': round(ic_series.abs().mean(), 4),
            'sample_count': len(ic_series)
        }
    
    def run_analysis(self, start_date: str, end_date: str):
        """运行完整IC分析"""
        print("=" * 60)
        print("📊 V11因子IC分析报告")
        print("=" * 60)
        
        # 获取交易日
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM stock_kline 
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (start_date, end_date))
            dates = [row[0] for row in cursor.fetchall()]
        
        print(f"\n分析区间: {start_date} ~ {end_date}")
        print(f"交易日数量: {len(dates)}天\n")
        
        # 分析各因子
        factors = [
            ('momentum', '动量因子(当日涨幅)'),
            ('turnover', '成交额因子'),
            ('volume', '成交量因子')
        ]
        
        results = []
        for factor_key, factor_name in factors:
            print(f"🔍 分析因子: {factor_name}...")
            result = self.analyze_factor(factor_key, dates)
            results.append(result)
        
        # 打印结果
        print("\n" + "=" * 60)
        print("📈 IC分析结果")
        print("=" * 60)
        
        for r in results:
            if 'error' in r:
                print(f"\n❌ {r['factor']}: {r['error']}")
                continue
            
            ic_mean = r['ic_mean']
            ic_abs = r['ic_abs_mean']
            
            # 评价
            if ic_abs < 0.02:
                eval_text = "❌ 无效"
            elif ic_abs < 0.03:
                eval_text = "⚠️ 较弱"
            elif ic_abs < 0.05:
                eval_text = "✅ 有效"
            else:
                eval_text = "🌟 很强"
            
            print(f"\n{eval_text} | {r['factor']}")
            print(f"  IC均值: {ic_mean:+.4f}")
            print(f"  |IC|均值: {ic_abs:.4f}")
            print(f"  ICIR: {r['icir']:+.4f}")
            print(f"  正IC占比: {r['ic_positive_ratio']:.1%}")
            print(f"  样本数: {r['sample_count']}天")
        
        # 解读指南
        print("\n" + "=" * 60)
        print("📚 IC解读指南")
        print("=" * 60)
        print("""
| |IC| 范围    | 评价      | 建议
|---------------|----------|------
| < 0.02        | ❌ 无效   | 舍弃
| 0.02 - 0.03   | ⚠️ 较弱   | 谨慎
| 0.03 - 0.05   | ✅ 有效   | 推荐
| > 0.05        | 🌟 很强   | 核心

ICIR > 0.5: 稳定性较好
ICIR > 1.0: 非常稳定
        """)
        
        return results


if __name__ == "__main__":
    analyzer = ICAnalyzer()
    analyzer.connect_db()
    
    # 分析最近一个月
    results = analyzer.run_analysis('2026-03-01', '2026-04-03')
    
    print("\n✅ IC分析完成!")
    print("=" * 60)
