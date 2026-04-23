#!/usr/bin/env python3
"""
IC分析工具 V10 - 扩展版
=======================
全面评估V10核心因子的预测能力

因子列表:
- Quality: ROE (净资产收益率)
- Value: -PE (负市盈率，越低越好)
- Momentum: 20日收益率 (追涨)
- Reversal: -20日收益率 (抄底)
- LowVol: -60日波动率 (低波动)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pymysql

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class ICAnalyzerV10:
    """V10因子IC分析器"""
    
    def __init__(self):
        self.conn = None
        
    def connect_db(self):
        """连接数据库"""
        self.conn = pymysql.connect(**DB_CONFIG)
        print("✅ 数据库连接成功")
        
    def get_price_history(self, code: str, date: str, days: int = 65) -> List[float]:
        """获取历史价格"""
        query = """
        SELECT close FROM stock_kline 
        WHERE code = %s AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT %s
        """
        df = pd.read_sql(query, self.conn, params=(code, date, days))
        return list(reversed(df['close'].tolist())) if not df.empty else []
    
    def get_factor_data(self, date: str) -> pd.DataFrame:
        """获取因子数据"""
        query = """
        SELECT 
            b.code,
            b.name,
            b.industry,
            b.roe_clean as roe,
            b.pe_fixed as pe,
            k.open, k.close, k.volume, k.turnover
        FROM stock_basic b
        JOIN stock_kline k ON b.code = k.code
        WHERE k.trade_date = %s
        AND b.is_delisted = 0
        AND b.is_st = 0
        AND k.open BETWEEN 5 AND 150
        AND k.turnover >= 1.0
        """
        
        df = pd.read_sql(query, self.conn, params=(date,))
        
        if len(df) < 100:
            return pd.DataFrame()
        
        # 为每只股票计算技术指标
        print(f"  计算因子 ({len(df)}只股票)...", end='')
        
        factors = []
        for _, row in df.iterrows():
            code = row['code']
            prices = self.get_price_history(code, date, 65)
            
            if len(prices) < 21:
                continue
            
            # Quality: ROE
            quality = row['roe'] if pd.notna(row['roe']) else None
            
            # Value: -PE
            pe = row['pe'] if pd.notna(row['pe']) else 50
            value = -pe if pe > 0 else -50
            
            # 20日收益率
            ret_20d = (prices[-1] - prices[-21]) / prices[-21] * 100 if len(prices) >= 21 else 0
            
            # Momentum: 20日收益
            momentum = ret_20d
            
            # Reversal: -20日收益
            reversal = -ret_20d
            
            # LowVol: -60日波动率
            if len(prices) >= 61:
                returns_60d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-60, 0)]
                lowvol = -np.std(returns_60d) * 100
            elif len(prices) >= 21:
                returns_20d = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(-20, 0)]
                lowvol = -np.std(returns_20d) * 100
            else:
                continue
            
            factors.append({
                'code': code,
                'name': row['name'],
                'industry': row['industry'],
                'quality': quality,
                'value': value,
                'momentum': momentum,
                'reversal': reversal,
                'lowvol': lowvol,
                'today_close': prices[-1]
            })
        
        result_df = pd.DataFrame(factors)
        print(f" ✓ {len(result_df)}只")
        return result_df
    
    def get_forward_return(self, codes: List[str], today: str, hold_days: int = 1) -> Dict:
        """获取未来收益"""
        # 获取未来第N个交易日
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date FROM stock_kline 
                WHERE trade_date > %s 
                GROUP BY trade_date 
                ORDER BY trade_date 
                LIMIT %s
            """, (today, hold_days))
            rows = cursor.fetchall()
            if len(rows) < hold_days:
                return {}
            future_day = rows[-1][0]
        
        # 获取未来价格
        placeholders = ','.join(['%s'] * len(codes))
        query = f"""
        SELECT 
            code,
            close as future_close
        FROM stock_kline
        WHERE code IN ({placeholders}) 
        AND trade_date = %s
        """
        
        df = pd.read_sql(query, self.conn, params=codes + [future_day])
        
        returns = {}
        for _, row in df.iterrows():
            if pd.notna(row['future_close']):
                returns[row['code']] = row['future_close']
        
        return returns
    
    def calculate_ic(self, factor_values: pd.Series, forward_returns: pd.Series) -> float:
        """计算Spearman IC"""
        # 移除NaN
        valid = ~(np.isnan(factor_values) | np.isnan(forward_returns))
        if valid.sum() < 10:
            return np.nan
        
        f = factor_values[valid]
        r = forward_returns[valid]
        
        # Spearman秩相关系数 = Pearson(排名, 排名)
        f_rank = f.rank()
        r_rank = r.rank()
        
        return f_rank.corr(r_rank)
    
    def analyze_factor(self, factor_name: str, dates: List[str], hold_days: int = 1) -> Dict:
        """分析单个因子"""
        ic_values = []
        
        for date in dates:
            try:
                # 获取因子数据
                df = self.get_factor_data(date)
                if len(df) < 50:
                    continue
                
                # 获取未来收益
                future_prices = self.get_forward_return(df['code'].tolist(), date, hold_days)
                
                # 计算收益率
                df['forward_return'] = df.apply(
                    lambda x: (future_prices.get(x['code'], np.nan) - x['today_close']) / x['today_close'] * 100 
                    if x['code'] in future_prices else np.nan,
                    axis=1
                )
                
                df = df.dropna(subset=['forward_return', factor_name])
                
                if len(df) < 30:
                    continue
                
                # 计算IC
                ic = self.calculate_ic(df[factor_name], df['forward_return'])
                
                if not np.isnan(ic):
                    ic_values.append(ic)
                    
            except Exception as e:
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
        print("=" * 70)
        print("📊 V10因子IC分析报告")
        print("=" * 70)
        
        # 获取交易日
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM stock_kline 
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (start_date, end_date))
            dates = [row[0] for row in cursor.fetchall()]
        
        print(f"\n分析区间: {start_date} ~ {end_date}")
        print(f"交易日数量: {len(dates)}天")
        print("=" * 70)
        
        # 分析各因子
        factors = [
            ('quality', 'Quality (ROE)'),
            ('value', 'Value (-PE)'),
            ('momentum', 'Momentum (20日收益)'),
            ('reversal', 'Reversal (-20日收益)'),
            ('lowvol', 'LowVol (-60日波动率)')
        ]
        
        results = []
        for factor_key, factor_name in factors:
            print(f"\n🔍 分析因子: {factor_name}...")
            result = self.analyze_factor(factor_key, dates, hold_days=1)
            results.append(result)
        
        # 打印结果
        print("\n" + "=" * 70)
        print("📈 IC分析结果 (1日持仓)")
        print("=" * 70)
        
        print(f"\n{'因子':<20} {'IC均值':>10} {'|IC|':>8} {'ICIR':>8} {'评价':<8}")
        print("-" * 70)
        
        for r in results:
            if 'error' in r:
                print(f"{r['factor']:<20} {'错误':>10} {r['error']}")
                continue
            
            ic_mean = r['ic_mean']
            ic_abs = r['ic_abs_mean']
            icir = r['icir']
            
            # 评价
            if ic_abs < 0.02:
                eval_text = "❌ 无效"
            elif ic_abs < 0.03:
                eval_text = "⚠️ 较弱"
            elif ic_abs < 0.05:
                eval_text = "✅ 有效"
            else:
                eval_text = "🌟 很强"
            
            print(f"{r['factor']:<20} {ic_mean:>+10.4f} {ic_abs:>8.4f} {icir:>+8.2f} {eval_text:<8}")
        
        # 详细结果
        print("\n" + "=" * 70)
        print("📊 详细统计")
        print("=" * 70)
        
        for r in results:
            if 'error' in r:
                continue
            
            print(f"\n{r['factor']}:")
            print(f"  IC均值: {r['ic_mean']:+.4f}")
            print(f"  IC标准差: {r['ic_std']:.4f}")
            print(f"  |IC|均值: {r['ic_abs_mean']:.4f}")
            print(f"  ICIR: {r['icir']:+.4f}")
            print(f"  正IC占比: {r['ic_positive_ratio']:.1%}")
            print(f"  样本数: {r['sample_count']}天")
        
        # 解读指南
        print("\n" + "=" * 70)
        print("📚 IC解读指南")
        print("=" * 70)
        print("""
| |IC| 范围    | 评价      | 建议
|---------------|----------|------------------
| < 0.02        | ❌ 无效   | 舍弃或重构
| 0.02 - 0.03   | ⚠️ 较弱   | 谨慎使用
| 0.03 - 0.05   | ✅ 有效   | 推荐使用
| > 0.05        | 🌟 很强   | 核心因子

ICIR > 0.5: 稳定性较好
ICIR > 1.0: 非常稳定 (但可能过拟合)

V10因子预测:
- Quality: 预计IC ≈ 0.01-0.02 (ROE数据不完整)
- Value: 预计IC < 0 (可能负相关，价值陷阱)
- Momentum: 预计IC ≈ 0.03-0.05 (A股动量效应)
- Reversal: 预计IC < 0 (与动量相反)
- LowVol: 预计IC ≈ 0.01-0.02 (低波动异象)
        """)
        
        return results


if __name__ == "__main__":
    analyzer = ICAnalyzerV10()
    analyzer.connect_db()
    
    # 分析最近2年
    results = analyzer.run_analysis('2024-01-01', '2026-04-03')
    
    print("\n✅ IC分析完成!")
    print("=" * 70)
