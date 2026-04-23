#!/usr/bin/env python3
"""
基本面数据清洗与行业中性化
============================
1. 去极值（MAD方法 / 3σ方法）
2. 行业中性化（行业均值归一化）
3. 输出清洗后的pe_ratio_clean, pb_ratio_clean, roe_clean
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pymysql
import numpy as np
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8',
    'port': 3306,
    'user': 'openclaw_user',
    'password': 'open@2026',
    'database': 'stock',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}


class FundamentalDataCleaner:
    """基本面数据清洗器"""
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def get_stock_industry_map(self) -> Dict[str, str]:
        """获取股票行业映射"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT code, industry FROM stock_basic 
                WHERE industry IS NOT NULL AND industry != ''
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}
    
    def get_fundamental_data(self) -> pd.DataFrame:
        """获取原始基本面数据"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    code,
                    name,
                    pe_ratio,
                    pb_ratio,
                    roe,
                    industry
                FROM stock_basic
                WHERE pe_ratio IS NOT NULL OR pb_ratio IS NOT NULL OR roe IS NOT NULL
            """)
            
            data = []
            for row in cursor.fetchall():
                data.append({
                    'code': row[0],
                    'name': row[1],
                    'pe_ratio': float(row[2]) if row[2] is not None else None,
                    'pb_ratio': float(row[3]) if row[3] is not None else None,
                    'roe': float(row[4]) if row[4] is not None else None,
                    'industry': row[5] or '其他'
                })
            
            df = pd.DataFrame(data)
            # 确保数值列为float类型
            for col in ['pe_ratio', 'pb_ratio', 'roe']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
    
    def mad_winsorize(self, series: pd.Series, n: int = 3) -> pd.Series:
        """
        MAD去极值（中位数绝对偏差）
        
        步骤:
        1. 计算中位数 median
        2. 计算 |x - median| 的中位数作为 MAD
        3. 上下界 = median ± n * 1.4826 * MAD
        4. 超出边界的值用边界值替代
        """
        median = series.median()
        mad = np.median(np.abs(series - median))
        
        # 标准正态分布下，MAD ≈ 0.6745 * σ，所以 1.4826 ≈ 1/0.6745
        scale = 1.4826
        lower_bound = median - n * scale * mad
        upper_bound = median + n * scale * mad
        
        return series.clip(lower=lower_bound, upper=upper_bound)
    
    def sigma_winsorize(self, series: pd.Series, n: int = 3) -> pd.Series:
        """
        3σ去极值
        
        上下界 = mean ± n * std
        """
        mean = series.mean()
        std = series.std()
        
        lower_bound = mean - n * std
        upper_bound = mean + n * std
        
        return series.clip(lower=lower_bound, upper=upper_bound)
    
    def industry_neutralize(self, df: pd.DataFrame, factor: str) -> pd.Series:
        """
        行业中性化
        
        步骤:
        1. 按行业分组
        2. 计算行业均值和标准差
        3. 因子值 = (原始值 - 行业均值) / 行业标准差
        
        对于估值因子（PE/PB），越低越好，所以取负值
        对于质量因子（ROE），越高越好，保持正值
        """
        result = pd.Series(index=df.index, dtype=float)
        
        for industry, group in df.groupby('industry'):
            values = group[factor].replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(values) < 5:  # 行业内样本太少，跳过中性化
                result.loc[group.index] = values
                continue
            
            industry_mean = values.mean()
            industry_std = values.std()
            
            if industry_std < 1e-10:  # 避免除零
                result.loc[group.index] = 0
            else:
                # Z-score标准化（行业内）
                zscore = (values - industry_mean) / industry_std
                result.loc[group.index] = zscore
        
        return result
    
    def clean_fundamental_data(self, method: str = 'mad') -> pd.DataFrame:
        """
        执行数据清洗流程
        
        Args:
            method: 'mad' 或 'sigma'
        
        Returns:
            清洗后的DataFrame，包含原始值和清洗后的值
        """
        logger.info("=" * 70)
        logger.info("开始基本面数据清洗")
        logger.info("=" * 70)
        
        # 1. 获取原始数据
        df = self.get_fundamental_data()
        logger.info(f"获取到 {len(df)} 只股票的基本面数据")
        
        # 2. 数据预处理
        # 处理异常值和无穷大
        for col in ['pe_ratio', 'pb_ratio', 'roe']:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        
        # PE/PB必须为正数
        df.loc[df['pe_ratio'] <= 0, 'pe_ratio'] = np.nan
        df.loc[df['pb_ratio'] <= 0, 'pb_ratio'] = np.nan
        
        # 3. 去极值
        factors = ['pe_ratio', 'pb_ratio', 'roe']
        
        for factor in factors:
            valid_data = df[factor].dropna()
            if len(valid_data) < 10:
                logger.warning(f"{factor} 有效数据不足，跳过清洗")
                continue
            
            logger.info(f"\n【{factor}】去极值前统计:")
            logger.info(f"  均值: {valid_data.mean():.2f}, 中位数: {valid_data.median():.2f}")
            logger.info(f"  最小值: {valid_data.min():.2f}, 最大值: {valid_data.max():.2f}")
            logger.info(f"  有效样本: {len(valid_data)}")
            
            # 选择去极值方法
            if method == 'mad':
                df[f'{factor}_winsorized'] = self.mad_winsorize(valid_data, n=3)
            else:
                df[f'{factor}_winsorized'] = self.sigma_winsorize(valid_data, n=3)
            
            winsorized = df[f'{factor}_winsorized'].dropna()
            logger.info(f"【{factor}】去极值后统计:")
            logger.info(f"  均值: {winsorized.mean():.2f}, 中位数: {winsorized.median():.2f}")
            logger.info(f"  最小值: {winsorized.min():.2f}, 最大值: {winsorized.max():.2f}")
        
        # 4. 行业中性化
        logger.info("\n" + "=" * 70)
        logger.info("开始行业中性化")
        logger.info("=" * 70)
        
        for factor in factors:
            winsor_col = f'{factor}_winsorized'
            neutral_col = f'{factor}_neutral'
            
            if winsor_col not in df.columns:
                continue
            
            # 创建临时DataFrame用于中性化计算
            temp_df = df[['code', 'industry', winsor_col]].copy()
            temp_df = temp_df.dropna(subset=[winsor_col])
            
            if len(temp_df) < 10:
                continue
            
            # 行业中性化
            df.loc[temp_df.index, neutral_col] = self.industry_neutralize(temp_df, winsor_col)
            
            neutralized = df[neutral_col].dropna()
            if len(neutralized) > 0:
                logger.info(f"【{factor}】行业中性化后:")
                logger.info(f"  均值: {neutralized.mean():.4f}, 标准差: {neutralized.std():.4f}")
                logger.info(f"  范围: [{neutralized.min():.2f}, {neutralized.max():.2f}]")
        
        # 5. 生成最终清洗后的值
        df['pe_clean'] = df.get('pe_ratio_neutral', df.get('pe_ratio_winsorized', df['pe_ratio']))
        df['pb_clean'] = df.get('pb_ratio_neutral', df.get('pb_ratio_winsorized', df['pb_ratio']))
        df['roe_clean'] = df.get('roe_neutral', df.get('roe_winsorized', df['roe']))
        
        # 对于估值因子，越低越好，取负值使得高分=低估值
        df['pe_score'] = -df['pe_clean']
        df['pb_score'] = -df['pb_clean']
        # ROE越高越好，保持正值
        df['roe_score'] = df['roe_clean']
        
        logger.info("\n" + "=" * 70)
        logger.info("数据清洗完成")
        logger.info("=" * 70)
        
        return df
    
    def update_database(self, df: pd.DataFrame):
        """更新数据库，添加清洗后的字段"""
        logger.info("\n更新数据库...")
        
        with self.conn.cursor() as cursor:
            # 检查字段是否存在
            cursor.execute("DESCRIBE stock_basic")
            existing_cols = {row[0] for row in cursor.fetchall()}
            
            # 添加清洗后的字段
            new_columns = [
                ('pe_clean', 'FLOAT'),
                ('pb_clean', 'FLOAT'),
                ('roe_clean', 'FLOAT'),
                ('pe_score', 'FLOAT'),
                ('pb_score', 'FLOAT'),
                ('roe_score', 'FLOAT')
            ]
            
            for col_name, col_type in new_columns:
                if col_name not in existing_cols:
                    try:
                        cursor.execute(f"ALTER TABLE stock_basic ADD COLUMN {col_name} {col_type}")
                        logger.info(f"  添加字段: {col_name}")
                    except Exception as e:
                        logger.warning(f"  添加字段 {col_name} 失败: {e}")
            
            self.conn.commit()
            
            # 更新数据
            update_sql = """
                UPDATE stock_basic 
                SET pe_clean = %s, pb_clean = %s, roe_clean = %s,
                    pe_score = %s, pb_score = %s, roe_score = %s
                WHERE code = %s
            """
            
            update_data = []
            for _, row in df.iterrows():
                if pd.notna(row.get('pe_clean')) or pd.notna(row.get('pb_clean')) or pd.notna(row.get('roe_clean')):
                    update_data.append((
                        float(row['pe_clean']) if pd.notna(row.get('pe_clean')) else None,
                        float(row['pb_clean']) if pd.notna(row.get('pb_clean')) else None,
                        float(row['roe_clean']) if pd.notna(row.get('roe_clean')) else None,
                        float(row['pe_score']) if pd.notna(row.get('pe_score')) else None,
                        float(row['pb_score']) if pd.notna(row.get('pb_score')) else None,
                        float(row['roe_score']) if pd.notna(row.get('roe_score')) else None,
                        row['code']
                    ))
            
            # 批量更新
            batch_size = 500
            for i in range(0, len(update_data), batch_size):
                batch = update_data[i:i+batch_size]
                cursor.executemany(update_sql, batch)
                self.conn.commit()
                logger.info(f"  已更新 {min(i+batch_size, len(update_data))}/{len(update_data)} 条记录")
            
            logger.info(f"✅ 数据库更新完成，共更新 {len(update_data)} 条记录")
    
    def generate_report(self, df: pd.DataFrame):
        """生成清洗报告"""
        report_path = f"/root/.openclaw/workspace/股票分析项目/docs/fundamental_cleaning_report_{datetime.now().strftime('%Y%m%d')}.md"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 基本面数据清洗报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## 1. 数据概况\n\n")
            f.write(f"- 总股票数: {len(df)}\n")
            f.write(f"- 有PE数据: {df['pe_ratio'].notna().sum()}\n")
            f.write(f"- 有PB数据: {df['pb_ratio'].notna().sum()}\n")
            f.write(f"- 有ROE数据: {df['roe'].notna().sum()}\n\n")
            
            f.write("## 2. 去极值效果\n\n")
            for factor in ['pe_ratio', 'pb_ratio', 'roe']:
                f.write(f"### {factor.upper()}\n\n")
                f.write("| 指标 | 原始值 | 去极值后 |\n")
                f.write("|------|--------|----------|\n")
                
                orig = df[factor].dropna()
                winsor_col = f'{factor}_winsorized'
                
                if winsor_col not in df.columns:
                    f.write(f"| - | 数据不足，跳过清洗 | - |\n\n")
                    continue
                    
                winsor = df[winsor_col].dropna()
                
                if len(orig) > 0 and len(winsor) > 0:
                    f.write(f"| 均值 | {orig.mean():.2f} | {winsor.mean():.2f} |\n")
                    f.write(f"| 中位数 | {orig.median():.2f} | {winsor.median():.2f} |\n")
                    f.write(f"| 最小值 | {orig.min():.2f} | {winsor.min():.2f} |\n")
                    f.write(f"| 最大值 | {orig.max():.2f} | {winsor.max():.2f} |\n")
                    f.write(f"| 标准差 | {orig.std():.2f} | {winsor.std():.2f} |\n\n")
                else:
                    f.write(f"| - | 数据不足 | - |\n\n")
            
            f.write("## 3. 行业分布\n\n")
            industry_counts = df['industry'].value_counts().head(15)
            f.write("| 行业 | 股票数量 |\n")
            f.write("|------|----------|\n")
            for industry, count in industry_counts.items():
                f.write(f"| {industry} | {count} |\n")
            
            f.write("\n## 4. 清洗后字段说明\n\n")
            f.write("- `pe_clean` / `pb_clean` / `roe_clean`: 清洗并行业中性化后的值\n")
            f.write("- `pe_score` / `pb_score`: 估值得分（负值，越高表示估值越低）\n")
            f.write("- `roe_score`: ROE得分（正值，越高表示质量越好）\n")
        
        logger.info(f"\n📄 报告已保存: {report_path}")
        return report_path


def main():
    """主函数"""
    cleaner = FundamentalDataCleaner()
    
    try:
        cleaner.connect()
        
        # 执行清洗
        df_cleaned = cleaner.clean_fundamental_data(method='mad')
        
        # 更新数据库
        cleaner.update_database(df_cleaned)
        
        # 生成报告
        report_path = cleaner.generate_report(df_cleaned)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 基本面数据清洗完成!")
        logger.info(f"   报告: {report_path}")
        logger.info("=" * 70)
        
    finally:
        cleaner.close()


if __name__ == '__main__':
    main()
