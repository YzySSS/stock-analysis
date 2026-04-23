#!/usr/bin/env python3
"""
V13 Hybrid 参数优化器
======================
使用网格搜索优化权重和阈值
目标：最大化卡尔玛比率（年化收益/最大回撤）

优化参数：
1. 权重：trend / volatility / volume / breadth
2. 阈值：strong_trend_threshold / range_bound_threshold
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from itertools import product
from typing import Dict, List, Tuple
import pymysql

from v13_hybrid_market_detector import MarketEnvironmentDetector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class V13HybridOptimizer:
    """V13 Hybrid 参数优化器"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def optimize_weights_and_thresholds(
        self,
        start_date: str,
        end_date: str,
        index_code: str = '000001'
    ) -> Dict:
        """
        网格搜索优化权重和阈值
        
        优化目标：最大化卡尔玛比率
        约束条件：
        - 权重和为1
        - 切换频率不宜过高（平均每月<2次）
        """
        logger.info("=" * 80)
        logger.info("V13 Hybrid 参数优化")
        logger.info(f"回测区间: {start_date} 至 {end_date}")
        logger.info("=" * 80)
        
        # 定义搜索空间
        # 权重搜索（简化版，保持比例关系）
        weight_configs = [
            {'trend': 0.45, 'volatility': 0.25, 'volume': 0.20, 'breadth': 0.10},  # 趋势主导
            {'trend': 0.40, 'volatility': 0.30, 'volume': 0.20, 'breadth': 0.10},  # DeepSeek推荐
            {'trend': 0.35, 'volatility': 0.35, 'volume': 0.20, 'breadth': 0.10},  # 平衡型
            {'trend': 0.35, 'volatility': 0.30, 'volume': 0.25, 'breadth': 0.10},  # 成交量加权
            {'trend': 0.40, 'volatility': 0.25, 'volume': 0.20, 'breadth': 0.15},  # 宽度加权
        ]
        
        # 阈值搜索
        strong_trend_thresholds = [65, 68, 70, 72, 75]
        range_bound_thresholds = [35, 38, 40, 42, 45]
        
        # 确保 strong > range
        threshold_pairs = [
            (st, rb) for st in strong_trend_thresholds 
            for rb in range_bound_thresholds 
            if st - rb >= 20  # 至少20分差距
        ]
        
        logger.info(f"权重配置数: {len(weight_configs)}")
        logger.info(f"阈值组合数: {len(threshold_pairs)}")
        logger.info(f"总搜索空间: {len(weight_configs) * len(threshold_pairs)}")
        
        # 获取历史数据用于回测
        self.connect_db()
        
        sql = """
        SELECT trade_date, close, high, low, volume,
               (close - LAG(close, 1) OVER (ORDER BY trade_date)) / LAG(close, 1) OVER (ORDER BY trade_date) as daily_return
        FROM stock_kline 
        WHERE code = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        self.cursor.execute(sql, (index_code, start_date, end_date))
        index_data = pd.DataFrame(self.cursor.fetchall())
        
        self.close_db()
        
        if len(index_data) == 0:
            logger.error("未获取到历史数据")
            return {}
        
        logger.info(f"历史数据条数: {len(index_data)}")
        
        # 存储结果
        results = []
        
        # 网格搜索
        for weight_idx, weights in enumerate(weight_configs):
            for threshold_idx, (strong_th, range_th) in enumerate(threshold_pairs):
                
                logger.info(f"\n测试配置 [{weight_idx+1}/{len(weight_configs)}, {threshold_idx+1}/{len(threshold_pairs)}]")
                logger.info(f"  权重: {weights}")
                logger.info(f"  阈值: strong={strong_th}, range={range_th}")
                
                # 创建检测器
                detector = MarketEnvironmentDetector(weights=weights)
                detector.thresholds = {
                    'strong_trend': strong_th,
                    'weak_trend': (strong_th + range_th) // 2,
                    'range_bound': range_th,
                    'bear': 0
                }
                
                # 模拟回测
                try:
                    metrics = self._simulate_backtest(
                        detector, index_data, 
                        weights, (strong_th, range_th)
                    )
                    
                    results.append({
                        'weights': json.dumps(weights),
                        'strong_threshold': strong_th,
                        'range_threshold': range_th,
                        **metrics
                    })
                    
                    logger.info(f"  卡尔玛比率: {metrics['calmar_ratio']:.2f}")
                    
                except Exception as e:
                    logger.error(f"  回测失败: {e}")
        
        # 转换为DataFrame并排序
        df_results = pd.DataFrame(results)
        df_results = df_results.sort_values('calmar_ratio', ascending=False)
        
        # 保存结果
        output_file = '/root/.openclaw/workspace/股票分析项目/backtest_results/v13_hybrid_optimization_results.csv'
        df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n优化结果已保存: {output_file}")
        
        # 输出最优配置
        if len(df_results) > 0:
            best = df_results.iloc[0]
            logger.info("\n" + "=" * 80)
            logger.info("最优配置:")
            logger.info("=" * 80)
            logger.info(f"权重: {best['weights']}")
            logger.info(f"强趋势阈值: {best['strong_threshold']}")
            logger.info(f"震荡市阈值: {best['range_threshold']}")
            logger.info(f"卡尔玛比率: {best['calmar_ratio']:.2f}")
            logger.info(f"年化收益: {best['annual_return']:.2f}%")
            logger.info(f"最大回撤: {best['max_drawdown']:.2f}%")
            logger.info(f"切换次数: {best['switches']}")
        
        return df_results.to_dict('records')
    
    def _simulate_backtest(
        self, 
        detector: MarketEnvironmentDetector,
        index_data: pd.DataFrame,
        weights: Dict,
        thresholds: Tuple[int, int]
    ) -> Dict:
        """
        模拟回测，计算卡尔玛比率
        
        简化逻辑：
        - 强趋势市：持有指数，获得市场收益
        - 震荡市：空仓，获得0收益
        - 熊市：空仓，获得0收益
        """
        strong_th, range_th = thresholds
        
        # 模拟每日持仓状态
        positions = []  # 1=持仓, 0=空仓
        prev_regime = None
        switches = 0
        
        for idx, row in index_data.iterrows():
            date = row['trade_date']
            
            try:
                # 检测市场状态
                regime, score, _, _ = detector.detect(date)
                
                # 决定持仓
                if regime in ['strong_trend', 'weak_trend']:
                    position = 1  # 持仓
                else:
                    position = 0  # 空仓
                
                positions.append(position)
                
                # 统计切换次数
                if prev_regime and prev_regime != regime:
                    switches += 1
                prev_regime = regime
                
            except Exception as e:
                # 检测失败时保持上一日状态
                positions.append(positions[-1] if positions else 0)
        
        # 计算策略收益
        index_data['position'] = positions
        index_data['strategy_return'] = index_data['position'] * index_data['daily_return'].fillna(0)
        
        # 计算累计收益
        index_data['cum_return'] = (1 + index_data['strategy_return']).cumprod() - 1
        
        # 计算指标
        total_return = index_data['cum_return'].iloc[-1] * 100
        
        # 年化收益
        n_days = len(index_data)
        annual_return = ((1 + total_return/100) ** (252/n_days) - 1) * 100 if n_days > 0 else 0
        
        # 最大回撤
        cum_returns = (1 + index_data['strategy_return']).cumprod()
        running_max = cum_returns.expanding().max()
        drawdown = (cum_returns - running_max) / running_max
        max_drawdown = drawdown.min() * 100
        
        # 卡尔玛比率
        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # 切换频率检查（平均每月<2次）
        n_months = n_days / 21
        switches_per_month = switches / n_months if n_months > 0 else 0
        
        # 如果切换太频繁，惩罚卡尔玛比率
        if switches_per_month > 3:
            calmar_ratio *= 0.8
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'calmar_ratio': calmar_ratio,
            'switches': switches,
            'switches_per_month': switches_per_month
        }
    
    def analyze_historical_regimes(
        self,
        start_date: str,
        end_date: str,
        index_code: str = '000001'
    ):
        """
        分析历史市场状态分布
        用于验证检测器是否符合2024-2025年的实际情况
        """
        logger.info("=" * 80)
        logger.info("历史市场状态分析")
        logger.info(f"区间: {start_date} 至 {end_date}")
        logger.info("=" * 80)
        
        # 使用默认配置进行检测
        detector = MarketEnvironmentDetector()
        df = detector.batch_detect(start_date, end_date, index_code)
        
        # 统计各年度分布
        df['year'] = pd.to_datetime(df['date']).dt.year
        
        print("\n各年度市场状态分布:")
        print("-" * 80)
        
        for year in sorted(df['year'].unique()):
            year_data = df[df['year'] == year]
            stats = year_data['regime'].value_counts()
            total = len(year_data)
            
            print(f"\n{year}年 (共{total}个交易日):")
            for regime, count in stats.items():
                pct = count / total * 100
                print(f"  {regime:15s}: {count:3d}天 ({pct:5.1f}%)")
            
            # 平均得分
            avg_score = year_data['score'].mean()
            print(f"  平均得分: {avg_score:.1f}")
        
        # 保存详细结果
        output_file = f'/root/.openclaw/workspace/股票分析项目/backtest_results/v13_hybrid_regime_{start_date[:4]}_{end_date[:4]}.csv'
        detector.save_results(df, output_file)
        
        return df


def main():
    """主函数"""
    optimizer = V13HybridOptimizer()
    
    print("\n" + "=" * 80)
    print("V13 Hybrid 市场环境检测系统")
    print("=" * 80)
    
    # 步骤1：分析历史市场状态
    print("\n【步骤1】分析2024-2025年历史市场状态...")
    df_regimes = optimizer.analyze_historical_regimes('2024-01-01', '2025-12-31')
    
    # 步骤2：优化参数
    print("\n【步骤2】网格搜索最优权重和阈值...")
    print("(这将需要一些时间，请耐心等待)")
    
    results = optimizer.optimize_weights_and_thresholds('2024-01-01', '2025-12-31')
    
    print("\n" + "=" * 80)
    print("优化完成！")
    print("=" * 80)
    print("\n后续步骤:")
    print("1. 查看优化结果: backtest_results/v13_hybrid_optimization_results.csv")
    print("2. 选择前3名配置进行详细回测验证")
    print("3. 在2026年数据上进行样本外测试")
    print("4. 确定最终参数配置")


if __name__ == '__main__':
    main()
