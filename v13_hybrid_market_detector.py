#!/usr/bin/env python3
"""
V13 Hybrid 市场环境检测器
============================
基于综合指数法的市场环境判断系统
权重：趋势40% / 波动率30% / 成交量20% / 市场宽度10%

推荐阈值（需回测优化）：
- 强趋势市：>= 70分，使用V13原版
- 弱趋势市：55-69分，使用V13保守版
- 震荡市：40-54分，使用V12精简版
- 熊市：< 40分，空仓或极小仓位
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, List
import pymysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/.openclaw/workspace/股票分析项目/logs/v13_hybrid_detector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}


class MarketEnvironmentDetector:
    """
    市场环境检测器 - 综合指数法
    """
    
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        初始化检测器
        
        Args:
            weights: 自定义权重，默认使用DeepSeek推荐权重
        """
        # DeepSeek推荐权重
        self.weights = weights or {
            'trend': 0.40,       # 40% - 趋势维度
            'volatility': 0.30,  # 30% - 波动率维度
            'volume': 0.20,      # 20% - 成交量维度
            'breadth': 0.10      # 10% - 市场宽度维度
        }
        
        # 阈值配置（可通过回测优化）
        self.thresholds = {
            'strong_trend': 70,   # 强趋势市
            'weak_trend': 55,     # 弱趋势市
            'range_bound': 40,    # 震荡市
            'bear': 0             # 熊市
        }
        
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """连接数据库"""
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def detect(self, date: str, index_code: str = '000001') -> Tuple[str, float, str, Dict]:
        """
        检测市场环境
        
        Args:
            date: 日期 (YYYY-MM-DD)
            index_code: 指数代码，默认上证指数
            
        Returns:
            regime: 市场状态 (strong_trend/weak_trend/range_bound/bear)
            score: 0-100分
            action: 建议操作
            details: 各维度详细得分
        """
        logger.info(f"[{date}] 开始检测市场环境...")
        
        try:
            self.connect_db()
            
            # 计算各维度得分
            scores = {
                'trend': self._calc_trend_score(date, index_code),
                'volatility': self._calc_volatility_score(date, index_code),
                'volume': self._calc_volume_score(date, index_code),
                'breadth': self._calc_breadth_score(date)
            }
            
            self.close_db()
            
            # 加权综合得分
            total_score = sum(
                scores[k] * self.weights[k]
                for k in scores
            )
            
            # 判断市场状态
            regime, action = self._determine_regime(total_score)
            
            details = {
                'total_score': round(total_score, 2),
                'dimension_scores': {k: round(v, 2) for k, v in scores.items()},
                'weights': self.weights,
                'thresholds': self.thresholds
            }
            
            logger.info(f"[{date}] 检测结果: {regime} | 得分: {total_score:.1f} | {action}")
            
            return regime, total_score, action, details
            
        except Exception as e:
            logger.error(f"[{date}] 检测失败: {e}")
            if self.conn:
                self.close_db()
            raise
    
    def _calc_trend_score(self, date: str, index_code: str) -> float:
        """
        趋势维度评分 (0-100分)
        构成：均线排列分50% + 趋势强度分50%
        """
        try:
            # 获取价格数据
            sql = """
            SELECT close FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 70
            """
            self.cursor.execute(sql, (index_code, date))
            rows = self.cursor.fetchall()
            
            if len(rows) < 60:
                logger.warning(f"[{date}] 数据不足，无法计算趋势得分")
                return 50.0
            
            closes = [float(r['close']) for r in rows]
            current_price = closes[0]
            
            # 计算均线
            ma5 = np.mean(closes[:5])
            ma10 = np.mean(closes[:10])
            ma20 = np.mean(closes[:20])
            ma60 = np.mean(closes[:60])
            
            # 1. 均线排列分 (50分)
            alignment_score = 0
            if current_price > ma5:
                alignment_score += 12.5
            if ma5 > ma10:
                alignment_score += 12.5
            if ma10 > ma20:
                alignment_score += 12.5
            if ma20 > ma60:
                alignment_score += 12.5
            
            # 2. 趋势强度分 (50分)
            # 使用MA20相对于MA60的变化率
            trend_strength = (ma20 - ma60) / ma60 * 100
            # 归一化到0-50分
            if trend_strength > 5:  # 强势上涨
                strength_score = 50
            elif trend_strength > 2:
                strength_score = 40
            elif trend_strength > 0:
                strength_score = 30
            elif trend_strength > -2:
                strength_score = 20
            else:
                strength_score = 10
            
            total = alignment_score + strength_score
            return min(100, max(0, total))
            
        except Exception as e:
            logger.error(f"计算趋势得分失败: {e}")
            return 50.0
    
    def _calc_volatility_score(self, date: str, index_code: str) -> float:
        """
        波动率维度评分 (0-100分)
        构成：ATR比率50% + 布林带宽度50%
        理念：适中波动率得分高，过高或过低得分低
        """
        try:
            # 获取历史数据
            sql = """
            SELECT high, low, close FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 70
            """
            self.cursor.execute(sql, (index_code, date))
            rows = self.cursor.fetchall()
            
            if len(rows) < 60:
                return 50.0
            
            highs = [float(r['high']) for r in rows]
            lows = [float(r['low']) for r in rows]
            closes = [float(r['close']) for r in rows]
            
            # 1. ATR计算 (20日 vs 60日)
            def calc_atr(highs, lows, closes, period):
                tr_list = []
                for i in range(period):
                    high = highs[i]
                    low = lows[i]
                    close_prev = closes[i + 1] if i + 1 < len(closes) else closes[i]
                    tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
                    tr_list.append(tr)
                return np.mean(tr_list)
            
            atr_20 = calc_atr(highs, lows, closes, 20)
            atr_60 = calc_atr(highs, lows, closes, 60) if len(highs) >= 60 else atr_20
            
            atr_ratio = atr_20 / atr_60 if atr_60 > 0 else 1.0
            
            # ATR比率评分 (50分)
            # 理想范围：0.8-1.3（波动适中且略微扩张）
            if 0.9 <= atr_ratio <= 1.3:  # 波动扩张，趋势信号
                atr_score = 50
            elif 0.7 <= atr_ratio < 0.9:  # 波动收缩
                atr_score = 30
            elif atr_ratio < 0.7:  # 过度收缩=震荡
                atr_score = 15
            elif 1.3 < atr_ratio <= 1.8:  # 波动过大
                atr_score = 35
            else:  # 波动极度扩大=恐慌
                atr_score = 20
            
            # 2. 布林带宽度评分 (50分)
            ma20 = np.mean(closes[:20])
            std20 = np.std(closes[:20])
            upper_band = ma20 + 2 * std20
            lower_band = ma20 - 2 * std20
            bandwidth = (upper_band - lower_band) / ma20
            
            # 理想带宽：4%-8%
            if 0.04 <= bandwidth <= 0.08:
                band_score = 50
            elif bandwidth < 0.03:  # 过度收敛
                band_score = 20
            elif bandwidth > 0.12:  # 过度发散
                band_score = 25
            else:
                band_score = 40
            
            return atr_score + band_score
            
        except Exception as e:
            logger.error(f"计算波动率得分失败: {e}")
            return 50.0
    
    def _calc_volume_score(self, date: str, index_code: str) -> float:
        """
        成交量维度评分 (0-100分)
        理念：温和放量得分高，缩量或异常放量得分低
        """
        try:
            # 获取成交量数据
            sql = """
            SELECT volume, close FROM stock_kline 
            WHERE code = %s AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 70
            """
            self.cursor.execute(sql, (index_code, date))
            rows = self.cursor.fetchall()
            
            if len(rows) < 60:
                return 50.0
            
            volumes = [float(r['volume']) for r in rows]
            closes = [float(r['close']) for r in rows]
            
            # 1. 成交量比率 (当前20日均量 / 前20-60日均量)
            vol_20 = np.mean(volumes[:20])
            vol_60 = np.mean(volumes[:60])
            vol_ratio = vol_20 / vol_60 if vol_60 > 0 else 1.0
            
            # 成交量比率评分 (50分)
            if 1.0 <= vol_ratio <= 1.5:  # 温和放量
                ratio_score = 50
            elif 0.8 <= vol_ratio < 1.0:  # 正常
                ratio_score = 40
            elif 0.5 <= vol_ratio < 0.8:  # 缩量
                ratio_score = 25
            elif vol_ratio < 0.5:  # 严重缩量
                ratio_score = 15
            elif 1.5 < vol_ratio <= 2.5:  # 明显放量
                ratio_score = 35
            else:  # 异常放量
                ratio_score = 20
            
            # 2. 量价配合评分 (50分)
            # 计算上涨日 vs 下跌日的成交量
            up_vol = []
            down_vol = []
            for i in range(min(20, len(closes) - 1)):
                if closes[i] > closes[i + 1]:
                    up_vol.append(volumes[i])
                else:
                    down_vol.append(volumes[i])
            
            if up_vol and down_vol:
                avg_up_vol = np.mean(up_vol)
                avg_down_vol = np.mean(down_vol)
                up_down_ratio = avg_up_vol / avg_down_vol if avg_down_vol > 0 else 1.0
                
                # 上涨放量得分高
                if up_down_ratio > 1.5:  # 明显上涨放量
                    obv_score = 50
                elif up_down_ratio > 1.2:
                    obv_score = 40
                elif up_down_ratio > 0.9:
                    obv_score = 30
                else:  # 下跌放量
                    obv_score = 20
            else:
                obv_score = 30
            
            return ratio_score + obv_score
            
        except Exception as e:
            logger.error(f"计算成交量得分失败: {e}")
            return 50.0
    
    def _calc_breadth_score(self, date: str) -> float:
        """
        市场宽度维度评分 (0-100分)
        理念：上涨股票比例高且持续，得分高
        """
        try:
            # 获取当日上涨股票比例
            sql = """
            SELECT 
                COUNT(CASE WHEN close > open THEN 1 END) as up_count,
                COUNT(*) as total_count
            FROM stock_kline 
            WHERE trade_date = %s
            AND code NOT REGEXP '^688'  -- 排除科创板
            AND code NOT REGEXP '^8'    -- 排除北交所
            AND code NOT REGEXP '^4'    -- 排除新三板
            """
            self.cursor.execute(sql, (date,))
            row = self.cursor.fetchone()
            
            if not row or row['total_count'] == 0:
                return 50.0
            
            up_ratio = row['up_count'] / row['total_count']
            
            # 上涨比例评分 (50分)
            if up_ratio > 0.7:  # 普涨
                breadth_score = 50
            elif up_ratio > 0.6:
                breadth_score = 40
            elif up_ratio > 0.5:
                breadth_score = 30
            elif up_ratio > 0.4:
                breadth_score = 20
            else:  # 普跌
                breadth_score = 10
            
            # 获取近20日平均上涨比例
            sql_20d = """
            SELECT 
                AVG(up_ratio) as avg_up_ratio
            FROM (
                SELECT trade_date,
                    COUNT(CASE WHEN close > open THEN 1 END) / COUNT(*) as up_ratio
                FROM stock_kline 
                WHERE trade_date <= %s
                AND trade_date >= DATE_SUB(%s, INTERVAL 20 DAY)
                AND code NOT REGEXP '^688'
                AND code NOT REGEXP '^8'
                AND code NOT REGEXP '^4'
                GROUP BY trade_date
            ) t
            """
            self.cursor.execute(sql_20d, (date, date))
            row_20d = self.cursor.fetchone()
            
            trend_score = 25  # 默认
            if row_20d and row_20d['avg_up_ratio']:
                avg_up = row_20d['avg_up_ratio']
                if avg_up > 0.6:  # 持续强势
                    trend_score = 50
                elif avg_up > 0.55:
                    trend_score = 40
                elif avg_up > 0.45:
                    trend_score = 30
                elif avg_up > 0.4:
                    trend_score = 20
                else:
                    trend_score = 10
            
            return breadth_score + trend_score
            
        except Exception as e:
            logger.error(f"计算市场宽度得分失败: {e}")
            return 50.0
    
    def _determine_regime(self, score: float) -> Tuple[str, str]:
        """
        根据得分判断市场状态
        
        Returns:
            regime: 市场状态
            action: 建议操作
        """
        if score >= self.thresholds['strong_trend']:
            return 'strong_trend', 'V13原版，正常仓位(5只)，阈值55分，止损-8%'
        elif score >= self.thresholds['weak_trend']:
            return 'weak_trend', 'V13保守版，降低仓位(3只)，阈值60分，止损-8%'
        elif score >= self.thresholds['range_bound']:
            return 'range_bound', 'V12精简版，轻仓(2只)，阈值60分，止损-5%'
        else:
            return 'bear', '空仓或极小仓位(1只)，阈值65分，严格止损-5%'
    
    def batch_detect(self, start_date: str, end_date: str, index_code: str = '000001') -> pd.DataFrame:
        """
        批量检测一段时间内的市场环境
        
        Returns:
            DataFrame包含每日检测结果
        """
        self.connect_db()
        
        # 获取交易日列表
        sql = """
        SELECT DISTINCT trade_date FROM stock_kline 
        WHERE code = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        self.cursor.execute(sql, (index_code, start_date, end_date))
        dates = [str(r['trade_date']) for r in self.cursor.fetchall()]
        
        self.close_db()
        
        results = []
        for i, date in enumerate(dates):
            if i % 50 == 0:
                logger.info(f"批量检测进度: {i}/{len(dates)} ({i/len(dates)*100:.1f}%)")
            
            try:
                regime, score, action, details = self.detect(date, index_code)
                results.append({
                    'date': date,
                    'regime': regime,
                    'score': score,
                    'action': action,
                    **details['dimension_scores']
                })
            except Exception as e:
                logger.error(f"[{date}] 检测失败: {e}")
                results.append({
                    'date': date,
                    'regime': 'error',
                    'score': 0,
                    'action': str(e),
                    'trend': 0, 'volatility': 0, 'volume': 0, 'breadth': 0
                })
        
        return pd.DataFrame(results)
    
    def save_results(self, df: pd.DataFrame, output_file: str):
        """保存检测结果"""
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"结果已保存: {output_file}")
        
        # 统计信息
        stats = df['regime'].value_counts()
        logger.info("市场状态分布:")
        for regime, count in stats.items():
            pct = count / len(df) * 100
            logger.info(f"  {regime}: {count}天 ({pct:.1f}%)")


def main():
    """主函数 - 测试检测器"""
    detector = MarketEnvironmentDetector()
    
    # 测试单个日期
    test_dates = ['2024-01-15', '2024-06-15', '2025-03-15', '2025-09-15']
    
    print("\n" + "=" * 80)
    print("V13 Hybrid 市场环境检测器 - 测试")
    print("=" * 80)
    
    for date in test_dates:
        try:
            regime, score, action, details = detector.detect(date)
            print(f"\n📅 {date}")
            print(f"   市场状态: {regime}")
            print(f"   综合得分: {score:.1f}/100")
            print(f"   建议操作: {action}")
            print(f"   各维度得分:")
            for dim, s in details['dimension_scores'].items():
                w = detector.weights[dim]
                print(f"      {dim:12s}: {s:5.1f}分 (权重{w*100:.0f}%)")
        except Exception as e:
            print(f"\n📅 {date} - 检测失败: {e}")
    
    print("\n" + "=" * 80)
    print("批量检测示例 (2024年1月-3月)")
    print("=" * 80)
    
    # 批量检测
    df = detector.batch_detect('2024-01-01', '2024-03-31')
    
    # 显示前10条
    print("\n前10天检测结果:")
    print(df.head(10).to_string(index=False))
    
    # 保存结果
    output_file = '/root/.openclaw/workspace/股票分析项目/backtest_results/v13_hybrid_regime_2024q1.csv'
    detector.save_results(df, output_file)
    
    print("\n" + "=" * 80)
    print("检测器测试完成！")
    print("=" * 80)


if __name__ == '__main__':
    main()
