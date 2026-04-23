#!/usr/bin/env python3
"""
策略数据源
==========
为选股策略提供数据访问接口
"""

import logging
from typing import List, Dict, Optional
import pymysql
from config import DB_CONFIG

logger = logging.getLogger(__name__)


class StrategyDataSource:
    """策略数据源 - 从MySQL数据库获取数据"""
    
    def __init__(self):
        self.conn = None
    
    def _get_conn(self):
        """获取数据库连接"""
        if self.conn is None:
            self.conn = pymysql.connect(**DB_CONFIG)
        return self.conn
    
    def get_technical_indicators(self, code: str, date: str) -> Dict:
        """
        获取技术指标数据
        
        Returns:
            {
                'ma_trend': 'up'/'down',
                'macd_signal': 'golden_cross'/'death_cross',
                'volume_ratio': float,
                'rsi': float
            }
        """
        try:
            conn = self._get_conn()
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                # 获取最近20天数据计算指标
                cursor.execute('''
                    SELECT * FROM stock_kline 
                    WHERE code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC 
                    LIMIT 20
                ''', (code, date))
                rows = cursor.fetchall()
                
                if len(rows) < 20:
                    return {}
                
                # 计算均线趋势
                closes = [r['close'] for r in reversed(rows)]
                ma5 = sum(closes[-5:]) / 5
                ma10 = sum(closes[-10:]) / 10
                ma20 = sum(closes[-20:]) / 20
                
                ma_trend = 'up' if ma5 > ma10 > ma20 else 'down'
                
                # 计算成交量比率
                recent_vol = sum(r['volume'] for r in rows[:5])
                past_vol = sum(r['volume'] for r in rows[5:15])
                volume_ratio = recent_vol / past_vol if past_vol > 0 else 1.0
                
                # 简化RSI计算
                gains = [closes[i] - closes[i-1] for i in range(1, len(closes)) if closes[i] > closes[i-1]]
                losses = [closes[i-1] - closes[i] for i in range(1, len(closes)) if closes[i] < closes[i-1]]
                avg_gain = sum(gains) / len(gains) if gains else 0
                avg_loss = sum(losses) / len(losses) if losses else 0
                rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 50
                
                return {
                    'ma_trend': ma_trend,
                    'macd_signal': 'golden_cross' if ma_trend == 'up' else 'death_cross',
                    'volume_ratio': round(volume_ratio, 2),
                    'rsi': round(rsi, 2)
                }
        except Exception as e:
            logger.debug(f"获取技术指标失败 {code}: {e}")
            return {}
    
    def get_recent_prices(self, code: str, days: int = 20) -> List[Dict]:
        """
        获取最近价格数据
        
        Returns:
            [{date, open, high, low, close, volume}, ...]
        """
        try:
            conn = self._get_conn()
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute('''
                    SELECT trade_date as date, open, high, low, close, volume
                    FROM stock_kline 
                    WHERE code = %s
                    ORDER BY trade_date DESC 
                    LIMIT %s
                ''', (code, days))
                rows = cursor.fetchall()
                return list(reversed(rows))
        except Exception as e:
            logger.debug(f"获取价格数据失败 {code}: {e}")
            return []
    
    def get_index_data(self, code: str, date: str) -> Dict:
        """
        获取指数数据
        
        Args:
            code: 指数代码，如 '000001'（上证指数）
            date: 日期
        """
        try:
            conn = self._get_conn()
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                # 获取指数当日数据
                cursor.execute('''
                    SELECT * FROM stock_kline 
                    WHERE code = %s AND trade_date = %s
                ''', (code, date))
                row = cursor.fetchone()
                
                if not row:
                    return {}
                
                # 获取近期数据计算MA
                cursor.execute('''
                    SELECT close, volume FROM stock_kline 
                    WHERE code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC 
                    LIMIT 20
                ''', (code, date))
                rows = cursor.fetchall()
                
                closes = [r['close'] for r in reversed(rows)]
                volumes = [r['volume'] for r in rows]
                
                ma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else closes[-1] if closes else 0
                ma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else ma5
                ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else ma10
                
                # 成交量比率
                recent_vol = sum(volumes[:5]) / 5 if volumes else 1
                past_vol = sum(volumes[5:15]) / 10 if len(volumes) > 10 else recent_vol
                volume_ratio = recent_vol / past_vol if past_vol > 0 else 1.0
                
                return {
                    'code': code,
                    'date': date,
                    'close': row['close'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'volume': row['volume'],
                    'ma5': ma5,
                    'ma10': ma10,
                    'ma20': ma20,
                    'volume_ratio': round(volume_ratio, 2)
                }
        except Exception as e:
            logger.debug(f"获取指数数据失败 {code}: {e}")
            return {}
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            self.conn = None


class SectorFactor:
    """板块轮动因子计算器 - 从数据库查询"""
    
    def calculate(self, code: str, date: str) -> float:
        """计算板块轮动得分 (0-100)"""
        try:
            import pymysql
            from config import DB_CONFIG
            
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                # 首先获取股票所属行业
                cursor.execute('''
                    SELECT industry FROM stock_basic WHERE code = %s
                ''', (code,))
                row = cursor.fetchone()
                if not row or not row[0]:
                    return 50.0
                
                industry = row[0]
                
                # 查询该行业在当天的表现
                cursor.execute('''
                    SELECT momentum_score, rank_pct 
                    FROM sector_rotation 
                    WHERE sector_name = %s AND date = %s
                    ORDER BY created_at DESC 
                    LIMIT 1
                ''', (industry, date))
                
                row = cursor.fetchone()
                if row:
                    # 使用动量分数，如果没有则使用排名百分位
                    score = row[0] if row[0] is not None else (row[1] * 100 if row[1] else 50)
                    return float(score)
                else:
                    # 查询不到数据时返回中性分数
                    logger.debug(f"无板块数据 {industry} {date}")
                    return 50.0
        except Exception as e:
            logger.debug(f"计算板块因子失败 {code}: {e}")
            return 50.0


class SentimentFactor:
    """情绪面因子计算器 - 从数据库查询"""
    
    def calculate(self, code: str, name: str, date: str) -> float:
        """计算情绪面得分 (0-100)"""
        try:
            import pymysql
            from config import DB_CONFIG
            
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                # 查询 sentiment_daily 表（正确的表名）
                cursor.execute('''
                    SELECT 
                        sentiment_score,
                        news_count,
                        positive_news,
                        negative_news,
                        sentiment_type
                    FROM sentiment_daily 
                    WHERE code = %s AND trade_date = %s
                ''', (code, date))
                
                row = cursor.fetchone()
                if not row or row[0] is None:
                    logger.debug(f"无舆情数据 {code} {date}")
                    return 50.0
                
                sentiment_score = float(row[0])  # 0-100 的范围
                news_count = row[1] or 0
                positive_news = row[2] or 0
                negative_news = row[3] or 0
                sentiment_type = row[4]  # 0=负面, 1=中性, 2=正面
                
                if news_count == 0:
                    return 50.0
                
                # 基础分数就是 sentiment_score
                base_score = sentiment_score
                
                # 根据新闻数量调整置信度
                # 新闻越多，分数越可信
                confidence = min(1.0, news_count / 5)  # 最多5条新闻达到满置信度
                
                # 调整后的分数 = 中性 + (原始分数 - 中性) * 置信度
                adjusted_score = 50 + (base_score - 50) * confidence
                
                # 根据正负比例微调
                if positive_news > negative_news * 2:
                    adjusted_score = min(100, adjusted_score + 3)  # 明显正面
                elif negative_news > positive_news * 2:
                    adjusted_score = max(0, adjusted_score - 3)  # 明显负面
                
                return max(0, min(100, adjusted_score))
        except Exception as e:
            logger.debug(f"计算情绪因子失败 {code}: {e}")
            return 50.0


class CapitalFactor:
    """资金流向因子计算器"""
    
    def calculate(self, code: str, date: str) -> float:
        """计算资金流向得分 (0-100)"""
        # 简化实现 - 实际应根据资金流数据计算
        return 50.0


class RiskFactor:
    """风险因子计算器"""
    
    def calculate(self, code: str, date: str) -> float:
        """计算风险得分 (0-100，越低越好)"""
        # 简化实现 - 实际应根据波动率等指标计算
        return 30.0
