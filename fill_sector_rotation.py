#!/usr/bin/env python3
"""
板块轮动数据补充脚本
==================
从 stock_kline 和 stock_basic 计算板块轮动数据
"""

import os
import sys
import pymysql
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


def calculate_sector_rotation(date_str: str):
    """计算指定日期的板块轮动数据"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        
        with conn.cursor() as cursor:
            # 获取所有行业
            cursor.execute('''
                SELECT DISTINCT industry FROM stock_basic 
                WHERE industry IS NOT NULL AND industry != ''
            ''')
            industries = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"{date_str}: 共 {len(industries)} 个行业")
            
            sector_data = []
            
            for idx, industry in enumerate(industries):
                # 获取该行业所有股票的涨跌幅
                cursor.execute('''
                    SELECT 
                        sb.code,
                        sb.name,
                        sk.pct_change,
                        sk.volume,
                        sk.amount
                    FROM stock_basic sb
                    JOIN stock_kline sk ON sb.code = sk.code
                    WHERE sb.industry = %s 
                    AND sk.trade_date = %s
                    AND sb.is_delisted = 0
                ''', (industry, date_str))
                
                stocks = cursor.fetchall()
                if not stocks:
                    continue
                
                # 计算行业指标
                changes = [s[2] for s in stocks if s[2] is not None]
                volumes = [s[3] for s in stocks if s[3] is not None]
                
                if not changes:
                    continue
                
                avg_change = sum(changes) / len(changes)
                up_count = sum(1 for c in changes if c > 0)
                down_count = sum(1 for c in changes if c < 0)
                
                # 动量得分 = 平均涨跌幅 * 10 + 50（映射到0-100）
                momentum_score = avg_change * 10 + 50
                momentum_score = max(0, min(100, momentum_score))
                
                # 强度得分 = 上涨股票比例 * 100
                strength_score = (up_count / len(changes)) * 100 if changes else 50
                
                # 找出领涨股（涨幅前3）
                stock_changes = [(s[0], s[1], s[2]) for s in stocks if s[2] is not None]
                stock_changes.sort(key=lambda x: x[2], reverse=True)
                leading = ', '.join([f"{s[0]}({s[2]:.1f}%)" for s in stock_changes[:3]])
                
                sector_data.append({
                    'name': industry,
                    'strength': strength_score,
                    'momentum': momentum_score,
                    'leading': leading,
                    'avg_change': avg_change,
                    'rank': 0  # 后面计算
                })
            
            # 按动量得分排序计算排名
            sector_data.sort(key=lambda x: x['momentum'], reverse=True)
            for idx, data in enumerate(sector_data):
                data['rank'] = idx + 1
            
            # 写入数据库
            inserted = 0
            for data in sector_data:
                try:
                    cursor.execute('''
                        INSERT INTO sector_rotation 
                        (trade_date, sector_name, strength_score, momentum_score, 
                         leading_stocks, avg_change, rank_position, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE
                        strength_score = VALUES(strength_score),
                        momentum_score = VALUES(momentum_score),
                        leading_stocks = VALUES(leading_stocks),
                        avg_change = VALUES(avg_change),
                        rank_position = VALUES(rank_position)
                    ''', (
                        date_str, data['name'], data['strength'], data['momentum'],
                        data['leading'], data['avg_change'], data['rank']
                    ))
                    inserted += 1
                except Exception as e:
                    logger.warning(f"写入失败 {data['name']}: {e}")
            
            conn.commit()
            logger.info(f"{date_str}: 成功写入 {inserted} 个行业数据")
        
        conn.close()
        return True
    except Exception as e:
        logger.error(f"计算板块数据失败 {date_str}: {e}")
        return False


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='补充板块轮动数据')
    parser.add_argument('--days', type=int, default=30, help='补充最近N天的数据')
    args = parser.parse_args()
    
    # 生成日期列表
    end_date = datetime.now()
    dates = []
    for i in range(args.days):
        d = end_date - timedelta(days=i)
        dates.append(d.strftime('%Y-%m-%d'))
    
    dates.reverse()  # 从旧到新
    
    logger.info(f"计划处理: {len(dates)} 天")
    
    success_count = 0
    for date_str in dates:
        if calculate_sector_rotation(date_str):
            success_count += 1
    
    logger.info(f"完成: {success_count}/{len(dates)} 天")


if __name__ == "__main__":
    main()
