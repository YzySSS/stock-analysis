#!/usr/bin/env python3
"""
修复 V11_IC_2024Q1_001 的90条记录
===============================
补充正确的入场价、出场价、股票名称和行业信息
"""

import pymysql
import logging
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4'
}


def get_stock_name_industry(cursor, code):
    """从stock_basic获取股票名称和行业"""
    # 尝试不同格式的代码匹配
    formats = [code, code.zfill(6), str(int(code)) if code.isdigit() else code]
    
    for fmt in formats:
        cursor.execute("""
            SELECT name, industry FROM stock_basic 
            WHERE code = %s OR code = %s
        """, (fmt, fmt.lstrip('0')))
        result = cursor.fetchone()
        if result:
            return result[0], result[1]
    
    return None, None


def get_price(cursor, code, date):
    """从stock_kline获取指定日期的收盘价"""
    # 尝试不同格式的代码匹配
    formats = [code, code.zfill(6), str(int(code)) if code.isdigit() else code]
    
    for fmt in formats:
        cursor.execute("""
            SELECT close FROM stock_kline 
            WHERE code = %s AND trade_date = %s
        """, (fmt, date))
        result = cursor.fetchone()
        if result and result[0]:
            return float(result[0])
    
    return None


def fix_records():
    """修复90条记录"""
    logger.info("=" * 70)
    logger.info("修复 V11_IC_2024Q1_001 记录")
    logger.info("=" * 70)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 获取所有需要修复的记录
    cursor.execute("""
        SELECT id, code, select_date, entry_date, exit_date
        FROM backtest_trades 
        WHERE run_id = 'V11_IC_2024Q1_001'
        ORDER BY id
    """)
    records = cursor.fetchall()
    
    logger.info(f"找到 {len(records)} 条需要修复的记录")
    
    fixed_count = 0
    error_count = 0
    
    for idx, record in enumerate(records):
        record_id, code, select_date, entry_date, exit_date = record
        
        try:
            # 获取股票名称和行业
            name, industry = get_stock_name_industry(cursor, code)
            
            # 获取入场价（选股日收盘价）
            entry_price = get_price(cursor, code, select_date)
            
            # 获取出场价（出场日收盘价）
            exit_price = get_price(cursor, code, exit_date)
            
            # 如果价格缺失，尝试使用其他日期
            if entry_price is None:
                # 尝试查找选股日之后的第一个交易日
                cursor.execute("""
                    SELECT trade_date, close FROM stock_kline 
                    WHERE code = %s AND trade_date >= %s
                    ORDER BY trade_date LIMIT 1
                """, (code.zfill(6) if code.isdigit() else code, select_date))
                result = cursor.fetchone()
                if result:
                    entry_date = result[0]
                    entry_price = float(result[1])
                    logger.warning(f"记录 {record_id}: 使用 {entry_date} 作为入场日")
            
            if exit_price is None and entry_price:
                # 尝试查找出场日之前的最后一个交易日
                cursor.execute("""
                    SELECT trade_date, close FROM stock_kline 
                    WHERE code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 1
                """, (code.zfill(6) if code.isdigit() else code, exit_date))
                result = cursor.fetchone()
                if result:
                    exit_price = float(result[1])
            
            # 如果仍然没有价格，跳过
            if entry_price is None or exit_price is None:
                logger.error(f"记录 {record_id} ({code}): 无法获取价格数据")
                error_count += 1
                continue
            
            # 计算收益
            gross_return = (exit_price - entry_price) / entry_price
            
            # 扣除成本 (佣金0.03% + 印花税0.05% + 滑点0.2% = 0.28%)
            cost = Decimal('0.0028')
            net_return = Decimal(str(gross_return)) - cost
            
            # 更新记录
            cursor.execute("""
                UPDATE backtest_trades 
                SET name = %s,
                    industry = %s,
                    entry_date = %s,
                    entry_price = %s,
                    exit_date = %s,
                    exit_price = %s,
                    gross_return = %s,
                    net_return = %s
                WHERE id = %s
            """, (
                name or f"股票{code}",
                industry or "未知",
                entry_date,
                entry_price,
                exit_date,
                exit_price,
                gross_return,
                net_return,
                record_id
            ))
            
            fixed_count += 1
            
            if (idx + 1) % 10 == 0:
                logger.info(f"进度: {idx + 1}/{len(records)} 已修复")
                conn.commit()
        
        except Exception as e:
            logger.error(f"记录 {record_id} 修复失败: {e}")
            error_count += 1
            continue
    
    conn.commit()
    
    logger.info("=" * 70)
    logger.info(f"修复完成: {fixed_count} 条成功, {error_count} 条失败")
    logger.info("=" * 70)
    
    cursor.close()
    conn.close()
    
    return fixed_count, error_count


def verify_fix():
    """验证修复结果"""
    logger.info("\n验证修复结果...")
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN entry_price > 0 THEN 1 ELSE 0 END) as has_entry,
            SUM(CASE WHEN exit_price > 0 THEN 1 ELSE 0 END) as has_exit,
            SUM(CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END) as has_name
        FROM backtest_trades 
        WHERE run_id = 'V11_IC_2024Q1_001'
    """)
    result = cursor.fetchone()
    
    logger.info(f"总记录数: {result[0]}")
    logger.info(f"有入场价的记录: {result[1]}")
    logger.info(f"有出场价的记录: {result[2]}")
    logger.info(f"有名称的记录: {result[3]}")
    
    # 显示修复后的样本
    cursor.execute("""
        SELECT id, code, name, industry, entry_date, entry_price, exit_price, net_return
        FROM backtest_trades 
        WHERE run_id = 'V11_IC_2024Q1_001'
        ORDER BY id
        LIMIT 5
    """)
    sample = cursor.fetchall()
    
    logger.info("\n修复后样本:")
    logger.info(f"{'ID':<5} {'代码':<10} {'名称':<10} {'入场价':>10} {'出场价':>10} {'净收益':>10}")
    logger.info("-" * 70)
    for row in sample:
        logger.info(f"{row[0]:<5} {row[1]:<10} {row[2] or 'N/A':<10} {row[5] or 0:>10.2f} {row[6] or 0:>10.2f} {row[7] or 0:>10.4f}")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    fixed, errors = fix_records()
    verify_fix()
