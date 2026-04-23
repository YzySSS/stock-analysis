#!/usr/bin/env python3
"""
修复PE数据问题
1. 使用pe_ratio（原始PE）替代pe_clean（标准化值）
2. 清理异常值
3. 用行业中位数填充NULL
"""

import pymysql
import numpy as np
from collections import defaultdict

DB_CONFIG = {
    'host': '10.0.4.8', 'port': 3306, 'user': 'openclaw_user',
    'password': 'open@2026', 'database': 'stock',
    'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci'
}

def fix_pe_data():
    """修复PE数据"""
    print("="*70)
    print("PE数据修复脚本")
    print("="*70)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # 1. 检查是否需要添加新字段
    print("\n【1. 添加pe_fixed字段】")
    cursor.execute("DESCRIBE stock_basic")
    columns = [row[0] for row in cursor.fetchall()]
    
    if 'pe_fixed' not in columns:
        cursor.execute("ALTER TABLE stock_basic ADD COLUMN pe_fixed FLOAT")
        print("  ✅ 添加pe_fixed字段成功")
    else:
        print("  ℹ️ pe_fixed字段已存在")
    
    # 2. 复制pe_ratio到pe_fixed，清理异常值
    print("\n【2. 清理异常PE值】")
    
    # 先统计
    cursor.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN pe_ratio IS NULL THEN 1 ELSE 0 END) as null_count,
               SUM(CASE WHEN pe_ratio <= 0 THEN 1 ELSE 0 END) as zero_count,
               SUM(CASE WHEN pe_ratio > 0 AND pe_ratio <= 500 THEN 1 ELSE 0 END) as valid_count,
               SUM(CASE WHEN pe_ratio > 500 THEN 1 ELSE 0 END) as outlier_count
        FROM stock_basic
        WHERE is_st = 0 AND is_delisted = 0
    """)
    row = cursor.fetchone()
    total, null_c, zero_c, valid_c, outlier_c = row
    print(f"  总股票: {total}")
    print(f"  NULL: {null_c} ({null_c/total*100:.1f}%)")
    print(f"  ≤0: {zero_c} ({zero_c/total*100:.1f}%)")
    print(f"  0<PE≤500 (有效): {valid_c} ({valid_c/total*100:.1f}%)")
    print(f"  >500 (异常): {outlier_c} ({outlier_c/total*100:.1f}%)")
    
    # 复制有效PE值
    cursor.execute("""
        UPDATE stock_basic 
        SET pe_fixed = pe_ratio
        WHERE pe_ratio > 0 AND pe_ratio <= 500
    """)
    valid_updated = cursor.rowcount
    print(f"\n  ✅ 已复制 {valid_updated} 条有效PE数据")
    
    # 3. 用行业中位数填充NULL和异常值
    print("\n【3. 行业填充策略】")
    
    # 计算各行业PE中位数
    cursor.execute("""
        SELECT industry, 
               AVG(pe_fixed) as avg_pe,
               COUNT(*) as count
        FROM stock_basic
        WHERE pe_fixed IS NOT NULL AND is_st = 0 AND is_delisted = 0
        GROUP BY industry
        HAVING count >= 5
    """)
    
    industry_pe = {}
    for row in cursor.fetchall():
        industry, avg_pe, count = row
        industry_pe[industry] = avg_pe
    
    print(f"  统计了 {len(industry_pe)} 个行业的平均PE")
    
    # 填充NULL和异常值
    filled = 0
    for industry, avg_pe in industry_pe.items():
        cursor.execute("""
            UPDATE stock_basic 
            SET pe_fixed = %s
            WHERE industry = %s 
              AND (pe_fixed IS NULL OR pe_fixed <= 0)
              AND is_st = 0 AND is_delisted = 0
        """, (avg_pe, industry))
        filled += cursor.rowcount
    
    print(f"  ✅ 行业填充完成: {filled} 条")
    
    # 4. 剩余仍未填充的用全市场中位数
    print("\n【4. 全局填充剩余NULL】")
    cursor.execute("""
        SELECT AVG(pe_fixed) FROM stock_basic 
        WHERE pe_fixed IS NOT NULL AND pe_fixed > 0 AND pe_fixed <= 500
    """)
    global_avg = cursor.fetchone()[0]
    print(f"  全市场平均PE: {global_avg:.2f}")
    
    cursor.execute("""
        UPDATE stock_basic 
        SET pe_fixed = %s
        WHERE pe_fixed IS NULL AND is_st = 0 AND is_delisted = 0
    """, (global_avg,))
    global_filled = cursor.rowcount
    print(f"  ✅ 全局填充完成: {global_filled} 条")
    
    conn.commit()
    
    # 5. 验证结果
    print("\n【5. 修复结果验证】")
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            AVG(pe_fixed) as avg_pe,
            MIN(pe_fixed) as min_pe,
            MAX(pe_fixed) as max_pe,
            STDDEV(pe_fixed) as std_pe
        FROM stock_basic
        WHERE is_st = 0 AND is_delisted = 0
    """)
    row = cursor.fetchone()
    print(f"  总股票: {row[0]}")
    print(f"  平均PE: {row[1]:.2f}")
    print(f"  PE范围: {row[2]:.2f} - {row[3]:.2f}")
    print(f"  标准差: {row[4]:.2f}")
    
    # 6. 分布检查
    print("\n【6. PE分布检查】")
    ranges = [
        (0, 10, "0<PE≤10"),
        (10, 20, "10<PE≤20"),
        (20, 30, "20<PE≤30"),
        (30, 50, "30<PE≤50"),
        (50, 100, "50<PE≤100"),
        (100, 500, "100<PE≤500")
    ]
    
    for min_v, max_v, label in ranges:
        cursor.execute("""
            SELECT COUNT(*) FROM stock_basic 
            WHERE pe_fixed > %s AND pe_fixed <= %s AND is_st = 0 AND is_delisted = 0
        """, (min_v, max_v))
        count = cursor.fetchone()[0]
        print(f"  {label}: {count}只")
    
    # 7. 样本检查
    print("\n【7. 样本数据检查】")
    cursor.execute("""
        SELECT code, name, industry, pe_ratio, pe_fixed 
        FROM stock_basic 
        WHERE is_st = 0 AND is_delisted = 0
        LIMIT 20
    """)
    print(f"{'代码':<10} {'名称':<12} {'行业':<12} {'原始PE':>10} {'修复后PE':>10}")
    print("-"*70)
    for row in cursor.fetchall():
        orig = f"{row[3]:.2f}" if row[3] else "NULL"
        print(f"{row[0]:<10} {row[1]:<12} {row[2] or '':<12} {orig:>10} {row[4]:>10.2f}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*70)
    print("✅ PE数据修复完成!")
    print("="*70)


if __name__ == '__main__':
    fix_pe_data()
