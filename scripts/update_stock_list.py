#!/usr/bin/env python3
"""
全A股列表更新脚本
================
每天晚上21:00自动运行，增量更新全A股列表

功能：
- 从AkShare获取最新全A股列表
- 与现有列表对比，增量更新
- 记录新增/删除的股票
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# 默认路径
DEFAULT_STOCK_LIST_FILE = os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt")
DEFAULT_LOG_FILE = os.path.expanduser("~/.clawdbot/stock_watcher/stock_list_update.log")

def get_stock_list_file():
    """获取股票列表文件路径"""
    return os.getenv('STOCK_LIST_FILE', DEFAULT_STOCK_LIST_FILE)

def ensure_dir(file_path):
    """确保目录存在"""
    dir_path = os.path.dirname(file_path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"✅ 创建目录: {dir_path}")

def load_existing_stocks(file_path):
    """加载现有的股票列表"""
    if not os.path.exists(file_path):
        return set()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_stocks(file_path, stocks):
    """保存股票列表到文件"""
    ensure_dir(file_path)
    
    # 按代码排序并保存
    sorted_stocks = sorted(stocks, key=lambda x: (x.startswith('6'), x))
    
    with open(file_path, 'w', encoding='utf-8') as f:
        for code in sorted_stocks:
            f.write(f"{code}\n")
    
    return len(sorted_stocks)

def log_update(log_file, message):
    """记录更新日志"""
    ensure_dir(log_file)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def update_stock_list():
    """更新全A股列表"""
    try:
        import akshare as ak
    except ImportError:
        print("❌ 请先安装 akshare: pip install akshare")
        return False
    
    stock_list_file = get_stock_list_file()
    log_file = os.getenv('STOCK_LIST_LOG', DEFAULT_LOG_FILE)
    
    print(f"📁 股票列表文件: {stock_list_file}")
    print(f"📝 日志文件: {log_file}")
    print("-" * 60)
    
    # 1. 加载现有列表
    existing_stocks = load_existing_stocks(stock_list_file)
    print(f"📊 现有股票数量: {len(existing_stocks)}")
    
    # 2. 获取最新列表
    print("📥 正在从AkShare获取最新全A股列表...")
    try:
        stock_info = ak.stock_info_a_code_name()
        new_stocks = set(stock_info['code'].tolist())
        print(f"✅ 获取到最新股票: {len(new_stocks)} 只")
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        log_update(log_file, f"ERROR: 获取失败 - {e}")
        return False
    
    # 3. 计算差异
    added_stocks = new_stocks - existing_stocks
    removed_stocks = existing_stocks - new_stocks
    
    print("-" * 60)
    print("📈 变化统计:")
    print(f"   新增股票: {len(added_stocks)} 只")
    print(f"   移除股票: {len(removed_stocks)} 只")
    
    if added_stocks:
        print(f"\n   新增股票代码:")
        for code in sorted(added_stocks)[:10]:  # 只显示前10个
            name = stock_info[stock_info['code'] == code]['name'].values
            name_str = name[0] if len(name) > 0 else 'Unknown'
            print(f"      + {code} ({name_str})")
        if len(added_stocks) > 10:
            print(f"      ... 还有 {len(added_stocks) - 10} 只")
    
    if removed_stocks:
        print(f"\n   移除股票代码:")
        for code in sorted(removed_stocks)[:10]:
            print(f"      - {code}")
        if len(removed_stocks) > 10:
            print(f"      ... 还有 {len(removed_stocks) - 10} 只")
    
    # 4. 保存更新后的列表
    if added_stocks or removed_stocks or not existing_stocks:
        total_count = save_stocks(stock_list_file, new_stocks)
        print(f"\n✅ 已保存更新: {total_count} 只股票")
        
        # 记录日志
        log_msg = f"UPDATE: 总数={total_count}, 新增={len(added_stocks)}, 移除={len(removed_stocks)}"
        log_update(log_file, log_msg)
        
        if added_stocks:
            log_update(log_file, f"ADDED: {', '.join(sorted(added_stocks))}")
        if removed_stocks:
            log_update(log_file, f"REMOVED: {', '.join(sorted(removed_stocks))}")
    else:
        print("\n📌 没有变化，无需更新")
        log_update(log_file, "NO_CHANGE: 列表无变化")
    
    print("-" * 60)
    print("✅ 全A股列表更新完成")
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("🔄 全A股列表更新任务")
    print("=" * 60)
    print(f"⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    success = update_stock_list()
    
    if success:
        print("\n🎉 任务执行成功")
        sys.exit(0)
    else:
        print("\n❌ 任务执行失败")
        sys.exit(1)
