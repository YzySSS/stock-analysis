#!/usr/bin/env python3
"""
简化双版本运行脚本 - V9 vs V11
"""

import subprocess
import sys
import os
from datetime import datetime

def main():
    print("="*70)
    print("🔄 双版本并行运行 (V9 vs V11)")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    project_root = "/workspace/projects/workspace/股票分析项目"
    
    # 运行V11 (当前main.py)
    print("\n" + "="*70)
    print("🚀 运行 V11 版本 (当前最新)")
    print("="*70)
    
    env_v11 = os.environ.copy()
    env_v11['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v11')
    
    result_v11 = subprocess.run(
        [sys.executable, 'main.py', '--mode', 'premarket', '--no-send'],
        cwd=project_root,
        env=env_v11,
        capture_output=True,
        text=True,
        timeout=300
    )
    
    print(result_v11.stdout[-3000:] if len(result_v11.stdout) > 3000 else result_v11.stdout)
    
    # 运行V9 (使用V10的备份作为V9基础)
    print("\n" + "="*70)
    print("🚀 运行 V9 版本 (基础多因子)")
    print("="*70)
    
    env_v9 = os.environ.copy()
    env_v9['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v9')
    env_v9['SIMPLE_MODE'] = '1'  # 启用简单模式
    
    result_v9 = subprocess.run(
        [sys.executable, 'versions/v10/main_v10.py', '--mode', 'premarket', '--no-send'],
        cwd=project_root,
        env=env_v9,
        capture_output=True,
        text=True,
        timeout=300
    )
    
    print(result_v9.stdout[-3000:] if len(result_v9.stdout) > 3000 else result_v9.stdout)
    
    # 对比结果
    print("\n" + "="*70)
    print("📊 版本对比")
    print("="*70)
    
    import glob
    v9_reports = glob.glob(os.path.join(project_root, 'daily_reports_v9/premarket/*.md'))
    v11_reports = glob.glob(os.path.join(project_root, 'daily_reports_v11/premarket/*.md'))
    
    print(f"V9 报告: {len(v9_reports)} 份")
    print(f"V11 报告: {len(v11_reports)} 份")
    
    print("\n" + "="*70)
    print("✅ 双版本运行完成")
    print("="*70)

if __name__ == "__main__":
    main()
