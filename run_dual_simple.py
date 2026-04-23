#!/usr/bin/env python3
"""
简化版双版本运行
直接分别运行V10和V11
"""

import subprocess
import sys
import os
from datetime import datetime

def main():
    print("="*70)
    print("🔄 双版本并行运行 (V10 vs V11)")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    project_root = "/workspace/projects/workspace/股票分析项目"
    
    # 运行V11 (使用当前main.py)
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
    
    # 运行V10 (使用备份的V10版本)
    print("\n" + "="*70)
    print("🚀 运行 V10 版本 (2026-03-23)")
    print("="*70)
    
    env_v10 = os.environ.copy()
    env_v10['STOCK_REPORTS_DIR'] = os.path.join(project_root, 'daily_reports_v10')
    
    result_v10 = subprocess.run(
        [sys.executable, 'versions/v10/main_v10.py', '--mode', 'premarket', '--no-send'],
        cwd=project_root,
        env=env_v10,
        capture_output=True,
        text=True,
        timeout=300
    )
    
    print(result_v10.stdout[-3000:] if len(result_v10.stdout) > 3000 else result_v10.stdout)
    
    # 对比结果
    print("\n" + "="*70)
    print("📊 版本对比")
    print("="*70)
    
    import glob
    v10_reports = glob.glob(os.path.join(project_root, 'daily_reports_v10/premarket/*.md'))
    v11_reports = glob.glob(os.path.join(project_root, 'daily_reports_v11/premarket/*.md'))
    
    print(f"V10 报告: {len(v10_reports)} 份")
    print(f"V11 报告: {len(v11_reports)} 份")
    
    print("\n" + "="*70)
    print("✅ 双版本运行完成")
    print("="*70)

if __name__ == "__main__":
    main()
