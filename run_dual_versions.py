#!/usr/bin/env python3
"""
双版本并行运行脚本 - 版本A(5因子) vs 版本B(V11)
每天定时任务调用，同时推送两个版本的报告到飞书
"""

import subprocess
import sys
import os
from datetime import datetime

def run_version(version_name, script_path, report_dir, mode='premarket'):
    """运行指定版本并推送"""
    print(f"\n{'='*70}")
    print(f"🚀 启动 {version_name} - {mode}")
    print(f"{'='*70}")
    
    project_root = "/workspace/projects/workspace/股票分析项目"
    
    # 设置环境变量
    env = os.environ.copy()
    env['STOCK_REPORTS_DIR'] = report_dir
    env['VERSION_NAME'] = version_name
    
    # 盘前模式启用简化版策略
    if mode == 'premarket' and '版本B' in version_name:
        env['PREMARKET_MODE'] = '1'
    
    # 运行选股
    result = subprocess.run(
        [sys.executable, script_path, '--mode', mode],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=300
    )
    
    print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    
    if result.returncode != 0:
        print(f"❌ {version_name} 运行失败")
        return False
    
    return True

def main():
    print("="*70)
    print("🔄 双版本并行运行 (版本A vs 版本B)")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # 获取运行模式
    mode = 'premarket'
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    
    print(f"\n运行模式: {mode}")
    
    project_root = "/workspace/projects/workspace/股票分析项目"
    
    # 运行版本A (5因子)
    version_a_success = run_version(
        '版本A(5因子)',
        os.path.join(project_root, 'versions/version_a/run_version_a.py'),
        os.path.join(project_root, 'daily_reports_version_a'),
        mode
    )
    
    # 运行版本B (V11)
    version_b_success = run_version(
        '版本B(V11)',
        os.path.join(project_root, 'versions/version_b/run_version_b.py'),
        os.path.join(project_root, 'daily_reports_version_b'),
        mode
    )
    
    # 总结
    print("\n" + "="*70)
    print("✅ 双版本运行完成")
    print("="*70)
    print(f"版本A(5因子): {'成功' if version_a_success else '失败'}")
    print(f"版本B(V11): {'成功' if version_b_success else '失败'}")
    
    return 0 if (version_a_success and version_b_success) else 1

if __name__ == "__main__":
    sys.exit(main())
