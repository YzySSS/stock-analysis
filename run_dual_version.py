#!/usr/bin/env python3
"""
双版本并行运行脚本
同时运行V10和V11版本选股，对比结果
"""

import subprocess
import sys
import os
from datetime import datetime
import time

def run_version(version, mode='premarket'):
    """运行指定版本"""
    print(f"\n{'='*60}")
    print(f"🚀 启动 {version} 版本 - {mode}")
    print(f"{'='*60}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    version_script = os.path.join(script_dir, 'versions', version, f'run_{version}.py')
    
    if not os.path.exists(version_script):
        print(f"❌ 脚本不存在: {version_script}")
        return None
    
    # 构建命令
    cmd = [sys.executable, version_script, '--mode', mode, '--no-send']
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        print(result.stdout)
        if result.stderr:
            print(f"⚠️ 警告: {result.stderr[:500]}")
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"❌ {version} 超时")
        return False
    except Exception as e:
        print(f"❌ {version} 失败: {e}")
        return False

def compare_results():
    """对比两个版本的结果"""
    print(f"\n{'='*60}")
    print("📊 版本对比")
    print(f"{'='*60}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    v10_dir = os.path.join(script_dir, 'daily_reports_v10')
    v11_dir = os.path.join(script_dir, 'daily_reports_v11')
    
    # 获取最新报告
    import glob
    
    v10_reports = glob.glob(os.path.join(v10_dir, 'premarket', '*.md'))
    v11_reports = glob.glob(os.path.join(v11_dir, 'premarket', '*.md'))
    
    print(f"\nV10 报告: {len(v10_reports)} 份")
    print(f"V11 报告: {len(v11_reports)} 份")
    
    # 显示最新报告
    if v10_reports:
        latest_v10 = max(v10_reports, key=os.path.getmtime)
        print(f"\nV10 最新: {os.path.basename(latest_v10)}")
    
    if v11_reports:
        latest_v11 = max(v11_reports, key=os.path.getmtime)
        print(f"V11 最新: {os.path.basename(latest_v11)}")

def main():
    """主函数"""
    print("="*60)
    print("🔄 双版本并行运行 (V10 vs V11)")
    print("="*60)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # 确定运行模式
    mode = 'premarket'  # 默认盘前
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    
    print(f"\n运行模式: {mode}")
    
    # 运行V10
    v10_success = run_version('v10', mode)
    time.sleep(2)
    
    # 运行V11
    v11_success = run_version('v11', mode)
    
    # 对比结果
    compare_results()
    
    # 总结
    print(f"\n{'='*60}")
    print("✅ 双版本运行完成")
    print(f"{'='*60}")
    print(f"V10: {'成功' if v10_success else '失败'}")
    print(f"V11: {'成功' if v11_success else '失败'}")
    
    return 0 if (v10_success and v11_success) else 1

if __name__ == "__main__":
    sys.exit(main())
