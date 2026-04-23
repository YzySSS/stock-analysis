#!/usr/bin/env python3
"""
盘中分析调度器 1.0
==================
统一调用 main.py 的各模式

定时任务时间表:
- 8:50  盘前分析: V9选股 + V10+报告
- 12:30 盘中简报: 上午收盘总结 + 下午选股分析
- 15:50 盘后分析: V10+深度复盘
"""

import os
import sys
import argparse

# 设置飞书Webhook（如果环境变量未设置）
if not os.getenv('FEISHU_WEBHOOK'):
    os.environ['FEISHU_WEBHOOK'] = 'https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067'

# 设置报告存储目录（如果环境变量未设置）
if not os.getenv('STOCK_REPORTS_DIR'):
    os.environ['STOCK_REPORTS_DIR'] = '/workspace/projects/workspace/股票分析项目/daily_reports'

# 直接调用 main.py 的功能
sys.path.insert(0, os.path.dirname(__file__))

from main import StockAnalysisSystem


def premarket_analysis():
    """盘前分析 - 8:50 - V9选股 + V10+报告"""
    print("🌅 执行盘前分析 (1.0) - V9选股 + V10+报告...")
    system = StockAnalysisSystem()
    result = system.run_premarket(top_n=3, send=True)  # Top 3
    return result.get('success', False)


def noon_analysis():
    """盘中简报 - 12:30 - 上午收盘总结 + 下午选股分析"""
    print("☀️ 执行盘中简报 (1.0) - 上午收盘总结 + 下午选股分析...")
    system = StockAnalysisSystem()
    result = system.run_intraday(mode='noon', send=True)
    return result.get('success', False)


def postmarket_analysis():
    """盘后深度复盘 - 15:50 - V10+深度复盘"""
    print("🌇 执行盘后深度复盘 (1.0) - V10+深度复盘...")
    system = StockAnalysisSystem()
    result = system.run_postmarket(send=True)
    return result.get('success', False)


def main():
    parser = argparse.ArgumentParser(description='盘中分析调度器 1.0')
    parser.add_argument('--mode', type=str, required=True,
                       choices=['premarket', 'noon', 'postmarket'],
                       help='分析模式 (premarket:8:50, noon:12:30, postmarket:15:50)')
    
    args = parser.parse_args()
    
    print(f"🚀 股票分析调度器 1.0 - 模式: {args.mode}")
    print("="*60)
    
    success = False
    if args.mode == 'premarket':
        success = premarket_analysis()
    elif args.mode == 'noon':
        success = noon_analysis()
    elif args.mode == 'postmarket':
        success = postmarket_analysis()
    
    print("="*60)
    print(f"{'✅' if success else '❌'} 执行{'成功' if success else '失败'}")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
