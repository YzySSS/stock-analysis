#!/usr/bin/env python3
"""
V8选股策略周报
每周回顾推荐准确率
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.recommendation_tracker import RecommendationTracker
from datetime import datetime


def generate_weekly_report():
    """生成周报"""
    tracker = RecommendationTracker()
    
    print("\n" + "="*70)
    print("📊 V8选股策略 - 周度回顾")
    print("="*70)
    print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # 获取周报内容
    report = tracker.generate_weekly_report()
    print(report)
    
    # 保存周报
    report_dir = "/workspace/projects/workspace/股票分析项目/reports"
    os.makedirs(report_dir, exist_ok=True)
    
    filename = f"{report_dir}/weekly_report_{datetime.now().strftime('%Y%m%d')}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n💾 周报已保存: {filename}")


if __name__ == "__main__":
    generate_weekly_report()
