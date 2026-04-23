#!/usr/bin/env python3
"""
盘后复盘工具

用法:
    python3 postmarket_review.py              # 生成今天的复盘报告
    python3 postmarket_review.py --date 20260315  # 生成指定日期的复盘
    python3 postmarket_review.py --week       # 生成本周复盘汇总
    python3 postmarket_review.py --month      # 生成本月复盘汇总
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
from datetime import datetime, timedelta
from postmarket_analyzer import create_daily_summary, DailyReportManager, PostMarketAnalyzer
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_weekly_summary():
    """生成本周复盘汇总"""
    print("📊 生成本周复盘汇总...")
    
    manager = DailyReportManager()
    analyzer = PostMarketAnalyzer(manager)
    
    # 获取最近7天的总结
    summaries = manager.get_latest_summary(days=7)
    
    if not summaries:
        print("❌ 本周无复盘记录")
        return
    
    # 计算周统计
    total_accuracy = sum(s.get('accuracy_rate', 0) for s in summaries) / len(summaries)
    total_predictions = sum(s.get('total_predictions', 0) for s in summaries)
    total_accurate = sum(s.get('accurate_count', 0) for s in summaries)
    
    print(f"\n📈 本周统计 ({len(summaries)}天)")
    print(f"   总预测: {total_predictions} 只")
    print(f"   总准确: {total_accurate} 只")
    print(f"   平均准确率: {total_accuracy:.1f}%")
    
    # 收集所有改进建议
    all_adjustments = []
    for s in summaries:
        all_adjustments.extend(s.get('strategy_adjustments', []))
    
    if all_adjustments:
        print(f"\n💡 本周策略改进建议:")
        # 去重统计
        from collections import Counter
        adjustment_counts = Counter(all_adjustments)
        for advice, count in adjustment_counts.most_common():
            print(f"   - {advice} (出现{count}次)")
    
    # 保存周总结
    week_end = datetime.now().strftime('%Y%m%d')
    week_start = (datetime.now() - timedelta(days=6)).strftime('%Y%m%d')
    
    report_content = f"""# 📊 本周复盘汇总 ({week_start} - {week_end})

## 📈 统计概况

- **统计天数**: {len(summaries)} 天
- **总预测数**: {total_predictions} 只
- **总准确数**: {total_accurate} 只
- **平均准确率**: {total_accuracy:.1f}%

## 💡 策略改进汇总

"""
    
    if all_adjustments:
        for advice, count in Counter(all_adjustments).most_common():
            report_content += f"- {advice} (出现{count}次)\n"
    else:
        report_content += "- 本周无策略调整\n"
    
    report_content += f"\n\n---\n*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
    
    # 保存到summary目录
    summary_path = manager.save_report(
        report_content,
        'summary',
        week_end,
        f"weekly_{week_start}_{week_end}.md"
    )
    
    print(f"\n✅ 周总结已保存: {summary_path}")


def generate_monthly_summary():
    """生成本月复盘汇总"""
    print("📊 生成本月复盘汇总...")
    
    manager = DailyReportManager()
    
    # 获取最近30天的总结
    summaries = manager.get_latest_summary(days=30)
    
    if not summaries:
        print("❌ 本月无复盘记录")
        return
    
    # 计算月统计
    total_accuracy = sum(s.get('accuracy_rate', 0) for s in summaries) / len(summaries)
    total_predictions = sum(s.get('total_predictions', 0) for s in summaries)
    total_accurate = sum(s.get('accurate_count', 0) for s in summaries)
    
    print(f"\n📈 本月统计 ({len(summaries)}天)")
    print(f"   总预测: {total_predictions} 只")
    print(f"   总准确: {total_accurate} 只")
    print(f"   平均准确率: {total_accuracy:.1f}%")
    
    # 保存月总结
    month = datetime.now().strftime('%Y%m')
    
    report_content = f"""# 📊 {month} 月度复盘汇总

## 📈 统计概况

- **统计天数**: {len(summaries)} 天
- **总预测数**: {total_predictions} 只
- **总准确数**: {total_accurate} 只
- **平均准确率**: {total_accuracy:.1f}%

## 🎯 策略表现

本月策略整体表现{'优秀' if total_accuracy > 60 else '良好' if total_accuracy > 50 else '一般'}。

---

*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    summary_path = manager.save_report(
        report_content,
        'summary',
        month,
        f"monthly_{month}.md"
    )
    
    print(f"\n✅ 月总结已保存: {summary_path}")


def list_recent_summaries(days: int = 7):
    """列出最近复盘"""
    manager = DailyReportManager()
    summaries = manager.get_latest_summary(days=days)
    
    if not summaries:
        print(f"❌ 最近{days}天无复盘记录")
        return
    
    print(f"\n📋 最近{len(summaries)}天复盘记录:")
    print("-" * 60)
    
    for s in summaries:
        date = s.get('date', 'unknown')
        accuracy = s.get('accuracy_rate', 0)
        total = s.get('total_predictions', 0)
        accurate = s.get('accurate_count', 0)
        
        emoji = "🟢" if accuracy >= 60 else "🟡" if accuracy >= 50 else "🔴"
        print(f"{emoji} {date}: {accurate}/{total} 准确 ({accuracy}%)")


def main():
    parser = argparse.ArgumentParser(description='盘后复盘工具')
    parser.add_argument('--date', type=str,
                       help='指定日期 (格式: YYYYMMDD)')
    parser.add_argument('--week', action='store_true',
                       help='生成本周汇总')
    parser.add_argument('--month', action='store_true',
                       help='生成本月汇总')
    parser.add_argument('--list', action='store_true',
                       help='列出最近复盘记录')
    parser.add_argument('--days', type=int, default=7,
                       help='显示最近N天的记录 (默认7天)')
    
    args = parser.parse_args()
    
    print("📊 股票分析 - 盘后复盘工具")
    print("=" * 60)
    
    if args.week:
        generate_weekly_summary()
    elif args.month:
        generate_monthly_summary()
    elif args.list:
        list_recent_summaries(args.days)
    else:
        # 生成单日复盘
        date = args.date or datetime.now().strftime('%Y%m%d')
        print(f"\n📝 生成 {date} 盘后复盘报告...")
        
        try:
            report = create_daily_summary(date)
            print(f"\n✅ 复盘报告生成成功!")
            print(f"\n报告预览:\n")
            print(report[:800] + "..." if len(report) > 800 else report)
        except Exception as e:
            print(f"❌ 生成失败: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
