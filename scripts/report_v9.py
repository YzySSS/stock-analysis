#!/usr/bin/env python3
"""
股票报告 V9 - 整合V8全A股选股策略
- 盘前：V8选股 TOP 3
- 盘中：持仓监控 + V8选股
- 盘后：日终总结 + V8选股回顾
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
import requests
import time
from datetime import datetime
from typing import List, Dict

# 导入V8筛选器
from scripts.screen_sector_v8 import SectorScreenerV8


class StockReportV9:
    """V9 股票报告 - 整合V8选股"""
    
    def __init__(self):
        self.sina_session = requests.Session()
        self.sina_session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def get_v8_picks(self, top_n: int = 3) -> List[Dict]:
        """获取V8选股结果"""
        print("\n🔍 运行V8全A股选股策略...")
        screener = SectorScreenerV8()
        picks = screener.run(top_n=top_n, for_cron=True)
        return picks
    
    def format_picks_for_report(self, picks: List[Dict]) -> str:
        """格式化选股结果为报告文本"""
        lines = ["\n" + "="*60]
        lines.append("🎯 V8 全A股选股策略 - TOP推荐")
        lines.append("="*60)
        
        for i, pick in enumerate(picks, 1):
            f = pick['factors']
            lines.append(f"\n{i}. {pick['name']} ({pick['code']})")
            lines.append(f"   当前价: ¥{pick['price']:.2f}")
            lines.append(f"   涨跌幅: {pick['change_pct']:+.2f}%")
            lines.append(f"   综合评分: {pick['total_score']:.1f}")
            lines.append(f"   因子得分: 技术{f.technical} | 财务{f.fundamental} | 机构{f.institution} | 风险{f.risk}")
        
        lines.append("\n" + "="*60)
        return "\n".join(lines)
    
    def generate_premarket_report(self) -> str:
        """盘前报告 - 包含V8选股"""
        print("\n" + "="*60)
        print("📋 盘前报告 - V9 (含V8选股)")
        print("="*60)
        
        # 获取V8选股
        picks = self.get_v8_picks(top_n=3)
        
        report = []
        report.append(f"\n📅 日期: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append("📊 策略: V8全A股多因子选股")
        report.append(self.format_picks_for_report(picks))
        
        report_text = "\n".join(report)
        
        # 保存报告
        self._save_report(report_text, "premarket")
        
        return report_text
    
    def generate_intraday_report(self, positions: List[Dict] = None) -> str:
        """盘中报告 - 持仓监控 + V8选股"""
        print("\n" + "="*60)
        print("📋 盘中报告 - V9 (含V8选股)")
        print("="*60)
        
        report = []
        report.append(f"\n📅 时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        # 持仓监控
        if positions:
            report.append("\n📊 持仓监控:")
            for pos in positions[:5]:
                report.append(f"  • {pos.get('name', 'Unknown')}: {pos.get('change_pct', 0):+.2f}%")
        
        # V8选股
        picks = self.get_v8_picks(top_n=3)
        report.append(self.format_picks_for_report(picks))
        
        report_text = "\n".join(report)
        self._save_report(report_text, "intraday")
        
        return report_text
    
    def generate_postmarket_report(self) -> str:
        """盘后报告 - 日终总结 + V8选股"""
        print("\n" + "="*60)
        print("📋 盘后报告 - V9 (含V8选股)")
        print("="*60)
        
        report = []
        report.append(f"\n📅 日期: {datetime.now().strftime('%Y-%m-%d')}")
        report.append("\n📊 日终总结:")
        report.append("  今日V8选股策略已完成全市场扫描")
        
        # V8选股
        picks = self.get_v8_picks(top_n=3)
        report.append(self.format_picks_for_report(picks))
        
        report_text = "\n".join(report)
        self._save_report(report_text, "postmarket")
        
        return report_text
    
    def _save_report(self, content: str, report_type: str):
        """保存报告"""
        reports_dir = "/workspace/projects/workspace/股票分析项目/reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"{reports_dir}/report_v9_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"\n💾 报告已保存: {filename}")


def main():
    """主函数 - 根据参数生成不同报告"""
    import argparse
    parser = argparse.ArgumentParser(description='股票报告 V9')
    parser.add_argument('--type', choices=['premarket', 'intraday', 'postmarket'], 
                       default='premarket', help='报告类型')
    args = parser.parse_args()
    
    report = StockReportV9()
    
    if args.type == 'premarket':
        content = report.generate_premarket_report()
    elif args.type == 'intraday':
        content = report.generate_intraday_report()
    else:
        content = report.generate_postmarket_report()
    
    print(content)


if __name__ == "__main__":
    main()
