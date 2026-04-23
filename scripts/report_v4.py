#!/usr/bin/env python3
"""
股票报告生成器 V4 - 简化版
包含持仓分析
"""

import sys
import os
import requests
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime
from typing import List
import pandas as pd

# 持仓配置
POSITIONS = [
    {'code': '603300', 'name': '海南华铁', 'cost': 11.578},
    {'code': '000880', 'name': '潍柴重机', 'cost': 20.54},
    {'code': '600519', 'name': '贵州茅台', 'cost': 1547.91},
    {'code': '002031', 'name': '巨轮智能', 'cost': 7.441},
    {'code': '003028', 'name': '振邦智能', 'cost': 39.49},
]

class SinaAPI:
    """新浪财经API"""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def format_code(self, code: str) -> str:
        code = code.strip().lower()
        if code.startswith('6'):
            return f"sh{code}"
        return f"sz{code}"
    
    def get_quotes(self, codes: List[str]) -> pd.DataFrame:
        formatted = [self.format_code(c) for c in codes]
        url = f"https://hq.sinajs.cn/rn={int(time.time()*1000)}&list={','.join(formatted)}"
        
        try:
            r = self.session.get(url, timeout=10)
            r.encoding = 'gbk'
            return self._parse(r.text)
        except Exception as e:
            print(f"获取失败: {e}")
            return pd.DataFrame()
    
    def _parse(self, text: str) -> pd.DataFrame:
        stocks = []
        for line in text.strip().split(';'):
            if 'var hq_str_' not in line or '=' not in line:
                continue
            try:
                var_part = line.split('="')[0]
                data = line.split('="')[1].rstrip('"').split(',')
                if len(data) < 30:
                    continue
                
                code = var_part.replace('var hq_str_', '')
                price = float(data[3])
                pre_close = float(data[2])
                change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                
                stocks.append({
                    'code': code,
                    'name': data[0],
                    'price': price,
                    'change_percent': change_pct,
                    'open': float(data[1]),
                    'high': float(data[4]),
                    'low': float(data[5]),
                    'bid1_price': float(data[11]) if len(data) > 11 else 0,
                    'ask1_price': float(data[21]) if len(data) > 21 else 0,
                })
            except:
                continue
        return pd.DataFrame(stocks)


class StockReportV4:
    def __init__(self):
        self.api = SinaAPI()
        self.positions = POSITIONS
    
    def analyze_position(self, code, current_price):
        """分析持仓并给出买卖建议"""
        for p in self.positions:
            if p['code'] == code:
                profit_pct = ((current_price - p['cost']) / p['cost'] * 100)
                
                if profit_pct > 15:
                    return '🔴 强烈建议止盈', '减仓50%以上，锁定利润', profit_pct
                elif profit_pct > 10:
                    return '🟡 建议部分止盈', '减仓30%，保留底仓', profit_pct
                elif profit_pct > 5:
                    return '🟢 建议持有', '趋势向好，继续持有', profit_pct
                elif profit_pct > -5:
                    return '⚪ 建议观望', '震荡整理，设好止损观察', profit_pct
                elif profit_pct > -10:
                    return '🟠 关注止损', '接近止损线，做好减仓准备', profit_pct
                else:
                    return '🔴 强烈建议止损', '已超止损线，严格减仓', profit_pct
        return '⚪ 未知', '无持仓数据', 0
    
    def premarket(self):
        """盘前报告"""
        codes = [p['code'] for p in self.positions]
        df = self.api.get_quotes(codes)
        
        lines = [
            "📊 盘前报告 V4",
            "=" * 70,
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "数据源: 新浪财经(5档盘口) + BaoStock",
            "",
            "📈 持仓分析与买卖建议",
            "-" * 70,
            ""
        ]
        
        if not df.empty:
            for _, row in df.iterrows():
                code = row.get('code', '').replace('sz', '').replace('sh', '').replace('bj', '')
                current = row.get('price', 0)
                name = row.get('name', '未知')
                
                advice, action, profit = self.analyze_position(code, current)
                emoji = '🟢' if profit >= 0 else '🔴'
                
                lines.extend([
                    f"{emoji} {name} ({code})",
                    f"   现价: ¥{current:.2f} | 持仓盈亏: {profit:+.2f}%",
                    f"   5档盘口: 买{row.get('bid1_price', 0):.2f} 卖{row.get('ask1_price', 0):.2f}",
                    f"   {advice}",
                    f"   操作建议: {action}",
                    ""
                ])
        
        lines.extend([
            "💡 今日策略",
            "-" * 70,
            "• 开盘30分钟观察资金流向",
            "• 按建议执行止盈止损",
            "• 严格控制仓位，不追高",
            "",
            "⚠️ 风险提示: 以上分析仅供参考，不构成投资建议",
            "",
            "-" * 70,
            "🤖 OpenClaw AI V4"
        ])
        
        return "\n".join(lines)
    
    def intraday(self):
        """盘中简报"""
        codes = [p['code'] for p in self.positions]
        df = self.api.get_quotes(codes)
        
        lines = [
            "⚡ 盘中简报 V4",
            "=" * 70,
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "📊 持仓实时监控",
            "-" * 70,
            ""
        ]
        
        if not df.empty:
            for _, row in df.iterrows():
                code = row.get('code', '').replace('sz', '').replace('sh', '').replace('bj', '')
                current = row.get('price', 0)
                change = row.get('change_percent', 0)
                name = row.get('name', '未知')
                
                advice, action, profit = self.analyze_position(code, current)
                emoji = '🚀' if change > 5 else '🟢' if change > 0 else '🔴' if change < 0 else '⚪'
                
                lines.extend([
                    f"{emoji} {name} ({code})",
                    f"   现价: ¥{current:.2f} ({change:+.2f}%) | 盈亏: {profit:+.2f}%",
                    f"   今日最高: ¥{row.get('high', 0):.2f} / 最低: ¥{row.get('low', 0):.2f}",
                    f"   {advice}",
                    ""
                ])
                
                if abs(change) > 5:
                    direction = "大涨" if change > 0 else "大跌"
                    lines.append(f"   ⚠️ 异动: 今日{direction}，注意量能变化")
                    lines.append("")
        
        lines.extend([
            "💎 午后策略",
            "-" * 70,
            "• 按持仓建议执行操作",
            "• 尾盘注意控制风险",
            "• 严格止损止盈",
            "",
            "-" * 70,
            "🤖 OpenClaw AI V4"
        ])
        
        return "\n".join(lines)
    
    def postmarket(self):
        """盘后复盘"""
        codes = [p['code'] for p in self.positions]
        df = self.api.get_quotes(codes)
        
        lines = [
            "📊 盘后复盘 V4",
            "=" * 70,
            f"日期: {datetime.now().strftime('%Y-%m-%d')}",
            "",
            "📈 今日持仓表现",
            "-" * 70,
            ""
        ]
        
        total_profit = 0
        if not df.empty:
            for _, row in df.iterrows():
                code = row.get('code', '').replace('sz', '').replace('sh', '').replace('bj', '')
                current = row.get('price', 0)
                change = row.get('change_percent', 0)
                name = row.get('name', '未知')
                
                advice, action, profit = self.analyze_position(code, current)
                total_profit += profit
                
                emoji = '🟢' if change > 0 else '🔴' if change < 0 else '⚪'
                lines.extend([
                    f"{emoji} {name} ({code})",
                    f"   收盘: ¥{current:.2f} ({change:+.2f}%)",
                    f"   持仓盈亏: {profit:+.2f}%",
                    f"   最高: ¥{row.get('high', 0):.2f} / 最低: ¥{row.get('low', 0):.2f}",
                    ""
                ])
        
        avg_profit = total_profit / len(self.positions) if self.positions else 0
        lines.extend([
            f"📊 今日总体: 平均盈亏 {avg_profit:+.2f}%",
            "",
            "💡 明日买卖建议",
            "-" * 70,
            ""
        ])
        
        if not df.empty:
            for _, row in df.iterrows():
                code = row.get('code', '').replace('sz', '').replace('sh', '').replace('bj', '')
                current = row.get('price', 0)
                name = row.get('name', '未知')
                
                advice, action, profit = self.analyze_position(code, current)
                lines.extend([
                    f"{name} ({code}):",
                    f"   当前盈亏: {profit:+.2f}%",
                    f"   明日建议: {advice}",
                    f"   操作: {action}",
                    ""
                ])
        
        lines.extend([
            "🎯 明日大盘展望",
            "-" * 70,
            "• 关注今晚美股走势",
            "• 留意政策面消息",
            "• 严格执行风控",
            "",
            "-" * 70,
            "🤖 OpenClaw AI V4"
        ])
        
        return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("用法: python report_v4.py [premarket|intraday|postmarket]")
        sys.exit(1)
    
    report_type = sys.argv[1]
    generator = StockReportV4()
    
    if report_type == "premarket":
        print(generator.premarket())
    elif report_type == "intraday":
        print(generator.intraday())
    elif report_type == "postmarket":
        print(generator.postmarket())
    else:
        print(f"未知类型: {report_type}")
        sys.exit(1)


if __name__ == "__main__":
    main()
