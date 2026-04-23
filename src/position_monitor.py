#!/usr/bin/env python3
"""
持仓监控与报告模块

功能：
- 实时监控持仓盈亏
- 止损止盈提醒
- 生成持仓分析报告
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from typing import List, Dict
from datetime import datetime
import pandas as pd

from src.datasource_v2 import get_data_manager
from config.positions import get_all_positions, Position


class PositionMonitor:
    """持仓监控器"""
    
    def __init__(self):
        self.manager = get_data_manager()
        self.positions = get_all_positions()
    
    def monitor_positions(self) -> pd.DataFrame:
        """
        监控所有持仓
        
        Returns:
            DataFrame: 包含实时盈亏数据
        """
        codes = [p.code for p in self.positions]
        
        # 获取实时行情
        df_realtime = self.manager.get_realtime_quotes(codes)
        
        if df_realtime.empty:
            print("⚠️ 无法获取实时行情")
            return pd.DataFrame()
        
        # 合并持仓成本数据
        results = []
        for pos in self.positions:
            row = df_realtime[df_realtime['code'].str.contains(pos.code)]
            
            if not row.empty:
                current_price = row.iloc[0].get('price', 0)
                profit_data = pos.calculate_profit(current_price)
                
                results.append({
                    'code': pos.code,
                    'name': pos.name,
                    'cost_price': pos.cost_price,
                    'current_price': current_price,
                    'profit_amount': profit_data['profit_amount'],
                    'profit_percent': profit_data['profit_percent'],
                    'is_profit': profit_data['is_profit'],
                    'volume': row.iloc[0].get('volume', 0),
                    'high': row.iloc[0].get('high', 0),
                    'low': row.iloc[0].get('low', 0),
                })
        
        return pd.DataFrame(results)
    
    def generate_position_report(self) -> str:
        """生成持仓分析报告"""
        df = self.monitor_positions()
        
        if df.empty:
            return "❌ 无法生成报告，数据获取失败"
        
        # 计算总体盈亏
        total_profit = df['profit_percent'].mean()
        profit_count = df[df['is_profit']].shape[0]
        loss_count = df[~df['is_profit']].shape[0]
        
        lines = [
            "📊 **持仓监控报告**",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🕐 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "📈 **总体概况**",
            f"- 持仓数量: {len(df)} 只",
            f"- 平均盈亏: {total_profit:+.2f}%",
            f"- 盈利: {profit_count} 只 | 亏损: {loss_count} 只",
            "",
            "📋 **持仓明细**",
        ]
        
        # 按盈亏排序
        df_sorted = df.sort_values('profit_percent', ascending=False)
        
        for i, row in df_sorted.iterrows():
            emoji = "🟢" if row['is_profit'] else "🔴"
            status = "盈利" if row['is_profit'] else "亏损"
            
            lines.extend([
                f"",
                f"{emoji} {row['name']} ({row['code']}) - {status}",
                f"   ├─ 成本: ¥{row['cost_price']:.2f} → 现价: ¥{row['current_price']:.2f}",
                f"   ├─ 盈亏: {row['profit_percent']:+.2f}% (¥{row['profit_amount']:.2f})",
                f"   └─ 今日: 最高¥{row['high']:.2f} / 最低¥{row['low']:.2f}",
            ])
            
            # 添加提醒
            if row['profit_percent'] > 10:
                lines.append(f"   ⚠️ **提醒**: 盈利超10%，可考虑止盈")
            elif row['profit_percent'] < -10:
                lines.append(f"   ⚠️ **提醒**: 亏损超10%，关注止损")
        
        lines.extend([
            "",
            "💡 **策略建议**",
        ])
        
        # 根据整体情况给出建议
        if total_profit > 5:
            lines.append("• 整体盈利良好，可考虑减仓锁定利润")
        elif total_profit < -5:
            lines.append("• 整体亏损较大，建议控制仓位，避免加仓")
        else:
            lines.append("• 整体波动不大，可继续持有观察")
        
        lines.extend([
            "• 严格设置止损线，单只股票亏损不超过10%",
            "• 盈利股票可考虑分批止盈",
            "",
            "⚠️ **风险提示**",
            "• 以上分析仅供参考，不构成投资建议",
            "• 股市有风险，投资需谨慎",
            "",
            "---",
            "🤖 OpenClaw AI 持仓监控系统"
        ])
        
        return "\n".join(lines)
    
    def check_alerts(self) -> List[str]:
        """检查告警（止损/止盈）"""
        df = self.monitor_positions()
        alerts = []
        
        if df.empty:
            return alerts
        
        for _, row in df.iterrows():
            # 止盈提醒 (>10%)
            if row['profit_percent'] > 10:
                alerts.append(f"🎯 止盈提醒: {row['name']} 盈利 {row['profit_percent']:.1f}%，建议考虑止盈")
            
            # 止损提醒 (<-10%)
            elif row['profit_percent'] < -10:
                alerts.append(f"🛑 止损提醒: {row['name']} 亏损 {abs(row['profit_percent']):.1f}%，建议关注止损")
            
            # 大幅波动提醒 (>5%单日)
            daily_change = ((row['current_price'] - row['cost_price']) / row['cost_price']) * 100
            if abs(daily_change) > 5:
                direction = "上涨" if daily_change > 0 else "下跌"
                alerts.append(f"⚡ 异动提醒: {row['name']} 今日大幅{direction} {abs(daily_change):.1f}%")
        
        return alerts


def main():
    """主函数"""
    print("📊 持仓监控报告")
    print("=" * 60)
    
    monitor = PositionMonitor()
    
    # 生成报告
    report = monitor.generate_position_report()
    print(report)
    
    # 检查告警
    alerts = monitor.check_alerts()
    if alerts:
        print("\n🚨 **告警信息**")
        for alert in alerts:
            print(alert)
    else:
        print("\n✅ 暂无告警")


if __name__ == "__main__":
    main()
