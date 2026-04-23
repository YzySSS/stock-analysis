#!/usr/bin/env python3
"""
持仓管理交互工具
================
支持从同花顺图片导入持仓，实时统计盈亏

使用方法:
    python3 position_cli.py                    # 查看当前持仓
    python3 position_cli.py --import-image     # 导入同花顺截图（图片路径）
    python3 position_cli.py --add              # 手动添加持仓
    python3 position_cli.py --remove CODE      # 删除持仓
    python3 position_cli.py --clear            # 清空所有持仓
    python3 position_cli.py --summary          # 盈亏汇总
    python3 position_cli.py --analysis         # AI持仓分析
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import argparse
from datetime import datetime
from typing import List, Dict

from position_manager import position_manager, Position
from ths_position_parser import parse_ths_image, TongHuaShunParser
from deepseek_analyzer import deepseek_analyzer


def print_header(title: str):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f"📊 {title}")
    print("=" * 70)


def print_position(position: Position, index: int = None):
    """打印单个持仓"""
    prefix = f"{index}. " if index else ""
    emoji = '🟢' if position.current_return >= 0 else '🔴'
    
    cost_value = position.buy_price * position.shares
    current_value = position.current_price * position.shares
    profit_amount = current_value - cost_value
    
    print(f"{prefix}{emoji} {position.name} ({position.code})")
    print(f"   数量: {position.shares:,} 股")
    print(f"   成本: ¥{position.buy_price:.3f} × {position.shares:,} = ¥{cost_value:,.2f}")
    print(f"   现价: ¥{position.current_price:.3f} × {position.shares:,} = ¥{current_value:,.2f}")
    print(f"   盈亏: ¥{profit_amount:,.2f} ({position.current_return:+.2f}%)")
    if position.stop_loss:
        print(f"   止损: ¥{position.stop_loss:.3f} | 目标: ¥{position.target_price:.3f}")
    if position.buy_date:
        print(f"   买入: {position.buy_date}")
    print()


def show_positions():
    """显示当前所有持仓"""
    print_header("当前持仓列表")
    
    positions = position_manager.get_all_positions()
    
    if not positions:
        print("📭 当前没有持仓")
        return
    
    print(f"共 {len(positions)} 只股票\n")
    
    for i, pos in enumerate(positions, 1):
        print_position(pos, i)
    
    # 显示汇总
    show_summary()


def show_summary():
    """显示盈亏汇总"""
    summary = position_manager.get_position_summary()
    
    print("-" * 70)
    print("💰 盈亏汇总")
    print("-" * 70)
    print(f"  持仓数量: {summary['total_positions']} 只")
    print(f"  总成本:   ¥{summary['total_cost']:,.2f}")
    print(f"  总市值:   ¥{summary['total_value']:,.2f}")
    print(f"  总盈亏:   ¥{summary['total_value'] - summary['total_cost']:,.2f}")
    print(f"  收益率:   {summary['total_return']:+.2f}%")
    print(f"  平均收益: {summary['avg_return']:+.2f}%")
    print("=" * 70)


def import_from_image_interactive():
    """交互式导入持仓图片"""
    print_header("从同花顺截图导入持仓")
    
    print("""
📱 使用方法:
1. 打开同花顺APP → 交易 → 持仓
2. 截图保存到电脑
3. 使用OCR工具识别图片文字（如微信/QQ截图识别、百度OCR等）
4. 将识别的文字粘贴到下方

提示: 直接粘贴文字比上传图片更稳定
""")
    
    # 获取用户输入
    print("请粘贴OCR识别后的文字（输入空行结束）:")
    print("-" * 70)
    
    lines = []
    while True:
        try:
            line = input()
            if line.strip() == "":
                if len(lines) > 0:
                    break
                else:
                    print("请输入持仓数据，或按Ctrl+C取消")
                    continue
            lines.append(line)
        except KeyboardInterrupt:
            print("\n❌ 已取消")
            return
    
    ocr_text = "\n".join(lines)
    
    # 解析
    print("\n🔍 正在解析...")
    result = parse_ths_image(ocr_text=ocr_text)
    
    if not result['success']:
        print("❌ 解析失败，未能识别出持仓数据")
        print("提示: 请确保粘贴的是完整的同花顺持仓页文字")
        return
    
    # 显示解析结果
    print(f"\n✅ 成功解析 {result['parsed_count']} 只股票")
    print("\n📋 识别到的持仓:")
    print("-" * 70)
    
    for p in result['positions']:
        emoji = '🟢' if p.profit_pct >= 0 else '🔴'
        print(f"  {emoji} {p.name} ({p.code})")
        print(f"     数量: {p.quantity:,} | 成本: ¥{p.cost_price} | 现价: ¥{p.current_price} | {p.profit_pct:+.2f}%")
    
    print("-" * 70)
    print(f"  总成本: ¥{result['summary']['total_cost']:,.2f}")
    print(f"  总市值: ¥{result['summary']['total_value']:,.2f}")
    print(f"  总盈亏: ¥{result['summary']['total_profit']:,.2f} ({result['summary']['profit_pct']:+.2f}%)")
    print("=" * 70)
    
    # 确认导入
    print("\n是否导入以上持仓到系统？")
    print("  [1] 导入并覆盖现有持仓")
    print("  [2] 追加到现有持仓")
    print("  [3] 仅查看，不导入")
    print("  [0] 取消")
    
    try:
        choice = input("\n请选择: ").strip()
    except KeyboardInterrupt:
        print("\n❌ 已取消")
        return
    
    if choice == "1":
        # 清空并导入
        for pos in position_manager.get_all_positions():
            position_manager.remove_position(pos.code)
        
        for pos_data in result['position_manager_format']:
            position_manager.add_position(
                code=pos_data['code'],
                name=pos_data['name'],
                buy_price=pos_data['buy_price'],
                shares=pos_data['shares'],
                stop_loss=pos_data['stop_loss'],
                target_price=pos_data['target_price']
            )
        
        print("✅ 已导入并覆盖现有持仓")
        
    elif choice == "2":
        # 追加导入
        for pos_data in result['position_manager_format']:
            existing = position_manager.get_position(pos_data['code'])
            if existing:
                # 询问是否覆盖
                print(f"  {pos_data['name']} ({pos_data['code']}) 已存在，是否覆盖? [y/N]: ", end="")
                try:
                    confirm = input().strip().lower()
                    if confirm == 'y':
                        position_manager.remove_position(pos_data['code'])
                        position_manager.add_position(
                            code=pos_data['code'],
                            name=pos_data['name'],
                            buy_price=pos_data['buy_price'],
                            shares=pos_data['shares'],
                            stop_loss=pos_data['stop_loss'],
                            target_price=pos_data['target_price']
                        )
                        print(f"    已更新")
                    else:
                        print(f"    已跳过")
                except KeyboardInterrupt:
                    print(f"    已跳过")
            else:
                position_manager.add_position(
                    code=pos_data['code'],
                    name=pos_data['name'],
                    buy_price=pos_data['buy_price'],
                    shares=pos_data['shares'],
                    stop_loss=pos_data['stop_loss'],
                    target_price=pos_data['target_price']
                )
        
        print("✅ 已追加导入")
        
    elif choice == "3":
        print("📊 仅查看模式，未导入")
        
    else:
        print("❌ 已取消")


def add_position_interactive():
    """交互式添加持仓"""
    print_header("添加持仓")
    
    try:
        code = input("股票代码: ").strip()
        name = input("股票名称: ").strip()
        buy_price = float(input("买入价格: ").strip())
        shares = int(input("持仓数量: ").strip())
        buy_date = input("买入日期 (YYYY-MM-DD, 可选): ").strip()
        
        if not buy_date:
            buy_date = datetime.now().strftime('%Y-%m-%d')
        
        # 计算默认止损止盈
        stop_loss = round(buy_price * 0.93, 2)
        target_price = round(buy_price * 1.15, 2)
        
        print(f"\n默认止损价: ¥{stop_loss} (-7%)")
        print(f"默认目标价: ¥{target_price} (+15%)")
        custom = input("是否修改? [y/N]: ").strip().lower()
        
        if custom == 'y':
            stop_loss = float(input("止损价: ").strip())
            target_price = float(input("目标价: ").strip())
        
        # 添加
        position_manager.add_position(
            code=code,
            name=name,
            buy_price=buy_price,
            shares=shares,
            buy_date=buy_date,
            stop_loss=stop_loss,
            target_price=target_price
        )
        
        print(f"\n✅ 已添加: {name} ({code})")
        
    except KeyboardInterrupt:
        print("\n❌ 已取消")
    except ValueError as e:
        print(f"\n❌ 输入错误: {e}")


def remove_position_interactive(code: str = None):
    """删除持仓"""
    if not code:
        print_header("删除持仓")
        show_positions()
        try:
            code = input("\n要删除的股票代码: ").strip()
        except KeyboardInterrupt:
            print("\n❌ 已取消")
            return
    
    pos = position_manager.get_position(code)
    if not pos:
        print(f"❌ 未找到持仓: {code}")
        return
    
    try:
        confirm = input(f"确认删除 {pos.name} ({code})? [y/N]: ").strip().lower()
        if confirm == 'y':
            position_manager.remove_position(code)
            print(f"✅ 已删除: {pos.name}")
        else:
            print("❌ 已取消")
    except KeyboardInterrupt:
        print("\n❌ 已取消")


def update_prices():
    """更新持仓股价"""
    print_header("更新持仓股价")
    print("正在获取最新行情...")
    
    try:
        from data_source import data_manager
        source = data_manager.get_source()
        
        if not source:
            print("❌ 数据源不可用")
            return
        
        positions = position_manager.get_all_positions()
        codes = [p.code for p in positions]
        
        if not codes:
            print("📭 没有持仓需要更新")
            return
        
        # 获取行情
        stocks = source.get_a_stock_spot()
        quotes = {s.code: {'price': s.price, 'change_percent': s.change_percent} for s in stocks if s.code in codes}
        
        # 更新
        position_manager.update_prices(quotes)
        
        print(f"✅ 已更新 {len(quotes)} 只股票价格")
        
    except Exception as e:
        print(f"⚠️ 更新失败: {e}")


def show_analysis():
    """AI持仓分析"""
    print_header("AI 持仓分析")
    
    positions = position_manager.get_all_positions()
    
    if not positions:
        print("📭 当前没有持仓")
        return
    
    # 转换为分析格式
    pos_data = []
    for p in positions:
        pos_data.append({
            'code': p.code,
            'name': p.name,
            'buy_price': p.buy_price,
            'current_price': p.current_price,
            'current_return': p.current_return,
            'shares': p.shares,
            'buy_date': p.buy_date,
            'stop_loss': p.stop_loss,
            'target_price': p.target_price
        })
    
    print("🤖 正在分析持仓...\n")
    
    result = deepseek_analyzer.analyze_positions(pos_data)
    
    if result.get('success'):
        print(result['analysis'])
    else:
        print("❌ 分析失败")


def check_alerts():
    """检查持仓预警"""
    print_header("持仓预警")
    
    alerts = position_manager.get_alert_positions()
    
    if not alerts:
        print("✅ 没有触发预警的持仓")
        return
    
    print(f"⚠️ 发现 {len(alerts)} 条预警\n")
    
    for alert in alerts:
        p = alert['position']
        emoji = '🔴' if alert['alert_type'] == 'stop_loss' else '🟢'
        print(f"{emoji} {p.name} ({p.code})")
        print(f"   预警类型: {'止损' if alert['alert_type'] == 'stop_loss' else '止盈'}")
        print(f"   当前价: ¥{p.current_price}")
        print(f"   {'止损价' if alert['alert_type'] == 'stop_loss' else '目标价'}: ¥{p.stop_loss if alert['alert_type'] == 'stop_loss' else p.target_price}")
        print(f"   盈亏: {p.current_return:+.2f}%")
        print()


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description='持仓管理工具 - 支持同花顺图片导入',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python3 position_cli.py                    # 查看持仓
  python3 position_cli.py --import-image     # 从同花顺截图导入
  python3 position_cli.py --add              # 手动添加
  python3 position_cli.py --remove 002594    # 删除指定股票
  python3 position_cli.py --update           # 更新股价
  python3 position_cli.py --analysis         # AI分析
  python3 position_cli.py --alerts           # 检查预警
  python3 position_cli.py --clear            # 清空所有持仓
        """
    )
    
    parser.add_argument('--import-image', action='store_true',
                       help='从同花顺截图导入持仓')
    parser.add_argument('--add', action='store_true',
                       help='手动添加持仓')
    parser.add_argument('--remove', type=str, metavar='CODE',
                       help='删除指定代码的持仓')
    parser.add_argument('--update', action='store_true',
                       help='更新持仓股价')
    parser.add_argument('--summary', action='store_true',
                       help='显示盈亏汇总')
    parser.add_argument('--analysis', action='store_true',
                       help='AI持仓分析')
    parser.add_argument('--alerts', action='store_true',
                       help='检查持仓预警')
    parser.add_argument('--clear', action='store_true',
                       help='清空所有持仓')
    
    args = parser.parse_args()
    
    # 执行对应操作
    if args.import_image:
        import_from_image_interactive()
    elif args.add:
        add_position_interactive()
    elif args.remove:
        remove_position_interactive(args.remove)
    elif args.update:
        update_prices()
    elif args.summary:
        show_summary()
    elif args.analysis:
        show_analysis()
    elif args.alerts:
        check_alerts()
    elif args.clear:
        try:
            confirm = input("⚠️ 确定清空所有持仓? 此操作不可恢复! [yes/N]: ").strip().lower()
            if confirm == 'yes':
                for pos in position_manager.get_all_positions():
                    position_manager.remove_position(pos.code)
                print("✅ 已清空所有持仓")
            else:
                print("❌ 已取消")
        except KeyboardInterrupt:
            print("\n❌ 已取消")
    else:
        # 默认显示持仓
        show_positions()
        print("\n💡 提示: 使用 --help 查看所有命令")


if __name__ == "__main__":
    main()