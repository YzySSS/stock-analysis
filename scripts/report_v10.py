#!/usr/bin/env python3
"""
股票报告 V10 - 完整版（含持仓分析+详细说明）
- V8全A股选股 TOP 3
- 持仓股分析（7只）
- 买卖建议
- 详细说明
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
import requests
import time
from datetime import datetime
from typing import List, Dict

# 导入V8筛选器和追踪器
from scripts.screen_sector_v8 import SectorScreenerV8
from config.positions import get_all_positions, Position
from src.recommendation_tracker import RecommendationTracker


class SinaAPI:
    """新浪财经API"""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def get_quotes(self, codes: List[str]) -> Dict[str, Dict]:
        """获取行情"""
        formatted = [f"sh{c}" if c.startswith('6') else f"sz{c}" for c in codes]
        url = f"https://hq.sinajs.cn/list={','.join(formatted)}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = 'gbk'
            return self._parse(response.text, codes)
        except Exception as e:
            print(f"获取行情失败: {e}")
            return {}
    
    def _parse(self, text: str, codes: List[str]) -> Dict:
        results = {}
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines):
            if i >= len(codes):
                break
            code = codes[i]
            if '=""' in line:
                continue
            
            try:
                data = line.split('="')[1].rstrip('";')
                parts = data.split(',')
                if len(parts) < 33:
                    continue
                
                name = parts[0]
                prev = float(parts[2]) if parts[2] else 0
                price = float(parts[3]) if parts[3] else 0
                change = ((price - prev) / prev * 100) if prev else 0
                
                results[code] = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change, 2),
                    'prev_close': prev
                }
            except:
                continue
        
        return results


class StockReportV10:
    """V10 完整报告"""
    
    def __init__(self):
        self.sina = SinaAPI()
        self.positions = get_all_positions()
        self.tracker = RecommendationTracker()
    
    def analyze_positions(self) -> List[Dict]:
        """分析持仓股"""
        print("\n📊 分析持仓股...")
        
        codes = [p.code for p in self.positions]
        quotes = self.sina.get_quotes(codes)
        
        results = []
        for pos in self.positions:
            code = pos.code
            quote = quotes.get(code, {})
            
            current_price = quote.get('price', pos.current_price or pos.cost_price)
            change_pct = quote.get('change_pct', 0)
            
            # 计算盈亏
            cost_total = pos.cost_price * pos.quantity
            current_total = current_price * pos.quantity
            profit_amount = current_total - cost_total
            profit_pct = (profit_amount / cost_total * 100) if cost_total > 0 else 0
            
            # 买卖建议
            advice = self._get_position_advice(profit_pct, change_pct)
            
            results.append({
                'code': code,
                'name': pos.name,
                'cost_price': pos.cost_price,
                'current_price': current_price,
                'quantity': pos.quantity,
                'profit_amount': profit_amount,
                'profit_pct': profit_pct,
                'today_change': change_pct,
                'advice': advice,
                'market_value': current_total
            })
        
        # 按盈亏排序
        results.sort(key=lambda x: x['profit_pct'], reverse=True)
        return results
    
    def _get_position_advice(self, profit_pct: float, today_change: float) -> Dict:
        """生成持仓建议"""
        if profit_pct > 15:
            return {
                'action': '🔴 强烈建议止盈',
                'suggestion': '盈利超过15%，建议减仓30-50%锁定利润',
                'level': 'sell'
            }
        elif profit_pct > 10:
            return {
                'action': '🟡 建议部分止盈',
                'suggestion': '盈利10-15%，可考虑减仓20-30%',
                'level': 'watch'
            }
        elif profit_pct > 5:
            return {
                'action': '🟢 建议持有',
                'suggestion': '盈利5-10%，趋势良好，继续持有',
                'level': 'hold'
            }
        elif profit_pct > -5:
            return {
                'action': '⚪ 观望',
                'suggestion': '盈亏在5%以内，正常波动，设好止损位',
                'level': 'watch'
            }
        elif profit_pct > -10:
            return {
                'action': '🟠 关注止损',
                'suggestion': '亏损5-10%，关注支撑位，准备减仓',
                'level': 'warning'
            }
        else:
            return {
                'action': '🔴 建议止损',
                'suggestion': '亏损超过10%，建议严格止损或减仓',
                'level': 'sell'
            }
    
    def get_v8_picks(self) -> List[Dict]:
        """获取V8选股"""
        print("\n🔍 运行V8全A股选股...")
        screener = SectorScreenerV8()
        picks = screener.run(top_n=3, for_cron=True)
        return picks
    
    def _get_pick_advice(self, pick: Dict) -> str:
        """生成选股建议"""
        score = pick['total_score']
        change = pick['change_pct']
        
        if score >= 60 and change > 5:
            return "⭐ 强势突破，可关注开盘表现，适合激进型投资者"
        elif score >= 55 and change > 0:
            return "✅ 评分较高，基本面良好，可考虑逢低布局"
        elif score >= 50:
            return "➡️ 评分中等，建议观望等待更好的买点"
        else:
            return "⚠️ 评分一般，暂不推荐"
    
    def generate_report(self, report_type: str = "盘前") -> str:
        """生成完整报告"""
        print(f"\n{'='*70}")
        print(f"📋 股票报告 V10 - {report_type}报告")
        print(f"{'='*70}")
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        # 1. 持仓分析
        position_analysis = self.analyze_positions()
        
        # 2. V8选股
        v8_picks = self.get_v8_picks()
        
        # 构建报告
        report_lines = []
        report_lines.append(f"\n{'='*70}")
        report_lines.append(f"📊 股票报告 V10 | {datetime.now().strftime('%Y-%m-%d %H:%M')} | {report_type}")
        report_lines.append(f"{'='*70}")
        
        # 持仓汇总
        total_cost = sum(p['cost_price'] * p['quantity'] for p in position_analysis)
        total_value = sum(p['market_value'] for p in position_analysis)
        total_profit = total_value - total_cost
        total_profit_pct = (total_profit / total_cost * 100) if total_cost > 0 else 0
        
        report_lines.append(f"\n💼 持仓汇总 (共{len(position_analysis)}只)")
        report_lines.append(f"总成本: ¥{total_cost:,.2f}")
        report_lines.append(f"总市值: ¥{total_value:,.2f}")
        report_lines.append(f"总盈亏: ¥{total_profit:,.2f} ({total_profit_pct:+.2f}%)")
        report_lines.append("-" * 70)
        
        # 持仓明细
        report_lines.append(f"\n📈 持仓明细 (按盈亏排序)")
        for i, p in enumerate(position_analysis, 1):
            emoji = '🟢' if p['profit_pct'] >= 0 else '🔴'
            report_lines.append(f"\n{i}. {emoji} {p['name']} ({p['code']})")
            report_lines.append(f"   成本: ¥{p['cost_price']:.3f} → 现价: ¥{p['current_price']:.3f}")
            report_lines.append(f"   持仓: {p['quantity']}股 | 市值: ¥{p['market_value']:,.2f}")
            report_lines.append(f"   盈亏: ¥{p['profit_amount']:+,.2f} ({p['profit_pct']:+.2f}%)")
            report_lines.append(f"   今日: {p['today_change']:+.2f}%")
            report_lines.append(f"   建议: {p['advice']['action']}")
            report_lines.append(f"   说明: {p['advice']['suggestion']}")
        
        # V8选股
        report_lines.append(f"\n{'='*70}")
        report_lines.append(f"🎯 V8全A股选股策略 - TOP 3推荐")
        report_lines.append(f"{'='*70}")
        report_lines.append(f"基于4914只全A股多因子评分")
        report_lines.append("-" * 70)
        
        for i, pick in enumerate(v8_picks, 1):
            f = pick['factors']
            advice = self._get_pick_advice(pick)
            
            report_lines.append(f"\n{i}. ⭐ {pick['name']} ({pick['code']})")
            report_lines.append(f"   当前价: ¥{pick['price']:.2f}")
            report_lines.append(f"   涨跌幅: {pick['change_pct']:+.2f}%")
            report_lines.append(f"   综合评分: {pick['total_score']:.1f}")
            report_lines.append(f"   因子得分: 技术{f.technical} | 财务{f.fundamental} | 机构{f.institution} | 风险{f.risk}")
            report_lines.append(f"   选股说明: {advice}")
        
        report_lines.append(f"\n{'='*70}")
        report_lines.append("💡 免责声明: 以上分析仅供参考，不构成投资建议。股市有风险，投资需谨慎。")
        report_lines.append(f"{'='*70}")
        
        report_text = "\n".join(report_lines)
        
        # 保存报告
        self._save_report(report_text, report_type)
        
        # 记录推荐到追踪器（盘后报告除外，盘后报告使用V10+版本）
        if report_type in ['盘前', '盘中']:
            now = datetime.now()
            self.tracker.add_recommendation(
                date=now.strftime('%Y-%m-%d'),
                time=now.strftime('%H:%M'),
                report_type=report_type,
                picks=v8_picks,
                market_status='开盘中' if report_type == '盘中' else '盘前'
            )
        
        return report_text
    
    def _save_report(self, content: str, report_type: str):
        """保存报告"""
        reports_dir = "/workspace/projects/workspace/股票分析项目/reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"{reports_dir}/report_v10_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"\n💾 报告已保存: {filename}")
    
    def send_to_feishu(self, report_text: str):
        """简化版飞书推送（文字）"""
        FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"
        
        # 取前3只持仓和前3只推荐做摘要
        message = {
            "msg_type": "text",
            "content": {
                "text": f"📊 V10股票报告 | {datetime.now().strftime('%m-%d %H:%M')}\n\n"
                       f"{report_text[:2000]}...\n\n"
                       f"【详细报告见文件】"
            }
        }
        
        try:
            response = requests.post(
                FEISHU_WEBHOOK,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            result = response.json()
            if result.get('code') == 0:
                print("✅ 飞书推送成功！")
            else:
                print(f"⚠️ 飞书推送: {result}")
        except Exception as e:
            print(f"⚠️ 飞书推送异常: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='股票报告 V10')
    parser.add_argument('--type', choices=['盘前', '盘中', '盘后'], default='盘前')
    parser.add_argument('--push', action='store_true', help='推送到飞书')
    args = parser.parse_args()
    
    report = StockReportV10()
    content = report.generate_report(args.type)
    
    print(content)
    
    if args.push:
        report.send_to_feishu(content)


if __name__ == "__main__":
    main()
