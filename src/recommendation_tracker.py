#!/usr/bin/env python3
"""
推荐记录与评估系统
记录每天的推荐股票，评估准确率
"""
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd

DATA_DIR = "/workspace/projects/workspace/股票分析项目/data"
RECORDS_FILE = os.path.join(DATA_DIR, "recommendation_records.json")


class RecommendationTracker:
    """推荐记录跟踪器"""
    
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.records = self._load_records()
    
    def _load_records(self) -> List[Dict]:
        """加载历史记录"""
        if os.path.exists(RECORDS_FILE):
            with open(RECORDS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    
    def _save_records(self):
        """保存记录"""
        with open(RECORDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.records, f, ensure_ascii=False, indent=2)
    
    def add_recommendation(self, date: str, time: str, report_type: str, 
                          picks: List[Dict], market_status: str = "unknown"):
        """
        添加推荐记录
        
        Args:
            date: 日期 (YYYY-MM-DD)
            time: 时间 (HH:MM)
            report_type: 报告类型 (盘前/盘中/盘后)
            picks: 推荐股票列表
            market_status: 市场状态 (开盘中/已收盘)
        """
        record = {
            "id": f"{date}_{time}_{report_type}",
            "date": date,
            "time": time,
            "report_type": report_type,
            "market_status": market_status,
            "picks": []
        }
        
        for pick in picks:
            record["picks"].append({
                "code": pick['code'],
                "name": pick['name'],
                "price_at_rec": pick['price'],
                "change_pct_at_rec": pick['change_pct'],
                "score": pick['total_score'],
                "factors": {
                    "technical": pick['factors'].technical,
                    "fundamental": pick['factors'].fundamental,
                    "institution": pick['factors'].institution,
                    "risk": pick['factors'].risk
                },
                # 收盘后评估字段
                "price_at_close": None,
                "change_pct_day": None,
                "accuracy": None,  # 推荐准确率
                "evaluated": False
            })
        
        # 检查是否已存在相同记录，存在则更新
        existing_idx = None
        for i, r in enumerate(self.records):
            if r['id'] == record['id']:
                existing_idx = i
                break
        
        if existing_idx is not None:
            self.records[existing_idx] = record
        else:
            self.records.append(record)
        
        self._save_records()
        print(f"✅ 已记录 {date} {time} {report_type} 推荐 ({len(picks)}只)")
    
    def update_close_price(self, date: str, code: str, close_price: float, 
                          day_change_pct: float):
        """
        更新收盘价和当日表现（盘后评估）
        
        Args:
            date: 日期
            code: 股票代码
            close_price: 收盘价
            day_change_pct: 当日涨跌幅
        """
        updated = False
        
        for record in self.records:
            if record['date'] == date:
                for pick in record['picks']:
                    if pick['code'] == code and not pick['evaluated']:
                        pick['price_at_close'] = close_price
                        pick['change_pct_day'] = day_change_pct
                        
                        # 计算推荐准确率
                        # 规则：推荐时涨幅>5%且当日继续涨，或推荐时评分高且当日表现好
                        if pick['change_pct_at_rec'] > 5:
                            # 强势推荐：当日应该继续涨或跌幅<2%
                            if day_change_pct > 0 or day_change_pct > -2:
                                pick['accuracy'] = 'correct'
                            else:
                                pick['accuracy'] = 'wrong'
                        elif pick['score'] >= 55:
                            # 高评分推荐：当日应该跑赢大盘或跌幅<3%
                            if day_change_pct > -3:
                                pick['accuracy'] = 'correct'
                            else:
                                pick['accuracy'] = 'wrong'
                        else:
                            # 一般推荐
                            if day_change_pct > -5:
                                pick['accuracy'] = 'correct'
                            else:
                                pick['accuracy'] = 'wrong'
                        
                        pick['evaluated'] = True
                        updated = True
        
        if updated:
            self._save_records()
    
    def get_daily_recommendations(self, date: str) -> List[Dict]:
        """获取某天的所有推荐"""
        return [r for r in self.records if r['date'] == date]
    
    def get_accuracy_stats(self, days: int = 7) -> Dict:
        """
        获取准确率统计
        
        Args:
            days: 统计最近N天
        
        Returns:
            统计结果
        """
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        total_recs = 0
        correct = 0
        wrong = 0
        unevaluated = 0
        
        daily_stats = {}
        
        for record in self.records:
            if record['date'] < cutoff_date:
                continue
            
            date = record['date']
            if date not in daily_stats:
                daily_stats[date] = {'total': 0, 'correct': 0, 'wrong': 0}
            
            for pick in record['picks']:
                if pick['evaluated']:
                    total_recs += 1
                    daily_stats[date]['total'] += 1
                    
                    if pick['accuracy'] == 'correct':
                        correct += 1
                        daily_stats[date]['correct'] += 1
                    else:
                        wrong += 1
                        daily_stats[date]['wrong'] += 1
                else:
                    unevaluated += 1
        
        accuracy_rate = (correct / total_recs * 100) if total_recs > 0 else 0
        
        return {
            'period_days': days,
            'total_recommendations': total_recs,
            'correct': correct,
            'wrong': wrong,
            'unevaluated': unevaluated,
            'accuracy_rate': round(accuracy_rate, 2),
            'daily_breakdown': daily_stats
        }
    
    def generate_weekly_report(self) -> str:
        """生成周报"""
        stats = self.get_accuracy_stats(days=7)
        
        lines = []
        lines.append("\n" + "="*70)
        lines.append("📊 V8选股策略 - 周度准确率报告")
        lines.append("="*70)
        lines.append(f"统计周期: 最近{stats['period_days']}天")
        lines.append(f"总推荐数: {stats['total_recommendations']}只")
        lines.append(f"正确推荐: {stats['correct']}只 ({stats['accuracy_rate']}%)")
        lines.append(f"错误推荐: {stats['wrong']}只")
        lines.append(f"待评估: {stats['unevaluated']}只")
        lines.append("-"*70)
        
        # 每日明细
        if stats['daily_breakdown']:
            lines.append("\n📅 每日明细:")
            for date, day_stat in sorted(stats['daily_breakdown'].items()):
                day_accuracy = (day_stat['correct'] / day_stat['total'] * 100) if day_stat['total'] > 0 else 0
                lines.append(f"  {date}: {day_stat['correct']}/{day_stat['total']} 正确 ({day_accuracy:.1f}%)")
        
        # 建议
        lines.append("\n💡 策略评估:")
        if stats['accuracy_rate'] >= 70:
            lines.append("  ✅ 准确率优秀(≥70%)，当前策略效果良好，继续保持")
        elif stats['accuracy_rate'] >= 50:
            lines.append("  ⚠️ 准确率一般(50-70%)，建议微调因子权重")
            lines.append("     建议: 增加财务因子权重，降低技术因子权重")
        else:
            lines.append("  🔴 准确率偏低(<50%)，需要重新评估策略")
            lines.append("     建议: 1) 检查数据源质量 2) 调整因子组合 3) 增加风险因子权重")
        
        lines.append("="*70)
        
        return "\n".join(lines)
    
    def get_today_recommendations_for_review(self, date: str = None) -> List[Dict]:
        """获取今日推荐用于盘后回顾"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        today_recs = self.get_daily_recommendations(date)
        
        # 按时间排序：盘前 -> 盘中 -> 盘后
        type_order = {'盘前': 0, '盘中': 1, '盘后': 2}
        today_recs.sort(key=lambda x: type_order.get(x['report_type'], 99))
        
        return today_recs


if __name__ == "__main__":
    # 测试
    tracker = RecommendationTracker()
    
    # 模拟添加今日推荐
    test_picks = [
        {'code': '002565', 'name': '顺灏股份', 'price': 14.74, 'change_pct': 10.0, 
         'total_score': 61.2, 'factors': type('obj', (object,), {'technical': 80, 'fundamental': 50, 'institution': 50, 'risk': 65})},
        {'code': '600683', 'name': '京投发展', 'price': 10.60, 'change_pct': 9.96,
         'total_score': 61.2, 'factors': type('obj', (object,), {'technical': 80, 'fundamental': 50, 'institution': 50, 'risk': 65})},
    ]
    
    tracker.add_recommendation('2026-03-18', '08:50', '盘前', test_picks)
    
    # 模拟更新收盘价
    tracker.update_close_price('2026-03-18', '002565', 15.50, 5.2)
    tracker.update_close_price('2026-03-18', '600683', 10.30, -2.8)
    
    # 生成周报
    print(tracker.generate_weekly_report())
