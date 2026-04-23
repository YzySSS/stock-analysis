#!/usr/bin/env python3
"""
股票筛选定时任务 - 每日选股策略
每天早上运行，输出TOP 3推荐
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
from datetime import datetime

# 导入V8筛选器
from scripts.screen_sector_v8 import SectorScreenerV8


def run_daily_screening():
    """每日选股策略"""
    print(f"\n{'='*60}")
    print(f"📅 每日选股策略 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    
    # 运行筛选器，输出TOP 3，简洁模式
    screener = SectorScreenerV8()
    results = screener.run(top_n=3, for_cron=True)
    
    # 格式化输出，方便飞书推送
    output = {
        "date": datetime.now().strftime('%Y-%m-%d'),
        "time": datetime.now().strftime('%H:%M'),
        "strategy": "全A股多因子选股",
        "recommendations": []
    }
    
    for i, stock in enumerate(results, 1):
        output["recommendations"].append({
            "rank": i,
            "code": stock['code'],
            "name": stock['name'],
            "price": stock['price'],
            "change_pct": stock['change_pct'],
            "score": stock['total_score'],
            "factors": {
                "technical": stock['factors'].technical,
                "fundamental": stock['factors'].fundamental,
                "institution": stock['factors'].institution,
                "risk": stock['factors'].risk
            }
        })
    
    # 保存结果
    output_file = f"/workspace/projects/workspace/股票分析项目/reports/daily_pick_{datetime.now().strftime('%Y%m%d')}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 结果已保存: {output_file}")
    
    return output


if __name__ == "__main__":
    run_daily_screening()
