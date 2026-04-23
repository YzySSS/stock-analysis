#!/usr/bin/env python3
"""
测试盘前报告技术位显示 - 使用历史数据
"""

import sys
sys.path.insert(0, 'src')

from main import ReportGeneratorV10Plus
from stock_history_db import StockHistoryDB
from technical_analysis import calculate_technical_levels, format_technical_levels
import pandas as pd

# 初始化数据库（使用正确的单例函数）
from stock_history_db import get_stock_history_db
history_db = get_stock_history_db()

# 模拟选股结果（基于已有持仓或推荐记录）
sample_picks = [
    {
        'name': '银行ETF',
        'code': '159887',
        'price': 1.258,
        'change_pct': 0.32,
        'total_score': 72.5,
        'sector': '银行',
        'factors': {
            'technical': 14.5,
            'sentiment': 2.0,
            'sector': 24.0,
            'money_flow': 13.5,
            'risk': 13.0,
            'news_sentiment': 3.0
        }
    },
    {
        'name': '电力ETF',
        'code': '159611',
        'price': 1.133,
        'change_pct': 1.25,
        'total_score': 70.8,
        'sector': '电力',
        'factors': {
            'technical': 14.0,
            'sentiment': 2.2,
            'sector': 23.5,
            'money_flow': 14.0,
            'risk': 13.5,
            'news_sentiment': 2.8
        }
    },
    {
        'name': '顺丰控股',
        'code': '002352',
        'price': 35.71,
        'change_pct': 2.15,
        'total_score': 68.5,
        'sector': '交运',
        'factors': {
            'technical': 13.5,
            'sentiment': 2.0,
            'sector': 22.0,
            'money_flow': 14.5,
            'risk': 13.0,
            'news_sentiment': 2.5
        }
    }
]

print('=' * 70)
print('🌅 盘前报告技术位测试')
print('=' * 70)
print()

# 为每只股票计算技术位
print('1. 计算技术位...')
for pick in sample_picks:
    code = pick['code']
    price = pick['price']
    
    try:
        # 从数据库获取历史数据（价格列表）
        hist_prices = history_db.get_prices(code, days=60)
        
        if hist_prices and len(hist_prices) >= 20:
            levels = calculate_technical_levels(hist_prices, price)
            pick['technical_levels'] = levels.to_dict()
            print(f'   ✅ {pick["name"]} ({code}): 技术位已计算 ({len(hist_prices)}天数据)')
        else:
            # 数据不足时使用估算
            from technical_analysis import _estimate_simple_levels
            levels = _estimate_simple_levels(price)
            pick['technical_levels'] = levels.to_dict()
            print(f'   ⚠️ {pick["name"]} ({code}): 历史数据不足，使用估算')
    except Exception as e:
        print(f'   ❌ {pick["name"]} ({code}): 计算失败 - {e}')
        # 使用估算
        from technical_analysis import _estimate_simple_levels
        levels = _estimate_simple_levels(price)
        pick['technical_levels'] = levels.to_dict()

print()

# 生成报告
print('2. 生成盘前报告...')
reporter = ReportGeneratorV10Plus()
report = reporter.generate_premarket_report(sample_picks, quotes={})
print('   ✅ 报告生成完成')
print()

# 显示报告
print('=' * 70)
print('📋 盘前报告内容：')
print('=' * 70)
print()
print(report)
