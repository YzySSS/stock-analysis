#!/usr/bin/env python3
"""
飞书推送测试 - V8选股结果
"""
import json
import requests
import os
from datetime import datetime

# 飞书Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

# 读取选股结果
REPORT_FILE = "/workspace/projects/workspace/股票分析项目/reports/daily_pick_20260318.json"

def load_report():
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def format_message(data):
    """格式化飞书消息"""
    date = data['date']
    picks = data['recommendations']
    
    # 构建卡片内容
    stock_cards = []
    for pick in picks:
        f = pick['factors']
        stock_cards.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**{pick['rank']}. {pick['name']} ({pick['code']})**\n"
                          f"💰 价格: ¥{pick['price']:.2f} | 📈 涨幅: {pick['change_pct']:+.2f}%\n"
                          f"⭐ 综合评分: {pick['score']:.1f}\n"
                          f"📊 因子: 技术{f['technical']} | 财务{f['fundamental']} | 机构{f['institution']} | 风险{f['risk']}"
            }
        })
        stock_cards.append({"tag": "hr"})
    
    # 移除最后一个hr
    if stock_cards:
        stock_cards.pop()
    
    message = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"📊 V8全A股选股策略 | {date}"
                },
                "template": "orange"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "🏆 **TOP 3 推荐** (基于4914只全A股多因子评分)"
                    }
                },
                {
                    "tag": "hr"
                }
            ] + stock_cards + [
                {
                    "tag": "hr"
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": "💡 评分因子：技术30% | 财务25% | 机构15% | 风险15%\n数据来源：新浪财经 + 聚宽"
                        }
                    ]
                }
            ]
        }
    }
    
    return message

def send_to_feishu():
    """发送到飞书"""
    try:
        data = load_report()
        message = format_message(data)
        
        response = requests.post(
            FEISHU_WEBHOOK,
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        result = response.json()
        if result.get('code') == 0:
            print("✅ 飞书推送成功！")
            return True
        else:
            print(f"❌ 飞书推送失败: {result}")
            return False
            
    except Exception as e:
        print(f"❌ 推送异常: {e}")
        return False

if __name__ == "__main__":
    print("正在推送V8选股结果到飞书...")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("-" * 60)
    send_to_feishu()
