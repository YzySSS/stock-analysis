#!/usr/bin/env python3
"""
盘前选股数据源测试
测试以下数据的可获取性：
1. 北向资金（沪深港通）
2. 龙虎榜数据
3. 美股板块（费城半导体等）
4. 人气榜（同花顺/东财）
5. 昨日主力净流入
"""

import sys
sys.path.insert(0, 'src')

import requests
import json
from datetime import datetime, timedelta

print("="*70)
print("🔍 盘前选股数据源测试")
print("="*70)

# 1. 测试北向资金（东方财富接口）
print("\n1️⃣ 测试北向资金数据...")
try:
    url = "http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&volt=2&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f60,f107,f108,f111,f112,f113,f114,f115,f116,f117,f118,f119,f120,f121,f122,f123,f124,f125,f126,f127,f128,f129,f130,f131,f132,f133,f134,f135,f136,f137,f138,f139,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f150,f151,f152,f153,f154,f155,f156,f157,f158,f159,f160,f161,f162,f163,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f193,f194,f195,f196,f197,f198,f199,f200&secid=1.000001"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()
        print(f"   ✅ 东方财富接口可访问")
        # 尝试获取北向资金字段
        if 'data' in data:
            print(f"   数据示例: {list(data['data'].keys())[:5]}")
    else:
        print(f"   ⚠️ 接口返回状态码: {response.status_code}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 2. 测试龙虎榜数据
print("\n2️⃣ 测试龙虎榜数据...")
try:
    # 东方财富龙虎榜接口
    url = "http://data.eastmoney.com/stock/lhb.html"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        print(f"   ✅ 龙虎榜页面可访问")
    else:
        print(f"   ⚠️ 页面返回状态码: {response.status_code}")
        
    # 尝试API接口
    url2 = "http://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SECURITY_CODE&sortTypes=-1&pageSize=50&pageNumber=1&reportName=RPT_DMSK_TS&columns=SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,TOTAL_BILLBOARD_AMT,DEAL_NET_RATIO,ACCUM_AMOUNT&source=WEB&client=WEB"
    response2 = requests.get(url2, timeout=10)
    if response2.status_code == 200:
        data = response2.json()
        if data.get('result') and data['result'].get('data'):
            print(f"   ✅ 龙虎榜API可用，获取到 {len(data['result']['data'])} 条数据")
            sample = data['result']['data'][0]
            print(f"   示例: {sample.get('SECURITY_NAME_ABBR')}({sample.get('SECURITY_CODE')}) 净买入: {sample.get('BILLBOARD_NET_AMT')}")
    else:
        print(f"   ⚠️ API返回状态码: {response2.status_code}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 3. 测试美股板块数据
print("\n3️⃣ 测试美股板块数据...")
try:
    # 费城半导体指数(SOX) - Alpha Vantage或Yahoo Finance
    # 先测试Yahoo Finance
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5ESOX"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()
        if 'chart' in data and data['chart'].get('result'):
            result = data['chart']['result'][0]
            meta = result.get('meta', {})
            print(f"   ✅ 费城半导体指数可获取")
            print(f"   最新价: {meta.get('regularMarketPrice')}")
            print(f"   前一交易日: {meta.get('chartPreviousClose')}")
        else:
            print(f"   ⚠️ 数据格式异常")
    else:
        print(f"   ⚠️ 接口返回状态码: {response.status_code}")
        print(f"   可能需要代理或API Key")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 4. 测试同花顺人气榜
print("\n4️⃣ 测试同花顺人气榜...")
try:
    # 同花顺人气榜接口
    url = "http://basic.10jqka.com.cn/api/stockphb/"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        print(f"   ✅ 同花顺接口可访问")
        # 尝试解析数据
        try:
            data = response.json()
            print(f"   数据字段: {list(data.keys())[:5]}")
        except:
            print(f"   ⚠️ 返回非JSON数据，可能需要特殊处理")
    else:
        print(f"   ⚠️ 接口返回状态码: {response.status_code}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 5. 测试东方财富人气榜
print("\n5️⃣ 测试东方财富人气榜...")
try:
    url = "http://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/Index?type=web&code=SH000001"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        print(f"   ✅ 东财页面可访问")
    else:
        print(f"   ⚠️ 页面返回状态码: {response.status_code}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 6. 测试昨日主力净流入（已有数据）
print("\n6️⃣ 测试昨日主力净流入（本地数据）...")
try:
    from stock_history_db import StockHistoryDB
    db = StockHistoryDB()
    # 检查是否有资金流向相关表
    import sqlite3
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        if 'fund_flow' in tables or 'money_flow' in tables:
            print(f"   ✅ 发现资金流向表")
        else:
            print(f"   ⚠️ 无资金流向表，需要额外获取")
            print(f"   现有表: {tables[:10]}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

print("\n" + "="*70)
print("📊 测试结果汇总")
print("="*70)
print("""
数据源可行性评估:

✅ 高可行性（已有/易获取）:
   - 昨日收盘价/涨幅/成交量（本地数据库）
   - 历史均线/突破/RSI（本地计算）
   - 龙虎榜数据（东方财富API）

⚠️ 中可行性（需要开发）:
   - 北向资金（东财接口可访问，需封装）
   - 人气榜（同花顺/东财需解析）

❓ 低可行性（可能受限）:
   - 美股板块（可能需要代理或付费API）
   - 实时主力资金（可能需要付费数据源）

建议实施顺序:
1. 先用本地数据重构技术因子（昨日数据）
2. 增加龙虎榜数据（已有API）
3. 尝试封装北向资金接口
4. 人气榜作为可选增强
""")
