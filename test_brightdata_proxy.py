#!/usr/bin/env python3
"""
Bright Data 住宅代理测试脚本
测试 AkShare 通过代理访问东方财富
"""

import os
import requests
import time

# ==================== 配置 ====================
# Bright Data 代理配置
PROXY_HOST = "brd.superproxy.io"
PROXY_PORT = "33335"
PROXY_USER = "brd-customer-hl_8abbb7fa-zone-isp_proxy1"
PROXY_PASS = "1chayfaf4h24"

# SSL 证书路径
CERT_PATH = os.path.join(os.path.dirname(__file__), "brightdata_ca.crt")

# 构建代理 URL
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

# 设置环境变量代理
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL

print("=" * 60)
print("Bright Data 住宅代理测试")
print("=" * 60)
print(f"代理服务器: {PROXY_HOST}:{PROXY_PORT}")
print(f"代理用户: {PROXY_USER}")
print(f"SSL证书: {CERT_PATH}")
print()

# ==================== 测试 1: 基础连通性 ====================
print("[测试 1] 基础代理连通性...")
try:
    resp = requests.get(
        "https://geo.brdtest.com/welcome.txt",
        proxies={"http": PROXY_URL, "https": PROXY_URL},
        timeout=10,
        verify=CERT_PATH  # 使用 SSL 证书
    )
    print(f"✅ 代理连通性测试: {resp.status_code}")
    print(f"   响应内容: {resp.text[:100]}")
except Exception as e:
    print(f"❌ 代理连通性测试失败: {e}")

print()

# ==================== 测试 2: 东方财富接口 ====================
print("[测试 2] 东方财富接口 (AkShare 底层)...")
try:
    resp = requests.get(
        "https://push2.eastmoney.com/api/qt/clist/get",
        proxies={"http": PROXY_URL, "https": PROXY_URL},
        timeout=30,
        verify=CERT_PATH,  # 使用 SSL 证书
        params={
            "pn": 1,
            "pz": 20,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            "fs": "m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f2,f3"
        }
    )
    if resp.status_code == 200:
        data = resp.json()
        stock_count = len(data.get("data", {}).get("diff", []))
        print(f"✅ 东方财富接口访问成功")
        print(f"   获取到 {stock_count} 只股票数据")
    else:
        print(f"❌ 东方财富接口返回: {resp.status_code}")
except Exception as e:
    print(f"❌ 东方财富接口测试失败: {e}")

print()

# ==================== 测试 3: AkShare 完整测试 ====================
print("[测试 3] AkShare 股票数据获取...")
try:
    import akshare as ak
    
    # 测试获取A股实时行情
    df = ak.stock_zh_a_spot_em()
    print(f"✅ AkShare 测试成功")
    print(f"   获取到 {len(df)} 只股票")
    if len(df) > 0:
        print(f"   样本: {df.iloc[0]['名称']}({df.iloc[0]['代码']}): {df.iloc[0]['最新价']}元")
except Exception as e:
    print(f"❌ AkShare 测试失败: {e}")

print()

# ==================== 测试 4: 历史数据获取 ====================
print("[测试 4] AkShare 历史数据获取...")
for i in range(3):
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(
            symbol='000001',
            period='daily',
            start_date='20260301',
            end_date='20260402',
            adjust='qfq'
        )
        print(f"✅ 历史数据测试成功")
        print(f"   获取到 {len(df)} 条记录")
        print(f"   最新日期: {df.iloc[-1]['日期'] if len(df) > 0 else 'N/A'}")
        break
    except Exception as e:
        print(f"   重试{i+1}失败: {str(e)[:80]}")
        if i < 2:
            time.sleep(2)

print()
print("=" * 60)
print("测试完成")
print("=" * 60)
print("如果以上测试都通过，说明代理配置成功！")
print("可以将代理配置集成到 main.py 中使用 AkShare 作为主数据源。")
