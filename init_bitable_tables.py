#!/usr/bin/env python3
"""
初始化飞书多维表格 - 创建所有策略数据表
"""
import os
import sys
import requests

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
BITABLE_APP_TOKEN = os.getenv("FEISHU_BITABLE_TOKEN", "ZRR7bcleZanrLUsSG4zcYGoRnwh")

# 策略表格配置（启用状态可以通过环境变量覆盖）
ENABLED_STRATEGIES = os.getenv("ENABLED_STRATEGIES", "V10").split(",")

def get_table_name(strategy):
    """根据启用状态返回表格名称"""
    enabled = strategy in ENABLED_STRATEGIES
    if enabled:
        return f"{strategy}选股结果"
    else:
        return f"{strategy}选股结果（未启用）"

# 所有策略
ALL_STRATEGIES = ["V9", "V10", "V11"]

# 标准字段
def get_fields():
    return [
        {"name": "选股日期", "type": 5},  # DateTime
        {"name": "股票代码", "type": 1},  # Text
        {"name": "股票名称", "type": 1},  # Text
        {"name": "选出时价格", "type": 2},  # Number
        {"name": "第1天收盘价", "type": 2},
        {"name": "第2天收盘价", "type": 2},
        {"name": "第3天收盘价", "type": 2},
        {"name": "第1天涨幅%", "type": 2},
        {"name": "第2天涨幅%", "type": 2},
        {"name": "第3天涨幅%", "type": 2},
    ]

def main():
    print("=" * 60)
    print("飞书多维表格 - 策略数据表初始化")
    print("=" * 60)
    
    # 检查环境变量
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        print("\n❌ 错误：请设置飞书应用凭证")
        print("\n请执行以下命令或添加到 .env 文件：")
        print('  export FEISHU_APP_ID="cli_xxxxxxxx"')
        print('  export FEISHU_APP_SECRET="xxxxxxxx"')
        print("\n从飞书开发者后台获取：https://open.feishu.cn/app")
        sys.exit(1)
    
    # 获取 token
    print("\n1. 获取访问令牌...")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10
    )
    
    if resp.status_code != 200:
        print(f"❌ 获取令牌失败: HTTP {resp.status_code}")
        sys.exit(1)
    
    data = resp.json()
    if data.get("code") != 0:
        print(f"❌ 获取令牌失败: {data.get('msg')}")
        sys.exit(1)
    
    token = data["tenant_access_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("✅ 令牌获取成功")
    
    # 列出现有表格
    print("\n2. 检查现有表格...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BITABLE_APP_TOKEN}/tables"
    resp = requests.get(url, headers=headers, timeout=10)
    
    existing_tables = {}
    if resp.status_code == 200:
        data = resp.json()
        if data.get("code") == 0:
            for table in data["data"]["items"]:
                existing_tables[table["name"]] = table["table_id"]
                print(f"   已存在: {table['name']}")
    
    # 创建策略表格
    print(f"\n3. 创建策略数据表...")
    print(f"   启用的策略: {', '.join(ENABLED_STRATEGIES)}")
    
    fields = get_fields()
    
    for strategy in ALL_STRATEGIES:
        table_name = get_table_name(strategy)
        enabled = strategy in ENABLED_STRATEGIES
        
        if table_name in existing_tables:
            print(f"   ✅ {table_name}: 已存在")
            continue
        
        print(f"   🔄 创建: {table_name}...", end=" ")
        
        # 创建表格
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BITABLE_APP_TOKEN}/tables"
        payload = {"table": {"name": table_name, "fields": fields}}
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                table_id = data["data"]["table_id"]
                
                # 添加状态字段
                field_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BITABLE_APP_TOKEN}/tables/{table_id}/fields"
                field_payload = {
                    "field_name": "状态",
                    "type": 3,  # SingleSelect
                    "property": {
                        "options": [
                            {"name": "进行中", "color": 1},
                            {"name": "已完成", "color": 2}
                        ]
                    }
                }
                requests.post(field_url, headers=headers, json=field_payload, timeout=10)
                
                status = "启用" if enabled else "未启用"
                print(f"成功 (ID: {table_id[:8]}..., {status})")
            else:
                print(f"失败: {data.get('msg')}")
        else:
            print(f"失败: HTTP {resp.status_code}")
    
    # 最终表格列表
    print(f"\n4. 最终表格列表:")
    print("-" * 60)
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("code") == 0:
            for table in data["data"]["items"]:
                print(f"   📊 {table['name']}")
    
    print("\n" + "=" * 60)
    print("✅ 初始化完成！")
    print("=" * 60)
    print("\n使用说明:")
    print("  1. 启用的策略会记录选股数据到对应表格")
    print("  2. 未启用的策略表格为空白，作为占位")
    print("  3. 修改 ENABLED_STRATEGIES 环境变量可切换启用状态")
    print("\n示例:")
    print('  export ENABLED_STRATEGIES="V9,V10"  # 启用V9和V10')
    print('  export ENABLED_STRATEGIES="V10"     # 仅启用V10')

if __name__ == "__main__":
    main()
