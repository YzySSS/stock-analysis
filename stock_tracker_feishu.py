#!/usr/bin/env python3
"""
飞书多维表格 - 股票选股跟踪工具
记录盘前选股及后续3个交易日表现
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# ============ 配置 ============
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
BITABLE_APP_TOKEN = "ZRR7bcleZanrLUsSG4zcYGoRnwh"

class FeishuBitable:
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = self._get_tenant_access_token()
    
    def _get_tenant_access_token(self) -> str:
        """获取 tenant_access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        })
        return resp.json()["tenant_access_token"]
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }
    
    def list_tables(self, app_token: str) -> List[Dict]:
        """获取 base 中的所有表格"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
        resp = requests.get(url, headers=self._headers())
        return resp.json().get("data", {}).get("items", [])
    
    def create_table(self, app_token: str, table_name: str, fields: List[Dict]) -> Optional[str]:
        """创建新表格"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
        payload = {
            "table": {
                "name": table_name,
                "fields": fields
            }
        }
        resp = requests.post(url, headers=self._headers(), json=payload)
        result = resp.json()
        if result.get("code") == 0:
            return result["data"]["table_id"]
        print(f"创建表格失败: {result}")
        return None
    
    def list_fields(self, app_token: str, table_id: str) -> List[Dict]:
        """获取表格字段列表"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        resp = requests.get(url, headers=self._headers())
        return resp.json().get("data", {}).get("items", [])
    
    def add_field(self, app_token: str, table_id: str, field_name: str, field_type: int, property: dict = None) -> bool:
        """添加字段"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        payload = {"field_name": field_name, "type": field_type}
        if property:
            payload["property"] = property
        resp = requests.post(url, headers=self._headers(), json=payload)
        return resp.json().get("code") == 0
    
    def list_records(self, app_token: str, table_id: str) -> List[Dict]:
        """获取所有记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        records = []
        page_token = None
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params)
            data = resp.json().get("data", {})
            records.extend(data.get("items", []))
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
        return records
    
    def create_record(self, app_token: str, table_id: str, fields: Dict) -> Optional[str]:
        """创建记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        payload = {"fields": fields}
        resp = requests.post(url, headers=self._headers(), json=payload)
        result = resp.json()
        if result.get("code") == 0:
            return result["data"]["record"]["record_id"]
        print(f"创建记录失败: {result}")
        return None
    
    def update_record(self, app_token: str, table_id: str, record_id: str, fields: Dict) -> bool:
        """更新记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        payload = {"fields": fields}
        resp = requests.put(url, headers=self._headers(), json=payload)
        return resp.json().get("code") == 0


class StockTracker:
    """股票选股跟踪器"""
    
    # 字段类型常量
    FIELD_TEXT = 1
    FIELD_NUMBER = 2
    FIELD_SINGLE_SELECT = 3
    FIELD_MULTI_SELECT = 4
    FIELD_DATE = 5
    
    # 标准字段定义
    STANDARD_FIELDS = [
        ("选股日期", FIELD_DATE),
        ("股票代码", FIELD_TEXT),
        ("股票名称", FIELD_TEXT),
        ("选出时价格", FIELD_NUMBER),
        ("第1天收盘价", FIELD_NUMBER),
        ("第2天收盘价", FIELD_NUMBER),
        ("第3天收盘价", FIELD_NUMBER),
        ("第1天涨幅%", FIELD_NUMBER),
        ("第2天涨幅%", FIELD_NUMBER),
        ("第3天涨幅%", FIELD_NUMBER),
    ]
    
    def __init__(self, feishu: FeishuBitable, app_token: str):
        self.feishu = feishu
        self.app_token = app_token
        self._table_cache = {}
    
    def get_or_create_table(self, table_name: str) -> Optional[str]:
        """获取或创建表格"""
        # 先检查缓存
        if table_name in self._table_cache:
            return self._table_cache[table_name]
        
        # 列出所有表格
        tables = self.feishu.list_tables(self.app_token)
        for table in tables:
            if table["name"] == table_name:
                self._table_cache[table_name] = table["table_id"]
                return table["table_id"]
        
        # 创建新表格
        print(f"创建新表格: {table_name}")
        fields = [{"name": name, "type": ftype} for name, ftype in self.STANDARD_FIELDS]
        table_id = self.feishu.create_table(self.app_token, table_name, fields)
        if table_id:
            self._table_cache[table_name] = table_id
            # 添加状态字段（单选）
            self.feishu.add_field(
                self.app_token, table_id, "状态", self.FIELD_SINGLE_SELECT,
                property={"options": [
                    {"name": "进行中", "color": "blue"},
                    {"name": "已完成", "color": "green"}
                ]}
            )
        return table_id
    
    def add_stock_pick(self, table_name: str, pick_date: str, code: str, name: str, price: float) -> Optional[str]:
        """
        添加选股记录
        
        Args:
            table_name: 表格名称（如"V9选股"、"V10选股"）
            pick_date: 选股日期，格式 YYYY-MM-DD
            code: 股票代码
            name: 股票名称
            price: 选出时价格
        """
        table_id = self.get_or_create_table(table_name)
        if not table_id:
            return None
        
        # 日期格式转换（字符串转时间戳）
        dt = datetime.strptime(pick_date, "%Y-%m-%d")
        timestamp_ms = int(dt.timestamp() * 1000)
        
        fields = {
            "选股日期": timestamp_ms,
            "股票代码": code,
            "股票名称": name,
            "选出时价格": price,
            "状态": "进行中"
        }
        
        record_id = self.feishu.create_record(self.app_token, table_id, fields)
        if record_id:
            print(f"✅ 已添加: {name}({code}) @ {price}")
        return record_id
    
    def update_close_price(self, table_name: str, pick_date: str, code: str, 
                           day1_close: float = None, day2_close: float = None, day3_close: float = None):
        """
        更新收盘价并计算涨幅
        
        Args:
            table_name: 表格名称
            pick_date: 选股日期
            code: 股票代码
            day1_close: 第1天收盘价
            day2_close: 第2天收盘价  
            day3_close: 第3天收盘价
        """
        table_id = self.get_or_create_table(table_name)
        if not table_id:
            return False
        
        # 查找记录
        records = self.feishu.list_records(self.app_token, table_id)
        target_record = None
        
        for record in records:
            fields = record.get("fields", {})
            # 日期比较需要转换
            record_date_ms = fields.get("选股日期")
            if record_date_ms:
                record_date = datetime.fromtimestamp(record_date_ms / 1000).strftime("%Y-%m-%d")
                if record_date == pick_date and fields.get("股票代码") == code:
                    target_record = record
                    break
        
        if not target_record:
            print(f"❌ 未找到记录: {pick_date} {code}")
            return False
        
        # 获取选出时价格
        pick_price = target_record["fields"].get("选出时价格")
        if not pick_price:
            print(f"❌ 记录缺少选出时价格")
            return False
        
        # 构建更新字段
        update_fields = {}
        
        if day1_close is not None:
            update_fields["第1天收盘价"] = day1_close
            update_fields["第1天涨幅%"] = round((day1_close - pick_price) / pick_price * 100, 2)
        
        if day2_close is not None:
            update_fields["第2天收盘价"] = day2_close
            update_fields["第2天涨幅%"] = round((day2_close - pick_price) / pick_price * 100, 2)
        
        if day3_close is not None:
            update_fields["第3天收盘价"] = day3_close
            update_fields["第3天涨幅%"] = round((day3_close - pick_price) / pick_price * 100, 2)
            update_fields["状态"] = "已完成"
        
        success = self.feishu.update_record(
            self.app_token, table_id, target_record["record_id"], update_fields
        )
        
        if success:
            print(f"✅ 已更新: {code} 收盘价")
        return success
    
    def get_pending_records(self, table_name: str) -> List[Dict]:
        """获取所有进行中的记录（需要更新收盘价的）"""
        table_id = self.get_or_create_table(table_name)
        if not table_id:
            return []
        
        records = self.feishu.list_records(self.app_token, table_id)
        pending = []
        
        for record in records:
            fields = record.get("fields", {})
            if fields.get("状态") == "进行中":
                pending.append({
                    "record_id": record["record_id"],
                    "选股日期": fields.get("选股日期"),
                    "股票代码": fields.get("股票代码"),
                    "股票名称": fields.get("股票名称"),
                    "选出时价格": fields.get("选出时价格"),
                    "第1天收盘价": fields.get("第1天收盘价"),
                    "第2天收盘价": fields.get("第2天收盘价"),
                })
        
        return pending


# ============ 使用示例 ============

def main():
    """示例用法"""
    
    # 从环境变量读取（请确保设置了这些变量）
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    
    if not app_id or not app_secret:
        print("请设置环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        print("示例: export FEISHU_APP_ID=cli_xxx")
        print("       export FEISHU_APP_SECRET=xxx")
        return
    
    # 初始化
    feishu = FeishuBitable(app_id, app_secret)
    tracker = StockTracker(feishu, BITABLE_APP_TOKEN)
    
    # 示例1: 添加选股记录
    print("\n=== 添加选股记录 ===")
    tracker.add_stock_pick(
        table_name="V9选股",
        pick_date="2025-03-31",
        code="000001",
        name="平安银行",
        price=10.5
    )
    
    # 示例2: 更新收盘价（T+1）
    print("\n=== 更新第1天收盘价 ===")
    tracker.update_close_price(
        table_name="V9选股",
        pick_date="2025-03-31",
        code="000001",
        day1_close=10.8
    )
    
    # 示例3: 更新收盘价（T+2）
    print("\n=== 更新第2天收盘价 ===")
    tracker.update_close_price(
        table_name="V9选股",
        pick_date="2025-03-31",
        code="000001",
        day2_close=10.6
    )
    
    # 示例4: 更新收盘价（T+3，完成后状态变为"已完成"）
    print("\n=== 更新第3天收盘价 ===")
    tracker.update_close_price(
        table_name="V9选股",
        pick_date="2025-03-31",
        code="000001",
        day3_close=11.0
    )
    
    # 示例5: 查看进行中的记录
    print("\n=== 进行中的记录 ===")
    pending = tracker.get_pending_records("V9选股")
    for record in pending:
        print(f"  - {record['股票名称']}({record['股票代码']}) 选出价:{record['选出时价格']}")


if __name__ == "__main__":
    main()
