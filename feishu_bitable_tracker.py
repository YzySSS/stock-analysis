"""
飞书多维表格集成模块 - 用于记录选股和跟踪表现
支持多策略表格，未启用策略显示为「策略名（未启用）」
"""
import os
import requests
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set

# 尝试导入 logger，如果不存在则使用 print
try:
    from utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

# 配置
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
BITABLE_APP_TOKEN = os.getenv("FEISHU_BITABLE_TOKEN", "ZRR7bcleZanrLUsSG4zcYGoRnwh")

# 策略配置：名称和启用状态
# 可以通过环境变量覆盖，如：ENABLED_STRATEGIES=V9,V10
DEFAULT_STRATEGIES = {
    "V9": {"enabled": False, "name": "V9_选股"},   # 默认未启用
    "V10": {"enabled": True, "name": "V10_选股"},  # 默认启用
    "V11": {"enabled": False, "name": "V11_选股"}, # 默认未启用
}


def get_strategy_config() -> Dict[str, Dict]:
    """
    获取策略配置
    支持通过环境变量 ENABLED_STRATEGIES 覆盖
    例如：ENABLED_STRATEGIES=V9,V10 表示启用V9和V10
    """
    config = DEFAULT_STRATEGIES.copy()
    
    # 从环境变量读取启用的策略
    enabled_env = os.getenv("ENABLED_STRATEGIES", "")
    if enabled_env:
        enabled_list = [s.strip() for s in enabled_env.split(",")]
        for strategy in config:
            config[strategy]["enabled"] = strategy in enabled_list
    
    return config


def get_table_name(strategy: str, config: Dict = None) -> str:
    """
    获取表格名称
    如果策略未启用，返回「策略名_disabled」
    """
    if config is None:
        config = get_strategy_config()
    
    strategy_info = config.get(strategy, {"enabled": False, "name": f"{strategy}_选股"})
    base_name = strategy_info["name"]
    
    if not strategy_info["enabled"]:
        return f"{base_name}_disabled"
    
    return base_name


def get_all_strategy_names() -> List[str]:
    """获取所有策略的表格名称（包括未启用的）"""
    config = get_strategy_config()
    return [get_table_name(s, config) for s in config.keys()]


class FeishuBitableClient:
    """飞书多维表格客户端"""
    
    def __init__(self):
        self.app_id = FEISHU_APP_ID
        self.app_secret = FEISHU_APP_SECRET
        self.app_token = BITABLE_APP_TOKEN
        self._token = None
        self._tables_cache = {}
    
    def _get_token(self) -> str:
        """获取 tenant_access_token"""
        if self._token:
            return self._token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                self._token = data["tenant_access_token"]
                return self._token
        
        logger.error(f"获取飞书token失败: {resp.text}")
        raise Exception("无法获取飞书访问令牌")
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }
    
    def list_tables(self) -> List[Dict]:
        """获取所有表格"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                return data["data"]["items"]
        return []
    
    def get_table_id(self, table_name: str, auto_create: bool = True) -> Optional[str]:
        """获取表格ID（缓存）"""
        if table_name in self._tables_cache:
            return self._tables_cache[table_name]
        
        tables = self.list_tables()
        for table in tables:
            if table["name"] == table_name:
                self._tables_cache[table_name] = table["table_id"]
                return table["table_id"]
        
        # 没有找到，尝试创建
        if auto_create:
            return self.create_table(table_name)
        return None
    
    def create_table(self, table_name: str) -> Optional[str]:
        """创建新表格"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"
        
        # 标准字段
        fields = [
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
        
        payload = {"table": {"name": table_name, "fields": fields}}
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                table_id = data["data"]["table_id"]
                
                # 添加状态字段（单选）
                self._add_status_field(table_id)
                
                self._tables_cache[table_name] = table_id
                logger.info(f"✅ 创建表格成功: {table_name}")
                return table_id
        
        logger.error(f"❌ 创建表格失败: {resp.text}")
        return None
    
    def _add_status_field(self, table_id: str):
        """添加状态字段"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        payload = {
            "field_name": "状态",
            "type": 3,  # SingleSelect
            "property": {
                "options": [
                    {"name": "进行中", "color": 1},
                    {"name": "已完成", "color": 2}
                ]
            }
        }
        requests.post(url, headers=self._headers(), json=payload, timeout=10)
    
    def create_record(self, table_id: str, fields: Dict) -> Optional[str]:
        """创建记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        payload = {"fields": fields}
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                return data["data"]["record"]["record_id"]
        
        logger.error(f"❌ 创建记录失败: {resp.text}")
        return None
    
    def update_record(self, table_id: str, record_id: str, fields: Dict) -> bool:
        """更新记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"
        payload = {"fields": fields}
        resp = requests.put(url, headers=self._headers(), json=payload, timeout=10)
        
        if resp.status_code == 200:
            return resp.json().get("code") == 0
        
        return False
    
    def list_records(self, table_id: str) -> List[Dict]:
        """获取所有记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        records = []
        page_token = None
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            resp = requests.get(url, headers=self._headers(), params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0:
                    items = data["data"]["items"]
                    records.extend(items)
                    if not data["data"].get("has_more"):
                        break
                    page_token = data["data"]["page_token"]
                else:
                    break
            else:
                break
        
        return records


class StockPickTracker:
    """选股跟踪器"""
    
    def __init__(self):
        self.strategy_config = get_strategy_config()
        if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
            logger.warning("⚠️ 飞书APP ID/Secret未配置，无法记录到多维表格")
            self.client = None
        else:
            try:
                self.client = FeishuBitableClient()
                logger.info("✅ 飞书多维表格客户端初始化成功")
                # 初始化时创建所有策略表格（包括未启用的）
                self._init_all_tables()
            except Exception as e:
                logger.error(f"❌ 飞书客户端初始化失败: {e}")
                self.client = None
    
    def _init_all_tables(self):
        """初始化所有策略表格（包括未启用的）"""
        if not self.client:
            return
        
        logger.info("🔄 初始化策略表格...")
        for strategy in self.strategy_config.keys():
            table_name = get_table_name(strategy, self.strategy_config)
            table_id = self.client.get_table_id(table_name)
            status = "✅ 已创建" if table_id else "❌ 失败"
            logger.info(f"  {table_name}: {status}")
    
    def get_enabled_strategies(self) -> List[str]:
        """获取启用的策略列表"""
        return [s for s, info in self.strategy_config.items() if info["enabled"]]
    
    def is_strategy_enabled(self, strategy: str) -> bool:
        """检查策略是否启用"""
        return self.strategy_config.get(strategy, {}).get("enabled", False)
    
    def record_pick(self, strategy: str, code: str, name: str, price: float, 
                    pick_date: str = None) -> bool:
        """
        记录选股
        
        Args:
            strategy: 策略版本，如 "V9", "V10"
            code: 股票代码
            name: 股票名称
            price: 选出时价格
            pick_date: 选股日期，格式 YYYY-MM-DD，默认今天
        """
        if not self.client:
            return False
        
        # 检查策略是否启用
        if not self.is_strategy_enabled(strategy):
            logger.warning(f"⚠️ 策略 {strategy} 未启用，跳过记录")
            return False
        
        try:
            table_name = get_table_name(strategy, self.strategy_config)
            table_id = self.client.get_table_id(table_name)
            if not table_id:
                return False
            
            # 日期处理
            if not pick_date:
                pick_date = datetime.now().strftime("%Y-%m-%d")
            dt = datetime.strptime(pick_date, "%Y-%m-%d")
            timestamp_ms = int(dt.timestamp() * 1000)
            
            fields = {
                "选股日期": timestamp_ms,
                "股票代码": code,
                "股票名称": name,
                "选出时价格": price,
                "状态": "进行中"
            }
            
            record_id = self.client.create_record(table_id, fields)
            if record_id:
                logger.info(f"✅ 已记录选股: {strategy} {name}({code}) @ {price}")
                return True
            
        except Exception as e:
            logger.error(f"❌ 记录选股失败: {e}")
        
        return False
    
    def update_prices(self, strategy: str, pick_date: str, code: str,
                      day1: float = None, day2: float = None, day3: float = None) -> bool:
        """
        更新收盘价
        
        Args:
            strategy: 策略版本
            pick_date: 选股日期
            code: 股票代码
            day1: 第1天收盘价
            day2: 第2天收盘价
            day3: 第3天收盘价
        """
        if not self.client:
            return False
        
        # 检查策略是否启用
        if not self.is_strategy_enabled(strategy):
            logger.warning(f"⚠️ 策略 {strategy} 未启用，跳过更新")
            return False
        
        try:
            table_name = get_table_name(strategy, self.strategy_config)
            table_id = self.client.get_table_id(table_name)
            if not table_id:
                return False
            
            # 查找记录
            records = self.client.list_records(table_id)
            target_record = None
            
            for record in records:
                fields = record.get("fields", {})
                record_date_ms = fields.get("选股日期")
                if record_date_ms:
                    record_date = datetime.fromtimestamp(record_date_ms / 1000).strftime("%Y-%m-%d")
                    if record_date == pick_date and fields.get("股票代码") == code:
                        target_record = record
                        break
            
            if not target_record:
                logger.warning(f"⚠️ 未找到记录: {strategy} {pick_date} {code}")
                return False
            
            # 获取选出价
            pick_price = target_record["fields"].get("选出时价格")
            if not pick_price:
                logger.warning(f"⚠️ 记录缺少选出时价格")
                return False
            
            # 构建更新字段
            update_fields = {}
            
            if day1 is not None:
                update_fields["第1天收盘价"] = day1
                update_fields["第1天涨幅%"] = round((day1 - pick_price) / pick_price * 100, 2)
            
            if day2 is not None:
                update_fields["第2天收盘价"] = day2
                update_fields["第2天涨幅%"] = round((day2 - pick_price) / pick_price * 100, 2)
            
            if day3 is not None:
                update_fields["第3天收盘价"] = day3
                update_fields["第3天涨幅%"] = round((day3 - pick_price) / pick_price * 100, 2)
                update_fields["状态"] = "已完成"
            
            if update_fields:
                success = self.client.update_record(
                    table_id, target_record["record_id"], update_fields
                )
                if success:
                    logger.info(f"✅ 已更新收盘价: {strategy} {code}")
                    return True
            
        except Exception as e:
            logger.error(f"❌ 更新收盘价失败: {e}")
        
        return False
    
    def get_pending_records(self, strategy: str) -> List[Dict]:
        """获取需要跟踪的记录（进行中状态）"""
        if not self.client:
            return []
        
        # 检查策略是否启用
        if not self.is_strategy_enabled(strategy):
            return []
        
        try:
            table_name = get_table_name(strategy, self.strategy_config)
            table_id = self.client.get_table_id(table_name)
            if not table_id:
                return []
            
            records = self.client.list_records(table_id)
            pending = []
            
            for record in records:
                fields = record.get("fields", {})
                if fields.get("状态") == "进行中":
                    record_date_ms = fields.get("选股日期")
                    pick_date = datetime.fromtimestamp(record_date_ms / 1000).strftime("%Y-%m-%d") if record_date_ms else ""
                    
                    pending.append({
                        "record_id": record["record_id"],
                        "pick_date": pick_date,
                        "code": fields.get("股票代码"),
                        "name": fields.get("股票名称"),
                        "pick_price": fields.get("选出时价格"),
                        "day1_close": fields.get("第1天收盘价"),
                        "day2_close": fields.get("第2天收盘价"),
                    })
            
            return pending
            
        except Exception as e:
            logger.error(f"❌ 获取待更新记录失败: {e}")
            return []


# 单例模式
transfer = None

def get_tracker() -> StockPickTracker:
    """获取跟踪器实例"""
    global transfer
    if transfer is None:
        transfer = StockPickTracker()
    return transfer


# 便捷函数
def record_stock_pick(strategy: str, code: str, name: str, price: float, pick_date: str = None) -> bool:
    """记录选股（便捷函数）"""
    tracker = get_tracker()
    return tracker.record_pick(strategy, code, name, price, pick_date)

def update_stock_prices(strategy: str, pick_date: str, code: str,
                       day1: float = None, day2: float = None, day3: float = None) -> bool:
    """更新收盘价（便捷函数）"""
    tracker = get_tracker()
    return tracker.update_prices(strategy, pick_date, code, day1, day2, day3)

def get_strategy_status() -> Dict[str, bool]:
    """获取所有策略的启用状态"""
    config = get_strategy_config()
    return {s: info["enabled"] for s, info in config.items()}

def list_all_tables() -> List[str]:
    """列出所有策略表格名称"""
    return get_all_strategy_names()
