#!/usr/bin/env python3
"""
增强版数据源模块 - 多源备份 + 重试机制

优先级:
1. AkShare (带重试) - 数据最全
2. 腾讯财经 - 速度快、稳定
3. 新浪财经 - 备用方案
4. 本地缓存 - 最后的保底
"""

import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RetryableAkShare:
    """
    带重试机制的AkShare包装器
    
    自动处理：
    - 网络超时重试
    - 连接失败重试  
    - 返回空数据重试
    """
    
    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.0):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._ak = None
        self._init_akshare()
    
    def _init_akshare(self):
        """初始化AkShare"""
        try:
            import akshare as ak
            self._ak = ak
            logger.info("✅ AkShare 初始化成功")
        except ImportError:
            logger.error("❌ AkShare 未安装")
            self._ak = None
    
    def __getattr__(self, name: str):
        """
        代理所有AkShare方法，添加重试逻辑
        """
        if self._ak is None:
            raise RuntimeError("AkShare 未初始化")
        
        original_func = getattr(self._ak, name)
        
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(self.max_retries):
                try:
                    logger.debug(f"[AkShare] 调用 {name} (尝试 {attempt + 1}/{self.max_retries})")
                    result = original_func(*args, **kwargs)
                    
                    # 检查结果是否有效
                    if result is not None and not (hasattr(result, 'empty') and result.empty):
                        logger.debug(f"[AkShare] {name} 成功")
                        return result
                    
                    # 空结果，可能是网络问题，尝试重试
                    if attempt < self.max_retries - 1:
                        wait_time = self.backoff_factor * (2 ** attempt)
                        logger.warning(f"[AkShare] {name} 返回空数据，{wait_time}秒后重试...")
                        time.sleep(wait_time)
                        
                except Exception as e:
                    last_exception = e
                    if attempt < self.max_retries - 1:
                        wait_time = self.backoff_factor * (2 ** attempt)
                        logger.warning(f"[AkShare] {name} 失败: {e}，{wait_time}秒后重试...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[AkShare] {name} 最终失败: {e}")
            
            # 所有重试都失败了
            if last_exception:
                raise last_exception
            return None
        
        return wrapper


class SinaDataSource:
    """
    新浪财经数据源
    
    优点：
    - 接口简单
    - 通常不会被封
    
    缺点：
    - 数据字段较少
    - 没有历史K线
    """
    
    BASE_URL = "https://hq.sinajs.cn/list="
    
    def __init__(self):
        self.session = requests.Session()
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情
        
        新浪数据格式：
        var hq_str_sh600519="贵州茅台,1450.00,1420.00,1460.00,1470.00,1420.00..."
        """
        # 转换代码格式
        formatted_codes = []
        for code in codes:
            code = code.replace('.SZ', '').replace('.SH', '')
            if code.startswith('6'):
                formatted_codes.append(f"sh{code}")
            elif code.startswith('0') or code.startswith('3'):
                formatted_codes.append(f"sz{code}")
            else:
                formatted_codes.append(code)
        
        url = self.BASE_URL + ','.join(formatted_codes)
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gbk'
            return self._parse_response(response.text)
        except Exception as e:
            logger.error(f"新浪财经获取失败: {e}")
            return pd.DataFrame()
    
    def _parse_response(self, text: str) -> pd.DataFrame:
        """解析新浪返回的数据"""
        stocks = []
        
        for line in text.strip().split(';'):
            if not line.strip() or '=' not in line:
                continue
            
            try:
                var_name, data_part = line.split('=', 1)
                data = data_part.strip('"').split(',')
                
                if len(data) >= 33:
                    # 提取代码
                    code_prefix = var_name.split('_')[-1]
                    code = code_prefix[2:]  # 去掉sh/sz前缀
                    
                    stock = {
                        'code': code,
                        'name': data[0],
                        'open': float(data[1]),
                        'yesterday_close': float(data[2]),
                        'price': float(data[3]),
                        'high': float(data[4]),
                        'low': float(data[5]),
                        'bid': float(data[6]),
                        'ask': float(data[7]),
                        'volume': int(data[8]),
                        'amount': float(data[9]),
                        'datetime': f"{data[-3]} {data[-2]}",
                    }
                    
                    # 计算涨跌幅
                    if stock['yesterday_close'] > 0:
                        stock['change_percent'] = round(
                            (stock['price'] - stock['yesterday_close']) / stock['yesterday_close'] * 100,
                            2
                        )
                    else:
                        stock['change_percent'] = 0.0
                    
                    stocks.append(stock)
                    
            except Exception as e:
                logger.debug(f"解析行失败: {line[:50]}... - {e}")
                continue
        
        return pd.DataFrame(stocks)


class DataSourceManagerV2:
    """
    增强版数据源管理器
    
    多源备份策略：
    1. BaoStock (历史K线首选)
    2. AkShare (带3次重试)
    3. 腾讯财经 (实时行情)
    4. 新浪财经 (实时行情)
    5. 本地缓存
    """
    
    def __init__(self):
        self.sources = {}
        self._init_sources()
    
    def _init_sources(self):
        """初始化所有数据源"""
        # 1. BaoStock (历史K线首选)
        try:
            from src.baostock_datasource import BaoStockDataSource
            self.baostock = BaoStockDataSource()
            logger.info("✅ 数据源: BaoStock (历史K线)")
        except Exception as e:
            logger.warning(f"⚠️ BaoStock 初始化失败: {e}")
            self.baostock = None

        # 2. AkShare (带重试)
        try:
            self.ak = RetryableAkShare(max_retries=3)
            logger.info("✅ 数据源: AkShare (带重试)")
        except Exception as e:
            logger.warning(f"⚠️ AkShare 初始化失败: {e}")
            self.ak = None

        # 3. 新浪财经（实时行情首选，支持5档盘口）
        try:
            from src.sina_finance_api import SinaFinanceAPI
            self.sina = SinaFinanceAPI()
            logger.info("✅ 数据源: 新浪财经（5档盘口）")
        except Exception as e:
            logger.warning(f"⚠️ 新浪数据源初始化失败: {e}")
            self.sina = None

        # 4. 腾讯财经（备用）
        try:
            from src.tencent_datasource import TencentDataSource
            self.tencent = TencentDataSource()
            logger.info("✅ 数据源: 腾讯财经")
        except Exception as e:
            logger.warning(f"⚠️ 腾讯数据源初始化失败: {e}")
            self.tencent = None
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情 - 多源备份
        
        优先级:
        1. 新浪财经（5档盘口数据）
        2. 腾讯财经
        3. AkShare
        """
        sources_to_try = [
            ("新浪财经", self._get_from_sina),
            ("腾讯财经", self._get_from_tencent),
            ("AkShare", self._get_from_akshare),
        ]
        
        for name, getter in sources_to_try:
            try:
                df = getter(codes)
                if df is not None and not df.empty:
                    logger.info(f"✅ 从 {name} 获取数据成功 ({len(df)} 条)")
                    return df
            except Exception as e:
                logger.warning(f"⚠️ {name} 获取失败: {e}")
                continue
        
        logger.error("❌ 所有数据源都失败了")
        return pd.DataFrame()
    
    def get_history_k_data(self, code: str, days: int = 60) -> pd.DataFrame:
        """
        获取历史K线数据 - 优先使用BaoStock
        
        Args:
            code: 股票代码
            days: 获取天数
        
        Returns:
            DataFrame: 历史K线数据
        """
        # 优先使用BaoStock
        if self.baostock is not None:
            try:
                df = self.baostock.get_daily_data(code, days=days)
                if df is not None and not df.empty:
                    logger.info(f"✅ 从 BaoStock 获取 {code} 历史数据成功 ({len(df)} 条)")
                    return df
            except Exception as e:
                logger.warning(f"⚠️ BaoStock 获取失败: {e}")
        
        # 备用：AkShare
        if self.ak is not None:
            try:
                import pandas as pd
                from datetime import datetime, timedelta
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                df = self.ak.stock_zh_a_hist(
                    symbol=code,
                    period='daily',
                    start_date=start_date.strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d'),
                    adjust='qfq'
                )
                
                if df is not None and not df.empty:
                    logger.info(f"✅ 从 AkShare 获取 {code} 历史数据成功 ({len(df)} 条)")
                    return df
            except Exception as e:
                logger.warning(f"⚠️ AkShare 获取失败: {e}")
        
        logger.error(f"❌ 无法获取 {code} 历史数据")
        return pd.DataFrame()
    
    def _get_from_akshare(self, codes: List[str]) -> pd.DataFrame:
        """从AkShare获取"""
        if self.ak is None:
            return None
        
        df = self.ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            # 筛选指定股票
            df['代码'] = df['代码'].astype(str).str.zfill(6)
            codes_str = [c.zfill(6) for c in codes]
            df = df[df['代码'].isin(codes_str)]
        return df
    
    def _get_from_tencent(self, codes: List[str]) -> pd.DataFrame:
        """从腾讯获取"""
        if self.tencent is None:
            return None
        return self.tencent.get_realtime_quotes(codes)
    
    def _get_from_sina(self, codes: List[str]) -> pd.DataFrame:
        """从新浪获取"""
        if self.sina is None:
            return None
        return self.sina.get_realtime_quotes(codes)


# 便捷函数
def get_data_manager() -> DataSourceManagerV2:
    """获取数据源管理器实例"""
    return DataSourceManagerV2()


if __name__ == "__main__":
    print("🧪 增强版数据源测试")
    print("=" * 60)
    
    manager = DataSourceManagerV2()
    
    # 测试股票列表
    test_codes = ['000001', '600519', '300750']
    
    print(f"\n📊 获取 {test_codes} 的实时行情")
    df = manager.get_realtime_quotes(test_codes)
    
    if not df.empty:
        print(f"\n✅ 成功获取 {len(df)} 只股票")
        print("\n数据预览:")
        print(df[['code', 'name', 'price', 'change_percent']].to_string())
    else:
        print("\n❌ 所有数据源都失败了")
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
