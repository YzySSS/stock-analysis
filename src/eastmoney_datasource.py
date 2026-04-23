#!/usr/bin/env python3
"""
东方财富数据源 - 直接调用 API
用于获取 A股实时行情和历史数据
"""

import os
import json
import logging
import requests
import urllib3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from functools import lru_cache
import time

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== 代理配置 ====================
# 支持通过环境变量配置代理
BRD_PROXY_HOST = os.getenv('BRD_PROXY_HOST', 'brd.superproxy.io')
BRD_PROXY_PORT = os.getenv('BRD_PROXY_PORT', '33335')
BRD_PROXY_USER = os.getenv('BRD_PROXY_USER', 'brd-customer-hl_8abbb7fa-zone-isp_proxy1')
BRD_PROXY_PASS = os.getenv('BRD_PROXY_PASS', '1chayfaf4h24')
USE_PROXY = os.getenv('USE_PROXY', 'false').lower() == 'true'

PROXY_URL = f"http://{BRD_PROXY_USER}:{BRD_PROXY_PASS}@{BRD_PROXY_HOST}:{BRD_PROXY_PORT}"

# 创建全局 session
_session = requests.Session()
_session.verify = False
_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

# 如果使用代理，设置代理
if USE_PROXY:
    _session.proxies = {'http': PROXY_URL, 'https': PROXY_URL}
    logger.info("✅ 已启用 Bright Data 代理")
else:
    logger.info("ℹ️ 未使用代理")


@dataclass
class StockData:
    """股票数据结构"""
    code: str
    name: str
    price: float
    change_percent: float
    volume: int
    turnover: float
    market_cap: Optional[float] = None
    pe: Optional[float] = None
    pb: Optional[float] = None


@dataclass
class MarketOverview:
    """大盘指数数据结构"""
    index_name: str
    index_code: str
    price: float
    change_percent: float
    volume: float
    up_count: int = 0
    down_count: int = 0


def get_a_stock_spot() -> List[StockData]:
    """
    获取 A股实时行情 - 东方财富接口
    支持代理和直连两种模式
    """
    stocks = []
    
    try:
        # 东方财富 API - 获取沪深A股
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': 1,
            'pz': 5000,  # 获取全部A股
            'po': 1,
            'np': 1,
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': 2,
            'invt': 2,
            'fid': 'f3',
            'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
            'fields': 'f1,f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f62,f115,f128,f136,f152'
        }
        
        logger.info("正在获取东方财富 A股实时行情...")
        resp = _session.get(url, params=params, timeout=60)
        
        if resp.status_code != 200:
            logger.error(f"API返回状态码: {resp.status_code}")
            return []
        
        data = resp.json()
        if not data or 'data' not in data or not data['data']:
            logger.error("API返回数据为空")
            return []
            
        diff = data['data'].get('diff', [])
        total = data['data'].get('total', 0)
        
        logger.info(f"东方财富返回: 总数 {total}, 获取 {len(diff)} 只")
        
        for item in diff:
            try:
                code = str(item.get('f12', '')).zfill(6)
                name = item.get('f14', '')
                
                # f2: 最新价, f3: 涨跌幅
                price = item.get('f2', 0)
                if price == '-' or price == '':
                    price = 0
                price = float(price) if price else 0
                
                change_percent = item.get('f3', 0)
                if change_percent == '-' or change_percent == '':
                    change_percent = 0
                change_percent = float(change_percent) if change_percent else 0
                
                # f5: 成交量, f6: 成交额
                volume = int(item.get('f5', 0)) if item.get('f5') else 0
                turnover = float(item.get('f6', 0)) if item.get('f6') else 0
                
                # f20: 总市值
                market_cap = float(item.get('f20', 0)) if item.get('f20') else None
                
                # f115: 市盈率
                pe_str = item.get('f115', '')
                pe = float(pe_str) if pe_str and pe_str != '-' else None
                
                # f152: 市净率
                pb_str = item.get('f152', '')
                pb = float(pb_str) if pb_str and pb_str != '-' else None
                
                if code and name:  # 只添加有效数据
                    stocks.append(StockData(
                        code=code,
                        name=name,
                        price=price,
                        change_percent=change_percent,
                        volume=volume,
                        turnover=turnover,
                        market_cap=market_cap,
                        pe=pe,
                        pb=pb
                    ))
                    
            except Exception as e:
                continue
        
        logger.info(f"✅ 成功获取 {len(stocks)} 只股票数据")
        
    except Exception as e:
        logger.error(f"获取 A股实时行情失败: {e}")
    
    return stocks


def get_index_spot() -> List[MarketOverview]:
    """
    获取大盘指数行情
    """
    indices = []
    
    index_map = {
        '000001': '上证指数',
        '399001': '深证成指',
        '399006': '创业板指',
        '000300': '沪深300',
        '000016': '上证50',
        '000905': '中证500'
    }
    
    for code, name in index_map.items():
        try:
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                'secid': f'1.{code}',
                'fields': 'f43,f44,f45,f46,f47,f48,f50,f51,f52,f55,f57,f58,f59,f60,f116,f117,f162,f167,f168,f169,f170,f171,f173,f177'
            }
            
            resp = _session.get(url, params=params, timeout=10)
            data = resp.json()
            
            if data.get('data'):
                item = data['data']
                price = float(item.get('f43', 0)) / 100  # 最新价
                change = float(item.get('f44', 0)) / 100  # 涨跌幅
                volume = float(item.get('f47', 0)) / 100000000  # 成交量(亿)
                
                indices.append(MarketOverview(
                    index_name=name,
                    index_code=code,
                    price=price,
                    change_percent=change,
                    volume=volume
                ))
                
        except Exception as e:
            logger.warning(f"获取指数 {code} 失败: {e}")
            continue
    
    return indices


def get_stock_history(code: str, days: int = 30) -> List[Dict]:
    """
    获取个股历史K线数据
    """
    try:
        # 判断市场
        if code.startswith('6'):
            secid = f'1.{code}'
        else:
            secid = f'0.{code}'
        
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            'secid': secid,
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65',
            'klt': 101,  # 日K
            'fqt': 1,    # 前复权
            'beg': start_date,
            'end': end_date,
            'lmt': days
        }
        
        resp = _session.get(url, params=params, timeout=30)
        data = resp.json()
        
        klines = []
        if data.get('data') and data['data'].get('klines'):
            for line in data['data']['klines']:
                parts = line.split(',')
                klines.append({
                    'date': parts[0],
                    'open': float(parts[1]),
                    'close': float(parts[2]),
                    'high': float(parts[3]),
                    'low': float(parts[4]),
                    'volume': float(parts[5]),
                    'amount': float(parts[6]) if len(parts) > 6 else 0
                })
        
        return klines[-days:] if len(klines) > days else klines
        
    except Exception as e:
        logger.error(f"获取历史数据失败 {code}: {e}")
        return []


# 测试入口
if __name__ == "__main__":
    print("=" * 60)
    print("东方财富数据源 (Bright Data ISP 代理) 测试")
    print("=" * 60)
    
    print("\n[测试 1] 获取 A股实时行情...")
    stocks = get_a_stock_spot()
    print(f"获取到 {len(stocks)} 只股票")
    if stocks:
        print(f"样本: {stocks[0].name}({stocks[0].code}): {stocks[0].price} ({stocks[0].change_percent:+.2f}%)")
    
    print("\n[测试 2] 获取大盘指数...")
    indices = get_index_spot()
    for idx in indices:
        print(f"  {idx.index_name}: {idx.price} ({idx.change_percent:+.2f}%)")
    
    print("\n[测试 3] 获取个股历史数据...")
    history = get_stock_history('000001', days=5)
    print(f"获取到 {len(history)} 条记录")
    if history:
        print(f"最新: {history[-1]}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)