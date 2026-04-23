#!/usr/bin/env python3
"""
腾讯财经数据接口
备用数据源（当AkShare/efinance不可用时）
"""

import requests
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class TencentDataSource:
    """
    腾讯财经数据接口
    
    优点：
    - 接口简单，返回实时行情
    - 不受AkShare网络限制
    
    缺点：
    - 不提供历史K线
    - 仅提供实时快照
    """
    
    BASE_URL = "http://qt.gtimg.cn/q="
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情
        
        Args:
            codes: 股票代码列表，如 ['000001', '600519']
        
        Returns:
            DataFrame: 实时行情数据
        """
        # 转换代码格式
        formatted_codes = []
        for code in codes:
            code = code.replace('.SZ', '').replace('.SH', '').replace('.BJ', '')
            if code.startswith('6'):
                formatted_codes.append(f"sh{code}")
            else:
                formatted_codes.append(f"sz{code}")
        
        url = self.BASE_URL + ','.join(formatted_codes)
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gbk'
            
            return self._parse_response(response.text)
            
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def _parse_response(self, text: str) -> pd.DataFrame:
        """解析腾讯财经返回的数据"""
        stocks = []
        
        # 数据格式: v_sh000001="1~上证指数~000001~4084.79~..."
        for line in text.strip().split(';'):
            if not line.strip():
                continue
            
            try:
                # 提取变量名和数据
                if '=' in line:
                    var_part, data_part = line.split('=', 1)
                    data = data_part.strip('"').split('~')
                    
                    if len(data) >= 35:
                        stock = {
                            'code': data[2],           # 代码
                            'name': data[1],           # 名称
                            'price': float(data[3]),   # 当前价
                            'yesterday_close': float(data[4]),  # 昨收
                            'open': float(data[5]),    # 开盘
                            'volume': int(data[6]),    # 成交量
                            'bid1': float(data[9]),    # 买一价
                            'ask1': float(data[19]),   # 卖一价
                            'high': float(data[33]),   # 最高
                            'low': float(data[34]),    # 最低
                            'datetime': data[30],      # 时间
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
    
    def get_index_quotes(self) -> pd.DataFrame:
        """获取大盘指数行情"""
        indices = ['sh000001', 'sz399001', 'sz399006', 'sh000300']
        return self.get_realtime_quotes(indices)


if __name__ == "__main__":
    print("🧪 腾讯财经数据源测试")
    print("=" * 60)
    
    ds = TencentDataSource()
    
    # 测试获取个股行情
    print("\n1. 获取个股实时行情")
    df = ds.get_realtime_quotes(['000001', '600519', '300750'])
    
    if not df.empty:
        print(f"✅ 成功获取 {len(df)} 只股票")
        print("\n数据预览:")
        print(df[['code', 'name', 'price', 'change_percent', 'high', 'low']].to_string())
    else:
        print("❌ 获取失败")
    
    # 测试获取大盘指数
    print("\n2. 获取大盘指数")
    df_index = ds.get_index_quotes()
    
    if not df_index.empty:
        print("✅ 成功获取大盘指数")
        print(df_index[['name', 'price', 'change_percent']].to_string())
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
