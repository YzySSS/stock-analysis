#!/usr/bin/env python3
"""
新浪财经股票数据接口文档

接口特点：
- 完全免费，无需注册
- 支持A股、港股、美股
- 提供5档买卖盘数据
- 实时更新（交易时段）
- HTTP协议，稳定性好

官方接口地址：http://hq.sinajs.cn/
"""

import requests
import time
from typing import List, Dict, Optional
import pandas as pd


class SinaFinanceAPI:
    """
    新浪财经股票数据接口
    
    数据源：新浪财经 (sina.com.cn)
    协议：HTTP
    频率限制：无明显限制（但建议不要过高频率请求）
    """
    
    BASE_URL = "https://hq.sinajs.cn/"
    
    # 市场代码映射
    MARKET_MAP = {
        'sh': 'sh',  # 上海
        'sz': 'sz',  # 深圳
        'hk': 'hk',  # 港股
        'gb': 'gb',  # 美股 (gb_+代码)
        'us': 'gb',  # 美股别名
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://finance.sina.com.cn',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        })
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情数据
        
        Args:
            codes: 股票代码列表，如 ['000001', '600519', '00700']
                  支持格式：000001、sh600519、sz000001、hk00700
        
        Returns:
            DataFrame: 包含详细行情数据（含5档盘口）
        
        Example:
            >>> api = SinaFinanceAPI()
            >>> df = api.get_realtime_quotes(['000001', '600519'])
            >>> print(df[['name', 'price', 'change_percent']])
        """
        # 格式化代码
        formatted_codes = [self._format_code(c) for c in codes]
        code_str = ','.join(formatted_codes)
        
        # 添加时间戳防止缓存
        timestamp = int(time.time() * 1000)
        url = f"{self.BASE_URL}rn={timestamp}&list={code_str}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gbk'
            
            return self._parse_response(response.text)
            
        except Exception as e:
            print(f"❌ 请求失败: {e}")
            return pd.DataFrame()
    
    def _format_code(self, code: str) -> str:
        """
        格式化股票代码

        规则：
        - 6开头：上海 sh600xxx
        - 5开头：上海 sh510xxx/sh560xxx (ETF)
        - 0/3开头：深圳 sz000xxx/sz300xxx
        - 7开头：港股 hk00700
        - 纯字母：美股 gb_aapl
        """
        code = code.strip().lower()

        # 如果已经包含市场前缀，直接返回
        if code.startswith(('sh', 'sz', 'hk', 'gb_')):
            return code

        # 判断市场
        if code.startswith('6') or code.startswith('5'):
            return f"sh{code}"
        elif code.startswith(('0', '3')):
            return f"sz{code}"
        elif code.startswith('7'):
            return f"hk{code}"
        elif code.isalpha():
            # 美股
            return f"gb_{code}"
        else:
            # 默认深圳
            return f"sz{code}"
    
    def _parse_response(self, text: str) -> pd.DataFrame:
        """
        解析新浪返回的数据
        
        返回格式示例：
        var hq_str_sh600519="贵州茅台,1460.18,1420.00,1460.18,1470.00,1420.00,1460.18,1460.19,...
        
        字段说明（按顺序）：
        0: 股票名称
        1: 今日开盘价
        2: 昨日收盘价
        3: 当前价格
        4: 今日最高价
        5: 今日最低价
        6: 竞买价（买一）
        7: 竞卖价（卖一）
        8: 成交量（股）
        9: 成交金额（元）
        10-18: 买一~买五（量,价）
        19-27: 卖一~卖五（量,价）
        28: 日期
        29: 时间
        """
        stocks = []
        
        for line in text.strip().split(';'):
            if not line.strip() or 'var hq_str_' not in line:
                continue
            
            try:
                # 提取变量名和数据
                var_part = line.split('="')[0]
                data_part = line.split('="')[1].rstrip('"')
                
                # 提取代码
                code = var_part.replace('var hq_str_', '')
                
                # 解析数据
                fields = data_part.split(',')
                
                if len(fields) < 30:
                    continue
                
                # 计算涨跌幅
                try:
                    price = float(fields[3])
                    pre_close = float(fields[2])
                    change_percent = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                except:
                    price = 0
                    change_percent = 0
                
                stock = {
                    'code': code,
                    'name': fields[0],
                    'open': float(fields[1]) if fields[1] else 0,
                    'pre_close': float(fields[2]) if fields[2] else 0,
                    'price': price,
                    'high': float(fields[4]) if fields[4] else 0,
                    'low': float(fields[5]) if fields[5] else 0,
                    'bid1': float(fields[6]) if fields[6] else 0,  # 买一价
                    'ask1': float(fields[7]) if fields[7] else 0,  # 卖一价
                    'volume': int(float(fields[8])) if fields[8] else 0,
                    'amount': float(fields[9]) if fields[9] else 0,
                    'change_percent': change_percent,
                    
                    # 5档买盘
                    'bid1_volume': int(float(fields[10])) if len(fields) > 10 and fields[10] else 0,
                    'bid1_price': float(fields[11]) if len(fields) > 11 and fields[11] else 0,
                    'bid2_volume': int(float(fields[12])) if len(fields) > 12 and fields[12] else 0,
                    'bid2_price': float(fields[13]) if len(fields) > 13 and fields[13] else 0,
                    'bid3_volume': int(float(fields[14])) if len(fields) > 14 and fields[14] else 0,
                    'bid3_price': float(fields[15]) if len(fields) > 15 and fields[15] else 0,
                    'bid4_volume': int(float(fields[16])) if len(fields) > 16 and fields[16] else 0,
                    'bid4_price': float(fields[17]) if len(fields) > 17 and fields[17] else 0,
                    'bid5_volume': int(float(fields[18])) if len(fields) > 18 and fields[18] else 0,
                    'bid5_price': float(fields[19]) if len(fields) > 19 and fields[19] else 0,
                    
                    # 5档卖盘
                    'ask1_volume': int(float(fields[20])) if len(fields) > 20 and fields[20] else 0,
                    'ask1_price': float(fields[21]) if len(fields) > 21 and fields[21] else 0,
                    'ask2_volume': int(float(fields[22])) if len(fields) > 22 and fields[22] else 0,
                    'ask2_price': float(fields[23]) if len(fields) > 23 and fields[23] else 0,
                    'ask3_volume': int(float(fields[24])) if len(fields) > 24 and fields[24] else 0,
                    'ask3_price': float(fields[25]) if len(fields) > 25 and fields[25] else 0,
                    'ask4_volume': int(float(fields[26])) if len(fields) > 26 and fields[26] else 0,
                    'ask4_price': float(fields[27]) if len(fields) > 27 and fields[27] else 0,
                    'ask5_volume': int(float(fields[28])) if len(fields) > 28 and fields[28] else 0,
                    'ask5_price': float(fields[29]) if len(fields) > 29 and fields[29] else 0,
                    
                    'date': fields[30] if len(fields) > 30 else '',
                    'time': fields[31] if len(fields) > 31 else '',
                }
                
                stocks.append(stock)
                
            except Exception as e:
                print(f"⚠️ 解析行失败: {line[:50]}... - {e}")
                continue
        
        return pd.DataFrame(stocks)
    
    def get_market_depth(self, code: str) -> Optional[Dict]:
        """
        获取5档盘口深度数据
        
        Args:
            code: 股票代码
        
        Returns:
            Dict: 包含买卖5档的详细数据
        """
        df = self.get_realtime_quotes([code])
        
        if df.empty:
            return None
        
        row = df.iloc[0]
        
        return {
            'code': row['code'],
            'name': row['name'],
            'price': row['price'],
            'bid_depth': [
                {'price': row['bid1_price'], 'volume': row['bid1_volume']},
                {'price': row['bid2_price'], 'volume': row['bid2_volume']},
                {'price': row['bid3_price'], 'volume': row['bid3_volume']},
                {'price': row['bid4_price'], 'volume': row['bid4_volume']},
                {'price': row['bid5_price'], 'volume': row['bid5_volume']},
            ],
            'ask_depth': [
                {'price': row['ask1_price'], 'volume': row['ask1_volume']},
                {'price': row['ask2_price'], 'volume': row['ask2_volume']},
                {'price': row['ask3_price'], 'volume': row['ask3_volume']},
                {'price': row['ask4_price'], 'volume': row['ask4_volume']},
                {'price': row['ask5_price'], 'volume': row['ask5_volume']},
            ]
        }


# 使用示例
if __name__ == "__main__":
    print("📊 新浪财经股票数据接口 - 完整测试")
    print("=" * 80)
    
    api = SinaFinanceAPI()
    
    # 1. 基础行情测试
    print("\n1️⃣ 获取基础行情（A股）")
    print("-" * 80)
    
    codes = ['000001', '600519', '300750', '00700']  # 平安银行、茅台、宁德时代、腾讯
    df = api.get_realtime_quotes(codes)
    
    if not df.empty:
        print(f"✅ 成功获取 {len(df)} 只股票")
        print("\n基础信息:")
        print(df[['code', 'name', 'price', 'change_percent', 'volume']].to_string())
    else:
        print("❌ 获取失败")
    
    # 2. 5档盘口测试
    print("\n\n2️⃣ 5档盘口深度")
    print("-" * 80)
    
    if not df.empty:
        test_code = df.iloc[0]['code']
        depth = api.get_market_depth(test_code.replace('sh', '').replace('sz', ''))
        
        if depth:
            print(f"\n股票: {depth['name']} ({depth['code']})")
            print(f"当前价: {depth['price']}")
            
            print("\n📗 买盘5档:")
            for i, bid in enumerate(depth['bid_depth'], 1):
                print(f"  买{i}: ¥{bid['price']:.2f} × {bid['volume']}手")
            
            print("\n📕 卖盘5档:")
            for i, ask in enumerate(depth['ask_depth'], 1):
                print(f"  卖{i}: ¥{ask['price']:.2f} × {ask['volume']}手")
    
    # 3. 接口文档
    print("\n\n3️⃣ 接口字段说明")
    print("-" * 80)
    print("""
接口地址: https://hq.sinajs.cn/rn={timestamp}&list={codes}

请求参数:
  - rn: 时间戳（防缓存）
  - list: 股票代码，多个用逗号分隔
        格式: sh600519,sz000001,hk00700
        sh=上海, sz=深圳, hk=港股, gb_=美股

返回字段（按顺序）:
  0  - 股票名称
  1  - 今日开盘价
  2  - 昨日收盘价
  3  - 当前价格
  4  - 今日最高价
  5  - 今日最低价
  6  - 竞买价（买一价）
  7  - 竞卖价（卖一价）
  8  - 成交量（股）
  9  - 成交金额（元）
  10 - 买一手数
  11 - 买一价格
  12 - 买二手数
  13 - 买二价格
  14 - 买三手数
  15 - 买三价格
  16 - 买四手数
  17 - 买四价格
  18 - 买五手数
  19 - 买五价格
  20 - 卖一手数
  21 - 卖一价格
  22 - 卖二手数
  23 - 卖二价格
  24 - 卖三手数
  25 - 卖三价格
  26 - 卖四手数
  27 - 卖四价格
  28 - 卖五手数
  29 - 卖五价格
  30 - 日期
  31 - 时间

请求头要求:
  - User-Agent: 浏览器UA
  - Referer: https://finance.sina.com.cn
  - Accept: */*
    """)
    
    print("\n" + "=" * 80)
    print("✅ 测试完成")
