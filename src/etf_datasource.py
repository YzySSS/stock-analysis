#!/usr/bin/env python3
"""
ETF行情数据获取模块
补充腾讯财经不提供的ETF数据
"""

import requests
import json
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


def get_etf_quotes(codes: list) -> Dict[str, dict]:
    """
    获取ETF实时行情
    
    Args:
        codes: ETF代码列表，如 ['159887', '510050']
    
    Returns:
        {code: {'price': float, 'change_pct': float, 'name': str}}
    """
    quotes = {}
    
    # 使用东方财富接口获取ETF数据
    for code in codes:
        try:
            # 转换代码格式
            if code.startswith(('15', '16', '18')):
                full_code = f"{code}.SZ"
            else:
                full_code = f"{code}.SH"
            
            # 东财接口
            url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&volt=2&secid={full_code}&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f60,f107,f108,f170"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if data.get('data'):
                d = data['data']
                quotes[code] = {
                    'price': float(d.get('f43', 0)) / 100 if d.get('f43') else 0,  # 当前价
                    'change_pct': float(d.get('f170', 0)) if d.get('f170') else 0,  # 涨跌幅
                    'name': d.get('f58', ''),
                    'volume': int(d.get('f47', 0)) if d.get('f47') else 0,
                }
        except Exception as e:
            logger.debug(f"获取ETF {code} 失败: {e}")
    
    return quotes


def update_quotes_with_etf(quotes: Dict[str, dict], etf_codes: list) -> Dict[str, dict]:
    """
    补充ETF数据到现有quotes中
    
    Args:
        quotes: 现有股票行情数据
        etf_codes: 需要补充的ETF代码列表
    
    Returns:
        合并后的quotes
    """
    # 找出缺失的ETF
    missing_etfs = [code for code in etf_codes if code not in quotes]
    
    if missing_etfs:
        logger.info(f"补充 {len(missing_etfs)} 只ETF行情数据")
        etf_quotes = get_etf_quotes(missing_etfs)
        quotes.update(etf_quotes)
    
    return quotes


if __name__ == "__main__":
    # 测试
    test_codes = ['159887', '159611', '159142', '510050']
    quotes = get_etf_quotes(test_codes)
    
    print("ETF行情测试:")
    for code, data in quotes.items():
        print(f"  {code}: {data.get('name')} ¥{data.get('price')} ({data.get('change_pct'):+.2f}%)")
