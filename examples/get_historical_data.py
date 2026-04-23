#!/usr/bin/env python3
"""
历史数据获取示例
演示如何获取A股历史K线数据
"""

import akshare as ak
from datetime import datetime, timedelta

def get_historical_kline(code: str, start_date: str, end_date: str):
    """
    获取历史K线数据
    
    Args:
        code: 股票代码，如 '000001'
        start_date: 开始日期，格式 'YYYYMMDD'
        end_date: 结束日期，格式 'YYYYMMDD'
    
    Returns:
        DataFrame: 包含 OHLCV 数据
    """
    df = ak.stock_zh_a_hist(
        symbol=code,
        period='daily',
        start_date=start_date,
        end_date=end_date,
        adjust='qfq'  # 前复权
    )
    return df


def get_today_quote(code: str):
    """获取今天实时行情"""
    df = ak.stock_zh_a_spot_em()
    stock = df[df['代码'] == code]
    return stock


if __name__ == "__main__":
    # 示例：获取平安银行2025年3月第二周的数据
    # 注意：2025-03-16是周日，休市
    
    print("📊 历史K线数据获取示例")
    print("=" * 60)
    
    # 2025年3月10日-16日
    df = get_historical_kline('000001', '20250310', '20250316')
    print("\n平安银行 2025年3月10日-16日:")
    print(df[['日期', '开盘', '收盘', '最高', '最低', '涨跌幅']])
    
    # 输出会显示：
    # - 3月10日（周一）有数据
    # - 3月11日（周二）有数据
    # - 3月12日（周三）有数据
    # - 3月13日（周四）有数据
    # - 3月14日（周五）有数据
    # - 3月15日（周六）无数据（休市）
    # - 3月16日（周日）无数据（休市）
