#!/usr/bin/env python3
"""
BaoStock 数据源适配器

特点：
- 完全免费，无需注册Token
- 支持历史K线（1990年起）
- 支持分钟级数据（5/15/30/60分钟）
- 支持前复权/后复权
- HTTP协议，稳定可靠
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class BaoStockDataSource:
    """
    BaoStock 数据源
    
    官方文档: http://baostock.com/baostock/index.php
    """
    
    def __init__(self):
        self.bs = None
        self.connected = False
        self._init_baostock()
    
    def _init_baostock(self):
        """初始化BaoStock"""
        try:
            import baostock as bs
            self.bs = bs
            logger.info("✅ BaoStock 数据源初始化成功")
        except ImportError:
            logger.error("❌ 请先安装 baostock: pip install baostock")
            self.bs = None
    
    def _ensure_connected(self) -> bool:
        """确保已连接"""
        if self.connected:
            return True
        
        if self.bs is None:
            return False
        
        try:
            result = self.bs.login()
            if result.error_code == '0':
                self.connected = True
                logger.info("✅ BaoStock 登录成功")
                return True
            else:
                logger.error(f"❌ BaoStock 登录失败: {result.error_msg}")
                return False
        except Exception as e:
            logger.error(f"❌ BaoStock 登录异常: {e}")
            return False
    
    def _format_code(self, code: str) -> str:
        """
        格式化股票代码
        
        000001 -> sz.000001 (深圳)
        600000 -> sh.600000 (上海)
        """
        code = code.replace('.SZ', '').replace('.SH', '').replace('.BJ', '')
        
        if code.startswith('6') or code.startswith('5') or code.startswith('9'):
            return f"sh.{code}"
        else:
            return f"sz.{code}"
    
    def get_history_k_data(self, 
                          code: str,
                          start_date: str,
                          end_date: str,
                          frequency: str = 'd',
                          adjustflag: str = '2') -> Optional[pd.DataFrame]:
        """
        获取历史K线数据
        
        Args:
            code: 股票代码，如 '000001'
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            frequency: 频率 'd'=日, 'w'=周, 'm'=月, '5'=5分钟, '15'=15分钟, '30'=30分钟, '60'=60分钟
            adjustflag: 复权类型 '1'=不复权, '2'=前复权, '3'=后复权
        
        Returns:
            DataFrame: 包含 OHLCV 数据
        """
        if not self._ensure_connected():
            return None
        
        try:
            formatted_code = self._format_code(code)
            
            # 根据频率选择字段
            if frequency in ['d', 'w', 'm']:
                fields = 'date,open,high,low,close,volume,amount,turn,pctChg'
            else:
                fields = 'date,time,open,high,low,close,volume,amount'
            
            rs = self.bs.query_history_k_data_plus(
                formatted_code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )
            
            if rs.error_code != '0':
                logger.error(f"❌ BaoStock 查询失败: {rs.error_msg}")
                return None
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"⚠️ BaoStock 返回空数据: {code}")
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'pctChg' in df.columns:
                df['pctChg'] = pd.to_numeric(df['pctChg'], errors='coerce')
            
            logger.info(f"✅ BaoStock 获取 {code} 数据: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"❌ BaoStock 获取数据失败: {e}")
            return None
    
    def get_daily_data(self, code: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        获取最近N天的日线数据
        
        Args:
            code: 股票代码
            days: 天数
        
        Returns:
            DataFrame
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
        
        return self.get_history_k_data(code, start_date, end_date, frequency='d')
    
    def get_minute_data(self, 
                       code: str,
                       start_date: str,
                       end_date: str,
                       frequency: str = '5') -> Optional[pd.DataFrame]:
        """
        获取分钟级数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: '5', '15', '30', '60'
        
        Returns:
            DataFrame
        """
        return self.get_history_k_data(code, start_date, end_date, frequency=frequency)
    
    def get_stock_basic_info(self, code: str) -> Optional[Dict]:
        """
        获取股票基本信息
        """
        if not self._ensure_connected():
            return None
        
        try:
            formatted_code = self._format_code(code)
            rs = self.bs.query_stock_basic(code=formatted_code)
            
            if rs.error_code == '0' and rs.next():
                data = rs.get_row_data()
                return {
                    'code': data[0] if len(data) > 0 else code,
                    'name': data[1] if len(data) > 1 else '',
                    'ipo_date': data[2] if len(data) > 2 else '',
                    'out_date': data[3] if len(data) > 3 else '',
                    'type': data[4] if len(data) > 4 else '',
                    'status': data[5] if len(data) > 5 else '',
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取股票信息失败: {e}")
            return None
    
    def get_all_stock_codes(self) -> List[str]:
        """
        获取所有股票代码
        """
        if not self._ensure_connected():
            return []
        
        try:
            rs = self.bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            
            if rs.error_code != '0':
                return []
            
            codes = []
            while rs.next():
                data = rs.get_row_data()
                if len(data) > 0:
                    # 去掉前缀 sh. 或 sz.
                    code = data[0].replace('sh.', '').replace('sz.', '')
                    codes.append(code)
            
            return codes
            
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return []
    
    def logout(self):
        """登出"""
        if self.connected and self.bs:
            try:
                self.bs.logout()
                self.connected = False
                logger.info("✅ BaoStock 已登出")
            except Exception as e:
                logger.error(f"❌ BaoStock 登出失败: {e}")


# 全局实例
baostock_source = BaoStockDataSource()


if __name__ == "__main__":
    print("🧪 BaoStock 数据源测试")
    print("=" * 60)
    
    source = BaoStockDataSource()
    
    # 测试获取历史数据
    print("\n📊 获取平安银行(000001)最近5天数据")
    df = source.get_daily_data('000001', days=5)
    
    if df is not None and not df.empty:
        print(f"✅ 成功获取 {len(df)} 条数据")
        print("\n数据预览:")
        print(df[['date', 'open', 'high', 'low', 'close', 'volume', 'pctChg']].to_string())
    else:
        print("❌ 获取失败")
    
    # 测试获取分钟数据
    print("\n📊 获取5分钟K线数据")
    df_min = source.get_minute_data('000001', '2026-03-16', '2026-03-16', frequency='5')
    
    if df_min is not None and not df.empty:
        print(f"✅ 成功获取 {len(df_min)} 条数据")
        print("\n前5条:")
        print(df_min.head()[['time', 'open', 'high', 'low', 'close', 'volume']].to_string())
    
    # 登出
    source.logout()
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
