#!/usr/bin/env python3
"""
股票数据源接入模块
支持多数据源: akshare(免费)、tushare(专业)、baostock、yfinance、tencent(备用)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class StockData:
    """股票数据结构"""
    code: str                    # 股票代码
    name: str                    # 股票名称
    price: float                 # 当前价格
    change_percent: float        # 涨跌幅%
    volume: int                  # 成交量
    turnover: float              # 成交额
    market_cap: Optional[float] = None  # 市值
    pe: Optional[float] = None   # 市盈率
    pb: Optional[float] = None   # 市净率


@dataclass
class MarketOverview:
    """市场概况数据结构"""
    index_name: str              # 指数名称
    index_code: str              # 指数代码
    price: float                 # 当前点数
    change_percent: float        # 涨跌幅%
    volume: float                # 成交量(亿)
    up_count: int                # 上涨家数
    down_count: int              # 下跌家数


@dataclass  
class LimitUpStock:
    """涨停股数据结构"""
    code: str
    name: str
    price: float
    limit_up_days: int           # 连板天数
    concept: Optional[str] = None  # 所属概念


class DataSource(ABC):
    """数据源抽象基类"""
    
    @abstractmethod
    def get_a_stock_spot(self) -> List[StockData]:
        """获取A股实时行情"""
        pass
    
    @abstractmethod
    def get_index_spot(self) -> List[MarketOverview]:
        """获取大盘指数行情"""
        pass
    
    @abstractmethod
    def get_limit_up_stocks(self, date: str = None) -> List[LimitUpStock]:
        """获取涨停股列表"""
        pass
    
    @abstractmethod
    def get_stock_history(self, code: str, days: int = 30) -> Any:
        """获取个股历史数据"""
        pass


class AKShareDataSource(DataSource):
    """
    AkShare 免费数据源
    文档: https://www.akshare.xyz/
    优点: 免费、Python原生、数据全面
    缺点: 偶尔不稳定、有频率限制
    """
    
    def __init__(self):
        self.name = "akshare"
        self.enabled = True
        try:
            import akshare as ak
            self.ak = ak
            logger.info(f"✅ AkShare 数据源初始化成功 (版本: {ak.__version__})")
        except ImportError:
            logger.error("❌ 请先安装 akshare: pip install akshare")
            self.enabled = False
    
    def get_a_stock_spot(self) -> List[StockData]:
        """获取A股实时行情 - 东方财富数据源"""
        if not self.enabled:
            return []
        
        try:
            df = self.ak.stock_zh_a_spot_em()
            stocks = []
            
            for _, row in df.iterrows():
                try:
                    stock = StockData(
                        code=str(row['代码']).zfill(6),
                        name=row['名称'],
                        price=float(row['最新价']) if pd.notna(row['最新价']) else 0.0,
                        change_percent=float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0.0,
                        volume=int(row['成交量']) if pd.notna(row['成交量']) else 0,
                        turnover=float(row['成交额']) if pd.notna(row['成交额']) else 0.0,
                        market_cap=float(row['总市值']) if '总市值' in row and pd.notna(row['总市值']) else None,
                        pe=float(row['市盈率-动态']) if '市盈率-动态' in row and pd.notna(row['市盈率-动态']) else None,
                        pb=float(row['市净率']) if '市净率' in row and pd.notna(row['市净率']) else None
                    )
                    stocks.append(stock)
                except Exception as e:
                    continue
            
            logger.info(f"✅ 获取A股实时行情成功: {len(stocks)} 只股票")
            return stocks
            
        except Exception as e:
            logger.error(f"❌ 获取A股实时行情失败: {e}")
            return []
    
    def get_index_spot(self) -> List[MarketOverview]:
        """获取大盘指数行情"""
        if not self.enabled:
            return []
        
        try:
            # 主要指数代码映射
            index_codes = {
                'sh000001': '上证指数',
                'sz399001': '深证成指', 
                'sz399006': '创业板指',
                'sh000688': '科创50',
                'sh000300': '沪深300',
                'sh000016': '上证50',
                'sz399905': '中证500'
            }
            
            markets = []
            for code, name in index_codes.items():
                try:
                    # 获取单指数数据
                    df = self.ak.index_zh_a_hist(symbol=code[:6], period="daily", 
                                                  start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'),
                                                  end_date=datetime.now().strftime('%Y%m%d'))
                    if not df.empty:
                        latest = df.iloc[-1]
                        prev = df.iloc[-2] if len(df) > 1 else latest
                        
                        market = MarketOverview(
                            index_name=name,
                            index_code=code,
                            price=float(latest['收盘']),
                            change_percent=round((latest['收盘'] - prev['收盘']) / prev['收盘'] * 100, 2),
                            volume=float(latest['成交量']) / 1e8,  # 转换为亿
                            up_count=0,  # 需要单独获取
                            down_count=0
                        )
                        markets.append(market)
                except Exception as e:
                    continue
            
            logger.info(f"✅ 获取大盘指数成功: {len(markets)} 个指数")
            return markets
            
        except Exception as e:
            logger.error(f"❌ 获取大盘指数失败: {e}")
            return []
    
    def get_limit_up_stocks(self, date: str = None) -> List[LimitUpStock]:
        """获取涨停股列表"""
        if not self.enabled:
            return []
        
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        try:
            df = self.ak.stock_zt_pool_em(date=date)
            stocks = []
            
            for _, row in df.iterrows():
                try:
                    stock = LimitUpStock(
                        code=str(row['代码']).zfill(6),
                        name=row['名称'],
                        price=float(row['最新价']),
                        limit_up_days=int(row.get('连板数', 1)),
                        concept=row.get('所属行业', '')
                    )
                    stocks.append(stock)
                except Exception as e:
                    continue
            
            logger.info(f"✅ 获取涨停股成功: {len(stocks)} 只 (日期: {date})")
            return stocks
            
        except Exception as e:
            logger.error(f"❌ 获取涨停股失败: {e}")
            return []
    
    def get_stock_history(self, code: str, days: int = 30) -> Any:
        """获取个股历史数据"""
        if not self.enabled:
            return None
        
        try:
            # 判断市场
            if code.startswith('6'):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = self.ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d')
            )
            
            logger.info(f"✅ 获取历史数据成功: {code}, {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"❌ 获取历史数据失败 {code}: {e}")
            return None


class TushareDataSource(DataSource):
    """
    Tushare 专业数据源 (需要Token)
    文档: https://tushare.pro/
    优点: 数据稳定、专业、全面
    缺点: 需要注册获取Token, 部分数据收费
    """
    
    def __init__(self, token: str = None):
        self.name = "tushare"
        self.token = token or self._get_token_from_env()
        self.enabled = False
        self.pro = None
        
        if not self.token:
            logger.warning("⚠️ 未配置 Tushare Token, 跳过初始化")
            return
        
        try:
            import tushare as ts
            self.ts = ts
            self.ts.set_token(self.token)
            self.pro = self.ts.pro_api()
            self.enabled = True
            logger.info("✅ Tushare 数据源初始化成功")
        except ImportError:
            logger.error("❌ 请先安装 tushare: pip install tushare")
        except Exception as e:
            logger.error(f"❌ Tushare 初始化失败: {e}")
    
    def _get_token_from_env(self) -> Optional[str]:
        """从环境变量获取Token"""
        import os
        return os.environ.get('TUSHARE_TOKEN')
    
    def get_a_stock_spot(self) -> List[StockData]:
        """获取A股实时行情"""
        if not self.enabled:
            return []
        
        try:
            # Tushare实时行情需要积分,这里使用日线数据作为替代
            df = self.pro.daily(trade_date=datetime.now().strftime('%Y%m%d'))
            
            if df is None or df.empty:
                # 获取最新交易日
                trade_cal = self.pro.trade_cal(exchange='SSE', 
                                               start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                                               end_date=datetime.now().strftime('%Y%m%d'))
                last_trade_date = trade_cal[trade_cal['is_open']==1]['cal_date'].max()
                df = self.pro.daily(trade_date=last_trade_date)
            
            # 获取股票基本信息
            stock_basic = self.pro.stock_basic(exchange='', list_status='L')
            stock_dict = dict(zip(stock_basic['ts_code'], stock_basic['name']))
            
            stocks = []
            for _, row in df.iterrows():
                try:
                    ts_code = row['ts_code']
                    code = ts_code.split('.')[0]
                    
                    stock = StockData(
                        code=code,
                        name=stock_dict.get(ts_code, ''),
                        price=float(row['close']),
                        change_percent=round((row['close'] - row['pre_close']) / row['pre_close'] * 100, 2),
                        volume=int(row['vol'] * 100),  # 手转股
                        turnover=float(row['amount'] * 1000)  # 千元转元
                    )
                    stocks.append(stock)
                except Exception as e:
                    continue
            
            logger.info(f"✅ Tushare 获取A股行情成功: {len(stocks)} 只")
            return stocks
            
        except Exception as e:
            logger.error(f"❌ Tushare 获取行情失败: {e}")
            return []
    
    def get_index_spot(self) -> List[MarketOverview]:
        """获取大盘指数"""
        if not self.enabled:
            return []
        
        # 类似实现...
        logger.info("ℹ️ Tushare 指数数据暂未实现")
        return []
    
    def get_limit_up_stocks(self, date: str = None) -> List[LimitUpStock]:
        """获取涨停股"""
        if not self.enabled:
            return []
        
        # Tushare需要高积分才能获取
        logger.info("ℹ️ Tushare 涨停数据需要高积分,建议使用 AkShare")
        return []
    
    def get_stock_history(self, code: str, days: int = 30) -> Any:
        """获取历史数据"""
        if not self.enabled:
            return None
        
        try:
            # 判断市场后缀
            if code.startswith('6'):
                ts_code = f"{code}.SH"
            else:
                ts_code = f"{code}.SZ"
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = self.pro.daily(ts_code=ts_code,
                               start_date=start_date.strftime('%Y%m%d'),
                               end_date=end_date.strftime('%Y%m%d'))
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 获取历史数据失败: {e}")
            return None


class DataSourceManager:
    """数据源管理器 - 统一管理多个数据源"""
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
        self._init_sources()
    
    def _init_sources(self):
        """初始化所有数据源"""
        # 1. AkShare (免费首选)
        ak_source = AKShareDataSource()
        if ak_source.enabled:
            self.sources['akshare'] = ak_source
        
        # 2. Tushare (备用,需要Token)
        ts_source = TushareDataSource()
        if ts_source.enabled:
            self.sources['tushare'] = ts_source
    
    def get_source(self, name: str = None) -> Optional[DataSource]:
        """
        获取数据源
        name: 指定数据源名称,None则返回第一个可用的
        """
        if name and name in self.sources:
            return self.sources[name]
        
        # 返回第一个可用的
        for source in self.sources.values():
            if source.enabled:
                return source
        
        return None
    
    def get_all_sources(self) -> Dict[str, DataSource]:
        """获取所有数据源"""
        return self.sources
    
    def get_stock_spot(self) -> List[StockData]:
        """获取实时行情 - 使用第一个可用的数据源"""
        source = self.get_source()
        if source:
            return source.get_a_stock_spot()
        return []
    
    def get_market_overview(self) -> List[MarketOverview]:
        """获取市场概况"""
        source = self.get_source()
        if source:
            return source.get_index_spot()
        return []


# 全局数据源管理器实例
data_manager = DataSourceManager()


if __name__ == "__main__":
    """测试数据源"""
    print("=" * 60)
    print("股票数据源测试")
    print("=" * 60)
    
    # 测试数据源状态
    sources = data_manager.get_all_sources()
    print(f"\n已加载数据源: {list(sources.keys())}")
    
    # 测试获取实时行情
    print("\n--- 测试获取A股实时行情 ---")
    stocks = data_manager.get_stock_spot()
    if stocks:
        print(f"获取到 {len(stocks)} 只股票")
        # 显示前3只
        for stock in stocks[:3]:
            print(f"  {stock.code} {stock.name}: ¥{stock.price} ({stock.change_percent}%)")
    else:
        print("⚠️ 未能获取数据,请检查网络连接或交易时间")
    
    # 测试获取市场概况
    print("\n--- 测试获取大盘指数 ---")
    markets = data_manager.get_market_overview()
    if markets:
        for market in markets:
            print(f"  {market.index_name}: {market.price} ({market.change_percent}%)")
    else:
        print("⚠️ 未能获取指数数据")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)