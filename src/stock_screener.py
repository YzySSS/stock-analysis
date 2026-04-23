#!/usr/bin/env python3
"""
股票筛选器 - 从全市场筛选优质股票
支持：技术指标筛选 / 热点板块 / 龙虎榜 / 成交量异动
"""

import logging
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class StockScreenResult:
    """筛选结果"""
    code: str
    name: str
    price: float
    change_pct: float
    volume_ratio: float  # 量比
    score: int  # 综合评分
    reason: str  # 入选理由
    sector: str = ""  # 所属板块


class StockScreener:
    """
    股票筛选器
    
    筛选策略：
    1. 涨幅榜 - 今日强势股票
    2. 量比榜 - 成交量放大（资金关注）
    3. 热点板块 - 行业龙头股
    4. 技术指标 - 金叉/多头排列
    """
    
    def __init__(self):
        from data_source import data_manager
        self.data_source = data_manager.get_source()
    
    def screen_stocks(self, 
                     strategy: str = "comprehensive",
                     max_results: int = 20) -> List[StockScreenResult]:
        """
        筛选股票
        
        Args:
            strategy: 筛选策略
                - 'comprehensive': 综合筛选（推荐）
                - 'gainers': 今日涨幅榜
                - 'volume_spike': 放量异动
                - 'technical': 技术指标金叉
                - 'hot_sector': 热点板块
            max_results: 最多返回几只
        
        Returns:
            筛选结果列表
        """
        logger.info(f"开始筛选股票，策略: {strategy}")
        
        if strategy == "gainers":
            return self._screen_by_gainers(max_results)
        elif strategy == "volume_spike":
            return self._screen_by_volume(max_results)
        elif strategy == "technical":
            return self._screen_by_technical(max_results)
        elif strategy == "hot_sector":
            return self._screen_by_sector(max_results)
        else:  # comprehensive
            return self._screen_comprehensive(max_results)
    
    def _screen_by_gainers(self, max_results: int) -> List[StockScreenResult]:
        """按涨幅筛选"""
        logger.info("筛选今日强势股...")
        
        if not self.data_source:
            return []
        
        try:
            # 获取全部A股实时行情
            stocks = self.data_source.get_a_stock_spot()
            
            # 按涨幅排序
            sorted_stocks = sorted(stocks, 
                                 key=lambda x: x.change_percent, 
                                 reverse=True)
            
            results = []
            for stock in sorted_stocks[:max_results]:
                # 过滤ST股和科创板（波动太大）
                if 'ST' in stock.name or stock.code.startswith('688'):
                    continue
                
                results.append(StockScreenResult(
                    code=stock.code + ('.SH' if stock.code.startswith('6') else '.SZ'),
                    name=stock.name,
                    price=stock.price,
                    change_pct=stock.change_percent,
                    volume_ratio=stock.volume / max(stock.volume_5d_avg, 1),
                    score=min(int(50 + stock.change_pct), 95),
                    reason=f"今日涨幅{stock.change_percent:+.2f}%，强势上涨"
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"涨幅筛选失败: {e}")
            return []
    
    def _screen_by_volume(self, max_results: int) -> List[StockScreenResult]:
        """按成交量筛选（量比>2）"""
        logger.info("筛选放量异动股...")
        
        if not self.data_source:
            return []
        
        try:
            stocks = self.data_source.get_a_stock_spot()
            
            # 计算量比并筛选
            volume_stocks = []
            for stock in stocks:
                volume_ratio = stock.volume / max(stock.volume_5d_avg, 1)
                if volume_ratio > 2 and stock.change_percent > 0:
                    volume_stocks.append((stock, volume_ratio))
            
            # 按量比排序
            volume_stocks.sort(key=lambda x: x[1], reverse=True)
            
            results = []
            for stock, ratio in volume_stocks[:max_results]:
                results.append(StockScreenResult(
                    code=stock.code + ('.SH' if stock.code.startswith('6') else '.SZ'),
                    name=stock.name,
                    price=stock.price,
                    change_pct=stock.change_percent,
                    volume_ratio=ratio,
                    score=min(int(60 + ratio * 10), 90),
                    reason=f"量比{ratio:.2f}，成交量放大，资金关注"
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"成交量筛选失败: {e}")
            return []
    
    def _screen_by_technical(self, max_results: int) -> List[StockScreenResult]:
        """按技术指标筛选（金叉/多头排列）"""
        logger.info("筛选技术指标优质股...")
        
        from technical_analysis import TechnicalAnalyzer
        analyzer = TechnicalAnalyzer()
        
        # 先获取强势股候选
        candidates = self._screen_by_gainers(50)
        
        results = []
        for candidate in candidates:
            try:
                # 获取历史数据
                df = self.data_source.get_stock_history(candidate.code, days=60)
                if df is None or len(df) < 30:
                    continue
                
                # 技术分析
                indicators = analyzer.analyze(df)
                
                # 筛选条件：综合评分>70 且 信号为买入
                if indicators['composite_score'] >= 70 and \
                   indicators['signal'] in ['买入', '强烈买入']:
                    
                    results.append(StockScreenResult(
                        code=candidate.code,
                        name=candidate.name,
                        price=candidate.price,
                        change_pct=candidate.change_pct,
                        volume_ratio=candidate.volume_ratio,
                        score=indicators['composite_score'],
                        reason=f"技术指标优秀：{indicators['signal']}，评分{indicators['composite_score']}"
                    ))
                    
                    if len(results) >= max_results:
                        break
                        
            except Exception as e:
                continue
        
        return results
    
    def _screen_by_sector(self, max_results: int) -> List[StockScreenResult]:
        """按热点板块筛选"""
        logger.info("筛选热点板块龙头股...")
        
        # 热点板块（可以根据市场动态调整）
        hot_sectors = {
            '人工智能': ['000938', '002230', '300418'],
            '新能源': ['002594', '300750', '601012'],
            '芯片': ['002371', '603501', '300782'],
            '医药': ['600276', '000538', '300760'],
            '金融': ['000001', '600030', '601318'],
        }
        
        results = []
        for sector, codes in hot_sectors.items():
            for code in codes[:2]:  # 每个板块取前2只
                try:
                    # 获取实时行情
                    stocks = self.data_source.get_a_stock_spot()
                    stock = next((s for s in stocks if s.code == code), None)
                    
                    if stock and stock.change_percent > -2:  # 排除大跌的
                        results.append(StockScreenResult(
                            code=code + ('.SH' if code.startswith('6') else '.SZ'),
                            name=stock.name,
                            price=stock.price,
                            change_pct=stock.change_percent,
                            volume_ratio=1.0,
                            score=60 + int(stock.change_percent),
                            reason=f"热点板块[{sector}]龙头股",
                            sector=sector
                        ))
                except:
                    continue
        
        # 按涨幅排序
        results.sort(key=lambda x: x.change_pct, reverse=True)
        return results[:max_results]
    
    def _screen_comprehensive(self, max_results: int) -> List[StockScreenResult]:
        """综合筛选（多维度打分）"""
        logger.info("执行综合筛选...")
        
        # 1. 获取涨幅榜
        gainers = self._screen_by_gainers(30)
        
        # 2. 获取放量股
        volume_stocks = self._screen_by_volume(30)
        
        # 3. 合并并去重
        all_stocks = {}
        
        for stock in gainers:
            all_stocks[stock.code] = stock
        
        for stock in volume_stocks:
            if stock.code in all_stocks:
                # 更新分数（涨幅+成交量双重优势）
                all_stocks[stock.code].score = min(
                    all_stocks[stock.code].score + 10, 
                    95
                )
                all_stocks[stock.code].reason += " + " + stock.reason
            else:
                all_stocks[stock.code] = stock
        
        # 4. 按分数排序
        sorted_stocks = sorted(all_stocks.values(), 
                              key=lambda x: x.score, 
                              reverse=True)
        
        return sorted_stocks[:max_results]
    
    def get_recommended_stocks(self, 
                              count: int = 10,
                              exclude_codes: List[str] = None) -> List[str]:
        """
        获取推荐股票代码列表（简化接口）
        
        Args:
            count: 推荐几只
            exclude_codes: 排除的股票代码（如已持仓）
        
        Returns:
            股票代码列表
        """
        results = self.screen_stocks(strategy="comprehensive", max_results=count+5)
        
        codes = [r.code for r in results]
        
        # 排除指定股票
        if exclude_codes:
            codes = [c for c in codes if c not in exclude_codes]
        
        return codes[:count]


if __name__ == "__main__":
    print("🧪 股票筛选器测试")
    print("="*70)
    
    screener = StockScreener()
    
    # 测试各种筛选策略
    strategies = [
        ("今日强势股", "gainers"),
        ("放量异动", "volume_spike"),
        ("技术指标", "technical"),
        ("热点板块", "hot_sector"),
        ("综合筛选", "comprehensive"),
    ]
    
    for name, strategy in strategies:
        print(f"\n📊 {name}:")
        print("-"*70)
        
        try:
            results = screener.screen_stocks(strategy=strategy, max_results=5)
            
            for i, r in enumerate(results, 1):
                emoji = "🟢" if r.change_pct > 5 else "🟡" if r.change_pct > 0 else "🔴"
                print(f"{i}. {emoji} {r.name}({r.code})")
                print(f"   涨幅: {r.change_pct:+.2f}% | 评分: {r.score}")
                print(f"   理由: {r.reason[:60]}...")
                print()
                
        except Exception as e:
            print(f"  筛选失败: {e}")
    
    print("\n" + "="*70)
    print("✅ 测试完成！")
    print("\n使用方式:")
    print("  from stock_screener import StockScreener")
    print("  screener = StockScreener()")
    print("  stocks = screener.get_recommended_stocks(count=10)")
