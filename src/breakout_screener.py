#!/usr/bin/env python3
"""
增强版突破策略选股器 V1.0
策略核心：突破20日高点 + 放量 + 板块强势

买入条件：
1. 股价突破前20日最高价
2. 成交量 > 20日均量 × 1.5
3. 所在板块排名前3
4. RS相对强度 > 70
5. 排除ST/*ST/退市/新股

止损：突破点下方-3%
止盈：目标价+15% 或 跌破10日均线
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class BreakoutSignal:
    """突破信号"""
    code: str
    name: str
    sector: str
    current_price: float
    breakout_price: float  # 突破价（20日高点）
    volume_ratio: float  # 量比
    rs_rating: int  # RS强度评分 0-100
    sector_rank: int  # 板块排名
    score: int  # 综合得分
    stop_loss: float  # 止损价
    target_price: float  # 目标价
    reason: str  # 选股理由


class BreakoutScreener:
    """
    突破策略选股器
    
    核心逻辑：
    - 技术面：突破20日新高，趋势确立
    - 量能：放量突破，资金认可
    - 板块：强势板块中的突破更可靠
    - RS强度：相对大盘强度，筛选强势股
    """
    
    def __init__(self, data_source=None):
        self.data_source = data_source
        self.lookback_days = 20  # 突破周期
        self.volume_threshold = 1.5  # 量比阈值
        self.rs_threshold = 70  # RS强度阈值
        
    def screen_breakout_stocks(self, 
                                all_stocks: List[Dict],
                                top_sectors: List[Dict],
                                max_results: int = 5) -> List[BreakoutSignal]:
        """
        筛选突破股票
        
        Args:
            all_stocks: 全市场股票数据
            top_sectors: 强势板块列表
            max_results: 最多返回几只
            
        Returns:
            突破信号列表
        """
        logger.info("开始执行突破策略选股...")
        
        # 获取强势板块的股票代码集合
        hot_sector_codes = set()
        sector_rank_map = {}
        for i, sector in enumerate(top_sectors[:5], 1):
            codes = sector.get('codes', [])
            hot_sector_codes.update(codes)
            for code in codes:
                sector_rank_map[code] = i
        
        logger.info(f"强势板块包含 {len(hot_sector_codes)} 只股票")
        
        # 筛选突破股票
        breakout_candidates = []
        
        for stock in all_stocks:
            code = stock.get('code')
            
            # 基础过滤
            if not self._is_tradable(stock):
                continue
            
            # 检查是否在强势板块
            if code not in hot_sector_codes:
                continue
            
            # 获取历史数据计算突破
            hist_data = self._get_stock_history(code, days=self.lookback_days + 5)
            if hist_data is None or len(hist_data) < self.lookback_days:
                continue
            
            # 检查是否突破
            breakout_info = self._check_breakout(stock, hist_data)
            if not breakout_info:
                continue
            
            # 计算RS强度
            rs_rating = self._calculate_rs_rating(code, hist_data)
            if rs_rating < self.rs_threshold:
                continue
            
            # 检查量能
            volume_ratio = stock.get('volume_ratio', 1.0)
            if volume_ratio < self.volume_threshold:
                continue
            
            # 计算得分
            sector_rank = sector_rank_map.get(code, 99)
            score = self._calculate_score(
                breakout_info['breakout_strength'],
                volume_ratio,
                rs_rating,
                sector_rank
            )
            
            # 生成信号
            signal = BreakoutSignal(
                code=code,
                name=stock.get('name', code),
                sector=stock.get('sector', '其他'),
                current_price=stock.get('price', 0),
                breakout_price=breakout_info['high_20d'],
                volume_ratio=volume_ratio,
                rs_rating=rs_rating,
                sector_rank=sector_rank,
                score=score,
                stop_loss=round(breakout_info['high_20d'] * 0.97, 3),
                target_price=round(stock.get('price', 0) * 1.15, 3),
                reason=breakout_info['reason']
            )
            
            breakout_candidates.append(signal)
            logger.debug(f"突破候选: {signal.name} 得分{score} RS{rs_rating}")
        
        # 排序并返回
        breakout_candidates.sort(key=lambda x: x.score, reverse=True)
        selected = breakout_candidates[:max_results]
        
        logger.info(f"突破策略选股完成，选中 {len(selected)} 只")
        for s in selected:
            logger.info(f"  {s.name}({s.code}): 突破价¥{s.breakout_price} 得分{s.score}")
        
        return selected
    
    def _is_tradable(self, stock: Dict) -> bool:
        """检查股票是否可以买入"""
        name = stock.get('name', '')
        code = stock.get('code', '')
        
        if not name or not code:
            return False
        
        # 过滤ST/*ST
        if 'ST' in name or name.startswith('*ST'):
            return False
        
        # 过滤退市
        if '退' in name:
            return False
        
        # 过滤新股（N开头）
        if name.startswith('N'):
            return False
        
        # 过滤北交所
        if code.startswith(('43', '83', '87')):
            return False
        
        # 过滤B股
        if code.startswith(('900', '200')):
            return False
        
        # 价格检查
        price = stock.get('price', 0)
        if price <= 0 or price > 500:  # 排除异常价格
            return False
        
        return True
    
    def _get_stock_history(self, code: str, days: int = 25) -> Optional[pd.DataFrame]:
        """
        获取股票历史数据
        
        实际使用时需要接入数据源，这里提供模拟数据用于测试
        """
        try:
            # TODO: 接入实际数据源
            # 这里生成模拟数据用于测试
            dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
            
            # 生成有趋势的价格数据
            base_price = 10.0
            prices = []
            for i in range(days):
                # 添加一些随机性和趋势
                change = np.random.normal(0.002, 0.02)
                if i > days - 5:  # 最后5天模拟上涨趋势
                    change += 0.015
                base_price *= (1 + change)
                prices.append(base_price)
            
            df = pd.DataFrame({
                'date': dates,
                'close': prices,
                'high': [p * (1 + np.random.uniform(0, 0.01)) for p in prices],
                'low': [p * (1 - np.random.uniform(0, 0.01)) for p in prices],
                'volume': [np.random.randint(1000000, 5000000) for _ in prices]
            })
            
            return df
            
        except Exception as e:
            logger.warning(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def _check_breakout(self, stock: Dict, hist_data: pd.DataFrame) -> Optional[Dict]:
        """
        检查是否突破20日高点
        
        Returns:
            None - 未突破
            Dict - 突破信息
        """
        current_price = stock.get('price', 0)
        current_volume = stock.get('volume', 0)
        
        if current_price <= 0:
            return None
        
        # 计算20日高点（不含今日）
        hist_high = hist_data['high'].iloc[:-1].max()
        hist_volume_avg = hist_data['volume'].iloc[:-1].mean()
        
        # 检查是否突破
        if current_price <= hist_high * 1.01:  # 允许1%的误差
            return None
        
        # 计算突破强度
        breakout_strength = (current_price - hist_high) / hist_high * 100
        
        # 检查量能
        volume_ratio = current_volume / hist_volume_avg if hist_volume_avg > 0 else 1.0
        
        # 生成理由
        reason = f"突破20日高点¥{hist_high:.2f}，强度{breakout_strength:.1f}%，量比{volume_ratio:.1f}"
        
        return {
            'high_20d': round(hist_high, 3),
            'breakout_strength': breakout_strength,
            'volume_ratio': volume_ratio,
            'reason': reason
        }
    
    def _calculate_rs_rating(self, code: str, hist_data: pd.DataFrame) -> int:
        """
        计算RS相对强度评分
        
        RS = 个股涨幅 / 大盘涨幅 × 100
        评分0-100，越高表示相对大盘越强
        """
        try:
            # 计算个股涨幅
            price_20d_ago = hist_data['close'].iloc[0]
            price_now = hist_data['close'].iloc[-1]
            stock_return = (price_now - price_20d_ago) / price_20d_ago * 100
            
            # TODO: 获取同期大盘涨幅（这里用模拟数据）
            # 实际应该获取上证指数同期涨幅
            market_return = np.random.uniform(-2, 3)  # 模拟大盘涨幅
            
            # 计算RS
            if market_return != 0:
                rs = (stock_return / market_return) * 100
            else:
                rs = 100 if stock_return > 0 else 50
            
            # 转换为0-100评分
            # 假设RS在50-150之间映射到0-100分
            rs_rating = int((rs - 50) / 100 * 100)
            rs_rating = max(0, min(100, rs_rating))
            
            return rs_rating
            
        except Exception as e:
            logger.warning(f"计算 {code} RS评分失败: {e}")
            return 50
    
    def _calculate_score(self, 
                         breakout_strength: float,
                         volume_ratio: float,
                         rs_rating: int,
                         sector_rank: int) -> int:
        """
        计算综合得分
        
        各项权重：
        - 突破强度 30%
        - 量能 25%
        - RS强度 25%
        - 板块排名 20%
        """
        # 突破强度得分 (0-30)
        if breakout_strength > 5:
            breakout_score = 30
        elif breakout_strength > 3:
            breakout_score = 25
        elif breakout_strength > 1:
            breakout_score = 20
        else:
            breakout_score = 15
        
        # 量能得分 (0-25)
        if volume_ratio > 3:
            volume_score = 25
        elif volume_ratio > 2:
            volume_score = 20
        elif volume_ratio > 1.5:
            volume_score = 15
        else:
            volume_score = 10
        
        # RS得分 (0-25)
        rs_score = min(25, int(rs_rating / 100 * 25))
        
        # 板块排名得分 (0-20)
        if sector_rank == 1:
            sector_score = 20
        elif sector_rank == 2:
            sector_score = 15
        elif sector_rank == 3:
            sector_score = 10
        else:
            sector_score = 5
        
        total_score = breakout_score + volume_score + rs_score + sector_score
        return min(100, total_score)


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("突破策略选股器测试")
    print("=" * 60)
    
    screener = BreakoutScreener()
    
    # 模拟股票数据
    test_stocks = [
        {'code': '000001', 'name': '平安银行', 'price': 10.5, 'volume': 5000000, 'sector': '银行', 'volume_ratio': 2.0},
        {'code': '000002', 'name': '万科A', 'price': 15.2, 'volume': 8000000, 'sector': '房地产', 'volume_ratio': 1.8},
        {'code': '002594', 'name': '比亚迪', 'price': 250.0, 'volume': 12000000, 'sector': '新能源', 'volume_ratio': 2.5},
    ]
    
    test_sectors = [
        {'name': '新能源', 'codes': ['002594', '300750', '601012']},
        {'name': '银行', 'codes': ['000001', '600036']},
    ]
    
    results = screener.screen_breakout_stocks(test_stocks, test_sectors, max_results=3)
    
    print(f"\n选中 {len(results)} 只突破股票:")
    for r in results:
        print(f"\n{r.name} ({r.code}):")
        print(f"  当前价: ¥{r.current_price}")
        print(f"  突破价: ¥{r.breakout_price}")
        print(f"  止损价: ¥{r.stop_loss}")
        print(f"  目标价: ¥{r.target_price}")
        print(f"  RS强度: {r.rs_rating}")
        print(f"  综合得分: {r.score}")
        print(f"  理由: {r.reason}")
    
    print("\n" + "=" * 60)
