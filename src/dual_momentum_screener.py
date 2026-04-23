#!/usr/bin/env python3
"""
双动量策略选股器 V1.0
策略核心：个股动量 + 板块动量双重验证

理念来自Gary Antonacci的Dual Momentum Investing

买入条件：
1. 个股20日涨幅排名前10%
2. 所在板块20日涨幅排名前5%
3. 相对强度RS > 60
4. 成交量大于20日均量（确认动能）

卖出条件：
1. 个股动量排名掉出前30%
2. 或板块动量排名掉出前20%
3. 或跌破10日均线

相比单一动量，双动量能过滤掉弱势板块中的强势股（假强势）
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class MomentumScore:
    """动量评分"""
    code: str
    name: str
    sector: str
    price: float
    change_pct_20d: float  # 20日涨幅
    stock_momentum_rank: int  # 个股动量排名
    sector_momentum_rank: int  # 板块动量排名
    rs_rating: int  # 相对强度
    volume_ratio: float  # 量比
    composite_score: float  # 综合得分
    signal: str  # 信号: 买入/持有/卖出/观望


class DualMomentumScreener:
    """
    双动量选股器
    
    核心逻辑：
    - 绝对动量：个股自身涨幅
    - 相对动量：个股相对板块和大盘
    - 双重过滤：个股动量+板块动量同时验证
    
    优势：
    1. 避免弱势板块中的假突破
    2. 提高胜率，减少震荡市假信号
    3. 顺势而为，买在板块主升浪
    """
    
    def __init__(self, data_source=None):
        self.data_source = data_source
        self.lookback_days = 20  # 动量计算周期
        
        # 参数设置
        self.stock_top_pct = 0.10  # 个股前10%
        self.sector_top_pct = 0.05  # 板块前5%
        self.rs_threshold = 60  # RS阈值
        self.volume_threshold = 1.2  # 量比阈值
        
        # 卖出阈值（相对宽松，避免过早卖出）
        self.stock_exit_pct = 0.30  # 个股掉出前30%
        self.sector_exit_pct = 0.20  # 板块掉出前20%
    
    def screen(self, 
               all_stocks: List[Dict],
               sectors_data: Dict[str, List[Dict]],
               max_results: int = 10) -> List[MomentumScore]:
        """
        双动量选股
        
        Args:
            all_stocks: 全市场股票列表
            sectors_data: 板块数据 {板块名: [股票列表]}
            max_results: 最多返回结果数
            
        Returns:
            动量评分列表
        """
        logger.info("开始执行双动量选股...")
        
        # 步骤1：计算所有股票的20日涨幅
        stock_returns = self._calculate_stock_returns(all_stocks)
        
        # 步骤2：计算所有板块的20日涨幅
        sector_returns = self._calculate_sector_returns(sectors_data)
        
        # 步骤3：计算个股动量排名
        stock_ranks = self._rank_stocks_by_momentum(stock_returns)
        
        # 步骤4：计算板块动量排名
        sector_ranks = self._rank_sectors_by_momentum(sector_returns)
        
        # 步骤5：双动量筛选
        candidates = []
        
        for stock in all_stocks:
            code = stock.get('code')
            
            # 基础过滤
            if not self._is_tradable(stock):
                continue
            
            # 获取个股动量数据
            if code not in stock_returns:
                continue
            
            stock_return = stock_returns[code]
            stock_rank = stock_ranks.get(code, 9999)
            total_stocks = len(stock_ranks)
            stock_rank_pct = stock_rank / total_stocks
            
            # 获取板块动量数据
            sector = stock.get('sector', '其他')
            if sector not in sector_returns:
                continue
            
            sector_return = sector_returns[sector]
            sector_rank = sector_ranks.get(sector, 9999)
            total_sectors = len(sector_ranks)
            sector_rank_pct = sector_rank / total_sectors
            
            # 计算RS强度
            rs_rating = self._calculate_rs_rating(stock_return, sector_return)
            
            # 检查量能
            volume_ratio = stock.get('volume_ratio', 1.0)
            
            # 计算综合得分
            composite_score = self._calculate_composite_score(
                stock_rank_pct, sector_rank_pct, rs_rating, volume_ratio
            )
            
            # 判断信号
            signal = self._determine_signal(
                stock_rank_pct, sector_rank_pct, rs_rating, volume_ratio
            )
            
            momentum = MomentumScore(
                code=code,
                name=stock.get('name', code),
                sector=sector,
                price=stock.get('price', 0),
                change_pct_20d=stock_return,
                stock_momentum_rank=stock_rank,
                sector_momentum_rank=sector_rank,
                rs_rating=rs_rating,
                volume_ratio=volume_ratio,
                composite_score=composite_score,
                signal=signal
            )
            
            candidates.append(momentum)
        
        # 排序：综合得分高的在前
        candidates.sort(key=lambda x: x.composite_score, reverse=True)
        
        # 只返回有买入信号的前N个
        buy_candidates = [c for c in candidates if c.signal == '买入'][:max_results]
        
        logger.info(f"双动量选股完成，选中 {len(buy_candidates)} 只")
        for c in buy_candidates[:5]:
            logger.info(f"  {c.name}({c.code}): 得分{c.composite_score:.1f} "
                       f"个排{c.stock_momentum_rank} 板排{c.sector_momentum_rank}")
        
        return buy_candidates
    
    def _calculate_stock_returns(self, stocks: List[Dict]) -> Dict[str, float]:
        """计算所有股票的20日涨幅"""
        returns = {}
        
        for stock in stocks:
            code = stock.get('code')
            change_pct = stock.get('change_pct_20d')  # 假设数据中已有
            
            if change_pct is None:
                # 如果没有现成数据，尝试从数据源获取
                hist_data = self._get_stock_history(code, days=self.lookback_days)
                if hist_data is not None and len(hist_data) >= 2:
                    price_now = hist_data['close'].iloc[-1]
                    price_20d_ago = hist_data['close'].iloc[0]
                    change_pct = (price_now - price_20d_ago) / price_20d_ago * 100
                else:
                    change_pct = stock.get('change_pct', 0)  # 用当日涨幅代替
            
            returns[code] = change_pct
        
        return returns
    
    def _calculate_sector_returns(self, sectors_data: Dict[str, List[Dict]]) -> Dict[str, float]:
        """计算所有板块的20日涨幅（成分股平均）"""
        sector_returns = {}
        
        for sector_name, stocks in sectors_data.items():
            if not stocks:
                continue
            
            # 计算板块内股票平均涨幅
            total_return = 0
            count = 0
            
            for stock in stocks:
                code = stock.get('code')
                # 获取个股20日涨幅
                change_pct = stock.get('change_pct_20d')
                if change_pct is None:
                    change_pct = stock.get('change_pct', 0)
                
                total_return += change_pct
                count += 1
            
            if count > 0:
                sector_returns[sector_name] = total_return / count
            else:
                sector_returns[sector_name] = 0
        
        return sector_returns
    
    def _rank_stocks_by_momentum(self, stock_returns: Dict[str, float]) -> Dict[str, int]:
        """对股票按动量排名"""
        # 按涨幅排序
        sorted_stocks = sorted(stock_returns.items(), key=lambda x: x[1], reverse=True)
        
        # 生成排名字典
        ranks = {}
        for rank, (code, _) in enumerate(sorted_stocks, 1):
            ranks[code] = rank
        
        return ranks
    
    def _rank_sectors_by_momentum(self, sector_returns: Dict[str, float]) -> Dict[str, int]:
        """对板块按动量排名"""
        # 按涨幅排序
        sorted_sectors = sorted(sector_returns.items(), key=lambda x: x[1], reverse=True)
        
        # 生成排名字典
        ranks = {}
        for rank, (sector, _) in enumerate(sorted_sectors, 1):
            ranks[sector] = rank
        
        return ranks
    
    def _calculate_rs_rating(self, stock_return: float, sector_return: float) -> int:
        """
        计算RS相对强度评分
        
        RS = 个股涨幅相对于板块和大盘的综合评分
        """
        # 相对于板块的超额收益
        alpha_vs_sector = stock_return - sector_return
        
        # 基础分50
        base_score = 50
        
        # 根据超额收益加分
        if alpha_vs_sector > 10:
            base_score += 30
        elif alpha_vs_sector > 5:
            base_score += 20
        elif alpha_vs_sector > 0:
            base_score += 10
        elif alpha_vs_sector > -5:
            base_score -= 10
        else:
            base_score -= 20
        
        # 根据绝对涨幅调整
        if stock_return > 20:
            base_score += 10
        elif stock_return > 10:
            base_score += 5
        elif stock_return < -10:
            base_score -= 10
        
        return max(0, min(100, base_score))
    
    def _calculate_composite_score(self,
                                    stock_rank_pct: float,
                                    sector_rank_pct: float,
                                    rs_rating: int,
                                    volume_ratio: float) -> float:
        """
        计算综合得分
        
        权重：
        - 个股动量排名 35%
        - 板块动量排名 30%
        - RS强度 20%
        - 量能 15%
        """
        # 个股排名得分（排名越靠前得分越高）
        stock_score = (1 - stock_rank_pct) * 100 * 0.35
        
        # 板块排名得分
        sector_score = (1 - sector_rank_pct) * 100 * 0.30
        
        # RS得分
        rs_score = rs_rating * 0.20
        
        # 量能得分
        if volume_ratio >= 2:
            volume_score = 15
        elif volume_ratio >= 1.5:
            volume_score = 12
        elif volume_ratio >= 1.2:
            volume_score = 8
        else:
            volume_score = 5
        
        return stock_score + sector_score + rs_score + volume_score
    
    def _determine_signal(self,
                          stock_rank_pct: float,
                          sector_rank_pct: float,
                          rs_rating: int,
                          volume_ratio: float) -> str:
        """判断交易信号"""
        
        # 买入条件：双重动量验证
        if (stock_rank_pct <= self.stock_top_pct and  # 个股前10%
            sector_rank_pct <= self.sector_top_pct and  # 板块前5%
            rs_rating >= self.rs_threshold and  # RS足够强
            volume_ratio >= self.volume_threshold):  # 放量
            return '买入'
        
        # 持有条件：至少满足一个动量
        elif (stock_rank_pct <= self.stock_exit_pct or  # 个股还在前30%
              sector_rank_pct <= self.sector_exit_pct):  # 板块还在前20%
            return '持有'
        
        # 卖出条件：双重失效
        elif (stock_rank_pct > self.stock_exit_pct and  # 个股掉出前30%
              sector_rank_pct > self.sector_exit_pct):  # 板块掉出前20%
            return '卖出'
        
        else:
            return '观望'
    
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
        
        # 过滤新股
        if name.startswith('N'):
            return False
        
        # 过滤北交所
        if code.startswith(('43', '83', '87')):
            return False
        
        return True
    
    def _get_stock_history(self, code: str, days: int = 25) -> Optional[pd.DataFrame]:
        """获取股票历史数据（模拟）"""
        # TODO: 接入实际数据源
        return None
    
    def generate_report(self, candidates: List[MomentumScore]) -> str:
        """生成选股报告"""
        if not candidates:
            return "双动量选股：当前无买入信号"
        
        lines = [
            "\n" + "="*70,
            "双动量选股报告",
            "="*70,
            f"选股时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"动量周期: {self.lookback_days}日",
            f"入选标准: 个股前{self.stock_top_pct*100:.0f}% + 板块前{self.sector_top_pct*100:.0f}%",
            "",
            f"{'排名':<4}{'代码':<8}{'名称':<10}{'20日涨幅':<10}{'个排':<6}{'板排':<6}{'RS':<5}{'得分':<6}{'信号':<6}",
            "-"*70
        ]
        
        for i, c in enumerate(candidates, 1):
            lines.append(
                f"{i:<4}{c.code:<8}{c.name:<10}{c.change_pct_20d:>+7.1f}%  "
                f"{c.stock_momentum_rank:<6}{c.sector_momentum_rank:<6}"
                f"{c.rs_rating:<5}{c.composite_score:<6.1f}{c.signal:<6}"
            )
        
        lines.append("="*70)
        return "\n".join(lines)


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 70)
    print("双动量策略选股器测试")
    print("=" * 70)
    
    screener = DualMomentumScreener()
    
    # 模拟股票数据
    test_stocks = [
        {'code': '000001', 'name': '平安银行', 'sector': '银行', 'price': 10.5, 'volume_ratio': 1.3, 'change_pct_20d': 15.2},
        {'code': '000002', 'name': '万科A', 'sector': '房地产', 'price': 15.2, 'volume_ratio': 1.5, 'change_pct_20d': -5.3},
        {'code': '002594', 'name': '比亚迪', 'sector': '新能源', 'price': 250.0, 'volume_ratio': 2.1, 'change_pct_20d': 28.5},
        {'code': '300750', 'name': '宁德时代', 'sector': '新能源', 'price': 180.0, 'volume_ratio': 1.8, 'change_pct_20d': 22.1},
        {'code': '600519', 'name': '贵州茅台', 'sector': '白酒', 'price': 1650.0, 'volume_ratio': 1.1, 'change_pct_20d': 8.5},
    ]
    
    # 模拟板块数据
    test_sectors = {
        '新能源': [
            {'code': '002594', 'change_pct_20d': 28.5},
            {'code': '300750', 'change_pct_20d': 22.1},
        ],
        '银行': [
            {'code': '000001', 'change_pct_20d': 15.2},
        ],
        '房地产': [
            {'code': '000002', 'change_pct_20d': -5.3},
        ],
        '白酒': [
            {'code': '600519', 'change_pct_20d': 8.5},
        ],
    }
    
    results = screener.screen(test_stocks, test_sectors, max_results=5)
    
    print(screener.generate_report(results))
    
    print("\n" + "=" * 70)
