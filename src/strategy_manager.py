#!/usr/bin/env python3
"""
策略管理器 V1.0
整合三种交易策略：
1. 突破策略 - 追涨强势股
2. 网格策略 - ETF震荡市赚钱
3. 双动量 - 趋势+板块双重验证
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

# 导入各策略模块
from breakout_screener import BreakoutScreener, BreakoutSignal
from grid_trader import ETFGridTrader, GridStrategy
from dual_momentum_screener import DualMomentumScreener, MomentumScore

logger = logging.getLogger(__name__)


@dataclass
class StrategyRecommendation:
    """策略推荐"""
    strategy_type: str  # breakout/grid/momentum
    code: str
    name: str
    action: str  # buy/sell/hold
    shares: int
    price: float
    stop_loss: float
    target_price: float
    confidence: int  # 0-100
    reason: str


class StrategyManager:
    """
    策略管理器
    
    统一管理三种策略的选股和交易信号
    """
    
    def __init__(self):
        self.breakout_screener = BreakoutScreener()
        self.grid_trader = ETFGridTrader()
        self.momentum_screener = DualMomentumScreener()
        
        logger.info("策略管理器初始化完成")
    
    def run_all_strategies(self,
                          all_stocks: List[Dict],
                          top_sectors: List[Dict],
                          sectors_data: Dict[str, List[Dict]],
                          etf_positions: List[Dict]) -> Dict[str, List]:
        """
        运行所有策略
        
        Args:
            all_stocks: 全市场股票数据
            top_sectors: 强势板块列表
            sectors_data: 板块详细数据
            etf_positions: 当前ETF持仓
            
        Returns:
            {
                'breakout': [BreakoutSignal],
                'grid_signals': [交易信号],
                'momentum': [MomentumScore],
                'recommendations': [StrategyRecommendation]
            }
        """
        results = {
            'breakout': [],
            'grid_signals': [],
            'momentum': [],
            'recommendations': []
        }
        
        logger.info("="*60)
        logger.info("运行所有策略")
        logger.info("="*60)
        
        # 1. 突破策略
        logger.info("\n【策略1】增强版突破策略")
        breakout_signals = self.breakout_screener.screen_breakout_stocks(
            all_stocks, top_sectors, max_results=5
        )
        results['breakout'] = breakout_signals
        
        # 2. 双动量策略
        logger.info("\n【策略2】双动量策略")
        momentum_scores = self.momentum_screener.screen(
            all_stocks, sectors_data, max_results=5
        )
        results['momentum'] = momentum_scores
        
        # 3. 网格策略
        logger.info("\n【策略3】ETF网格策略")
        grid_signals = []
        for etf in etf_positions:
            code = etf.get('code')
            current_price = etf.get('price', 0)
            signals = self.grid_trader.check_trading_signals(code, current_price)
            if signals:
                grid_signals.extend([{'code': code, **s} for s in signals])
        results['grid_signals'] = grid_signals
        
        # 生成统一推荐
        results['recommendations'] = self._generate_recommendations(
            breakout_signals, momentum_scores, grid_signals
        )
        
        return results
    
    def _generate_recommendations(self,
                                   breakout: List[BreakoutSignal],
                                   momentum: List[MomentumScore],
                                   grid_signals: List[Dict]) -> List[StrategyRecommendation]:
        """生成统一格式的推荐列表"""
        recommendations = []
        
        # 突破策略推荐
        for signal in breakout:
            rec = StrategyRecommendation(
                strategy_type='突破策略',
                code=signal.code,
                name=signal.name,
                action='买入',
                shares=1000,  # 默认仓位，实际应根据资金管理计算
                price=signal.current_price,
                stop_loss=signal.stop_loss,
                target_price=signal.target_price,
                confidence=min(95, signal.score),
                reason=f"突破20日高点，RS强度{signal.rs_rating}，板块排名{signal.sector_rank}"
            )
            recommendations.append(rec)
        
        # 双动量推荐
        for score in momentum:
            if score.signal == '买入':
                rec = StrategyRecommendation(
                    strategy_type='双动量',
                    code=score.code,
                    name=score.name,
                    action='买入',
                    shares=1000,
                    price=score.price,
                    stop_loss=round(score.price * 0.93, 3),
                    target_price=round(score.price * 1.15, 3),
                    confidence=min(95, int(score.composite_score)),
                    reason=f"个股排{score.stock_momentum_rank} 板块排{score.sector_momentum_rank} RS{score.rs_rating}"
                )
                recommendations.append(rec)
        
        # 网格策略推荐
        for signal in grid_signals:
            rec = StrategyRecommendation(
                strategy_type='网格交易',
                code=signal['code'],
                name=signal.get('name', signal['code']),
                action='买入' if signal['action'] == 'buy' else '卖出',
                shares=signal['shares'],
                price=signal['price'],
                stop_loss=0,
                target_price=0,
                confidence=80,
                reason=signal['reason']
            )
            recommendations.append(rec)
        
        # 按置信度排序
        recommendations.sort(key=lambda x: x.confidence, reverse=True)
        
        return recommendations
    
    def generate_strategy_report(self, results: Dict) -> str:
        """生成策略报告"""
        lines = [
            "\n" + "="*70,
            "综合策略报告",
            "="*70,
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            ""
        ]
        
        # 突破策略结果
        if results['breakout']:
            lines.extend([
                "【突破策略】选中股票:",
                "-"*70
            ])
            for s in results['breakout']:
                lines.append(
                    f"  {s.name}({s.code}) ¥{s.current_price} "
                    f"突破¥{s.breakout_price} 得分{s.score}"
                )
            lines.append("")
        
        # 双动量结果
        if results['momentum']:
            lines.extend([
                "【双动量策略】选中股票:",
                "-"*70
            ])
            for m in results['momentum']:
                lines.append(
                    f"  {m.name}({m.code}) +{m.change_pct_20d:.1f}% "
                    f"个排{m.stock_momentum_rank} 板排{m.sector_momentum_rank} "
                    f"得分{m.composite_score:.1f}"
                )
            lines.append("")
        
        # 网格策略结果
        if results['grid_signals']:
            lines.extend([
                "【网格策略】交易信号:",
                "-"*70
            ])
            for g in results['grid_signals']:
                action = "🔴 买入" if g['action'] == 'buy' else "🟢 卖出"
                lines.append(f"  {action} {g['code']} {g['shares']}股 @ ¥{g['price']:.3f}")
                lines.append(f"     {g['reason']}")
            lines.append("")
        
        # 综合推荐
        if results['recommendations']:
            lines.extend([
                "【今日重点推荐】",
                "-"*70,
                f"{'策略':<10}{'代码':<8}{'名称':<10}{'操作':<6}{'价格':<8}{'置信度':<8}",
                "-"*70
            ])
            for r in results['recommendations'][:10]:
                lines.append(
                    f"{r.strategy_type:<10}{r.code:<8}{r.name:<10}"
                    f"{r.action:<6}¥{r.price:<7.2f}{r.confidence}%"
                )
        
        lines.append("="*70)
        return "\n".join(lines)
    
    def init_grid_strategy(self, code: str, name: str, current_price: float,
                          lower_bound: float, upper_bound: float,
                          grid_count: int = 6, total_shares: int = 10000):
        """初始化ETF网格策略"""
        return self.grid_trader.create_strategy(
            code=code, name=name, current_price=current_price,
            lower_bound=lower_bound, upper_bound=upper_bound,
            grid_count=grid_count, total_shares=total_shares
        )


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 70)
    print("策略管理器测试")
    print("=" * 70)
    
    manager = StrategyManager()
    
    # 模拟数据
    test_stocks = [
        {'code': '000001', 'name': '平安银行', 'sector': '银行', 'price': 10.5, 'volume_ratio': 1.5, 'volume': 5000000},
        {'code': '002594', 'name': '比亚迪', 'sector': '新能源', 'price': 250.0, 'volume_ratio': 2.5, 'volume': 12000000},
        {'code': '300750', 'name': '宁德时代', 'sector': '新能源', 'price': 180.0, 'volume_ratio': 1.8, 'volume': 8000000},
    ]
    
    test_sectors = [
        {'name': '新能源', 'codes': ['002594', '300750']},
        {'name': '银行', 'codes': ['000001']},
    ]
    
    test_sectors_data = {
        '新能源': [
            {'code': '002594', 'change_pct_20d': 28.5},
            {'code': '300750', 'change_pct_20d': 22.1},
        ],
        '银行': [
            {'code': '000001', 'change_pct_20d': 8.5},
        ],
    }
    
    # 初始化网格策略
    manager.init_grid_strategy(
        code='159887', name='银行ETF',
        current_price=1.258, lower_bound=1.15, upper_bound=1.45,
        grid_count=6, total_shares=30900
    )
    
    etf_positions = [{'code': '159887', 'price': 1.20}]
    
    # 运行所有策略
    results = manager.run_all_strategies(
        test_stocks, test_sectors, test_sectors_data, etf_positions
    )
    
    # 打印报告
    print(manager.generate_strategy_report(results))
    
    print("\n" + "=" * 70)
