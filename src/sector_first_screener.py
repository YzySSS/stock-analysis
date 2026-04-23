#!/usr/bin/env python3
"""
板块优先股票筛选器 V2
策略：先筛选强势板块，再从板块中精选个股
"""
import logging
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class SectorInfo:
    """板块信息"""
    name: str
    code: str  # 板块代码
    change_pct: float  # 板块涨跌幅
    volume_ratio: float  # 板块量比
    leading_stocks: List[str] = field(default_factory=list)  # 成分股代码
    score: int = 0  # 板块综合评分
    trend: str = ""  # 趋势: 强势/震荡/弱势


@dataclass
class StockInSector:
    """板块内的股票"""
    code: str
    name: str
    sector: str
    price: float
    change_pct: float
    volume_ratio: float
    market_cap: float  # 市值(亿)
    pe_ttm: float = None
    score: int = 0
    reason: str = ""


class SectorFirstScreener:
    """
    板块优先筛选器
    
    筛选流程：
    1. 获取所有板块数据，计算板块得分
    2. 筛选TOP N强势板块
    3. 在每个强势板块内精选个股
    4. 汇总结果
    """
    
    # A股主要板块分类（基于同花顺/东方财富板块）
    SECTOR_MAP = {
        # 科技成长
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661'],
        '新能源': ['002594', '300750', '601012', '603659', '300014', '600438'],
        '光伏': ['601012', '600438', '002129', '300274', '688599'],
        '储能': ['300274', '002594', '300014', '300207', '688063'],
        '5G通信': ['000063', '600498', '300502', '002281', '300136'],
        '云计算': ['000938', '300017', '600845', '300454', '603881'],
        
        # 大消费
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589'],
        '医药': ['600276', '000538', '300760', '603259', '600436', '300122'],
        '医疗器械': ['300760', '603658', '300003', '688617'],
        '创新药': ['600276', '000661', '300122', '688180', '688235'],
        '食品饮料': ['000895', '600887', '603288', '600519', '000568'],
        '家电': ['000333', '000651', '600690', '002032'],
        '汽车': ['002594', '601633', '601127', '000625', '600660'],
        
        # 大金融
        '银行': ['000001', '600036', '601398', '601318', '601288'],
        '券商': ['600030', '300059', '601688', '000776', '601211'],
        '保险': ['601318', '601628', '601601'],
        
        # 周期资源
        '有色金属': ['601899', '002460', '600547', '603993', '000878'],
        '煤炭': ['601088', '601225', '600188', '601699'],
        '钢铁': ['600019', '000932', '600507'],
        '化工': ['002092', '600309', '601216', '600352'],
        '石油': ['601857', '600028', '601808'],
        
        # 基建地产
        '房地产': ['000002', '600048', '001979', '600606'],
        '建筑': ['601668', '601390', '601669', '601800'],
        '建材': ['000786', '600585', '002271', '600801'],
        
        # 其他
        '军工': ['600893', '000768', '600760', '600372'],
        '传媒': ['002027', '300413', '600637', '002555'],
        '电力': ['600900', '600011', '600795', '601985'],
        '交运': ['601006', '600009', '601111', '601919'],
    }
    
    def __init__(self, data_source=None):
        self.data_source = data_source
        self.sector_scores = {}  # 板块得分缓存
    
    def screen_sectors(self, top_n: int = 5) -> List[SectorInfo]:
        """
        第一步：筛选强势板块
        
        Returns:
            TOP N强势板块列表
        """
        logger.info(f"开始筛选强势板块 (TOP {top_n})...")
        
        sector_list = []
        
        for sector_name, stock_codes in self.SECTOR_MAP.items():
            try:
                # 计算板块指标
                sector = self._calculate_sector_metrics(sector_name, stock_codes)
                if sector:
                    sector_list.append(sector)
            except Exception as e:
                logger.warning(f"板块 {sector_name} 计算失败: {e}")
        
        # 按得分排序
        sector_list.sort(key=lambda x: x.score, reverse=True)
        
        logger.info(f"板块筛选完成，强势板块: {[s.name for s in sector_list[:top_n]]}")
        return sector_list[:top_n]
    
    def _calculate_sector_metrics(self, name: str, codes: List[str]) -> SectorInfo:
        """计算板块指标"""
        # 获取成分股数据
        stocks_data = self._get_stocks_data(codes)
        
        if not stocks_data:
            return None
        
        # 计算板块平均涨跌幅
        avg_change = sum(s['change_pct'] for s in stocks_data) / len(stocks_data)
        
        # 计算板块量比
        avg_volume_ratio = sum(s.get('volume_ratio', 1) for s in stocks_data) / len(stocks_data)
        
        # 计算板块得分
        score = 50  # 基础分
        
        # 涨跌幅得分
        if avg_change > 5:
            score += 25
        elif avg_change > 3:
            score += 20
        elif avg_change > 1:
            score += 10
        elif avg_change < -2:
            score -= 15
        
        # 量比得分（资金关注度）
        if avg_volume_ratio > 2:
            score += 15
        elif avg_volume_ratio > 1.5:
            score += 10
        elif avg_volume_ratio > 1:
            score += 5
        
        # 涨停数量加分
        limit_up_count = sum(1 for s in stocks_data if s['change_pct'] > 9.5)
        score += limit_up_count * 3
        
        # 确定趋势
        if avg_change > 2 and avg_volume_ratio > 1.5:
            trend = "强势"
        elif avg_change > 0 and avg_volume_ratio > 1:
            trend = "活跃"
        elif avg_change < -1:
            trend = "弱势"
        else:
            trend = "震荡"
        
        return SectorInfo(
            name=name,
            code=self._get_sector_code(name),
            change_pct=round(avg_change, 2),
            volume_ratio=round(avg_volume_ratio, 2),
            leading_stocks=codes,
            score=min(score, 100),
            trend=trend
        )
    
    def _get_stocks_data(self, codes: List[str]) -> List[Dict]:
        """获取股票数据（简化版，实际需要接入数据源）"""
        # TODO: 接入实际数据源
        # 这里先用模拟数据演示逻辑
        import random
        results = []
        for code in codes[:5]:  # 每个板块取前5只
            results.append({
                'code': code,
                'change_pct': random.uniform(-3, 8),
                'volume_ratio': random.uniform(0.5, 3),
            })
        return results
    
    def _get_sector_code(self, name: str) -> str:
        """获取板块代码"""
        # 简化的板块代码映射
        code_map = {
            '人工智能': '885726', '芯片半导体': '885908', '新能源': '885800',
            '白酒': '885525', '医药': '885664', '银行': '885406',
            '券商': '885450', '有色金属': '885431', '军工': '885600',
        }
        return code_map.get(name, '000000')
    
    def screen_stocks_in_sectors(self, 
                                  sectors: List[SectorInfo],
                                  stocks_per_sector: int = 3) -> List[StockInSector]:
        """
        第二步：在强势板块中筛选个股
        
        Args:
            sectors: 强势板块列表
            stocks_per_sector: 每个板块选几只
        
        Returns:
            精选股票列表
        """
        logger.info(f"开始在 {len(sectors)} 个板块中精选个股...")
        
        all_selected = []
        
        for sector in sectors:
            try:
                stocks = self._select_stocks_in_sector(sector, stocks_per_sector)
                all_selected.extend(stocks)
                logger.info(f"  [{sector.name}] 选中 {len(stocks)} 只，板块涨幅 {sector.change_pct:+.2f}%")
            except Exception as e:
                logger.warning(f"板块 {sector.name} 选股失败: {e}")
        
        # 按得分排序
        all_selected.sort(key=lambda x: x.score, reverse=True)
        
        return all_selected
    
    def _select_stocks_in_sector(self, sector: SectorInfo, count: int) -> List[StockInSector]:
        """在单个板块中选股"""
        # 获取成分股详细数据
        stocks_data = self._get_stocks_detailed(sector.leading_stocks)
        
        results = []
        for data in stocks_data:
            # 计算个股得分
            score = self._calculate_stock_score(data, sector)
            
            # 选股条件
            if data['change_pct'] > -2 and data.get('volume_ratio', 1) > 0.8:
                results.append(StockInSector(
                    code=data['code'],
                    name=data.get('name', data['code']),
                    sector=sector.name,
                    price=data.get('price', 0),
                    change_pct=data['change_pct'],
                    volume_ratio=data.get('volume_ratio', 1),
                    market_cap=data.get('market_cap', 0),
                    pe_ttm=data.get('pe_ttm'),
                    score=score,
                    reason=f"{sector.name}板块{trend_reason(sector.trend)}，个股评分{score}"
                ))
        
        # 按得分排序取TOP
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:count]
    
    def _get_stocks_detailed(self, codes: List[str]) -> List[Dict]:
        """获取股票详细数据（需接入实际数据源）"""
        # TODO: 接入实际数据源
        import random
        results = []
        for code in codes:
            change = random.uniform(-2, 10)
            results.append({
                'code': code,
                'name': f'股票{code}',
                'price': random.uniform(10, 100),
                'change_pct': change,
                'volume_ratio': random.uniform(0.8, 3),
                'market_cap': random.uniform(50, 5000),
                'pe_ttm': random.uniform(10, 50) if random.random() > 0.3 else None,
            })
        return results
    
    def _calculate_stock_score(self, data: Dict, sector: SectorInfo) -> int:
        """计算个股得分"""
        score = 50
        
        # 涨幅得分
        change = data['change_pct']
        if change > 7:
            score += 20
        elif change > 4:
            score += 15
        elif change > 2:
            score += 10
        elif change > 0:
            score += 5
        elif change < -3:
            score -= 10
        
        # 量比得分
        vr = data.get('volume_ratio', 1)
        if vr > 2:
            score += 10
        elif vr > 1.5:
            score += 5
        
        # 板块加成
        if sector.trend == "强势":
            score += 10
        elif sector.trend == "活跃":
            score += 5
        
        # 市值偏好（中盘股）
        cap = data.get('market_cap', 0)
        if 100 < cap < 1000:
            score += 5
        
        return min(score, 100)
    
    def run_screening(self, 
                      top_sectors: int = 5,
                      stocks_per_sector: int = 3) -> Dict:
        """
        执行完整筛选流程
        
        Returns:
            {
                'sectors': 强势板块列表,
                'stocks': 精选股票列表,
                'summary': 汇总信息
            }
        """
        logger.info("="*60)
        logger.info("启动板块优先筛选策略")
        logger.info("="*60)
        
        start_time = datetime.now()
        
        # Step 1: 筛选强势板块
        strong_sectors = self.screen_sectors(top_n=top_sectors)
        
        # Step 2: 板块内选股
        selected_stocks = self.screen_stocks_in_sectors(
            strong_sectors, 
            stocks_per_sector=stocks_per_sector
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        summary = {
            'total_sectors_analyzed': len(self.SECTOR_MAP),
            'strong_sectors_count': len(strong_sectors),
            'selected_stocks_count': len(selected_stocks),
            'time_elapsed': f"{elapsed:.2f}s"
        }
        
        return {
            'sectors': strong_sectors,
            'stocks': selected_stocks,
            'summary': summary
        }


def trend_reason(trend: str) -> str:
    """趋势描述"""
    return {
        '强势': '强势上涨',
        '活跃': '资金活跃',
        '震荡': '震荡整理',
        '弱势': '暂时弱势'
    }.get(trend, '走势平稳')


# 便捷函数
def quick_screen(top_sectors: int = 5, stocks_per_sector: int = 3) -> List[StockInSector]:
    """快速筛选接口"""
    screener = SectorFirstScreener()
    result = screener.run_screening(top_sectors, stocks_per_sector)
    return result['stocks']


if __name__ == "__main__":
    print("🧪 板块优先筛选器测试")
    print("="*70)
    
    screener = SectorFirstScreener()
    result = screener.run_screening(top_sectors=5, stocks_per_sector=3)
    
    print(f"\n📊 汇总:")
    print(f"  分析板块数: {result['summary']['total_sectors_analyzed']}")
    print(f"  强势板块: {result['summary']['strong_sectors_count']}")
    print(f"  选中股票: {result['summary']['selected_stocks_count']}")
    print(f"  耗时: {result['summary']['time_elapsed']}")
    
    print(f"\n🏆 强势板块:")
    for i, sector in enumerate(result['sectors'], 1):
        print(f"  {i}. {sector.name} (评分:{sector.score}, 涨幅:{sector.change_pct:+.2f}%, 趋势:{sector.trend})")
    
    print(f"\n⭐ 精选股票:")
    for i, stock in enumerate(result['stocks'][:10], 1):
        print(f"  {i}. {stock.name}({stock.code}) [{stock.sector}] 评分:{stock.score} 涨幅:{stock.change_pct:+.2f}%")
