#!/usr/bin/env python3
"""
板块优先股票筛选器 - 新浪数据版
实际接入数据源进行筛选
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
from typing import List, Dict
from datetime import datetime

from src.sector_first_screener import SectorFirstScreener, SectorInfo, StockInSector


class SinaDataProvider:
    """新浪数据提供者"""
    
    BASE_URL = "https://hq.sinajs.cn/list="
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def get_stocks_realtime(self, codes: List[str]) -> List[Dict]:
        """获取多只股票实时数据"""
        if not codes:
            return []
        
        # 代码格式转换
        formatted = []
        for code in codes:
            if code.startswith('6'):
                formatted.append(f"sh{code}")
            else:
                formatted.append(f"sz{code}")
        
        url = self.BASE_URL + ','.join(formatted)
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = 'gbk'
            return self._parse_sina_data(response.text, codes)
        except Exception as e:
            print(f"获取数据失败: {e}")
            return []
    
    def _parse_sina_data(self, text: str, original_codes: List[str]) -> List[Dict]:
        """解析新浪返回数据"""
        results = []
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines):
            if i >= len(original_codes):
                break
            
            code = original_codes[i]
            
            # 空数据检查
            if '=""' in line or '=";' in line:
                continue
            
            try:
                # 格式: var hq_str_sh600000="浦发银行,7.05,7.00,7.08,7.10,7.02,7.07,7.08,1234567,8765432..."
                data_part = line.split('="')[1].rstrip('";')
                parts = data_part.split(',')
                
                if len(parts) < 33:
                    continue
                
                name = parts[0]
                price = float(parts[3]) if parts[3] else 0  # 当前价
                prev_close = float(parts[2]) if parts[2] else 0  # 昨收
                change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
                volume = int(parts[8]) if parts[8] else 0  # 成交量
                
                # 计算量比（简化：用今日成交量 vs 5日均量，这里用固定值模拟）
                # 实际应该用历史数据计算
                volume_ratio = 1.0
                
                results.append({
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change_pct, 2),
                    'volume': volume,
                    'volume_ratio': volume_ratio,
                    'prev_close': prev_close,
                })
            except Exception as e:
                continue
        
        return results


class RealSectorScreener(SectorFirstScreener):
    """接入真实数据的板块优先筛选器"""
    
    def __init__(self):
        super().__init__()
        self.data_provider = SinaDataProvider()
        self._cache = {}  # 数据缓存
    
    def _get_stocks_data(self, codes: List[str]) -> List[Dict]:
        """从新浪获取板块成分股数据"""
        # 检查缓存
        cache_key = ','.join(sorted(codes))
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 分批获取（新浪限制每次最多800只）
        all_data = []
        batch_size = 200
        
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i+batch_size]
            data = self.data_provider.get_stocks_realtime(batch)
            all_data.extend(data)
            
            if i + batch_size < len(codes):
                time.sleep(0.2)  # 避免请求过快
        
        self._cache[cache_key] = all_data
        return all_data
    
    def _get_stocks_detailed(self, codes: List[str]) -> List[Dict]:
        """获取详细数据（复用同上）"""
        return self._get_stocks_data(codes)


def main():
    print("="*70)
    print("🏆 板块优先股票筛选器 - 新浪实时数据版")
    print("="*70)
    
    screener = RealSectorScreener()
    
    # 执行筛选
    result = screener.run_screening(
        top_sectors=5,      # TOP 5强势板块
        stocks_per_sector=3  # 每板块选3只
    )
    
    # 输出结果
    print(f"\n📊 筛选汇总:")
    print(f"  分析板块: {result['summary']['total_sectors_analyzed']} 个")
    print(f"  强势板块: {result['summary']['strong_sectors_count']} 个")
    print(f"  选中个股: {result['summary']['selected_stocks_count']} 只")
    print(f"  筛选耗时: {result['summary']['time_elapsed']}")
    
    print(f"\n🔥 强势板块排名:")
    for i, sector in enumerate(result['sectors'], 1):
        trend_emoji = {"强势": "🚀", "活跃": "📈", "震荡": "➡️", "弱势": "📉"}.get(sector.trend, "➖")
        print(f"  {i}. {sector.name:12s} {trend_emoji} 评分:{sector.score:3d} | 涨幅:{sector.change_pct:+6.2f}% | 量比:{sector.volume_ratio:.2f}")
    
    print(f"\n⭐ 精选个股 (按评分排序):")
    print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<12} {'评分':<6} {'涨幅':<8} {'价格':<8} {'入选理由'}")
    print("-" * 90)
    
    for i, stock in enumerate(result['stocks'][:15], 1):
        print(f"{i:<4} {stock.code:<8} {stock.name[:8]:<10} {stock.sector[:10]:<12} "
              f"{stock.score:<6} {stock.change_pct:+7.2f}% {stock.price:<8.2f} {stock.reason[:30]}")
    
    print(f"\n💡 使用说明:")
    print(f"  1. 筛选逻辑: 先选强势板块 → 再从中精选个股")
    print(f"  2. 板块评分: 基于板块平均涨幅 + 资金关注度 + 涨停数量")
    print(f"  3. 个股评分: 基于涨幅 + 量比 + 板块加成 + 市值")
    print(f"  4. 数据时效: 实时行情，每分钟可重新筛选")


if __name__ == "__main__":
    main()
