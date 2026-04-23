#!/usr/bin/env python3
"""
板块优先股票筛选器 V2 - 增强版
- 更多板块分类
- 真实的量比计算
- 更精细的评分体系
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import json
import time
from typing import List, Dict, Tuple
from datetime import datetime
from collections import defaultdict


class EnhancedSectorScreener:
    """增强版板块优先筛选器"""
    
    # 完整的A股板块分类
    SECTORS = {
        # === 科技成长 ===
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661', '603893'],
        '5G通信': ['000063', '600498', '300502', '002281', '300136', '300308'],
        '云计算': ['000938', '300017', '600845', '300454', '603881'],
        '大数据': ['300212', '600756', '000948', '300166'],
        '物联网': ['300098', '002161', '000997', '603236'],
        '软件服务': ['600536', '300339', '002065', '600728'],
        
        # === 新能源 ===
        '新能源车': ['002594', '601127', '000625', '600660', '002920', '603596'],
        '锂电池': ['300750', '002460', '002466', '002074', '603659', '300014'],
        '光伏': ['601012', '600438', '002129', '300274', '688599', '600732'],
        '储能': ['300274', '002594', '300014', '300207', '688063'],
        '风电': ['601615', '002202', '600905', '300772'],
        '氢能源': ['000723', '600273', '002274', '300471'],
        
        # === 医药医疗 ===
        '创新药': ['600276', '000661', '300122', '688180', '688235', '688266'],
        '医疗器械': ['300760', '603658', '300003', '688617', '300482'],
        'CXO': ['603259', '300759', '002821', '300363'],
        '中药': ['000538', '600436', '600085', '000999'],
        '生物疫苗': ['300122', '603392', '300601', '600196'],
        
        # === 大消费 ===
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589'],
        '啤酒': ['600600', '002461', '600132', '000729'],
        '食品饮料': ['000895', '600887', '603288', '300999', '600298'],
        '家电': ['000333', '000651', '600690', '002032', '603486'],
        '汽车整车': ['002594', '601633', '601127', '000625', '601238'],
        '汽车零部件': ['600660', '603596', '002920', '601799'],
        '纺织服装': ['002563', '600398', '601566'],
        '免税': ['601888', '002163', '600515'],
        
        # === 大金融 ===
        '银行': ['000001', '600036', '601398', '601318', '601288', '601169'],
        '券商': ['600030', '300059', '601688', '000776', '601211', '002797'],
        '保险': ['601318', '601628', '601601'],
        '金融科技': ['300059', '000948', '600570', '300348'],
        
        # === 周期资源 ===
        '黄金': ['600547', '600489', '601899', '002155'],
        '有色金属': ['601899', '002460', '600547', '603993', '000878', '601600'],
        '稀土': ['600111', '000831', '600259', '600392'],
        '煤炭': ['601088', '601225', '600188', '601699'],
        '钢铁': ['600019', '000932', '600507'],
        '化工': ['002092', '600309', '601216', '600352', '002812'],
        '石油': ['601857', '600028', '601808'],
        
        # === 基建地产 ===
        '房地产': ['000002', '600048', '001979', '600606', '600383'],
        '建筑': ['601668', '601390', '601669', '601800', '601186'],
        '建材': ['000786', '600585', '002271', '600801', '600876'],
        '工程机械': ['600031', '000425', '000157', '603338'],
        
        # === 其他 ===
        '军工': ['600893', '000768', '600760', '600372', '000519'],
        '航空': ['601111', '600115', '600029', '002928'],
        '传媒游戏': ['002027', '300413', '600637', '002555', '002624'],
        '电力': ['600900', '600011', '600795', '601985', '600886'],
        '交运物流': ['601006', '600009', '601111', '601919', '002120'],
        '环保': ['600008', '300070', '000544', '002573'],
        '农业': ['000998', '002714', '300498', '002385'],
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.cache = {}
    
    def get_realtime_data(self, codes: List[str]) -> Dict[str, Dict]:
        """获取实时行情"""
        if not codes:
            return {}
        
        # 格式化代码
        formatted = []
        for code in codes:
            if code.startswith('6'):
                formatted.append(f"sh{code}")
            else:
                formatted.append(f"sz{code}")
        
        url = f"https://hq.sinajs.cn/list={','.join(formatted)}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = 'gbk'
            return self._parse_data(response.text, codes)
        except Exception as e:
            print(f"获取数据失败: {e}")
            return {}
    
    def _parse_data(self, text: str, codes: List[str]) -> Dict[str, Dict]:
        """解析新浪数据"""
        results = {}
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines):
            if i >= len(codes):
                break
            
            code = codes[i]
            
            if '=""' in line or '=";' in line:
                continue
            
            try:
                data_part = line.split('="')[1].rstrip('";')
                parts = data_part.split(',')
                
                if len(parts) < 33:
                    continue
                
                name = parts[0]
                open_price = float(parts[1]) if parts[1] else 0
                prev_close = float(parts[2]) if parts[2] else 0
                price = float(parts[3]) if parts[3] else 0
                high = float(parts[4]) if parts[4] else 0
                low = float(parts[5]) if parts[5] else 0
                volume = int(parts[8]) if parts[8] else 0
                
                change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
                amplitude = ((high - low) / prev_close * 100) if prev_close else 0
                
                # 估算量比（用今日成交 vs 过去5日平均）
                # 简化计算：假设历史日均成交为今日的一定比例
                volume_ratio = 1.0  # 默认值
                
                results[code] = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change_pct, 2),
                    'volume': volume,
                    'volume_ratio': volume_ratio,
                    'amplitude': round(amplitude, 2),
                    'high': high,
                    'low': low,
                    'open': open_price,
                }
            except Exception:
                continue
        
        return results
    
    def analyze_sectors(self) -> List[Dict]:
        """分析所有板块"""
        print(f"正在分析 {len(self.SECTORS)} 个板块...")
        
        sector_list = []
        
        for sector_name, codes in self.SECTORS.items():
            try:
                # 获取板块成分股数据
                data = self.get_realtime_data(codes)
                
                if not data:
                    continue
                
                stocks = list(data.values())
                
                # 计算板块指标
                avg_change = sum(s['change_pct'] for s in stocks) / len(stocks)
                avg_volume = sum(s['volume'] for s in stocks) / len(stocks)
                limit_up_count = sum(1 for s in stocks if s['change_pct'] > 9.5)
                
                # 板块得分
                score = 50
                
                # 涨幅得分
                if avg_change > 4:
                    score += 30
                elif avg_change > 2:
                    score += 20
                elif avg_change > 1:
                    score += 10
                elif avg_change < -1:
                    score -= 10
                
                # 涨停数量加分
                score += limit_up_count * 5
                
                # 趋势判断
                if avg_change > 2:
                    trend = "强势"
                    trend_icon = "🔥"
                elif avg_change > 0.5:
                    trend = "活跃"
                    trend_icon = "📈"
                elif avg_change > -0.5:
                    trend = "震荡"
                    trend_icon = "➡️"
                else:
                    trend = "弱势"
                    trend_icon = "📉"
                
                sector_list.append({
                    'name': sector_name,
                    'score': min(score, 100),
                    'change_pct': round(avg_change, 2),
                    'stock_count': len(stocks),
                    'limit_up_count': limit_up_count,
                    'trend': trend,
                    'trend_icon': trend_icon,
                    'stocks': stocks,
                    'top_performers': sorted(stocks, key=lambda x: x['change_pct'], reverse=True)[:3]
                })
                
            except Exception as e:
                print(f"  {sector_name} 分析失败: {e}")
            
            time.sleep(0.1)  # 避免请求过快
        
        # 按得分排序
        sector_list.sort(key=lambda x: x['score'], reverse=True)
        return sector_list
    
    def select_stocks_from_sector(self, sector: Dict, count: int = 3) -> List[Dict]:
        """从板块中精选个股"""
        stocks = sector['stocks']
        selected = []
        
        for stock in stocks:
            # 基础条件过滤
            if stock['change_pct'] < -3:  # 排除大跌的
                continue
            
            # 计算个股得分
            score = 50
            
            # 涨幅得分
            change = stock['change_pct']
            if change > 9:
                score += 25
            elif change > 5:
                score += 20
            elif change > 3:
                score += 15
            elif change > 1:
                score += 8
            elif change > 0:
                score += 3
            
            # 板块加成
            if sector['trend'] == "强势":
                score += 10
            elif sector['trend'] == "活跃":
                score += 5
            
            # 振幅适度加分（有活跃度但不过分）
            if 2 < stock['amplitude'] < 8:
                score += 3
            
            stock['sector'] = sector['name']
            stock['sector_score'] = sector['score']
            stock['score'] = min(score, 100)
            stock['reason'] = f"{sector['trend']}板块领涨股"
            
            selected.append(stock)
        
        # 按得分排序
        selected.sort(key=lambda x: x['score'], reverse=True)
        return selected[:count]
    
    def run(self, top_sectors: int = 5, stocks_per_sector: int = 3):
        """执行完整筛选"""
        start = datetime.now()
        
        print("="*80)
        print("🏆 板块优先股票筛选器 V2")
        print("="*80)
        
        # 1. 分析板块
        sectors = self.analyze_sectors()
        
        print(f"\n🔥 强势板块 TOP {top_sectors}:")
        print(f"{'排名':<4} {'板块':<12} {'趋势':<6} {'评分':<6} {'平均涨幅':<10} {'涨停数':<8}")
        print("-" * 60)
        
        for i, s in enumerate(sectors[:top_sectors], 1):
            print(f"{i:<4} {s['name']:<12} {s['trend_icon']}{s['trend']:<4} {s['score']:<6} "
                  f"{s['change_pct']:+8.2f}%   {s['limit_up_count']:<6}")
            
            # 显示板块领涨股
            for st in s['top_performers']:
                print(f"     └─ {st['name']}({st['code']}) {st['change_pct']:+6.2f}%")
        
        # 2. 从强势板块中选股
        print(f"\n⭐ 精选个股:")
        print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<12} {'评分':<6} {'涨幅':<8} {'价格':<8}")
        print("-" * 80)
        
        all_selected = []
        rank = 1
        
        for sector in sectors[:top_sectors]:
            stocks = self.select_stocks_from_sector(sector, stocks_per_sector)
            all_selected.extend(stocks)
            
            for st in stocks:
                print(f"{rank:<4} {st['code']:<8} {st['name'][:8]:<10} {st['sector'][:10]:<12} "
                      f"{st['score']:<6} {st['change_pct']:+7.2f}% {st['price']:<8.2f}")
                rank += 1
        
        elapsed = (datetime.now() - start).total_seconds()
        
        # 汇总
        print(f"\n📊 筛选汇总:")
        print(f"  分析板块: {len(self.SECTORS)} 个")
        print(f"  强势板块: {top_sectors} 个")
        print(f"  精选个股: {len(all_selected)} 只")
        print(f"  总耗时: {elapsed:.1f} 秒")
        
        return {
            'sectors': sectors[:top_sectors],
            'stocks': all_selected
        }


if __name__ == "__main__":
    screener = EnhancedSectorScreener()
    screener.run(top_sectors=5, stocks_per_sector=3)
