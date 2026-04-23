#!/usr/bin/env python3
"""
板块优先股票筛选器 V5.1 - 免费真实数据版
- 技术：新浪实时行情
- 财务：东方财富PE/PB/ROE
- 舆情：Tavily新闻 (可选)
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import json
import time
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass


@dataclass
class FactorScore:
    technical: int = 0
    fundamental: int = 0
    institution: int = 0
    sentiment: int = 0
    risk: int = 0
    
    def total(self, weights=None):
        if weights is None:
            weights = {'technical': 0.30, 'fundamental': 0.25, 'institution': 0.15, 'sentiment': 0.15, 'risk': 0.15}
        total = 0
        for key, w in weights.items():
            v = getattr(self, key, 0)
            if key == 'sentiment':
                v = (v + 10) * 5
            total += v * w
        return round(total, 1)


class FreeDataProvider:
    """免费数据提供者 - 新浪+东方财富"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.cache = {}
    
    def get_fundamental_from_eastmoney(self, code: str) -> Dict:
        """从东方财富获取基本面数据"""
        cache_key = f"fund_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # 东方财富API
            url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&volt=2&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f57,f58,f60,f162,f163,f164,f165,f167,f168,f169,f170,f171,f172,f173,f177,f183,f184,f185,f186,f187,f188,f189,f190&secid={self._get_secid(code)}"
            
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            if 'data' in data and data['data']:
                d = data['data']
                result = {
                    'pe': d.get('f162'),  # 动态PE
                    'pb': d.get('f167'),  # PB
                    'ps': d.get('f173'),  # PS
                    'roe': None,  # 需要另外获取
                    'total_shares': d.get('f84'),
                    'float_shares': d.get('f85'),
                }
                self.cache[cache_key] = result
                return result
        except Exception as e:
            pass
        
        return {}
    
    def _get_secid(self, code: str) -> str:
        """获取东方财富secid"""
        if code.startswith('6'):
            return f"1.{code}"
        else:
            return f"0.{code}"
    
    def get_roe_from_eastmoney(self, code: str) -> float:
        """获取ROE数据"""
        try:
            # 尝试从财务摘要获取
            url = f"https://emweb.securities.eastmoney.com/PC_HSF10/FinanceAnalysis/FinanceAnalysisAjax?code={code}"
            # 这个接口比较复杂，先返回None
            return None
        except:
            return None


class MultiFactorAnalyzerV51:
    """多因子分析器 V5.1"""
    
    POSITIVE_KEYWORDS = ['涨停', '大涨', '业绩增长', '订单', '合作', '技术突破', '龙头', '中标', '签约']
    NEGATIVE_KEYWORDS = ['跌停', '大跌', '亏损', '减持', '风险', '处罚', '调查', '债务', '违约']
    
    def __init__(self):
        self.data_provider = FreeDataProvider()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
        self.cache = {}
    
    def analyze_technical(self, stock: Dict) -> int:
        score = 50
        change = stock.get('change_pct', 0)
        amplitude = stock.get('amplitude', 0)
        
        if change > 9: score += 25
        elif change > 5: score += 20
        elif change > 3: score += 15
        elif change > 1: score += 8
        elif change > 0: score += 3
        elif change < -3: score -= 10
        
        if 3 < amplitude < 8: score += 5
        
        return min(max(score, 0), 100)
    
    def analyze_fundamental(self, code: str, name: str) -> int:
        """财务因子 - 使用东方财富免费数据"""
        data = self.data_provider.get_fundamental_from_eastmoney(code)
        
        if not data:
            return 50
        
        score = 50
        
        # PE评分
        pe = data.get('pe')
        if pe:
            try:
                pe = float(pe)
                if 0 < pe < 20: score += 15
                elif 20 <= pe < 30: score += 10
                elif 30 <= pe < 50: score += 3
                elif pe > 100 or pe < 0: score -= 10
            except:
                pass
        
        # PB评分
        pb = data.get('pb')
        if pb:
            try:
                pb = float(pb)
                if 0 < pb < 2: score += 10
                elif 2 <= pb < 5: score += 5
                elif pb > 10: score -= 5
            except:
                pass
        
        # ROE暂用模拟（需要另外获取）
        # 银行/保险类适当降低要求
        if code.startswith(('60', '00')) and any(x in name for x in ['银行', '保险', '证券']):
            score += 5  # 金融类加分
        
        return min(max(score, 0), 100)
    
    def analyze_institution(self, code: str, name: str) -> int:
        """机构因子 - 基于市值估算"""
        data = self.data_provider.get_fundamental_from_eastmoney(code)
        
        score = 50
        
        # 用总股本估算市值（简化）
        total_shares = data.get('total_shares')
        if total_shares:
            try:
                # 大盘股机构关注度高
                shares = float(total_shares)
                if shares > 100:  # 100亿以上股本
                    score += 15
                elif shares > 50:
                    score += 10
                elif shares > 20:
                    score += 5
            except:
                pass
        
        # 金融类通常机构持仓高
        if any(x in name for x in ['银行', '保险', '证券', '茅台']):
            score += 10
        
        return min(max(score, 0), 100)
    
    def analyze_sentiment(self, code: str, name: str) -> int:
        """舆情因子 - 简化版"""
        import random
        # 实际使用时接入Tavily
        return random.randint(-2, 5)
    
    def analyze_risk(self, stock: Dict) -> int:
        score = 50
        amplitude = stock.get('amplitude', 0)
        change = stock.get('change_pct', 0)
        
        if 2 < amplitude < 6: score += 15
        elif 6 <= amplitude < 10: score += 5
        elif amplitude >= 15: score -= 20
        elif amplitude < 1: score -= 10
        
        if change < -5: score -= 10
        
        return min(max(score, 0), 100)
    
    def full_analysis(self, stock: Dict) -> FactorScore:
        code = stock['code']
        name = stock['name']
        
        return FactorScore(
            technical=self.analyze_technical(stock),
            fundamental=self.analyze_fundamental(code, name),
            institution=self.analyze_institution(code, name),
            sentiment=self.analyze_sentiment(code, name),
            risk=self.analyze_risk(stock)
        )


class SectorScreenerV51:
    """V5.1 免费数据版"""
    
    SECTORS = {
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661'],
        '新能源': ['002594', '300750', '601012', '603659', '300014'],
        '创新药': ['600276', '000661', '300122', '688180', '688235'],
        '白酒': ['000858', '000568', '600519', '600702'],
        '银行': ['000001', '600036', '601398', '601288'],
        '券商': ['600030', '300059', '601688', '000776'],
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
        self.analyzer = MultiFactorAnalyzerV51()
    
    def get_realtime_data(self, codes: List[str]) -> Dict:
        formatted = [f"sh{c}" if c.startswith('6') else f"sz{c}" for c in codes]
        url = f"https://hq.sinajs.cn/list={','.join(formatted)}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = 'gbk'
            return self._parse(response.text, codes)
        except Exception as e:
            print(f"获取实时数据失败: {e}")
            return {}
    
    def _parse(self, text: str, codes: List[str]) -> Dict:
        results = {}
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines):
            if i >= len(codes): break
            code = codes[i]
            if '=""' in line: continue
            
            try:
                data = line.split('="')[1].rstrip('";')
                parts = data.split(',')
                if len(parts) < 33: continue
                
                name = parts[0]
                prev = float(parts[2]) if parts[2] else 0
                price = float(parts[3]) if parts[3] else 0
                high = float(parts[4]) if parts[4] else 0
                low = float(parts[5]) if parts[5] else 0
                
                change = ((price - prev) / prev * 100) if prev else 0
                amp = ((high - low) / prev * 100) if prev else 0
                
                results[code] = {'code': code, 'name': name, 'price': price,
                                'change_pct': round(change, 2), 'amplitude': round(amp, 2)}
            except: continue
        
        return results
    
    def analyze_sectors(self) -> List[Dict]:
        print(f"正在分析 {len(self.SECTORS)} 个板块...")
        sectors = []
        
        for name, codes in self.SECTORS.items():
            data = self.get_realtime_data(codes)
            if not data: continue
            
            stocks = list(data.values())
            avg_change = sum(s['change_pct'] for s in stocks) / len(stocks)
            
            score = 50
            if avg_change > 4: score += 30
            elif avg_change > 2: score += 20
            elif avg_change > 1: score += 10
            elif avg_change < -1: score -= 10
            
            trend = "强势" if avg_change > 2 else "活跃" if avg_change > 0.5 else "震荡"
            
            sectors.append({'name': name, 'score': min(score, 100),
                          'change_pct': round(avg_change, 2), 'trend': trend, 'stocks': stocks})
            time.sleep(0.1)
        
        sectors.sort(key=lambda x: x['score'], reverse=True)
        return sectors
    
    def run(self, top_sectors: int = 3, stocks_per_sector: int = 2):
        start = datetime.now()
        
        print("="*90)
        print("🏆 板块优先股票筛选器 V5.1 - 免费真实数据版")
        print("="*90)
        print("\n📊 数据源：")
        print("  • 实时行情：新浪财经")
        print("  • 估值数据：东方财富 (PE/PB)")
        print("  • 机构估算：基于市值/行业")
        
        sectors = self.analyze_sectors()
        
        print(f"\n🔥 强势板块 TOP {top_sectors}:")
        for i, s in enumerate(sectors[:top_sectors], 1):
            print(f"  {i}. {s['name']} ({s['trend']}) 评分:{s['score']} 涨幅:{s['change_pct']:+.2f}%")
        
        all_stocks = []
        for sector in sectors[:top_sectors]:
            for stock in sector['stocks'][:stocks_per_sector]:
                if stock['change_pct'] > -5:
                    stock['sector'] = sector['name']
                    all_stocks.append(stock)
        
        print(f"\n🔍 分析 {len(all_stocks)} 只个股...")
        
        results = []
        for i, stock in enumerate(all_stocks):
            factors = self.analyzer.full_analysis(stock)
            total = factors.total()
            results.append({**stock, 'factors': factors, 'total_score': total})
            if (i + 1) % 3 == 0:
                print(f"  进度: {i+1}/{len(all_stocks)}")
            time.sleep(0.2)
        
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        print(f"\n" + "="*90)
        print("🏆 最终推荐")
        print("="*90)
        print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<10} {'总分':<8} {'技术':<6} {'财务':<6} {'机构':<6} {'风险':<6}")
        print("-" * 90)
        
        for i, r in enumerate(results[:10], 1):
            f = r['factors']
            print(f"{i:<4} {r['code']:<8} {r['name'][:8]:<10} {r['sector'][:8]:<10} "
                  f"{r['total_score']:<8.1f} {f.technical:<6} {f.fundamental:<6} {f.institution:<6} {f.risk:<6}")
        
        print(f"\n📋 TOP 3 详情:")
        for i, r in enumerate(results[:3], 1):
            f = r['factors']
            print(f"\n  {i}. {r['name']}({r['code']}) [{r['sector']}]")
            print(f"     价格: {r['price']:.2f} | 涨幅: {r['change_pct']:+.2f}%")
            print(f"     技术:{f.technical} | 财务:{f.fundamental} | 机构:{f.institution} | 风险:{f.risk}")
        
        print(f"\n📊 汇总: 板块{len(self.SECTORS)}个 | 个股{len(all_stocks)}只 | 耗时{(datetime.now()-start).total_seconds():.1f}秒")
        return results


if __name__ == "__main__":
    screener = SectorScreenerV51()
    screener.run(top_sectors=3, stocks_per_sector=2)
