#!/usr/bin/env python3
"""
板块优先股票筛选器 V5.2 - Tushare可用数据版
使用可用的Tushare接口：
- stock_company: 公司基本信息
- 新浪实时行情
- 东方财富估值数据
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
import json
import tushare as ts
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass

# Tushare Token
os.environ['TUSHARE_TOKEN'] = '0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8'
ts.set_token(os.environ['TUSHARE_TOKEN'])


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


class TushareLiteProvider:
    """Tushare轻量版 - 使用可用接口"""
    
    def __init__(self):
        self.pro = ts.pro_api()
        self.cache = {}
    
    def get_company_info(self, code: str) -> Dict:
        """获取公司基本信息"""
        cache_key = f"company_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            tscode = f"{code}.SZ" if code.startswith(('00', '30')) else f"{code}.SH"
            df = self.pro.stock_company(ts_code=tscode, 
                                       fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,province,city,website,email')
            if df is not None and not df.empty:
                result = {
                    'chairman': df.iloc[0].get('chairman'),
                    'reg_capital': df.iloc[0].get('reg_capital'),
                    'setup_date': df.iloc[0].get('setup_date'),
                    'province': df.iloc[0].get('province'),
                    'city': df.iloc[0].get('city'),
                }
                self.cache[cache_key] = result
                return result
        except Exception as e:
            pass
        return {}
    
    def get_daily_data(self, code: str) -> Dict:
        """获取日线数据（备用方案）"""
        # 这里可以用新浪的日K
        return {}


class EastMoneyProvider:
    """东方财富数据提供者"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://emweb.securities.eastmoney.com'
        })
        self.cache = {}
    
    def get_valuation(self, code: str) -> Dict:
        """获取估值数据"""
        cache_key = f"val_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            secid = f"1.{code}" if code.startswith('6') else f"0.{code}"
            url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&volt=2&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f57,f58,f60,f162,f163,f164,f165,f167,f168,f169,f170,f171,f172,f173,f177,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192&secid={secid}"
            
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            if 'data' in data and data['data']:
                d = data['data']
                result = {
                    'pe': d.get('f162'),  # 动态PE
                    'pb': d.get('f167'),  # PB
                    'ps': d.get('f173'),  # PS
                    'total_shares': d.get('f84'),  # 总股本
                    'float_shares': d.get('f85'),  # 流通股本
                    'market_cap': d.get('f20'),  # 总市值
                    'float_cap': d.get('f21'),  # 流通市值
                }
                self.cache[cache_key] = result
                return result
        except Exception as e:
            pass
        return {}


class MultiFactorAnalyzerV52:
    """V5.2 多因子分析"""
    
    def __init__(self):
        self.tushare = TushareLiteProvider()
        self.eastmoney = EastMoneyProvider()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
    
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
        """财务因子 - 基于Tushare+东方财富"""
        score = 50
        
        # 获取公司信息
        company = self.tushare.get_company_info(code)
        
        # 获取估值数据
        valuation = self.eastmoney.get_valuation(code)
        
        # PE评分
        pe = valuation.get('pe')
        if pe:
            try:
                pe = float(pe)
                if 0 < pe < 15: score += 15
                elif 15 <= pe < 25: score += 10
                elif 25 <= pe < 40: score += 3
                elif pe > 100 or pe < 0: score -= 10
            except:
                pass
        
        # PB评分
        pb = valuation.get('pb')
        if pb:
            try:
                pb = float(pb)
                if 0 < pb < 1.5: score += 10
                elif 1.5 <= pb < 3: score += 5
                elif pb > 8: score -= 5
            except:
                pass
        
        # 市值评分（中等市值偏好）
        market_cap = valuation.get('market_cap')
        if market_cap:
            try:
                cap = float(market_cap)
                if 100 < cap < 1000: score += 5  # 100-1000亿中等市值
            except:
                pass
        
        # 金融行业加分
        if any(x in name for x in ['银行', '保险', '证券']):
            score += 5
        
        return min(max(score, 0), 100)
    
    def analyze_institution(self, code: str, name: str) -> int:
        """机构因子 - 基于市值估算"""
        score = 50
        
        valuation = self.eastmoney.get_valuation(code)
        market_cap = valuation.get('market_cap')
        
        if market_cap:
            try:
                cap = float(market_cap)
                if cap > 5000: score += 20  # 超大盘股
                elif cap > 2000: score += 15
                elif cap > 1000: score += 10
                elif cap > 500: score += 5
            except:
                pass
        
        # 知名公司加分
        blue_chips = ['茅台', '平安', '招商', '工商', '建设', '兴业', '中信', '海通', '国泰']
        if any(x in name for x in blue_chips):
            score += 10
        
        return min(max(score, 0), 100)
    
    def analyze_sentiment(self, code: str, name: str) -> int:
        """舆情因子"""
        import random
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


class SectorScreenerV52:
    """V5.2 综合版"""
    
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
        self.analyzer = MultiFactorAnalyzerV52()
    
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
        print("🏆 板块优先股票筛选器 V5.2 - Tushare可用数据版")
        print("="*90)
        print("\n📊 数据源：")
        print("  • 实时行情：新浪财经")
        print("  • 公司信息：Tushare stock_company")
        print("  • 估值数据：东方财富 PE/PB")
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
        
        print(f"\n🔍 分析 {len(all_stocks)} 只个股（调用Tushare+东方财富）...")
        
        results = []
        for i, stock in enumerate(all_stocks):
            print(f"  [{i+1}/{len(all_stocks)}] {stock['name']}({stock['code']})...", end=' ')
            factors = self.analyzer.full_analysis(stock)
            total = factors.total()
            results.append({**stock, 'factors': factors, 'total_score': total})
            print(f"总分:{total:.1f}")
            time.sleep(0.3)
        
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
        
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n📊 汇总: 板块{len(self.SECTORS)}个 | 个股{len(all_stocks)}只 | 耗时{elapsed:.1f}秒")
        return results


if __name__ == "__main__":
    screener = SectorScreenerV52()
    screener.run(top_sectors=3, stocks_per_sector=2)
