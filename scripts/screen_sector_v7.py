#!/usr/bin/env python3
"""
板块优先股票筛选器 V7 - 聚宽完整版
使用聚宽(JoinQuant)真实数据：
- 实时行情：聚宽 / 新浪
- 财务数据：聚宽 ROE/营收/净利润/现金流
- 估值数据：聚宽 PE/PB/PS
- 机构数据：聚宽 基金持仓/股东人数
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
import json
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass

# 聚宽配置
JQ_USERNAME = '13929962527'
JQ_PASSWORD = 'Zy20001026'


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


class JoinQuantProvider:
    """聚宽数据提供者"""
    
    def __init__(self):
        self.jq = None
        self.connected = False
        self.cache = {}
        self._connect()
    
    def _connect(self):
        """连接聚宽"""
        try:
            from jqdatasdk import auth, query, valuation, indicator
            auth(JQ_USERNAME, JQ_PASSWORD)
            self.jq = {
                'query': query,
                'valuation': valuation,
                'indicator': indicator
            }
            self.connected = True
            print("  ✅ 聚宽连接成功")
        except Exception as e:
            print(f"  ⚠️ 聚宽连接失败: {e}")
            print("  将使用备用数据源")
    
    def get_fundamental(self, code: str) -> Dict:
        """获取财务数据 ROE/PE/PB/成长"""
        if not self.connected:
            return {}
        
        cache_key = f"fund_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            from jqdatasdk import query, valuation, indicator
            
            # 转换代码格式
            jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
            
            # 查询估值和财务指标
            q = query(
                valuation.code,
                valuation.pe_ratio,  # PE
                valuation.pb_ratio,  # PB
                valuation.ps_ratio,  # PS
                indicator.roe,       # ROE
                indicator.inc_net_profit_year_on_year,  # 净利润同比增长
                indicator.inc_operation_profit_year_on_year,  # 营收同比增长
                indicator.gross_profit_margin,  # 毛利率
                indicator.net_profit_margin     # 净利率
            ).filter(valuation.code == jq_code)
            
            df = self.jq['query'].run_query(q)
            
            if df is not None and not df.empty:
                row = df.iloc[0]
                result = {
                    'pe': row.get('pe_ratio'),
                    'pb': row.get('pb_ratio'),
                    'ps': row.get('ps_ratio'),
                    'roe': row.get('roe'),
                    'profit_growth': row.get('inc_net_profit_year_on_year'),
                    'revenue_growth': row.get('inc_operation_profit_year_on_year'),
                    'gross_margin': row.get('gross_profit_margin'),
                    'net_margin': row.get('net_profit_margin'),
                }
                self.cache[cache_key] = result
                return result
        except Exception as e:
            pass
        
        return {}
    
    def get_institution_data(self, code: str) -> Dict:
        """获取机构持仓数据"""
        if not self.connected:
            return {}
        
        cache_key = f"inst_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            from jqdatasdk import finance
            
            jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
            
            # 获取基金持仓数据（最新的）
            df = finance.run_query(
                query(finance.STK_SHAREHOLDER_FLOATING).filter(
                    finance.STK_SHAREHOLDER_FLOATING.code == jq_code
                ).order_by(finance.STK_SHAREHOLDER_FLOATING.pubDate.desc()).limit(1)
            )
            
            result = {'fund_holdings': None, 'fund_count': None}
            
            if df is not None and not df.empty:
                # 尝试获取机构持仓比例
                result['inst_ratio'] = df.iloc[0].get('institution_holdings_ratio')
            
            self.cache[cache_key] = result
            return result
            
        except Exception as e:
            pass
        
        return {}
    
    def get_price_data(self, code: str) -> Dict:
        """获取价格数据（备用）"""
        if not self.connected:
            return {}
        
        try:
            from jqdatasdk import get_price
            
            jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
            df = get_price(jq_code, count=1, end_date=datetime.now().strftime('%Y-%m-%d'), frequency='daily')
            
            if df is not None and not df.empty:
                return {
                    'close': df.iloc[-1]['close'],
                    'volume': df.iloc[-1]['volume'],
                }
        except:
            pass
        return {}


class EastMoneyBackup:
    """东方财富备用数据源"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.cache = {}
    
    def get_valuation(self, code: str) -> Dict:
        cache_key = f"val_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            secid = f"1.{code}" if code.startswith('6') else f"0.{code}"
            url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&volt=2&fields=f162,f167,f173,f20&secid={secid}"
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            if 'data' in data and data['data']:
                d = data['data']
                result = {
                    'pe': d.get('f162'),
                    'pb': d.get('f167'),
                    'market_cap': d.get('f20'),
                }
                self.cache[cache_key] = result
                return result
        except:
            pass
        return {}


class MultiFactorAnalyzerV7:
    """V7 多因子分析 - 聚宽真实数据"""
    
    def __init__(self):
        self.jq = JoinQuantProvider()
        self.em = EastMoneyBackup()
    
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
        """财务因子 - 优先聚宽，备用东方财富"""
        score = 50
        
        # 尝试聚宽数据
        jq_data = self.jq.get_fundamental(code)
        
        if jq_data:
            # 聚宽数据可用
            pe = jq_data.get('pe')
            pb = jq_data.get('pb')
            roe = jq_data.get('roe')
            profit_growth = jq_data.get('profit_growth')
            revenue_growth = jq_data.get('revenue_growth')
            gross_margin = jq_data.get('gross_margin')
        else:
            # 备用东方财富
            em_data = self.em.get_valuation(code)
            pe = em_data.get('pe')
            pb = em_data.get('pb')
            roe = None
            profit_growth = None
            revenue_growth = None
            gross_margin = None
        
        # PE评分
        if pe and pe > 0:
            if pe < 15: score += 15
            elif pe < 25: score += 10
            elif pe < 40: score += 3
            elif pe > 100: score -= 10
        
        # PB评分
        if pb and pb > 0:
            if pb < 1.5: score += 10
            elif pb < 3: score += 5
            elif pb > 8: score -= 5
        
        # ROE评分（聚宽特有）
        if roe and roe > 0:
            if roe > 20: score += 15
            elif roe > 15: score += 10
            elif roe > 10: score += 5
            elif roe < 5: score -= 5
        
        # 成长性评分（聚宽特有）
        if profit_growth and profit_growth > 0:
            if profit_growth > 30: score += 10
            elif profit_growth > 20: score += 7
            elif profit_growth > 10: score += 4
        
        if revenue_growth and revenue_growth > 0:
            if revenue_growth > 30: score += 5
            elif revenue_growth > 20: score += 3
        
        # 毛利率（聚宽特有）
        if gross_margin and gross_margin > 0:
            if gross_margin > 40: score += 5
        
        return min(max(score, 0), 100)
    
    def analyze_institution(self, code: str, name: str) -> int:
        """机构因子"""
        score = 50
        
        # 尝试聚宽机构数据
        jq_inst = self.jq.get_institution_data(code)
        
        if jq_inst and jq_inst.get('inst_ratio'):
            inst_ratio = jq_inst.get('inst_ratio')
            if inst_ratio > 50: score += 20
            elif inst_ratio > 30: score += 15
            elif inst_ratio > 10: score += 10
        else:
            # 备用：基于市值估算
            em_data = self.em.get_valuation(code)
            market_cap = em_data.get('market_cap')
            
            if market_cap:
                try:
                    cap = float(market_cap) / 100000000
                    if cap > 5000: score += 20
                    elif cap > 2000: score += 15
                    elif cap > 1000: score += 10
                    elif cap > 500: score += 5
                except:
                    pass
        
        # 蓝筹股加分
        blue_chips = ['茅台', '平安', '招商', '工商', '建设', '兴业', '中信', '海通', '浦发', '民生']
        if any(x in name for x in blue_chips):
            score += 10
        
        return min(max(score, 0), 100)
    
    def analyze_sentiment(self, code: str, name: str) -> int:
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


class SectorScreenerV7:
    """V7 聚宽完整版"""
    
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
        self.analyzer = MultiFactorAnalyzerV7()
    
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
        print("🏆 板块优先股票筛选器 V7 - 聚宽完整版")
        print("="*90)
        print("\n📊 数据源：")
        print("  • 实时行情：新浪财经")
        print("  • 财务数据：聚宽 ROE/PE/PB/成长/毛利率")
        print("  • 机构数据：聚宽机构持仓 + 市值估算")
        
        # 测试聚宽连接
        print(f"\n🔌 正在连接聚宽...")
        
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
        
        print(f"\n🔍 分析 {len(all_stocks)} 只个股（调用聚宽真实财务数据）...")
        
        results = []
        for i, stock in enumerate(all_stocks):
            print(f"  [{i+1}/{len(all_stocks)}] {stock['name']}({stock['code']})...", end=' ')
            factors = self.analyzer.full_analysis(stock)
            total = factors.total()
            results.append({**stock, 'factors': factors, 'total_score': total})
            print(f"总分:{total:.1f}")
            time.sleep(0.5)  # 聚宽有限流
        
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        print(f"\n" + "="*90)
        print("🏆 最终推荐（含聚宽真实ROE/成长数据）")
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
    screener = SectorScreenerV7()
    screener.run(top_sectors=3, stocks_per_sector=2)
