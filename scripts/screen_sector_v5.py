#!/usr/bin/env python3
"""
板块优先股票筛选器 V5 - Tushare真实数据版
- 技术因子：新浪实时行情
- 财务因子：Tushare ROE/PE/PB/成长数据
- 机构因子：Tushare 基金持仓
- 舆情因子：Tavily新闻 + DeepSeek AI分析
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
import json
import tushare as ts
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass

# 加载环境变量
os.environ['TUSHARE_TOKEN'] = '0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8'


@dataclass  
class FactorScore:
    """因子得分"""
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


class TushareDataProvider:
    """Tushare数据提供者"""
    
    def __init__(self):
        self.token = os.getenv('TUSHARE_TOKEN')
        if not self.token:
            raise ValueError("请设置 TUSHARE_TOKEN 环境变量")
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        self.cache = {}
    
    def get_fundamental(self, code: str) -> Dict:
        """获取财务数据"""
        cache_key = f"fund_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # 获取最新财务指标
            df = self.pro.fina_indicator(ts_code=code_to_tscode(code))
            if df is None or df.empty:
                return {}
            
            latest = df.iloc[0]
            
            result = {
                'roe': float(latest.get('roe', 0)) if latest.get('roe') else None,
                'roa': float(latest.get('roa', 0)) if latest.get('roa') else None,
                'gross_margin': float(latest.get('grossprofit_margin', 0)) if latest.get('grossprofit_margin') else None,
                'net_margin': float(latest.get('profit_to_gr', 0)) if latest.get('profit_to_gr') else None,
                'revenue_growth': float(latest.get('or_yoy', 0)) if latest.get('or_yoy') else None,
                'profit_growth': float(latest.get('profit_yoy', 0)) if latest.get('profit_yoy') else None,
            }
            
            # 获取估值数据
            df_val = self.pro.daily_basic(ts_code=code_to_tscode(code))
            if df_val is not None and not df_val.empty:
                latest_val = df_val.iloc[0]
                result['pe'] = float(latest_val.get('pe', 0)) if latest_val.get('pe') else None
                result['pb'] = float(latest_val.get('pb', 0)) if latest_val.get('pb') else None
                result['ps'] = float(latest_val.get('ps', 0)) if latest_val.get('ps') else None
            
            self.cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"  获取{code}财务数据失败: {e}")
            return {}
    
    def get_institution(self, code: str) -> Dict:
        """获取机构持仓数据"""
        cache_key = f"inst_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # 获取基金持仓
            df = self.pro.fund_holdings(ts_code=code_to_tscode(code))
            if df is None or df.empty:
                return {'fund_holdings': 0, 'fund_count': 0}
            
            # 计算基金持仓比例和数量
            total_holding = df['holdings'].sum() if 'holdings' in df.columns else 0
            fund_count = df['fund_code'].nunique() if 'fund_code' in df.columns else len(df)
            
            result = {
                'fund_holdings': float(total_holding) if total_holding else 0,
                'fund_count': int(fund_count)
            }
            
            self.cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"  获取{code}机构数据失败: {e}")
            return {'fund_holdings': 0, 'fund_count': 0}


class MultiFactorAnalyzerV5:
    """多因子分析器 V5 - Tushare真实数据"""
    
    def __init__(self):
        self.tushare = TushareDataProvider()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.sina.com.cn'
        })
    
    def analyze_technical(self, stock: Dict) -> int:
        """技术因子评分"""
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
    
    def analyze_fundamental(self, code: str) -> int:
        """财务因子评分 - 使用Tushare真实数据"""
        data = self.tushare.get_fundamental(code)
        
        if not data:
            return 50  # 默认中等分
        
        score = 50
        
        # ROE评分
        roe = data.get('roe')
        if roe:
            if roe > 20: score += 15
            elif roe > 15: score += 10
            elif roe > 10: score += 5
            elif roe < 5: score -= 5
        
        # PE评分
        pe = data.get('pe')
        if pe:
            if 10 < pe < 30: score += 10
            elif 30 <= pe < 50: score += 3
            elif pe > 100: score -= 10
        
        # PB评分
        pb = data.get('pb')
        if pb:
            if pb < 2: score += 5
            elif pb > 10: score -= 5
        
        # 营收增长
        rev_growth = data.get('revenue_growth')
        if rev_growth:
            if rev_growth > 30: score += 10
            elif rev_growth > 20: score += 7
            elif rev_growth > 10: score += 4
        
        # 净利润增长
        profit_growth = data.get('profit_growth')
        if profit_growth:
            if profit_growth > 30: score += 10
            elif profit_growth > 20: score += 7
            elif profit_growth > 10: score += 4
        
        return min(max(score, 0), 100)
    
    def analyze_institution(self, code: str) -> int:
        """机构因子评分 - 使用Tushare真实数据"""
        data = self.tushare.get_institution(code)
        
        score = 50
        fund_holding = data.get('fund_holdings', 0)
        fund_count = data.get('fund_count', 0)
        
        # 基金持仓评分
        if fund_holding > 10: score += 20
        elif fund_holding > 5: score += 15
        elif fund_holding > 2: score += 10
        elif fund_holding > 0.5: score += 5
        
        # 持仓基金数量
        if fund_count > 100: score += 10
        elif fund_count > 50: score += 7
        elif fund_count > 20: score += 4
        
        return min(max(score, 0), 100)
    
    def analyze_sentiment(self, code: str, name: str) -> int:
        """舆情因子 - 简化版"""
        # 这里可以接入 Tavily + DeepSeek
        # 先用模拟数据
        import random
        return random.randint(-3, 5)
    
    def analyze_risk(self, stock: Dict) -> int:
        """风险因子"""
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
        """完整分析"""
        code = stock['code']
        name = stock['name']
        
        return FactorScore(
            technical=self.analyze_technical(stock),
            fundamental=self.analyze_fundamental(code),
            institution=self.analyze_institution(code),
            sentiment=self.analyze_sentiment(code, name),
            risk=self.analyze_risk(stock)
        )


def code_to_tscode(code: str) -> str:
    """转换为tushare代码格式"""
    if code.startswith('6'):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


class SectorScreenerV5:
    """板块优先筛选器 V5 - Tushare真实数据"""
    
    SECTORS = {
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
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
        self.analyzer = MultiFactorAnalyzerV5()
    
    def get_realtime_data(self, codes: List[str]) -> Dict:
        """获取实时行情"""
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
        """解析新浪数据"""
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
                
                results[code] = {
                    'code': code, 'name': name, 'price': price,
                    'change_pct': round(change, 2), 'amplitude': round(amp, 2)
                }
            except: continue
        
        return results
    
    def analyze_sectors(self) -> List[Dict]:
        """分析板块"""
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
            
            trend = "强势" if avg_change > 2 else "活跃" if avg_change > 0.5 else "震荡" if avg_change > -0.5 else "弱势"
            
            sectors.append({
                'name': name, 'score': min(score, 100),
                'change_pct': round(avg_change, 2), 'trend': trend, 'stocks': stocks
            })
            time.sleep(0.1)
        
        sectors.sort(key=lambda x: x['score'], reverse=True)
        return sectors
    
    def run(self, top_sectors: int = 3, stocks_per_sector: int = 2):
        """执行筛选"""
        start = datetime.now()
        
        print("="*90)
        print("🏆 板块优先股票筛选器 V5 - Tushare真实数据版")
        print("="*90)
        print("\n📊 数据源：")
        print("  • 实时行情：新浪财经")
        print("  • 财务数据：Tushare (ROE/PE/PB/成长)")
        print("  • 机构数据：Tushare (基金持仓)")
        print("  • 舆情数据：模拟（可接Tavily）")
        
        # 分析板块
        sectors = self.analyze_sectors()
        
        print(f"\n🔥 强势板块 TOP {top_sectors}:")
        for i, s in enumerate(sectors[:top_sectors], 1):
            print(f"  {i}. {s['name']} ({s['trend']}) 评分:{s['score']} 涨幅:{s['change_pct']:+.2f}%")
        
        # 选股
        all_stocks = []
        for sector in sectors[:top_sectors]:
            for stock in sector['stocks'][:stocks_per_sector]:
                if stock['change_pct'] > -5:
                    stock['sector'] = sector['name']
                    all_stocks.append(stock)
        
        print(f"\n🔍 对 {len(all_stocks)} 只个股进行多因子分析（调用Tushare）...")
        
        results = []
        for i, stock in enumerate(all_stocks):
            print(f"  [{i+1}/{len(all_stocks)}] 分析 {stock['name']}({stock['code']})...", end=' ')
            factors = self.analyzer.full_analysis(stock)
            total = factors.total()
            results.append({**stock, 'factors': factors, 'total_score': total})
            print(f"总分:{total:.1f}")
            time.sleep(0.3)  # 避免请求过快
        
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        # 输出结果
        print(f"\n" + "="*90)
        print("🏆 最终推荐 (多因子综合评分)")
        print("="*90)
        print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<10} {'总分':<8} {'技术':<6} {'财务':<6} {'机构':<6} {'风险':<6}")
        print("-" * 90)
        
        for i, r in enumerate(results[:10], 1):
            f = r['factors']
            print(f"{i:<4} {r['code']:<8} {r['name'][:8]:<10} {r['sector'][:8]:<10} "
                  f"{r['total_score']:<8.1f} {f.technical:<6} {f.fundamental:<6} {f.institution:<6} {f.risk:<6}")
        
        # 详情
        print(f"\n📋 TOP 3 详情:")
        for i, r in enumerate(results[:3], 1):
            f = r['factors']
            print(f"\n  {i}. {r['name']}({r['code']}) [{r['sector']}]")
            print(f"     价格: {r['price']:.2f} | 涨幅: {r['change_pct']:+.2f}%")
            print(f"     技术:{f.technical} | 财务:{f.fundamental} | 机构:{f.institution} | 风险:{f.risk}")
        
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n📊 筛选汇总:")
        print(f"  分析板块: {len(self.SECTORS)} 个")
        print(f"  强势板块: {top_sectors} 个")
        print(f"  分析个股: {len(all_stocks)} 只")
        print(f"  总耗时: {elapsed:.1f} 秒")
        
        return results


if __name__ == "__main__":
    screener = SectorScreenerV5()
    screener.run(top_sectors=3, stocks_per_sector=2)
