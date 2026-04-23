#!/usr/bin/env python3
"""
板块优先股票筛选器 V8 - 全A股版（4914只）
使用聚宽真实数据，覆盖沪深主板+创业板
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

WATCHLIST_FILE = os.path.expanduser("~/.clawdbot/stock_watcher/watchlist.txt")
ALL_A_STOCKS_FILE = os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt")


@dataclass
class FactorScore:
    technical: int = 0
    fundamental: int = 0
    institution: int = 0
    sentiment: int = 0
    risk: int = 0
    sector: int = 50  # 新增：行业轮动因子，默认50分（中性）
    
    def total(self, weights=None):
        if weights is None:
            # 调整权重：降低技术因子，增加行业因子
            weights = {
                'technical': 0.25, 
                'fundamental': 0.25, 
                'institution': 0.15, 
                'sentiment': 0.10, 
                'risk': 0.10,
                'sector': 0.15  # 行业轮动因子权重15%
            }
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
        try:
            from jqdatasdk import auth
            auth(JQ_USERNAME, JQ_PASSWORD)
            self.connected = True
            print("  ✅ 聚宽连接成功")
        except Exception as e:
            print(f"  ⚠️ 聚宽连接失败: {e}")
    
    def get_fundamental(self, code: str) -> Dict:
        """获取财务数据"""
        if not self.connected:
            return {}
        
        cache_key = f"fund_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            from jqdatasdk import query, valuation, indicator
            
            jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
            
            q = query(
                valuation.code,
                valuation.pe_ratio,
                valuation.pb_ratio,
                valuation.ps_ratio,
                indicator.roe,
                indicator.inc_net_profit_year_on_year,
                indicator.inc_operation_profit_year_on_year,
                indicator.gross_profit_margin,
                indicator.net_profit_margin
            ).filter(valuation.code == jq_code)
            
            from jqdatasdk import run_query
            df = run_query(q)
            
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
        except:
            pass
        return {}
    
    def get_industry(self, code: str) -> str:
        """获取行业分类"""
        if not self.connected:
            return "其他"
        
        cache_key = f"ind_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            from jqdatasdk import get_industry
            jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
            industry = get_industry(jq_code)
            
            if industry and jq_code in industry:
                # 获取申万行业分类
                sw = industry[jq_code].get('sw_l1', {})
                if sw:
                    name = sw.get('industry_name', '其他')
                    self.cache[cache_key] = name
                    return name
        except:
            pass
        
        return "其他"


class SinaDataProvider:
    """新浪实时数据"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
    
    def get_batch_quotes(self, codes: List[str]) -> Dict[str, Dict]:
        """批量获取行情（每次最多800）"""
        if not codes:
            return {}
        
        formatted = [f"sh{c}" if c.startswith('6') else f"sz{c}" for c in codes]
        url = f"https://hq.sinajs.cn/list={','.join(formatted)}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = 'gbk'
            return self._parse(response.text, codes)
        except Exception as e:
            print(f"获取数据失败: {e}")
            return {}
    
    def _parse(self, text: str, codes: List[str]) -> Dict:
        results = {}
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines):
            if i >= len(codes):
                break
            code = codes[i]
            if '=""' in line or '=";' in line:
                continue
            
            try:
                data = line.split('="')[1].rstrip('";')
                parts = data.split(',')
                if len(parts) < 33:
                    continue
                
                name = parts[0]
                prev = float(parts[2]) if parts[2] else 0
                price = float(parts[3]) if parts[3] else 0
                high = float(parts[4]) if parts[4] else 0
                low = float(parts[5]) if parts[5] else 0
                volume = int(parts[8]) if parts[8] else 0
                
                change = ((price - prev) / prev * 100) if prev else 0
                amp = ((high - low) / prev * 100) if prev else 0
                
                results[code] = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change, 2),
                    'amplitude': round(amp, 2),
                    'volume': volume,
                }
            except:
                continue
        
        return results


class MultiFactorAnalyzer:
    """多因子分析"""
    
    def __init__(self):
        self.jq = JoinQuantProvider()
        self.sector_scores = {}  # 缓存行业轮动分数
    
    def set_sector_scores(self, sector_scores: Dict[str, int]):
        """设置行业轮动分数"""
        self.sector_scores = sector_scores
    
    def analyze(self, stock: Dict) -> FactorScore:
        """完整分析"""
        code = stock['code']
        
        # 技术因子
        tech_score = 50
        change = stock.get('change_pct', 0)
        amp = stock.get('amplitude', 0)
        
        if change > 9: tech_score += 25
        elif change > 5: tech_score += 20
        elif change > 3: tech_score += 15
        elif change > 1: tech_score += 8
        elif change > 0: tech_score += 3
        elif change < -3: tech_score -= 10
        
        if 3 < amp < 8: tech_score += 5
        
        # 获取聚宽财务数据
        fund = self.jq.get_fundamental(code)
        
        # 财务因子
        fund_score = 50
        if fund:
            pe = fund.get('pe')
            if pe and pe > 0:
                if pe < 15: fund_score += 15
                elif pe < 25: fund_score += 10
                elif pe < 40: fund_score += 3
                elif pe > 100: fund_score -= 10
            
            pb = fund.get('pb')
            if pb and pb > 0:
                if pb < 1.5: fund_score += 10
                elif pb < 3: fund_score += 5
                elif pb > 8: fund_score -= 5
            
            roe = fund.get('roe')
            if roe and roe > 0:
                if roe > 20: fund_score += 15
                elif roe > 15: fund_score += 10
                elif roe > 10: fund_score += 5
                elif roe < 5: fund_score -= 5
            
            profit_g = fund.get('profit_growth')
            if profit_g and profit_g > 0:
                if profit_g > 30: fund_score += 10
                elif profit_g > 20: fund_score += 7
                elif profit_g > 10: fund_score += 4
        
        # 机构因子（市值估算）
        inst_score = 50
        # 简化处理
        
        # 舆情因子
        sent_score = 0
        
        # 风险因子
        risk_score = 50
        if 2 < amp < 6: risk_score += 15
        elif 6 <= amp < 10: risk_score += 5
        elif amp >= 15: risk_score -= 20
        elif amp < 1: risk_score -= 10
        
        if change < -5: risk_score -= 10
        
        # 行业轮动因子（从缓存获取）
        sector_score = 50  # 默认中性
        stock_sector = stock.get('sector', '其他')
        if stock_sector and stock_sector in self.sector_scores:
            sector_score = self.sector_scores[stock_sector]
        
        return FactorScore(
            technical=min(max(tech_score, 0), 100),
            fundamental=min(max(fund_score, 0), 100),
            institution=min(max(inst_score, 0), 100),
            sentiment=max(-10, min(10, sent_score)),
            risk=min(max(risk_score, 0), 100),
            sector=min(max(sector_score, 0), 100)
        )


class SectorScreenerV8:
    """V8 全A股版"""
    
    def __init__(self):
        self.sina = SinaDataProvider()
        self.analyzer = MultiFactorAnalyzer()
        self.all_stocks = self._load_watchlist()
        print(f"✅ 已加载 {len(self.all_stocks)} 只股票")
    
    def _load_watchlist(self) -> List[str]:
        """加载全A股列表 - 优先使用聚宽全A股数据"""
        # 优先使用全A股列表（5486只）
        if os.path.exists(ALL_A_STOCKS_FILE):
            try:
                with open(ALL_A_STOCKS_FILE, 'r', encoding='utf-8') as f:
                    codes = [line.strip() for line in f if line.strip()]
                print(f"  📊 使用全A股列表: {len(codes)} 只")
                return codes
            except Exception as e:
                print(f"  ⚠️ 读取全A股列表失败: {e}")
        
        # 回退到精简列表
        if os.path.exists(WATCHLIST_FILE):
            try:
                with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                    codes = [line.strip().split('|')[0] for line in f if line.strip()]
                print(f"  📊 使用精简列表: {len(codes)} 只")
                return codes
            except Exception as e:
                print(f"  ⚠️ 读取精简列表失败: {e}")
        
        # 如果都不存在，返回空列表
        print("  ⚠️ 未找到股票列表文件")
        return []
    
    def get_all_quotes(self) -> Dict[str, Dict]:
        """获取全市场实时行情"""
        print(f"\n正在获取 {len(self.all_stocks)} 只股票实时行情...")
        
        all_quotes = {}
        batch_size = 800
        total = len(self.all_stocks)
        
        for i in range(0, total, batch_size):
            batch = self.all_stocks[i:i+batch_size]
            quotes = self.sina.get_batch_quotes(batch)
            all_quotes.update(quotes)
            
            progress = min(i + batch_size, total)
            print(f"  进度: {progress}/{total} ({progress/total*100:.1f}%)")
            time.sleep(0.5)  # 避免请求过快
        
        print(f"✅ 成功获取 {len(all_quotes)} 只股票数据")
        return all_quotes
    
    def analyze_sectors_by_industry(self, quotes: Dict[str, Dict]) -> List[Dict]:
        """按行业分析板块，并为股票添加行业标签"""
        print("\n正在按行业分类...")
        
        # 获取行业分类（简化：用聚宽获取部分主要行业的股票）
        # 实际项目中可以缓存行业分类
        sectors_map = {}
        
        # 简化的行业分类（基于股票代码前缀）
        for code, data in quotes.items():
            # 根据代码前缀大致分类
            if code.startswith('6'):
                sector = "沪市主板"
            elif code.startswith('00'):
                sector = "深市主板"
            elif code.startswith('30'):
                sector = "创业板"
            else:
                sector = "其他"
            
            # 为股票数据添加行业标签
            data['sector'] = sector
            
            if sector not in sectors_map:
                sectors_map[sector] = []
            sectors_map[sector].append(data)
        
        # 计算板块得分
        sectors = []
        for name, stocks in sectors_map.items():
            if len(stocks) < 10:  # 跳过太小的板块
                continue
            
            avg_change = sum(s['change_pct'] for s in stocks) / len(stocks)
            limit_up = sum(1 for s in stocks if s['change_pct'] > 9.5)
            
            score = 50
            if avg_change > 2: score += 20
            elif avg_change > 1: score += 10
            elif avg_change < -1: score -= 10
            score += limit_up * 2
            
            sectors.append({
                'name': name,
                'score': min(score, 100),
                'change_pct': round(avg_change, 2),
                'stock_count': len(stocks),
                'stocks': sorted(stocks, key=lambda x: x['change_pct'], reverse=True)
            })
        
        sectors.sort(key=lambda x: x['score'], reverse=True)
        return sectors
    
    def calculate_sector_rotation_scores(self, sectors: List[Dict]) -> Dict[str, int]:
        """计算行业轮动分数
        
        根据板块表现分配分数：
        - 强势板块（涨幅>2%）：80-100分
        - 中等板块（涨幅0-2%）：50-80分  
        - 弱势板块（涨幅<0%）：0-50分
        """
        sector_scores = {}
        
        for sector in sectors:
            name = sector['name']
            change_pct = sector['change_pct']
            score = sector.get('score', 50)
            
            # 基于板块涨跌幅计算轮动分数
            if change_pct > 3:
                base_score = 90
            elif change_pct > 2:
                base_score = 80
            elif change_pct > 1:
                base_score = 70
            elif change_pct > 0:
                base_score = 60
            elif change_pct > -1:
                base_score = 50
            elif change_pct > -2:
                base_score = 40
            else:
                base_score = 30
            
            # 结合板块得分进行调整
            final_score = (base_score + score) / 2
            sector_scores[name] = min(max(int(final_score), 0), 100)
        
        return sector_scores
    
    def select_top_stocks(self, quotes: Dict[str, Dict], top_n: int = 3) -> List[Dict]:
        """从全市场精选个股"""
        print(f"\n正在从 {len(quotes)} 只股票中精选...")
        
        # 初步筛选：排除ST、排除大跌、排除涨停、排除弱势行业
        candidates = []
        sector_scores = getattr(self.analyzer, 'sector_scores', {})
        
        for code, data in quotes.items():
            if data['change_pct'] < -5:  # 排除大跌
                continue
            if 'ST' in data.get('name', ''):  # 排除ST
                continue
            # 排除涨停股（无法买入）
            change_pct = data['change_pct']
            if code.startswith(('30', '68')):  # 创业板/科创板涨停 20%
                if change_pct >= 19.5:
                    continue
            elif code.startswith('8'):  # 北交所涨停 30%
                if change_pct >= 29.5:
                    continue
            else:  # 主板涨停 10%
                if change_pct >= 9.5:
                    continue
            
            # 排除弱势行业（行业轮动分数<40）
            stock_sector = data.get('sector', '其他')
            if stock_sector in sector_scores and sector_scores[stock_sector] < 40:
                continue
            
            candidates.append(data)
        
        print(f"  初筛后: {len(candidates)} 只（已排除ST、大跌、涨停、弱势行业）")
        
        # 多因子分析（只分析前200只涨得好的，节省API调用）
        candidates.sort(key=lambda x: x['change_pct'], reverse=True)
        top_candidates = candidates[:200]
        
        print(f"  分析TOP {len(top_candidates)} 只...")
        
        results = []
        for i, stock in enumerate(top_candidates):
            factors = self.analyzer.analyze(stock)
            total = factors.total()
            results.append({
                **stock,
                'factors': factors,
                'total_score': total
            })
            
            if (i + 1) % 50 == 0:
                print(f"    已分析 {i+1}/{len(top_candidates)}")
            time.sleep(0.1)
        
        # 按总分排序
        results.sort(key=lambda x: x['total_score'], reverse=True)
        return results[:top_n]
    
    def run(self, top_n: int = 3, for_cron: bool = False):
        """执行全A股筛选
        
        Args:
            top_n: 输出前N只股票，默认3只
            for_cron: 是否为定时任务模式（简化输出）
        """
        start = datetime.now()
        
        if not for_cron:
            print("="*90)
            print("🏆 板块优先股票筛选器 V8 - 全A股版（4914只）")
            print("="*90)
            print(f"\n📊 股票池：{len(self.all_stocks)} 只（沪深主板+创业板）")
            print("  • 数据源：新浪财经 + 聚宽财务数据")
        
        # 1. 获取全市场行情
        quotes = self.get_all_quotes()
        
        if not quotes:
            if not for_cron:
                print("❌ 获取行情失败")
            return []
        
        # 2. 板块分析
        sectors = self.analyze_sectors_by_industry(quotes)
        
        if not for_cron:
            print(f"\n🔥 强势板块:")
            for i, s in enumerate(sectors[:5], 1):
                print(f"  {i}. {s['name']} | 股票数:{s['stock_count']} | 平均涨幅:{s['change_pct']:+.2f}% | 评分:{s['score']}")
        
        # 计算行业轮动分数
        sector_scores = self.calculate_sector_rotation_scores(sectors)
        self.analyzer.set_sector_scores(sector_scores)
        
        if not for_cron:
            print(f"\n📊 行业轮动因子已加载")
            top_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            print(f"  强势行业: {', '.join([f'{k}({v})' for k, v in top_sectors])}")
        
        # 3. 精选个股
        top_stocks = self.select_top_stocks(quotes, top_n=top_n)
        
        elapsed = (datetime.now() - start).total_seconds()
        
        # 4. 输出结果
        if for_cron:
            # 定时任务模式：简洁输出，方便推送
            print(f"\n📊 全A股筛选完成 | 耗时{elapsed:.1f}s")
            print(f"\n🏆 TOP {top_n} 推荐:")
            for i, r in enumerate(top_stocks, 1):
                print(f"{i}. {r['name']}({r['code']}) {r['change_pct']:+.2f}% | 总分:{r['total_score']:.1f}")
        else:
            # 普通模式：详细输出
            print(f"\n" + "="*90)
            print(f"🏆 TOP {top_n} 推荐（全A股多因子评分 - 含行业轮动因子）")
            print("="*90)
            print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'涨幅':<8} {'行业':<8} {'总分':<8} {'技术':<6} {'财务':<6} {'行业':<6} {'风险':<6}")
            print("-" * 90)
            
            for i, r in enumerate(top_stocks, 1):
                f = r['factors']
                sector_name = r.get('sector', '其他')[:4]
                print(f"{i:<4} {r['code']:<8} {r['name'][:8]:<10} "
                      f"{r['change_pct']:+7.2f}% {sector_name:<8}"
                      f"{r['total_score']:<8.1f} {f.technical:<6} {f.fundamental:<6} {f.sector:<6} {f.risk:<6}")
            
            print(f"\n📊 筛选汇总:")
            print(f"  股票池: {len(self.all_stocks)} 只")
            print(f"  获取行情: {len(quotes)} 只")
            print(f"  最终推荐: {len(top_stocks)} 只")
            print(f"  总耗时: {elapsed:.1f} 秒")
        
        return top_stocks


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='全A股股票筛选器V8')
    parser.add_argument('--top', type=int, default=3, help='输出前N只股票，默认3')
    parser.add_argument('--cron', action='store_true', help='定时任务模式（简洁输出）')
    args = parser.parse_args()
    
    screener = SectorScreenerV8()
    screener.run(top_n=args.top, for_cron=args.cron)
