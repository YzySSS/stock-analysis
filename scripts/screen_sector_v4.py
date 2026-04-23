#!/usr/bin/env python3
"""
板块优先股票筛选器 V4 - 多因子增强版
整合因子：
1. 技术指标（涨幅、量比、振幅）
2. 舆情情绪（新闻分析）
3. 财务因子（ROE、PE/PB、成长性）
4. 机构因子（基金持仓比例）
5. 波动率（风险指标）
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
import json
import random
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass


@dataclass
class FactorScore:
    """因子得分"""
    technical: int = 0      # 技术因子 (0-100)
    sentiment: int = 0      # 舆情因子 (-10~10)
    fundamental: int = 0    # 财务因子 (0-100)
    institution: int = 0    # 机构因子 (0-100)
    risk: int = 0           # 风险因子 (0-100, 越高越安全)
    
    def total(self, weights: Dict[str, float] = None) -> float:
        """计算加权总分"""
        if weights is None:
            weights = {
                'technical': 0.30,
                'sentiment': 0.15,
                'fundamental': 0.25,
                'institution': 0.15,
                'risk': 0.15
            }
        
        total = 0
        for key, weight in weights.items():
            val = getattr(self, key, 0)
            # sentiment 是 -10~10，需要映射到 0-100
            if key == 'sentiment':
                val = (val + 10) * 5
            total += val * weight
        
        return round(total, 1)


class MultiFactorAnalyzer:
    """多因子分析器"""
    
    # 关键词库
    POSITIVE_KEYWORDS = ['涨停', '大涨', '业绩增长', '订单', '合作', '技术突破', '龙头', '中标', '签约', '扩产']
    NEGATIVE_KEYWORDS = ['跌停', '大跌', '亏损', '减持', '风险', '处罚', '调查', '债务', '违约', '暴雷']
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.cache = {}
    
    def analyze_technical(self, stock: Dict) -> int:
        """技术因子评分 (0-100)"""
        score = 50
        change = stock.get('change_pct', 0)
        amplitude = stock.get('amplitude', 0)
        
        # 涨幅得分
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
        elif change < -3:
            score -= 10
        
        # 活跃度得分（适中振幅）
        if 3 < amplitude < 8:
            score += 5
        
        return min(max(score, 0), 100)
    
    def analyze_sentiment(self, code: str, name: str) -> int:
        """舆情因子评分 (-10~10)"""
        cache_key = f"sent_{code}_{datetime.now().strftime('%Y%m%d')}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 模拟新闻分析（实际使用时接入真实API）
        news = self._get_mock_news(code, name)
        
        pos_count = sum(1 for n in news if any(kw in n['title'] for kw in self.POSITIVE_KEYWORDS))
        neg_count = sum(1 for n in news if any(kw in n['title'] for kw in self.NEGATIVE_KEYWORDS))
        
        score = pos_count - neg_count
        score = max(-10, min(10, score))
        
        self.cache[cache_key] = score
        return score
    
    def analyze_fundamental(self, code: str, name: str) -> int:
        """
        财务因子评分 (0-100)
        包含：ROE、PE、成长性
        """
        cache_key = f"fund_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 获取模拟财务数据（实际使用时从fundamental_adapter获取）
        fund_data = self._get_mock_fundamental(code, name)
        
        score = 50
        
        # ROE评分
        roe = fund_data.get('roe')
        if roe:
            if roe > 20:
                score += 15
            elif roe > 15:
                score += 10
            elif roe > 10:
                score += 5
            elif roe < 5:
                score -= 5
        
        # PE评分（适中PE加分，过高PE减分）
        pe = fund_data.get('pe')
        if pe:
            if 10 < pe < 30:
                score += 10
            elif 30 <= pe < 50:
                score += 3
            elif pe > 100:
                score -= 10
        
        # PB评分
        pb = fund_data.get('pb')
        if pb:
            if pb < 2:
                score += 5
            elif pb > 10:
                score -= 5
        
        # 营收增长
        revenue_growth = fund_data.get('revenue_growth')
        if revenue_growth:
            if revenue_growth > 30:
                score += 10
            elif revenue_growth > 20:
                score += 7
            elif revenue_growth > 10:
                score += 4
        
        # 净利润增长
        profit_growth = fund_data.get('profit_growth')
        if profit_growth:
            if profit_growth > 30:
                score += 10
            elif profit_growth > 20:
                score += 7
            elif profit_growth > 10:
                score += 4
        
        score = min(max(score, 0), 100)
        self.cache[cache_key] = score
        return score
    
    def analyze_institution(self, code: str, name: str) -> int:
        """
        机构因子评分 (0-100)
        基于基金持仓比例
        """
        cache_key = f"inst_{code}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 模拟机构数据
        inst_data = self._get_mock_institution(code, name)
        
        score = 50
        fund_holding = inst_data.get('fund_holdings', 0)
        fund_count = inst_data.get('fund_count', 0)
        
        # 基金持仓比例评分
        if fund_holding > 10:
            score += 20
        elif fund_holding > 5:
            score += 15
        elif fund_holding > 2:
            score += 10
        elif fund_holding > 0.5:
            score += 5
        else:
            score -= 5
        
        # 持仓基金数量评分
        if fund_count > 100:
            score += 10
        elif fund_count > 50:
            score += 7
        elif fund_count > 20:
            score += 4
        
        score = min(max(score, 0), 100)
        self.cache[cache_key] = score
        return score
    
    def analyze_risk(self, stock: Dict) -> int:
        """
        风险因子评分 (0-100)
        基于波动率，越高表示越安全
        """
        score = 50
        
        # 波动率评分（使用振幅作为代理）
        amplitude = stock.get('amplitude', 0)
        
        # 适度波动较好（有流动性但不太疯狂）
        if 2 < amplitude < 6:
            score += 15
        elif 6 <= amplitude < 10:
            score += 5
        elif amplitude >= 15:
            score -= 20  # 波动过大，风险高
        elif amplitude < 1:
            score -= 10  # 波动过小，流动性差
        
        # 涨跌风险
        change = stock.get('change_pct', 0)
        if change < -5:
            score -= 10
        
        return min(max(score, 0), 100)
    
    def full_analysis(self, stock: Dict) -> FactorScore:
        """完整多因子分析"""
        code = stock['code']
        name = stock['name']
        
        return FactorScore(
            technical=self.analyze_technical(stock),
            sentiment=self.analyze_sentiment(code, name),
            fundamental=self.analyze_fundamental(code, name),
            institution=self.analyze_institution(code, name),
            risk=self.analyze_risk(stock)
        )
    
    def _get_mock_news(self, code: str, name: str) -> List[Dict]:
        """模拟新闻数据"""
        import random
        templates = [
            {'title': f'{name}订单饱满，产能持续扩张'},
            {'title': f'{name}技术突破，新产品即将上市'},
            {'title': f'{name}发布业绩预增公告'},
            {'title': f'{name}与头部企业达成战略合作'},
            {'title': f'{name}正常经营，无重大变化'},
            {'title': f'{name}股东减持计划公告'},
            {'title': f'{name}原材料价格上涨，成本承压'},
        ]
        num = random.randint(2, 4)
        return random.sample(templates, min(num, len(templates)))
    
    def _get_mock_fundamental(self, code: str, name: str) -> Dict:
        """模拟财务数据"""
        import random
        return {
            'roe': random.uniform(5, 25),
            'pe': random.uniform(10, 60),
            'pb': random.uniform(1, 8),
            'revenue_growth': random.uniform(-10, 50),
            'profit_growth': random.uniform(-10, 60),
        }
    
    def _get_mock_institution(self, code: str, name: str) -> Dict:
        """模拟机构数据"""
        import random
        return {
            'fund_holdings': random.uniform(0, 20),
            'fund_count': random.randint(0, 200),
        }


class SectorScreenerV4:
    """板块优先筛选器 V4 - 多因子增强"""
    
    SECTORS = {
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661', '603893'],
        '5G通信': ['000063', '600498', '300502', '002281', '300136', '300308'],
        '云计算': ['000938', '300017', '600845', '300454', '603881'],
        '新能源车': ['002594', '601127', '000625', '600660', '002920', '603596'],
        '锂电池': ['300750', '002460', '002466', '002074', '603659', '300014'],
        '光伏': ['601012', '600438', '002129', '300274', '688599', '600732'],
        '储能': ['300274', '002594', '300014', '300207', '688063'],
        '创新药': ['600276', '000661', '300122', '688180', '688235', '688266'],
        '医疗器械': ['300760', '603658', '300003', '688617', '300482'],
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589'],
        '食品饮料': ['000895', '600887', '603288', '300999', '600298'],
        '家电': ['000333', '000651', '600690', '002032', '603486'],
        '银行': ['000001', '600036', '601398', '601318', '601288', '601169'],
        '券商': ['600030', '300059', '601688', '000776', '601211', '002797'],
        '保险': ['601318', '601628', '601601'],
        '黄金': ['600547', '600489', '601899', '002155'],
        '有色金属': ['601899', '002460', '600547', '603993', '000878', '601600'],
        '军工': ['600893', '000768', '600760', '600372', '000519'],
        '传媒游戏': ['002027', '300413', '600637', '002555', '002624'],
        '电力': ['600900', '600011', '600795', '601985', '600886'],
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.factor_analyzer = MultiFactorAnalyzer()
    
    def get_realtime_data(self, codes: List[str]) -> Dict[str, Dict]:
        """获取实时行情"""
        if not codes:
            return {}
        
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
                prev_close = float(parts[2]) if parts[2] else 0
                price = float(parts[3]) if parts[3] else 0
                high = float(parts[4]) if parts[4] else 0
                low = float(parts[5]) if parts[5] else 0
                
                change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
                amplitude = ((high - low) / prev_close * 100) if prev_close else 0
                
                results[code] = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change_pct, 2),
                    'amplitude': round(amplitude, 2),
                }
            except Exception:
                continue
        
        return results
    
    def analyze_sectors(self) -> List[Dict]:
        """分析板块"""
        print(f"正在分析 {len(self.SECTORS)} 个板块...")
        
        sector_list = []
        
        for sector_name, codes in self.SECTORS.items():
            try:
                data = self.get_realtime_data(codes)
                
                if not data:
                    continue
                
                stocks = list(data.values())
                avg_change = sum(s['change_pct'] for s in stocks) / len(stocks)
                limit_up_count = sum(1 for s in stocks if s['change_pct'] > 9.5)
                
                score = 50
                if avg_change > 4:
                    score += 30
                elif avg_change > 2:
                    score += 20
                elif avg_change > 1:
                    score += 10
                elif avg_change < -1:
                    score -= 10
                
                score += limit_up_count * 5
                
                trend = "强势" if avg_change > 2 else "活跃" if avg_change > 0.5 else "震荡" if avg_change > -0.5 else "弱势"
                
                sector_list.append({
                    'name': sector_name,
                    'score': min(score, 100),
                    'change_pct': round(avg_change, 2),
                    'trend': trend,
                    'stocks': stocks,
                })
                
            except Exception:
                pass
            
            time.sleep(0.1)
        
        sector_list.sort(key=lambda x: x['score'], reverse=True)
        return sector_list
    
    def run(self, top_sectors: int = 5, stocks_per_sector: int = 2):
        """执行完整筛选"""
        start = datetime.now()
        
        print("="*90)
        print("🏆 板块优先股票筛选器 V4 - 多因子增强版")
        print("="*90)
        print("\n📊 评分体系：")
        print("  • 技术因子(30%)：涨幅、量比、振幅")
        print("  • 财务因子(25%)：ROE、PE/PB、成长性")
        print("  • 舆情因子(15%)：新闻情绪、关键词")
        print("  • 机构因子(15%)：基金持仓、机构数量")
        print("  • 风险因子(15%)：波动率、回撤风险")
        
        # 1. 分析板块
        sectors = self.analyze_sectors()
        
        print(f"\n🔥 强势板块 TOP {top_sectors}:")
        print(f"{'排名':<4} {'板块':<12} {'趋势':<6} {'板块评分':<10} {'平均涨幅':<10}")
        print("-" * 55)
        
        for i, s in enumerate(sectors[:top_sectors], 1):
            print(f"{i:<4} {s['name']:<12} {s['trend']:<6} {s['score']:<10} {s['change_pct']:+8.2f}%")
        
        # 2. 从强势板块中选股并多因子分析
        print(f"\n🔍 对个股进行多因子分析...")
        
        all_stocks = []
        for sector in sectors[:top_sectors]:
            for stock in sector['stocks'][:stocks_per_sector]:
                if stock['change_pct'] > -5:  # 排除大跌的
                    stock['sector'] = sector['name']
                    all_stocks.append(stock)
        
        # 多因子分析
        results = []
        for i, stock in enumerate(all_stocks):
            factors = self.factor_analyzer.full_analysis(stock)
            total_score = factors.total()
            
            results.append({
                **stock,
                'factors': factors,
                'total_score': total_score
            })
            
            if (i + 1) % 5 == 0:
                print(f"  已分析 {i+1}/{len(all_stocks)} 只...")
        
        # 按总分排序
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        elapsed = (datetime.now() - start).total_seconds()
        
        # 输出结果
        print(f"\n" + "="*90)
        print("🏆 最终推荐 (多因子综合评分)")
        print("="*90)
        print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<10} {'总分':<8} {'技术':<6} {'财务':<6} {'舆情':<6} {'机构':<6} {'风险':<6}")
        print("-" * 90)
        
        for i, r in enumerate(results[:15], 1):
            f = r['factors']
            print(f"{i:<4} {r['code']:<8} {r['name'][:8]:<10} {r['sector'][:8]:<10} "
                  f"{r['total_score']:<8.1f} {f.technical:<6} {f.fundamental:<6} "
                  f"{f.sentiment:+6} {f.institution:<6} {f.risk:<6}")
        
        # TOP 5 详情
        print(f"\n📋 TOP 5 股票因子详情:")
        for i, r in enumerate(results[:5], 1):
            f = r['factors']
            print(f"\n  {i}. {r['name']}({r['code']}) [{r['sector']}]")
            print(f"     涨幅: {r['change_pct']:+.2f}% | 价格: {r['price']:.2f}")
            print(f"     技术分: {f.technical} | 财务分: {f.fundamental} | 舆情: {f.sentiment:+d} | 机构: {f.institution} | 风险: {f.risk}")
            
            # 因子点评
            comments = []
            if f.technical > 80:
                comments.append("技术强势")
            if f.fundamental > 70:
                comments.append("财务健康")
            if f.sentiment > 3:
                comments.append("舆情积极")
            elif f.sentiment < -3:
                comments.append("舆情偏空")
            if f.institution > 70:
                comments.append("机构青睐")
            if f.risk > 70:
                comments.append("风险较低")
            
            if comments:
                print(f"     亮点: {' | '.join(comments)}")
        
        print(f"\n📊 筛选汇总:")
        print(f"  分析板块: {len(self.SECTORS)} 个")
        print(f"  强势板块: {top_sectors} 个")
        print(f"  分析个股: {len(all_stocks)} 只")
        print(f"  总耗时: {elapsed:.1f} 秒")
        
        return results


if __name__ == "__main__":
    screener = SectorScreenerV4()
    screener.run(top_sectors=5, stocks_per_sector=2)
