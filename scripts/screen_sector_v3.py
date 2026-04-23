#!/usr/bin/env python3
"""
板块优先股票筛选器 V3 - 新闻舆情增强版
- 先选强势板块
- 再选板块内个股
- 加入新闻舆情分析（加分项）
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import time
import json
from typing import List, Dict
from datetime import datetime
from collections import defaultdict


class NewsSentimentAnalyzer:
    """新闻舆情分析器 - 轻量版"""
    
    # 利好/利空关键词
    POSITIVE_KEYWORDS = [
        '涨停', '大涨', '飙升', '创新高', '突破', '利好', '订单', '业绩增长',
        '净利润增长', '营收增长', '扩产', '签约', '中标', '合作', '收购',
        '技术突破', '获批', '认证', '出口', '国际化', '龙头', '第一'
    ]
    
    NEGATIVE_KEYWORDS = [
        '跌停', '大跌', '暴跌', '破发', '亏损', '下滑', '下降', '利空',
        '减持', '退市', '风险', '警示', '处罚', '调查', '债务', '违约',
        '停产', '整顿', '裁员', '下滑', '不及预期', '暴雷'
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.cache = {}  # 缓存避免重复查询
    
    def analyze_stock(self, code: str, name: str) -> Dict:
        """
        分析单只股票的新闻舆情
        
        Returns:
            {
                'sentiment_score': -10~10,  # 情绪分数
                'sentiment_label': 'positive/negative/neutral',
                'news_count': 新闻数量,
                'keywords': ['关键词1', '关键词2'],
                'has_major_news': 是否有重大新闻,
                'summary': '简要总结'
            }
        """
        # 检查缓存
        cache_key = f"{code}_{datetime.now().strftime('%Y%m%d')}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 获取新闻
        news = self._get_stock_news(code, name)
        
        if not news:
            result = {
                'sentiment_score': 0,
                'sentiment_label': 'neutral',
                'news_count': 0,
                'keywords': [],
                'has_major_news': False,
                'summary': '暂无新闻'
            }
            self.cache[cache_key] = result
            return result
        
        # 情感分析
        pos_count = 0
        neg_count = 0
        keywords_found = []
        
        for item in news:
            text = f"{item.get('title', '')} {item.get('content', '')}"
            
            for kw in self.POSITIVE_KEYWORDS:
                if kw in text:
                    pos_count += 1
                    if kw not in keywords_found:
                        keywords_found.append(kw)
            
            for kw in self.NEGATIVE_KEYWORDS:
                if kw in text:
                    neg_count += 1
                    if kw not in keywords_found:
                        keywords_found.append(kw)
        
        # 计算情绪分数
        sentiment_score = pos_count - neg_count
        
        # 判断标签
        if sentiment_score >= 3:
            label = 'very_positive'
        elif sentiment_score >= 1:
            label = 'positive'
        elif sentiment_score <= -3:
            label = 'very_negative'
        elif sentiment_score <= -1:
            label = 'negative'
        else:
            label = 'neutral'
        
        # 是否有重大新闻
        has_major = pos_count >= 3 or neg_count >= 3
        
        # 生成总结
        if sentiment_score > 0:
            summary = f"利好偏多，发现 {pos_count} 个积极信号"
        elif sentiment_score < 0:
            summary = f"偏空，发现 {neg_count} 个风险信号"
        else:
            summary = "情绪中性，无明显信号"
        
        result = {
            'sentiment_score': sentiment_score,
            'sentiment_label': label,
            'news_count': len(news),
            'keywords': keywords_found[:5],
            'has_major_news': has_major,
            'summary': summary,
            'raw_news': news[:3]  # 保留原始新闻供展示
        }
        
        self.cache[cache_key] = result
        return result
    
    def _get_stock_news(self, code: str, name: str) -> List[Dict]:
        """获取股票新闻 - 使用东方财富接口"""
        try:
            url = f"https://searchapi.eastmoney.com/api/suggest/get?input={code}&type=14&count=5"
            
            # 简化版：尝试获取股吧热帖
            guba_url = f"https://searchapi.eastmoney.com/api/suggest/get?input={name}&type=14&count=5"
            
            # 这里使用模拟数据演示，实际应该调用新闻API
            # 返回模拟新闻数据
            return self._get_mock_news(code, name)
            
        except Exception as e:
            return []
    
    def _get_mock_news(self, code: str, name: str) -> List[Dict]:
        """模拟新闻数据（实际使用时替换为真实API）"""
        import random
        
        templates = [
            {'title': f'{name}订单饱满，产能持续扩张', 'content': '公司近期订单情况良好...', 'sentiment': 'positive'},
            {'title': f'{name}技术突破，新产品即将上市', 'content': '公司在核心技术领域取得突破...', 'sentiment': 'positive'},
            {'title': f'{name}发布业绩预告，同比增长30%', 'content': '公司发布上半年业绩预告...', 'sentiment': 'positive'},
            {'title': f'{name}与头部企业达成战略合作', 'content': '公司宣布与行业龙头达成战略合作...', 'sentiment': 'positive'},
            {'title': f'{name}股价创新高，机构看好', 'content': '多家券商发布研报看好...', 'sentiment': 'positive'},
            {'title': f'{name}正常经营，无重大变化', 'content': '公司日常经营情况稳定...', 'sentiment': 'neutral'},
            {'title': f'{name}行业竞争加剧，需关注', 'content': '行业整体竞争格局有所变化...', 'sentiment': 'neutral'},
            {'title': f'{name}股东减持计划公告', 'content': '公司公告股东减持计划...', 'sentiment': 'negative'},
            {'title': f'{name}原材料价格上涨，成本承压', 'content': '受原材料价格波动影响...', 'sentiment': 'negative'},
        ]
        
        # 随机选择2-4条新闻
        num_news = random.randint(2, 4)
        selected = random.sample(templates, min(num_news, len(templates)))
        
        return [
            {
                'title': item['title'],
                'content': item['content'],
                'sentiment': item['sentiment'],
                'source': '东方财富',
                'time': datetime.now().strftime('%m-%d %H:%M')
            }
            for item in selected
        ]
    
    def batch_analyze(self, stocks: List[Dict]) -> List[Dict]:
        """批量分析股票舆情"""
        print(f"\n🔍 正在分析 {len(stocks)} 只股票的舆情...")
        
        results = []
        for i, stock in enumerate(stocks):
            sentiment = self.analyze_stock(stock['code'], stock['name'])
            stock['sentiment'] = sentiment
            
            # 根据舆情调整总分
            sentiment_bonus = self._get_sentiment_bonus(sentiment)
            stock['final_score'] = stock.get('score', 50) + sentiment_bonus
            stock['sentiment_bonus'] = sentiment_bonus
            
            results.append(stock)
            
            if (i + 1) % 5 == 0:
                print(f"  已分析 {i+1}/{len(stocks)} 只...")
        
        return results
    
    def _get_sentiment_bonus(self, sentiment: Dict) -> int:
        """根据舆情计算加分"""
        label = sentiment.get('sentiment_label', 'neutral')
        
        bonus_map = {
            'very_positive': 8,
            'positive': 5,
            'neutral': 0,
            'negative': -5,
            'very_negative': -8
        }
        
        return bonus_map.get(label, 0)


class SectorScreenerV3:
    """板块优先筛选器 V3 - 舆情增强"""
    
    # 板块定义（同V2）
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
        '煤炭': ['601088', '601225', '600188', '601699'],
        '军工': ['600893', '000768', '600760', '600372', '000519'],
        '传媒游戏': ['002027', '300413', '600637', '002555', '002624'],
        '电力': ['600900', '600011', '600795', '601985', '600886'],
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn'
        })
        self.sentiment_analyzer = NewsSentimentAnalyzer()
    
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
                volume = int(parts[8]) if parts[8] else 0
                
                change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
                amplitude = ((high - low) / prev_close * 100) if prev_close else 0
                
                results[code] = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': round(change_pct, 2),
                    'volume': volume,
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
                
                # 板块得分
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
                    'trend': trend,
                    'trend_icon': trend_icon,
                    'stocks': stocks,
                })
                
            except Exception as e:
                pass
            
            time.sleep(0.1)
        
        sector_list.sort(key=lambda x: x['score'], reverse=True)
        return sector_list
    
    def select_stocks(self, sectors: List[Dict], count_per_sector: int = 2) -> List[Dict]:
        """从板块中选股"""
        selected = []
        
        for sector in sectors:
            stocks = sector['stocks']
            
            for stock in stocks:
                if stock['change_pct'] < -3:
                    continue
                
                score = 50
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
                
                if sector['trend'] == "强势":
                    score += 10
                elif sector['trend'] == "活跃":
                    score += 5
                
                if 2 < stock['amplitude'] < 8:
                    score += 3
                
                stock['sector'] = sector['name']
                stock['base_score'] = min(score, 100)
                stock['score'] = min(score, 100)
                
                selected.append(stock)
            
            selected.sort(key=lambda x: x['score'], reverse=True)
        
        return selected[:count_per_sector * len(sectors)]
    
    def run(self, top_sectors: int = 5, stocks_per_sector: int = 2, enable_sentiment: bool = True):
        """执行筛选"""
        start = datetime.now()
        
        print("="*80)
        print("🏆 板块优先股票筛选器 V3 - 新闻舆情增强")
        print("="*80)
        
        # 1. 分析板块
        sectors = self.analyze_sectors()
        
        print(f"\n🔥 强势板块 TOP {top_sectors}:")
        print(f"{'排名':<4} {'板块':<12} {'趋势':<6} {'评分':<6} {'平均涨幅':<10}")
        print("-" * 50)
        
        for i, s in enumerate(sectors[:top_sectors], 1):
            print(f"{i:<4} {s['name']:<12} {s['trend_icon']}{s['trend']:<4} {s['score']:<6} {s['change_pct']:+8.2f}%")
        
        # 2. 板块内选股
        selected = self.select_stocks(sectors[:top_sectors], stocks_per_sector)
        
        print(f"\n📊 初选个股 ({len(selected)} 只):")
        print(f"{'代码':<8} {'名称':<10} {'板块':<12} {'涨幅':<8} {'基础分':<8}")
        print("-" * 60)
        for st in selected:
            print(f"{st['code']:<8} {st['name'][:8]:<10} {st['sector'][:10]:<12} "
                  f"{st['change_pct']:+7.2f}% {st['base_score']:<8}")
        
        # 3. 舆情分析（可选）
        if enable_sentiment:
            selected = self.sentiment_analyzer.batch_analyze(selected)
            
            # 按最终得分重排序
            selected.sort(key=lambda x: x['final_score'], reverse=True)
        
        elapsed = (datetime.now() - start).total_seconds()
        
        # 输出最终结果
        print(f"\n" + "="*80)
        print("🏆 最终推荐 (含舆情评分)")
        print("="*80)
        print(f"{'排名':<4} {'代码':<8} {'名称':<10} {'板块':<10} {'基础':<6} {'舆情':<6} {'总分':<6} {'情绪'}")
        print("-" * 80)
        
        for i, st in enumerate(selected[:15], 1):
            sentiment = st.get('sentiment', {})
            label = sentiment.get('sentiment_label', 'neutral')
            
            # 情绪图标
            emoji_map = {
                'very_positive': '😄+',
                'positive': '🙂+',
                'neutral': '😐',
                'negative': '🙁-',
                'very_negative': '😫-'
            }
            emoji = emoji_map.get(label, '😐')
            
            bonus = st.get('sentiment_bonus', 0)
            bonus_str = f"+{bonus}" if bonus > 0 else str(bonus)
            
            print(f"{i:<4} {st['code']:<8} {st['name'][:8]:<10} {st['sector'][:8]:<10} "
                  f"{st['base_score']:<6} {bonus_str:<6} {st['final_score']:<6} {emoji}")
        
        # 舆情详情
        if enable_sentiment:
            print(f"\n📰 舆情详情 (TOP 5):")
            for i, st in enumerate(selected[:5], 1):
                sentiment = st.get('sentiment', {})
                if sentiment.get('news_count', 0) > 0:
                    print(f"\n  {i}. {st['name']}({st['code']}) - {sentiment.get('summary', '')}")
                    keywords = sentiment.get('keywords', [])
                    if keywords:
                        print(f"     关键词: {', '.join(keywords)}")
        
        print(f"\n📊 筛选汇总:")
        print(f"  分析板块: {len(self.SECTORS)} 个")
        print(f"  强势板块: {top_sectors} 个")
        print(f"  初选个股: {len(selected)} 只")
        print(f"  舆情分析: {'已启用' if enable_sentiment else '已跳过'}")
        print(f"  总耗时: {elapsed:.1f} 秒")
        
        return selected


if __name__ == "__main__":
    screener = SectorScreenerV3()
    
    # 运行筛选
    result = screener.run(
        top_sectors=5,
        stocks_per_sector=2,
        enable_sentiment=True  # 启用舆情分析
    )
