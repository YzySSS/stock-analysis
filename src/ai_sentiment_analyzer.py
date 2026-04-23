#!/usr/bin/env python3
"""
AI舆情分析模块
复用 daily_stock_analysis 项目架构，简化实现
- 支持新闻搜索 + AI情感分析
- 支持YAML策略定义
"""

import os
import yaml
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import requests

# 导入新闻聚合器
from news_provider import NewsAggregator

logger = logging.getLogger(__name__)


class NewsSearcher:
    """新闻搜索器 - 支持多种搜索源"""
    
    def __init__(self):
        self.tavily_key = os.getenv("TAVILY_API_KEY")
        self.serpapi_key = os.getenv("SERPAPI_KEY")
        self.minimax_key = os.getenv("MINIMAX_API_KEY")
        
        # 使用新闻聚合器（AkShare免费 + Coze Web Search）
        self.aggregator = NewsAggregator()
    
    def search_stock_news(self, stock_code: str, stock_name: str, days: int = 3) -> List[Dict]:
        """
        搜索股票相关新闻
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            days: 搜索最近几天的新闻
        
        Returns:
            新闻列表
        """
        # 方案1: 使用聚合器（AkShare免费 + Coze Web Search）
        news = self.aggregator.get_stock_news(stock_code, stock_name)
        if news:
            return news
        
        # 方案2: Tavily API（如果配置了）
        if self.tavily_key:
            return self._search_tavily(f"{stock_name} {stock_code} 股票", days)
        
        # 方案3: SerpAPI
        if self.serpapi_key:
            return self._search_serpapi(f"{stock_name} {stock_code} 股票", days)
        
        logger.warning("未配置新闻搜索，跳过舆情分析")
        return []
    
    def _search_tavily(self, query: str, days: int) -> List[Dict]:
        """使用 Tavily 搜索"""
        try:
            url = "https://api.tavily.com/search"
            response = requests.post(url, json={
                "api_key": self.tavily_key,
                "query": query,
                "search_depth": "basic",
                "include_answer": False,
                "max_results": 10
            }, timeout=30)
            
            data = response.json()
            results = data.get("results", [])
            
            # 过滤时间
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered = []
            for r in results:
                published = r.get("published_date", "")
                if published:
                    try:
                        pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                        if pub_date >= cutoff_date:
                            filtered.append({
                                "title": r.get("title", ""),
                                "content": r.get("content", ""),
                                "url": r.get("url", ""),
                                "source": r.get("source", ""),
                                "date": published
                            })
                    except:
                        filtered.append(r)
                else:
                    filtered.append(r)
            
            return filtered
            
        except Exception as e:
            logger.error(f"Tavily搜索失败: {e}")
            return []
    
    def _search_serpapi(self, query: str, days: int) -> List[Dict]:
        """使用 SerpAPI 搜索"""
        try:
            url = "https://serpapi.com/search"
            response = requests.get(url, params={
                "q": query,
                "api_key": self.serpapi_key,
                "engine": "google",
                "num": 10
            }, timeout=30)
            
            data = response.json()
            results = data.get("organic_results", [])
            
            return [{
                "title": r.get("title", ""),
                "content": r.get("snippet", ""),
                "url": r.get("link", ""),
                "source": r.get("source", "")
            } for r in results]
            
        except Exception as e:
            logger.error(f"SerpAPI搜索失败: {e}")
            return []


class SentimentAnalyzer:
    """舆情情感分析器 - 优先使用本地关键词，LLM作为备选"""
    
    # 正面关键词库
    POSITIVE_WORDS = [
        '上涨', '涨停', '利好', '增长', '突破', '强势', '看好', '反弹',
        '增持', '买入', '推荐', '买入评级', '强烈推荐', '增持评级',
        '超预期', '龙头', '领涨', '创新高', '放量上涨', '资金流入',
        '业绩大增', '利润增长', '营收增长', '订单饱满', '产能扩张',
        '政策支持', '行业景气', '供需紧张', '涨价', '产品提价',
        '技术突破', '研发成功', '新药获批', '订单暴增', '中标',
        '并购重组', '资产注入', '股权激励', '回购', '分红',
        '北向资金流入', '机构加仓', '主力买入', '游资抢筹',
        '净利润增长', '营收增长', '毛利率提升', '净利率提升',
        '市场占有率提升', '行业龙头', '竞争优势', '护城河'
    ]
    
    # 负面关键词库
    NEGATIVE_WORDS = [
        '下跌', '跌停', '利空', '下滑', '跌破', '弱势', '看空', '调整',
        '减持', '卖出', '回避', '卖出评级', '减持评级', '中性评级',
        '低于预期', '风险', '暴雷', '踩雷', '业绩下滑', '利润下降',
        '亏损', '亏损扩大', '营收下滑', '订单减少', '产能过剩',
        '政策打压', '行业低迷', '供过于求', '降价', '产品降价',
        '技术失败', '研发失败', '新药被拒', '订单取消', '失标',
        '分拆', '资产剥离', '股权质押', '爆仓', '债务违约',
        '北向资金流出', '机构减仓', '主力卖出', '游资出逃',
        '立案调查', '监管函', '问询函', '关注函', '警示函',
        '财务造假', '信披违规', '内幕交易', '操纵市场',
        '净利润下滑', '营收下滑', '毛利率下降', '净利率下降',
        '市场占有率下降', '竞争加剧', '行业衰退'
    ]
    
    def __init__(self, api_key: Optional[str] = None, use_local_only: bool = True):
        # 优先使用 DeepSeek 配置，兼容 OpenAI 格式
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.base_url = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com/v1")
        self.model = os.getenv("DEEPSEEK_MODEL") or os.getenv("OPENAI_MODEL", "deepseek-chat")
        
        # 默认只使用本地分析（更稳定快速）
        # API分析可以通过设置 use_local_only=False 启用
        self.use_local_only = use_local_only or not bool(self.api_key)
        
        if self.use_local_only:
            logger.debug("使用本地关键词分析（稳定快速）")
        elif self.api_key and "deepseek" in self.base_url:
            logger.info(f"✅ 使用 DeepSeek API 进行情感分析: {self.model}")
        elif self.api_key:
            logger.info(f"使用 OpenAI API 进行情感分析: {self.model}")
    
    def analyze_sentiment(self, stock_code: str, stock_name: str, news_list: List[Dict]) -> Dict:
        """
        分析新闻情感
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            news_list: 新闻列表
        
        Returns:
            情感分析结果
        """
        if not news_list:
            return {
                "sentiment": "neutral",
                "score": 50,
                "summary": "无近期新闻",
                "risks": [],
                "opportunities": []
            }
        
        # 构建prompt
        news_text = "\n\n".join([
            f"标题: {n.get('title', '')}\n内容: {n.get('content', '')}"
            for n in news_list[:5]
        ])
        
        # 优先使用本地关键词匹配（不依赖外部API）
        local_result = self._analyze_sentiment_local(news_list)
        
        # 如果本地分析有明确结果，直接返回
        if local_result["sentiment"] != "neutral" or local_result["score"] != 50:
            return local_result
        
        # 本地分析为中性，尝试使用API（如果有配置）
        if self.use_local_only or not self.api_key:
            return local_result
        
        # API分析
        return self._analyze_sentiment_api(stock_code, stock_name, news_list)
    
    def _analyze_sentiment_local(self, news_list: List[Dict]) -> Dict:
        """
        本地关键词情感分析（不依赖外部API）
        """
        if not news_list:
            return {
                "sentiment": "neutral",
                "score": 50,
                "summary": "无近期新闻",
                "risks": [],
                "opportunities": []
            }
        
        # 合并所有新闻文本
        all_text = ""
        for news in news_list:
            all_text += news.get("title", "") + " " + news.get("content", "")[:200] + " "
        
        all_text = all_text.lower()
        
        # 统计关键词
        pos_count = sum(1 for word in self.POSITIVE_WORDS if word in all_text)
        neg_count = sum(1 for word in self.NEGATIVE_WORDS if word in all_text)
        
        # 计算得分
        total = pos_count + neg_count
        if total == 0:
            return {
                "sentiment": "neutral",
                "score": 50,
                "summary": "新闻内容无明显情感倾向",
                "risks": [],
                "opportunities": []
            }
        
        # 映射到0-100分
        score = 50 + (pos_count - neg_count) / max(total, 3) * 50
        score = max(0, min(100, score))
        
        # 确定情感倾向
        if score >= 60:
            sentiment = "positive"
        elif score <= 40:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        
        # 提取风险和机会
        risks = []
        opportunities = []
        
        for word in self.NEGATIVE_WORDS[:10]:  # 只检查前10个高频词
            if word in all_text and word not in risks:
                risks.append(word)
                if len(risks) >= 2:
                    break
        
        for word in self.POSITIVE_WORDS[:10]:
            if word in all_text and word not in opportunities:
                opportunities.append(word)
                if len(opportunities) >= 2:
                    break
        
        return {
            "sentiment": sentiment,
            "score": round(score),
            "summary": f"本地分析: 正面词{pos_count}个, 负面词{neg_count}个",
            "risks": risks,
            "opportunities": opportunities
        }
    
    def _analyze_sentiment_api(self, stock_code: str, stock_name: str, news_list: List[Dict]) -> Dict:
        """
        API情感分析（备选）
        """
        news_text = "\n\n".join([
            f"标题: {n.get('title', '')}\n内容: {n.get('content', '')}"
            for n in news_list[:5]
        ])
        
        prompt = f"""作为专业股票分析师，请分析以下关于 {stock_name}({stock_code}) 的新闻舆情：

{news_text}

请输出JSON格式分析结果：
{{
    "sentiment": "positive/negative/neutral",
    "score": 0-100的情感分数,
    "summary": "一句话总结市场情绪",
    "risks": ["风险点1", "风险点2"],
    "opportunities": ["利好点1", "利好点2"],
    "key_events": ["关键事件1", "关键事件2"]
}}
"""
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3
                },
                timeout=10  # 缩短超时时间
            )
            
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            # 提取JSON
            try:
                result = json.loads(content)
            except:
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    raise Exception("无法解析API响应")
            
            return result
            
        except Exception as e:
            logger.warning(f"API情感分析失败，使用本地分析: {e}")
            # API失败，返回本地分析结果
            return self._analyze_sentiment_local(news_list)


class StrategyLoader:
    """策略加载器 - 复用 daily_stock_analysis 的YAML策略"""
    
    def __init__(self, strategies_dir: str = "./strategies"):
        self.strategies_dir = strategies_dir
        self.strategies = {}
        self._load_builtin_strategies()
    
    def _load_builtin_strategies(self):
        """加载内置策略"""
        # 这里可以直接复用 GitHub项目的策略文件
        # 先内置几个核心策略
        
        self.strategies = {
            "ma_golden_cross": {
                "name": "ma_golden_cross",
                "display_name": "均线金叉策略",
                "description": "MA5上穿MA10形成金叉时买入",
                "category": "trend",
                "instructions": """
                **均线金叉策略**
                
                判断标准：
                1. **金叉确认**：MA5 > MA10 且 MA5前一日 < MA10前一日
                2. **趋势配合**：MA10 > MA20（多头排列更佳）
                3. **量能确认**：成交量放大，量比 > 1.2
                
                入场条件：
                - 金叉形成当日或次日回踩
                - 价格不宜偏离MA5过远（乖离率 < 5%）
                
                出场条件：
                - MA5下穿MA10（死叉）
                - 或价格跌破MA10
                
                评分调整：
                - 金叉当日：sentiment_score + 20
                - 多头排列：额外 + 10
                - 量能放大：额外 + 10
                """
            },
            "chan_theory": {
                "name": "chan_theory", 
                "display_name": "缠论策略",
                "description": "基于缠论中枢、背驰的交易策略",
                "category": "framework",
                "instructions": """
                **缠论策略**
                
                判断标准：
                1. **趋势判断**：识别上涨/下跌趋势
                2. **中枢识别**：找出价格中枢区间
                3. **背驰判断**：MACD背驰或量价背驰
                
                入场条件：
                - 下跌背驰（一买）
                - 或二买、三买确认
                
                出场条件：
                - 上涨背驰（一卖）
                - 或跌破关键支撑
                
                风险控制：
                - 严格止损，一般设在中枢下轨
                """
            },
            "breakout": {
                "name": "breakout",
                "display_name": "突破策略", 
                "description": "突破前期高点或平台整理",
                "category": "pattern",
                "instructions": """
                **突破策略**
                
                判断标准：
                1. **平台整理**：价格在狭窄区间盘整5-10日
                2. **突破确认**：收盘价突破平台上沿
                3. **量能配合**：突破时成交量放大
                
                入场条件：
                - 放量突破当日
                - 回踩确认支撑有效
                
                出场条件：
                - 跌破突破价位（假突破止损）
                - 或达到目标涨幅
                """
            }
        }
    
    def get_strategy(self, name: str) -> Optional[Dict]:
        """获取策略定义"""
        return self.strategies.get(name)
    
    def list_strategies(self) -> List[str]:
        """列出所有策略"""
        return list(self.strategies.keys())


class AIStockAnalyzer:
    """
    AI股票分析器
    复用 daily_stock_analysis 架构，简化实现
    """
    
    def __init__(self):
        self.news_searcher = NewsSearcher()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.strategy_loader = StrategyLoader()
        
        # LLM配置
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
    
    def analyze(self, 
                stock_code: str, 
                stock_name: str,
                strategy: str = "ma_golden_cross",
                technical_data: Dict = None) -> Dict:
        """
        综合分析一只股票
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            strategy: 使用的策略名称
            technical_data: 技术分析数据（从原有模块传入）
        
        Returns:
            AI分析结果
        """
        logger.info(f"开始AI分析: {stock_name}({stock_code})")
        
        # 1. 获取新闻舆情
        news = self.news_searcher.search_stock_news(stock_code, stock_name)
        
        # 2. 情感分析
        sentiment = self.sentiment_analyzer.analyze_sentiment(
            stock_code, stock_name, news
        )
        
        # 3. 获取策略定义
        strategy_def = self.strategy_loader.get_strategy(strategy)
        
        # 4. AI综合决策
        decision = self._make_decision(
            stock_code, stock_name, 
            technical_data, sentiment, 
            strategy_def
        )
        
        return {
            "code": stock_code,
            "name": stock_name,
            "strategy": strategy,
            "sentiment": sentiment,
            "decision": decision,
            "news_count": len(news),
            "analysis_time": datetime.now().isoformat()
        }
    
    def _make_decision(self, 
                       stock_code: str, 
                       stock_name: str,
                       technical_data: Dict,
                       sentiment: Dict,
                       strategy: Dict) -> Dict:
        """AI生成交易决策"""
        
        if not self.api_key:
            return {
                "action": "hold",
                "confidence": 0,
                "reason": "未配置AI API，无法生成决策",
                "entry_price": None,
                "stop_loss": None,
                "target_price": None
            }
        
        # 构建决策prompt
        prompt = f"""作为专业股票交易员，请根据以下信息生成交易决策：

股票: {stock_name}({stock_code})

**技术分析数据**:
{json.dumps(technical_data, ensure_ascii=False, indent=2)}

**舆情分析**:
{json.dumps(sentiment, ensure_ascii=False, indent=2)}

**使用策略**: {strategy.get('display_name', strategy.get('name', ''))}
策略说明: {strategy.get('instructions', '')}

请输出JSON格式决策：
{{
    "action": "buy/sell/hold",
    "confidence": 0-100的置信度,
    "reason": "决策理由，一句话",
    "entry_price": "建议买入价格区间",
    "stop_loss": "止损价位",
    "target_price": "目标价位",
    "risk_level": "high/medium/low",
    "checklist": [
        "检查项1: 满足/注意/不满足",
        "检查项2: 满足/注意/不满足"
    ]
}}
"""
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3
                },
                timeout=60
            )
            
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            # 提取JSON
            try:
                decision = json.loads(content)
            except:
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    decision = json.loads(json_match.group())
                else:
                    decision = {
                        "action": "hold",
                        "confidence": 0,
                        "reason": content[:200],
                        "entry_price": None,
                        "stop_loss": None,
                        "target_price": None
                    }
            
            return decision
            
        except Exception as e:
            logger.error(f"AI决策生成失败: {e}")
            return {
                "action": "hold",
                "confidence": 0,
                "reason": f"决策生成失败: {e}",
                "entry_price": None,
                "stop_loss": None,
                "target_price": None
            }


if __name__ == "__main__":
    # 测试代码
    print("🧪 AI舆情分析模块测试")
    print("="*60)
    
    analyzer = AIStockAnalyzer()
    
    # 列出策略
    print("\n📋 可用策略:")
    for s in analyzer.strategy_loader.list_strategies():
        strategy = analyzer.strategy_loader.get_strategy(s)
        print(f"  - {strategy['display_name']}: {strategy['description']}")
    
    print("\n✅ 模块加载成功")
    print("\n使用方式:")
    print("  1. 配置 TAVILY_API_KEY 或 SERPAPI_KEY（新闻搜索）")
    print("  2. 配置 OPENAI_API_KEY（AI分析）")
    print("  3. 调用 analyzer.analyze(stock_code, stock_name)")
