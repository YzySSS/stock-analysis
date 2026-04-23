#!/usr/bin/env python3
"""
股票分析项目 1.0
================
整合版本：V9选股 + V10+报告

核心功能：
- V9: 全A股，多因子评分，板块轮动增强
- V10+: 深度复盘，AI诊断，策略改进

定时任务：
- 8:50  盘前分析: V9选股 + V10+报告
- 12:30 盘中简报: 上午收盘总结 + 下午选股分析
- 15:50 盘后分析: V10+深度复盘
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# 版本信息
VERSION = "1.4"
VERSION_NAME = "V1.4-P2"

from datetime import datetime, timedelta
from typing import List, Dict
import logging
import time
import json
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# DeepSeek AI 分析模块
# ============================================================================

def analyze_with_deepseek(historical_data_text: str) -> str:
    """
    调用DeepSeek API分析盘前选股策略表现
    
    Args:
        historical_data_text: 历史选股表现数据文本
        
    Returns:
        DeepSeek分析结果
    """
    # 提示词模板
    prompt_template = '''
## 角色设定
你是一位资深的量化交易策略分析师，拥有10年以上A股量化策略开发经验，精通多因子选股模型、风险控制和策略优化。

## 任务
请分析以下股票盘前选股策略的表现数据，并给出专业的诊断报告和改进建议。

---

## 一、策略详情

### 策略名称
版本B (V10+P1+P2+舆情) - 7因子增强选股策略

### 因子配置 (总分100分)

| 因子 | 权重 | 计算方法 |
|------|------|---------|
| 技术 | 18分 | 价格形态、趋势强度、突破信号 |
| 情绪 | 5分 | 技术情绪(简化)，基于涨跌幅度 |
| 板块轮动 | 33分 ⭐ | RPS相对强度、板块动量、龙头识别 |
| 资金流向 | 19分 | 成交量、换手率、资金净流入 |
| 风险 | 15分 | 波动率、60日价格分位、回撤控制 |
| 一致预期 | 5分 | 业绩预期、机构评级 |
| 舆情 | 5分 | 新闻情绪分析(正面/负面/中性) |

### 核心机制

1. **市场强弱综合指数** (0-100分)
   - 趋势得分(40%): 基于大盘指数均线排列
   - 宽度得分(30%): 上涨股票占比
   - 成交量得分(20%): 量能变化
   - 情绪得分(10%): 涨跌比

2. **动态阈值机制**
   - 基础阈值: 60分
   - 动态调整: threshold = 60 + (60 - market_strength) × 0.2
   - 强势市场(>60): 阈值降至55-60分
   - 弱势市场(<40): 阈值提升至70-75分

3. **动态因子权重**
   - 强势市场: 进攻性因子×1.05, 风险因子×0.90
   - 弱势市场: 进攻性因子×0.95, 风险因子×1.20

4. **仓位管理**
   - position_ratio = min(100%, max(20%, market_strength / 50))

---

## 二、历史选股表现数据

{historical_data}

---

## 三、需要你回答的问题

### 1. 问题诊断
- 为什么准确率这么低？
- 为什么存在追涨问题（推荐时已涨0-5%）？
- 板块轮动因子权重33%是否合理？

### 2. 策略缺陷分析
- 当前7因子配置是否存在冗余或缺失？
- 权重分配是否合理？
- 动态阈值机制的实际效果如何？

### 3. 改进建议
请提供具体的、可落地的改进方案：
- 因子权重如何调整？
- 阈值机制如何优化？
- 如何有效避免追涨？

### 4. 风险控制
- 如何防范单日大亏？
- 是否需要增加个股层面的止损机制？

---

## 输出格式要求

请以结构化格式输出：

1. **执行摘要** (3-5条核心结论)
2. **详细诊断** (分点论述)
3. **改进方案** (分短期/中期/长期)
4. **具体参数建议** (如权重数值、阈值等)
5. **风险提示**

请用中文回答，语言专业但易懂。
'''
    
    prompt = prompt_template.format(historical_data=historical_data_text)
    
    # 获取API Key
    api_key = os.environ.get('DEEPSEEK_API_KEY', '')
    if not api_key:
        try:
            with open(os.path.join(os.path.dirname(__file__), '.env'), 'r') as f:
                for line in f:
                    if 'DEEPSEEK_API_KEY' in line and '=' in line:
                        api_key = line.split('=')[1].strip().strip('"').strip("'")
                        break
        except:
            pass
    
    if not api_key:
        return "⚠️ 未配置DeepSeek API Key，跳过AI分析"
    
    try:
        logger.info("正在调用DeepSeek API进行策略分析...")
        
        req = urllib.request.Request(
            'https://api.deepseek.com/v1/chat/completions',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {api_key}'
            },
            data=json.dumps({
                'model': 'deepseek-chat',
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0.7,
                'max_tokens': 3000
            }).encode('utf-8')
        )
        
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            analysis = result['choices'][0]['message']['content']
            logger.info("DeepSeek分析完成")
            return analysis
    
    except Exception as e:
        logger.error(f"DeepSeek API调用失败: {e}")
        return f"⚠️ DeepSeek分析失败: {str(e)[:100]}"


def format_picks_for_deepseek(picks: List[Dict], opening_prices: Dict = None) -> str:
    """
    格式化选股数据为DeepSeek分析的输入格式
    
    Args:
        picks: 选股列表
        opening_prices: 开盘价数据
        
    Returns:
        格式化后的文本
    """
    if not picks:
        return "无选股数据"
    
    lines = []
    
    # 按日期分组
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    
    lines.append(f"### 📅 {today} (今日)")
    lines.append("")
    
    # 计算统计数据
    total = len(picks)
    gains = []
    for pick in picks:
        ret = pick.get('actual_return', 0)
        gains.append(ret)
        status = "✅" if ret > 0 else "❌"
        lines.append(f"{status} {pick['name']}({pick['code']}): 推荐{pick.get('change_pct', 0):+.2f}% → 收盘{ret:+.2f}% | 评分:{pick['total_score']:.0f}")
    
    avg_return = sum(gains) / len(gains) if gains else 0
    correct = sum(1 for g in gains if g > 0)
    
    lines.append("")
    lines.append(f"**统计**: 选股{total}只, 正确{correct}只, 准确率{correct/total*100:.1f}%, 平均收益{avg_return:+.2f}%")
    lines.append("")
    
    return "\n".join(lines)


# ============================================================================
# V9 选股器 - 板块轮动增强版
# ============================================================================

class StockScreenerV9:
    """V9 板块轮动增强选股器 - 全A股版
    
    核心升级：
    1. 板块轮动因子权重提升（25% → 30%）
    2. 增加板块动量检测（3日/5日/10日趋势）
    3. 增加板块资金流向分析
    4. 强势板块龙头股识别
    """
    
    # 核心板块映射（用于板块轮动分析）
    SECTOR_MAP = {
        # 科技成长
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019'],
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661'],
        '新能源': ['002594', '300750', '601012', '603659', '300014', '600438'],
        '光伏': ['601012', '600438', '002129', '300274', '688599'],
        '储能': ['300274', '002594', '300014', '300207', '688063'],
        '5G通信': ['000063', '600498', '300502', '002281', '300136'],
        '云计算': ['000938', '300017', '600845', '300454', '603881'],
        
        # 大消费
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589'],
        '医药': ['600276', '000538', '300760', '603259', '600436', '300122'],
        '医疗器械': ['300760', '603658', '300003', '688617'],
        '创新药': ['600276', '000661', '300122', '688180', '688235'],
        '食品饮料': ['000895', '600887', '603288', '600519', '000568'],
        '家电': ['000333', '000651', '600690', '002032'],
        '汽车': ['002594', '601633', '601127', '000625', '600660'],
        
        # 大金融
        '银行': ['000001', '600036', '601398', '601318', '601288'],
        '券商': ['600030', '300059', '601688', '000776', '601211'],
        '保险': ['601318', '601628', '601601'],
        
        # 周期资源
        '有色金属': ['601899', '002460', '600547', '603993', '000878'],
        '煤炭': ['601088', '601225', '600188', '601699'],
        '钢铁': ['600019', '000932', '600507'],
        '化工': ['002092', '600309', '601216', '600352'],
        '石油': ['601857', '600028', '601808'],
        
        # 基建地产
        '房地产': ['000002', '600048', '001979', '600606'],
        '建筑': ['601668', '601390', '601669', '601800'],
        '建材': ['000786', '600585', '002271', '600801'],
        
        # 其他
        '军工': ['600893', '000768', '600760', '600372'],
        '传媒': ['002027', '300413', '600637', '002555'],
        '电力': ['600900', '600011', '600795', '601985'],
        '交运': ['601006', '600009', '601111', '601919'],
    }
    
    def __init__(self):
        self.sina = SinaDataProvider()
        self.analyzer = MultiFactorAnalyzerV10()  # P0+P1+P2优化：使用V10分析器
        self.market_timing = MarketTiming()  # P0新增：大盘择时
        self.rps_calculator = SectorRPSCalculator()  # P1新增：RPS计算
        self.hist_data_provider = HistoricalDataProvider()  # P1+P2新增：历史数据提供者
        self.hist_data_manager = HistoricalDataManager()  # P1保留：历史数据管理
        self.fund_flow_provider = FundFlowDataProvider()  # P2新增：资金流向
        self.consensus_provider = ConsensusDataProvider()  # P2新增：一致预期
        self.all_stocks = self._load_all_stocks()
        self.sector_performance = {}  # 板块表现缓存
        self.historical_data_cache = {}  # P0保留：历史数据缓存
        logger.info(f"V10+P2选股器初始化完成，股票池: {len(self.all_stocks)}只")
    
    def _load_all_stocks(self) -> List[str]:
        """加载全A股列表（5486只）"""
        # 尝试多个路径
        possible_paths = [
            os.getenv('STOCK_LIST_FILE'),  # 环境变量
            os.path.join(os.path.dirname(__file__), 'data', 'all_a_stocks.txt'),  # 项目本地
            os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt"),  # 默认路径
        ]
        
        for all_a_stocks_file in possible_paths:
            if all_a_stocks_file and os.path.exists(all_a_stocks_file):
                try:
                    with open(all_a_stocks_file, 'r', encoding='utf-8') as f:
                        stocks = [line.strip() for line in f if line.strip()]
                    logger.info(f"✅ 从文件加载全A股列表: {len(stocks)} 只")
                    return stocks
                except Exception as e:
                    logger.warning(f"⚠️ 读取全A股列表失败: {e}")
        
        # 回退到精简列表
        watchlist_file = os.getenv('STOCK_WATCHLIST_FILE', os.path.expanduser("~/.clawdbot/stock_watcher/watchlist.txt"))
        if os.path.exists(watchlist_file):
            try:
                with open(watchlist_file, 'r', encoding='utf-8') as f:
                    stocks = [line.strip().split('|')[0] for line in f if line.strip()]
                logger.info(f"📊 使用精简列表: {len(stocks)} 只")
                return stocks
            except Exception as e:
                logger.warning(f"⚠️ 读取精简列表失败: {e}")
        
        # 如果都不存在，使用默认核心股票
        logger.warning("⚠️ 未找到股票列表文件，使用默认核心股票")
        return [
            '000001', '000002', '000858', '002594', '300750', '600519', '601318', '601398', '601888', '603288'
        ]
    
    def screen(self, top_n: int = 3) -> tuple:
        """
        V10选股 - P0优化版

        Args:
            top_n: 选出前N只

        Returns:
            (选股结果列表, quotes数据字典)
        """
        logger.info(f"V10开始选股，目标: 前{top_n}只")

        # 1. 先获取行情（大盘择时需要）
        quotes = self._get_quotes()
        if not quotes:
            logger.error("获取行情失败")
            return [], {}
        
        # P0新增：Step 0 - 大盘择时分析（V2.0综合指数版）
        logger.info("Step 0: 大盘择时分析（综合指数版）")
        try:
            from sina_finance_api import SinaDataProvider
            sina = SinaDataProvider()
            index_quotes = sina.get_index_quotes()
        except:
            index_quotes = None
        
        market_context = self.market_timing.analyze(index_quotes=index_quotes, quotes=quotes)
        logger.info(f"  市场强弱指数: {market_context.get('market_strength', 50):.1f}/100")
        logger.info(f"  市场环境: {market_context.get('reason', '未知')}")
        logger.info(f"  动态阈值: {market_context.get('threshold', 60):.1f}分 (基础60±调整)")
        logger.info(f"  仓位比例: {market_context.get('position_ratio', 1.0)*100:.0f}%")
        
        # 显示因子权重调整
        multipliers = market_context.get('factor_multipliers', {})
        if multipliers.get('risk', 1.0) != 1.0:
            logger.info(f"  因子调整: 风险×{multipliers.get('risk', 1.0):.1f}")

        # 2. 增强板块分析 - V10+P1核心
        sectors = self._analyze_sectors_v10(quotes)
        logger.info(f"强势板块: {', '.join([s['name'] for s in sectors[:3]])}")
        for s in sectors[:3]:
            logger.info(f"  {s['name']}: 涨幅{s['change_pct']}% RPS{s['rps']} 得分{s['score']}")
        
        # 盘中选股阈值调整（比盘前更宽松）
        adjusted_threshold = max(55, market_context.get('threshold', 60) - 3)
        logger.info(f"  盘中选股阈值: {adjusted_threshold:.1f}分 (比盘前宽松3分)")

        # 3. 计算板块动量 - P1双周期
        sector_momentum = self._calc_sector_momentum_v10(sectors)
        
        # P1: 获取板块RPS数据
        sector_rps_data = getattr(self, '_sector_rps_cache', {})

        # P0新增：Step 3.5 - 预过滤（成交量结构）
        logger.info("Step 3.5: 成交量结构预过滤")
        filtered_quotes = {}
        for code, data in quotes.items():
            if self._pass_volume_filter(data, code):
                filtered_quotes[code] = data
        logger.info(f"  成交量过滤后: {len(filtered_quotes)}/{len(quotes)} 只")

        # Phase 2新增：Step 3.6 - 批量预计算舆情因子
        logger.info("Step 3.6: 批量预计算舆情因子")
        sentiment_factors = {}
        try:
            from sentiment_factor import get_sentiment_calculator
            sentiment_calc = get_sentiment_calculator()
            
            # 仅计算板块成分股（约200只），其他股票使用缓存或默认值
            sector_stocks = set()
            for sector_codes in self.SECTOR_MAP.values():
                sector_stocks.update(sector_codes)
            
            # 筛选出在行情中的板块股
            stock_list = [(code, filtered_quotes[code].get('name', code)) 
                         for code in sector_stocks if code in filtered_quotes]
            
            logger.info(f"  计算板块成分股舆情: {len(stock_list)} 只")
            
            # 批量计算（仅缓存模式，更快）
            sentiment_factors = sentiment_calc.batch_calculate(
                stock_list, progress_interval=50, use_cache_only=True
            )
            logger.info(f"  舆情因子计算完成: {len(sentiment_factors)} 只")
        except Exception as e:
            logger.warning(f"  舆情因子批量计算失败: {e}，将在评分时实时计算")
            sentiment_factors = {}

        # 4. 计算个股评分并过滤
        results = []
        for code, data in filtered_quotes.items():
            name = data.get('name', code)

            # 过滤无法买入的股票
            if self._is_untradable(name, code):
                continue

            # P0+P1优化：获取历史数据用于技术指标和风险计算
            historical_prices = self._get_historical_prices(code)
            
            # 保存到历史数据管理器
            if historical_prices:
                self.hist_data_manager.price_cache[f"{code}_60"] = historical_prices

            # 确保 data 中有 sector 字段（用于板块因子计算）
            if 'sector' not in data or data.get('sector') == '其他':
                data['sector'] = self._identify_sector(code)

            # V10+P1+Phase2：使用优化后的多因子分析（含舆情因子）
            # 检查是否为盘前模式（通过环境变量）
            premarket_mode = os.getenv('PREMARKET_MODE', '0') == '1'
            score = self.analyzer.calculate_score(
                data, sectors, sector_momentum, 
                historical_prices, market_context,
                sector_rps_data,  # P1新增：RPS数据
                sentiment_factors.get(code),  # Phase 2新增：舆情因子
                premarket_mode=premarket_mode  # 盘前模式
            )
            
            # 应用动态阈值
            threshold = score.get('threshold', 60)
            
            if score['total'] >= threshold:
                # 识别股票所属板块
                try:
                    from stock_sector import get_stock_sector
                    stock_sector = get_stock_sector(code)
                except:
                    stock_sector = self._identify_sector(code)
                results.append({
                    'code': code,
                    'name': name,
                    'price': data.get('price', 0),
                    'change_pct': data.get('change_pct', 0),
                    'sector': stock_sector,
                    'total_score': score['total'],
                    'factors': score,
                    'threshold': threshold,  # 记录阈值用于显示
                    'position_ratio': score.get('position_ratio', 1.0),  # 仓位比例
                    'reason': score.get('reason', '综合评分优异'),
                    'is_sector_leader': self._is_sector_leader(code, data, sectors),
                    'market_note': score.get('market_note', '')
                })

        # 5. 排序并返回Top3（仓位管理由外部根据position_ratio决定）
        results.sort(key=lambda x: (x['total_score'], x['is_sector_leader']), reverse=True)
        top_stocks = results[:3]  # 固定取前3只，仓位比例在报告中体现

        # 记录市场状态信息
        position_ratio = market_context.get('position_ratio', 1.0) if market_context else 1.0
        logger.info(f"V10选股完成，市场强度{market_context.get('market_strength', 50):.0f}，仓位比例{position_ratio*100:.0f}%")
        logger.info(f"  选出 {len(top_stocks)} 只")
        for s in top_stocks:
            leader_tag = " [板块龙头]" if s.get('is_sector_leader') else ""
            logger.info(f"  {s['name']}({s['code']}): {s['total_score']}分 (阈值{s['threshold']:.0f}){leader_tag}")

        return top_stocks, quotes
    
    def _is_untradable(self, name: str, code: str) -> bool:
        """
        检查股票是否无法买入
        
        过滤条件:
        - ST/*ST 股
        - 退市股 (名称含"退")
        - 停牌股 (价格为0或涨跌幅异常)
        - 北交所 (43/83/87开头，流动性差)
        - 新股 (N开头)
        """
        if not name:
            return True
        
        # 1. ST/*ST 股
        if name.startswith('ST') or name.startswith('*ST') or 'ST' in name:
            return True
        
        # 2. 退市股
        if '退' in name or name.startswith('退市'):
            return True
        
        # 3. 新股 (N开头)
        if name.startswith('N'):
            return True
        
        # 4. 北交所 (流动性差，暂不推荐)
        if code.startswith('43') or code.startswith('83') or code.startswith('87'):
            return True
        
        # 5. B股 (900/200开头)
        if code.startswith('900') or code.startswith('200'):
            return True
        
        return False
    
    def _pass_volume_filter(self, data: Dict, code: str) -> bool:
        """
        P0新增：成交量结构过滤（测试版放宽标准）
        """
        turnover = data.get('turnover', 0)
        volume = data.get('volume', 0)
        price = data.get('price', 0)
        
        # 如果没有成交额数据，尝试用成交量*价格估算
        if turnover == 0 and volume > 0 and price > 0:
            turnover = volume * price
        
        # 测试版：如果没有成交额数据，直接通过
        if turnover == 0:
            return True
        
        # 根据板块设置不同标准（测试版放宽）
        if code.startswith(('00', '60')):
            min_turnover = 1e7  # 1000万（测试版）
        else:
            min_turnover = 5e6  # 500万（测试版）
        
        if turnover < min_turnover:
            return False
        
        return True
    
    def _get_historical_prices(self, code: str) -> List[float]:
        """
        P1+P2：获取历史价格数据（优先从数据库，无数据则跳过）
        
        用于计算：
        - 技术指标（MA/RSI/突破）
        - 风险指标（回撤/历史分位）
        
        优化：只从数据库读取，不触发API调用（避免超时）
        """
        # 检查内存缓存
        if code in self.historical_data_cache:
            return self.historical_data_cache[code]
        
        # 只从数据库获取（板块成分股）
        try:
            from stock_history_db import get_stock_history_db
            db = get_stock_history_db()
            prices = db.get_prices(code, days=60)
            if prices and len(prices) >= 20:  # 至少20天数据
                self.historical_data_cache[code] = prices
                logger.debug(f"✅ 从数据库获取 {code} 历史数据: {len(prices)} 天")
                return prices
        except Exception as e:
            logger.debug(f"数据库获取失败 {code}: {e}")
        
        # 数据库没有，返回空列表（使用简化计算）
        return []
    
    def _get_quotes(self) -> Dict:
        """获取实时行情（多数据源备份）"""
        quotes = {}
        
        # 0. 尝试东方财富（通过代理或直连）
        try:
            from src.eastmoney_datasource import get_a_stock_spot
            stocks = get_a_stock_spot()
            quotes = {s.code: {
                'name': s.name,
                'price': s.price,
                'change_pct': s.change_percent,
                'sector': '其他'
            } for s in stocks if s.code in self.all_stocks}
            if quotes:
                logger.info(f"✅ 东方财富 获取行情成功: {len(quotes)}只")
                return quotes
        except Exception as e:
            logger.warning(f"⚠️ 东方财富失败: {e}")
        
        # 1. 尝试 AkShare
        try:
            from data_source import data_manager
            source = data_manager.get_source()
            if source:
                stocks = source.get_a_stock_spot()
                quotes = {s.code: {
                    'name': s.name,
                    'price': s.price,
                    'change_pct': s.change_percent,
                    'sector': '其他'
                } for s in stocks if s.code in self.all_stocks}
                if quotes:
                    logger.info(f"✅ AkShare 获取行情成功: {len(quotes)}只")
                    return quotes
        except Exception as e:
            logger.warning(f"⚠️ AkShare 失败: {e}")
        
        # 2. 尝试聚宽 JoinQuant（优先专业数据源）
        try:
            from jqdatasdk import auth, get_price
            import os
            jq_user = os.getenv('JQ_USERNAME', '13929962527')
            jq_pass = os.getenv('JQ_PASSWORD', 'Zy20001026')
            auth(jq_user, jq_pass)
            
            quotes = {}
            for code in self.all_stocks[:50]:  # 聚宽限制，先取前50
                try:
                    jq_code = f"{code}.XSHE" if code.startswith(('00', '30')) else f"{code}.XSHG"
                    df = get_price(jq_code, frequency='1m', count=1)
                    if not df.empty:
                        price = float(df['close'].iloc[-1])
                        pre_close = float(df['pre_close'].iloc[-1]) if 'pre_close' in df.columns else price
                        change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                        quotes[code] = {
                            'name': code,
                            'price': price,
                            'change_pct': change_pct,
                            'sector': '其他'
                        }
                except:
                    continue
            
            if quotes:
                logger.info(f"✅ 聚宽 JoinQuant 获取行情成功: {len(quotes)}只")
                return quotes
        except Exception as e:
            logger.warning(f"⚠️ 聚宽失败: {e}")
        
        # 3. 尝试腾讯财经（备用）- 分批获取
        try:
            from tencent_datasource import TencentDataSource
            tencent = TencentDataSource()
            # 分批获取全A股，腾讯接口一次最多支持约800只
            batch_size = 800
            all_quotes = {}
            stocks_to_fetch = self.all_stocks  # 获取全部A股
            
            for i in range(0, len(stocks_to_fetch), batch_size):
                batch = stocks_to_fetch[i:i+batch_size]
                try:
                    df = tencent.get_realtime_quotes(batch)
                    if not df.empty:
                        for _, row in df.iterrows():
                            code = str(row['code']).zfill(6)
                            if code in self.all_stocks:
                                all_quotes[code] = {
                                    'name': row['name'],
                                    'price': row['price'],
                                    'change_pct': row.get('change_percent', 0),
                                    'sector': '其他'
                                }
                        logger.info(f"  批次 {i//batch_size + 1}: 获取 {len(df)} 只")
                except Exception as batch_e:
                    logger.warning(f"  批次 {i//batch_size + 1} 失败: {batch_e}")
                time.sleep(0.3)  # 避免请求过快
            
            if all_quotes:
                logger.info(f"✅ 腾讯财经 获取行情成功: {len(all_quotes)}只")
                return all_quotes
        except Exception as e:
            logger.warning(f"⚠️ 腾讯财经失败: {e}")
        
        # 4. 尝试新浪财经（备用2）
        try:
            from sina_finance_api import SinaFinanceAPI
            sina = SinaFinanceAPI()
            # 分批获取全A股，新浪接口一次最多支持约800只
            batch_size = 800
            all_quotes = {}
            for i in range(0, len(self.all_stocks), batch_size):  # 获取全部A股
                batch = self.all_stocks[i:i+batch_size]
                df = sina.get_realtime_quotes(batch)
                if not df.empty:
                    for _, row in df.iterrows():
                        code = str(row['code']).zfill(6)
                        if code in self.all_stocks:
                            all_quotes[code] = {
                                'name': row.get('name', code),
                                'price': row.get('price', 0),
                                'change_pct': row.get('change_percent', 0),
                                'sector': '其他'
                            }
                time.sleep(0.5)  # 避免请求过快
            if all_quotes:
                logger.info(f"✅ 新浪财经 获取行情成功: {len(all_quotes)}只")
                return all_quotes
        except Exception as e:
            logger.warning(f"⚠️ 新浪财经失败: {e}")
        
        logger.error("❌ 所有数据源都失败")
        return {}
    
    def _analyze_sectors_v10(self, quotes: Dict) -> List[Dict]:
        """
        V10+P1板块轮动分析 - 深化版
        
        P1优化：
        1. 双周期动量（5日/20日）
        2. 板块RPS排名
        3. 成交额占比变化
        """
        sector_stats = {}
        
        # 初始化所有板块
        for sector_name, codes in self.SECTOR_MAP.items():
            sector_stats[sector_name] = {
                'codes': codes,
                'total_change': 0,
                'total_volume': 0,
                'count': 0,
                'up_count': 0,
                'limit_up_count': 0,
                'turnover': 0
            }
        
        # 统计板块内股票表现
        total_market_turnover = 0
        for code, data in quotes.items():
            change_pct = data.get('change_pct', 0)
            volume = data.get('volume', 0)
            price = data.get('price', 0)
            turnover = data.get('turnover', volume * price)
            total_market_turnover += turnover
            
            # 找到股票所属板块
            for sector_name, codes in self.SECTOR_MAP.items():
                if code in codes:
                    sector_stats[sector_name]['total_change'] += change_pct
                    sector_stats[sector_name]['total_volume'] += volume
                    sector_stats[sector_name]['turnover'] += turnover
                    sector_stats[sector_name]['count'] += 1
                    if change_pct > 0:
                        sector_stats[sector_name]['up_count'] += 1
                    if change_pct >= 9.5:
                        sector_stats[sector_name]['limit_up_count'] += 1
                    break
        
        # P1: 计算板块RPS
        sector_rps_data = self.rps_calculator.calculate_all_sectors_rps(
            self.SECTOR_MAP, quotes
        )
        
        # 计算板块得分
        sector_list = []
        for name, stats in sector_stats.items():
            if stats['count'] == 0:
                continue
            
            avg_change = stats['total_change'] / stats['count']
            up_ratio = stats['up_count'] / stats['count'] if stats['count'] > 0 else 0
            
            # P1: 获取RPS数据
            rps_info = sector_rps_data.get(name, {'rps_5': 50, 'rps_20': 50, 'composite_rps': 50})
            composite_rps = rps_info.get('composite_rps', 50)
            
            # P1: 计算成交额占比
            turnover_ratio = (stats['turnover'] / total_market_turnover * 100) if total_market_turnover > 0 else 0
            
            # V10+P1板块评分公式（考虑RPS）
            score = 40  # 降低基础分
            
            # 1. RPS得分 (0-25分) - P1新增
            if composite_rps >= 90:
                score += 25
            elif composite_rps >= 70:
                score += 20
            elif composite_rps >= 50:
                score += 15
            elif composite_rps >= 30:
                score += 8
            
            # 2. 涨幅得分 (0-20分) - 权重降低
            if avg_change > 3:
                score += 20
            elif avg_change > 2:
                score += 15
            elif avg_change > 1:
                score += 10
            elif avg_change > 0:
                score += 5
            else:
                score -= 10
            
            # 3. 上涨家数占比得分 (0-10分)
            score += up_ratio * 10
            
            # 4. 涨停数量加分 (每只+2分)
            score += stats['limit_up_count'] * 2
            
            # 5. 成交额占比加分 (0-5分) - P1新增
            if turnover_ratio > 5:
                score += 5
            elif turnover_ratio > 3:
                score += 3
            elif turnover_ratio > 1:
                score += 1
            
            sector_list.append({
                'name': name,
                'change_pct': round(avg_change, 2),
                'stock_count': stats['count'],
                'up_count': stats['up_count'],
                'limit_up_count': stats['limit_up_count'],
                'up_ratio': round(up_ratio * 100, 1),
                'score': min(100, int(score)),
                'rps': round(composite_rps, 1),
                'turnover_ratio': round(turnover_ratio, 2),
                'trend': '强势' if avg_change > 2 else '活跃' if avg_change > 0 else '弱势'
            })
        
        sector_list.sort(key=lambda x: x['score'], reverse=True)
        
        # 保存RPS数据供后续使用
        self._sector_rps_cache = sector_rps_data
        
        return sector_list
    
    def _calc_sector_momentum_v10(self, sectors: List[Dict]) -> Dict:
        """
        P1: 双周期板块动量计算
        
        动量 = 5日动量 * 0.4 + 20日动量 * 0.6
        简化版：使用当日涨幅作为代理
        """
        momentum = {}
        for sector in sectors[:10]:
            # 简化计算：当日涨幅作为动量代理
            # 实际应该使用5日和20日涨幅
            daily_change = sector.get('change_pct', 0)
            rps = sector.get('rps', 50)
            
            # 综合动量 = 当日涨幅 * 0.5 + RPS动量 * 0.5
            momentum[sector['name']] = daily_change * 0.5 + (rps - 50) * 0.1
            
        return momentum
    
    def _calc_sector_momentum(self, sectors: List[Dict]) -> Dict:
        """计算板块动量 - V9新增"""
        momentum = {}
        for sector in sectors[:10]:  # 只计算前10板块
            # 动量 = 涨幅 * 上涨家数占比
            momentum[sector['name']] = sector['change_pct'] * (sector['up_ratio'] / 100)
        return momentum
    
    def _identify_sector(self, code: str) -> str:
        """识别股票所属板块"""
        for sector_name, codes in self.SECTOR_MAP.items():
            if code in codes:
                return sector_name
        return '其他'
    
    def _is_sector_leader(self, code: str, data: Dict, sectors: List[Dict]) -> bool:
        """判断是否为板块龙头 - V9新增"""
        if not sectors:
            return False
        
        stock_sector = self._identify_sector(code)
        change_pct = data.get('change_pct', 0)
        
        # 找到所属板块
        for sector in sectors:
            if sector['name'] == stock_sector:
                # 如果涨幅超过板块平均的1.5倍，认为是龙头
                if sector['change_pct'] > 0 and change_pct > sector['change_pct'] * 1.5:
                    return True
                # 或者涨幅排名板块前三
                if change_pct > 5 and sector['change_pct'] > 1:
                    return True
        
        return False


class HistoricalDataProvider:
    """
    历史数据提供者 - 多源备份（Baostock/聚宽/AkShare）
    
    功能：
    1. 获取个股历史价格（用于计算MA/RSI/回撤）
    2. 获取板块历史数据（用于计算RPS）
    3. 本地缓存，避免重复请求
    
    数据源优先级（回测用）：
    1. Baostock（首选，稳定免费，数据到2026年）
    2. 聚宽JoinQuant（备选，数据到2025-12-20，有财务数据）
    3. AkShare（备选，数据全但可能限流）
    
    数据源对比：
    - Baostock: 免费、数据新(2026)、无财务数据
    - 聚宽: 免费、数据到2025-12、有PE/PB/行业分类
    - AkShare: 免费、数据最新、可能限流
    """
    
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(__file__), 'data_cache')
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self.price_cache = {}  # 内存缓存
        self.baostock_login = False
        
    def _login_baostock(self) -> bool:
        """登录Baostock"""
        if self.baostock_login:
            return True
        try:
            import baostock as bs
            result = bs.login()
            if result.error_code == '0':
                self.baostock_login = True
                return True
        except Exception as e:
            logger.warning(f"Baostock登录失败: {e}")
        return False
    
    def _logout_baostock(self):
        """登出Baostock"""
        if self.baostock_login:
            try:
                import baostock as bs
                bs.logout()
                self.baostock_login = False
            except:
                pass
    
    def get_stock_history(self, code: str, days: int = 60) -> List[float]:
        """
        获取个股历史收盘价
        
        Args:
            code: 股票代码 (如 '000001')
            days: 获取天数（默认60天）
        
        Returns:
            收盘价列表， oldest first
        """
        # 检查内存缓存
        cache_key = f"{code}_{days}"
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        # 检查文件缓存
        cache_file = os.path.join(self.cache_dir, f"{code}_hist.json")
        if os.path.exists(cache_file):
            try:
                import json
                with open(cache_file, 'r') as f:
                    cached = json.load(f)
                    # 检查缓存是否新鲜（当天）
                    cache_date = cached.get('date', '')
                    if cache_date == datetime.now().strftime('%Y-%m-%d'):
                        prices = cached.get('prices', [])[-days:]
                        self.price_cache[cache_key] = prices
                        return prices
            except Exception as e:
                logger.warning(f"读取缓存失败 {code}: {e}")
        
        # 1. 从Baostock获取（首选，数据最新到2026年）
        prices = self._fetch_from_baostock(code, days)
        
        # 2. 尝试聚宽JoinQuant（备选，有财务数据，数据到2025-12-20）
        if not prices:
            prices = self._fetch_from_joinquant(code, days)
        
        # 3. 尝试AkShare（备选）
        if not prices:
            prices = self._fetch_from_akshare(code, days)
        
        # 缓存结果
        if prices:
            self.price_cache[cache_key] = prices
            self._save_cache(code, prices)
        
        return prices
    
    def _fetch_from_baostock(self, code: str, days: int) -> List[float]:
        """从Baostock获取历史数据"""
        if not self._login_baostock():
            return []
        
        try:
            import baostock as bs
            
            # 转换代码格式
            if code.startswith(('00', '30')):
                bs_code = f"sz.{code}"
            else:
                bs_code = f"sh.{code}"
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,close',
                start_date=start_date,
                end_date=end_date,
                frequency='d',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code != '0':
                return []
            
            prices = []
            while rs.next():
                close_price = rs.get_row_data()[1]
                if close_price:
                    prices.append(float(close_price))
            
            return prices[-days:] if len(prices) > days else prices
            
        except Exception as e:
            logger.warning(f"Baostock获取失败 {code}: {e}")
            return []
    
    def _fetch_from_joinquant(self, code: str, days: int) -> List[float]:
        """从聚宽JoinQuant获取历史数据（备选，有财务数据）"""
        try:
            import jqdatasdk as jq
            
            # 登录（使用已保存的凭据）
            jq.auth('13929962527', 'Zy20001026')
            
            # 转换代码格式
            if code.startswith(('00', '30', '39')):
                jq_code = f"{code}.XSHE"  # 深圳
            else:
                jq_code = f"{code}.XSHG"  # 上海
            
            # 聚宽数据截止到2025-12-20
            end_date = min(datetime.now(), datetime(2025, 12, 20))
            
            df = jq.get_price(
                jq_code,
                count=days,
                end_date=end_date.strftime('%Y-%m-%d'),
                frequency='daily'
            )
            
            if df.empty:
                return []
            
            prices = df['close'].tolist()
            logger.info(f"✅ 聚宽获取 {code}: {len(prices)} 天")
            return prices
            
        except Exception as e:
            logger.warning(f"聚宽获取失败 {code}: {e}")
            return []
    
    def _fetch_from_akshare(self, code: str, days: int) -> List[float]:
        """从AkShare获取历史数据（备选）"""
        try:
            import akshare as ak
            
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_date,
                end_date=end_date,
                adjust='qfq'
            )
            
            if df.empty:
                return []
            
            prices = df['收盘'].tolist()
            return prices[-days:] if len(prices) > days else prices
            
        except Exception as e:
            logger.warning(f"AkShare获取失败 {code}: {e}")
            return []
    
    def _save_cache(self, code: str, prices: List[float]):
        """保存到本地缓存"""
        cache_file = os.path.join(self.cache_dir, f"{code}_hist.json")
        try:
            import json
            with open(cache_file, 'w') as f:
                json.dump({
                    'code': code,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'prices': prices
                }, f)
        except Exception as e:
            logger.warning(f"保存缓存失败 {code}: {e}")
    
    def get_batch_history(self, codes: List[str], days: int = 60) -> Dict[str, List[float]]:
        """批量获取历史数据"""
        results = {}
        for code in codes:
            prices = self.get_stock_history(code, days)
            if prices:
                results[code] = prices
        return results
    
    def clear_cache(self):
        """清理过期缓存"""
        try:
            import json
            today = datetime.now().strftime('%Y-%m-%d')
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('_hist.json'):
                    filepath = os.path.join(self.cache_dir, filename)
                    try:
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                        if data.get('date') != today:
                            os.remove(filepath)
                    except:
                        pass
        except Exception as e:
            logger.warning(f"清理缓存失败: {e}")

    # ==================== 聚宽特有功能（回测用）====================

    def get_fundamentals_jq(self, code: str, date: str = None) -> Dict:
        """
        从聚宽获取基本面数据（回测用）

        Args:
            code: 股票代码
            date: 查询日期（默认2025-12-20之前）

        Returns:
            {
                'pe': float,      # 市盈率
                'pb': float,      # 市净率
                'ps': float,      # 市销率
                'market_cap': float,  # 总市值（亿）
                'industry': str   # 行业分类
            }
        """
        try:
            import jqdatasdk as jq
            jq.auth('13929962527', 'Zy20001026')

            if date is None:
                date = '2025-12-20'  # 聚宽数据截止日期

            # 转换代码格式
            if code.startswith(('00', '30', '39')):
                jq_code = f"{code}.XSHE"
            else:
                jq_code = f"{code}.XSHG"

            # 获取估值数据
            df = jq.get_valuation(jq_code, count=1, end_date=date)
            if df.empty:
                return {}

            # 获取行业分类
            industry = jq.get_industry(jq_code, date=date)

            return {
                'pe': float(df['pe_ratio'].iloc[-1]) if 'pe_ratio' in df.columns else None,
                'pb': float(df['pb_ratio'].iloc[-1]) if 'pb_ratio' in df.columns else None,
                'ps': float(df['ps_ratio'].iloc[-1]) if 'ps_ratio' in df.columns else None,
                'market_cap': float(df['market_cap'].iloc[-1]) / 1e8 if 'market_cap' in df.columns else None,  # 转换为亿
                'industry': industry.get(jq_code, {}).get('sw_l1', {}).get('industry_name', '未知') if industry else '未知',
                'date': date
            }

        except Exception as e:
            logger.warning(f"聚宽基本面数据获取失败 {code}: {e}")
            return {}

    def get_index_data_jq(self, index_code: str = '000300', days: int = 60) -> List[Dict]:
        """
        从聚宽获取指数历史数据（回测用）

        Args:
            index_code: 指数代码（000300=沪深300）
            days: 天数

        Returns:
            [{'date': str, 'close': float, 'volume': float}, ...]
        """
        try:
            import jqdatasdk as jq
            jq.auth('13929962527', 'Zy20001026')

            end_date = min(datetime.now(), datetime(2025, 12, 20))
            jq_code = f"{index_code}.XSHG"

            df = jq.get_price(jq_code, count=days, end_date=end_date.strftime('%Y-%m-%d'), frequency='daily')

            if df.empty:
                return []

            return [
                {
                    'date': str(idx),
                    'close': float(row['close']),
                    'volume': float(row.get('volume', 0))
                }
                for idx, row in df.iterrows()
            ]

        except Exception as e:
            logger.warning(f"聚宽指数数据获取失败 {index_code}: {e}")
            return []


class FundFlowDataProvider:
    """
    P2: 资金流向数据提供者
    
    数据来源（按优先级）：
    1. 东方财富 - 主力净流入、大单数据
    2. 同花顺 - 资金流向、北向资金
    3. 腾讯/新浪财经 - 基础资金流向
    
    当前实现：简化版，基于量价关系估算
    """
    
    def __init__(self):
        self.cache = {}
        self.cache_time = 300  # 缓存5分钟
    
    def get_fund_flow(self, code: str, quotes_data: Dict = None) -> Dict:
        """
        获取资金流向数据（简化版）
        
        Returns:
            {
                'main_inflow_pct': float,  # 主力净流入占比
                'large_order_pct': float,  # 大单买入占比
                'north_bound': bool,       # 北向资金净流入
                'retail_outflow': bool     # 散户净流出（估算）
            }
        """
        # 检查缓存
        if code in self.cache:
            cache_entry = self.cache[code]
            if time.time() - cache_entry['time'] < self.cache_time:
                return cache_entry['data']
        
        # 简化版：基于量价关系估算资金流向
        result = self._estimate_fund_flow(code, quotes_data)
        
        # 缓存结果
        self.cache[code] = {
            'data': result,
            'time': time.time()
        }
        
        return result
    
    def _estimate_fund_flow(self, code: str, quotes_data: Dict = None) -> Dict:
        """
        基于量价关系估算资金流向（P2简化实现）
        
        实际应该调用东方财富/同花顺API
        """
        if not quotes_data or code not in quotes_data:
            return {
                'main_inflow_pct': 0,
                'large_order_pct': 50,
                'north_bound': False,
                'retail_outflow': False,
                'is_estimated': True
            }
        
        data = quotes_data[code]
        change_pct = data.get('change_pct', 0)
        volume = data.get('volume', 0)
        turnover = data.get('turnover', 0)
        
        # 估算逻辑：
        # 1. 大涨+放量 = 主力流入
        # 2. 上涨+缩量 = 散户行情
        # 3. 下跌+放量 = 主力流出
        
        main_inflow_pct = 0
        large_order_pct = 50
        
        if change_pct > 3 and volume > 1e6:
            # 大涨放量，估算主力流入10%
            main_inflow_pct = 10
            large_order_pct = 65
        elif change_pct > 1 and volume > 5e5:
            # 小涨放量，估算主力流入5%
            main_inflow_pct = 5
            large_order_pct = 58
        elif change_pct < -2 and volume > 1e6:
            # 大跌放量，估算主力流出
            main_inflow_pct = -8
            large_order_pct = 40
        
        return {
            'main_inflow_pct': main_inflow_pct,
            'large_order_pct': large_order_pct,
            'north_bound': change_pct > 2,  # 简化：大涨假设有北向参与
            'retail_outflow': change_pct > 5 and volume < 5e5,  # 缩量大涨可能是散户减少
            'is_estimated': True
        }
    
    def get_consecutive_north_bound(self, code: str, days: int = 3) -> bool:
        """检查北向资金是否连续N日净流入（需要历史数据）"""
        # P2简化：返回随机结果（实际应该查历史）
        return False


class ConsensusDataProvider:
    """
    P2: 一致性预期数据提供者
    
    数据来源：
    1. 机构调研数据
    2. 券商评级/目标价
    3. 业绩预告/快报
    
    当前实现：框架，实际数据需要付费API
    """
    
    def __init__(self):
        self.cache = {}
    
    def get_consensus_data(self, code: str) -> Dict:
        """
        获取一致性预期数据
        
        Returns:
            {
                'has_research': bool,      # 近期有机构调研
                'rating_upgrade': bool,    # 评级上调
                'target_raise': float,     # 目标价上调幅度
                'earnings_beat': bool      # 业绩超预期
            }
        """
        # P2框架：返回默认值（实际应该调用API）
        return {
            'has_research': False,
            'rating_upgrade': False,
            'target_raise': 0,
            'earnings_beat': False,
            'data_available': False  # 标记数据是否真实可用
        }
    
    def get_institutional_research(self, code: str, days: int = 30) -> List[Dict]:
        """获取机构调研记录"""
        return []  # 框架实现
    
    def get_analyst_ratings(self, code: str) -> Dict:
        """获取券商分析师评级"""
        return {'buy': 0, 'hold': 0, 'sell': 0}  # 框架实现


class SectorRPSCalculator:
    """板块RPS计算工具类 - P1新增
    
    RPS (Relative Price Strength): 相对价格强度
    计算方法：某板块在N日内的涨幅排名百分比
    """
    
    def __init__(self):
        self.sector_historical_data = {}  # 板块历史数据缓存
        self.stock_historical_data = {}   # 个股历史数据缓存
    
    def calculate_sector_rps(self, sector_name: str, sector_codes: List[str],
                             all_stocks_quotes: Dict, period: int = 5) -> float:
        """
        计算单个板块的RPS
        
        Args:
            sector_name: 板块名称
            sector_codes: 板块内股票代码列表
            all_stocks_quotes: 全市场股票行情
            period: 计算周期（5日或20日）
        
        Returns:
            RPS值 0-100，数值越大表示越强
        """
        # 计算板块内股票的平均涨幅
        sector_changes = []
        for code in sector_codes:
            if code in all_stocks_quotes:
                change_pct = all_stocks_quotes[code].get('change_pct', 0)
                sector_changes.append(change_pct)
        
        if not sector_changes:
            return 50  # 默认值
        
        sector_avg_change = sum(sector_changes) / len(sector_changes)
        
        # 计算所有板块的平均涨幅
        all_sector_changes = []
        for code, data in all_stocks_quotes.items():
            all_sector_changes.append(data.get('change_pct', 0))
        
        if not all_sector_changes:
            return 50
        
        # 计算RPS排名
        # 简化版：统计板块涨幅超过多少百分比的股票
        count_stronger = sum(1 for c in all_sector_changes if c > sector_avg_change)
        rps = (1 - count_stronger / len(all_sector_changes)) * 100
        
        return rps
    
    def calculate_all_sectors_rps(self, sector_map: Dict, 
                                  all_stocks_quotes: Dict) -> Dict[str, Dict]:
        """
        计算所有板块的RPS
        
        Returns:
            {
                '板块名': {
                    'rps_5': float,   # 5日RPS
                    'rps_20': float,  # 20日RPS（简化用当日）
                    'composite_rps': float,  # 综合RPS
                    'turnover_change': float  # 成交额占比变化
                }
            }
        """
        result = {}
        
        for sector_name, codes in sector_map.items():
            # 5日RPS（简化用当日数据）
            rps_5 = self.calculate_sector_rps(sector_name, codes, all_stocks_quotes, 5)
            
            # 20日RPS（简化用当日数据，实际应该用20日涨幅）
            rps_20 = rps_5  # 简化处理
            
            # 综合RPS = RPS(5) * 0.3 + RPS(20) * 0.5 + 成交额变化 * 0.2
            composite_rps = rps_5 * 0.3 + rps_20 * 0.5 + 50 * 0.2  # 成交额变化默认50
            
            result[sector_name] = {
                'rps_5': rps_5,
                'rps_20': rps_20,
                'composite_rps': composite_rps,
                'turnover_change': 50  # 默认值
            }
        
        return result
    
    def get_sector_rps_rank(self, sector_name: str, all_rps: Dict) -> int:
        """获取板块RPS排名"""
        sorted_sectors = sorted(all_rps.items(), 
                               key=lambda x: x[1]['composite_rps'], 
                               reverse=True)
        for i, (name, _) in enumerate(sorted_sectors, 1):
            if name == sector_name:
                return i
        return len(sorted_sectors)
    
    def is_top_rps_sector(self, sector_name: str, all_rps: Dict, percentile: float = 0.1) -> bool:
        """判断板块是否处于RPS前N%"""
        rank = self.get_sector_rps_rank(sector_name, all_rps)
        total = len(all_rps)
        return rank <= total * percentile


class HistoricalDataManager:
    """历史数据管理器 - P1新增
    
    管理股票和板块的历史价格数据，用于计算：
    - 多周期动量
    - 最大回撤
    - 历史分位
    """
    
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(__file__), 'data_cache')
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.price_cache = {}  # 内存缓存
    
    def get_historical_prices(self, code: str, days: int = 60) -> List[float]:
        """获取股票历史价格（尝试从缓存或文件，默认60天）"""
        cache_key = f"{code}_{days}"
        
        # 先查内存缓存
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        # 尝试从文件加载
        cache_file = os.path.join(self.cache_dir, f"{code}_prices.json")
        if os.path.exists(cache_file):
            try:
                import json
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    prices = data.get('prices', [])[-days:]
                    self.price_cache[cache_key] = prices
                    return prices
            except Exception as e:
                logger.warning(f"加载历史数据失败 {code}: {e}")
        
        return []  # 返回空列表表示无数据
    
    def save_historical_prices(self, code: str, prices: List[float]):
        """保存历史价格到文件"""
        cache_file = os.path.join(self.cache_dir, f"{code}_prices.json")
        try:
            import json
            with open(cache_file, 'w') as f:
                json.dump({'prices': prices, 'updated': datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"保存历史数据失败 {code}: {e}")
    
    def calculate_max_drawdown(self, prices: List[float], period: int = 20) -> float:
        """计算N日最大回撤"""
        if len(prices) < period:
            return 0
        
        recent_prices = prices[-period:]
        max_price = max(recent_prices)
        current_price = recent_prices[-1]
        
        if max_price <= 0:
            return 0
        
        drawdown = (max_price - current_price) / max_price * 100
        return drawdown
    
    def calculate_price_percentile(self, prices: List[float], 
                                    period: int = 60) -> float:
        """计算当前价格在N日内的分位（0-100）"""
        if len(prices) < period:
            return 50  # 默认中间值
        
        recent_prices = prices[-period:]
        current_price = recent_prices[-1]
        
        # 排序后找位置
        sorted_prices = sorted(recent_prices)
        position = sorted_prices.index(current_price)
        percentile = position / len(sorted_prices) * 100
        
        return percentile


class TechnicalIndicators:
    """技术指标计算工具类 - P0新增"""
    
    @staticmethod
    def calculate_ma(prices: List[float], period: int) -> float:
        """计算移动平均线"""
        if len(prices) < period:
            return prices[-1] if prices else 0
        return sum(prices[-period:]) / period
    
    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> float:
        """计算RSI指标"""
        if len(prices) < period + 1:
            return 50
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return 50
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def is_above_all_ma(prices: List[float]) -> bool:
        """检查是否站上所有均线（MA5/10/20/60）"""
        if len(prices) < 60:
            return False
        
        current_price = prices[-1]
        ma5 = TechnicalIndicators.calculate_ma(prices, 5)
        ma10 = TechnicalIndicators.calculate_ma(prices, 10)
        ma20 = TechnicalIndicators.calculate_ma(prices, 20)
        ma60 = TechnicalIndicators.calculate_ma(prices, 60)
        
        return current_price > ma5 > ma10 > ma20 > ma60
    
    @staticmethod
    def is_below_ma(prices: List[float], period: int) -> bool:
        """检查是否跌破某条均线"""
        if len(prices) < period:
            return False
        current_price = prices[-1]
        ma = TechnicalIndicators.calculate_ma(prices, period)
        return current_price < ma
    
    @staticmethod
    def is_break_high(prices: List[float], period: int) -> bool:
        """检查是否突破N日高点"""
        if len(prices) < period + 1:
            return False
        current_price = prices[-1]
        period_high = max(prices[-period-1:-1])  # 不包含今天
        return current_price > period_high


class MarketTiming:
    """大盘择时模块 - V2.0 综合指数版
    
    市场强弱综合指数 (0-100):
    - 趋势指标 (40%): 大盘指数与20日均线偏离度
    - 宽度指标 (30%): 两市上涨家数占比(5日均值)
    - 成交量指标 (20%): 成交额相对20日均值变化
    - 情绪指标 (10%): 涨停家数/跌停家数比
    
    判断标准:
    - >60: 强势市场
    - 40-60: 震荡市场
    - <40: 弱势市场
    """
    
    def __init__(self):
        self.market_strength = 50  # 默认中性
        self.reason = ""
        self.weights = {
            'trend': 0.40,      # 趋势权重
            'breadth': 0.30,    # 宽度权重
            'volume': 0.20,     # 成交量权重
            'sentiment': 0.10   # 情绪权重
        }
    
    def analyze(self, index_quotes: Dict = None, quotes: Dict = None) -> Dict:
        """
        分析大盘环境 - 综合指数版
        
        Args:
            index_quotes: 指数行情数据
            quotes: 个股行情数据（用于计算宽度和情绪）
        
        Returns:
            {
                'market_strength': float,  # 0-100 综合指数
                'market_status': str,      # strong/normal/weak
                'threshold_adjust': float, # 阈值调整量
                'max_picks': int,          # 最大选股数
                'components': Dict,        # 各分项得分
                'reason': str              # 原因说明
            }
        """
        try:
            components = {}
            
            # 1. 趋势指标 (40%) - 大盘与20日均线偏离度
            trend_score = self._calc_trend_score(index_quotes)
            components['trend'] = round(trend_score, 1)
            
            # 2. 宽度指标 (30%) - 上涨家数占比
            breadth_score = self._calc_breadth_score(quotes)
            components['breadth'] = round(breadth_score, 1)
            
            # 3. 成交量指标 (20%) - 成交额变化
            volume_score = self._calc_volume_score(index_quotes)
            components['volume'] = round(volume_score, 1)
            
            # 4. 情绪指标 (10%) - 涨跌停比
            sentiment_score = self._calc_sentiment_score(quotes)
            components['sentiment'] = round(sentiment_score, 1)
            
            # 计算综合指数
            self.market_strength = (
                trend_score * self.weights['trend'] +
                breadth_score * self.weights['breadth'] +
                volume_score * self.weights['volume'] +
                sentiment_score * self.weights['sentiment']
            )
            
            # 动态阈值调整（连续变化）
            # threshold = 60 + (60 - market_strength) * 0.2
            # 市场越强，阈值越低；市场越弱，阈值越高
            base_threshold = 60
            threshold = base_threshold + (60 - self.market_strength) * 0.2
            threshold = max(55, min(75, threshold))  # 限制在55-75之间
            
            # 仓位比例（替代选股数量限制）
            # 仓位 = min(100%, max(20%, market_strength / 50))
            position_ratio = min(1.0, max(0.2, self.market_strength / 50))
            
            # 确定市场状态
            if self.market_strength >= 60:
                market_status = 'strong'
                reason = f"强势市场(指数:{self.market_strength:.0f}): 趋势{trend_score:.0f}/宽度{breadth_score:.0f}/量能{volume_score:.0f}/情绪{sentiment_score:.0f}"
            elif self.market_strength >= 40:
                market_status = 'normal'
                reason = f"震荡市场(指数:{self.market_strength:.0f}): 趋势{trend_score:.0f}/宽度{breadth_score:.0f}/量能{volume_score:.0f}/情绪{sentiment_score:.0f}"
            elif self.market_strength >= 25:
                market_status = 'weak'
                reason = f"弱势市场(指数:{self.market_strength:.0f}): 趋势{trend_score:.0f}/宽度{breadth_score:.0f}/量能{volume_score:.0f}/情绪{sentiment_score:.0f}"
            else:
                market_status = 'extreme_weak'
                reason = f"极弱市场(指数:{self.market_strength:.0f}): 趋势{trend_score:.0f}/宽度{breadth_score:.0f}/量能{volume_score:.0f}/情绪{sentiment_score:.0f}"
            
            # 动态因子权重系数（市场弱势时降低进攻性因子权重）
            factor_multipliers = {
                'technical': 1.0,
                'sentiment': 1.0,
                'sector': 1.0,
                'money_flow': 1.0,
                'risk': 1.0,
                'consensus': 1.0,
                'news_sentiment': 1.0
            }
            
            if self.market_strength < 40:
                # 弱势市场：风险权重提高20%，进攻性因子降低
                factor_multipliers['risk'] = 1.2
                factor_multipliers['technical'] = 0.95
                factor_multipliers['sentiment'] = 0.95
                factor_multipliers['sector'] = 0.95
                factor_multipliers['money_flow'] = 0.95
            elif self.market_strength > 60:
                # 强势市场：进攻性因子权重提高，风险权重降低
                factor_multipliers['risk'] = 0.9
                factor_multipliers['technical'] = 1.05
                factor_multipliers['sector'] = 1.05
                factor_multipliers['money_flow'] = 1.05
            
            return {
                'market_strength': round(self.market_strength, 1),
                'market_status': market_status,
                'threshold': round(threshold, 1),  # 直接返回计算后的阈值
                'position_ratio': round(position_ratio, 2),  # 仓位比例
                'factor_multipliers': factor_multipliers,  # 因子权重系数
                'components': components,
                'reason': reason
            }
            
        except Exception as e:
            logger.warning(f"大盘择时分析失败: {e}")
            return {
                'market_strength': 50,
                'market_status': 'normal',
                'threshold_adjust': 0,
                'max_picks': 3,
                'components': {},
                'reason': f'分析异常: {e}'
            }
    
    def _calc_trend_score(self, index_quotes: Dict) -> float:
        """趋势指标: 大盘与20日均线偏离度 (0-100)"""
        try:
            # 获取沪深300数据
            hs300 = index_quotes.get('000300') if index_quotes else None
            if not hs300:
                return 50  # 默认中性
            
            # 获取历史数据计算MA20
            from stock_history_db import StockHistoryDB
            db = StockHistoryDB()
            hist_prices = db.get_prices('000300', days=25)  # 取25天确保有20天有效
            
            if len(hist_prices) >= 20:
                ma20 = sum(hist_prices[-20:]) / 20
                current_price = hs300.get('price', ma20)
                
                # 计算偏离度
                if ma20 > 0:
                    deviation = (current_price - ma20) / ma20 * 100
                    # 映射到 0-100: -5%->0, 0%->50, +5%->100
                    score = 50 + deviation * 10
                    return max(0, min(100, score))
            
            # 简化版: 用涨跌幅估算
            change_pct = hs300.get('change_pct', 0)
            score = 50 + change_pct * 10
            return max(0, min(100, score))
            
        except Exception as e:
            logger.debug(f"趋势指标计算失败: {e}")
            return 50
    
    def _calc_breadth_score(self, quotes: Dict) -> float:
        """宽度指标: 上涨家数占比 (0-100)"""
        try:
            if not quotes or len(quotes) == 0:
                return 50
            
            up_count = sum(1 for q in quotes.values() if q.get('change_pct', 0) > 0)
            total = len(quotes)
            
            if total > 0:
                up_ratio = up_count / total * 100
                return up_ratio  # 直接就是0-100
            
            return 50
        except Exception as e:
            logger.debug(f"宽度指标计算失败: {e}")
            return 50
    
    def _calc_volume_score(self, index_quotes: Dict) -> float:
        """成交量指标: 成交额相对20日均值变化 (0-100)"""
        try:
            # 简化版: 基于当前市场情绪估算
            # 实际应该获取历史成交额数据
            hs300 = index_quotes.get('000300') if index_quotes else None
            if hs300:
                # 放量上涨 = 强, 放量下跌 = 弱, 缩量 = 中性
                change_pct = hs300.get('change_pct', 0)
                # 映射: -3%->30, 0%->50, +3%->70 (基础分)
                base_score = 50 + change_pct * 7
                return max(20, min(80, base_score))  # 成交量指标范围窄一些
            
            return 50
        except Exception as e:
            logger.debug(f"成交量指标计算失败: {e}")
            return 50
    
    def _calc_sentiment_score(self, quotes: Dict) -> float:
        """情绪指标: 涨停/跌停家数比 (0-100)"""
        try:
            if not quotes:
                return 50
            
            limit_up = sum(1 for q in quotes.values() if q.get('change_pct', 0) >= 9.5)
            limit_down = sum(1 for q in quotes.values() if q.get('change_pct', 0) <= -9.5)
            
            if limit_down == 0:
                if limit_up == 0:
                    return 50  # 无涨跌停，中性
                else:
                    return 100  # 有涨停无跌停，极强
            else:
                # 涨停/跌停比映射到 0-100
                ratio = limit_up / limit_down
                score = 50 + (ratio - 1) * 25
                return max(0, min(100, score))
            
        except Exception as e:
            logger.debug(f"情绪指标计算失败: {e}")
            return 50
    
    def _get_hs300_data(self, index_quotes: Dict = None) -> Dict:
        """获取沪深300数据"""
        if index_quotes and '000300' in index_quotes:
            return index_quotes['000300']
        
        # 尝试从实时行情获取
        try:
            import requests
            url = 'http://qt.gtimg.cn/q=sh000300'
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                # 解析腾讯数据格式
                data = resp.text
                if 'v_sh000300' in data:
                    parts = data.split('~')
                    if len(parts) > 5:
                        return {
                            'price': float(parts[3]),
                            'pre_close': float(parts[4]),
                            'change_pct': float(parts[5])
                        }
        except Exception as e:
            logger.warning(f"获取沪深300失败: {e}")
        
        return None


class MultiFactorAnalyzerV10:
    """V10+P2+舆情多因子分析器 - 优化版
    
    权重分配（优化后）：
    - 技术趋势:     20% (0-20分)  - 趋势确认（MA、突破、RSI）
    - 情绪(技术):    3% (0-3分)   - 换手率+涨停结构（与技术面低相关）
    - 板块轮动:     33% (0-33分)  - RPS+双周期动量（带强度阈值）
    - 资金流向:     19% (0-19分)  - 主力+大单+北向
    - 风险控制:   15-20% (0-15/20分) - 动态权重（弱势市场提升至20%）
    - 一致预期:      3% (0-3分)   - 机构调研+评级（降低权重）
    - 舆情因子:      7% (0-7分)   - 新闻情感分析（提升权重）
    
    优化要点：
    1. 技术+情绪合计23%，但降低相关性（情绪简化）
    2. 板块轮动33%，但增加RPS<80时的惩罚机制
    3. 风险因子动态权重：弱势市场15%→20%
    4. 一致预期3%+舆情7%，体现信息驱动
    """
    
    def __init__(self):
        self.market_timing = MarketTiming()
        self.tech_indicators = TechnicalIndicators()
        self.fund_flow_provider = FundFlowDataProvider()  # P2新增
        self.consensus_provider = ConsensusDataProvider()  # P2新增
        
        # Phase 2新增：舆情因子计算器
        try:
            from sentiment_factor import get_sentiment_calculator
            self.sentiment_calculator = get_sentiment_calculator()
            logger.info("✅ 舆情因子计算器初始化成功")
        except Exception as e:
            logger.warning(f"⚠️ 舆情因子计算器初始化失败: {e}，将使用默认中性值")
            self.sentiment_calculator = None
    
    def calculate_score(self, data: Dict, sectors: List[Dict], 
                       sector_momentum: Dict = None,
                       historical_prices: List[float] = None,
                       market_context: Dict = None,
                       sector_rps_data: Dict = None,
                       sentiment_factor: Dict = None,
                       premarket_mode: bool = False) -> Dict:
        """
        计算多因子评分 - V10+P1+P2+舆情完整版
        
        因子权重（Phase 2调整后）：
        - 技术: 18% (0-18分) - 微调
        - 情绪: 5% (0-5分) - 技术情绪，简化
        - 板块: 33% (0-33分) - 微调
        - 资金: 19% (0-19分) - 微调
        - 风险: 15% (0-15分) - 不变
        - 一致预期: 5% (0-5分) - P2新增
        - 舆情: 5% (0-5分) - Phase 2新增
        
        Args:
            premarket_mode: 是否为盘前选股模式（使用简化版策略）
        """
        # 盘前模式：使用简化版策略
        if premarket_mode:
            try:
                from premarket_screener_v11 import premarket_screener
                return premarket_screener.calculate_total_score(
                    data, sectors, sector_momentum, historical_prices
                )
            except Exception as e:
                logger.warning(f"盘前选股器失败，回退到标准模式: {e}")
        
        price = data.get('price', 0)
        change_pct = data.get('change_pct', 0)
        volume = data.get('volume', 0)
        turnover = data.get('turnover', 0)
        code = data.get('code', '')
        name = data.get('name', '')
        
        # 计算价格分位（用于风险因子）
        price_percentile = None
        if historical_prices and len(historical_prices) >= 60:
            current_price = historical_prices[-1]
            sorted_prices = sorted(historical_prices[-60:])
            try:
                position = sorted_prices.index(current_price)
                price_percentile = position / len(sorted_prices) * 100
            except ValueError:
                pass
        
        # P2: 获取资金流向数据
        fund_flow_data = self.fund_flow_provider.get_fund_flow(code, {code: data})
        
        # 1. 技术因子 (0-20分)
        tech_score = self._calc_technical_score_v10(
            change_pct, price, historical_prices
        )
        
        # 2. 情绪因子 (0-10分)
        sentiment_score = self._calc_sentiment_score_v10(
            change_pct, volume, turnover, code
        )
        
        # 3. 板块轮动因子 (0-35分)
        sector_score = self._calc_sector_rotation_score_v10(
            data, sectors, sector_momentum, code, name,
            sector_rps_data
        )
        
        # 4. 资金流向因子 (0-20分) - P2精细化
        money_flow_score = self._calc_money_flow_score_v10(
            change_pct, volume, turnover, code, fund_flow_data
        )
        
        # 5. 风险因子 (0-15分)
        risk_score = self._calc_risk_score_v10(
            change_pct, historical_prices, price_percentile
        )
        
        # 6. 一致性预期因子 (0-5分) - P2新增
        consensus_score = self._calc_consensus_score_v10(code)
        
        # 7. 舆情因子 (0-5分) - Phase 2新增
        news_sentiment_score = 0
        if sentiment_factor:
            # 使用预计算的舆情因子 (-10~+10 映射到 0~5)
            news_sentiment_score = (sentiment_factor.get('score', 0) + 10) / 4  # -10~+10 -> 0~5
            news_sentiment_score = max(0, min(5, news_sentiment_score))
        else:
            # 实时计算（较慢）
            if self.sentiment_calculator:
                try:
                    sentiment_result = self.sentiment_calculator.calculate_sentiment_factor(
                        code, name
                    )
                    news_sentiment_score = (sentiment_result['score'] + 10) / 4
                    news_sentiment_score = max(0, min(5, news_sentiment_score))
                except Exception as e:
                    logger.debug(f"{code} 舆情因子计算失败: {e}")
                    news_sentiment_score = 2.5  # 默认中性
            else:
                news_sentiment_score = 2.5  # 默认中性
        
        # 优化后的权重计算（根据用户建议调整）
        # 新权重：技术20 + 情绪3 + 板块33 + 资金19 + 风险15-20 + 一致预期3 + 舆情7 = 100-105

        # 1. 基础权重调整
        tech_adjusted = tech_score * 1.0        # 20分制保持20分 (×1.0)
        sentiment_adjusted = sentiment_score * 0.3  # 10分制 -> 3分制 (×0.3)
        sector_adjusted = sector_score * 0.9429  # 35分制 -> 33分制 (×0.9429)
        money_flow_adjusted = money_flow_score * 0.95  # 20分制 -> 19分制 (×0.95)
        risk_adjusted = risk_score * 1.0  # 15分制保持15分 (×1.0)
        consensus_adjusted = consensus_score * 0.6  # 5分制 -> 3分制 (×0.6)
        news_sentiment_adjusted = news_sentiment_score * 1.4  # 5分制 -> 7分制 (×1.4)
        
        # 应用动态因子权重系数（基于市场强弱）
        if market_context and market_context.get('factor_multipliers'):
            multipliers = market_context['factor_multipliers']
            tech_adjusted *= multipliers.get('technical', 1.0)
            sentiment_adjusted *= multipliers.get('sentiment', 1.0)
            sector_adjusted *= multipliers.get('sector', 1.0)
            money_flow_adjusted *= multipliers.get('money_flow', 1.0)
            risk_adjusted *= multipliers.get('risk', 1.0)
            consensus_adjusted *= multipliers.get('consensus', 1.0)
            news_sentiment_adjusted *= multipliers.get('news_sentiment', 1.0)
        
        # 3. 板块轮动强度阈值：RPS<80时打8折
        top_sector_rps = 0
        if sector_rps_data and len(sector_rps_data) > 0:
            # 确保所有值都是数值类型
            try:
                rps_values = [v for v in sector_rps_data.values() if isinstance(v, (int, float))]
                if rps_values:
                    top_sector_rps = max(rps_values)
            except:
                top_sector_rps = 0
        elif sectors and len(sectors) > 0:
            top_sector_rps = sectors[0].get('rps', 0) if sectors else 0
        
        sector_penalty = 1.0
        if top_sector_rps < 80:
            sector_penalty = 0.8  # 板块普弱时打8折
            logger.debug(f"板块RPS={top_sector_rps:.1f}<80，板块因子打8折")
        sector_adjusted = sector_adjusted * sector_penalty
        
        total = (tech_adjusted + sentiment_adjusted + sector_adjusted + 
                money_flow_adjusted + risk_adjusted + consensus_adjusted + news_sentiment_adjusted)
        
        # 应用动态阈值（基于市场强度连续调整）
        threshold = 60  # 基础阈值
        position_ratio = 1.0  # 默认仓位100%
        market_note = ""
        
        if market_context:
            # 使用市场强弱直接计算的阈值
            threshold = market_context.get('threshold', 60)
            position_ratio = market_context.get('position_ratio', 1.0)
            market_note = market_context.get('reason', '')
            
            # 添加动态调整标记
            if market_context.get('factor_multipliers', {}).get('risk', 1.0) != 1.0:
                market_note += f" [风险权重×{market_context['factor_multipliers']['risk']:.1f}]"
            if sector_penalty < 1.0:
                market_note += " [板块因子降权]"
        
        return {
            'total': round(total, 1),
            'technical': round(tech_adjusted, 1),
            'sentiment': round(sentiment_adjusted, 1),
            'sector': round(sector_adjusted, 1),
            'money_flow': round(money_flow_adjusted, 1),
            'risk': round(risk_adjusted, 1),
            'consensus': round(consensus_adjusted, 1),
            'news_sentiment': round(news_sentiment_adjusted, 1),
            'threshold': threshold,
            'position_ratio': position_ratio,
            'market_note': market_note,
            'sector_rps': top_sector_rps,
            'reason': f'技术:{tech_adjusted:.0f} 情绪:{sentiment_adjusted:.0f} 板块:{sector_adjusted:.0f} 资金:{money_flow_adjusted:.0f} 风险:{risk_adjusted:.0f} 舆情:{news_sentiment_adjusted:.0f}'
        }
    
    def _calc_technical_score_v10(self, change_pct: float, price: float,
                                   historical_prices: List[float] = None) -> float:
        """技术因子评分 V10 (0-20分) - 趋势确认"""
        score = 8  # 降低基础分
        
        # 1. 当日涨幅得分（降低权重）
        if change_pct > 3:
            score += 3
        elif change_pct > 1:
            score += 2
        elif change_pct > 0:
            score += 1
        elif change_pct < -3:
            score -= 3
        
        # 2. 趋势确认指标（需要历史数据）
        if historical_prices and len(historical_prices) >= 60:
            # 突破20日高点：+5分
            if self.tech_indicators.is_break_high(historical_prices, 20):
                score += 5
            
            # 突破60日高点：+5分
            if self.tech_indicators.is_break_high(historical_prices, 60):
                score += 5
            
            # 站上所有均线（MA5/10/20/60）：+4分
            if self.tech_indicators.is_above_all_ma(historical_prices):
                score += 4
            
            # RSI在强势区（50-80）：+3分
            rsi = self.tech_indicators.calculate_rsi(historical_prices, 14)
            if 50 <= rsi <= 80:
                score += 3
            
            # 跌破20日均线：-5分（惩罚）
            if self.tech_indicators.is_below_ma(historical_prices, 20):
                score -= 5
        
        return min(20, max(0, score))
    
    def _calc_sentiment_score_v10(self, change_pct: float, volume: float,
                                   turnover: float, code: str) -> float:
        """
        情绪(技术)因子评分 V10优化版 (0-10分 -> 映射到0-3分)
        
        优化要点：
        1. 降低与趋势因子的相关性
        2. 更关注"量能异动"而非"价格趋势"
        3. 权重降至3%，简化计算
        """
        score = 5  # 基础分
        
        # 计算换手率（简化估算）
        if code.startswith(('00', '60')):
            estimated_cap = 50e8
        else:
            estimated_cap = 20e8
        
        turnover_rate = (turnover / estimated_cap * 100) if estimated_cap > 0 else 0
        
        # 量能异动（与价格脱钩，只关注成交活跃度）
        # 异常放量（换手率>10%但未涨停）
        if turnover_rate > 10 and change_pct < 9:
            score += 3  # 放量但未涨停，说明资金分歧大
        elif 5 <= turnover_rate <= 10:
            score += 2  # 正常活跃
        elif 2 <= turnover_rate < 5:
            score += 1  # 轻度活跃
        elif turnover_rate < 1:
            score -= 2  # 极度冷清，回避
        
        # 涨停结构（只关注是否涨停，不关注涨幅大小）
        # 与趋势因子区分：趋势因子关注突破，情绪因子关注封板质量
        if change_pct >= 9.9:
            score += 2  # 涨停，情绪高涨
        elif change_pct <= -9.9:
            score -= 3  # 跌停，情绪恐慌
        
        return min(10, max(0, score))
    
    def _calc_sector_rotation_score_v10(self, data: Dict, sectors: List[Dict], 
                                         sector_momentum: Dict, code: str, name: str,
                                         sector_rps_data: Dict = None) -> float:
        """
        板块轮动因子评分 V10+P1 (0-35分) - 深化版
        
        P1优化：
        1. 双周期动量（5日/20日）
        2. 板块RPS排名
        3. 龙头识别升级（RPS前10%/30%）
        """
        score = 10  # 基础分
        stock_sector = data.get('sector', '其他')
        change_pct = data.get('change_pct', 0)
        
        # P1优化：使用RPS数据（如果提供）
        if sector_rps_data and stock_sector in sector_rps_data:
            rps_info = sector_rps_data[stock_sector]
            composite_rps = rps_info.get('composite_rps', 50)
            
            # 1. 板块RPS排名加分 (0-15分)
            if composite_rps >= 90:  # 前10%
                score += 15
            elif composite_rps >= 70:  # 前30%
                score += 10
            elif composite_rps >= 50:  # 前50%
                score += 5
        else:
            # 1. 强势板块匹配 (0-15分) - 原逻辑降级
            for i, s in enumerate(sectors[:5]):
                if stock_sector == s['name']:
                    rank_bonus = [15, 12, 9, 6, 3][i]
                    score += rank_bonus
                    break
        
        # 2. 双周期板块动量 (0-12分) - P1优化
        if sector_momentum and stock_sector in sector_momentum:
            momentum = sector_momentum[stock_sector]
            # 5日动量（短期）权重0.4 + 20日动量（中期）权重0.6
            # 简化处理：直接使用当日动量
            if momentum > 8:
                score += 12
            elif momentum > 5:
                score += 9
            elif momentum > 3:
                score += 6
            elif momentum > 1:
                score += 3
        
        # 3. 龙头股识别升级 (0-8分) - P1优化
        if sectors and len(sectors) > 0:
            sector_data = next((s for s in sectors if s['name'] == stock_sector), None)
            if sector_data:
                sector_change = sector_data.get('change_pct', 0)
                
                # P1升级：超越板块平均2倍（原1.5倍）
                if sector_change > 0 and change_pct > sector_change * 2:
                    score += 8  # 超级龙头
                elif sector_change > 0 and change_pct > sector_change * 1.5:
                    score += 5  # 普通龙头
                # 超越板块平均
                elif change_pct > sector_change:
                    score += 3
                
                # 涨幅>5%且板块强势
                if change_pct > 5 and sector_change > 2:
                    score += 2
        
        return min(35, max(0, score))
    
    def _calc_money_flow_score_v10(self, change_pct: float, volume: float,
                                    turnover: float, code: str,
                                    fund_flow_data: Dict = None) -> float:
        """
        资金流向因子评分 V10+P2 (0-20分) - 精细化版
        
        P2优化：
        1. 主力净流入占比
        2. 大单主动性买入占比
        3. 北向资金连续净流入
        4. 散户净流出信号
        """
        score = 6  # 降低基础分
        
        # P0基础：成交量结构
        if code.startswith(('00', '60')):
            if turnover > 1e8:
                score += 3
            elif turnover > 5e7:
                score += 2
            
            if change_pct > 0 and turnover > 1e8:
                score += 2
            elif change_pct > 0 and turnover > 5e7:
                score += 1
        else:
            if turnover > 3e7:
                score += 3
            elif turnover > 1.5e7:
                score += 2
            
            if change_pct > 0 and turnover > 3e7:
                score += 2
            elif change_pct > 0 and turnover > 1.5e7:
                score += 1
        
        # P2精细化：资金流向数据
        if fund_flow_data:
            main_inflow_pct = fund_flow_data.get('main_inflow_pct', 0)
            large_order_pct = fund_flow_data.get('large_order_pct', 50)
            north_bound = fund_flow_data.get('north_bound', False)
            
            # 1. 主力净流入占比 (0-6分)
            if main_inflow_pct >= 10:
                score += 6
            elif main_inflow_pct >= 5:
                score += 4
            elif main_inflow_pct > 0:
                score += 2
            elif main_inflow_pct < -5:  # 主力净流出
                score -= 4
            
            # 2. 大单主动性买入占比 (0-4分)
            if large_order_pct >= 60:
                score += 4
            elif large_order_pct >= 55:
                score += 2
            elif large_order_pct < 45:  # 大单卖出多
                score -= 2
            
            # 3. 北向资金 (0-3分)
            if north_bound:
                score += 3
            
            # 4. 诱多信号检测
            if change_pct > 0 and main_inflow_pct < -5:
                # 上涨但主力流出，可能是诱多
                score -= 3
        
        # 下跌+放量：偏空信号
        if change_pct < 0 and turnover > 1e8:
            score -= 3
        
        return min(20, max(0, score))
    
    def _calc_consensus_score_v10(self, code: str) -> float:
        """
        P2新增：一致性预期因子评分 (0-5分)
        
        权重从技术/情绪因子平移而来
        """
        score = 0
        
        consensus = self.consensus_provider.get_consensus_data(code)
        
        if not consensus.get('data_available', False):
            # 数据不可用，返回中性分
            return 2.5
        
        # 1. 近期有机构调研 (0-2分)
        if consensus.get('has_research', False):
            score += 2
        
        # 2. 券商评级上调 (0-2分)
        if consensus.get('rating_upgrade', False):
            score += 2
        
        # 3. 目标价上调 (0-1分)
        target_raise = consensus.get('target_raise', 0)
        if target_raise >= 20:
            score += 1
        
        return min(5, score)
    
    def _calc_risk_score_v10(self, change_pct: float, 
                              historical_prices: List[float] = None,
                              price_percentile: float = None) -> float:
        """
        风险因子评分 V10+P1 (0-15分) - 扩展维度
        
        P1优化：
        1. 波动率控制
        2. 20日最大回撤
        3. 历史高位判断（80%分位）
        """
        score = 10  # 基础分
        
        # 1. 波动率（保留）
        if abs(change_pct) < 3:
            score += 2
        elif abs(change_pct) < 5:
            score += 0
        elif abs(change_pct) < 8:
            score -= 2
        else:
            score -= 4
        
        # 2. 20日最大回撤（P1保留）
        if historical_prices and len(historical_prices) >= 20:
            recent_high = max(historical_prices[-20:])
            current = historical_prices[-1]
            max_drawdown = (recent_high - current) / recent_high * 100 if recent_high > 0 else 0
            
            if max_drawdown > 15:  # P1升级：从12%提升到15%
                score -= 5
            elif max_drawdown > 10:  # P1升级：从8%提升到10%
                score -= 3
            elif max_drawdown > 5:
                score -= 1
        
        # 3. 历史高位判断（P1新增）
        if price_percentile is not None:
            if price_percentile >= 85:  # 处于85%分位以上
                score -= 4
            elif price_percentile >= 80:  # 处于80%分位以上
                score -= 2
        elif historical_prices and len(historical_prices) >= 60:
            # 计算60日价格分位
            current_price = historical_prices[-1]
            sorted_prices = sorted(historical_prices[-60:])
            try:
                position = sorted_prices.index(current_price)
                percentile = position / len(sorted_prices) * 100
                
                if percentile >= 85:
                    score -= 4
                elif percentile >= 80:
                    score -= 2
            except ValueError:
                pass
        
        return min(15, max(0, score))


class SinaDataProvider:
    """新浪财经数据提供者（简化版）"""
    pass  # 使用data_source中已有的实现


# ============================================================================
# V10+ 报告生成器
# ============================================================================

class ReportGeneratorV10Plus:
    """V10+ 报告生成器"""

    def __init__(self):
        self.ai_available = self._check_ai()
        if self.ai_available:
            logger.info("V10+: AI分析可用")

    def _check_ai(self) -> bool:
        """检查AI是否可用"""
        try:
            from ai_analyzer_dashboard import StockAIAnalyzer
            analyzer = StockAIAnalyzer()
            return bool(analyzer.api_key)
        except:
            return False

    def _generate_position_analysis(self, market_context: Dict = None) -> str:
        """生成持仓股分析（调用DeepSeek AI）"""
        try:
            from position_manager import position_manager
            from deepseek_analyzer import deepseek_analyzer

            # 获取当前持仓
            positions = position_manager.get_all_positions()

            if not positions:
                return ""

            # 更新持仓价格
            # 这里需要从外部传入quotes数据，暂时使用缓存的价格
            position_data = []
            for p in positions:
                position_data.append({
                    'code': p.code,
                    'name': p.name,
                    'buy_price': p.buy_price,
                    'current_price': p.current_price,
                    'current_return': p.current_return,
                    'shares': p.shares,
                    'buy_date': p.buy_date,
                    'stop_loss': p.stop_loss,
                    'target_price': p.target_price
                })

            # 调用DeepSeek分析
            result = deepseek_analyzer.analyze_positions(position_data, market_context)

            if result.get('success'):
                return result.get('analysis', '')

        except Exception as e:
            logger.warning(f"持仓分析失败: {e}")

        return ""

    def _generate_detailed_position_report(self, quotes: Dict = None) -> str:
        """生成详细的持仓报告（不依赖DeepSeek，本地生成）"""
        try:
            from position_manager import position_manager

            # 获取当前持仓
            positions = position_manager.get_all_positions()

            if not positions:
                return ""

            # 如果有新的行情数据，更新持仓价格
            if quotes:
                position_manager.update_prices(quotes)
                positions = position_manager.get_all_positions()

            # 计算汇总
            total_cost = 0
            total_value = 0
            position_details = []

            for p in positions:
                cost = p.buy_price * p.shares
                value = p.current_price * p.shares
                profit = value - cost
                profit_pct = (p.current_price - p.buy_price) / p.buy_price * 100 if p.buy_price > 0 else 0

                total_cost += cost
                total_value += value

                # 生成建议（后续会用DeepSeek API增强）
                position_details.append({
                    'name': p.name,
                    'code': p.code,
                    'buy_price': p.buy_price,
                    'current_price': p.current_price,
                    'shares': p.shares,
                    'cost': cost,
                    'value': value,
                    'profit': profit,
                    'profit_pct': profit_pct,
                    'stop_loss': p.stop_loss,
                    'target_price': p.target_price
                })

            # 按盈亏排序
            position_details.sort(key=lambda x: x['profit_pct'], reverse=True)

            # 计算总盈亏
            total_profit = total_value - total_cost
            total_profit_pct = (total_profit / total_cost * 100) if total_cost > 0 else 0

            # 生成报告
            lines = [
                "## 💼 持仓分析",
                "",
                f"**持仓汇总**: 共{len(positions)}只 | 总成本: ¥{total_cost:,.2f} | 总市值: ¥{total_value:,.2f}",
                f"**总盈亏**: ¥{total_profit:,.2f} ({total_profit_pct:+.2f}%)",
                "",
                "----------------------------------------------------------------------",
                "",
                "### 📈 持仓明细 (按盈亏排序)",
                "",
            ]

            # 使用DeepSeek API生成持仓分析
            try:
                from deepseek_analyzer import stock_picker_analyzer
                position_analyzer = stock_picker_analyzer
            except:
                position_analyzer = None

            for i, p in enumerate(position_details, 1):
                emoji = "🟢" if p['profit_pct'] >= 0 else "🔴"
                profit_pct = p['profit_pct']

                # 生成操作建议标签
                if profit_pct >= 5:
                    suggestion_label = "🟢 建议持有"
                elif profit_pct >= 0:
                    suggestion_label = "⚪ 观望"
                elif profit_pct >= -5:
                    suggestion_label = "⚪ 观望"
                else:
                    suggestion_label = "🟠 关注止损"

                # 使用AI生成详细分析
                if position_analyzer:
                    analysis = position_analyzer.analyze_position(p)
                else:
                    analysis = self._generate_fallback_position_analysis(p)

                lines.extend([
                    f"{i}. {emoji} **{p['name']}** ({p['code']})",
                    f"   - 成本: ¥{p['buy_price']:.3f} → 现价: ¥{p['current_price']:.3f}",
                    f"   - 持仓: {p['shares']:,}股 | 市值: ¥{p['value']:,.2f}",
                    f"   - 盈亏: ¥{p['profit']:,.2f} ({profit_pct:+.2f}%)",
                    f"   - 止损价: ¥{p['stop_loss']:.3f} | 目标价: ¥{p['target_price']:.3f}",
                    f"   - 建议: **{suggestion_label}**",
                    f"   - 说明: {analysis}",
                    "",
                ])

            lines.extend([
                "======================================================================",
                "",
            ])

            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"详细持仓报告生成失败: {e}")
            return ""

    def _generate_fallback_position_analysis(self, position: Dict) -> str:
        """生成备用持仓分析（当DeepSeek API不可用时）"""
        profit_pct = position.get('profit_pct', 0)
        stop_loss = position.get('stop_loss', 0)
        target_price = position.get('target_price', 0)
        current_price = position.get('current_price', 0)

        parts = []

        # 根据盈亏状态生成分析
        if profit_pct >= 5:
            parts.append(f"盈利{profit_pct:.1f}%，趋势良好，可继续持有")
            parts.append(f"，建议关注¥{target_price:.2f}目标价，可考虑分批止盈")
        elif profit_pct >= 0:
            parts.append(f"小幅盈利{profit_pct:.1f}%，正常波动")
            parts.append(f"，建议设好止损位¥{stop_loss:.2f}，等待趋势明朗")
        elif profit_pct >= -5:
            parts.append(f"小幅亏损{profit_pct:.1f}%，关注支撑位")
            if current_price <= stop_loss * 1.02:  # 接近止损
                parts.append(f"，已接近止损价¥{stop_loss:.2f}，如跌破建议果断减仓")
            else:
                parts.append(f"，如跌破¥{stop_loss:.2f}止损价建议减仓")
        else:
            parts.append(f"亏损{profit_pct:.1f}%，已触发关注阈值")
            if current_price <= stop_loss:
                parts.append(f"，已跌破止损价¥{stop_loss:.2f}，建议严格执行止损避免深套")
            else:
                parts.append(f"，关注是否跌破止损价¥{stop_loss:.2f}，准备减仓或止损")

        return "".join(parts)

    def _generate_fallback_pick_analysis(self, pick: Dict) -> str:
        """生成备用选股分析（当DeepSeek API不可用时）"""
        factors = pick.get('factors', {})
        total_score = pick.get('total_score', 0)
        sector = pick.get('sector', '其他')
        is_leader = pick.get('is_sector_leader', False)

        # 找出最高分因子
        factor_scores = {
            '技术面': factors.get('technical', 0),
            '情绪面': factors.get('sentiment', 0),
            '板块轮动': factors.get('sector', 0),
            '资金面': factors.get('money_flow', 0),
            '风控': factors.get('risk', 0)
        }
        best_factor = max(factor_scores, key=factor_scores.get)
        best_score = factor_scores[best_factor]

        # 生成分析
        parts = []

        # 核心优势
        if best_score >= 20:
            parts.append(f"{best_factor}表现优异({best_score:.0f}分)")
        elif total_score >= 70:
            parts.append("各因子均衡，综合评分优秀")
        else:
            parts.append("综合评分良好，符合选股标准")

        # 板块因素
        if is_leader:
            parts.append(f"，为{sector}板块龙头")
        elif sector != '其他':
            parts.append(f"，受益于{sector}板块轮动")

        # 投资者类型和风险
        if total_score >= 75:
            parts.append("。适合激进型投资者，建议关注开盘表现，注意控制仓位风险。")
        elif total_score >= 70:
            parts.append("。适合稳健型投资者，可关注低吸机会，设好止损位。")
        else:
            parts.append("。适合保守型投资者，建议观察后再决策，注意市场波动风险。")

        return "".join(parts)

    def _generate_strategy_section(self, strategy_results: Dict) -> str:
        """生成策略报告段落"""
        if not strategy_results:
            return ""
        
        lines = [
            "",
            "======================================================================",
            "",
            "## 🎯 三策略融合信号",
            "",
        ]
        
        # 突破策略
        breakout_signals = strategy_results.get('breakout', [])
        if breakout_signals:
            lines.extend([
                "### 📈 突破策略 (追涨强势股)",
                "",
            ])
            for i, signal in enumerate(breakout_signals[:3], 1):
                lines.append(
                    f"{i}. **{signal.name}** ({signal.code}) 置信度{signal.score}%"
                )
                lines.append(
                    f"   - 突破价: ¥{signal.breakout_price} → 当前: ¥{signal.current_price}"
                )
                lines.append(
                    f"   - 止损: ¥{signal.stop_loss} | 目标: ¥{signal.target_price}"
                )
                lines.append(f"   - {signal.reason}")
                lines.append("")
        
        # 双动量策略
        momentum_scores = strategy_results.get('momentum', [])
        if momentum_scores:
            lines.extend([
                "### 🔄 双动量策略 (趋势+板块双重验证)",
                "",
            ])
            for i, score in enumerate(momentum_scores[:3], 1):
                lines.append(
                    f"{i}. **{score.name}** ({score.code}) 得分{score.composite_score:.0f}"
                )
                lines.append(
                    f"   - 20日涨幅: {score.change_pct_20d:+.1f}% | RS强度: {score.rs_rating}"
                )
                lines.append(
                    f"   - 个股排名: {score.stock_momentum_rank} | 板块排名: {score.sector_momentum_rank}"
                )
                lines.append("")
        
        # 网格策略
        grid_signals = strategy_results.get('grid_signals', [])
        if grid_signals:
            lines.extend([
                "### 🔲 网格策略 (ETF震荡市交易)",
                "",
            ])
            for signal in grid_signals[:5]:
                action_emoji = "🔴 买入" if signal['action'] == 'buy' else "🟢 卖出"
                lines.append(
                    f"- {action_emoji} **{signal['code']}** {signal['shares']}股 @ ¥{signal['price']:.3f}"
                )
                lines.append(f"  {signal['reason']}")
            lines.append("")
        
        # 如果没有信号
        if not breakout_signals and not momentum_scores and not grid_signals:
            lines.append("当前市场环境下，三策略暂无明确交易信号。")
            lines.append("")
        
        lines.append("---")
        lines.append("")
        
        return "\n".join(lines)

    def _generate_market_overview(self, quotes: Dict = None) -> str:
        """生成市场概况（大盘指数、涨跌统计、强势板块）"""
        try:
            lines = [
                "## 📊 市场概况",
                "======================================================================",
                "",
            ]
            
            # 1. 大盘指数
            lines.extend([
                "### 📈 大盘指数",
                "",
            ])
            
            # 获取指数数据
            try:
                from sina_finance_api import SinaDataProvider
                sina = SinaDataProvider()
                index_quotes = sina.get_index_quotes()
                
                index_map = {
                    '000001': '上证指数',
                    '399001': '深证成指', 
                    '399006': '创业板指',
                    '000300': '沪深300',
                    '000016': '上证50',
                    '000905': '中证500'
                }
                
                for code, name in index_map.items():
                    if code in index_quotes:
                        data = index_quotes[code]
                        change = data.get('change_percent', 0)
                        emoji = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
                        lines.append(f"- {emoji} **{name}**: {data.get('price', 0):.2f} ({change:+.2f}%)")
            except Exception as e:
                lines.append("- 指数数据获取中...")
            
            lines.append("")
            
            # 2. 涨跌统计
            if quotes:
                lines.extend([
                    "### 📊 涨跌分布",
                    "",
                ])
                
                up = sum(1 for q in quotes.values() if q.get('change_pct', 0) > 0)
                down = sum(1 for q in quotes.values() if q.get('change_pct', 0) < 0)
                flat = len(quotes) - up - down
                total = len(quotes)
                
                lines.extend([
                    f"| 类型 | 数量 | 占比 |",
                    f"|------|------|------|",
                    f"| 🟢 上涨 | {up:,} | {up/total*100:.1f}% |" if total > 0 else f"| 🟢 上涨 | {up:,} | - |",
                    f"| 🔴 下跌 | {down:,} | {down/total*100:.1f}% |" if total > 0 else f"| 🔴 下跌 | {down:,} | - |",
                    f"| ⚪ 平盘 | {flat:,} | {flat/total*100:.1f}% |" if total > 0 else f"| ⚪ 平盘 | {flat:,} | - |",
                    "",
                ])
                
                # 涨停跌停统计
                limit_up = sum(1 for q in quotes.values() if q.get('change_pct', 0) >= 9.5)
                limit_down = sum(1 for q in quotes.values() if q.get('change_pct', 0) <= -9.5)
                lines.append(f"📊 **涨停**: {limit_up} 只 | **跌停**: {limit_down} 只")
                lines.append("")
            
            # 3. 强势板块
            lines.extend([
                "### 🏭 强势板块 TOP5",
                "",
                "| 排名 | 板块 | 平均涨幅 | 趋势 |",
                "|------|------|----------|------|",
            ])
            
            # 计算板块表现
            try:
                sector_perf = {}
                for code, data in quotes.items() if quotes else []:
                    sector = data.get('sector', '其他')
                    if sector not in sector_perf:
                        sector_perf[sector] = {'changes': [], 'codes': []}
                    sector_perf[sector]['changes'].append(data.get('change_pct', 0))
                    sector_perf[sector]['codes'].append(code)
                
                # 排序
                sorted_sectors = []
                for sector, info in sector_perf.items():
                    if len(info['changes']) >= 3:  # 至少3只股票
                        avg_change = sum(info['changes']) / len(info['changes'])
                        sorted_sectors.append((sector, avg_change, len(info['codes'])))
                
                sorted_sectors.sort(key=lambda x: x[1], reverse=True)
                
                for i, (sector, avg_change, count) in enumerate(sorted_sectors[:5], 1):
                    trend = "🔥 强势" if avg_change > 3 else "⚡ 活跃" if avg_change > 1 else "📊 震荡"
                    lines.append(f"| {i} | {sector} | {avg_change:+.2f}% | {trend} |")
            except:
                lines.append("| - | 数据计算中 | - | - |")
            
            lines.extend([
                "",
                "----------------------------------------------------------------------",
                "",
            ])
            
            return "\n".join(lines)
            
        except Exception as e:
            logger.warning(f"市场概况生成失败: {e}")
            return ""

    def _generate_detailed_position_report_with_levels(self, quotes: Dict = None) -> str:
        """生成带技术位的详细持仓报告"""
        try:
            from position_manager import position_manager
            from stock_history_db import StockHistoryDB
            from technical_analysis import calculate_technical_levels, format_technical_levels

            # 获取当前持仓
            positions = position_manager.get_all_positions()

            if not positions:
                return ""

            # 如果有新的行情数据，更新持仓价格
            if quotes:
                position_manager.update_prices(quotes)
                positions = position_manager.get_all_positions()

            # 计算汇总
            total_cost = 0
            total_value = 0
            position_details = []

            # 初始化数据库
            history_db = StockHistoryDB()

            for p in positions:
                cost = p.buy_price * p.shares
                value = p.current_price * p.shares
                profit = value - cost
                profit_pct = (p.current_price - p.buy_price) / p.buy_price * 100 if p.buy_price > 0 else 0

                total_cost += cost
                total_value += value

                # 计算技术位
                tech_levels_str = ""
                try:
                    hist_prices = history_db.get_prices(p.code, days=60)
                    if hist_prices and len(hist_prices) >= 20:
                        levels = calculate_technical_levels(hist_prices, p.current_price)
                        tech_levels_str = format_technical_levels(levels, p.current_price)
                    else:
                        tech_levels_str = "   历史数据不足，无法计算技术位"
                except Exception as e:
                    tech_levels_str = "   技术位计算失败"

                position_details.append({
                    'name': p.name,
                    'code': p.code,
                    'buy_price': p.buy_price,
                    'current_price': p.current_price,
                    'shares': p.shares,
                    'cost': cost,
                    'value': value,
                    'profit': profit,
                    'profit_pct': profit_pct,
                    'stop_loss': p.stop_loss,
                    'target_price': p.target_price,
                    'tech_levels': tech_levels_str
                })

            # 按盈亏排序
            position_details.sort(key=lambda x: x['profit_pct'], reverse=True)

            # 计算总盈亏
            total_profit = total_value - total_cost
            total_profit_pct = (total_profit / total_cost * 100) if total_cost > 0 else 0

            # 生成报告
            lines = [
                "## 💼 持仓分析",
                "",
                f"**持仓汇总**: 共{len(positions)}只 | 总成本: ¥{total_cost:,.2f} | 总市值: ¥{total_value:,.2f}",
                f"**总盈亏**: ¥{total_profit:,.2f} ({total_profit_pct:+.2f}%)",
                "",
                "----------------------------------------------------------------------",
                "",
                "### 📈 持仓明细 (按盈亏排序)",
                "",
            ]

            # 使用DeepSeek API生成持仓分析
            try:
                from deepseek_analyzer import stock_picker_analyzer
                position_analyzer = stock_picker_analyzer
            except:
                position_analyzer = None

            for i, p in enumerate(position_details, 1):
                emoji = "🟢" if p['profit_pct'] >= 0 else "🔴"
                profit_pct = p['profit_pct']

                # 生成操作建议标签
                if profit_pct >= 5:
                    suggestion_label = "🟢 建议持有"
                elif profit_pct >= 0:
                    suggestion_label = "⚪ 观望"
                elif profit_pct >= -5:
                    suggestion_label = "⚪ 观望"
                else:
                    suggestion_label = "🟠 关注止损"

                # 使用AI生成详细分析
                if position_analyzer:
                    analysis = position_analyzer.analyze_position(p)
                else:
                    analysis = self._generate_fallback_position_analysis(p)

                lines.extend([
                    f"{i}. {emoji} **{p['name']}** ({p['code']})",
                    f"   - 成本: ¥{p['buy_price']:.3f} → 现价: ¥{p['current_price']:.3f}",
                    f"   - 持仓: {p['shares']:,}股 | 市值: ¥{p['value']:,.2f}",
                    f"   - 盈亏: ¥{p['profit']:,.2f} ({profit_pct:+.2f}%)",
                    f"   - 建议: **{suggestion_label}**",
                    f"   - 说明: {analysis}",
                    f"   - 技术位分析:",
                ])
                
                # 添加技术位详情
                for tech_line in p['tech_levels'].split('\n'):
                    lines.append(f"      {tech_line}")
                
                lines.append("")

            lines.extend([
                "======================================================================",
                "",
            ])

            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"详细持仓报告生成失败: {e}")
            # 降级到基础版本
            return self._generate_detailed_position_report(quotes)

    def generate_premarket_report(self, picks: List[Dict], quotes: Dict = None, strategy_results: Dict = None) -> str:
        """生成盘前报告 - 两段式：持仓分析 + 推荐股"""
        # 根据版本标识设置标题
        version_name = os.getenv('VERSION_NAME', '')
        if version_name:
            subtitle = f" - {version_name}"
        else:
            subtitle = ""
        
        lines = [
            f"═══════════════════════════════════════════════",
            f"🌅 盘前选股报告 1.0{subtitle}",
            f"═══════════════════════════════════════════════",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**选股引擎**: V9板块轮动增强",
            f"**报告引擎**: V10+",
            f"**股票数量**: {len(picks)} 只",
            f"",
            "----------------------------------------------------------------------",
            "",
        ]

        # ========== 第一段：持仓分析（含技术位） ==========
        position_report = self._generate_detailed_position_report_with_levels(quotes)
        if position_report:
            lines.append(position_report)
        else:
            lines.extend([
                "## 💼 持仓分析",
                "",
                "当前无持仓数据",
                "",
            ])

        # ========== 第二段：推荐股 ==========
        lines.extend([
            "## 🎯 V10板块轮动策略 - TOP 推荐",
            "======================================================================",
            f"基于全A股多因子评分（P0优化版）",
            f"因子权重: 技术20% | 情绪10% | 板块35% | 资金20% | 风险15%",
            "----------------------------------------------------------------------",
            "",
        ])

        # 添加板块轮动分析
        sector_count = {}
        for pick in picks:
            sector = pick.get('sector', '其他')
            sector_count[sector] = sector_count.get(sector, 0) + 1

        if sector_count:
            lines.extend([
                "### 📊 板块分布",
                "",
            ])
            for sector, count in sector_count.items():
                leader_tag = "🌟" if any(p.get('is_sector_leader') for p in picks if p.get('sector') == sector) else ""
                lines.append(f"- **{sector}**: {count}只 {leader_tag}")
            lines.extend(["", "---", ""])

        # 使用DeepSeek分析器生成详细选股说明
        try:
            from deepseek_analyzer import stock_picker_analyzer
            market_context = f"当前强势板块: {', '.join(list(sector_count.keys())[:3])}"
        except:
            stock_picker_analyzer = None
            market_context = ""

        for i, pick in enumerate(picks, 1):
            # 生成选股建议
            score = pick['total_score']
            change_pct = pick.get('change_pct', 0)
            price = pick.get('price', 0)

            if score >= 75:
                suggestion = "⭐ 强烈推荐"
            elif score >= 70:
                suggestion = "✅ 推荐关注"
            else:
                suggestion = "📈 可关注"

            # 使用AI生成详细选股说明
            if stock_picker_analyzer:
                analysis = stock_picker_analyzer.analyze_pick(pick, market_context)
            else:
                # 备用分析
                analysis = self._generate_fallback_pick_analysis(pick)

            # ========== 技术位分析（新增）==========
            tech_levels_str = ""
            try:
                from technical_analysis import calculate_technical_levels, format_technical_levels
                from stock_history_db import StockHistoryDB
                
                # 初始化数据库连接
                history_db = StockHistoryDB()
                
                # 获取历史数据（价格列表）
                hist_prices = history_db.get_prices(pick['code'], days=60)
                if hist_prices and len(hist_prices) >= 20:
                    levels = calculate_technical_levels(hist_prices, price)
                    tech_levels_str = format_technical_levels(levels, price)
                    # 保存到pick中供后续使用
                    pick['technical_levels'] = levels.to_dict()
                else:
                    tech_levels_str = "   技术位: 历史数据不足，无法计算"
            except Exception as e:
                logger.warning(f"计算技术位失败 {pick['code']}: {e}")
                tech_levels_str = "   技术位: 计算失败"

            lines.extend([
                f"{i}. ⭐ **{pick['name']}** ({pick['code']})",
                f"   - 当前价: ¥{price:.2f} ({change_pct:+.2f}%)",
                f"   - 综合评分: **{score:.1f}**",
                f"   - 因子得分: 技术{pick.get('factors', {}).get('technical', 'N/A')} | 情绪{pick.get('factors', {}).get('sentiment', 'N/A')} | 板块{pick.get('factors', {}).get('sector', 'N/A')} | 资金{pick.get('factors', {}).get('money_flow', 'N/A')} | 风险{pick.get('factors', {}).get('risk', 'N/A')} | 舆情{pick.get('factors', {}).get('news_sentiment', 'N/A')}",
                f"   - 所属板块: {pick.get('sector', 'N/A')}",
                f"   - 选股说明: **{suggestion}**，{analysis}",
            ])
            
            # 添加技术位详情（新增）
            if tech_levels_str:
                lines.append(f"   - 技术位分析:")
                for tech_line in tech_levels_str.split('\n'):
                    lines.append(f"      {tech_line}")
            
            lines.append("")

        lines.extend([
            "======================================================================",
            "",
            "⚠️ **风险提示**: 以上分析仅供参考，不构成投资建议。",
            f"",
            "---",
            f"*股票分析项目 1.2 | V10-P0优化 | 三策略融合*"
        ])
        
        # 添加策略报告
        if strategy_results:
            strategy_section = self._generate_strategy_section(strategy_results)
            if strategy_section:
                # 在风险提示之前插入策略部分
                lines.insert(-4, strategy_section)

        # ========== 记录到飞书多维表格 ==========
        try:
            from feishu_bitable_tracker import record_stock_pick
            import os
            
            # 获取策略版本（从环境变量或默认V10）
            strategy_version = os.getenv('STRATEGY_VERSION', 'V10')
            today = datetime.now().strftime('%Y-%m-%d')
            
            for pick in picks:
                record_stock_pick(
                    strategy=strategy_version,
                    code=pick['code'],
                    name=pick['name'],
                    price=pick.get('price', 0),
                    pick_date=today
                )
        except Exception as e:
            logger.warning(f"记录到飞书表格失败: {e}")

        return "\n".join(lines)

    def generate_postmarket_report(self, picks: List[Dict] = None, 
                                    opening_prices: Dict = None,
                                    market_stats: Dict = None,
                                    strategy_results: Dict = None) -> str:
        """
        生成盘后报告 - 优化版（精简冗余）
        
        结构：
        1. 市场回顾 + 推荐表现（合并）
        2. 个股明细表格
        3. 诊断总结（合并诊断+建议+AI分析）
        4. 持仓分析
        """
        lines = [
            f"# 🌇 盘后复盘报告",
            f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')} | **策略**: V10-P0优化",
            f"",
        ]
        
        # ===== 第一段：市场回顾 + 推荐表现（合并）=====
        market_avg = market_stats.get('avg_change', 0) if market_stats else 0
        
        if market_stats and picks:
            premarket_count = sum(1 for p in picks if p.get('type') == '盘前')
            intraday_count = sum(1 for p in picks if p.get('type') == '盘中')
            
            # 计算实际表现
            for pick in picks:
                code = pick['code']
                morning_price = pick.get('price', 0)
                open_price = morning_price
                
                if opening_prices:
                    for key, data in opening_prices.items():
                        if key.strip().replace('\n', '') == code or key.strip().replace('\n', '').endswith(code):
                            open_price = data.get('open_price', morning_price)
                            break
                
                if morning_price > 0 and open_price > 0:
                    pick['actual_return'] = round((open_price - morning_price) / morning_price * 100, 2)
                else:
                    pick['actual_return'] = 0
            
            avg_return = sum(p['actual_return'] for p in picks) / len(picks)
            success_count = sum(1 for p in picks if p['actual_return'] > 0)
            best = max(picks, key=lambda x: x['actual_return'])
            worst = min(picks, key=lambda x: x['actual_return'])
            
            lines.extend([
                f"## 📊 市场回顾 & 策略表现",
                f"",
                f"**市场环境**: 大盘{market_avg:+.2f}% | 涨{market_stats.get('up', 0)}/跌{market_stats.get('down', 0)} | 涨停{market_stats.get('limit_up', 0)}",
                f"**推荐表现**: 共{len(picks)}只(盘前{premarket_count}/盘中{intraday_count}) | 平均{avg_return:+.2f}% | 胜率{success_count/len(picks)*100:.0f}%",
                f"**最佳**: {best['name']} {best['actual_return']:+.2f}% | **最差**: {worst['name']} {worst['actual_return']:+.2f}%",
                f"",
            ])
        
        # ===== 第二段：个股明细表格 =====
        if picks:
            lines.extend([
                f"## 📈 推荐股明细",
                f"",
                "| 股票 | 类型 | 涨跌 | 评分 | 表现 |",
                "|------|------|------|------|------|",
            ])
            
            for pick in picks:
                ret = pick.get('actual_return', 0)
                emoji = "🔥" if ret >= 5 else "✅" if ret >= 0 else "⚠️" if ret >= -3 else "❌"
                perf = "优秀" if ret >= 5 else "良好" if ret >= 2 else "持平" if ret >= 0 else "回撤" if ret >= -3 else "不佳"
                
                lines.append(
                    f"| {pick['name']}({pick['code']}) | {pick.get('type', '盘前')} | {ret:+.2f}% | {pick['total_score']:.0f} | {emoji} {perf} |"
                )
            
            lines.append("")
        
        # ===== 第三段：诊断总结（合并）=====
        if picks:
            avg_return = sum(p['actual_return'] for p in picks) / len(picks)
            
            # 诊断
            if avg_return > 2:
                diag = "🟢 策略优秀，方向契合主线"
            elif avg_return > 0:
                diag = "🟡 策略小盈，有优化空间"
            elif avg_return > -2:
                diag = "🟠 策略持平，需调方向"
            else:
                diag = "🔴 策略不佳，审视因子"
            
            lines.extend([
                f"## 🔍 诊断 & 建议",
                f"",
                f"**{diag}**",
                f"",
                f"**改进方向**: ①板块轮动跟踪 ②止盈止损纪律 ③风险分散 ④尾盘确认",
                f"",
            ])
            
            # 简化的AI分析（仅表现最差的1-2只）
            underperformers = sorted(
                [p for p in picks if p['actual_return'] < market_avg],
                key=lambda x: x['actual_return']
            )[:2]  # 最多分析2只最差的
            
            if underperformers:
                lines.extend([f"**重点关注**:", ""])
                for pick in underperformers:
                    lines.append(f"- {pick['name']}({pick['code']}): {pick['actual_return']:+.2f}%，评分{pick['total_score']}分，{pick.get('sector', '其他')}板块")
                lines.append("")
            
            # ===== DeepSeek AI深度分析 =====
            lines.extend([
                f"## 🤖 DeepSeek AI深度分析",
                f"",
            ])
            
            try:
                # 格式化数据
                historical_text = format_picks_for_deepseek(picks, opening_prices)
                
                # 调用DeepSeek分析
                ai_analysis = analyze_with_deepseek(historical_text)
                
                # 添加AI分析结果
                lines.append(ai_analysis)
                lines.append("")
                
            except Exception as e:
                logger.error(f"DeepSeek分析集成失败: {e}")
                lines.append(f"⚠️ AI分析暂时不可用: {str(e)[:50]}")
                lines.append("")
        
        # ===== 第四段：持仓分析 =====
        position_report = self._generate_detailed_position_report(opening_prices)
        if position_report:
            lines.append(position_report)
        
        # ========== 更新飞书多维表格收盘价 ==========
        try:
            from feishu_bitable_tracker import update_stock_prices
            import os
            
            strategy_version = os.getenv('STRATEGY_VERSION', 'V10')
            today = datetime.now().strftime('%Y-%m-%d')
            
            for pick in picks:
                if pick.get('type') == '盘前':
                    # 获取收盘价（如果有）
                    code = pick['code']
                    open_price = None
                    
                    if opening_prices:
                        for key, data in opening_prices.items():
                            if key.strip().replace('\n', '') == code or key.strip().replace('\n', '').endswith(code):
                                open_price = data.get('open_price')
                                break
                    
                    # 使用实际收益反推出收盘价
                    morning_price = pick.get('price', 0)
                    actual_return = pick.get('actual_return', 0)
                    if morning_price > 0 and actual_return != 0:
                        close_price = morning_price * (1 + actual_return / 100)
                        
                        # 更新第1天收盘价
                        update_stock_prices(
                            strategy=strategy_version,
                            pick_date=today,
                            code=code,
                            day1=round(close_price, 2)
                        )
        except Exception as e:
            logger.warning(f"更新飞书表格收盘价失败: {e}")

        # 结尾
        lines.extend([
            f"---",
            f"*V10-P0优化 | 盘后复盘*",
        ])
        
        return "\n".join(lines)


# ============================================================================
# 通知管理
# ============================================================================

class NotificationManager:
    """通知管理器"""

    def send(self, title: str, content: str):
        """发送通知"""
        webhook = os.getenv("FEISHU_WEBHOOK")
        if not webhook:
            logger.warning("未配置FEISHU_WEBHOOK，跳过推送")
            return

        try:
            import requests
            response = requests.post(
                webhook,
                json={"msg_type": "text", "content": {"text": f"{title}\n\n{content[:2000]}"}},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"✅ 推送已发送: {title}")
        except Exception as e:
            logger.error(f"推送失败: {e}")

    def send_in_parts(self, title: str, content: str, max_length: int = 2500):
        """分段发送长内容 - 按主要章节分割"""
        webhook = os.getenv("FEISHU_WEBHOOK")
        if not webhook:
            logger.warning("未配置FEISHU_WEBHOOK，跳过推送")
            return

        try:
            import requests
            import time

            # 按主要章节分割（## 标题）
            lines = content.split('\n')
            parts = []
            current_part = []

            # 需要合并的标题对：(前一段标题关键词, 后一段标题关键词)
            merge_pairs = [
                ('今日市场回顾', '今日推荐复盘'),
            ]

            for line in lines:
                # 遇到一级标题（## ）时，判断是否与前一段合并
                if line.startswith('## ') and current_part:
                    # 检查是否需要与下一段合并
                    prev_title = ''
                    for l in current_part:
                        if l.startswith('## '):
                            prev_title = l[3:].strip()
                            break
                    current_title = line[3:].strip()

                    # 判断是否需要合并
                    should_merge = any(
                        pair[0] in prev_title and pair[1] in current_title
                        for pair in merge_pairs
                    )

                    if should_merge:
                        # 合并：继续追加到当前段
                        current_part.append(line)
                    else:
                        # 不合并：保存当前段并开始新段
                        parts.append('\n'.join(current_part))
                        current_part = [line]
                else:
                    current_part.append(line)

            # 添加最后一段
            if current_part:
                parts.append('\n'.join(current_part))

            # 过滤空段并合并过短段落
            filtered_parts = []
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                # 如果段落太短（少于200字符），尝试与上一段合并
                if len(part) < 200 and filtered_parts:
                    filtered_parts[-1] += '\n\n' + part
                else:
                    filtered_parts.append(part)

            total_parts = len(filtered_parts)

            for i, part in enumerate(filtered_parts, 1):
                part_title = f"{title} | 第{i}/{total_parts}段"
                message = f"{part_title}\n{'='*60}\n{part}"

                # 截断超长内容
                if len(message) > max_length:
                    message = message[:max_length] + "\n...(内容已截断)"

                response = requests.post(
                    webhook,
                    json={"msg_type": "text", "content": {"text": message}},
                    timeout=10
                )
                response.raise_for_status()
                logger.info(f"✅ 分段推送已发送: {part_title}")

                # 避免发送过快
                if i < total_parts:
                    time.sleep(0.5)

        except Exception as e:
            logger.error(f"分段推送失败: {e}")


# ============================================================================
# 报告存储
# ============================================================================

class ReportStorage:
    """报告存储管理"""
    
    def __init__(self, base_path: str = None):
        if base_path is None:
            # 优先使用环境变量，否则使用默认路径
            base_path = os.getenv('STOCK_REPORTS_DIR', os.path.join(os.path.dirname(__file__), 'daily_reports'))
        self.base_path = base_path
        self._ensure_dirs()
    
    def _ensure_dirs(self):
        """确保目录存在"""
        for subdir in ['premarket', 'intraday', 'postmarket', 'summary']:
            path = os.path.join(self.base_path, subdir)
            os.makedirs(path, exist_ok=True)
    
    def save(self, content: str, report_type: str) -> str:
        """保存报告"""
        timestamp = datetime.now().strftime('%H%M%S')
        date = datetime.now().strftime('%Y%m%d')
        filename = f"{date}_{timestamp}_{report_type}_v10.md"
        
        filepath = os.path.join(self.base_path, report_type, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"📝 报告已保存: {filepath}")
        return filepath


# ============================================================================
# 主系统
# ============================================================================

class StockAnalysisSystem:
    """
    股票分析系统 1.0
    ================
    V9选股 + V10+报告 + 三策略融合
    """

    def __init__(self):
        self.version = VERSION
        self.screener = StockScreenerV9()
        self.reporter = ReportGeneratorV10Plus()
        self.notifier = NotificationManager()
        self.storage = ReportStorage()
        
        # 初始化三策略管理器
        try:
            from strategy_manager import StrategyManager
            self.strategy_manager = StrategyManager()
            logger.info("✅ 策略管理器初始化完成 (突破+网格+双动量)")
        except Exception as e:
            logger.warning(f"⚠️ 策略管理器初始化失败: {e}")
            self.strategy_manager = None

        logger.info(f"股票分析系统 {self.version} 初始化完成")

    def run_premarket(self, top_n: int = 3, send: bool = True) -> Dict:
        """
        盘前分析 - V12单策略版本

        Args:
            top_n: 选股数量
            send: 是否发送推送

        Returns:
            分析结果
        """
        print("\n" + "="*80)
        print(f"🚀 股票分析系统 V12 - 盘前分析")
        print("="*80)

        # 1. V12选股
        logger.info("Step 1: V12选股")
        try:
            from strategies.v12_strategy import V12Strategy
            from config import DB_CONFIG
            import pymysql
            
            v12 = V12Strategy()
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 使用V12策略选股
            picks = v12.select(date=today, top_n=top_n)
            
            if not picks:
                logger.error("V12选股失败")
                return {'success': False, 'error': '选股失败'}
            
            logger.info(f"✅ V12选股完成: {len(picks)}只")
            
        except Exception as e:
            logger.error(f"V12选股失败: {e}")
            return {'success': False, 'error': str(e)}

        # 2. 获取实时行情
        quotes = {}
        try:
            codes = [p['code'] for p in picks]
            # 从数据库获取行情
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT code, close, open, high, low, volume, turnover, pct_change
                    FROM stock_kline WHERE code IN (%s) AND trade_date = (
                        SELECT MAX(trade_date) FROM stock_kline
                    )
                """ % ','.join(['%s'] * len(codes)), tuple(codes))
                for row in cursor.fetchall():
                    quotes[row[0]] = {
                        'price': float(row[1]),
                        'open': float(row[2]),
                        'high': float(row[3]),
                        'low': float(row[4]),
                        'volume': float(row[5]),
                        'turnover': float(row[6]),
                        'change_pct': float(row[7])
                    }
            conn.close()
        except Exception as e:
            logger.warning(f"获取行情失败: {e}")

        # 3. 生成V12报告
        logger.info("Step 2: V12生成报告")
        report = self._generate_v12_premarket_report(picks, quotes)

        # 4. 保存报告
        logger.info("Step 3: 保存报告")
        filepath = self.storage.save(report, 'premarket')

        # 5. 发送推送
        if send:
            logger.info("Step 4: 发送推送")
            title = "═══════════════════════\n🌅 盘前选股报告 V12\n═══════════════════════"
            self.notifier.send_in_parts(title, report)
        
        print(f"\n✅ 盘前分析完成: {len(picks)}只股票")
        print("="*80)
        
        return {
            'success': True,
            'picks': picks,
            'report_path': filepath,
            'report_content': report
        }
    
    def _generate_v12_premarket_report(self, picks: List[Dict], quotes: Dict = None) -> str:
        """生成V12盘前报告"""
        lines = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        lines.append(f"# 🌅 盘前选股报告 V12 - {today}")
        lines.append("")
        lines.append("## 📊 V12策略说明")
        lines.append("")
        lines.append("**因子配置：**")
        lines.append("- 趋势因子 25%：MA20/MA60趋势 + 相对位置")
        lines.append("- 动量因子 25%：20日涨幅（0-40%最优）")
        lines.append("- 质量因子 20%：低波动率优先")
        lines.append("- 情绪因子 20%：当日涨跌判断")
        lines.append("- 估值因子 10%：基础分")
        lines.append("")
        lines.append("**硬性过滤：**")
        lines.append("- 股价 5-150元")
        lines.append("- 成交额 ≥1亿元")
        lines.append("- 股价在MA20之上")
        lines.append("- 排除ST/退市股")
        lines.append("")
        
        lines.append("## 🎯 今日推荐")
        lines.append("")
        lines.append("| 排名 | 代码 | 名称 | 总分 | 趋势 | 动量 | 质量 | 情绪 | 昨日价 | 涨跌幅 |")
        lines.append("|------|------|------|------|------|------|------|------|--------|--------|")
        
        for i, pick in enumerate(picks, 1):
            code = pick['code']
            name = pick['name']
            score = pick['score']
            factors = pick.get('factors', {})
            
            trend = factors.get('trend', 0)
            momentum = factors.get('momentum', 0)
            quality = factors.get('quality', 0)
            sentiment = factors.get('sentiment', 0)
            
            price = pick.get('price', 0)
            change_pct = pick.get('change_pct', 0)
            
            lines.append(f"| {i} | {code} | {name} | {score:.1f} | {trend:.0f} | {momentum:.0f} | {quality:.0f} | {sentiment:.0f} | {price:.2f} | {change_pct:+.2f}% |")
        
        lines.append("")
        lines.append("## 💡 操作建议")
        lines.append("")
        lines.append("1. **买入时机**：开盘后观察5分钟，若无大幅高开（>3%），可分批买入")
        lines.append("2. **止损设置**：-5%硬性止损")
        lines.append("3. **止盈策略**：次日开盘卖出（V12回测显示一日策略最优）")
        lines.append("4. **仓位管理**：单票不超过总资金33%")
        lines.append("")
        lines.append("---")
        lines.append(f"*报告生成时间: {datetime.now().strftime('%H:%M:%S')}*")
        
        return '\n'.join(lines)
    
    def _load_today_premarket_picks(self) -> List[Dict]:
        """加载今天盘前选股的结果"""
        import glob
        import re
        
        today = datetime.now().strftime('%Y%m%d')
        premarket_dir = os.path.join(self.storage.base_path, 'premarket')
        
        if not os.path.exists(premarket_dir):
            return []
        
        # 查找今天的盘前报告
        pattern = os.path.join(premarket_dir, f'{today}_*_premarket_*.md')
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"未找到今天({today})的盘前报告")
            return []
        
        # 取最新的文件
        latest_file = max(files, key=os.path.getctime)
        logger.info(f"找到盘前报告: {latest_file}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 解析报告中的选股数据
            picks = []
            
            # 查找推荐股票部分（格式：1. ⭐ **股票名** (代码)）
            stock_pattern = r'\d+\.\s+⭐\s+\*\*(.+?)\*\*\s+\((\d{6})\)'
            matches = re.findall(stock_pattern, content)
            
            for name, code in matches:
                # 尝试提取该股票的更多信息
                # 查找当前价：- 当前价: ¥xx.xx (xxx%)
                price_pattern = rf'{re.escape(name)}.*?当前价:\s*¥([\d.]+)'
                price_match = re.search(price_pattern, content, re.DOTALL)
                price = float(price_match.group(1)) if price_match else 0
                
                # 查找评分：- 综合评分: **xx.x**
                score_pattern = rf'{re.escape(name)}.*?综合评分:\s*\*\*([\d.]+)\*\*'
                score_match = re.search(score_pattern, content, re.DOTALL)
                score = float(score_match.group(1)) if score_match else 70
                
                # 查找所属板块
                sector_pattern = rf'{re.escape(name)}.*?所属板块:\s*(\S+)'
                sector_match = re.search(sector_pattern, content, re.DOTALL)
                sector = sector_match.group(1) if sector_match else '其他'
                
                # 查找因子得分 - 因子得分: 技术23 | 情绪16 | 板块17 | 资金8 | 风险7
                factors_pattern = rf'{re.escape(name)}.*?因子得分:\s*技术([\d]+)\s*\|\s*情绪([\d]+)\s*\|\s*板块([\d]+)\s*\|\s*资金([\d]+)\s*\|\s*风险([\d]+)'
                factors_match = re.search(factors_pattern, content, re.DOTALL)
                if factors_match:
                    tech, emotion, sector_score, money, risk = factors_match.groups()
                    factors_str = f"技术:{tech} 情绪:{emotion} 板块:{sector_score} 资金:{money} 风险:{risk}"
                else:
                    factors_str = 'N/A'
                
                pick = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'sector': sector,
                    'total_score': score,
                    'factors_str': factors_str,
                    'type': '盘前',
                    'reason': f"盘前选股，评分{score}"
                }
                picks.append(pick)
            
            logger.info(f"从盘前报告中解析出 {len(picks)} 只推荐股")
            for p in picks:
                logger.info(f"  {p['name']}({p['code']}): ¥{p['price']} 评分{p['total_score']}")
            return picks
            
        except Exception as e:
            logger.error(f"读取盘前报告失败: {e}")
            return []
    
    def _load_today_intraday_picks(self) -> List[Dict]:
        """加载今天盘中选股的结果"""
        import glob
        import re
        
        today = datetime.now().strftime('%Y%m%d')
        intraday_dir = os.path.join(self.storage.base_path, 'intraday')
        
        if not os.path.exists(intraday_dir):
            return []
        
        # 查找今天的盘中报告
        pattern = os.path.join(intraday_dir, f'{today}_*_intraday_*.md')
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"未找到今天({today})的盘中报告")
            return []
        
        # 取最新的文件
        latest_file = max(files, key=os.path.getctime)
        logger.info(f"找到盘中报告: {latest_file}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 解析报告中的选股数据
            picks = []
            
            # 查找下午选股部分（格式：1. 🚀 强烈推荐 **股票名** (代码)）
            stock_pattern = r'\d+\.\s+🚀\s+强烈推荐\s+\*\*(.+?)\*\*\s+\((\d{6})\)'
            matches = re.findall(stock_pattern, content)
            
            for name, code in matches:
                # 查找当前价
                price_pattern = rf'{re.escape(name)}.*?当前价:\s*¥([\d.]+)'
                price_match = re.search(price_pattern, content, re.DOTALL)
                price = float(price_match.group(1)) if price_match else 0
                
                # 查找评分
                score_pattern = rf'{re.escape(name)}.*?综合评分:\s*\*\*([\d]+)\*\*'
                score_match = re.search(score_pattern, content, re.DOTALL)
                score = float(score_match.group(1)) if score_match else 85
                
                # 查找所属板块
                sector_pattern = rf'{re.escape(name)}.*?所属板块:\s*(\S+)'
                sector_match = re.search(sector_pattern, content, re.DOTALL)
                sector = sector_match.group(1) if sector_match else '其他'
                
                pick = {
                    'code': code,
                    'name': name,
                    'price': price,
                    'sector': sector,
                    'total_score': score,
                    'type': '盘中',
                    'factors_str': '技术:25 情绪:20 板块:20 资金:15 风险:7',
                    'reason': f"盘中选股，评分{score}"
                }
                picks.append(pick)
            
            logger.info(f"从盘中报告中解析出 {len(picks)} 只推荐股")
            for p in picks:
                logger.info(f"  {p['name']}({p['code']}): ¥{p['price']} 评分{p['total_score']}")
            return picks
            
        except Exception as e:
            logger.error(f"读取盘中报告失败: {e}")
            return []
    
    def run_postmarket(self, picks: List[Dict] = None, send: bool = True) -> Dict:
        """
        盘后分析 - 深度复盘盘前+盘中推荐股
        
        Args:
            picks: 选股结果（用于复盘，如未提供则自动读取今天报告）
            send: 是否发送推送
            
        Returns:
            分析结果
        """
        print("\n" + "=" * 80)
        print(f"🚀 股票分析系统 1.2 (V10-P0优化) - 盘后深度复盘")
        print("=" * 80)
        
        # Step 0: 读取今天盘前和盘中报告
        all_picks = []
        if picks is None:
            logger.info("Step 0: 读取今天盘前选股记录")
            premarket_picks = self._load_today_premarket_picks()
            if premarket_picks:
                for p in premarket_picks:
                    p['type'] = '盘前'
                all_picks.extend(premarket_picks)
                logger.info(f"✅ 读取盘前推荐: {len(premarket_picks)}只")
            
            logger.info("Step 0b: 读取今天盘中选股记录")
            intraday_picks = self._load_today_intraday_picks()
            if intraday_picks:
                for p in intraday_picks:
                    p['type'] = '盘中'
                all_picks.extend(intraday_picks)
                logger.info(f"✅ 读取盘中推荐: {len(intraday_picks)}只")
            
            picks = all_picks
            logger.info(f"✅ 总共读取: {len(picks)}只推荐股")
        
        # Step 1: 获取收盘行情（多数据源备份）
        logger.info("Step 1: 获取收盘行情数据（多数据源备份）")
        closing_quotes = {}
        market_stats = {}
        
        # 获取今日收盘价数据（盘后分析应该用收盘价而非开盘价）
        logger.info("Step 1a: 获取今日收盘价数据")
        closing_prices = {}
        if picks:
            try:
                from sina_finance_api import SinaFinanceAPI
                sina = SinaFinanceAPI()
                pick_codes = [p['code'] for p in picks]
                
                # 获取实时数据，其中包含收盘价（盘后即为收盘价）
                df = sina.get_realtime_quotes(pick_codes)
                if not df.empty:
                    for _, row in df.iterrows():
                        code = str(row['code']).zfill(6)
                        # 新浪数据中的price字段在盘后即为收盘价
                        close_price = row.get('price', 0)
                        open_price = row.get('open', 0)
                        if close_price > 0:
                            closing_prices[code] = {
                                'name': row.get('name', code),
                                'open_price': open_price,  # 保留开盘价用于计算
                                'current_price': close_price,  # 盘后即为收盘价
                                'close_price': close_price  # 明确标识为收盘价
                            }
                    logger.info(f"✅ 获取到 {len(closing_prices)} 只股票的收盘数据")
            except Exception as e:
                logger.warning(f"⚠️ 获取收盘价失败: {e}")
        
        # 获取多数据源行情（用于市场统计）
        all_stocks, index_data, data_source = self._get_intraday_data_with_fallback()
        
        if all_stocks:
            # 计算市场统计
            up_count = sum(1 for s in all_stocks if s.change_percent > 0)
            down_count = sum(1 for s in all_stocks if s.change_percent < 0)
            limit_up = sum(1 for s in all_stocks if s.change_percent >= 9.5)
            limit_down = sum(1 for s in all_stocks if s.change_percent <= -9.5)
            avg_change = sum(s.change_percent for s in all_stocks) / len(all_stocks) if all_stocks else 0
            
            market_stats = {
                'up': up_count,
                'down': down_count,
                'limit_up': limit_up,
                'limit_down': limit_down,
                'avg_change': avg_change
            }
            logger.info(f"✅ 市场统计: 涨{up_count}/跌{down_count}/涨停{limit_up} (数据源: {data_source})")
            
            # 获取推荐股的收盘数据（如果新浪接口没获取到）
            if picks:
                for pick in picks:
                    code = pick['code']
                    if code not in closing_prices:
                        # 从all_stocks中查找
                        for stock in all_stocks:
                            if stock.code == code:
                                closing_prices[code] = {
                                    'name': stock.name,
                                    'open_price': stock.price,  # 用现价代替
                                    'current_price': stock.price,
                                    'close_price': stock.price  # 明确标识为收盘价
                                }
                                break
                
                logger.info(f"✅ 获取到 {len(closing_prices)} 只推荐股的收盘数据")
        
        # Step 2: 准备报告数据
        logger.info("Step 2: 准备报告数据")
        
        # Step 3: 生成V10+盘后报告
        logger.info("Step 3: 生成V10+盘后深度复盘报告")
        report = self.reporter.generate_postmarket_report(
            picks=picks,
            opening_prices=closing_prices,  # 传递收盘价数据（保持参数名兼容）
            market_stats=market_stats
        )
        
        # Step 4: 保存报告
        filepath = self.storage.save(report, 'postmarket')
        
        # Step 5: 发送推送
        if send:
            self.notifier.send_in_parts("🌇 盘后复盘报告 1.0", report)
        
        print(f"\n✅ 盘后分析完成")
        print("=" * 80)
        
        return {
            'success': True,
            'picks': picks,
            'closing_quotes': closing_quotes,
            'market_stats': market_stats,
            'report_path': filepath,
            'report_content': report
        }
    
    def _get_intraday_data_with_fallback(self) -> tuple:
        """
        获取盘中数据（多数据源备份）
        
        优先级：
        1. AkShare (东方财富) - 数据最全
        2. 腾讯财经 - 简单稳定
        3. 新浪财经 - 最终备用
        
        Returns:
            (all_stocks_list, index_data_list, data_source_name)
        """
        from data_source import data_manager, StockData
        import pandas as pd
        
        all_stocks = []
        index_data = []
        
        # ========== 尝试 1: AkShare ==========
        logger.info("[数据源] 尝试 AkShare...")
        try:
            source = data_manager.get_source('akshare')
            if source and source.enabled:
                all_stocks = source.get_a_stock_spot()
                index_data = source.get_index_spot()
                if all_stocks and len(all_stocks) > 1000:
                    logger.info(f"✅ [数据源] AkShare 成功: {len(all_stocks)}只股票, {len(index_data)}个指数")
                    return all_stocks, index_data, "AkShare"
        except Exception as e:
            logger.warning(f"⚠️ [数据源] AkShare 失败: {e}")
        
        # ========== 尝试 2: 腾讯财经 ==========
        logger.info("[数据源] 尝试 腾讯财经...")
        try:
            from tencent_datasource import TencentDataSource
            tencent = TencentDataSource()
            
            # 获取全A股代码列表
            stock_codes = self.screener.all_stocks if hasattr(self.screener, 'all_stocks') else []
            if stock_codes:
                # 分批获取（腾讯接口一次最多约800只）
                batch_size = 800
                all_quotes = []
                for i in range(0, len(stock_codes), batch_size):
                    batch = stock_codes[i:i+batch_size]
                    df = tencent.get_realtime_quotes(batch)
                    if not df.empty:
                        for _, row in df.iterrows():
                            stock = StockData(
                                code=str(row['code']).zfill(6),
                                name=row['name'],
                                price=float(row['price']) if pd.notna(row['price']) else 0.0,
                                change_percent=float(row['change_percent']) if pd.notna(row['change_percent']) else 0.0,
                                volume=int(row['volume']) if 'volume' in row and pd.notna(row['volume']) else 0,
                                turnover=0.0,
                                market_cap=None,
                                pe=None,
                                pb=None
                            )
                            all_quotes.append(stock)
                    import time
                    time.sleep(0.3)  # 避免请求过快
                
                if len(all_quotes) > 100:
                    logger.info(f"✅ [数据源] 腾讯财经 成功: {len(all_quotes)}只股票")
                    
                    # 获取指数
                    index_df = tencent.get_index_quotes()
                    index_list = []
                    if not index_df.empty:
                        for _, row in index_df.iterrows():
                            from data_source import MarketOverview
                            idx = MarketOverview(
                                index_name=row['name'],
                                index_code=str(row['code']).zfill(6),
                                price=float(row['price']),
                                change_percent=float(row['change_percent']),
                                volume=0,
                                up_count=0,
                                down_count=0
                            )
                            index_list.append(idx)
                    
                    return all_quotes, index_list, "腾讯财经"
        except Exception as e:
            logger.warning(f"⚠️ [数据源] 腾讯财经 失败: {e}")
        
        # ========== 尝试 3: 新浪财经 ==========
        logger.info("[数据源] 尝试 新浪财经...")
        try:
            from sina_finance_api import SinaFinanceAPI
            sina = SinaFinanceAPI()
            
            # 获取全A股代码列表
            stock_codes = self.screener.all_stocks if hasattr(self.screener, 'all_stocks') else []
            if stock_codes:
                # 分批获取
                batch_size = 800
                all_quotes = []
                for i in range(0, len(stock_codes), batch_size):
                    batch = stock_codes[i:i+batch_size]
                    df = sina.get_realtime_quotes(batch)
                    if not df.empty:
                        for _, row in df.iterrows():
                            stock = StockData(
                                code=str(row['code']).zfill(6),
                                name=row.get('name', ''),
                                price=float(row['price']) if pd.notna(row['price']) else 0.0,
                                change_percent=float(row['change_percent']) if pd.notna(row['change_percent']) else 0.0,
                                volume=int(row['volume']) if 'volume' in row and pd.notna(row['volume']) else 0,
                                turnover=0.0,
                                market_cap=None,
                                pe=None,
                                pb=None
                            )
                            all_quotes.append(stock)
                    import time
                    time.sleep(0.5)  # 避免请求过快
                
                if len(all_quotes) > 100:
                    logger.info(f"✅ [数据源] 新浪财经 成功: {len(all_quotes)}只股票")
                    return all_quotes, [], "新浪财经"
        except Exception as e:
            logger.warning(f"⚠️ [数据源] 新浪财经 失败: {e}")
        
        logger.error("❌ [数据源] 所有数据源均失败")
        return [], [], "None"
    
    def run_intraday(self, mode: str = 'noon', send: bool = True) -> Dict:
        """
        盘中分析 - 上午收盘总结 + 下午选股分析
        
        Args:
            mode: noon(午间)/afternoon(下午)
            send: 是否发送推送
            
        Returns:
            分析结果
        """
        from data_source import data_manager
        from sector_first_screener import SectorFirstScreener
        import pandas as pd
        
        title = "午间简报" if mode == 'noon' else "下午简报"
        print(f"\n" + "="*80)
        print(f"🕐 {title} 1.2 (V10完整版) - 上午收盘总结 + 下午选股")
        print("="*80)
        print("📊 选股引擎: V10多因子评分 (含60天历史数据+舆情因子)")
        print("📈 因子权重: 技术20% | 情绪3% | 板块33% | 资金19% | 风险15% | 舆情7%")
        print("="*80)
        
        # ============ 1. 获取数据（多数据源备份） ============
        logger.info("Step 1: 获取大盘数据（多数据源备份）")
        
        all_stocks, index_data, data_source = self._get_intraday_data_with_fallback()
        
        if not all_stocks:
            logger.error("❌ 无法获取行情数据，所有数据源均失败")
            # 生成错误报告
            error_report = self._generate_error_report(title, "无法获取实时行情数据，请检查网络连接或稍后重试")
            filepath = self.storage.save(error_report, 'intraday')
            if send:
                self.notifier.send_in_parts(f"📊 {title} 1.0", error_report)
            return {
                'success': False,
                'error': '所有数据源均失败',
                'report_path': filepath
            }
        
        logger.info(f"✅ 使用数据源: {data_source}, 获取到 {len(all_stocks)} 只股票")
        
        # 计算涨跌分布
        up_count = sum(1 for s in all_stocks if s.change_percent > 0)
        down_count = sum(1 for s in all_stocks if s.change_percent < 0)
        flat_count = len(all_stocks) - up_count - down_count
        
        # 涨跌停统计
        limit_up = sum(1 for s in all_stocks if s.change_percent >= 9.5)
        limit_down = sum(1 for s in all_stocks if s.change_percent <= -9.5)
        
        # 成交量分析
        total_volume = sum(s.volume for s in all_stocks) / 1e8  # 转换为亿手
        avg_change = sum(s.change_percent for s in all_stocks) / len(all_stocks) if all_stocks else 0
        
        # ============ 2. 下午选股分析 ============
        logger.info("Step 2: 执行下午选股分析（V10-P0优化）")
        
        # P0新增：Step 2.0 - 大盘择时分析
        logger.info("Step 2.0: 大盘择时分析")
        market_timing = MarketTiming()
        market_context = market_timing.analyze()
        logger.info(f"  市场环境: {market_context.get('reason', '未知')}")
        logger.info(f"  阈值调整: +{market_context.get('threshold_adjust', 0)}分")
        logger.info(f"  最大选股数: {market_context.get('max_picks', 3)}只")
        
        afternoon_picks = []
        strong_sectors = []
        
        if all_stocks:
            # 手动计算板块表现（使用实际数据）
            sector_performance = self._calculate_sector_performance(all_stocks)
            strong_sectors = sorted(sector_performance, key=lambda x: x['score'], reverse=True)[:5]
            
            # V10完整版：使用多因子评分（含历史数据）
            logger.info("Step 2.1: 从本地数据库加载历史数据...")
            afternoon_picks = self._select_afternoon_opportunities_v10(
                all_stocks, strong_sectors, market_context
            )
            
            logger.info(f"✅ V10盘中选股完成，选出 {len(afternoon_picks)} 只 (使用60天历史数据)")
        
        # ============ 3. 生成报告 ============
        logger.info("Step 3: 生成盘中简报 (V10完整版)")
        report = self._generate_intraday_report(
            title=title,
            index_data=index_data,
            market_stats={
                'up': up_count, 'down': down_count, 'flat': flat_count,
                'limit_up': limit_up, 'limit_down': limit_down,
                'total_volume': total_volume, 'avg_change': avg_change
            },
            strong_sectors=strong_sectors,
            afternoon_picks=afternoon_picks,
            quotes=all_stocks,
            data_source=data_source
        )
        
        # 保存报告
        filepath = self.storage.save(report, 'intraday')
        
        # 发送推送
        if send:
            self.notifier.send_in_parts(f"📊 {title} 1.2 (V10-P0)", report)
        
        print(f"\n✅ {title}完成 - 选出 {len(afternoon_picks)} 只下午潜力股")
        print(f"   选股逻辑: V10多因子评分 + 60天历史数据")
        print("="*80)
        
    def _calculate_sector_performance(self, stocks: List) -> List[Dict]:
        """计算板块表现（基于股票列表）"""
        # 板块映射（代码前缀对应行业）
        sector_map = {
            '人工智能': ['000938', '002230', '002415', '300033', '300418'],
            '芯片半导体': ['002371', '300782', '603501', '688981', '688012'],
            '新能源': ['002594', '300750', '601012', '603659', '300014'],
            '白酒': ['000858', '000568', '000596', '600519', '600702'],
            '医药': ['600276', '000538', '300760', '603259', '600436'],
            '银行': ['000001', '600036', '601398', '601318', '601288'],
            '券商': ['600030', '300059', '601688', '000776', '601211'],
            '有色金属': ['601899', '002460', '600547', '603993', '000878'],
            '军工': ['600893', '000768', '600760', '600372'],
            '传媒': ['002027', '300413', '600637', '002555'],
        }
        
        sector_stats = []
        
        for sector_name, codes in sector_map.items():
            # 找到板块内的股票
            sector_stocks = [s for s in stocks if s.code in codes]
            
            if len(sector_stocks) >= 2:
                avg_change = sum(s.change_percent for s in sector_stocks) / len(sector_stocks)
                avg_volume = sum(s.volume for s in sector_stocks) / len(sector_stocks)
                limit_up_count = sum(1 for s in sector_stocks if s.change_percent >= 9.5)
                
                # 计算板块得分
                score = 50
                if avg_change > 3:
                    score += 25
                elif avg_change > 1.5:
                    score += 15
                elif avg_change > 0:
                    score += 5
                elif avg_change < -2:
                    score -= 10
                
                score += limit_up_count * 5  # 涨停加分
                
                sector_stats.append({
                    'name': sector_name,
                    'avg_change': round(avg_change, 2),
                    'stock_count': len(sector_stocks),
                    'limit_up_count': limit_up_count,
                    'score': min(score, 100),
                    'trend': '强势' if avg_change > 2 else '活跃' if avg_change > 0 else '震荡' if avg_change > -1 else '弱势'
                })
        
        return sector_stats
    
    def _select_afternoon_opportunities_v10(self, stocks: List, strong_sectors: List[Dict], 
                                             market_context: Dict = None) -> List[Dict]:
        """
        V10下午选股 - 使用完整历史数据版（含舆情因子）
        
        选股逻辑：
        1. 从数据库获取60天历史数据
        2. 从数据库获取舆情因子（预计算）
        3. 应用成交量结构过滤
        4. 应用大盘择时
        5. 使用完整V10多因子评分（含舆情因子）
        6. 排除无法买入的股票
        """
        picks = []
        
        # P0新增：Step 0 - 大盘择时已在外层完成，这里直接使用
        adjusted_top_n = 3
        # 使用市场强弱计算的动态阈值（比盘前宽松3分）
        if market_context:
            base_threshold = market_context.get('threshold', 60)
            threshold = max(55, base_threshold - 3)  # 盘中宽松3分
        else:
            threshold = 55  # 默认阈值
        
        # 初始化V10分析器
        analyzer = MultiFactorAnalyzerV10()
        
        # 初始化历史数据库（单例模式）
        from stock_history_db import get_stock_history_db
        db = get_stock_history_db()
        
        # Phase 2新增：批量预计算舆情因子（仅缓存模式）
        logger.info("Step 2.2: 批量获取舆情因子（数据库）")
        sentiment_factors = {}
        try:
            from sentiment_factor import get_sentiment_calculator
            sentiment_calc = get_sentiment_calculator()
            
            # 仅计算板块成分股（约200只），其他股票使用缓存或默认值
            sector_stocks = set()
            for sector_codes in self.SECTOR_MAP.values():
                sector_stocks.update(sector_codes)
            
            # 筛选出在行情中的板块股
            stock_list = [(s.code, s.name) for s in stocks if s.code in sector_stocks]
            
            logger.info(f"  计算板块成分股舆情: {len(stock_list)} 只")
            
            # 批量计算（仅缓存模式，更快）
            sentiment_factors = sentiment_calc.batch_calculate(
                stock_list, progress_interval=50, use_cache_only=True
            )
            logger.info(f"  舆情因子计算完成: {len(sentiment_factors)} 只")
        except Exception as e:
            logger.warning(f"  舆情因子批量计算失败: {e}，将在评分时实时计算")
            sentiment_factors = {}
        
        # 计算板块动量
        sector_momentum = {}
        for sector in strong_sectors:
            sector_momentum[sector['name']] = sector.get('change_pct', 0) * (sector.get('up_ratio', 50) / 100)
        
        # 筛选符合条件的股票
        candidates = []
        processed = 0
        total = len(stocks)
        
        for stock in stocks:
            processed += 1
            if processed % 500 == 0:
                logger.info(f"  已处理 {processed}/{total} 只股票...")
            
            # 过滤无法买入的股票
            if self.screener._is_untradable(stock.name, stock.code):
                continue
            
            # P0新增：成交量结构过滤
            # 注意：腾讯财经的volume是"手"（1手=100股），需要转换为股数
            volume_in_shares = stock.volume * 100  # 转换为股数
            turnover = getattr(stock, 'turnover', 0)
            if turnover == 0:
                turnover = volume_in_shares * stock.price  # 使用转换后的股数计算成交额
            
            # 构建data字典
            data = {
                'code': stock.code,
                'name': stock.name,
                'price': stock.price,
                'change_pct': stock.change_percent,
                'volume': volume_in_shares,  # 使用股数而非手数
                'turnover': turnover,
                'sector': self._identify_sector_for_intraday(stock.code, strong_sectors)
            }
            
            # 应用成交量过滤
            if not self._pass_volume_filter_intraday(data, stock.code):
                continue
            
            # 过滤已涨停的
            if stock.change_percent >= 9.5:
                continue
            
            # 过滤跌太多的
            if stock.change_percent < -3:
                continue
            
            # 从数据库获取历史数据（60天）
            historical_prices = db.get_prices(stock.code, days=60)
            
            # Phase 2优化：使用完整V10多因子评分（含舆情因子）
            score = analyzer.calculate_score(
                data, strong_sectors, sector_momentum, 
                historical_prices, market_context,
                sector_rps_data=None,  # P1新增：RPS数据
                sentiment_factor=sentiment_factors.get(stock.code)  # Phase 2新增：舆情因子
            )
            
            # 应用动态阈值（盘中选股比盘前宽松3分）
            base_threshold = score.get('threshold', 60)
            final_threshold = max(55, base_threshold - 3)  # 盘中宽松3分，最低55
            
            if score['total'] >= final_threshold:
                candidates.append({
                    'code': stock.code,
                    'name': stock.name,
                    'price': stock.price,
                    'change_pct': stock.change_percent,
                    'sector': data['sector'],
                    'total_score': score['total'],
                    'factors': score,
                    'reason': score.get('reason', '综合评分优异'),
                    'is_sector_leader': self._is_sector_leader_intraday(stock, strong_sectors),
                    'market_note': score.get('market_note', '')
                })
        
        # 按得分排序，取前N
        candidates.sort(key=lambda x: (x['total_score'], x['is_sector_leader']), reverse=True)
        return candidates[:adjusted_top_n]
    
    def _pass_volume_filter_intraday(self, data: Dict, code: str) -> bool:
        """P0新增：盘中成交量结构过滤（简化版）"""
        turnover = data.get('turnover', 0)
        volume = data.get('volume', 0)
        price = data.get('price', 0)
        
        # 如果没有成交额数据，尝试用成交量*价格估算
        if turnover == 0 and volume > 0 and price > 0:
            turnover = volume * price
        
        # 盘中标准适当放宽（相比盘前）
        if code.startswith(('00', '60')):
            min_turnover = 3e7  # 主板：3000万
        else:
            min_turnover = 1e7  # 中小盘：1000万
        
        if turnover < min_turnover:
            return False
        
        return True
    
    def _identify_sector_for_intraday(self, code: str, strong_sectors: List[Dict]) -> str:
        """识别股票所属板块（用于盘中）- 使用完整的SECTOR_MAP"""
        # 使用StockScreenerV9的完整板块映射
        from main import StockScreenerV9
        sector_map = StockScreenerV9.SECTOR_MAP
        
        for sector_name, codes in sector_map.items():
            if code in codes:
                return sector_name
        return '其他'
    
    def _is_sector_leader_intraday(self, stock, strong_sectors: List[Dict]) -> bool:
        """判断是否为板块龙头（盘中简化版）"""
        sector_name = self._identify_sector_for_intraday(stock.code, strong_sectors)
        
        for sector in strong_sectors:
            if sector['name'] == sector_name:
                sector_change = sector.get('change_pct', 0)
                # 超越板块平均1.5倍
                if sector_change > 0 and stock.change_percent > sector_change * 1.5:
                    return True
                # 或者涨幅>5%且板块强势
                if stock.change_percent > 5 and sector_change > 1:
                    return True
        return False
    
    def _generate_error_report(self, title: str, error_message: str) -> str:
        """生成错误报告"""
        lines = [
            f"# 📊 {title} 1.0",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**状态**: ❌ 数据获取失败",
            f"",
            "======================================================================",
            "",
            "## ⚠️ 数据获取失败",
            "",
            f"**错误信息**: {error_message}",
            "",
            "**可能原因**:",
            "- 网络连接问题",
            "- 数据源服务暂时不可用（如非交易时段限制）",
            "- 数据源接口变更或限流",
            "",
            "**建议**:",
            "- 检查网络连接",
            "- 稍后重试",
            "- 联系管理员检查数据源配置",
            "",
            "======================================================================",
            "",
            "## 💼 持仓分析",
            "",
            "持仓数据不受影响，以下为上次的持仓分析：",
            "",
        ]
        
        # 尝试添加持仓分析
        try:
            position_report = self.reporter._generate_detailed_position_report(None)
            if position_report:
                lines.append(position_report)
        except Exception as e:
            lines.append(f"*持仓数据加载失败: {e}*")
        
        lines.extend([
            "",
            "---",
            "*股票分析项目 1.2 | V10-P0优化*"
        ])

        return "\n".join(lines)

    def _generate_intraday_report(self, title: str, index_data: List,
                                   market_stats: Dict, strong_sectors: List[Dict],
                                   afternoon_picks: List[Dict], quotes: Dict = None,
                                   data_source: str = "Unknown") -> str:
        """生成盘中简报报告 - 两段式：持仓分析 + 下午潜力股"""
        lines = [
            f"# 📊 {title} 1.2",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**选股引擎**: V10板块轮动 - 完整版（含60天历史数据+舆情因子）",
            f"**因子权重**: 技术20% | 情绪3% | 板块33% | 资金19% | 风险15% | 舆情7%",
            f"**数据支持**: 本地历史数据库 (5093只 x 60天) + 舆情数据库",
            f"**数据来源**: {data_source} ✅",
            f"",
            "======================================================================",
            "",
        ]

        # ========== 第一段：持仓分析 ==========
        position_report = self.reporter._generate_detailed_position_report(quotes)
        if position_report:
            lines.append(position_report)
        else:
            lines.extend([
                "## 💼 持仓分析",
                "",
                "当前无持仓数据",
                "",
            ])

        # ========== 第二段：大盘总结 + 下午潜力股 ==========
        lines.extend([
            "## 📈 上午收盘总结",
            "======================================================================",
            "",
            "### 大盘指数表现",
            "",
        ])

        # 大盘指数
        if index_data:
            for idx in index_data[:4]:  # 主要指数
                emoji = "🟢" if idx.change_percent > 0 else "🔴"
                lines.append(f"- {emoji} **{idx.index_name}**: {idx.price:.2f} ({idx.change_percent:+.2f}%)")
        else:
            lines.append("- 指数数据获取中...")
        lines.append("")

        # 涨跌分布
        total_count = sum([market_stats['up'], market_stats['down'], market_stats['flat']])
        lines.extend([
            "### 涨跌分布统计",
            "",
            f"| 类型 | 数量 | 占比 |",
            f"|------|------|------|",
        ])
        if total_count > 0:
            lines.extend([
                f"| 🟢 上涨 | {market_stats['up']:,} | {market_stats['up']/total_count*100:.1f}% |",
                f"| 🔴 下跌 | {market_stats['down']:,} | {market_stats['down']/total_count*100:.1f}% |",
                f"| ⚪ 平盘 | {market_stats['flat']:,} | {market_stats['flat']/total_count*100:.1f}% |",
            ])
        else:
            lines.extend([
                f"| 🟢 上涨 | {market_stats['up']:,} | - |",
                f"| 🔴 下跌 | {market_stats['down']:,} | - |",
                f"| ⚪ 平盘 | {market_stats['flat']:,} | - |",
            ])
        lines.extend([
            "",
            f"📊 **涨停**: {market_stats['limit_up']} 只 | **跌停**: {market_stats['limit_down']} 只",
            f"📊 **总成交额**: {market_stats['total_volume']:.0f} 亿手 | **平均涨跌幅**: {market_stats['avg_change']:+.2f}%",
            "",
        ])

        # 强势板块
        lines.extend([
            "### 🏭 上午强势板块",
            "",
        ])

        if strong_sectors:
            lines.append("| 排名 | 板块 | 平均涨幅 | 涨停数 | 趋势 | 评分 |")
            lines.append("|------|------|----------|--------|------|------|")
            for i, sector in enumerate(strong_sectors, 1):
                trend_emoji = "🔥" if sector['trend'] == '强势' else "⚡" if sector['trend'] == '活跃' else "📊"
                lines.append(f"| {i} | {sector['name']} | {sector['avg_change']:+.2f}% | {sector['limit_up_count']} | {trend_emoji} {sector['trend']} | {sector['score']} |")
        else:
            lines.append("板块数据计算中...")

        lines.append("")

        # 下午选股
        lines.extend([
            "----------------------------------------------------------------------",
            "",
            "## 🎯 下午选股分析",
            "",
            "**选股逻辑**: 上午已放量但未涨停 + 处于强势板块内 + 量比大于1.5 + 涨幅2-7%区间",
            "",
        ])

        # 使用AI生成下午选股分析
        try:
            from deepseek_analyzer import stock_picker_analyzer
            afternoon_analyzer = stock_picker_analyzer
            market_context_afternoon = f"强势板块: {', '.join([s['name'] for s in strong_sectors[:3]])}"
        except:
            afternoon_analyzer = None
            market_context_afternoon = ""

        if afternoon_picks:
            for i, pick in enumerate(afternoon_picks, 1):
                score = pick.get('total_score', 0)
                change_pct = pick.get('change_pct', 0)
                factors = pick.get('factors', {})
                price = pick.get('price', 0)

                # 生成建议标签
                if score >= 75:
                    suggestion = "🚀 强烈推荐"
                elif score >= 70:
                    suggestion = "⭐ 推荐关注"
                else:
                    suggestion = "📈 可关注"

                # 使用AI生成详细分析
                if afternoon_analyzer:
                    analysis = afternoon_analyzer.analyze_pick(pick, market_context_afternoon)
                else:
                    # 备用分析
                    if score >= 75:
                        analysis = "上午强势+热点板块，资金持续流入，下午有望延续上涨。适合激进型投资者，注意控制仓位。"
                    elif score >= 70:
                        analysis = "上午表现良好，符合强势板块方向。适合稳健型投资者午后关注，建议设好止损。"
                    else:
                        analysis = "有一定上涨潜力，建议观察午后资金流向再决定。适合保守型投资者谨慎参与。"

                leader_tag = " [板块龙头]" if pick.get('is_sector_leader') else ""
                
                # ========== 技术位分析（新增）==========
                tech_levels_str = ""
                try:
                    from technical_analysis import calculate_technical_levels, format_technical_levels
                    from stock_history_db import StockHistoryDB
                    
                    # 初始化数据库连接
                    history_db = StockHistoryDB()
                    
                    # 获取历史数据（价格列表）
                    hist_prices = history_db.get_prices(pick['code'], days=60)
                    if hist_prices and len(hist_prices) >= 20:
                        levels = calculate_technical_levels(hist_prices, price)
                        tech_levels_str = format_technical_levels(levels, price)
                        # 保存到pick中供后续使用
                        pick['technical_levels'] = levels.to_dict()
                    else:
                        tech_levels_str = "   技术位: 历史数据不足，无法计算"
                except Exception as e:
                    logger.warning(f"计算技术位失败 {pick['code']}: {e}")
                    tech_levels_str = "   技术位: 计算失败"
                
                lines.extend([
                    f"{i}. {suggestion} **{pick.get('name', 'N/A')}** ({pick.get('code', 'N/A')}){leader_tag}",
                    f"   - 当前价: ¥{price:.2f} ({change_pct:+.2f}%)",
                    f"   - 综合评分: **{score:.1f}**",
                    f"   - 因子得分: 技术{factors.get('technical', 'N/A')} | 情绪{factors.get('sentiment', 'N/A')} | 板块{factors.get('sector', 'N/A')} | 资金{factors.get('money_flow', 'N/A')} | 风险{factors.get('risk', 'N/A')} | 舆情{factors.get('news_sentiment', 'N/A')}",
                    f"   - 所属板块: {pick.get('sector', '其他')}",
                    f"   - 选股说明: **{suggestion}**，{analysis}",
                ])
                
                # 添加技术位详情（新增）
                if tech_levels_str:
                    lines.append(f"   - 技术位分析:")
                    for tech_line in tech_levels_str.split('\n'):
                        lines.append(f"      {tech_line}")
                
                lines.append("")
        else:
            lines.append("选股数据计算中...")

        lines.extend([
            "======================================================================",
            "",
            "## ⚠️ 风险提示",
            "",
            "- 下午选股基于上午盘面，需结合午后资金流向动态调整",
            "- 建议关注强势板块内的补涨机会",
            "- 以上分析仅供参考，不构成投资建议",
            "",
            "---",
            "*股票分析项目 1.2 | V10完整版 | 下午机会版*"
        ])

        return "\n".join(lines)


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    """主入口"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f'股票分析系统 1.0 (V9+V10+)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python3 main.py --mode premarket    # 盘前分析
  python3 main.py --mode postmarket   # 盘后分析
  python3 main.py --mode noon         # 午间简报
  python3 main.py --no-send           # 不发送推送
        """
    )
    
    parser.add_argument('--mode', type=str,
                       choices=['premarket', 'postmarket', 'noon'],
                       default='premarket',
                       help='运行模式: premarket(8:50)/noon(12:30)/postmarket(15:50)')
    parser.add_argument('--top', type=int, default=3,
                       help='选股数量 (默认: 3)')
    parser.add_argument('--no-send', action='store_true',
                       help='不发送推送')
    
    args = parser.parse_args()
    
    # 初始化系统
    system = StockAnalysisSystem()
    
    # 执行对应模式
    if args.mode == 'premarket':
        result = system.run_premarket(top_n=args.top, send=not args.no_send)
    elif args.mode == 'postmarket':
        result = system.run_postmarket(send=not args.no_send)
    elif args.mode == 'noon':
        result = system.run_intraday(mode='noon', send=not args.no_send)
    else:
        print(f"❌ 未知模式: {args.mode}")
        return 1
    
    return 0 if (result and result.get('success')) else 1


if __name__ == "__main__":
    sys.exit(main())
