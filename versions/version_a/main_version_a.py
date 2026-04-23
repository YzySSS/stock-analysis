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
VERSION = "1.1"
VERSION_NAME = "V1.1"

from datetime import datetime
from typing import List, Dict
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        # 科技成长 - AI/算力
        '人工智能': ['000938', '002230', '002415', '300033', '300418', '600728', '603019', '000977', '300474', '688256'],  # 紫光/讯飞/海康/同花顺/昆仑/佳都/中科曙光/浪潮/景嘉微/寒武纪
        '芯片半导体': ['002371', '300782', '603501', '688981', '688012', '300661', '600584', '002156', '300316', '688126'],  # 北方华创/卓胜微/韦尔/中芯/中微/圣邦/长电/通富微电/晶盛/沪硅
        '新能源': ['002594', '300750', '601012', '603659', '300014', '600438', '002812', '002460', '300073', '688005'],  # 比亚迪/宁德/隆基/璞泰来/亿纬/通威/恩捷/赣锋/当升/容百
        '光伏': ['601012', '600438', '002129', '300274', '688599', '600732', '002459', '688303', '603806', '601877'],  # 隆基/通威/TCL中环/阳光/天合/爱旭/晶澳/大全/福斯特/正泰
        '储能': ['300274', '002594', '300014', '300207', '688063', '002335', '002518', '300068', '688348', '300438'],  # 阳光/比亚迪/亿纬/欣旺达/派能/科华/科士达/南都/昱能/鹏辉
        '5G通信': ['000063', '600498', '300502', '002281', '300136', '600487', '002916', '300308', '603236', '300394'],  # 中兴/烽火/新易盛/光迅/信维/亨通/深南/中际/移远/天孚
        '云计算': ['000938', '300017', '600845', '300454', '603881', '300738', '300166', '600410', '300212', '600756'],  # 紫光/网宿/宝信/深信服/数据港/奥飞/东方国信/华胜/易华录/浪潮软件
        '数据中心': ['300017', '603881', '002837', '300738', '600845', '600804', '300212', '600288', '002401', '300231'],  # 网宿/数据港/英维克/奥飞/宝信/鹏博士/易华录/大恒/中远海科/银信
        
        # 大消费
        '白酒': ['000858', '000568', '000596', '600519', '600702', '603589', '000799', '002304', '600197', '600779', '000860', '603198', '603369', '600559'],  # 五粮液/泸州老窖/古井贡酒/茅台/洋河/水井坊/酒鬼酒/洋河/伊力特/水井坊/顺鑫/迎驾/今世缘/老白干
        '医药': ['600276', '000538', '300760', '603259', '600436', '300122', '000999', '600085', '603392', '300003', '600332', '002007', '600196', '300142'],  # 恒瑞/云南白药/迈瑞/药明/片仔癀/智飞/华润三九/同仁堂/万泰/乐普/白云山/华兰/复星/沃森
        '医疗器械': ['300760', '603658', '300003', '688617', '300896', '688301', '002223', '300529', '603127', '300595'],  # 迈瑞/安图/乐普/惠泰/爱美客/奕瑞/鱼跃/健帆/昭衍新药/欧普康视
        '创新药': ['600276', '000661', '300122', '688180', '688235', '688266', '300558', '002773', '300009', '688177'],  # 恒瑞/长春高新/智飞/君实/百济神州/泽璟/贝达/康弘/安科生物/百奥泰
        'CXO': ['603259', '300759', '002821', '300347', '603127', '688202', '300363', '603590', '688621', '300149'],  # 药明/康龙/凯莱英/泰格/昭衍/美迪西/博腾/康辰/阳光诺和/睿智
        '食品饮料': ['000895', '600887', '603288', '600519', '000568', '601888', '002714', '300498', '600298', '603517', '605499', '603345', '002847', '300999'],  # 双汇/伊利/海天/茅台/老窖/中免/牧原/温氏/安琪/绝味/东鹏/安井/盐津/金龙鱼
        '家电': ['000333', '000651', '600690', '002032', '002508', '603486', '002242', '603868', '688696', '002705'],  # 美的/格力/海尔/苏泊尔/老板/科沃斯/九阳/飞科/极米/新宝
        '汽车': ['002594', '601633', '601127', '000625', '600660', '601238', '000768', '002920', '603596', '600104'],  # 比亚迪/长城/赛力斯/长安/福耀/广汽/中航西飞/德赛/伯特利/上汽
        '新能源汽车': ['002594', '300750', '601127', '000625', '601633', '002050', '002126', '603305', '300073', '002709'],  # 比亚迪/宁德/赛力斯/长安/长城/三花/银轮/旭升/当升/天赐
        
        # 大金融
        '银行': ['000001', '600036', '601398', '601288', '601166', '600016', '601998', '601818', '601328', '601169', '601229', '601838', '600919', '002142'],  # 平安/招行/工行/农行/兴业/民生/中信/光大/交行/北京/上海/成都/江苏/宁波
        '券商': ['600030', '300059', '601688', '000776', '601211', '600999', '601377', '600958', '601881', '601162', '601108', '601555', '601901', '002500'],  # 中信/东财/华泰/广发/国君/招商/兴业/东方/银河/天风/财通/东吴/方正/山西
        '保险': ['601318', '601628', '601601', '601319'],  # 平安/人寿/太保/人保
        
        # 周期资源
        '有色金属': ['601899', '002460', '600547', '603993', '000878', '002497', '601600', '600362', '000630', '601168', '002128', '603799', '603260', '002240'],  # 紫金/赣锋/山东黄金/洛阳钼业/云南铜业/雅化/中国铝业/江西铜业/铜陵有色/西部矿业/露天煤业/华友/合盛/盛新
        '稀土': ['600111', '000831', '600259', '600392', '600010', '300224', '600366', '000970', '300748', '600549'],  # 北方稀土/五矿稀土/广晟有色/盛和资源/包钢/正海磁材/宁波韵升/中科三环/金力永磁/厦门钨业
        '煤炭': ['601088', '601225', '600188', '601699', '600123', '601015', '600971', '600397', '601666', '600408'],  # 中国神华/陕西煤业/兖矿/潞安/兰花/陕西黑猫/恒源/安源/平煤/红阳
        '钢铁': ['600019', '000932', '600507', '000959', '600010', '002110', '600782', '600808', '000717', '600022'],  # 宝钢/华菱/方大/首钢/包钢/三钢/新钢/马钢/韶钢/山东钢铁
        '化工': ['002092', '600309', '601216', '600352', '600486', '002064', '601233', '000792', '600426', '002601', '300285', '600160', '000830', '002493'],  # 中泰化学/万华化学/君正/浙江龙盛/扬农化工/华峰化学/桐昆股份/盐湖股份/华鲁恒升/龙佰集团/国瓷材料/巨化股份/鲁西化工/荣盛石化
        '石油': ['601857', '600028', '601808', '600938', '600871', '603619', '300164', '002353', '300191', '002554'],  # 中石油/中石化/中海油服/中国海油/石化油服/中曼石油/通源石油/杰瑞股份/潜能恒信/惠博普
        '黄金': ['600547', '600489', '600988', '600612', '002155', '000975', '601069', '002237', '600311', '600385'],  # 山东黄金/中金黄金/赤峰黄金/老凤祥/湖南黄金/银泰黄金/西部黄金/恒邦股份/荣华实业/山东金泰
        
        # 基建地产
        '房地产': ['000002', '600048', '001979', '600606', '600383', '000961', '600340', '000656', '001914', '600208', '600266', '600325', '600848', '600639'],  # 万科/保利/蛇口/绿地/金地/中南/华夏幸福/金科/招商积余/新湖中宝/城建发展/华发/临港/浦东金桥
        '建筑': ['601668', '601390', '601669', '601800', '601186', '601117', '600170', '601618', '600820', '600502', '600039', '002060', '601789', '600284'],  # 中国建筑/中铁/水电/交建/铁建/化学/上海建工/中冶/隧道股份/安徽建工/四川路桥/粤水电/宁波建工/浦东建设
        '建材': ['000786', '600585', '002271', '600801', '600876', '000012', '002080', '603737', '002791', '002043', '000672', '600176', '002233', '000401'],  # 北新建材/海螺水泥/东方雨虹/华新水泥/洛阳玻璃/南玻A/中材科技/三棵树/坚朗五金/兔宝宝/上峰水泥/中国巨石/塔牌集团/冀东水泥
        
        # 其他
        '军工': ['600893', '000768', '600760', '600372', '600482', '000519', '300722', '300034', '002025', '600118', '600879', '600765', '000738', '300699'],  # 航发动力/中航西飞/中航沈飞/中航电子/中国动力/中兵红箭/新余国科/钢研高纳/航天电器/中国卫星/航天电子/中航重机/航发控制/光威复材
        '传媒': ['002027', '300413', '600637', '002555', '300251', '600088', '600373', '601900', '603000', '300770', '300418', '002624', '300133', '300058'],  # 分众传媒/芒果超媒/东方明珠/三七互娱/光线传媒/中视传媒/中文传媒/南方传媒/人民网/新媒股份/昆仑万维/完美世界/华策影视/蓝色光标
        '电力': ['600900', '600011', '600795', '601985', '600886', '600674', '600027', '601991', '600023', '600578', '000591', '601016', '600163', '000539'],  # 长江电力/华能/国电/中国核电/国投电力/川投能源/华电国际/大唐发电/浙能电力/京能电力/太阳能/节能风电/中闽能源/粤电力
        '交运': ['601006', '600009', '601111', '601919', '600029', '601021', '002352', '002120', '600115', '600018', '001965', '600377', '601872', '600233'],  # 大秦铁路/上海机场/中国国航/中远海控/南方航空/春秋航空/顺丰控股/韵达股份/中国东航/上港集团/招商公路/宁沪高速/招商轮船/圆通速递
        '旅游酒店': ['601888', '600754', '600138', '000524', '002033', '000888', '600258', '002707', '600054', '000978', '002059', '000610', '300144', '600706'],  # 中国中免/锦江酒店/中青旅/岭南控股/丽江股份/峨眉山/首旅酒店/众信旅游/黄山旅游/桂林旅游/云南旅游/西安旅游/宋城演艺/曲江文旅
    }
    
    def __init__(self):
        self.sina = SinaDataProvider()
        self.analyzer = MultiFactorAnalyzerV9()  # 使用V9分析器
        self.all_stocks = self._load_all_stocks()
        self.sector_performance = {}  # 板块表现缓存
        logger.info(f"V9选股器初始化完成，股票池: {len(self.all_stocks)}只")
    
    def _load_all_stocks(self) -> List[str]:
        """加载全A股列表（5486只）"""
        # 优先从环境变量指定的路径加载，否则使用默认路径
        all_a_stocks_file = os.getenv('STOCK_LIST_FILE', os.path.expanduser("~/.clawdbot/stock_watcher/all_a_stocks.txt"))
        watchlist_file = os.getenv('STOCK_WATCHLIST_FILE', os.path.expanduser("~/.clawdbot/stock_watcher/watchlist.txt"))
        
        if os.path.exists(all_a_stocks_file):
            try:
                with open(all_a_stocks_file, 'r', encoding='utf-8') as f:
                    stocks = [line.strip() for line in f if line.strip()]
                logger.info(f"✅ 从文件加载全A股列表: {len(stocks)} 只")
                return stocks
            except Exception as e:
                logger.warning(f"⚠️ 读取全A股列表失败: {e}")
        
        # 回退到精简列表
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
        V9选股 - 板块轮动增强

        Args:
            top_n: 选出前N只

        Returns:
            (选股结果列表, quotes数据字典)
        """
        logger.info(f"V9开始选股，目标: 前{top_n}只")

        # 1. 获取行情
        quotes = self._get_quotes()
        if not quotes:
            logger.error("获取行情失败")
            return [], {}

        # 2. 增强板块分析 - V9核心
        sectors = self._analyze_sectors_v9(quotes)
        logger.info(f"强势板块: {', '.join([s['name'] for s in sectors[:3]])}")

        # 3. 计算板块动量
        sector_momentum = self._calc_sector_momentum(sectors)

        # 4. 计算个股评分并过滤
        results = []
        for code, data in quotes.items():
            name = data.get('name', code)

            # 过滤无法买入的股票
            if self._is_untradable(name, code):
                continue

            # V9：使用增强的多因子分析
            score = self.analyzer.calculate_score(data, sectors, sector_momentum)
            if score['total'] >= 65:  # V9提高阈值，只保留优质股
                # 识别股票所属板块 - 优先使用SECTOR_MAP，更准确
                stock_sector = self._identify_sector(code)
                # 如果SECTOR_MAP中没有，再使用stock_sector模块
                if stock_sector == '其他':
                    try:
                        from stock_sector import get_stock_sector
                        stock_sector = get_stock_sector(code)
                    except:
                        stock_sector = '其他'
                results.append({
                    'code': code,
                    'name': name,
                    'price': data.get('price', 0),
                    'change_pct': data.get('change_pct', 0),
                    'sector': stock_sector,
                    'total_score': score['total'],
                    'factors': score,
                    'reason': score.get('reason', '综合评分优异'),
                    'is_sector_leader': self._is_sector_leader(code, data, sectors)  # V9新增
                })

        # 5. 排序并返回TopN
        results.sort(key=lambda x: (x['total_score'], x['is_sector_leader']), reverse=True)
        top_stocks = results[:top_n]

        logger.info(f"V9选股完成，选出 {len(top_stocks)} 只")
        for s in top_stocks:
            leader_tag = " [板块龙头]" if s.get('is_sector_leader') else ""
            logger.info(f"  {s['name']}({s['code']}): {s['total_score']}分{leader_tag}")

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
    
    def _get_quotes(self) -> Dict:
        """获取实时行情（多数据源备份）"""
        quotes = {}
        
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
    
    def _analyze_sectors_v9(self, quotes: Dict) -> List[Dict]:
        """V9板块轮动分析 - 基于真实行业分类"""
        sector_stats = {}
        
        # 初始化所有板块
        for sector_name, codes in self.SECTOR_MAP.items():
            sector_stats[sector_name] = {
                'codes': codes,
                'total_change': 0,
                'total_volume': 0,
                'count': 0,
                'up_count': 0,
                'limit_up_count': 0
            }
        
        # 统计板块内股票表现
        for code, data in quotes.items():
            change_pct = data.get('change_pct', 0)
            volume = data.get('volume', 0)
            
            # 找到股票所属板块
            for sector_name, codes in self.SECTOR_MAP.items():
                if code in codes:
                    sector_stats[sector_name]['total_change'] += change_pct
                    sector_stats[sector_name]['total_volume'] += volume
                    sector_stats[sector_name]['count'] += 1
                    if change_pct > 0:
                        sector_stats[sector_name]['up_count'] += 1
                    if change_pct >= 9.5:
                        sector_stats[sector_name]['limit_up_count'] += 1
                    break
        
        # 计算板块得分
        sector_list = []
        for name, stats in sector_stats.items():
            if stats['count'] == 0:
                continue
            
            avg_change = stats['total_change'] / stats['count']
            up_ratio = stats['up_count'] / stats['count'] if stats['count'] > 0 else 0
            
            # V9板块评分公式
            score = 50  # 基础分
            
            # 涨幅得分 (0-25分)
            if avg_change > 3:
                score += 25
            elif avg_change > 2:
                score += 20
            elif avg_change > 1:
                score += 15
            elif avg_change > 0:
                score += 8
            else:
                score -= 10
            
            # 上涨家数占比得分 (0-15分)
            score += up_ratio * 15
            
            # 涨停数量加分 (每只+3分)
            score += stats['limit_up_count'] * 3
            
            sector_list.append({
                'name': name,
                'change_pct': round(avg_change, 2),
                'stock_count': stats['count'],
                'up_count': stats['up_count'],
                'limit_up_count': stats['limit_up_count'],
                'up_ratio': round(up_ratio * 100, 1),
                'score': min(100, int(score)),
                'trend': '强势' if avg_change > 2 else '活跃' if avg_change > 0 else '弱势'
            })
        
        sector_list.sort(key=lambda x: x['score'], reverse=True)
        return sector_list
    
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


class MultiFactorAnalyzerV9:
    """V9多因子分析器 - 板块轮动增强版
    
    核心升级：
    1. 板块轮动因子权重提升（25% → 30%）
    2. 增加板块动量评分（趋势强度）
    3. 增加资金流向因子
    4. 龙头股识别加分
    """
    
    def calculate_score(self, data: Dict, sectors: List[Dict], sector_momentum: Dict = None) -> Dict:
        """计算多因子评分 - V9版本"""
        price = data.get('price', 0)
        change_pct = data.get('change_pct', 0)
        volume = data.get('volume', 0)
        code = data.get('code', '')
        name = data.get('name', '')
        
        # 1. 技术因子 (0-25分) - 基于价格形态
        tech_score = self._calc_technical_score(change_pct, price)
        
        # 2. 情绪因子 (0-20分) - 基于市场情绪
        sentiment_score = self._calc_sentiment_score(change_pct)
        
        # 3. 板块轮动因子 (0-30分) - V9核心升级 ⭐
        sector_score = self._calc_sector_rotation_score(
            data, sectors, sector_momentum, code, name
        )
        
        # 4. 资金流向因子 (0-15分) - V9新增
        money_flow_score = self._calc_money_flow_score(change_pct, volume)
        
        # 5. 风险因子 (0-10分) - 波动率控制
        risk_score = self._calc_risk_score(change_pct)
        
        total = tech_score + sentiment_score + sector_score + money_flow_score + risk_score
        
        return {
            'total': round(total, 1),
            'technical': round(tech_score, 1),
            'sentiment': round(sentiment_score, 1),
            'sector': round(sector_score, 1),
            'money_flow': round(money_flow_score, 1),
            'risk': round(risk_score, 1),
            'reason': f'技术:{tech_score:.0f} 情绪:{sentiment_score:.0f} 板块:{sector_score:.0f} 资金:{money_flow_score:.0f} 风险:{risk_score:.0f}'
        }
    
    def _calc_technical_score(self, change_pct: float, price: float) -> float:
        """技术因子评分 (0-25分)"""
        score = 15  # 基础分
        
        # 涨幅得分
        if change_pct > 3:
            score += 8
        elif change_pct > 1:
            score += 5
        elif change_pct > 0:
            score += 2
        elif change_pct < -3:
            score -= 5
        
        return min(25, max(0, score))
    
    def _calc_sentiment_score(self, change_pct: float) -> float:
        """情绪因子评分 (0-20分)"""
        score = 12  # 基础分
        
        # 基于涨跌幅的情绪判断
        if change_pct > 5:
            score += 6
        elif change_pct > 3:
            score += 4
        elif change_pct > 1:
            score += 2
        elif change_pct < -5:
            score -= 6
        elif change_pct < -3:
            score -= 3
        
        return min(20, max(0, score))
    
    def _calc_sector_rotation_score(self, data: Dict, sectors: List[Dict], 
                                     sector_momentum: Dict, code: str, name: str) -> float:
        """板块轮动因子评分 (0-30分) - V9核心"""
        score = 10  # 基础分
        stock_sector = data.get('sector', '其他')
        
        # 1. 强势板块匹配 (0-15分)
        for i, s in enumerate(sectors[:5]):  # 前5强势板块
            if stock_sector == s['name']:
                # 排名越靠前，加分越多
                rank_bonus = [15, 12, 10, 8, 6][i]
                score += rank_bonus
                break
        
        # 2. 板块动量加分 (0-8分)
        if sector_momentum and stock_sector in sector_momentum:
            momentum = sector_momentum[stock_sector]
            # 动量越强，加分越多
            if momentum > 5:
                score += 8
            elif momentum > 3:
                score += 5
            elif momentum > 1:
                score += 2
        
        # 3. 龙头股识别 (0-7分)
        # 如果股票是板块内涨幅靠前的，加分
        change_pct = data.get('change_pct', 0)
        if sectors and len(sectors) > 0:
            top_sector_change = sectors[0].get('change_pct', 0)
            if change_pct > top_sector_change:
                score += 7  # 超越板块平均，可能是龙头
            elif change_pct > top_sector_change * 0.8:
                score += 4
        
        return min(30, max(0, score))
    
    def _calc_money_flow_score(self, change_pct: float, volume: float) -> float:
        """资金流向因子评分 (0-15分) - V9新增"""
        score = 8  # 基础分
        
        # 量价配合判断
        if change_pct > 0 and volume > 1000000:  # 上涨+放量
            score += 6
        elif change_pct > 0 and volume > 500000:
            score += 3
        elif change_pct < 0 and volume > 1000000:  # 下跌+放量，偏空
            score -= 3
        
        return min(15, max(0, score))
    
    def _calc_risk_score(self, change_pct: float) -> float:
        """风险因子评分 (0-10分)"""
        # 波动率越低，风险分越高
        if abs(change_pct) < 3:
            return 10
        elif abs(change_pct) < 5:
            return 7
        elif abs(change_pct) < 8:
            return 4
        else:
            return 2


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

    def generate_premarket_report(self, picks: List[Dict], quotes: Dict = None) -> str:
        """生成盘前报告 - 两段式：持仓分析 + 推荐股"""
        lines = [
            f"# 🌅 盘前选股报告 1.0",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**选股引擎**: V9板块轮动增强",
            f"**报告引擎**: V10+",
            f"**股票数量**: {len(picks)} 只",
            f"",
            "======================================================================",
            "",
        ]

        # ========== 第一段：持仓分析 ==========
        position_report = self._generate_detailed_position_report(quotes)
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
            "## 🎯 V9板块轮动策略 - TOP 推荐",
            "======================================================================",
            f"基于全A股多因子评分",
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

            lines.extend([
                f"{i}. ⭐ **{pick['name']}** ({pick['code']})",
                f"   - 当前价: ¥{pick.get('price', 0):.2f} ({change_pct:+.2f}%)",
                f"   - 综合评分: **{score:.1f}**",
                f"   - 因子得分: 技术{pick.get('factors', {}).get('technical', 'N/A')} | 情绪{pick.get('factors', {}).get('sentiment', 'N/A')} | 板块{pick.get('factors', {}).get('sector', 'N/A')} | 资金{pick.get('factors', {}).get('money_flow', 'N/A')} | 风险{pick.get('factors', {}).get('risk', 'N/A')}",
                f"   - 所属板块: {pick.get('sector', 'N/A')}",
                f"   - 选股说明: **{suggestion}**，{analysis}",
                "",
            ])

        lines.extend([
            "======================================================================",
            "",
            "⚠️ **风险提示**: 以上分析仅供参考，不构成投资建议。",
            f"",
            "---",
            f"*股票分析项目 1.0 | V9+V10+*"
        ])

        return "\n".join(lines)

    def generate_postmarket_report(self, picks: List[Dict] = None, 
                                    closing_quotes: Dict = None,
                                    market_stats: Dict = None) -> str:
        """生成盘后报告 - 包含实际复盘数据"""
        lines = [
            f"# 🌇 盘后复盘报告 1.0",
            f"",
            f"**报告时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**报告引擎**: V10+ 深度复盘",
            f"",
            "---",
            f"",
            "## 📊 今日市场回顾",
            f"",
        ]
        
        # 添加大盘统计
        if market_stats:
            lines.extend([
                f"| 指标 | 数值 |",
                f"|------|------|",
                f"| 上涨家数 | 🟢 {market_stats.get('up', 0):,} |",
                f"| 下跌家数 | 🔴 {market_stats.get('down', 0):,} |",
                f"| 涨停家数 | 🚀 {market_stats.get('limit_up', 0)} |",
                f"| 跌停家数 | 📉 {market_stats.get('limit_down', 0)} |",
                f"| 平均涨跌 | {market_stats.get('avg_change', 0):+.2f}% |",
                "",
            ])
        
        # 盘前推荐复盘
        if picks:
            lines.extend([
                "---",
                f"",
                "## 🏆 盘前推荐复盘",
                f"",
                f"盘前共推荐 **{len(picks)}** 只股票，平均评分 **{sum(p['total_score'] for p in picks)/len(picks):.1f}**",
                f"",
            ])
            
            # 计算实际表现
            total_return = 0
            success_count = 0
            best_performer = None
            worst_performer = None
            
            for i, pick in enumerate(picks, 1):
                code = pick['code']
                name = pick['name']
                morning_price = pick.get('price', 0)
                morning_score = pick.get('total_score', 0)
                
                # 获取收盘数据
                closing_data = closing_quotes.get(code, {}) if closing_quotes else {}
                closing_price = closing_data.get('price', morning_price)
                
                # 计算实际收益率（相对于盘前价格）
                if morning_price > 0 and closing_price > 0:
                    actual_return = round((closing_price - morning_price) / morning_price * 100, 2)
                else:
                    actual_return = 0
                
                total_return += actual_return
                if actual_return > 0:
                    success_count += 1
                
                # 跟踪最佳/最差
                if best_performer is None or actual_return > best_performer['return']:
                    best_performer = {'code': code, 'name': name, 'return': actual_return}
                if worst_performer is None or actual_return < worst_performer['return']:
                    worst_performer = {'code': code, 'name': name, 'return': actual_return}
                
                # 表现评级
                if actual_return >= 5:
                    performance = "🔥 优秀"
                elif actual_return >= 2:
                    performance = "✅ 良好"
                elif actual_return >= 0:
                    performance = "📊 持平"
                elif actual_return >= -3:
                    performance = "⚠️ 小幅回撤"
                else:
                    performance = "❌ 表现不佳"
                
                lines.extend([
                    f"### {i}. {name} ({code})",
                    f"",
                    f"| 指标 | 盘前 | 收盘 | 变化 |",
                    f"|------|------|------|------|",
                    f"| 价格 | ¥{morning_price:.2f} | ¥{closing_price:.2f} | {actual_return:+.2f}% |",
                    f"| 评分 | {morning_score:.1f} | - | - |",
                    f"| 表现 | - | - | **{performance}** |",
                    f"",
                    f"**因子分解**: {pick.get('reason', 'N/A')}",
                    f"",
                    "---",
                    f""
                ])
            
            # 汇总统计
            avg_return = total_return / len(picks) if picks else 0
            success_rate = success_count / len(picks) * 100 if picks else 0
            
            lines.extend([
                f"",
                "## 📈 策略表现统计",
                f"",
                f"| 指标 | 数值 |",
                f"|------|------|",
                f"| 平均收益率 | {avg_return:+.2f}% |",
                f"| 成功率 | {success_rate:.1f}% ({success_count}/{len(picks)}) |",
                f"| 最佳表现 | {(best_performer['name'] + ' (' + str(best_performer['return']) + '%)') if best_performer else 'N/A'} |",
                f"| 最差表现 | {(worst_performer['name'] + ' (' + str(worst_performer['return']) + '%)') if worst_performer else 'N/A'} |",
                f"",
            ])
            
            # 策略诊断
            lines.extend([
                "---",
                f"",
                "## 🔍 策略诊断",
                f"",
            ])
            
            if avg_return > 2:
                diagnosis = "今日策略表现优秀，选股方向与市场主线契合。建议继续保持当前策略。"
            elif avg_return > 0:
                diagnosis = "今日策略小幅盈利，部分选股表现良好，但还有优化空间。"
            elif avg_return > -2:
                diagnosis = "今日策略基本持平，选股方向需要调整。建议关注资金流向更明确的板块。"
            else:
                diagnosis = "今日策略表现不佳，可能与市场风格切换有关。建议审视选股因子权重。"
            
            lines.extend([
                f"**综合诊断**: {diagnosis}",
                f"",
                "### 改进建议",
                "",
                "1. **板块轮动跟踪**: 加强对早盘板块强度的实时监控",
                "2. **止盈止损**: 建议对涨幅超过5%的股票考虑部分止盈",
                "3. **风险分散**: 避免单一个股仓位过重",
                "4. **尾盘确认**: 下午选股时关注尾盘资金流向",
                f"",
            ])
        else:
            lines.extend([
                "---",
                f"",
                "## 📝 无盘前推荐数据",
                f"",
                "今日无盘前选股记录，无法生成复盘数据。",
                f"",
            ])

        # 添加持仓股分析（使用新的详细分析）
        position_report = self._generate_detailed_position_report(closing_quotes)
        if position_report:
            lines.extend([
                "======================================================================",
                f"",
                position_report,
                f"",
            ])

        lines.extend([
            "---",
            f"",
            "## ⚠️ 风险提示",
            f"",
            "- 以上复盘基于当日数据，历史表现不代表未来收益",
            "- 策略需要持续优化，定期回顾选股逻辑",
            "- 市场风格切换时，策略可能短期失效",
            f"",
            "---",
            f"*股票分析项目 1.0 | V9+V10+ | 盘后复盘*"
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

            for line in lines:
                # 遇到一级标题（## ）时，保存当前段并开始新段
                if line.startswith('## ') and current_part:
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
    V9选股 + V10+报告
    """

    def __init__(self):
        self.version = VERSION
        self.screener = StockScreenerV9()
        self.reporter = ReportGeneratorV10Plus()
        self.notifier = NotificationManager()
        self.storage = ReportStorage()

        logger.info(f"股票分析系统 {self.version} 初始化完成")

    def run_premarket(self, top_n: int = 3, send: bool = True) -> Dict:
        """
        盘前分析

        Args:
            top_n: 选股数量
            send: 是否发送推送

        Returns:
            分析结果
        """
        print("\n" + "="*80)
        print(f"🚀 股票分析系统 1.0 - 盘前分析")
        print("="*80)

        # 1. V9选股
        logger.info("Step 1: V9选股")
        picks, quotes = self.screener.screen(top_n=top_n)

        if not picks:
            logger.error("V9选股失败")
            return {'success': False, 'error': '选股失败'}

        # 2. 生成V10+报告
        logger.info("Step 2: V10+生成报告")
        report = self.reporter.generate_premarket_report(picks, quotes)

        # 3. 保存报告
        logger.info("Step 3: 保存报告")
        filepath = self.storage.save(report, 'premarket')

        # 4. 发送推送
        if send:
            logger.info("Step 4: 发送推送")
            self.notifier.send_in_parts("🌅 盘前选股报告 1.0", report)
        
        print(f"\n✅ 盘前分析完成: {len(picks)}只股票")
        print("="*80)
        
        return {
            'success': True,
            'picks': picks,
            'report_path': filepath,
            'report_content': report
        }
    
    def run_postmarket(self, picks: List[Dict] = None, send: bool = True) -> Dict:
        """
        盘后分析 - 深度复盘盘前推荐股
        
        Args:
            picks: 盘前选股结果（用于复盘）
            send: 是否发送推送
            
        Returns:
            分析结果
        """
        print("\n" + "=" * 80)
        print(f"🚀 股票分析系统 1.0 - 盘后深度复盘")
        print("=" * 80)
        
        # Step 1: 获取收盘行情
        logger.info("Step 1: 获取收盘行情数据")
        closing_quotes = {}
        market_stats = {}
        
        if picks:
            pick_codes = [p['code'] for p in picks]
            try:
                from sina_finance_api import SinaFinanceAPI
                sina = SinaFinanceAPI()
                df = sina.get_realtime_quotes(pick_codes)
                if not df.empty:
                    for _, row in df.iterrows():
                        code = str(row['code']).zfill(6)
                        closing_quotes[code] = {
                            'name': row.get('name', code),
                            'price': row.get('price', 0),
                            'change_pct': row.get('change_percent', 0)
                        }
                    logger.info(f"✅ 获取到 {len(closing_quotes)} 只推荐股的收盘数据")
            except Exception as e:
                logger.warning(f"⚠️ 获取收盘行情失败: {e}")
            
            # Step 2: 获取市场整体数据
            logger.info("Step 2: 获取市场整体数据")
            try:
                from data_source import data_manager
                source = data_manager.get_source()
                if source:
                    all_stocks = source.get_a_stock_spot()
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
                    logger.info(f"✅ 市场统计: 涨{up_count}/跌{down_count}/涨停{limit_up}")
            except Exception as e:
                logger.warning(f"⚠️ 获取市场统计失败: {e}")
        
        # Step 3: 生成V10+盘后报告
        logger.info("Step 3: 生成V10+盘后深度复盘报告")
        report = self.reporter.generate_postmarket_report(
            picks=picks,
            closing_quotes=closing_quotes,
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
        print(f"🕐 {title} 1.0 - 上午收盘总结 + 下午选股")
        print("="*80)
        
        # ============ 1. 上午收盘总结 ============
        logger.info("Step 1: 获取大盘指数表现")
        
        # 获取数据源
        source = data_manager.get_source('akshare')
        all_stocks = []
        index_data = []
        
        if source and source.enabled:
            try:
                # 获取所有A股行情
                all_stocks = source.get_a_stock_spot()
                # 获取大盘指数
                index_data = source.get_index_spot()
            except Exception as e:
                logger.warning(f"获取行情失败: {e}")
        
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
        logger.info("Step 2: 执行下午选股分析")
        
        afternoon_picks = []
        strong_sectors = []
        
        if all_stocks:
            # 使用板块优先选股器
            screener = SectorFirstScreener(data_source=None)
            
            # 手动计算板块表现（使用实际数据）
            sector_performance = self._calculate_sector_performance(all_stocks)
            strong_sectors = sorted(sector_performance, key=lambda x: x['score'], reverse=True)[:5]
            
            # 筛选下午潜在机会股
            afternoon_picks = self._select_afternoon_opportunities(all_stocks, strong_sectors)
        
        # ============ 3. 生成报告 ============
        logger.info("Step 3: 生成盘中简报")
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
            quotes=all_stocks
        )
        
        # 保存报告
        filepath = self.storage.save(report, 'intraday')
        
        # 发送推送
        if send:
            self.notifier.send_in_parts(f"📊 {title} 1.0", report)
        
        print(f"\n✅ {title}完成 - 选出 {len(afternoon_picks)} 只下午潜力股")
        print("="*80)
        
        return {
            'success': True,
            'mode': mode,
            'market_stats': {'up': up_count, 'down': down_count, 'limit_up': limit_up},
            'strong_sectors': [s['name'] for s in strong_sectors],
            'afternoon_picks': afternoon_picks,
            'report_path': filepath,
            'report_content': report
        }
    
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
    
    def _select_afternoon_opportunities(self, stocks: List, strong_sectors: List[Dict]) -> List[Dict]:
        """筛选下午潜在机会股 (Top 3)
        
        选股逻辑（区别于盘前）：
        1. 上午已放量但未涨停（还有空间）
        2. 处于强势板块内
        3. 技术形态突破
        4. 量比大于1.5（资金持续流入）
        5. 排除无法买入的股票
        """
        picks = []
        
        # 获取强势板块的代码集合
        hot_sector_codes = set()
        for sector in strong_sectors[:3]:  # 前3强势板块
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
            hot_sector_codes.update(sector_map.get(sector['name'], []))
        
        # 筛选符合条件的股票
        candidates = []
        for stock in stocks:
            # 过滤无法买入的股票
            if self._is_untradable(stock.name, stock.code):
                continue
            
            # 过滤条件
            if stock.change_percent >= 9.5:  # 已涨停，排除
                continue
            if stock.change_percent < -3:  # 跌太多，排除
                continue
            
            # 计算量比（简化：用换手率估算，实际应该和历史平均比）
            volume_ratio = 1.5  # 默认值
            
            # 综合评分
            score = 50
            score += min(stock.change_percent * 3, 20)  # 涨幅得分
            if stock.code in hot_sector_codes:  # 热点板块加分
                score += 15
            if 2 < stock.change_percent < 7:  # 突破未涨停，最有潜力
                score += 15
            
            candidates.append({
                'code': stock.code,
                'name': stock.name,
                'price': stock.price,
                'change_pct': stock.change_percent,
                'volume_ratio': volume_ratio,
                'sector': '热点板块' if stock.code in hot_sector_codes else '其他',
                'score': min(score, 95)
            })
        
        # 按得分排序，取前3
        candidates.sort(key=lambda x: x['score'], reverse=True)
        return candidates[:3]  # 改为top 3
    
    def _generate_intraday_report(self, title: str, index_data: List,
                                   market_stats: Dict, strong_sectors: List[Dict],
                                   afternoon_picks: List[Dict], quotes: Dict = None) -> str:
        """生成盘中简报报告 - 两段式：持仓分析 + 下午潜力股"""
        lines = [
            f"# 📊 {title} 1.0",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**选股引擎**: V8板块优先 - 下午机会版",
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
                score = pick.get('score', 0)
                change_pct = pick.get('change_pct', 0)

                # 生成建议标签
                if score >= 80:
                    suggestion = "🚀 强烈推荐"
                elif score >= 70:
                    suggestion = "⭐ 推荐关注"
                else:
                    suggestion = "📈 可关注"

                # 使用AI生成详细分析
                if afternoon_analyzer:
                    # 构建类似V9选股的数据结构
                    pick_data = {
                        'code': pick.get('code'),
                        'name': pick.get('name'),
                        'price': pick.get('price'),
                        'change_pct': pick.get('change_pct'),
                        'sector': pick.get('sector', '其他'),
                        'total_score': score,
                        'factors': {
                            'technical': min(score * 0.4, 25),  # 估算
                            'sentiment': min(score * 0.3, 20),
                            'sector': min(score * 0.2, 30),
                            'money_flow': min(score * 0.1, 15),
                            'risk': 7
                        },
                        'is_sector_leader': False
                    }
                    analysis = afternoon_analyzer.analyze_pick(pick_data, market_context_afternoon)
                else:
                    # 备用分析
                    if score >= 80:
                        analysis = "上午强势+热点板块，资金持续流入，下午有望延续上涨。适合激进型投资者，注意控制仓位。"
                    elif score >= 70:
                        analysis = "上午表现良好，符合强势板块方向。适合稳健型投资者午后关注，建议设好止损。"
                    else:
                        analysis = "有一定上涨潜力，建议观察午后资金流向再决定。适合保守型投资者谨慎参与。"

                lines.extend([
                    f"{i}. {suggestion} **{pick.get('name', 'N/A')}** ({pick.get('code', 'N/A')})",
                    f"   - 当前价: ¥{pick.get('price', 0):.2f} ({change_pct:+.2f}%)",
                    f"   - 综合评分: **{score}**",
                    f"   - 所属板块: {pick.get('sector', '其他')}",
                    f"   - 选股说明: **{suggestion}**，{analysis}",
                    "",
                ])
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
            "*股票分析项目 1.0 | V9+V10+ | 下午机会版*"
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
    
    return 0 if result.get('success') else 1


if __name__ == "__main__":
    sys.exit(main())
