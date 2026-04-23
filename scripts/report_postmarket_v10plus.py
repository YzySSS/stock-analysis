#!/usr/bin/env python3
"""
盘后报告 V10+ - 增强版（含推荐回顾和评估）
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
import requests
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict

# 导入V10报告和追踪器
from scripts.report_v10 import StockReportV10
from src.recommendation_tracker import RecommendationTracker

# 导入AI决策仪表盘分析器
try:
    import sys
    sys.path.insert(0, '/workspace/projects/workspace')
    from ai_dashboard_analyzer import AIDashboardAnalyzer, AnalysisResult
    AI_ANALYZER_AVAILABLE = True
    
    # 从 .env 文件加载环境变量
    try:
        from dotenv import load_dotenv
        env_path = '/workspace/projects/workspace/.env'
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print("✅ 已从 .env 加载配置")
    except ImportError:
        pass  # python-dotenv 未安装，依赖系统环境变量
except ImportError:
    AI_ANALYZER_AVAILABLE = False
    print("⚠️ AI决策仪表盘分析器未找到，AI分析功能将不可用")


class PostMarketReportV10Plus:
    """盘后报告增强版"""
    
    def __init__(self):
        self.v10 = StockReportV10()
        self.tracker = RecommendationTracker()
        self.sina = self.v10.sina
        
        # 初始化AI决策仪表盘分析器
        if AI_ANALYZER_AVAILABLE:
            import os
            # 从环境变量读取，如果没有则使用默认值
            # 注意：生产环境建议从配置文件或安全密钥管理服务读取
            api_key = os.getenv("DEEPSEEK_API_KEY")
            base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
            model = os.getenv("AI_MODEL", "deepseek-chat")
            
            self.ai_analyzer = AIDashboardAnalyzer(
                api_key=api_key,
                base_url=base_url,
                model=model
            )
            if self.ai_analyzer.is_available():
                print(f"✅ AI决策仪表盘分析器已加载 (模型: {model})")
            else:
                print("⚠️ AI决策仪表盘分析器未配置API Key")
                print("   请在系统环境变量中设置 DEEPSEEK_API_KEY")
        else:
            self.ai_analyzer = None
    
    def get_today_premarket_picks(self) -> List[Dict]:
        """获取今日盘前推荐"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_recs = self.tracker.get_daily_recommendations(today)
        
        for rec in today_recs:
            if rec['report_type'] == '盘前':
                return rec['picks']
        return []
    
    def get_today_intraday_picks(self) -> List[Dict]:
        """获取今日盘中推荐"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_recs = self.tracker.get_daily_recommendations(today)
        
        for rec in today_recs:
            if rec['report_type'] == '盘中':
                return rec['picks']
        return []
    
    def get_close_prices(self, picks: List[Dict]) -> Dict[str, Dict]:
        """获取收盘价"""
        codes = [p['code'] for p in picks]
        return self.sina.get_quotes(codes)
    
    def get_index_change(self) -> float:
        """获取上证指数当日涨跌幅"""
        try:
            index_quotes = self.sina.get_quotes(['000001'])
            if '000001' in index_quotes:
                return index_quotes['000001'].get('change_pct', 0)
        except:
            pass
        return 0.32  # 默认值，如果获取失败
    
    def evaluate_recommendations(self, picks: List[Dict], close_quotes: Dict) -> List[Dict]:
        """评估推荐表现"""
        results = []
        
        # 获取上证指数涨幅
        index_change = self.get_index_change()
        
        for pick in picks:
            code = pick['code']
            quote = close_quotes.get(code, {})
            
            rec_price = pick['price_at_rec']
            close_price = quote.get('price', rec_price)
            day_change = quote.get('change_pct', 0)
            
            # 计算从推荐到收盘的涨跌幅
            if rec_price > 0:
                performance = ((close_price - rec_price) / rec_price * 100)
            else:
                performance = 0
            
            # 判断推荐是否正确
            # 条件1：不亏损（原来的逻辑）
            if pick['change_pct_at_rec'] > 5:  # 强势推荐
                is_correct = performance > -2  # 跌幅不超过2%
            elif pick['score'] >= 55:  # 高评分
                is_correct = performance > -3  # 跌幅不超过3%
            else:
                is_correct = performance > -5  # 跌幅不超过5%
            
            # 条件2：跑输大盘（新增）
            # 如果表现低于上证指数涨幅，也标记为需要复盘
            underperform = performance < index_change
            
            # 标记类型
            if not is_correct:
                fail_type = '亏损'  # 推荐后下跌
            elif underperform:
                fail_type = '跑输大盘'  # 上涨但低于大盘
            else:
                fail_type = '正常'
            
            results.append({
                'code': code,
                'name': pick['name'],
                'rec_price': rec_price,
                'rec_change': pick['change_pct_at_rec'],
                'score': pick['score'],
                'close_price': close_price,
                'day_change': day_change,
                'performance': performance,
                'index_change': index_change,  # 上证指数涨幅
                'is_correct': is_correct,
                'underperform': underperform,  # 是否跑输大盘
                'fail_type': fail_type,
                'eval_note': '✅ 正确' if (is_correct and not underperform) else ('⚠️ 跑输' if underperform else '❌ 错误')
            })
        
        return results
    
    def analyze_failed_picks(self, failed_picks: List[Dict]) -> str:
        """分析选股失败原因（AI分析）"""
        if not failed_picks or not self.ai_analyzer or not self.ai_analyzer.is_available():
            return ""
        
        lines = []
        lines.append(f"\n{'='*70}")
        lines.append("🔍 选股失败原因分析（AI）")
        lines.append(f"{'='*70}")
        
        for pick in failed_picks:
            code = pick['code']
            name = pick['name']
            performance = pick['performance']
            
            # 获取该股票的详细数据
            try:
                # 获取实时行情
                quotes = self.sina.get_quotes([code])
                quote = quotes.get(code, {})
                
                # 准备AI分析数据
                stock_data = {
                    'code': code,
                    'name': name,
                    'current_price': quote.get('price', pick['close_price']),
                    'change_pct': pick['day_change'],
                    'rec_price': pick['rec_price'],
                    'rec_change': pick['rec_change'],
                    'performance': performance,
                    'score': pick['score'],
                }
                
                # 调用AI分析失败原因
                print(f"  🤖 分析 {name}({code}) 失败原因...")
                failure_analysis = self._analyze_failure_with_ai(stock_data)
                
                lines.append(f"\n📉 {name}({code}) 推荐后下跌 {performance:.2f}%")
                lines.append(f"   {failure_analysis}")
                
            except Exception as e:
                lines.append(f"\n📉 {name}({code}) 推荐后下跌 {performance:.2f}%")
                lines.append(f"   ⚠️ 分析失败: {str(e)[:50]}")
        
        lines.append(f"\n{'='*70}")
        return "\n".join(lines)
    
    def _analyze_failure_with_ai(self, stock_data: Dict) -> str:
        """使用DeepSeek进行深度错误分析"""
        if not self.ai_analyzer or not self.ai_analyzer.is_available():
            return "⚠️ AI分析器未配置"
        
        try:
            print(f"  🤖 DeepSeek深度分析 {stock_data['name']}({stock_data['code']})...")
            
            # 构建深度分析Prompt
            prompt = f"""你是一位量化投资策略分析师，请深度分析以下选股失败案例：

## 📊 选股失败案例

**股票信息：**
- 名称：{stock_data['name']}({stock_data['code']})
- 推荐时间：盘中（约12:30）
- 推荐时价格：¥{stock_data['rec_price']:.2f}
- 推荐时涨跌幅：{stock_data['rec_change']:+.2f}%
- 收盘价格：¥{stock_data['current_price']:.2f}
- 推荐后表现：{stock_data['performance']:+.2f}%（下跌）
- V8综合评分：{stock_data['score']}

## 🔍 请从以下维度深度分析：

### 1️⃣ 当时选股逻辑回顾
- 为什么V8选股器会在此时选中这只股票？
- 哪些因子（技术/财务/行业）给了高分？
- 当时的市场情绪和板块热度如何？

### 2️⃣ 失败原因深度剖析
- **技术层面**：是否存在追高？是否假突破？量能是否配合？
- **板块层面**：所属板块（钢铁）当日整体表现如何？是否拖累个股？
- **市场层面**：大盘走势如何？是否有系统性风险？
- **因子缺陷**：V8评分模型哪里出了问题？权重设置是否合理？

### 3️⃣ 策略问题诊断
- V8选股策略的根本缺陷是什么？
- 是否应该增加哪些过滤条件？
- 追涨策略在何种市场环境下失效？

### 4️⃣ 具体改进建议
- **立即改进**：明天选股时应注意什么？
- **策略优化**：V8模型如何调整？（如增加行业轮动权重、加强趋势确认）
- **风控加强**：是否应该设置更严格的止损或追高风险控制？

## 📋 输出要求

请用以下格式输出（不超过500字）：

**选股逻辑：** xxx

**失败原因：** 
- 技术：xxx
- 板块：xxx  
- 因子：xxx

**策略问题：** xxx

**改进建议：**
- 短期：xxx
- 长期：xxx"""

            # 调用DeepSeek API
            result = self._call_deepseek_for_analysis(prompt)
            return result if result else "⚠️ DeepSeek分析未返回结果"
            
        except Exception as e:
            return f"⚠️ DeepSeek分析失败: {str(e)[:80]}"
    
    def _call_deepseek_for_analysis(self, prompt: str) -> str:
        """调用DeepSeek API进行分析"""
        import requests
        import json
        
        headers = {
            "Authorization": f"Bearer {self.ai_analyzer.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.ai_analyzer.model,
            "messages": [
                {"role": "system", "content": "你是一位专业的量化投资策略分析师，擅长复盘选股策略失败原因并给出改进建议。"},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 800
        }
        
        response = requests.post(
            f"{self.ai_analyzer.base_url}/chat/completions",
            headers=headers,
            json=data,
            timeout=60
        )
        
        response.raise_for_status()
        result = response.json()
        
        return result['choices'][0]['message']['content']
    
    def _generate_ai_dashboard_analysis(self) -> str:
        """生成AI决策仪表盘分析（新增）"""
        lines = []
        lines.append(f"\n{'='*70}")
        lines.append("🤖 AI决策仪表盘分析")
        lines.append(f"{'='*70}")
        
        # 获取持仓列表
        positions = self.v10.positions
        
        # 只分析前3只持仓（避免API调用过多）
        top_positions = positions[:3]
        
        for pos in top_positions:
            code = pos.code
            name = pos.name
            
            # 获取实时行情
            quotes = self.sina.get_quotes([code])
            quote = quotes.get(code, {})
            
            current_price = quote.get('price', pos.current_price or pos.cost_price)
            change_pct = quote.get('change_pct', 0)
            
            # 准备AI分析数据
            stock_data = {
                'code': code,
                'name': name,
                'current_price': current_price,
                'change_pct': change_pct,
                'cost_price': pos.cost_price,
                'quantity': pos.quantity,
            }
            
            # 调用AI分析
            print(f"  🤖 正在分析 {name}({code})...")
            try:
                result = self.ai_analyzer.analyze(stock_data)
                
                if result.success:
                    lines.append(f"\n📊 {result.get_emoji()} {name} ({code})")
                    lines.append(f"   评分: {result.sentiment_score}/100 | 趋势: {result.trend_prediction}")
                    lines.append(f"   建议: {result.operation_advice} (置信度: {result.confidence_level})")
                    
                    # 核心结论
                    core = result.get_core_conclusion()
                    if core:
                        lines.append(f"   核心结论: {core}")
                    
                    # 狙击点位
                    sniper = result.get_sniper_points()
                    if sniper:
                        lines.append(f"   狙击点位:")
                        lines.append(f"      理想买点: {sniper.get('ideal_buy', 'N/A')}")
                        lines.append(f"      止损位: {sniper.get('stop_loss', 'N/A')}")
                        lines.append(f"      目标位: {sniper.get('take_profit', 'N/A')}")
                    
                    # 检查清单
                    checklist = result.get_checklist()
                    if checklist:
                        lines.append(f"   检查清单:")
                        for item in checklist[:3]:  # 只显示前3项
                            lines.append(f"      {item}")
                else:
                    lines.append(f"\n⚠️ {name}({code}) AI分析失败: {result.error_message}")
                    
            except Exception as e:
                lines.append(f"\n⚠️ {name}({code}) AI分析异常: {str(e)[:50]}")
        
        lines.append(f"\n{'='*70}")
        return "\n".join(lines)
    
    def generate_postmarket_report(self) -> str:
        """生成盘后报告"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\n{'='*70}")
        print(f"📋 盘后报告 V10+ | {today} | 含推荐回顾与评估")
        print(f"{'='*70}")
        
        lines = []
        lines.append(f"\n{'='*70}")
        lines.append(f"📊 盘后报告 V10+ | {today} {datetime.now().strftime('%H:%M')}")
        lines.append(f"{'='*70}")
        
        # 1. 生成标准V10报告（持仓+今日选股）
        print("\n1. 生成标准报告...")
        v10_content = self.v10.generate_report("盘后")
        
        # 2. AI决策仪表盘分析（新增）
        ai_analysis_content = ""
        if self.ai_analyzer and self.ai_analyzer.is_available():
            print("\n2. 生成AI决策仪表盘分析...")
            ai_analysis_content = self._generate_ai_dashboard_analysis()
        else:
            print("\n2. 跳过AI分析（未配置）")
        
        # 3. 获取今日盘前和盘中推荐
        print("\n3. 获取今日历史推荐...")
        premarket_picks = self.get_today_premarket_picks()
        intraday_picks = self.get_today_intraday_picks()
        
        # 4. 评估盘前推荐
        premarket_failed = []
        if premarket_picks:
            print("\n4. 评估盘前推荐...")
            lines.append(f"\n{'='*70}")
            lines.append("📈 盘前推荐回顾与评估 (08:50)")
            lines.append(f"{'='*70}")
            
            close_quotes = self.get_close_prices(premarket_picks)
            evaluated = self.evaluate_recommendations(premarket_picks, close_quotes)
            
            correct_count = sum(1 for e in evaluated if e['is_correct'])
            total_count = len(evaluated)
            accuracy = (correct_count / total_count * 100) if total_count > 0 else 0
            
            lines.append(f"\n准确率: {correct_count}/{total_count} ({accuracy:.1f}%)")
            lines.append("-"*70)
            
            for i, ev in enumerate(evaluated, 1):
                # 显示上证指数对比
                index_info = f" | 上证: {ev['index_change']:+.2f}%"
                lines.append(f"\n{i}. {ev['name']} ({ev['code']}) {ev['eval_note']}{index_info}")
                lines.append(f"   推荐时: ¥{ev['rec_price']:.2f} ({ev['rec_change']:+.2f}%)")
                lines.append(f"   收盘价: ¥{ev['close_price']:.2f} ({ev['day_change']:+.2f}%)")
                lines.append(f"   推荐后表现: {ev['performance']:+.2f}%")
                
                # 收集需要复盘的股票（亏损 或 跑输大盘）
                if not ev['is_correct'] or ev['underperform']:
                    premarket_failed.append(ev)
            
            # 显示复盘统计
            loss_count = sum(1 for e in evaluated if not e['is_correct'])
            underperform_count = sum(1 for e in evaluated if e['underperform'])
            if loss_count > 0 or underperform_count > 0:
                lines.append(f"\n📊 复盘统计: 亏损{loss_count}只, 跑输大盘{underperform_count}只")
            
            # AI分析失败原因
            if premarket_failed and self.ai_analyzer and self.ai_analyzer.is_available():
                print(f"\n   分析盘前复盘股票（共{len(premarket_failed)}只）...")
                failure_analysis = self.analyze_failed_picks(premarket_failed)
                if failure_analysis:
                    lines.append(failure_analysis)
            
            # 更新到追踪器
            for ev in evaluated:
                self.tracker.update_close_price(today, ev['code'], 
                                               ev['close_price'], ev['day_change'])
        
        # 5. 评估盘中推荐
        intraday_failed = []
        if intraday_picks:
            print("\n5. 评估盘中推荐...")
            lines.append(f"\n{'='*70}")
            lines.append("📈 盘中推荐回顾与评估 (12:30)")
            lines.append(f"{'='*70}")
            
            close_quotes = self.get_close_prices(intraday_picks)
            evaluated = self.evaluate_recommendations(intraday_picks, close_quotes)
            
            correct_count = sum(1 for e in evaluated if e['is_correct'])
            total_count = len(evaluated)
            accuracy = (correct_count / total_count * 100) if total_count > 0 else 0
            
            lines.append(f"\n准确率: {correct_count}/{total_count} ({accuracy:.1f}%)")
            lines.append("-"*70)
            
            for i, ev in enumerate(evaluated, 1):
                # 显示上证指数对比
                index_info = f" | 上证: {ev['index_change']:+.2f}%"
                lines.append(f"\n{i}. {ev['name']} ({ev['code']}) {ev['eval_note']}{index_info}")
                lines.append(f"   推荐时: ¥{ev['rec_price']:.2f} ({ev['rec_change']:+.2f}%)")
                lines.append(f"   收盘价: ¥{ev['close_price']:.2f} ({ev['day_change']:+.2f}%)")
                lines.append(f"   推荐后表现: {ev['performance']:+.2f}%")
                
                # 收集需要复盘的股票（亏损 或 跑输大盘）
                if not ev['is_correct'] or ev['underperform']:
                    intraday_failed.append(ev)
            
            # 显示复盘统计
            loss_count = sum(1 for e in evaluated if not e['is_correct'])
            underperform_count = sum(1 for e in evaluated if e['underperform'])
            if loss_count > 0 or underperform_count > 0:
                lines.append(f"\n📊 复盘统计: 亏损{loss_count}只, 跑输大盘{underperform_count}只")
            
            # AI分析失败原因
            if intraday_failed and self.ai_analyzer and self.ai_analyzer.is_available():
                print("\n   分析盘中失败原因...")
                failure_analysis = self.analyze_failed_picks(intraday_failed)
                if failure_analysis:
                    lines.append(failure_analysis)
            
            # 更新到追踪器
            for ev in evaluated:
                self.tracker.update_close_price(today, ev['code'],
                                               ev['close_price'], ev['day_change'])
        
        # 5. 生成周报（如果是周五或周日）
        weekday = datetime.now().weekday()
        if weekday in [4, 6]:  # 周五或周日
            print("\n5. 生成周报...")
            weekly_report = self.tracker.generate_weekly_report()
            lines.append(weekly_report)
        
        # 合并报告
        review_content = "\n".join(lines)
        full_report = v10_content + "\n" + ai_analysis_content + "\n" + review_content
        
        # 保存报告
        self._save_report(full_report, "盘后增强版")
        
        return full_report
    
    def _save_report(self, content: str, report_type: str):
        """保存报告"""
        reports_dir = "/workspace/projects/workspace/股票分析项目/reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"{reports_dir}/report_v10plus_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"\n💾 报告已保存: {filename}")
    
    def _parse_positions(self, report_text: str) -> list:
        """解析持仓明细"""
        positions = []
        lines = report_text.split('\n')
        in_positions = False
        
        for line in lines:
            if '持仓明细' in line:
                in_positions = True
                continue
            if '🎯 V8全A股选股策略' in line or '💡 免责声明' in line:
                in_positions = False
            
            if in_positions and line.strip().startswith(('1.', '2.', '3.', '4.', '5.', '6.', '7.')):
                # 解析持仓行
                parts = line.strip().split()
                if len(parts) >= 2:
                    emoji = '🟢' if '🟢' in line else '🔴'
                    name_match = re.search(r'[🟢🔴]\s+(\S+)\s+\((\d+)\)', line)
                    if name_match:
                        name = name_match.group(1)
                        code = name_match.group(2)
                        
                        # 查找盈亏信息
                        profit_match = re.search(r'盈亏:\s+¥([+-]?[\d,.]+).*?\(([+-]?[\d.]+)%\)', line)
                        profit = profit_match.group(1) if profit_match else "0"
                        profit_pct = profit_match.group(2) if profit_match else "0"
                        
                        positions.append({
                            'emoji': emoji,
                            'name': name,
                            'code': code,
                            'profit': profit,
                            'profit_pct': profit_pct
                        })
        
        return positions
    
    def _parse_v8_picks(self, report_text: str) -> list:
        """解析V8选股推荐"""
        picks = []
        lines = report_text.split('\n')
        in_picks = False
        
        for line in lines:
            if '🎯 V8全A股选股策略' in line:
                in_picks = True
                continue
            if '💡 免责声明' in line or '📊 盘后报告 V10+' in line:
                in_picks = False
            
            if in_picks and line.strip().startswith(('1.', '2.', '3.')):
                # 解析选股行
                name_match = re.search(r'⭐\s+(\S+)\s+\((\d+)\)', line)
                if name_match:
                    name = name_match.group(1)
                    code = name_match.group(2)
                    picks.append({'name': name, 'code': code})
        
        return picks
    
    def _parse_accuracy(self, report_text: str) -> dict:
        """解析准确率统计"""
        accuracy = {'premarket': '', 'intraday': ''}
        
        pre_match = re.search(r'盘前推荐回顾与评估.*?准确率:\s*(\d+/\d+).*?\((\d+\.?\d*)%\)', report_text, re.DOTALL)
        if pre_match:
            accuracy['premarket'] = f"{pre_match.group(1)} ({pre_match.group(2)}%)"
        
        intra_match = re.search(r'盘中推荐回顾与评估.*?准确率:\s*(\d+/\d+).*?\((\d+\.?\d*)%\)', report_text, re.DOTALL)
        if intra_match:
            accuracy['intraday'] = f"{intra_match.group(1)} ({intra_match.group(2)}%)"
        
        return accuracy
    
    def send_to_feishu(self, report_text: str):
        """推送到飞书 - 分段推送完整报告"""
        import re
        FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"
        
        today = datetime.now().strftime('%m-%d %H:%M')
        
        # 分段1: 持仓汇总 + 持仓明细
        section1 = self._extract_section(report_text, 
            r'📊 股票报告 V10.*?盘后.*?💼 持仓汇总', 
            r'🎯 V8全A股选股策略')
        
        # 分段2: V8选股 + AI决策仪表盘
        section2 = self._extract_section(report_text,
            r'🎯 V8全A股选股策略',
            r'💡 免责声明')
        
        # 分段3: 推荐回顾 + 深度分析
        section3 = self._extract_section(report_text,
            r'📊 盘后报告 V10+',
            r'💡 免责声明.*?\n')
        
        # 发送分段消息
        segments = [
            (f"📊 盘后报告 V10+ | {today} | 第1/3段", section1),
            (f"📊 盘后报告 V10+ | {today} | 第2/3段", section2),
            (f"📊 盘后报告 V10+ | {today} | 第3/3段", section3),
        ]
        
        success_count = 0
        for title, content in segments:
            if not content.strip():
                continue
                
            # 构建文本消息（支持长文本）
            message = {
                "msg_type": "text",
                "content": {
                    "text": f"{title}\n{'='*60}\n{content[:3500]}\n{'='*60}"
                }
            }
            
            try:
                response = requests.post(
                    FEISHU_WEBHOOK,
                    json=message,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                result = response.json()
                if result.get('code') == 0:
                    success_count += 1
                    print(f"✅ 飞书推送第{success_count}/3段成功")
                else:
                    print(f"⚠️ 飞书推送失败: {result}")
                time.sleep(0.5)  # 避免发送过快
            except Exception as e:
                print(f"⚠️ 飞书推送异常: {e}")
        
        if success_count == 0:
            # 如果都失败了，使用备用方案
            self._send_text_fallback(report_text)
    
    def _extract_section(self, text: str, start_pattern: str, end_pattern: str) -> str:
        """从报告中提取指定段落"""
        import re
        
        start_match = re.search(start_pattern, text, re.DOTALL)
        if not start_match:
            return ""
        
        start_pos = start_match.start()
        
        end_match = re.search(end_pattern, text[start_pos:], re.DOTALL)
        if end_match:
            end_pos = start_pos + end_match.start()
            return text[start_pos:end_pos]
        else:
            return text[start_pos:]
    
    def _send_text_fallback(self, report_text: str):
        """卡片发送失败时的回退方案"""
        FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"
        
        lines = report_text.split('\n')
        summary = []
        for line in lines:
            if any(keyword in line for keyword in ['盘后报告', '持仓汇总', '准确率', '策略评估', '推荐回顾']):
                summary.append(line)
            if len(summary) > 50:
                break
        
        message = {
            "msg_type": "text",
            "content": {
                "text": f"📊 盘后报告V10+ | {datetime.now().strftime('%m-%d %H:%M')}\n\n"
                       f"{chr(10).join(summary[:30])}\n\n"
                       f"【完整报告见文件】"
            }
        }
        
        try:
            requests.post(FEISHU_WEBHOOK, json=message, headers={'Content-Type': 'application/json'}, timeout=10)
            print("✅ 已回退到文本推送")
        except Exception as e:
            print(f"⚠️ 文本推送也失败: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='盘后报告V10+')
    parser.add_argument('--push', action='store_true', help='推送到飞书')
    args = parser.parse_args()
    
    report = PostMarketReportV10Plus()
    content = report.generate_postmarket_report()
    
    print(content)
    
    if args.push:
        report.send_to_feishu(content)
