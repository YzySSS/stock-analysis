#!/usr/bin/env python3
"""
盘后复盘分析模块

功能：
1. 汇总当天所有报告（盘前/盘中/盘后）
2. 对比预测与实际走势
3. 分析准确率及原因
4. 生成策略改进建议
5. 完成回测记录
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import re

logger = logging.getLogger(__name__)


@dataclass
class PredictionRecord:
    """预测记录"""
    code: str
    name: str
    report_type: str          # premarket/intraday/postmarket
    prediction_time: str      # 预测时间
    
    # 预测内容
    predicted_direction: str  # 预测方向: up/down/sideways
    operation_advice: str     # 操作建议
    sentiment_score: int      # 情绪评分
    key_price_levels: Dict    # 关键价位
    
    # 实际结果（盘后填写）
    actual_direction: str = None      # 实际方向
    actual_close: float = None        # 实际收盘价
    actual_change: float = None       # 实际涨跌幅
    prediction_accuracy: bool = None  # 预测是否准确
    error_reason: str = None          # 错误原因


@dataclass
class DailySummary:
    """每日复盘总结"""
    date: str
    
    # 统计
    total_predictions: int = 0
    accurate_count: int = 0
    accuracy_rate: float = 0.0
    
    # 分类统计
    buy_accuracy: float = 0.0    # 买入建议准确率
    sell_accuracy: float = 0.0   # 卖出建议准确率
    hold_accuracy: float = 0.0   # 观望建议准确率
    
    # 问题分析
    common_errors: List[str] = None       # 常见错误
    missed_signals: List[str] = None      # 遗漏信号
    false_alarms: List[str] = None        # 误判信号
    
    # 改进建议
    strategy_adjustments: List[str] = None  # 策略调整建议
    parameter_tweaks: Dict = None           # 参数调整建议
    
    # 数据源
    reports_reviewed: List[str] = None    # 审阅的报告列表
    
    def __post_init__(self):
        if self.common_errors is None:
            self.common_errors = []
        if self.missed_signals is None:
            self.missed_signals = []
        if self.false_alarms is None:
            self.false_alarms = []
        if self.strategy_adjustments is None:
            self.strategy_adjustments = []
        if self.parameter_tweaks is None:
            self.parameter_tweaks = {}
        if self.reports_reviewed is None:
            self.reports_reviewed = []


class DailyReportManager:
    """
    每日报告管理器
    
    管理报告存储结构：
    daily_reports/
    ├── premarket/      # 盘前报告
    ├── intraday/       # 盘中报告
    ├── postmarket/     # 盘后报告
    └── summary/        # 复盘总结
    """
    
    def __init__(self, base_path: str = None):
        if base_path is None:
            base_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 'daily_reports'
            )
        self.base_path = Path(base_path).resolve()
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保目录结构存在"""
        for subdir in ['premarket', 'intraday', 'postmarket', 'summary']:
            (self.base_path / subdir).mkdir(parents=True, exist_ok=True)
    
    def save_report(self, report_content: str, report_type: str, 
                   date: str = None, filename: str = None) -> str:
        """
        保存报告
        
        Args:
            report_content: 报告内容
            report_type: premarket/intraday/postmarket
            date: 日期 (默认今天)
            filename: 文件名 (默认自动生成)
        
        Returns:
            保存的文件路径
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        if filename is None:
            timestamp = datetime.now().strftime('%H%M%S')
            filename = f"{date}_{timestamp}.md"
        
        # 保存到对应目录
        save_dir = self.base_path / report_type
        save_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = save_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"报告已保存: {filepath}")
        return str(filepath)
    
    def get_reports_by_date(self, date: str = None, 
                           report_type: str = None) -> List[Path]:
        """
        获取某天的报告
        
        Args:
            date: 日期 (YYYYMMDD)
            report_type: 报告类型 (None表示全部)
        
        Returns:
            报告文件路径列表
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        reports = []
        
        if report_type:
            dirs = [self.base_path / report_type]
        else:
            dirs = [self.base_path / d for d in ['premarket', 'intraday', 'postmarket']]
        
        for dir_path in dirs:
            if dir_path.exists():
                for file in dir_path.glob(f"{date}_*.md"):
                    reports.append(file)
        
        return sorted(reports)
    
    def get_latest_summary(self, days: int = 7) -> List[Dict]:
        """获取最近N天的复盘总结"""
        summaries = []
        summary_dir = self.base_path / 'summary'
        
        if not summary_dir.exists():
            return summaries
        
        # 获取所有总结文件
        files = sorted(summary_dir.glob('summary_*.json'), reverse=True)
        
        for file in files[:days]:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    summaries.append(data)
            except Exception as e:
                logger.warning(f"读取总结文件失败 {file}: {e}")
        
        return summaries


class PostMarketAnalyzer:
    """
    盘后复盘分析器
    
    对比预测与实际，分析准确率
    """
    
    def __init__(self, report_manager: DailyReportManager = None):
        self.report_manager = report_manager or DailyReportManager()
        self.predictions: List[PredictionRecord] = []
    
    def extract_predictions_from_report(self, report_path: Path) -> List[PredictionRecord]:
        """
        从报告中提取预测信息
        
        解析报告内容，提取：
        - 股票代码、名称
        - 预测方向
        - 操作建议
        - 关键价位
        """
        predictions = []
        
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 从文件名获取报告类型和时间
            filename = report_path.name
            date_match = re.search(r'(\d{8})_(\d{6})', filename)
            if date_match:
                date_str = date_match.group(1)
                time_str = date_match.group(2)
                prediction_time = f"{date_str} {time_str[:2]}:{time_str[2:4]}:{time_str[4:]}"
            else:
                prediction_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 判断报告类型
            report_type = 'unknown'
            if 'premarket' in str(report_path):
                report_type = 'premarket'
            elif 'intraday' in str(report_path):
                report_type = 'intraday'
            elif 'postmarket' in str(report_path):
                report_type = 'postmarket'
            
            # 解析股票信息 (简化版，实际需要更复杂的解析)
            # 匹配类似: **平安银行(000001)** 或 ### 1. 🟢 平安银行 (000001)
            stock_patterns = [
                r'\*\*(\w+)\((\d{6})\)\*\*',  # **平安银行(000001)**
                r'### \d+\. [🟢🟡🔴] (\w+) \((\d{6})\)',  # ### 1. 🟢 平安银行 (000001)
                r'\*\*(\w+)\((\d+)\.SZ\)\*\*',  # **平安银行(000001.SZ)**
                r'\*\*(\w+)\((\d+)\.SH\)\*\*',  # **平安银行(600001.SH)**
            ]
            
            stocks = []
            for pattern in stock_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    if isinstance(match, tuple):
                        name, code = match
                        code = code.replace('.SZ', '').replace('.SH', '')
                        stocks.append((name, code))
                    else:
                        # 尝试其他匹配方式
                        pass
            
            # 去重
            stocks = list(set(stocks))
            
            for name, code in stocks:
                # 提取操作建议 (在股票附近查找)
                # 简化处理，实际需要更精确的文本分析
                operation = self._extract_operation_near_stock(content, name, code)
                score = self._extract_score_near_stock(content, name, code)
                
                pred = PredictionRecord(
                    code=code,
                    name=name,
                    report_type=report_type,
                    prediction_time=prediction_time,
                    predicted_direction=self._operation_to_direction(operation),
                    operation_advice=operation,
                    sentiment_score=score,
                    key_price_levels={}
                )
                predictions.append(pred)
                
        except Exception as e:
            logger.error(f"解析报告失败 {report_path}: {e}")
        
        return predictions
    
    def _operation_to_direction(self, operation: str) -> str:
        """操作建议转方向"""
        if not operation:
            return 'sideways'
        if '买入' in operation or '加仓' in operation:
            return 'up'
        if '卖出' in operation or '减仓' in operation:
            return 'down'
        return 'sideways'
    
    def _extract_operation_near_stock(self, content: str, name: str, code: str) -> str:
        """在股票名称附近提取操作建议"""
        # 查找股票位置
        patterns = [
            rf'\*\*{name}\({code}\)\*\*.*?建议\*\*[:：](.+?)(?:\n|\|)',
            rf'{name}.*?操作.*?建议[:：](.+?)(?:\n|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                return match.group(1).strip()[:20]
        
        return "观望"
    
    def _extract_score_near_stock(self, content: str, name: str, code: str) -> int:
        """在股票名称附近提取评分"""
        patterns = [
            rf'\*\*{name}\({code}\)\*\*.*?评分[:：](\d+)',
            rf'{name}.*?评分[:：](\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return int(match.group(1))
        
        return 50
    
    def get_actual_performance(self, code: str, date: str = None) -> Dict:
        """
        获取股票实际表现
        
        从AkShare获取当天实际走势
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        try:
            import akshare as ak
            
            # 获取当天行情
            df = ak.stock_zh_a_spot_em()
            stock_row = df[df['代码'].astype(str).str.zfill(6) == code.zfill(6)]
            
            if not stock_row.empty:
                row = stock_row.iloc[0]
                return {
                    'close': float(row.get('最新价', 0)),
                    'change': float(row.get('涨跌幅', 0)),
                    'direction': 'up' if float(row.get('涨跌幅', 0)) > 0 else 'down' if float(row.get('涨跌幅', 0)) < 0 else 'sideways',
                    'high': float(row.get('最高', 0)),
                    'low': float(row.get('最低', 0)),
                    'volume': int(row.get('成交量', 0))
                }
        except Exception as e:
            logger.error(f"获取实际表现失败 {code}: {e}")
        
        return None
    
    def analyze_predictions(self, predictions: List[PredictionRecord], 
                           date: str = None) -> DailySummary:
        """
        分析预测准确率
        
        对比预测与实际走势，生成复盘总结
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        summary = DailySummary(date=date)
        summary.total_predictions = len(predictions)
        
        # 按操作建议分类统计
        buy_preds = []
        sell_preds = []
        hold_preds = []
        
        for pred in predictions:
            # 获取实际表现
            actual = self.get_actual_performance(pred.code, date)
            if actual:
                pred.actual_close = actual['close']
                pred.actual_change = actual['change']
                pred.actual_direction = actual['direction']
                
                # 判断预测是否准确
                pred.prediction_accuracy = self._judge_accuracy(
                    pred.predicted_direction, 
                    pred.actual_direction,
                    pred.operation_advice,
                    pred.actual_change
                )
                
                if pred.prediction_accuracy:
                    summary.accurate_count += 1
                else:
                    # 记录错误原因
                    pred.error_reason = self._analyze_error_reason(pred, actual)
                
                # 分类
                if '买入' in pred.operation_advice:
                    buy_preds.append(pred)
                elif '卖出' in pred.operation_advice:
                    sell_preds.append(pred)
                else:
                    hold_preds.append(pred)
        
        # 计算准确率
        if summary.total_predictions > 0:
            summary.accuracy_rate = round(summary.accurate_count / summary.total_predictions * 100, 1)
        
        # 分类准确率
        summary.buy_accuracy = self._calc_category_accuracy(buy_preds)
        summary.sell_accuracy = self._calc_category_accuracy(sell_preds)
        summary.hold_accuracy = self._calc_category_accuracy(hold_preds)
        
        # 分析常见错误
        error_reasons = [p.error_reason for p in predictions if p.error_reason]
        summary.common_errors = self._summarize_errors(error_reasons)
        
        # 生成改进建议
        summary.strategy_adjustments = self._generate_strategy_advice(summary, predictions)
        
        return summary
    
    def _judge_accuracy(self, predicted: str, actual: str, 
                       operation: str, actual_change: float) -> bool:
        """判断预测是否准确"""
        # 买入建议：实际上涨或大涨即为准确
        if '买入' in operation:
            return actual_change > 0
        
        # 卖出建议：实际下跌即为准确
        if '卖出' in operation:
            return actual_change < 0
        
        # 观望建议：实际波动不大即为准确
        if '观望' in operation:
            return abs(actual_change) < 2
        
        # 默认按方向判断
        return predicted == actual
    
    def _analyze_error_reason(self, pred: PredictionRecord, actual: Dict) -> str:
        """分析错误原因"""
        reasons = []
        
        change = actual.get('change', 0)
        
        # 方向判断错误
        if '买入' in pred.operation_advice and change < -2:
            reasons.append("买入后大跌，可能受大盘影响")
        elif '卖出' in pred.operation_advice and change > 2:
            reasons.append("卖出后大涨，可能错过机会")
        
        # 评分与走势不符
        if pred.sentiment_score > 70 and change < 0:
            reasons.append("高评分但下跌，模型过度乐观")
        elif pred.sentiment_score < 40 and change > 0:
            reasons.append("低评分但上涨，模型过度悲观")
        
        # 技术信号失效
        if not reasons:
            if abs(change) > 5:
                reasons.append("极端行情，技术信号失效")
            else:
                reasons.append("正常波动范围内")
        
        return "；".join(reasons)
    
    def _calc_category_accuracy(self, preds: List[PredictionRecord]) -> float:
        """计算分类准确率"""
        if not preds:
            return 0.0
        accurate = sum(1 for p in preds if p.prediction_accuracy)
        return round(accurate / len(preds) * 100, 1)
    
    def _summarize_errors(self, error_reasons: List[str]) -> List[str]:
        """总结常见错误"""
        if not error_reasons:
            return []
        
        # 统计错误类型
        error_counts = {}
        for reason in error_reasons:
            # 简化分类
            if '大盘' in reason or '市场' in reason:
                key = "受大盘/市场情绪影响"
            elif '过度乐观' in reason:
                key = "模型过度乐观"
            elif '过度悲观' in reason:
                key = "模型过度悲观"
            elif '极端' in reason:
                key = "极端行情应对不足"
            else:
                key = "其他"
            
            error_counts[key] = error_counts.get(key, 0) + 1
        
        # 排序返回
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        return [f"{err} ({count}次)" for err, count in sorted_errors]
    
    def _generate_strategy_advice(self, summary: DailySummary, 
                                  predictions: List[PredictionRecord]) -> List[str]:
        """生成策略改进建议"""
        advice = []
        
        # 基于准确率给出建议
        if summary.accuracy_rate < 50:
            advice.append("整体准确率偏低，建议降低仓位，增加观望比例")
        elif summary.accuracy_rate > 70:
            advice.append("整体准确率良好，可维持当前策略")
        
        # 分类建议
        if summary.buy_accuracy < 50:
            advice.append("买入信号准确率偏低，建议收紧买入条件（如评分要求>75分）")
        if summary.sell_accuracy < 50:
            advice.append("卖出信号准确率偏低，建议增加止损条件")
        
        # 基于常见错误
        for error in summary.common_errors:
            if "大盘" in error:
                advice.append("增加大盘环境判断，大盘下跌时减少买入操作")
            if "过度乐观" in error:
                advice.append("调整评分模型，降低高评分股票的乐观倾向")
            if "极端行情" in error:
                advice.append("增加极端行情检测，大波动时切换为观望模式")
        
        # 参数调整建议
        if not advice:
            advice.append("当前策略表现正常，继续观察")
        
        return advice
    
    def generate_postmarket_report(self, date: str = None) -> str:
        """
        生成盘后复盘报告
        
        汇总当天所有报告，对比预测与实际
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 获取当天所有报告
        reports = self.report_manager.get_reports_by_date(date)
        
        if not reports:
            return f"# {date} 盘后复盘\n\n当天无报告记录"
        
        # 2. 提取预测
        all_predictions = []
        for report in reports:
            preds = self.extract_predictions_from_report(report)
            all_predictions.extend(preds)
        
        # 3. 分析准确率
        summary = self.analyze_predictions(all_predictions, date)
        summary.reports_reviewed = [str(r) for r in reports]
        
        # 4. 保存复盘总结
        self._save_summary(summary)
        
        # 5. 生成报告文本
        return self._format_postmarket_report(summary, all_predictions)
    
    def _save_summary(self, summary: DailySummary):
        """保存复盘总结"""
        summary_file = self.report_manager.base_path / 'summary' / f"summary_{summary.date}.json"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(summary), f, ensure_ascii=False, indent=2)
        
        logger.info(f"复盘总结已保存: {summary_file}")
    
    def _format_postmarket_report(self, summary: DailySummary, 
                                   predictions: List[PredictionRecord]) -> str:
        """格式化盘后复盘报告"""
        lines = [
            f"# 📊 {summary.date} 盘后复盘报告",
            "",
            "## 📈 准确率统计",
            "",
            f"- **总预测数**: {summary.total_predictions} 只",
            f"- **准确数**: {summary.accurate_count} 只",
            f"- **整体准确率**: {summary.accuracy_rate}%",
            "",
            "### 分类准确率",
            f"- 🟢 买入建议: {summary.buy_accuracy}%",
            f"- 🔴 卖出建议: {summary.sell_accuracy}%",
            f"- 🟡 观望建议: {summary.hold_accuracy}%",
            "",
            "---",
            "",
            "## 🔍 详细对比",
            "",
        ]
        
        # 按准确率排序展示
        sorted_preds = sorted(predictions, 
                             key=lambda x: (x.prediction_accuracy == False, x.code))
        
        for pred in sorted_preds:
            emoji = "✅" if pred.prediction_accuracy else "❌"
            actual_change = f"{pred.actual_change:+.2f}%" if pred.actual_change else "N/A"
            
            lines.append(f"### {emoji} {pred.name} ({pred.code})")
            lines.append(f"- **预测**: {pred.operation_advice} | 评分{pred.sentiment_score}")
            lines.append(f"- **实际**: {actual_change}")
            
            if pred.error_reason:
                lines.append(f"- **错误原因**: {pred.error_reason}")
            
            lines.append("")
        
        # 问题分析
        if summary.common_errors:
            lines.extend([
                "---",
                "",
                "## ⚠️ 问题分析",
                "",
                "### 常见错误",
            ])
            for error in summary.common_errors:
                lines.append(f"- {error}")
            lines.append("")
        
        # 改进建议
        if summary.strategy_adjustments:
            lines.extend([
                "---",
                "",
                "## 💡 策略改进建议",
                "",
            ])
            for i, advice in enumerate(summary.strategy_adjustments, 1):
                lines.append(f"{i}. {advice}")
            lines.append("")
        
        # 参数调整
        lines.extend([
            "---",
            "",
            "## 🔧 参数调整记录",
            "",
        ])
        
        if summary.parameter_tweaks:
            for param, value in summary.parameter_tweaks.items():
                lines.append(f"- **{param}**: {value}")
        else:
            lines.append("- 本次无参数调整")
        
        lines.append("")
        lines.append(f"---\n\n*复盘时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        return "\n".join(lines)


# 便捷函数
def create_daily_summary(date: str = None) -> str:
    """
    创建每日复盘总结
    
    便捷函数，一键生成盘后复盘报告
    """
    manager = DailyReportManager()
    analyzer = PostMarketAnalyzer(manager)
    
    report = analyzer.generate_postmarket_report(date)
    
    # 保存报告
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    report_path = manager.save_report(
        report, 
        'postmarket', 
        date,
        f"{date}_postmarket_summary.md"
    )
    
    logger.info(f"盘后复盘报告已生成: {report_path}")
    return report


if __name__ == "__main__":
    print("🧪 盘后复盘分析模块测试")
    print("=" * 60)
    
    # 1. 测试目录结构
    print("\n1. 测试目录结构")
    manager = DailyReportManager()
    print(f"✅ 报告目录: {manager.base_path}")
    for subdir in ['premarket', 'intraday', 'postmarket', 'summary']:
        path = manager.base_path / subdir
        print(f"   {subdir}/: {'存在' if path.exists() else '不存在'}")
    
    # 2. 创建测试报告
    print("\n2. 创建测试报告")
    test_report = """# 测试报告
**平安银行(000001)** 买入 | 评分72
建议积极买入

**贵州茅台(600519)** 观望 | 评分58
建议观望等待
"""
    
    premarket_path = manager.save_report(test_report, 'premarket')
    print(f"✅ 盘前报告: {premarket_path}")
    
    # 3. 测试解析预测
    print("\n3. 测试预测解析")
    analyzer = PostMarketAnalyzer(manager)
    predictions = analyzer.extract_predictions_from_report(Path(premarket_path))
    print(f"✅ 解析到 {len(predictions)} 条预测")
    for p in predictions:
        print(f"   {p.name}({p.code}): {p.operation_advice}")
    
    # 4. 测试生成复盘
    print("\n4. 生成盘后复盘")
    # 模拟实际数据
    for p in predictions:
        p.actual_change = 2.5 if '买入' in p.operation_advice else -1.0
        p.prediction_accuracy = True
    
    summary = analyzer.analyze_predictions(predictions)
    print(f"✅ 复盘生成完成")
    print(f"   总预测: {summary.total_predictions}")
    print(f"   准确率: {summary.accuracy_rate}%")
    print(f"   改进建议: {len(summary.strategy_adjustments)} 条")
    
    # 5. 生成完整报告
    print("\n5. 生成盘后复盘报告")
    report = analyzer._format_postmarket_report(summary, predictions)
    print(report[:500] + "...")
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")
