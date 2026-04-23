#!/usr/bin/env python3
"""
实时行情监控模块 - 盘中分析
支持：实时价格监控 / 买卖点触发 / 异动预警
"""

import time
import logging
from typing import Dict, List, Callable, Optional
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class PriceAlert:
    """价格预警"""
    code: str
    name: str
    alert_type: str  # 'break_high', 'break_low', 'volume_spike', 'rapid_change'
    current_price: float
    target_price: float
    change_pct: float
    message: str
    timestamp: datetime


@dataclass
class TradingSignal:
    """交易信号"""
    code: str
    name: str
    signal_type: str  # 'buy', 'sell', 'stop_loss', 'take_profit'
    current_price: float
    target_price: float
    confidence: int  # 0-100
    reason: str
    timestamp: datetime


class RealtimeMonitor:
    """
    实时行情监控器
    
    监控内容：
    1. 价格突破（突破买入价/止损价/目标价）
    2. 放量异动（量比突然放大）
    3. 快速涨跌（5分钟内涨跌超2%）
    """
    
    def __init__(self, alert_callback: Optional[Callable] = None):
        self.alert_callback = alert_callback
        self.watch_list: Dict[str, Dict] = {}  # 监控列表
        self.price_history: Dict[str, List[float]] = {}  # 价格历史
        self.is_running = False
    
    def add_watch(self, 
                  code: str, 
                  name: str,
                  buy_price: Optional[float] = None,
                  stop_loss: Optional[float] = None,
                  take_profit: Optional[float] = None,
                  volume_threshold: float = 2.0):
        """
        添加监控股票
        
        Args:
            code: 股票代码
            name: 股票名称
            buy_price: 买入触发价
            stop_loss: 止损价
            take_profit: 止盈价
            volume_threshold: 量比阈值（超过此值触发预警）
        """
        self.watch_list[code] = {
            'name': name,
            'buy_price': buy_price,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'volume_threshold': volume_threshold,
            'last_price': None,
            'last_volume': None
        }
        self.price_history[code] = []
        logger.info(f"添加监控: {name}({code}), 买入价:{buy_price}, 止损:{stop_loss}, 止盈:{take_profit}")
    
    def remove_watch(self, code: str):
        """移除监控"""
        if code in self.watch_list:
            del self.watch_list[code]
            del self.price_history[code]
            logger.info(f"移除监控: {code}")
    
    def start_monitoring(self, interval: int = 60):
        """
        开始监控（阻塞运行）
        
        Args:
            interval: 检查间隔（秒），默认60秒
        """
        self.is_running = True
        logger.info(f"开始实时监控，间隔{interval}秒，监控{len(self.watch_list)}只股票")
        
        while self.is_running:
            try:
                self._check_all()
                time.sleep(interval)
            except Exception as e:
                logger.error(f"监控异常: {e}")
                time.sleep(interval)
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        logger.info("停止实时监控")
    
    def _check_all(self):
        """检查所有监控股票"""
        for code, config in self.watch_list.items():
            try:
                self._check_single(code, config)
            except Exception as e:
                logger.error(f"检查 {code} 失败: {e}")
    
    def _check_single(self, code: str, config: Dict):
        """检查单只股票"""
        # 获取实时价格（简化版，实际应接入实时数据源）
        from data_source import data_manager
        
        source = data_manager.get_source()
        if not source:
            return
        
        # 获取实时行情
        try:
            # 这里应该用实时接口，暂时用spot数据
            stocks = source.get_a_stock_spot()
            stock_info = next((s for s in stocks if s.code == code[:6]), None)
            
            if not stock_info:
                return
            
            current_price = stock_info.price
            current_volume = stock_info.volume
            change_pct = stock_info.change_percent
            
            # 记录价格历史
            self.price_history[code].append(current_price)
            if len(self.price_history[code]) > 20:  # 保留最近20个价格点
                self.price_history[code].pop(0)
            
            # 检查交易信号
            signals = []
            
            # 1. 买入信号
            if config['buy_price'] and current_price <= config['buy_price'] * 1.005:
                signals.append(TradingSignal(
                    code=code,
                    name=config['name'],
                    signal_type='buy',
                    current_price=current_price,
                    target_price=config['buy_price'],
                    confidence=80,
                    reason=f"价格触及买入点 {config['buy_price']:.2f}",
                    timestamp=datetime.now()
                ))
            
            # 2. 止损信号
            if config['stop_loss'] and current_price <= config['stop_loss']:
                signals.append(TradingSignal(
                    code=code,
                    name=config['name'],
                    signal_type='stop_loss',
                    current_price=current_price,
                    target_price=config['stop_loss'],
                    confidence=95,
                    reason=f"⚠️ 跌破止损位 {config['stop_loss']:.2f}！",
                    timestamp=datetime.now()
                ))
            
            # 3. 止盈信号
            if config['take_profit'] and current_price >= config['take_profit']:
                signals.append(TradingSignal(
                    code=code,
                    name=config['name'],
                    signal_type='take_profit',
                    current_price=current_price,
                    target_price=config['take_profit'],
                    confidence=85,
                    reason=f"达到目标价位 {config['take_profit']:.2f}",
                    timestamp=datetime.now()
                ))
            
            # 4. 快速异动（5分钟内涨跌超2%）
            if len(self.price_history[code]) >= 5:
                recent_change = (current_price - self.price_history[code][-5]) / self.price_history[code][-5] * 100
                if abs(recent_change) > 2:
                    signals.append(TradingSignal(
                        code=code,
                        name=config['name'],
                        signal_type='rapid_change',
                        current_price=current_price,
                        target_price=0,
                        confidence=70,
                        reason=f"{'快速上涨' if recent_change > 0 else '快速下跌'} {recent_change:.2f}% (5分钟内)",
                        timestamp=datetime.now()
                    ))
            
            # 触发回调
            for signal in signals:
                if self.alert_callback:
                    self.alert_callback(signal)
                else:
                    self._default_alert_handler(signal)
            
            # 更新最后价格
            config['last_price'] = current_price
            config['last_volume'] = current_volume
            
        except Exception as e:
            logger.error(f"获取 {code} 实时数据失败: {e}")
    
    def _default_alert_handler(self, signal: TradingSignal):
        """默认预警处理"""
        emoji_map = {
            'buy': '🟢',
            'sell': '🔴',
            'stop_loss': '🚨',
            'take_profit': '💰',
            'rapid_change': '⚡'
        }
        
        emoji = emoji_map.get(signal.signal_type, '⚠️')
        
        print(f"\n{emoji} 交易信号触发!")
        print(f"股票: {signal.name} ({signal.code})")
        print(f"类型: {signal.signal_type}")
        print(f"当前价: {signal.current_price:.2f}")
        print(f"理由: {signal.reason}")
        print(f"时间: {signal.timestamp.strftime('%H:%M:%S')}")
        print("-" * 50)


class IntradayAnalyzer:
    """
    盘中分析器
    定时生成盘中简报
    """
    
    def __init__(self):
        self.monitor = RealtimeMonitor()
    
    def generate_intraday_report(self, positions: List[Dict]) -> str:
        """
        生成盘中简报
        
        Args:
            positions: 持仓列表 [{'code': '000001', 'name': '平安银行', 'cost': 10.5, 'shares': 1000}]
        
        Returns:
            Markdown格式的盘中简报
        """
        from data_source import data_manager
        
        source = data_manager.get_source()
        if not source:
            return "无法获取数据"
        
        lines = [
            f"# 📊 盘中简报 - {datetime.now().strftime('%H:%M')}",
            "",
            "## 💼 持仓监控",
            ""
        ]
        
        total_profit = 0
        
        for pos in positions:
            try:
                stocks = source.get_a_stock_spot()
                stock = next((s for s in stocks if s.code == pos['code'][:6]), None)
                
                if stock:
                    current = stock.price
                    cost = pos['cost']
                    shares = pos['shares']
                    profit = (current - cost) * shares
                    profit_pct = (current - cost) / cost * 100
                    
                    emoji = "🟢" if profit > 0 else "🔴"
                    
                    lines.append(f"### {emoji} {pos['name']} ({pos['code']})")
                    lines.append(f"- 成本: {cost:.2f} | 现价: {current:.2f}")
                    lines.append(f"- 盈亏: {profit:+.2f} ({profit_pct:+.2f}%)")
                    lines.append(f"- 涨跌: {stock.change_percent:+.2f}%")
                    lines.append("")
                    
                    total_profit += profit
            except Exception as e:
                logger.error(f"获取 {pos['code']} 数据失败: {e}")
        
        lines.extend([
            "---",
            "",
            f"**总盈亏**: {total_profit:+.2f}",
            f"**更新时间**: {datetime.now().strftime('%H:%M:%S')}"
        ])
        
        return "\n".join(lines)


if __name__ == "__main__":
    print("🧪 实时行情监控测试")
    print("="*60)
    
    # 创建监控器
    monitor = RealtimeMonitor()
    
    # 添加监控（示例）
    monitor.add_watch(
        code="000001.SZ",
        name="平安银行",
        buy_price=10.50,
        stop_loss=10.00,
        take_profit=11.50
    )
    
    print("\n📊 已添加监控:")
    for code, config in monitor.watch_list.items():
        print(f"  {config['name']}({code}): 买入≤{config['buy_price']}, 止损≤{config['stop_loss']}, 止盈≥{config['take_profit']}")
    
    print("\n⏰ 开始监控（按Ctrl+C停止）...")
    print("-"*60)
    
    try:
        monitor.start_monitoring(interval=30)  # 每30秒检查一次
    except KeyboardInterrupt:
        print("\n\n✅ 监控已停止")
