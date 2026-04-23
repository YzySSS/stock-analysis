#!/usr/bin/env python3
"""
持仓管理模块
============
管理用户的持仓股票，记录买入成本和当前状态
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """持仓记录"""
    code: str              # 股票代码
    name: str              # 股票名称
    buy_price: float       # 买入价格
    buy_date: str          # 买入日期
    shares: int            # 持股数量
    current_price: float = 0.0   # 当前价格
    current_return: float = 0.0  # 当前收益率
    stop_loss: float = 0.0       # 止损价
    target_price: float = 0.0    # 目标价
    notes: str = ""              # 备注


class PositionManager:
    """持仓管理器"""
    
    def __init__(self, data_file: str = None):
        if data_file is None:
            # 使用固定路径，不随环境变量变化
            # 优先从项目根目录的data文件夹加载
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_file = os.path.join(project_root, 'data', 'positions.json')
            
            # 如果不存在，尝试默认路径
            if not os.path.exists(data_file):
                base_dir = os.path.expanduser('~/.clawdbot')
                data_file = os.path.join(base_dir, 'positions.json')
        
        self.data_file = data_file
        self.positions: List[Position] = []
        self._load_positions()
    
    def _load_positions(self):
        """加载持仓数据"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 支持两种格式：列表或字典
                    if isinstance(data, list):
                        # 直接是列表格式
                        self.positions = [Position(**p) for p in data]
                    elif isinstance(data, dict):
                        # 字典格式，取positions字段
                        self.positions = [Position(**p) for p in data.get('positions', [])]
                    else:
                        self.positions = []
            except Exception as e:
                print(f"⚠️ 加载持仓数据失败: {e}")
                self.positions = []
        else:
            self.positions = []
    
    def save_positions(self):
        """保存持仓数据"""
        try:
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            data = {
                'last_update': datetime.now().isoformat(),
                'positions': [asdict(p) for p in self.positions]
            }
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"❌ 保存持仓数据失败: {e}")
            return False
    
    def add_position(self, code: str, name: str, buy_price: float, 
                     shares: int, buy_date: str = None, stop_loss: float = None,
                     target_price: float = None, notes: str = ""):
        """添加持仓"""
        if buy_date is None:
            buy_date = datetime.now().strftime('%Y-%m-%d')
        
        # 计算止损和目标价（如果没有指定）
        if stop_loss is None:
            stop_loss = round(buy_price * 0.93, 2)  # 默认7%止损
        if target_price is None:
            target_price = round(buy_price * 1.15, 2)  # 默认15%止盈
        
        position = Position(
            code=code,
            name=name,
            buy_price=buy_price,
            buy_date=buy_date,
            shares=shares,
            stop_loss=stop_loss,
            target_price=target_price,
            notes=notes
        )
        
        # 检查是否已存在，存在则更新
        existing = self.get_position(code)
        if existing:
            # 更新持仓（加仓）
            total_cost = (existing.buy_price * existing.shares + buy_price * shares)
            total_shares = existing.shares + shares
            existing.buy_price = round(total_cost / total_shares, 2)
            existing.shares = total_shares
            existing.stop_loss = min(existing.stop_loss, stop_loss)
            existing.target_price = max(existing.target_price, target_price)
        else:
            self.positions.append(position)
        
        self.save_positions()
        return True
    
    def remove_position(self, code: str) -> bool:
        """移除持仓（卖出）"""
        original_len = len(self.positions)
        self.positions = [p for p in self.positions if p.code != code]
        
        if len(self.positions) < original_len:
            self.save_positions()
            return True
        return False
    
    def get_position(self, code: str) -> Optional[Position]:
        """获取单个持仓"""
        for p in self.positions:
            if p.code == code:
                return p
        return None
    
    def get_all_positions(self) -> List[Position]:
        """获取所有持仓"""
        return self.positions
    
    def update_prices(self, quotes: Dict):
        """更新持仓股价"""
        # 获取ETF代码（15/51/56/58开头）
        etf_codes = [p.code for p in self.positions if p.code.startswith(('15', '51', '56', '58'))]
        
        # 对于ETF，从历史数据库补充价格
        if etf_codes:
            try:
                from src.stock_history_db import StockHistoryDB
                db = StockHistoryDB()
                for code in etf_codes:
                    if code not in quotes or quotes.get(code, {}).get('price', 0) == 0:
                        hist_prices = db.get_prices(code, days=1)
                        if hist_prices:
                            quotes[code] = {'price': hist_prices[-1]}
            except Exception as e:
                logger.warning(f"补充ETF价格失败: {e}")
        
        for position in self.positions:
            # 尝试多种代码格式匹配（处理带前缀的情况，如 sz000659 / sh600000）
            quote = None
            
            # 1. 首先尝试直接匹配
            if position.code in quotes:
                quote = quotes[position.code]
            else:
                # 2. 尝试带前缀匹配（sz/sh）
                prefixes = ['sz', 'sh']
                for prefix in prefixes:
                    prefixed_code = f"{prefix}{position.code}"
                    if prefixed_code in quotes:
                        quote = quotes[prefixed_code]
                        break
                
                # 3. 尝试去掉前缀匹配（如果quote key带前缀而position.code不带）
                for key in quotes.keys():
                    if isinstance(key, str) and (key.endswith(position.code) or key == position.code):
                        quote = quotes[key]
                        break
            
            if quote:
                position.current_price = quote.get('price', position.current_price)
                if position.buy_price > 0:
                    position.current_return = round(
                        (position.current_price - position.buy_price) / position.buy_price * 100, 2
                    )
        
        self.save_positions()
    
    def get_position_summary(self) -> Dict:
        """获取持仓汇总"""
        if not self.positions:
            return {
                'total_positions': 0,
                'total_cost': 0,
                'total_value': 0,
                'total_return': 0,
                'avg_return': 0
            }
        
        total_cost = sum(p.buy_price * p.shares for p in self.positions)
        total_value = sum(p.current_price * p.shares for p in self.positions)
        total_return = (total_value - total_cost) / total_cost * 100 if total_cost > 0 else 0
        
        return {
            'total_positions': len(self.positions),
            'total_cost': round(total_cost, 2),
            'total_value': round(total_value, 2),
            'total_return': round(total_return, 2),
            'avg_return': round(sum(p.current_return for p in self.positions) / len(self.positions), 2)
        }
    
    def get_alert_positions(self) -> List[Dict]:
        """获取需要关注的持仓（触发止损/止盈）"""
        alerts = []
        for p in self.positions:
            if p.current_price <= 0:
                continue
            
            if p.current_price <= p.stop_loss:
                alerts.append({
                    'position': p,
                    'alert_type': 'stop_loss',
                    'message': f'{p.name} 已触发止损价 ¥{p.stop_loss}'
                })
            elif p.current_price >= p.target_price:
                alerts.append({
                    'position': p,
                    'alert_type': 'target',
                    'message': f'{p.name} 已达到目标价 ¥{p.target_price}'
                })
        
        return alerts


# 全局持仓管理器实例
position_manager = PositionManager()


if __name__ == "__main__":
    # 测试
    print("🧪 持仓管理器测试")
    print("="*60)
    
    pm = PositionManager()
    
    # 添加测试持仓
    pm.add_position('000001', '平安银行', 10.5, 1000, stop_loss=9.5, target_price=12.0)
    pm.add_position('600519', '贵州茅台', 1500.0, 100, stop_loss=1400.0, target_price=1700.0)
    
    print(f"✅ 持仓数量: {len(pm.get_all_positions())}")
    
    # 更新价格
    test_quotes = {
        '000001': {'price': 10.8},
        '600519': {'price': 1480.0}
    }
    pm.update_prices(test_quotes)
    
    # 显示汇总
    summary = pm.get_position_summary()
    print(f"\n📊 持仓汇总:")
    print(f"  持仓数: {summary['total_positions']}")
    print(f"  总成本: ¥{summary['total_cost']}")
    print(f"  总市值: ¥{summary['total_value']}")
    print(f"  总收益率: {summary['total_return']:+.2f}%")
    
    # 检查预警
    alerts = pm.get_alert_positions()
    if alerts:
        print(f"\n⚠️ 预警: {len(alerts)} 只股票")
