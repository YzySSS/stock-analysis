#!/usr/bin/env python3
"""
同花顺持仓图片识别模块
========================
识别同花顺APP持仓截图，提取股票持仓信息
"""

import re
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class ParsedPosition:
    """解析后的持仓记录"""
    code: str              # 股票代码
    name: str              # 股票名称
    quantity: int          # 持仓数量
    cost_price: float      # 成本价
    current_price: float   # 当前价
    market_value: float    # 市值
    profit: float          # 盈亏金额
    profit_pct: float      # 盈亏比例


class TongHuaShunParser:
    """同花顺持仓图片文本解析器"""
    
    # 常见ETF代码映射（同花顺可能显示简称）
    ETF_MAP = {
        '黄金9999': '159937',
        '黄金ETF': '159937',
        '双创AI': '159142',
        '双创AIETF': '159142',
        '银行ETF': '159887',
        '银行ETF天弘': '159887',
        '锂电池ETF': '561160',
        '锂电池ETF基金': '561160',
        '电力ETF': '159611',
        '电力指数ETF': '159611',
        '光伏ETF': '159857',
        '芯片ETF': '159995',
        '半导体ETF': '512480',
        '酒ETF': '512690',
        '医药ETF': '512010',
        '券商ETF': '512000',
        '军工ETF': '512660',
        '新能源车ETF': '515030',
        '纳指ETF': '513100',
        '标普500ETF': '513500',
        '恒生科技ETF': '513130',
        '中概互联ETF': '513050',
    }
    
    def __init__(self):
        self.parsed_positions: List[ParsedPosition] = []
    
    def parse_from_text(self, text: str) -> List[ParsedPosition]:
        """
        从OCR文本中解析持仓信息
        
        同花顺持仓页典型格式：
        股票名称
        股票代码
        持仓/可用
        成本/现价
        盈亏/盈亏比例
        市值
        """
        positions = []
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # 尝试多种解析模式
        positions.extend(self._parse_pattern_v1(lines))
        
        if not positions:
            positions.extend(self._parse_pattern_v2(lines))
        
        if not positions:
            positions.extend(self._parse_pattern_v3(lines))
        
        self.parsed_positions = positions
        return positions
    
    def _parse_pattern_v1(self, lines: List[str]) -> List[ParsedPosition]:
        """模式1: 标准格式解析"""
        positions = []
        i = 0
        
        while i < len(lines) - 5:
            # 寻找股票代码（6位数字或ETF名称）
            code_match = None
            name = None
            
            for j in range(i, min(i + 3, len(lines))):
                # 匹配6位股票代码
                code_pattern = re.search(r'\b(\d{6})\b', lines[j])
                if code_pattern:
                    code_match = code_pattern.group(1)
                    name = lines[j-1] if j > i else lines[j]
                    # 清理名称
                    name = re.sub(r'\d+', '', name).strip()
                    i = j
                    break
            
            if not code_match and i < len(lines):
                # 尝试匹配ETF名称
                for etf_name, etf_code in self.ETF_MAP.items():
                    if etf_name in lines[i]:
                        code_match = etf_code
                        name = etf_name
                        break
            
            if code_match and i + 4 < len(lines):
                try:
                    # 解析后续行
                    quantity = self._extract_number(lines[i + 1])
                    cost_price = self._extract_price(lines[i + 2])
                    current_price = self._extract_price(lines[i + 3])
                    
                    # 盈亏信息
                    profit_info = lines[i + 4] if i + 4 < len(lines) else ""
                    profit, profit_pct = self._extract_profit(profit_info)
                    
                    # 市值
                    market_value = quantity * current_price if quantity and current_price else 0
                    
                    if quantity and cost_price:  # 至少要有数量和成本
                        positions.append(ParsedPosition(
                            code=code_match,
                            name=name or code_match,
                            quantity=int(quantity),
                            cost_price=float(cost_price),
                            current_price=float(current_price) if current_price else float(cost_price),
                            market_value=float(market_value),
                            profit=float(profit) if profit else 0,
                            profit_pct=float(profit_pct) if profit_pct else 0
                        ))
                        i += 5
                        continue
                except Exception:
                    pass
            
            i += 1
        
        return positions
    
    def _parse_pattern_v2(self, lines: List[str]) -> List[ParsedPosition]:
        """模式2: 表格格式解析"""
        positions = []
        
        for line in lines:
            # 尝试匹配: 名称 代码 数量 成本 现价 盈亏
            # 例如: 比亚迪 002594 300 94.96 104.62 +2889.00 +10.17%
            pattern = re.search(
                r'([\u4e00-\u9fa5]+).*?(\d{6}).*?(\d+).*?(\d+\.?\d*).*?(\d+\.?\d*).*?([\+\-]?\d+\.?\d*)%',
                line
            )
            
            if pattern:
                name, code, qty, cost, price, profit_pct = pattern.groups()
                positions.append(ParsedPosition(
                    code=code,
                    name=name,
                    quantity=int(qty),
                    cost_price=float(cost),
                    current_price=float(price),
                    market_value=int(qty) * float(price),
                    profit=0,
                    profit_pct=float(profit_pct)
                ))
        
        return positions
    
    def _parse_pattern_v3(self, lines: List[str]) -> List[ParsedPosition]:
        """模式3: 松散格式解析（容错模式）"""
        positions = []
        
        # 收集所有数字和代码
        codes = []
        numbers = []
        
        for line in lines:
            # 找股票代码
            code_matches = re.findall(r'\b(\d{6})\b', line)
            codes.extend(code_matches)
            
            # 找数字（价格、数量）
            num_matches = re.findall(r'\b(\d+\.?\d*)\b', line)
            numbers.extend([float(n) for n in num_matches if float(n) > 0])
        
        # 尝试配对
        for i, code in enumerate(codes):
            if i * 3 + 2 < len(numbers):
                try:
                    positions.append(ParsedPosition(
                        code=code,
                        name=code,
                        quantity=int(numbers[i * 3]),
                        cost_price=numbers[i * 3 + 1],
                        current_price=numbers[i * 3 + 2],
                        market_value=int(numbers[i * 3]) * numbers[i * 3 + 2],
                        profit=0,
                        profit_pct=0
                    ))
                except Exception:
                    pass
        
        return positions
    
    def _extract_number(self, text: str) -> Optional[int]:
        """提取整数"""
        match = re.search(r'\b(\d+)\b', text.replace(',', ''))
        return int(match.group(1)) if match else None
    
    def _extract_price(self, text: str) -> Optional[float]:
        """提取价格"""
        # 匹配形如 "成本:10.5" 或 "10.50" 或 "¥10.5"
        match = re.search(r'[\:：]?\s*¥?\s*(\d+\.\d{1,3})', text)
        if match:
            return float(match.group(1))
        # 匹配纯数字
        match = re.search(r'\b(\d+\.\d{1,3})\b', text)
        return float(match.group(1)) if match else None
    
    def _extract_profit(self, text: str) -> tuple:
        """提取盈亏信息"""
        # 提取百分比
        pct_match = re.search(r'([\+\-]?\d+\.?\d*)\s*%', text)
        profit_pct = float(pct_match.group(1)) if pct_match else 0
        
        # 提取金额
        amount_match = re.search(r'([\+\-]?\d+,?\d*\.?\d*)', text.replace(',', ''))
        profit = float(amount_match.group(1)) if amount_match else 0
        
        return profit, profit_pct
    
    def get_summary(self) -> Dict:
        """获取解析汇总"""
        if not self.parsed_positions:
            return {
                'count': 0,
                'total_cost': 0,
                'total_value': 0,
                'total_profit': 0,
                'profit_pct': 0
            }
        
        total_cost = sum(p.quantity * p.cost_price for p in self.parsed_positions)
        total_value = sum(p.market_value for p in self.parsed_positions)
        total_profit = sum(p.profit for p in self.parsed_positions)
        
        return {
            'count': len(self.parsed_positions),
            'total_cost': round(total_cost, 2),
            'total_value': round(total_value, 2),
            'total_profit': round(total_profit, 2),
            'profit_pct': round((total_value - total_cost) / total_cost * 100, 2) if total_cost > 0 else 0
        }
    
    def to_position_manager_format(self) -> List[Dict]:
        """转换为 position_manager 格式"""
        return [
            {
                'code': p.code,
                'name': p.name,
                'buy_price': p.cost_price,
                'shares': p.quantity,
                'current_price': p.current_price,
                'current_return': p.profit_pct,
                'buy_date': '',  # 图片中通常没有买入日期
                'stop_loss': round(p.cost_price * 0.93, 2),  # 默认7%止损
                'target_price': round(p.cost_price * 1.15, 2)  # 默认15%止盈
            }
            for p in self.parsed_positions
        ]


def parse_ths_image(image_path: str = None, ocr_text: str = None) -> Dict:
    """
    解析同花顺持仓图片
    
    Args:
        image_path: 图片路径（如果使用OCR工具）
        ocr_text: 直接传入OCR识别后的文本
    
    Returns:
        解析结果
    """
    parser = TongHuaShunParser()
    
    if ocr_text:
        positions = parser.parse_from_text(ocr_text)
    else:
        # 这里可以集成OCR工具，如 pytesseract 或百度OCR
        return {
            'success': False,
            'error': '请提供OCR识别后的文本，或传入图片路径（需要配置OCR）'
        }
    
    summary = parser.get_summary()
    pm_format = parser.to_position_manager_format()
    
    return {
        'success': len(positions) > 0,
        'positions': positions,
        'summary': summary,
        'position_manager_format': pm_format,
        'parsed_count': len(positions)
    }


if __name__ == "__main__":
    # 测试示例文本
    test_text = """
    持仓
    黄金9999
    159937
    5500/5500
    成本:10.877
    现价:10.635
    -1331.00
    -2.23%
    市值:58492.50
    
    双创AI
    159142
    44800/44800
    成本:1.158
    现价:1.061
    -4343.36
    -8.38%
    市值:47532.80
    
    比亚迪
    002594
    300/300
    成本:94.957
    现价:104.620
    +2889.00
    +10.17%
    市值:31386.00
    """
    
    print("🧪 同花顺持仓解析测试")
    print("=" * 60)
    
    result = parse_ths_image(ocr_text=test_text)
    
    print(f"✅ 解析成功: {result['parsed_count']} 只股票")
    print(f"\n📊 汇总:")
    print(f"  总成本: ¥{result['summary']['total_cost']:,.2f}")
    print(f"  总市值: ¥{result['summary']['total_value']:,.2f}")
    print(f"  总盈亏: ¥{result['summary']['total_profit']:,.2f} ({result['summary']['profit_pct']:+.2f}%)")
    
    print(f"\n📋 持仓明细:")
    for p in result['positions']:
        emoji = '🟢' if p.profit_pct >= 0 else '🔴'
        print(f"  {emoji} {p.name} ({p.code}): {p.quantity}股 | 成本¥{p.cost_price} → 现价¥{p.current_price} | {p.profit_pct:+.2f}%")
