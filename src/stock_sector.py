#!/usr/bin/env python3
"""
股票行业分类获取模块
使用新浪财经接口获取股票所属行业
"""

import requests
import json
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class StockSectorClassifier:
    """股票行业分类器"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._cache: Dict[str, str] = {}

    def get_sector(self, code: str) -> str:
        """
        获取股票所属行业

        Args:
            code: 股票代码，如 '000001'

        Returns:
            行业名称，如 '银行'
        """
        if code in self._cache:
            return self._cache[code]

        try:
            sector = self._fetch_from_sina(code)
            if sector:
                self._cache[code] = sector
                return sector
        except Exception as e:
            logger.debug(f"获取行业分类失败 {code}: {e}")

        # 根据代码前缀推断行业
        sector = self._infer_sector(code)
        self._cache[code] = sector
        return sector

    def _fetch_from_sina(self, code: str) -> Optional[str]:
        """从新浪财经获取行业分类"""
        # 转换代码格式
        if code.startswith('6'):
            sina_code = f"sh{code}"
        elif code.startswith('0') or code.startswith('3'):
            sina_code = f"sz{code}"
        else:
            return None

        try:
            url = f"https://vip.stock.finance.sina.com.cn/corp/view/vCI_CorpInfo.php?stockid={code}"
            response = self.session.get(url, timeout=5)
            response.encoding = 'gb2312'

            # 简单解析HTML获取行业
            html = response.text
            if '所属行业' in html:
                # 提取行业信息
                import re
                match = re.search(r'所属行业.*?>([^<]+)<', html)
                if match:
                    return match.group(1).strip()
        except Exception as e:
            logger.debug(f"新浪接口获取失败 {code}: {e}")

        return None

    def _infer_sector(self, code: str) -> str:
        """根据代码前缀和常见股票推断行业"""

        # 创业板
        if code.startswith('300') or code.startswith('301'):
            return '创业板'

        # 科创板
        if code.startswith('688'):
            return '科创板'

        # 北交所
        if code.startswith('8') or code.startswith('4'):
            return '北交所'

        # 根据代码范围推断（粗略）
        code_int = int(code) if code.isdigit() else 0

        # 沪市主板
        if code.startswith('6'):
            if code_int >= 600000 and code_int < 601000:
                return '银行/金融'
            elif code_int >= 601000 and code_int < 602000:
                return '医药/消费'
            elif code_int >= 603000 and code_int < 604000:
                return '制造业'
            else:
                return '沪市主板'

        # 深市主板
        if code.startswith('000') or code.startswith('001'):
            if code_int >= 1 and code_int < 100:
                return '房地产/基建'
            elif code_int >= 600 and code_int < 700:
                return '新能源'
            elif code_int >= 800 and code_int < 900:
                return '电子/科技'
            else:
                return '深市主板'

        # 中小板（已合并到主板）
        if code.startswith('002'):
            if code_int >= 2000 and code_int < 2100:
                return '化工/材料'
            elif code_int >= 2300 and code_int < 2400:
                return '机械设备'
            elif code_int >= 2700 and code_int < 2800:
                return '电子元器件'
            elif code_int >= 2900 and code_int < 3000:
                return '医药生物'
            else:
                return '中小板'

        return '其他'

    def batch_get_sectors(self, codes: list) -> Dict[str, str]:
        """批量获取行业分类"""
        result = {}
        for code in codes:
            result[code] = self.get_sector(code)
        return result


# 全局分类器实例
sector_classifier = StockSectorClassifier()


def get_stock_sector(code: str) -> str:
    """获取股票所属行业的便捷函数"""
    return sector_classifier.get_sector(code)


if __name__ == "__main__":
    print("🧪 股票行业分类测试")
    print("=" * 60)

    test_codes = ['000001', '000659', '000700', '000722', '002594', '300750', '600519', '601318']

    for code in test_codes:
        sector = get_stock_sector(code)
        print(f"{code}: {sector}")

    print("=" * 60)
    print("✅ 测试完成")
