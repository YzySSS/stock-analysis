#!/usr/bin/env python3
"""
获取全市场ETF列表并补充历史数据
"""

import sys
sys.path.insert(0, 'src')

import baostock as bs
import logging
from stock_history_db import StockHistoryDB
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_all_etfs() -> List[str]:
    """从Baostock获取所有ETF代码"""
    etf_codes = []
    
    # ETF代码段：
    # 深圳：159xxx (创业板ETF等), 160xxx (LOF)
    # 上海：510xxx (50ETF等), 511xxx (货币基金), 512xxx (行业ETF), 513xxx (跨境ETF), 515xxx (主题ETF), 518xxx (黄金ETF)
    
    prefixes = [
        'sz.159', 'sz.160',  # 深圳
        'sh.510', 'sh.511', 'sh.512', 'sh.513', 'sh.515', 'sh.518', 'sh.560', 'sh.561', 'sh.562', 'sh.563', 'sh.564', 'sh.565', 'sh.566', 'sh.567', 'sh.568', 'sh.569', 'sh.580', 'sh.588', 'sh.589'  # 上海
    ]
    
    for prefix in prefixes:
        try:
            rs = bs.query_all_stock(day='2026-03-25')  # 使用最近交易日
            if rs.error_code == '0':
                while (rs.error_code == '0') & rs.next():
                    code = rs.get_row_data()[0]
                    if code.startswith(prefix):
                        # 去掉前缀，保留6位代码
                        etf_codes.append(code.split('.')[1])
        except Exception as e:
            logger.warning(f"获取 {prefix} 失败: {e}")
    
    return list(set(etf_codes))  # 去重

def fetch_etf_history(code: str, days: int = 60):
    """获取ETF历史数据"""
    try:
        # 转换代码格式
        if code.startswith(('00', '30', '15', '16', '18')):
            bs_code = f"sz.{code}"
        else:
            bs_code = f"sh.{code}"
        
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            bs_code,
            'date,close,volume',
            start_date=start_date,
            end_date=end_date,
            frequency='d',
            adjustflag='2'
        )
        
        if rs.error_code != '0':
            return None
        
        prices_data = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[1] and float(row[1]) > 0:
                prices_data.append({
                    'date': row[0],
                    'close': float(row[1]),
                    'volume': int(row[2]) if row[2] else 0
                })
        
        return prices_data[-days:] if len(prices_data) > days else prices_data
        
    except Exception as e:
        logger.error(f"获取 {code} 失败: {e}")
        return None

def main():
    db = StockHistoryDB()
    
    print("="*60)
    print("补充全市场ETF历史数据")
    print("="*60)
    
    # 登录Baostock
    logger.info("登录Baostock...")
    bs.login()
    
    # 获取所有ETF代码（使用预设的常见ETF列表）
    # 实际上全市场有几百只ETF，但只补充常用的
    common_etfs = [
        # 宽基ETF
        '510050', '510300', '510500', '510880', '511380',  # 上证50/300/500/红利/转债
        '159915', '159922', '159919', '159901', '159952',  # 创业板/500/300/100/创业板50
        '588000', '588080', '588090',  # 科创50
        # 行业ETF - 金融
        '512000', '512800', '512880', '512900',  # 券商/银行/证券/消费
        '159987', '159843',  # 金融科技/证券
        # 行业ETF - 科技
        '515050', '515000', '515030', '515790',  # 5G/科技/新能源车/光伏
        '159995', '159997', '159807', '159806',  # 芯片/电子/科技/传媒
        # 行业ETF - 医药
        '512010', '512170', '512290', '159992',  # 医药/医疗/生物医药/创新药
        # 行业ETF - 消费
        '159928', '159736', '159996', '159865',  # 消费/食品/家电/养殖
        '512690', '515170',  # 酒/食品饮料
        # 行业ETF - 周期
        '510880', '515220', '159881', '516970',  # 红利/煤炭/半导体材料/电力
        # 行业ETF - 地产基建
        '512200', '515060',  # 地产/基建
        # 商品ETF
        '518880', '159934', '159937',  # 黄金
        '159985', '159981',  # 豆粕/能源化工
        # 跨境ETF
        '513050', '513100', '513300', '159920',  # 中概/纳指/标普/恒生
        # 其他
        '511010', '511020', '511220',  # 国债/地债/城投债
        '159611', '159887', '159142',  # 电力/银行/双创AI
    ]
    
    # 去重
    common_etfs = list(set(common_etfs))
    
    logger.info(f"计划补充 {len(common_etfs)} 只ETF数据")
    
    success_count = 0
    fail_count = 0
    
    for i, code in enumerate(common_etfs, 1):
        # 检查是否已有数据
        existing = db.get_prices(code, days=1)
        if existing:
            logger.info(f"[{i}/{len(common_etfs)}] ✅ {code}: 已有数据")
            success_count += 1
            continue
        
        logger.info(f"[{i}/{len(common_etfs)}] 📊 获取 {code}...")
        try:
            prices_data = fetch_etf_history(code, days=60)
            
            if prices_data and len(prices_data) > 0:
                db.save_prices(code, prices_data)
                logger.info(f"  ✅ 成功保存 {len(prices_data)} 天")
                success_count += 1
            else:
                logger.warning(f"  ⚠️ 未获取到数据")
                fail_count += 1
                
        except Exception as e:
            logger.error(f"  ❌ 失败: {e}")
            fail_count += 1
    
    bs.logout()
    
    print("\n" + "="*60)
    print(f"完成: 成功 {success_count}, 失败 {fail_count}")
    print("="*60)
    
    # 更新统计
    stats = db.get_stats()
    print(f"\n数据库统计:")
    print(f"  总股票数: {stats.get('stock_count', 'N/A')}")
    print(f"  总记录数: {stats.get('record_count', 'N/A')}")

if __name__ == "__main__":
    main()
