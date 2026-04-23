#!/usr/bin/env python3
"""
Phase 3 测试脚本 - 舆情因子集成验证
=====================================
测试内容：
1. 舆情因子计算器单元测试
2. V10评分器集成测试
3. 批量计算性能测试
4. 边界条件测试（API异常、无新闻等）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_sentiment_calculator():
    """测试1: 舆情因子计算器"""
    print("\n" + "="*60)
    print("测试1: 舆情因子计算器")
    print("="*60)
    
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    print(f"✅ 舆情计算器初始化成功")
    
    # 测试单只股票
    test_stocks = [
        ("000001", "平安银行"),
        ("000002", "万科A"),
        ("600519", "贵州茅台"),
        ("300750", "宁德时代"),
    ]
    
    print(f"\n单只股票测试:")
    for code, name in test_stocks:
        try:
            result = calc.calculate_sentiment_factor(code, name)
            print(f"  {name}({code}):")
            print(f"    舆情得分: {result['score']}")
            print(f"    原始情感: {result['raw_sentiment']:.2f}")
            print(f"    新闻数量: {result['news_count']}")
            print(f"    可信度: {result['credibility_avg']:.2f}")
            print(f"    详情: {result['details']}")
        except Exception as e:
            print(f"  {name}({code}): ❌ 失败 - {e}")
    
    print(f"\n✅ 舆情计算器测试完成")

def test_sentiment_batch():
    """测试2: 批量计算性能"""
    print("\n" + "="*60)
    print("测试2: 批量计算性能")
    print("="*60)
    
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    
    # 小规模批量测试
    test_stocks = [
        ("000001", "平安银行"),
        ("000002", "万科A"),
        ("600519", "贵州茅台"),
        ("300750", "宁德时代"),
        ("601318", "中国平安"),
        ("000858", "五粮液"),
        ("002594", "比亚迪"),
        ("601888", "中国中免"),
        ("603288", "海天味业"),
        ("600276", "恒瑞医药"),
    ]
    
    print(f"批量计算 {len(test_stocks)} 只股票...")
    start_time = time.time()
    
    results = calc.batch_calculate(test_stocks, progress_interval=5)
    
    elapsed = time.time() - start_time
    print(f"✅ 批量计算完成，耗时: {elapsed:.2f}秒")
    print(f"   平均: {elapsed/len(test_stocks):.2f}秒/只")
    
    # 统计结果
    scores = [r['score'] for r in results.values()]
    print(f"\n得分统计:")
    print(f"  最高: {max(scores)}")
    print(f"  最低: {min(scores)}")
    print(f"  平均: {sum(scores)/len(scores):.2f}")

def test_v10_integration():
    """测试3: V10评分器集成"""
    print("\n" + "="*60)
    print("测试3: V10评分器集成")
    print("="*60)
    
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    
    # 模拟评分器调用
    test_data = {
        'code': '000001',
        'name': '平安银行',
        'price': 10.5,
        'change_pct': 2.5,
        'volume': 1000000,
        'turnover': 10000000,
        'sector': '银行'
    }
    
    print(f"测试股票: {test_data['name']}({test_data['code']})")
    
    # 获取舆情因子
    sentiment_factor = calc.calculate_sentiment_factor(
        test_data['code'], test_data['name']
    )
    
    print(f"舆情因子结果:")
    print(f"  得分: {sentiment_factor['score']}")
    print(f"  映射到7分制: {(sentiment_factor['score'] + 10) / 20 * 7:.2f}")
    
    print(f"\n✅ V10集成测试完成")

def test_fallback_mechanism():
    """测试4: 降级机制"""
    print("\n" + "="*60)
    print("测试4: 降级机制")
    print("="*60)
    
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    
    # 测试不存在/无效的股票代码
    invalid_codes = [
        ("999999", "测试股票1"),
        ("000000", "测试股票2"),
    ]
    
    print(f"无效股票代码测试:")
    for code, name in invalid_codes:
        result = calc.calculate_sentiment_factor(code, name)
        print(f"  {code}: 得分={result['score']}, 详情={result['details']}")
    
    # 测试缓存命中
    print(f"\n缓存测试:")
    code, name = "000001", "平安银行"
    
    # 第一次（可能从API获取）
    start = time.time()
    result1 = calc.calculate_sentiment_factor(code, name)
    time1 = time.time() - start
    
    # 第二次（应该命中缓存）
    start = time.time()
    result2 = calc.calculate_sentiment_factor(code, name)
    time2 = time.time() - start
    
    print(f"  第一次: {time1:.3f}秒, 得分={result1['score']}")
    print(f"  第二次: {time2:.3f}秒, 得分={result2['score']}")
    print(f"  缓存加速: {time1/time2:.1f}倍" if time2 > 0 else "  缓存命中")
    
    print(f"\n✅ 降级机制测试完成")

def test_weight_calculation():
    """测试5: 权重计算验证"""
    print("\n" + "="*60)
    print("测试5: 权重计算验证")
    print("="*60)
    
    # 模拟各因子原始得分
    tech_score = 15          # 20分制
    sentiment_score = 8      # 10分制
    sector_score = 30        # 35分制
    money_flow_score = 15    # 20分制
    risk_score = 12          # 15分制
    consensus_score = 4      # 5分制
    news_sentiment_score = 3 # 5分制
    
    # 新权重计算
    tech_adjusted = tech_score * 1.0        # 20分
    sentiment_adjusted = sentiment_score * 0.3  # 3分
    sector_adjusted = sector_score * 0.9429  # 33分
    money_flow_adjusted = money_flow_score * 0.95  # 19分
    consensus_adjusted = consensus_score * 0.6  # 3分
    news_sentiment_adjusted = news_sentiment_score * 1.4  # 7分
    risk_adjusted = risk_score * 1.0  # 15分（正常市场）
    
    total = (tech_adjusted + sentiment_adjusted + sector_adjusted + 
            money_flow_adjusted + risk_adjusted + consensus_adjusted + 
            news_sentiment_adjusted)
    
    print(f"原始得分 -> 调整后得分:")
    print(f"  技术趋势:     {tech_score:>5} -> {tech_adjusted:>6.2f} (权重20%)")
    print(f"  情绪(技术):   {sentiment_score:>5} -> {sentiment_adjusted:>6.2f} (权重3%)")
    print(f"  板块轮动:     {sector_score:>5} -> {sector_adjusted:>6.2f} (权重33%)")
    print(f"  资金流向:     {money_flow_score:>5} -> {money_flow_adjusted:>6.2f} (权重19%)")
    print(f"  风险控制:     {risk_score:>5} -> {risk_adjusted:>6.2f} (权重15%)")
    print(f"  一致预期:     {consensus_score:>5} -> {consensus_adjusted:>6.2f} (权重3%)")
    print(f"  舆情因子:     {news_sentiment_score:>5} -> {news_sentiment_adjusted:>6.2f} (权重7%)")
    print(f"  ─────────────────────────────────")
    print(f"  总分:         {total:>6.2f}")
    
    # 验证权重分配
    weights = [20, 3, 33, 19, 15, 3, 7]
    print(f"\n权重验证: {sum(weights)}%")
    
    print(f"\n✅ 权重计算验证完成")

def test_caching():
    """测试6: 缓存统计"""
    print("\n" + "="*60)
    print("测试6: 缓存统计")
    print("="*60)
    
    from sentiment_factor import get_sentiment_calculator
    
    calc = get_sentiment_calculator()
    
    # 获取统计
    stats = calc.get_sentiment_stats()
    
    if stats:
        print(f"缓存统计 ({stats.get('date', '今天')}):")
        print(f"  总记录: {stats.get('total_cached', 0)}")
        print(f"  正面舆情: {stats.get('positive', 0)}")
        print(f"  负面舆情: {stats.get('negative', 0)}")
        print(f"  中性: {stats.get('neutral', 0)}")
    else:
        print(f"暂无缓存数据")
    
    print(f"\n✅ 缓存统计完成")

def main():
    """主测试函数"""
    print("="*60)
    print("Phase 3 测试验证 - 舆情因子集成")
    print("="*60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        test_sentiment_calculator()
        test_sentiment_batch()
        test_v10_integration()
        test_fallback_mechanism()
        test_weight_calculation()
        test_caching()
        
        print("\n" + "="*60)
        print("✅ 所有测试完成！")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
