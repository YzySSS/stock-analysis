# V12策略修复计划 V7 - DeepSeek建议版

## 📋 当前状态 (2026-04-08)

### ⚠️ 严重问题暴露
2年回测结果 (2024-01-02 至 2026-04-07) 揭示了策略的致命缺陷：

| 年份 | 交易 | 胜率 | 累计收益 | 最大回撤 | 评价 |
|------|------|------|----------|----------|------|
| 2024 | 100笔 | 36% | **-43.5%** | 64% | ❌ 熊市完全失效 |
| 2025 | 67笔 | 34% | **-40.7%** | 45% | ❌ 持续亏损 |
| 2026 | 66笔 | 71% | **+673%** | 20% | ⚠️ 可能过拟合 |
| **总计** | 233笔 | 45.5% | +159% | **84.6%** | ❌ 不可接受 |

**DeepSeek诊断**: "本质是选时投机，而非稳定量化选股"

### ✅ 已完成（但效果不佳）
- [x] Z-score标准化
- [x] 基本面因子(PE/PB/ROE)
- [x] 数据清洗+行业中性化
- [x] 止损机制(-5%)
- [x] 滑点/成本模型
- [x] 市场强度仓位
- [x] 动态因子权重
- [x] 冷却期(3天)
- [x] 行业权重约束
- [x] IC/IR优化

---

## 🔧 DeepSeek建议修复路线

### Phase 1: P0 - 紧急风控（必须立即实施）

#### 1.1 组合层面风控系统 ⚠️ **最高优先级**
**目标**: 将最大回撤从84%控制在20%以内

**新增文件**: `src/strategies/v12_risk_control.py`

```python
class RiskControlSystem:
    def __init__(self):
        self.max_drawdown_limit = 0.20
        self.position_limits = {
            'max_per_stock': 0.20,      # 单股最大20%
            'max_industry': 0.30,       # 单行业最大30%
            'max_total': 1.0
        }
    
    def calculate_position(self, market_status, account_drawdown):
        """动态仓位计算"""
        base = min(1.0, max(0.2, market_status / 100))
        
        # 回撤惩罚
        if account_drawdown > 0.10:
            penalty = 1 - (account_drawdown - 0.10) / 0.10
            base *= max(0.1, penalty)
        
        # 波动率调整
        if market_volatility > 0.30:
            base *= 0.5
            
        return base
    
    def should_stop_trading(self, drawdown, consecutive_losses, weekly_loss):
        """停止交易条件"""
        return any([
            drawdown > 0.20,              # 回撤超20%
            consecutive_losses >= 5,       # 连续5次亏损
            weekly_loss < -0.10,           # 周亏损超10%
            market_decline_5d > -0.15      # 5日跌幅超15%
        ])
```

**实施时间**: 2026-04-09
**预期效果**: 最大回撤降至30-40%

#### 1.2 市场环境过滤系统 ⚠️ **最高优先级**
**目标**: 在熊市(2024-2025)中空仓或大幅降低仓位

**新增文件**: `src/strategies/v12_market_filter.py`

```python
class MarketEnvironmentFilter:
    def get_market_status(self, data):
        """
        判断市场环境
        返回: 'bull', 'neutral', 'bear', 'crash'
        """
        indicators = {
            'ma_trend': self._check_ma_trend(),      # MA20 > MA60?
            'market_breadth': self._calculate_breadth(),  # 上涨比例>40%?
            'volatility': self._check_volatility(),  # VIX < 30?
            'liquidity': self._check_liquidity(),    # 成交未萎缩50%?
        }
        
        score = sum([
            1 if indicators['ma_trend'] == 'up' else 0,
            1 if indicators['market_breadth'] > 0.4 else 0,
            1 if indicators['volatility'] == 'low' else 0,
            1 if indicators['liquidity'] == 'high' else 0
        ])
        
        if score >= 4: return 'bull'
        elif score >= 2: return 'neutral'
        else: return 'bear'
    
    def should_stop_strategy(self, market_status, strategy_performance):
        """熊市+策略表现差 → 空仓"""
        if market_status == 'bear' and strategy_performance['1m_return'] < -0.10:
            return True
        return False
```

**实施时间**: 2026-04-09
**预期效果**: 2024-2025年亏损大幅减少

---

### Phase 2: P1 - 因子优化

#### 2.1 解决trend/momentum共线性
**目标**: 合并高度相关因子，降低冗余

**修改文件**: `v12_strategy_v7.py`

```python
def reconstruct_factors(self, factors_df):
    """重构因子"""
    # 合并trend和momentum
    factors_df['trend_momentum'] = (
        0.6 * normalize(factors_df['trend']) +
        0.4 * normalize(factors_df['momentum'])
    )
    factors_df = factors_df.drop(['trend', 'momentum'], axis=1)
    
    # 或PCA降维
    pca = PCA(n_components=5)
    return pca.fit_transform(factors_df)
```

**新因子权重**:
| 因子 | 权重 | 说明 |
|------|------|------|
| trend_momentum | 25% | 合并后复合因子 |
| quality | 20% | ROE质量因子 |
| sentiment | 25% | IC=0.70，提升权重但设上限30% |
| valuation | 15% | PE估值因子 |
| liquidity | 10% | 流动性因子 |
| size | 5% | 小市值因子（降低） |

**实施时间**: 2026-04-10

#### 2.2 2018-2023年回测验证
**目标**: 验证策略在更多市场周期中的表现

**数据需求**:
- 2018-2020年: 熊市+疫情测试
- 2021-2023年: 震荡市测试
- 当前2024-2026: 近期测试

**实施时间**: 2026-04-11 至 2026-04-15

---

### Phase 3: P2 - 精细化优化

#### 3.1 个股仓位精细化管理
**新增文件**: `src/strategies/v12_position_management.py`

```python
def position_sizing_per_stock(self, score, volatility, liquidity):
    """个股仓位计算"""
    base_weight = score / 100
    
    # 波动率调整
    if volatility > 0.40:
        base_weight *= 0.5
    
    # 流动性调整
    if liquidity < 0.01:  # 换手率<1%
        base_weight *= 0.3
    
    return min(base_weight, 0.20)  # 单股最大20%
```

#### 3.2 策略失效监测
**新增文件**: `src/strategies/v12_strategy_validator.py`

```python
def detect_strategy_decay(self, realtime_performance):
    """监测策略衰减"""
    decay_signals = []
    
    if realtime_performance['3m_return'] < -0.10:
        decay_signals.append('近期收益恶化')
    
    for factor in factors:
        if abs(recent_ic[factor]) < 0.5 * abs(historical_ic[factor]):
            decay_signals.append(f'{factor}因子IC衰减')
    
    if realtime_performance['market_regime'] != 'bull':
        decay_signals.append('市场环境变化')
    
    # 2个以上信号 → 策略失效
    return len(decay_signals) >= 2
```

---

## 📅 新执行时间表

| 日期 | 任务 | 输出 | 优先级 |
|------|------|------|--------|
| 04-09 | 风控系统实现 | v12_risk_control.py | P0 |
| 04-09 | 市场环境过滤 | v12_market_filter.py | P0 |
| 04-10 | V7策略版本 | v12_strategy_v7.py | P1 |
| 04-11 | 2年回测验证(带风控) | v12_v7_2year_report | P0 |
| 04-14 | 2018-2020数据获取 | backfill_2018_2020 | P1 |
| 04-15 | 2018-2023回测验证 | v12_v7_full_report | P1 |
| 04-16 | 策略对比分析 | V5/V6/V7对比 | P2 |

---

## 🎯 新关键指标目标

| 指标 | V6(2年) | V7目标 | 说明 |
|------|---------|--------|------|
| 胜率 | 45.5% | >50% | 至少不亏损 |
| 最大回撤 | **84.6%** | **<20%** | 最关键指标 |
| 2024年收益 | -43.5% | >-10% | 熊市控制 |
| 2025年收益 | -40.7% | >-10% | 震荡市控制 |
| 2026年收益 | +673% | >50% | 牛市收益 |
| 夏普比率 | 2.32 | >1.5 | 风险调整后收益 |

---

## 📁 新增文件清单

- `src/strategies/v12_risk_control.py` - 风控系统
- `src/strategies/v12_market_filter.py` - 市场环境过滤
- `src/strategies/v12_position_management.py` - 仓位管理
- `src/strategies/v12_strategy_validator.py` - 策略验证
- `src/strategies/v12_strategy_v7.py` - V7策略主文件
- `v12_backtest_v7_full.py` - V7回测引擎

---

## ⚠️ DeepSeek实盘建议

> **"当前状态下绝对不适合实盘"**

**必须满足以下条件才能考虑实盘**:
1. ✅ 最大回撤控制在20%以内
2. ✅ 2018-2023年回测验证通过
3. ✅ 熊市(2024-2025)亏损控制在10%以内
4. ✅ 策略失效监测系统运行正常
5. ✅ 至少3个月模拟盘验证

**建议初始规模**: 1-5万试跑
**建议杠杆**: 无杠杆
**止损线**: 账户回撤15%清仓观察

---

## 📚 参考文档

- `docs/V12_V6_DEEPSEEK_ANALYSIS_REQUEST.md` - 问题分析报告
- `docs/V12_V6_DEEPSEEK_ADVICE.md` - DeepSeek完整建议
- `v12_v6_full_opt_summary.json` - V6回测结果
- `v12_v6_full_opt_trades.csv` - V6交易明细
