# DeepSeek 分析报告：V12策略 V8 回测评估方法

## 一、核心问题诊断

### ❌ 当前累计收益计算方式的错误

你当前使用的**复利连乘**方式：
```python
cum = 1.0
for r in net_returns:
    cum *= (1 + r)
total_return = cum - 1
```

**问题分析**：
1. **假设满仓复利**：这种方式假设每笔交易的收益都会复利滚动，但实际上你的策略是每日选股，每笔交易独立
2. **忽略资金占用**：1日持有和5日持有占用资金的时间不同，不能直接连乘
3. **时间维度混乱**：多笔交易可能同时进行，复利连乘会重复计算

---

## 二、正确的累计收益计算方式

### ✅ 方式1：算术平均（推荐用于选股评估）

对于选股策略评估，**算术平均收益率**比几何平均更合适：

```python
# 算术平均
total_return = mean(net_returns) * 交易次数 / 总交易日 * 252

# 或简化为
total_return = mean(net_returns) * 年化换手次数
```

**适用场景**：评估选股信号质量，不模拟实际资金曲线

### ✅ 方式2：构建实际资金曲线（推荐用于实盘模拟）

如果需要模拟实盘，应该构建真实的资金曲线：

```python
# 假设初始资金100万，每次交易分配资金
capital = 1000000  # 初始资金
positions = []  # 当前持仓
capital_curve = [capital]

for date in trading_days:
    # 1. 检查持仓到期，平仓
    for pos in positions[:]:
        if pos['exit_date'] == date:
            capital += pos['shares'] * pos['exit_price'] * (1 - cost)
            positions.remove(pos)
    
    # 2. 选股并开仓
    available_capital = capital / max_positions  # 每只股分配的资金
    picks = select_stocks(date)
    
    for pick in picks:
        if len(positions) >= max_positions:
            break
        shares = available_capital / pick['entry_price']
        positions.append({
            'code': pick['code'],
            'shares': shares,
            'entry_price': pick['entry_price'],
            'exit_date': get_exit_date(date, hold_days)
        })
        capital -= shares * pick['entry_price']
    
    # 3. 记录当日市值
    total_value = capital + sum(pos['shares'] * current_price(pos['code']) for pos in positions)
    capital_curve.append(total_value)

# 计算收益率
total_return = (capital_curve[-1] - capital_curve[0]) / capital_curve[0]
```

### ✅ 方式3：简化资金曲线（折中方案）

```python
# 假设固定资金，每笔交易独立，计算加权平均
# 考虑资金占用时间

weighted_returns = []
for trade in trades:
    # 权重 = 资金占用时间 / 总时间
    weight = hold_days / total_days
    weighted_returns.append(trade['net_return'] * weight)

total_return = sum(weighted_returns) * (total_days / hold_days)
```

---

## 三、选股策略的正确评估指标

### 1. 基础指标

| 指标 | 计算方式 | 用途 |
|------|----------|------|
| **胜率** | 盈利交易数 / 总交易数 | 评估选股方向正确性 |
| **平均收益** | 所有交易收益率的算术平均 | 评估选股质量 |
| **盈亏比** | 平均盈利 / 平均亏损 | 评估风险收益比 |
| **最大回撤** | 资金曲线的最大跌幅 | 评估风险 |

### 2. 进阶指标

| 指标 | 计算方式 | 用途 |
|------|----------|------|
| **信息比率 (IR)** | 超额收益 / 跟踪误差 | 评估选股Alpha质量 |
| **胜率稳定性** | 按月/季度统计胜率波动 | 评估策略稳健性 |
| **因子IC** | 因子得分与未来收益的相关性 | 评估因子有效性 |

### 3. 基准对比（最重要）

选股策略评估必须有**基准对比**：
```python
# 计算超额收益
excess_return = strategy_return - benchmark_return

# 基准可选：
# - 沪深300指数
# - 中证500指数
# - 等权全市场平均
```

---

## 四、你的结果解读

### 当前结果分析

| 持有期 | 交易次数 | 胜率 | 平均收益 | 复利累计 | **问题** |
|--------|----------|------|----------|----------|----------|
| **1日** | 250 | 47.2% | +0.01% | -6.45% | 胜率<50%，平均收益接近0 |
| **3日** | 240 | 52.9% | +0.50% | +147.86% | 复利计算放大效应 |
| **5日** | 230 | 54.8% | +1.00% | +570.35% | 复利计算严重失真 |

### 问题诊断

**1日收益为负**：
- 胜率47.2% < 50%，说明选股方向略差
- 平均收益0.01%，扣除成本后实际亏损
- **结论**：超短线（1日）选股在这个周期失效

**3日/5日收益过高**：
- 复利连乘导致指数级放大
- 3日复利：(1.005)^240 ≈ 3.3，但你的结果是147%
- 5日复利：(1.01)^230 ≈ 9.9，但你的结果是570%

**说明**：复利计算有误，或交易时间重叠导致重复计算

---

## 五、推荐的正确评估方式

### 步骤1：使用算术平均重新计算

```python
# 正确的累计收益计算（算术平均）
avg_return = np.mean(net_returns)  # 平均单笔收益
trades_per_year = len(trades) / (total_days / 252)  # 年化交易次数
annual_return = avg_return * trades_per_year  # 年化收益
```

### 步骤2：计算基准对比

```python
# 同期市场基准收益
benchmark_return = 沪深300同期收益

# 超额收益
excess_return = annual_return - benchmark_return
```

### 步骤3：按市场环境分层分析

```python
# 分牛熊市场统计
bull_returns = [t for t in trades if t['market_status'] == 'bull']
bear_returns = [t for t in trades if t['market_status'] == 'bear']

print(f"牛市胜率: {win_rate(bull_returns)}, 平均收益: {mean(bull_returns)}")
print(f"熊市胜率: {win_rate(bear_returns)}, 平均收益: {mean(bear_returns)}")
```

---

## 六、修正建议

### 立即修改（P0）

1. **停止使用复利连乘**，改用算术平均
2. **添加基准对比**（沪深300/中证500）
3. **分层分析**：按市场环境（牛/熊/震荡）分别统计

### 后续优化（P1）

1. **构建真实资金曲线**：考虑资金占用和仓位管理
2. **添加超额收益指标**：信息比率、Alpha、Beta
3. **因子IC分析**：验证PE/ROE因子的预测能力

---

## 七、结论

你的策略评估方法存在**根本性错误**：

1. ❌ **复利连乘不适用于选股策略评估**
2. ❌ **缺少基准对比**
3. ❌ **未考虑资金占用时间**

**正确的评估方式**：
- ✅ 使用算术平均计算平均收益
- ✅ 添加沪深300/中证500基准对比
- ✅ 构建真实的资金曲线（如果需要模拟实盘）

**你的策略现状**：
- 1日持有：策略失效（胜率<50%）
- 3日/5日持有：复利计算失真，实际收益需重新计算
- 建议先用算术平均重新计算，再判断策略有效性
