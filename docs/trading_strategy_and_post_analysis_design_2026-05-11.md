# 交易策略与交易后分析模块设计（V2.2 草案）

> 日期：2026-05-11  
> 阶段：设计文档 + 最小 schema 草案  
> 背景：V2.1 已完成选股策略、基础回测、复权收益、资金流/筹码解释层。下一阶段把“选股”和“交易”拆开，使回测从单一收益口径升级为“选股策略 + 交易策略”的组合评估。

---

## 1. 核心产品边界

### 1.1 选股策略：回答“选谁”

现有选股策略页面主要仍然服务选股策略。

它负责：

- 候选池过滤
- 因子打分
- 分数底线
- 每日最大入选数量
- 入选原因 / 风险解释

典型例子：

- `v13_three_factor`
- `lowvol_reversal`
- 后续新多因子策略

### 1.2 交易策略：回答“怎么买、怎么卖”

新增交易策略模块。

它负责：

- 买入时点
- 卖出时点
- 持有周期
- 是否启用止盈止损
- 是否启用交易成本
- 是否启用成交约束
- 未来是否接入仓位、资金占用、再平衡

现有回测里的两个收益口径应升级为两个内置交易策略：

1. `next_open_1d`
   - 中文名：次日开盘卖出
   - 买入：入选日开盘价
   - 卖出：下一交易日开盘价
   - 当前对应：`1日收益`

2. `hold_3d_close`
   - 中文名：持有 3 日收盘卖出
   - 买入：入选日开盘价
   - 卖出：第 3 个后续交易日收盘价
   - 当前对应：`3日收益`

### 1.3 回测方案：选股策略 + 交易策略

后续回测不再只选择“策略”和“收益口径”，而是：

```text
选股策略 + 交易策略 = 回测方案
```

例子：

```text
三因子选股策略 + 次日开盘卖出
三因子选股策略 + 持有 3 日收盘卖出
低波反转选股策略 + 次日开盘卖出
低波反转选股策略 + 持有 3 日收盘卖出
```

### 1.4 交易后分析：回答“为什么这样，怎么优化”

交易后分析模块不负责生成策略，而是复盘回测结果。

它回答：

- 为什么在这个点买入？
- 为什么在这个点卖出？
- 哪些交易失败？为什么失败？
- 收益主要来自哪些股票 / 哪些交易日？
- 亏损主要来自哪些规则或市场环境？
- 后续应该怎么优化选股策略或交易策略？

---

## 2. 页面结构建议

### 2.1 推荐单独新开页面

建议导航上并列保留两个入口：`选股策略` 和 `交易策略`。

原因：

- 选股策略的心智是“选谁”。
- 交易策略的心智是“怎么买卖”。
- 两者都属于策略体系，但对象不同；放在同一个页面会让规则编辑、历史表现和交易复盘混在一起。
- 视觉上可以相邻放置，形成“策略管理体系”的感觉；信息架构上保持两个页面。

建议导航：

```text
首页
选股中心
跟踪复盘
回测中心       <-- 完整交易后分析放这里，跟随具体 run
选股策略
交易策略       <-- 新增，只管理交易规则
数据状态
```

### 2.2 V2.2 页面最小版

先做一个独立页面：`/trade-strategies`

包含三个区域：

1. 交易策略列表
   - 内置策略
   - 启用状态
   - 买入规则
   - 卖出规则
   - 是否使用真实计算

2. 交易策略详情
   - 策略名称
   - 策略类型
   - 买入规则 JSON
   - 卖出规则 JSON
   - 高级设置 JSON
   - 说明

3. 最近表现摘要
   - 最近回测结果
   - 胜率 / 平均收益
   - 失败交易率
   - 点击跳转到对应回测详情查看完整交易后分析

---

## 3. 回测中心改造方向

### 3.1 表单结构

当前回测表单：

```text
策略 + 日期 + 每日入选 + 分数底线 + 收益口径
```

后续改为：

```text
选股策略 + 交易策略 + 日期 + 每日入选 + 分数底线
```

高级设置保留真实计算：

- 复权收益
- 手续费
- 印花税
- 滑点
- 成交约束

### 3.2 两种评估口径

#### 研究口径

默认口径。

关闭真实计算，用于观察选股策略本身质量：

- 胜率
- 平均收益
- 总收益
- 最大回撤
- 不同交易策略下的收益差异

#### 真实交易模拟口径

高级设置开启。

用于观察真实执行后的结果：

- 手续费
- 印花税
- 滑点
- 涨跌停成交约束
- 停牌约束

产品文案建议：

```text
研究口径：关闭真实成本，用于评估选股策略本身。
真实交易模拟：开启交易成本和成交约束，用于接近真实执行结果。
```

---

## 4. 最小 schema 草案

> 目标：先把对象边界定清楚，不急着一次性实现完整撮合系统。

### 4.1 `trade_strategy`：交易策略定义表

```sql
CREATE TABLE IF NOT EXISTS trade_strategy (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    strategy_id VARCHAR(64) NOT NULL,
    display_name VARCHAR(128) NOT NULL,
    version VARCHAR(32) NOT NULL DEFAULT 'v1',
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    is_builtin TINYINT(1) NOT NULL DEFAULT 0,
    description TEXT DEFAULT NULL,
    buy_rule_json JSON NOT NULL,
    sell_rule_json JSON NOT NULL,
    risk_rule_json JSON DEFAULT NULL,
    cost_rule_json JSON DEFAULT NULL,
    execution_rule_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_trade_strategy_version (strategy_id, version),
    KEY idx_trade_strategy_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### 初始内置数据

```sql
INSERT INTO trade_strategy (
    strategy_id, display_name, version, status, is_builtin, description,
    buy_rule_json, sell_rule_json, risk_rule_json, cost_rule_json, execution_rule_json
) VALUES
(
    'next_open_1d',
    '次日开盘卖出',
    'v1',
    'active',
    1,
    '入选日开盘买入，下一交易日开盘卖出；对应当前 1 日收益口径。',
    JSON_OBJECT('entry_day', 'selection_day', 'entry_price', 'open'),
    JSON_OBJECT('exit_day_offset', 1, 'exit_price', 'open'),
    JSON_OBJECT(),
    JSON_OBJECT('enabled', false),
    JSON_OBJECT('enabled', false)
),
(
    'hold_3d_close',
    '持有 3 日收盘卖出',
    'v1',
    'active',
    1,
    '入选日开盘买入，第 3 个后续交易日收盘卖出；对应当前 3 日收益口径。',
    JSON_OBJECT('entry_day', 'selection_day', 'entry_price', 'open'),
    JSON_OBJECT('exit_day_offset', 3, 'exit_price', 'close'),
    JSON_OBJECT(),
    JSON_OBJECT('enabled', false),
    JSON_OBJECT('enabled', false)
);
```

---

### 4.2 `backtest_trade_order`：回测交易订单/成交记录表

用于记录每一笔模拟交易，包括失败交易。

```sql
CREATE TABLE IF NOT EXISTS backtest_trade_order (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(128) NOT NULL,
    selection_strategy_id VARCHAR(64) NOT NULL,
    trade_strategy_id VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    code VARCHAR(16) NOT NULL,
    name VARCHAR(64) DEFAULT NULL,
    rank_no INT DEFAULT NULL,
    score DECIMAL(12,4) DEFAULT NULL,

    order_side VARCHAR(16) NOT NULL,
    order_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    failure_reason VARCHAR(128) DEFAULT NULL,

    planned_trade_date DATE DEFAULT NULL,
    planned_price_type VARCHAR(32) DEFAULT NULL,
    planned_price DECIMAL(14,4) DEFAULT NULL,

    executed_trade_date DATE DEFAULT NULL,
    executed_price_type VARCHAR(32) DEFAULT NULL,
    executed_price DECIMAL(14,4) DEFAULT NULL,
    executed_quantity DECIMAL(20,4) DEFAULT NULL,
    executed_amount DECIMAL(20,4) DEFAULT NULL,

    fee_amount DECIMAL(20,4) DEFAULT NULL,
    stamp_tax_amount DECIMAL(20,4) DEFAULT NULL,
    slippage_amount DECIMAL(20,4) DEFAULT NULL,

    rule_snapshot_json JSON DEFAULT NULL,
    decision_reason_json JSON DEFAULT NULL,
    market_context_json JSON DEFAULT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY idx_bto_run (run_id),
    KEY idx_bto_code_date (code, trade_date),
    KEY idx_bto_status (order_status),
    KEY idx_bto_strategy (selection_strategy_id, trade_strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `order_status` 建议枚举

- `planned`：已计划
- `filled`：已成交
- `skipped`：策略跳过
- `failed`：成交失败
- `blocked_limit_up`：涨停买不进
- `blocked_limit_down`：跌停卖不出
- `suspended`：停牌
- `missing_price`：缺少价格

---

### 4.3 `backtest_trade_analysis`：交易后分析表

用于记录回测完成后的复盘结果。

```sql
CREATE TABLE IF NOT EXISTS backtest_trade_analysis (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(128) NOT NULL,
    analysis_scope VARCHAR(32) NOT NULL DEFAULT 'run',
    code VARCHAR(16) DEFAULT NULL,
    trade_date DATE DEFAULT NULL,

    summary TEXT DEFAULT NULL,
    buy_reason TEXT DEFAULT NULL,
    sell_reason TEXT DEFAULT NULL,
    failure_analysis TEXT DEFAULT NULL,
    optimization_suggestion TEXT DEFAULT NULL,

    metrics_json JSON DEFAULT NULL,
    reason_json JSON DEFAULT NULL,
    generated_by VARCHAR(32) NOT NULL DEFAULT 'rule',

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY idx_bta_run (run_id),
    KEY idx_bta_code_date (code, trade_date),
    KEY idx_bta_scope (analysis_scope)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `analysis_scope` 建议枚举

- `run`：整轮回测分析
- `trade`：单笔交易分析
- `date`：单个交易日分析
- `strategy_pair`：选股策略 + 交易策略组合分析

---

### 4.4 `backtest_run` 最小扩展

现有 `backtest_run` 可补充：

```sql
ALTER TABLE backtest_run
    ADD COLUMN trade_strategy_id VARCHAR(64) DEFAULT NULL AFTER strategy_id,
    ADD COLUMN evaluation_mode VARCHAR(32) NOT NULL DEFAULT 'research' AFTER return_mode,
    ADD COLUMN commission_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER use_adjusted_price,
    ADD COLUMN stamp_tax_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER commission_bps,
    ADD COLUMN slippage_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER stamp_tax_bps,
    ADD COLUMN execution_constraints_enabled TINYINT(1) NOT NULL DEFAULT 0 AFTER slippage_bps,
    ADD KEY idx_backtest_trade_strategy (trade_strategy_id),
    ADD KEY idx_backtest_evaluation_mode (evaluation_mode);
```

说明：

- `strategy_id` 暂时保持选股策略 ID。
- `trade_strategy_id` 新增为交易策略 ID。
- `return_mode` 兼容旧字段，后续逐步由 `trade_strategy_id` 替代。
- `evaluation_mode`：`research` / `realistic`。

---

## 5. 最小 API 草案

### 5.1 交易策略

```text
GET    /api/trade-strategies
GET    /api/trade-strategies/{strategy_id}
POST   /api/trade-strategies
PUT    /api/trade-strategies/{strategy_id}
```

V2.2 最小版只需要：

```text
GET /api/trade-strategies
```

先返回两个内置策略即可。

### 5.2 回测创建

新增字段：

```json
{
  "selection_strategy_id": "v13_three_factor",
  "trade_strategy_id": "next_open_1d",
  "evaluation_mode": "research",
  "advanced": {
    "use_adjusted_price": false,
    "commission_bps": 0,
    "stamp_tax_bps": 0,
    "slippage_bps": 0,
    "execution_constraints_enabled": false
  }
}
```

兼容期：

- 前端仍可传 `strategy_id`，后端映射为 `selection_strategy_id`。
- 前端仍可传 `return_mode=1d/3d`，后端映射为 `trade_strategy_id=next_open_1d/hold_3d_close`。

---

## 6. UI 草图说明

### 6.0 页面职责更新

V1 明确拆分：

```text
交易策略页：规则管理
回测详情页：结果复盘 / 交易后分析
```

交易后分析依赖具体 run、候选股票、买卖点、成本参数和当时市场环境，因此不放在交易策略页做主模块。交易策略页只展示最近表现摘要，并跳转到回测详情查看完整复盘。

### 6.1 交易策略页面布局

推荐结构：

```text
┌─────────────────────────────────────────────────────┐
│ 交易策略                                            │
│ 选股负责“选谁”，交易策略负责“怎么买卖”。            │
├─────────────────────────────────────────────────────┤
│ 策略列表                                            │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ 次日开盘卖出 │ │ 持有3日收盘卖 │ │ + 新建策略    │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
├───────────────────────────┬─────────────────────────┤
│ 策略规则详情              │ 最近回测表现             │
│ 买入：入选日开盘          │ 胜率 / 平均收益 / 失败数  │
│ 卖出：下一交易日开盘      │                         │
│ 成本：默认关闭            │                         │
├───────────────────────────┴─────────────────────────┤
│ 最近表现摘要 / 回测详情入口                          │
│ 胜率 / 平均收益 / 失败交易率 / 跳转完整复盘          │
└─────────────────────────────────────────────────────┘
```

### 6.2 回测中心调整

回测中心承载完整交易后分析。每个 run 的详情中增加：

```text
本轮交易复盘
- 买入点解释
- 卖出点解释
- 成功交易 / 失败交易
- 失败原因分布
- 成本影响
- 优化建议
```

回测表单主区域：

```text
选股策略 | 交易策略 | 日期范围 | 每日入选 | 分数底线 | 运行回测
```

高级设置：

```text
复权收益
评估模式：研究口径 / 真实交易模拟
手续费 bps
印花税 bps
滑点 bps
成交约束
```

---

## 7. 实施顺序建议

### Step 1：schema + seed

- 新增 `trade_strategy`
- 新增两个内置交易策略
- `backtest_run` 补 `trade_strategy_id` / `evaluation_mode`

### Step 2：回测兼容改造

- `return_mode=1d` 映射 `next_open_1d`
- `return_mode=3d` 映射 `hold_3d_close`
- 暂时不删除旧字段

### Step 3：新增 `/trade-strategies` 页面

- 只读展示两个内置策略
- 暂不开放复杂编辑

### Step 4：交易记录表落地

- 回测时写入 `backtest_trade_order`
- 成功和失败都记录

### Step 5：回测详情内交易后分析最小版

- 规则生成，不先接 LLM
- 放在 `/backtest` 的 run 详情中，不单独新开页面
- 先做固定模板：
  - 买入依据
  - 卖出依据
  - 失败原因
  - 优化建议

---

## 8. 当前决策

1. 导航上并列放置“选股策略 / 交易策略”；两者同属策略体系，但页面分开。
2. 当前 `1日收益 / 3日收益` 应升级为两个内置交易策略。
3. 回测高级设置保留真实计算参数，关闭时看选股策略本身胜率，开启时看真实交易模拟效果。
4. 交易后分析 V1 放在回测详情内，不放交易策略页；交易策略页只做规则管理和最近表现摘要。
5. 下一步优先做 schema + seed + 回测兼容映射，而不是先做复杂策略编辑器。

---

## 9. V2.2 最小开发进展（2026-05-11 晚）

已完成第一批最小闭环：

1. Schema 已落地
   - `trade_strategy`
   - `backtest_trade_order`
   - `backtest_trade_analysis`
   - `backtest_run` 扩展：`trade_strategy_id`、`evaluation_mode`、`commission_bps`、`stamp_tax_bps`、`slippage_bps`、`execution_constraints_enabled`

2. 内置交易策略 seed 已落地
   - `next_open_1d`：次日开盘卖出
   - `hold_3d_close`：持有 3 日收盘卖出

3. API 已落地
   - `GET /api/trade-strategies`
   - `GET /api/trade-strategies/{strategy_id}`

4. 页面已落地
   - 新增 `/trade-strategies` 只读页面
   - 导航已加入“交易策略”
   - 页面展示策略卡片、规则详情、回测使用说明

5. 回测兼容映射已落地
   - `trade_strategy_id=next_open_1d` 自动映射 `return_mode=1d`
   - `trade_strategy_id=hold_3d_close` 自动映射 `return_mode=3d`
   - 旧 `return_mode` 仍保留兼容
   - 回测高级设置新增 `stamp_tax_bps`

公网验证：

- `/api/trade-strategies` HTTP 200，返回两个内置策略。
- `/trade-strategies` HTTP 200。
- `/backtest` HTTP 200，页面包含交易策略选择和印花税设置。
- `POST /api/backtest/run` + `trade_strategy_id=next_open_1d` 成功，返回 `return_mode=1d`，3 trades，总收益 `-0.8809%`。
- `POST /api/backtest/run` + `trade_strategy_id=hold_3d_close` 成功，返回 `return_mode=3d`。

下一步建议：

1. 回测 run 保存时开始写入 `trade_strategy_id` 历史记录。
2. 在回测详情区域新增“交易后分析”最小卡片。
3. 回测成功/失败交易逐步写入 `backtest_trade_order`。

## 2026-05-12 补充：T+3 每日观察交易策略

大X明确希望把 T+1/T+2/T+3 每天收盘价、最大浮盈、最大回撤做成一个可选择的交易策略，而不是所有回测明细的默认附加展开。

已新增内置交易策略：

- `observe_t3_daily` / **T+3 每日观察回测**
- 买入：入选日开盘价
- 观察：T+1 / T+2 / T+3
- 每日明细：收盘价、相对入选价收盘收益、当日最高价对应最大浮盈、当日最低价对应最大回撤
- 汇总收益：暂按 T+3 收盘收益计算，便于和其他交易策略在回测中心同屏比较

该策略定位为“选股策略诊断工具”：用来观察入选后 1~3 天的价格路径，而不是完整交易执行模拟。
