# 策略页重构与接口排查落地方案

更新时间：2026-04-27 18:22

## 一、目标

基于最新产品决定，当前 V1 页面进入新的结构调整阶段：

1. **因子分析主展示移到策略管理页**
2. **选股中心结果列表增加“选股策略”字段**
3. **策略页从单策略薄页扩成多策略工作台**
4. **将 V13 / V12 纳入页面策略体系**
5. **同步排查接口数据是否正常，避免把数据问题误认为页面问题**

---

## 二、当前真实现状

## 2.1 页面结构现状

### 已有
- `/strategies`：只有基础策略列表
- `/selection`：已具备结果列表 + 策略摘要 + 因子分析
- `/tracking`：复盘页已成型

### 当前不符合最新设计决定的地方
- 因子分析仍在 `selection` 页面主区展示
- `strategies` 页面过薄，没有承接策略说明与因子分析主展示
- `selection` 列表虽然详情里能看到策略，但主表还没把“选股策略”作为明确字段突出展示
- 策略页目前只有一个正式注册策略，页面认知上太单薄

---

## 2.2 策略体系现状

### 已正式注册
- `lowvol_reversal`

### 已存在但未正式挂入注册表
- `v13_three_factor`
  - 位置：`app/strategies/active/v13_three_factor/`
  - 特征：已进入新架构目录，但更接近模板 / 骨架 + 近似逻辑

### 旧体系历史策略
- `V12`
  - 位置：`src/strategies/v12_strategy.py`
  - 以及 `v12_strategy_v6.py` / `v12_strategy_v7.py`
  - 特征：仍在旧体系，尚未迁入 `app/strategies` 当前注册链路

### 产品判断
- `V13` 可以较快纳入当前页面策略体系
- `V12` 适合先作为 **legacy / 待迁移策略** 展示，不应伪装成已完整接入当前执行链路

---

## 2.3 接口与数据现状排查

### 已确认正常的点
- `selection_result` 表中有最近选股数据
- 关键字段存在：
  - `run_id`
  - `trade_date`
  - `strategy_id`
  - `score`
  - `metadata_json`
  - `created_at`
- `metadata_json` 中已能取到：
  - `strategy_display_name`
  - `strategy_version`
  - `name`
  - `raw_metrics.close`
  - `factors`
  - `explain.reasons`
  - `explain.risks`

### 已确认的近期运行情况
最近选股 run 存在，例如：
- `selection_20260427_122725`
- `selection_20260427_120352`
- `selection_20260427_120327`

### 已确认的接口数据风险点
#### 风险 1：`/api/selection/results` 默认只拿“最近一个 run_id”
当前逻辑：
```sql
SELECT run_id FROM selection_result ORDER BY created_at DESC LIMIT 1
```
这会导致：
- 页面只能默认看最后一次 run
- 多策略并存后，用户容易误解为“系统只有这套策略结果”

#### 风险 2：`selection_result` 表没有 `selected_at` 字段
只有：
- `trade_date`
- `created_at`

这意味着页面层如果想展示：
- 选股交易日
- 实际入库时间

必须明确区分，不然容易混淆。

#### 风险 3：当前只有一个正式策略被注册
所以即便页面想展示多策略，接口 `/api/strategies` 当前也拿不出来。

#### 风险 4：部分“数据问题”其实是覆盖率问题
例如：
- 基本面字段大量缺失
- 并不是接口坏了，而是底层数据本来就没补齐

---

## 三、建议落地顺序

## P1 - 先改设计与注册体系

### 任务 1：策略注册表扩容
将以下策略纳入注册表：
- `lowvol_reversal`（current / active）
- `v13_three_factor`（current / experimental）
- `v12_legacy`（legacy / inactive or research）

### 任务 2：为 V13 增加标准 registry config
补：
- registry config 文件
- 策略元信息字段
- 因子说明字段（与当前策略页结构兼容）

### 任务 3：为 V12 增加“页面可展示元信息”
即使先不接执行链路，也要给它：
- id
- display_name
- version
- status
- description
- tags
- mode=legacy
- 核心因子说明

---

## P2 - 重构策略管理页

### 页面目标
让 `/strategies` 从“单纯策略列表页”变成：
- 策略总览页
- 策略详情页
- 因子分析主页

### 页面模块
1. 顶部统计卡
   - 默认策略
   - 策略数量
   - current 策略数
   - legacy 策略数

2. 左/上方策略列表
   - 支持点击切换当前策略

3. 右/下方策略详情
   - 策略说明
   - 版本
   - 状态
   - 核心因子
   - 阈值 / 最大持仓数

4. 因子分析表
   - 因子名
   - 类别
   - 方向
   - 权重
   - CI
   - 覆盖率
   - 缺失率
   - 启用状态

### 注意
- 对于 legacy 策略，如果没有真实 factor stats，可先展示配置级说明
- 不强装成所有策略都已可运行

---

## P3 - 改造选股中心

### 要做的事
1. 主表格新增“选股策略”列
2. 顶部摘要继续展示当前 run 的策略
3. 去掉完整因子分析主表，仅保留策略摘要 / 跳转入口
4. 保留展开详情中的策略信息

### 结果
选股中心只回答：
- 这次选了谁
- 属于哪套策略
- 为什么选

不再承担完整因子分析页职责。

---

## P4 - 接口与数据排查修复

### 需要验证的接口
1. `GET /api/strategies`
2. `GET /api/strategies/detail`
3. `GET /api/selection/results`
4. `GET /api/tracking`
5. `GET /api/dashboard/summary`
6. `GET /api/system/status`

### 当前已发现的问题
- 当前环境里直接跑 FastAPI TestClient 失败，提示缺少 `fastapi` 模块，说明本地直接验证方式需要改成：
  - 用当前运行中的服务实测
  - 或使用项目实际虚拟环境 / 启动方式验证

### 建议排查内容
#### A. 选股结果接口
重点看：
- 是否带出 `strategy_id`
- 是否带出 `strategy_display_name`
- `trade_date` / `created_at` 是否语义清晰
- `current_price` 是否与最新 `daily_kline` 一致
- `price_change_pct` 是否按选股价正常计算

#### B. 策略详情接口
重点看：
- 多策略后是否都能返回
- legacy 策略是否要特殊兼容
- 因子说明是否完整

#### C. 数据状态接口
重点看：
- 字段缺失统计是否与数据库一致
- 页面展示的问题是否本质来自底层缺失

---

## 四、明确的开始方式

## 第一刀（最稳）
先做 **文档 + 注册表 + 策略页重构骨架**：

1. 更新设计文档 ✅
2. 扩注册表
3. 扩 `/api/strategies`
4. 扩 `/api/strategies/detail`
5. 重构 `/strategies` 页面

## 第二刀
再做 **选股中心收口**：

1. 增加“选股策略”列
2. 弱化因子分析区
3. 增加跳转到策略页的入口

## 第三刀
最后做 **接口数据问题排查与修修补补**：

1. 比对接口与 DB
2. 修语义不一致字段
3. 修页面展示误导

---

## 五、当前最重要的现实判断

当前“看起来像有数据问题”这件事，来源可能有三类：

1. **页面结构放错了地方**
   - 例如因子分析放在选股页，让人误以为结果页本身有问题

2. **接口字段语义不清**
   - 例如 `trade_date` 与 `created_at` 容易混淆

3. **底层数据覆盖本来就不足**
   - 例如基本面大面积缺失，不是接口 bug，而是数据确实没补齐

所以排查时必须分层：
- 页面问题
- 接口问题
- 数据问题

不能混在一起看。

---

## 六、下一步建议

如果立刻开干，建议按这个顺序：

### 现在开始
1. 扩 `strategies.yaml`
2. 给 V13 / V12 增加 registry-level 元信息
3. 改 `/api/strategies` 与 `/api/strategies/detail`
4. 改 `/strategies` 页面为因子分析主页

### 然后
5. 改 `/selection` 页面，增加“选股策略”列
6. 去掉完整因子分析主表

### 同步
7. 对 `selection_result`、`daily_kline`、`stock_basic` 做接口字段一致性复查

一句话：

> **先把页面职责改对，再继续修接口，再分辨哪些其实是底层缺数据。**
