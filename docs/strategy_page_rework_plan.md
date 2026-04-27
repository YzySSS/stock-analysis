# 策略页重构与接口排查落地方案

更新时间：2026-04-27 18:34

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

### 当前仍待继续优化的地方
- 多策略结果还没有真正跑起来，`selection_result` 里当前仍只有 `lowvol_reversal`
- `selection` 页面虽然已加入“选股策略”列，但默认取数逻辑仍是“最近一个 run”
- `V12` 目前仍是页面展示态，不是可执行新架构策略

---

## 2.2 策略体系现状

### 当前已注册
- `lowvol_reversal`（current / active / executable）
- `v13_three_factor`（current / experimental / executable）
- `v12_legacy`（legacy / display-only / non-executable）

### 落地结果
- `V13` 已正式纳入当前页面策略体系
- `V12` 已作为 **legacy / 待迁移策略** 纳入页面展示
- 当前策略页已不再是单策略薄页，而是多策略 + 详情 + 因子分析主页

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
#### 风险 1：`/api/selection/results` 默认仍只拿“最近一个 run_id”
当前逻辑仍等价于：
```sql
SELECT run_id FROM selection_result ORDER BY created_at DESC LIMIT 1
```
这会导致：
- 页面默认只能看到最后一次 run
- 多策略并存后，用户容易误解为“系统只有这套策略结果”

#### 风险 2：`selection_result` 表没有 `selected_at` 字段
只有：
- `trade_date`
- `created_at`

当前已在接口 summary 中补充：
- `selected_trade_date`
- `run_created_at`

用来明确区分“选股交易日”和“实际入库时间”。

#### 风险 3：当前 `selection_result` 只有一个真实策略来源
实查结果：
- `selection_result.strategy_id` 当前只有 `lowvol_reversal`
- 共有 `18` 条记录，`5` 个 run

这意味着：
- 页面结构虽然已支持多策略
- 但结果数据层面暂时还没进入多策略并存阶段

#### 风险 4：很多“看起来像接口问题”的现象，本质是底层数据覆盖问题
例如：
- 最新 `daily_kline` 日期就是 `2026-04-23`
- 最新选股 run 的 `trade_date` 也是 `2026-04-23`
- 因此当前结果页中很多股票的 `price_change_pct = 0.0`

这不是前端计算错，而是因为：
- 最新行情还没有晚于选股日的数据可供比较

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

## 第二刀 ✅
已完成 **选股中心收口**：

1. 已增加“选股策略”列
2. 已移除完整因子分析主表
3. 已增加跳转到策略页的入口
4. 已在 summary 中区分 `selected_trade_date` 与 `run_created_at`

## 第三刀（当前进行中）
继续做 **接口数据问题排查与修修补补**：

1. 比对接口与 DB ✅（已完成一轮）
2. 修语义不一致字段 ✅（已补时间语义）
3. 继续确认哪些是底层数据未更新导致的显示问题
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

## 六、当前进展结论与下一步建议

### 当前已完成
1. 多策略 registry 已落地
2. 策略页已重构为因子分析主页
3. 选股页已加入“选股策略”列并移除完整因子分析主表
4. 接口 summary 已补时间语义区分

### 当前查实的数据事实
1. `daily_kline` 最新日期仍是 `2026-04-23`
2. `daily_kline` 当前覆盖 `3720 / 5200`
3. 最新选股 run 也是基于 `2026-04-23`
4. 因此当前结果页里很多 `price_change_pct=0.0` 是数据现状，不是页面 bug
5. `stock_basic` 基本面已覆盖仅 `67 / 5200`

### 下一步建议
1. 继续把接口层支持“按策略查看结果”提上日程
2. 优先继续补 `daily_kline`，避免复盘结果长期停在选股日
3. 再补估值 / 基本面，提高解释质量

一句话：

> **页面结构已经基本改对，接下来更该追的是结果数据更新与多策略结果接入。**
