# Tushare 接口接入更新计划（2026-04-28）

## 背景

当前项目已完成：
- 日线历史数据补齐到股票口径 100% 覆盖
- 日线自动增量 / 历史补齐 / 基本面自动任务的 cron 化
- 数据状态页已可展示最新同步状态

当前新的外部条件：
- 已配置 `TUSHARE_TOKEN`
- 当前账号已达到 **5000 积分**
- 结合当前项目阶段，可开始正式接入以下高价值接口：
  - `fina_indicator`
  - `daily_basic`
  - `cyq_perf`
  - `adj_factor`
  - `moneyflow`

---

## 总目标

把项目从“日线主链路已通”推进到“基本面 + 估值 + 筹码 + 资金流逐步可用”的状态，优先提升：

1. 基本面覆盖率
2. 估值覆盖率
3. 选股解释质量
4. 跟踪复盘信息密度

---

## 接入优先级

### P0（立刻推进）

#### 1. `fina_indicator`

**目标**
- 扩大 `stock_basic` 中以下字段覆盖：
  - `roe`
  - `roa`
  - `grossprofit_margin`
  - `netprofit_margin`
  - `revenue_yoy`
  - `profit_yoy`
  - `fundamental_period`
  - `fundamental_updated_at`

**当前状态**
- 已有 `app.data_ingestion.fundamental_sync`
- 已有自动任务入口 `scripts/run_fundamental_daily_update.py`
- 当前需要做真实小批量验证 + 正式跑数

**下一步动作**
1. 手动验证 `fundamental_sync.py` 小批量可跑
2. 确认字段真实写入 MySQL
3. 观察频控和失败重试表现
4. 再放给 nightly cron 稳定执行

---

#### 2. `daily_basic`

**目标**
- 提升 `stock_basic` 中以下估值字段覆盖：
  - `pe_tushare`
  - `pb_tushare`
  - `valuation_updated_at`

**当前状态**
- 仓库已有历史脚本 `update_pe_pb_tushare.py`
- 但尚未整合进 `app/` 新主线与数据状态页自动任务闭环

**下一步动作**
1. 审核并整理 `update_pe_pb_tushare.py` 逻辑
2. 迁移或重写为 `app.data_ingestion.valuation_sync`
3. 接入 task log 与 cron
4. 在数据状态页展示估值覆盖进度

---

### P1（紧接着推进）

#### 3. `cyq_perf`

**目标**
- 新增筹码结构数据能力，用于：
  - 选股解释增强
  - 跟踪复盘增强
  - 低吸/反转类策略优化

**建议新增字段 / 表**
- 新建 `chip_snapshot`（或类似命名）
- 核心字段建议包括：
  - `code`
  - `trade_date`
  - `cost_5pct`
  - `cost_15pct`
  - `cost_50pct`
  - `cost_85pct`
  - `cost_95pct`
  - `weight_avg`
  - `winner_rate`

**接入顺序**
1. 先建表
2. 先做单票/小批量同步器
3. 再接到策略解释与跟踪复盘页

---

#### 4. `adj_factor`

**目标**
- 提升价格可比性与回测一致性
- 为后续更严格复权逻辑留好基础

**建议方式**
- 新建 `adj_factor` 表，不直接污染当前 `daily_kline`
- 后续由 selector / tracker 决定是否按复权因子换算

**接入顺序**
1. 建表与同步器
2. 先做数据准备，不急着立刻接页面

---

### P2（增强项）

#### 5. `moneyflow`

**目标**
- 提供资金流视角
- 用于增强：
  - 因子分析
  - 单票详情
  - 选股解释

**建议方式**
- 新建 `moneyflow_snapshot` 表
- 先做近 3~6 个月数据，不急着全历史

---

## 推荐执行顺序

### 第一阶段（现在就开始）
1. 跑通 `fina_indicator` 小批量验证
2. 跑 `daily_basic` 估值补齐
3. 把基本面 / 估值覆盖率在数据状态页展示得更完整

### 第二阶段
4. 设计并落地 `cyq_perf` 数据表与同步器
5. 在选股解释 / 跟踪复盘中接入筹码指标

### 第三阶段
6. 接入 `adj_factor`
7. 接入 `moneyflow`
8. 再考虑更深层财务三表接口

---

## 关键原则

1. **先补当前页面最缺的数据，不先追求花哨接口全接入**
2. **优先把接口纳入 `app/` 新主线，不继续堆旧脚本**
3. **每接一个接口，都要同步考虑：表结构、同步器、task log、cron、数据状态页**
4. **新增数据先入库，再考虑页面消费；不要边抓边写死在前端逻辑里**

---

## 当前结论

5000 积分阶段，现阶段最值得正式纳入项目主线的接口优先级是：

1. `fina_indicator`
2. `daily_basic`
3. `cyq_perf`
4. `adj_factor`
5. `moneyflow`

一句话：

> 先把基本面和估值补厚，再把筹码和资金流接进来，让项目从“日线能看”升级到“分析像样”。
