# V2 数据库与接口设计（2026-04-29）

## 1. 设计目标

本设计文档聚焦 V2 的两类核心能力：

1. **历史因子输入层正式化**
2. **回测能力主线化**

要求：
- 延续当前 `app/` 主线与 MySQL 风格
- 表结构与现有 `stock_basic / daily_kline / selection_result / task_run_log` 协同
- API 风格尽量贴近当前 `selection.py / tracking.py / system.py`
- 回测收益口径沿用历史逻辑：
  - 一日收益 = 当日开盘买入 → 次日开盘卖出
  - 三日收益 = 当日开盘买入 → 第三日收盘卖出

---

## 2. 设计原则

### 2.1 主线优先
- 所有新能力优先进入 `app/` 主线
- 不继续堆散乱脚本作为长期方案

### 2.2 历史可回放优先于当前快照复用
- `stock_basic` 可以继续保留“当前快照”角色
- 但 V2 回测、历史解释、策略验证必须以历史表为准

### 2.3 产品主语义优先
- 策略、选股日期、收益口径是产品主字段
- `run_id` 保留为技术追踪字段，不作为页面核心心智

### 2.4 最小闭环优先
V2 第一阶段只先保证：
- 历史因子输入可按日期读取
- `lowvol_reversal` 可按历史时间段回测
- 页面能展示 1日 / 3日收益结果

---

## 3. 现有表与 V2 的关系

### 3.1 现有表继续保留职责

#### `stock_basic`
- 角色：当前快照层
- 用途：当前选股、当前解释、基础股票主数据

#### `daily_kline`
- 角色：历史行情主表
- 用途：策略历史输入、回测价格依据、复盘价格依据

#### `selection_result`
- 角色：当前选股/复盘结果表
- 用途：V1 持久化复盘池与当前策略输出

#### `task_run_log`
- 角色：任务运行日志表
- 用途：历史因子补数、回测任务、批量导入等统一记录

### 3.2 V2 新增表的职责

V2 建议新增 5 张主表：

1. `factor_input_daily`
2. `backtest_run`
3. `backtest_pick`
4. `backtest_trade`
5. `backtest_summary_daily`

可选增强表（第二阶段再建）：
- `chip_snapshot`
- `moneyflow_snapshot`
- `adj_factor_daily`
- `market_context_daily`

---

## 4. 数据库表设计

## 4.1 `factor_input_daily`

### 目标
存储“某只股票在某个交易日可用于策略/回测的历史输入快照”。

### 当前状态
- 已有雏形
- 但当前仍需升级为正式 V2 表，并补充更明确的来源与时点字段

### 建议结构

```sql
CREATE TABLE IF NOT EXISTS factor_input_daily (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,

    -- 估值/日频输入
    pe_tushare DECIMAL(12,4) DEFAULT NULL,
    pb_tushare DECIMAL(12,4) DEFAULT NULL,
    turnover_rate DECIMAL(12,4) DEFAULT NULL,
    turnover_rate_f DECIMAL(12,4) DEFAULT NULL,
    volume_ratio DECIMAL(12,4) DEFAULT NULL,
    total_mv DECIMAL(20,4) DEFAULT NULL,
    circ_mv DECIMAL(20,4) DEFAULT NULL,

    -- 基本面映射输入
    roe DECIMAL(12,4) DEFAULT NULL,
    roa DECIMAL(12,4) DEFAULT NULL,
    grossprofit_margin DECIMAL(12,4) DEFAULT NULL,
    netprofit_margin DECIMAL(12,4) DEFAULT NULL,
    revenue_yoy DECIMAL(12,4) DEFAULT NULL,
    profit_yoy DECIMAL(12,4) DEFAULT NULL,
    fundamental_period VARCHAR(16) DEFAULT NULL,
    fundamental_publish_date DATE DEFAULT NULL,

    -- 数据质量/来源
    valuation_source VARCHAR(32) DEFAULT NULL,
    fundamental_source VARCHAR(32) DEFAULT NULL,
    valuation_updated_at DATETIME DEFAULT NULL,
    fundamental_updated_at DATETIME DEFAULT NULL,
    completeness_score DECIMAL(8,4) DEFAULT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_factor_input_daily (code, trade_date),
    KEY idx_factor_input_trade_date (trade_date),
    KEY idx_factor_input_code (code),
    KEY idx_factor_input_period (fundamental_period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 说明

#### 为什么保留 `stock_basic`
- `stock_basic` 继续服务当前页面与当前选股
- `factor_input_daily` 服务历史回放 / 回测 / V2 策略

#### 为什么增加 `fundamental_publish_date`
- 便于后续从“按当前快照回填历史日”升级为“按财报可见日期映射”
- 这是避免假历史的关键字段

#### 为什么加 `turnover_rate / volume_ratio / total_mv / circ_mv`
- 这些都应优先从 `daily_basic` 来
- 能直接支撑：
  - `v13_three_factor`
  - `v12_legacy.liquidity`
  - 回测解释增强

---

## 4.2 `backtest_run`

### 目标
记录一次回测任务的整体配置、运行状态和结果摘要锚点。

### 建议结构

```sql
CREATE TABLE IF NOT EXISTS backtest_run (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    strategy_version VARCHAR(32) DEFAULT NULL,
    instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',

    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    return_mode VARCHAR(16) NOT NULL, -- 1d / 3d
    use_adjusted_price TINYINT(1) NOT NULL DEFAULT 0,

    status VARCHAR(32) NOT NULL DEFAULT 'running', -- running/success/failed
    sample_days INT DEFAULT 0,
    total_picks INT DEFAULT 0,
    total_trades INT DEFAULT 0,

    request_json JSON DEFAULT NULL,
    summary_json JSON DEFAULT NULL,
    error_message TEXT,

    started_at DATETIME NOT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_backtest_run_id (run_id),
    KEY idx_backtest_strategy (strategy_id),
    KEY idx_backtest_status (status),
    KEY idx_backtest_date_range (start_date, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 设计意图
- `run_id`：技术主键，用于追踪一次回测任务
- `strategy_id + start_date + end_date + return_mode`：业务维度
- `request_json`：完整保存页面配置，便于复现
- `summary_json`：保存总览指标，减少重复聚合

---

## 4.3 `backtest_pick`

### 目标
存储每个回测日、每只入选股票的“选股时刻信息”。

### 建议结构

```sql
CREATE TABLE IF NOT EXISTS backtest_pick (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    code VARCHAR(16) NOT NULL,

    rank_no INT DEFAULT NULL,
    score DECIMAL(12,4) DEFAULT NULL,
    entry_price DECIMAL(12,4) DEFAULT NULL,
    entry_price_type VARCHAR(16) DEFAULT 'open',

    factor_json JSON DEFAULT NULL,
    explain_json JSON DEFAULT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_backtest_pick (run_id, trade_date, code),
    KEY idx_backtest_pick_run (run_id),
    KEY idx_backtest_pick_date (trade_date),
    KEY idx_backtest_pick_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 设计意图
- 对应“某天选了哪些股票”
- 保留当时的打分与解释，避免回放时再重算造成口径飘移
- `entry_price` 默认按历史规则为当日开盘价

---

## 4.4 `backtest_trade`

### 目标
存储单只股票在指定收益口径下的实际收益结果。

### 建议结构

```sql
CREATE TABLE IF NOT EXISTS backtest_trade (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    code VARCHAR(16) NOT NULL,

    entry_date DATE NOT NULL,
    entry_price DECIMAL(12,4) NOT NULL,

    exit_date_1d DATE DEFAULT NULL,
    exit_price_1d DECIMAL(12,4) DEFAULT NULL,
    return_1d_pct DECIMAL(12,4) DEFAULT NULL,

    exit_date_3d DATE DEFAULT NULL,
    exit_price_3d DECIMAL(12,4) DEFAULT NULL,
    return_3d_pct DECIMAL(12,4) DEFAULT NULL,

    max_gain_pct DECIMAL(12,4) DEFAULT NULL,
    max_drawdown_pct DECIMAL(12,4) DEFAULT NULL,

    benchmark_code VARCHAR(16) DEFAULT NULL,
    benchmark_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
    benchmark_return_3d_pct DECIMAL(12,4) DEFAULT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_backtest_trade (run_id, trade_date, code),
    KEY idx_backtest_trade_run (run_id),
    KEY idx_backtest_trade_date (trade_date),
    KEY idx_backtest_trade_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 设计意图
- 一次回测只需要算一次价格路径
- 同时把 `1d` 和 `3d` 结果都落进去
- 页面展示时只切换口径，不必重跑一次回测

### 为什么同时存 1d / 3d
因为历史逻辑已经明确只关心这两种收益口径，同时落库可减少：
- 重复计算
- 再次扫价格表
- 页面切换口径时的延迟

---

## 4.5 `backtest_summary_daily`

### 目标
保存每个回测日的聚合结果，用于生成收益曲线和日级统计。

### 建议结构

```sql
CREATE TABLE IF NOT EXISTS backtest_summary_daily (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,

    pick_count INT DEFAULT 0,
    avg_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_return_3d_pct DECIMAL(12,4) DEFAULT NULL,
    win_rate_1d_pct DECIMAL(12,4) DEFAULT NULL,
    win_rate_3d_pct DECIMAL(12,4) DEFAULT NULL,
    benchmark_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
    benchmark_return_3d_pct DECIMAL(12,4) DEFAULT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_backtest_summary_daily (run_id, trade_date),
    KEY idx_backtest_summary_daily_run (run_id),
    KEY idx_backtest_summary_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 设计意图
- 快速出收益曲线
- 快速出日级统计卡片
- 避免每次页面打开都扫全量 `backtest_trade`

---

## 4.6 V2 第二阶段增强表（预留）

### `chip_snapshot`
来源：`cyq_perf`

### `moneyflow_snapshot`
来源：`moneyflow`

### `adj_factor_daily`
来源：`adj_factor`

### `market_context_daily`
来源：指数、市场宽度、成交量、大盘情绪等

这些表建议第二阶段再正式建，因为 V2 最小回测闭环的第一优先级仍是：
- `factor_input_daily`
- `backtest_*`

---

## 5. API 设计

## 5.1 设计原则

沿用当前风格：
- 查询：`GET`
- 运行任务：`POST`
- 结果详情：`GET`
- 与 `selection.py` 类似，用简洁 request/response 结构

---

## 5.2 回测运行接口

### `POST /api/backtest/run`

### 请求体

```json
{
  "strategy_id": "lowvol_reversal",
  "start_date": "2025-01-01",
  "end_date": "2025-12-31",
  "return_mode": "1d",
  "instrument_type": "stock",
  "use_adjusted_price": false,
  "save": true
}
```

### 字段说明
- `strategy_id`：策略 ID
- `start_date` / `end_date`：回测时间段
- `return_mode`：`1d` / `3d`
- `instrument_type`：默认 `stock`
- `use_adjusted_price`：是否启用复权价格（V2 第一阶段可先只收字段，默认 false）
- `save`：是否写入回测结果表；默认建议 true

### 返回

```json
{
  "run_id": "backtest_20260429_201500",
  "strategy_id": "lowvol_reversal",
  "status": "success",
  "summary": {
    "sample_days": 120,
    "total_picks": 356,
    "avg_return_pct": 1.82,
    "win_rate_pct": 58.43,
    "max_drawdown_pct": -7.36
  },
  "preview": {
    "best_trade": {...},
    "worst_trade": {...},
    "daily_curve": [...]
  }
}
```

### 说明
- 返回里 `avg_return_pct` 按当前 `return_mode` 对应口径输出
- 如果后续回测耗时较长，也可切换为：
  - 先返回 `running`
  - 再从 `task_run_log` / `backtest_run` 查询进度

---

## 5.3 回测结果总览接口

### `GET /api/backtest/results`

### 查询参数
- `run_id`：可选；默认取最近一次成功回测

### 返回

```json
{
  "run_id": "backtest_20260429_201500",
  "summary": {
    "strategy_id": "lowvol_reversal",
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "return_mode": "1d",
    "sample_days": 120,
    "total_picks": 356,
    "total_trades": 356,
    "avg_return_pct": 1.82,
    "win_rate_pct": 58.43,
    "benchmark_return_pct": 0.47,
    "excess_return_pct": 1.35,
    "max_drawdown_pct": -7.36
  },
  "curve": [
    {"trade_date": "2025-01-03", "avg_return_pct": 0.52, "cum_return_pct": 0.52},
    {"trade_date": "2025-01-06", "avg_return_pct": -0.11, "cum_return_pct": 0.41}
  ]
}
```

---

## 5.4 回测明细接口

### `GET /api/backtest/trades`

### 查询参数
- `run_id`：必填
- `limit`：默认 50
- `trade_date`：可选
- `code`：可选
- `return_mode`：`1d` / `3d`

### 返回

```json
{
  "run_id": "backtest_20260429_201500",
  "return_mode": "3d",
  "items": [
    {
      "trade_date": "2025-03-03",
      "code": "sh.600000",
      "name": "浦发银行",
      "strategy_id": "lowvol_reversal",
      "rank_no": 1,
      "score": 72.35,
      "entry_price": 10.25,
      "exit_price": 10.66,
      "return_pct": 4.0,
      "return_1d_pct": 1.2,
      "return_3d_pct": 4.0,
      "factor_scores": {...},
      "reasons": [...],
      "risks": [...]
    }
  ]
}
```

### 说明
- 页面切换 1日/3日 时，只切 `return_pct` 映射
- 原始字段 `return_1d_pct / return_3d_pct` 仍保留

---

## 5.5 回测运行列表接口

### `GET /api/backtest/runs`

### 用途
供页面查看最近回测任务历史。

### 返回

```json
{
  "items": [
    {
      "run_id": "backtest_20260429_201500",
      "strategy_id": "lowvol_reversal",
      "start_date": "2025-01-01",
      "end_date": "2025-12-31",
      "return_mode": "1d",
      "status": "success",
      "started_at": "2026-04-29 20:15:00",
      "finished_at": "2026-04-29 20:16:21"
    }
  ]
}
```

---

## 5.6 因子输入状态接口

### `GET /api/factor-input/status`

### 目标
让系统页或 V2 数据页能看到历史因子输入层的覆盖情况。

### 返回建议

```json
{
  "coverage": {
    "trade_date_start": "2025-01-01",
    "trade_date_end": "2026-04-28",
    "covered_stock_codes": 5100,
    "covered_rows": 850000,
    "fields": [
      {"field": "pe_tushare", "coverage_pct": 73.2},
      {"field": "turnover_rate", "coverage_pct": 69.5},
      {"field": "roe", "coverage_pct": 95.8}
    ]
  },
  "latest_task": {
    "task_name": "factor_input_history_backfill",
    "status": "running",
    "run_id": "factor_input_history_backfill_20260429_183000"
  }
}
```

---

## 6. 回测服务层建议

## 6.1 新增模块

建议新增：

- `app/backtest/models.py`
- `app/backtest/engine.py`
- `app/backtest/service.py`
- `app/api/routes/backtest.py`

## 6.2 职责划分

### `models.py`
- 回测请求模型
- 回测结果模型
- 单笔交易模型

### `engine.py`
- 按交易日推进回测
- 根据策略与日期取候选输入
- 计算 1日 / 3日收益

### `service.py`
- 封装接口层调用
- 控制保存/汇总/查询

### `routes/backtest.py`
- API 暴露
- 与现有 FastAPI 风格保持一致

---

## 7. 与 `task_run_log` 的集成

## 7.1 任务名建议

新增受追踪任务：
- `factor_input_history_backfill`
- `backtest_run`

## 7.2 日志元数据建议

### 因子补数任务 metadata

```json
{
  "start_date": "2025-01-01",
  "end_date": "2026-04-28",
  "limit_per_day": 500,
  "rows_synced": 123456
}
```

### 回测任务 metadata

```json
{
  "strategy_id": "lowvol_reversal",
  "start_date": "2025-01-01",
  "end_date": "2025-12-31",
  "return_mode": "1d",
  "sample_days": 120,
  "total_picks": 356
}
```

## 7.3 系统状态页扩展建议

后续可在 `/api/system/status` 增加：
- `factor_input_daily` 行数
- 历史因子输入覆盖率
- 最近一次回测任务状态
- 最近一次回测时间范围与策略

---

## 8. 迁移顺序建议

## 第一阶段：先正式化历史输入表
1. 升级 `factor_input_daily` schema
2. 增加 `turnover_rate / volume_ratio / total_mv / circ_mv`
3. 增加 `fundamental_publish_date`

## 第二阶段：增加回测表
4. 建 `backtest_run`
5. 建 `backtest_pick`
6. 建 `backtest_trade`
7. 建 `backtest_summary_daily`

## 第三阶段：接 API
8. `POST /api/backtest/run`
9. `GET /api/backtest/results`
10. `GET /api/backtest/trades`
11. `GET /api/backtest/runs`
12. `GET /api/factor-input/status`

## 第四阶段：页面
13. `/backtest` 页面
14. 结果总览 + 曲线 + 明细表
15. 支持切换 1日 / 3日收益口径

---

## 9. V2 第一版明确不做的事

为了防止范围失控，以下内容建议不放进 V2 第一版首批实现：

1. 不先做多策略横向对比器
2. 不先做参数网格搜索
3. 不先做复杂组合资金曲线撮合
4. 不先做完整舆情主线回迁
5. 不先做 `v12_legacy` 全量迁移

V2 第一版只先保证：
- 历史输入可信
- 回测跑得通
- 页面能看懂

---

## 10. 结论

V2 数据库与接口设计的核心，是把当前系统从：

> 当前快照选股系统

升级成：

> 可按历史日期验证策略的数据系统

最关键的不是先补多少新花样接口，而是先把这两层建稳：

1. `factor_input_daily` 历史因子输入层
2. `backtest_*` 回测运行与结果层

一句话总结：

> 先把“历史能重放”这件事做真，再谈 V2 的策略增强、筹码、资金流和更高级回测能力。
