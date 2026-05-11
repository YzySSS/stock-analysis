# V2.1 数据增强与回测严谨化进展（2026-05-11）

## 阶段目标

V2 基础版已经形成“历史输入层 + 回测引擎 + 回测页面 + 实时行情”的闭环。V2.1 开始补强原 V2 规划中的增强项，优先顺序：

1. 复权回测底座：`adj_factor`
2. 个股资金流输入层：`moneyflow`
3. 后续再接筹码 `cyq_perf`、更严格的财报 point-in-time、交易成本/滑点/成交约束

## 已完成：复权因子表与同步链路

### 新增表

`adj_factor_daily`

字段：

- `code`
- `trade_date`
- `adj_factor`
- `source='tushare_adj_factor'`

唯一键：`code + trade_date`

### 新增代码

- `app/data_ingestion/adj_factor_sync.py`
- `scripts/run_adj_factor_daily_update.py`

### 数据源

Tushare `adj_factor`

### 实跑验证

- 日期范围：`2026-05-08` ~ `2026-05-11`
- 处理交易日：2
- 入库：`11040` 行
- 覆盖代码数：`5521`

## 已完成：个股资金流表与同步链路

### 新增表

`stock_moneyflow_daily`

字段覆盖 Tushare `moneyflow` 主体字段：

- 小单/中单/大单/特大单买卖量和金额
- `net_mf_vol`
- `net_mf_amount`
- `source='tushare_moneyflow'`

唯一键：`code + trade_date`

### 新增代码

- `app/data_ingestion/moneyflow_sync.py`
- `scripts/run_moneyflow_daily_update.py`

### 数据源

Tushare `moneyflow`

### 实跑验证

- 日期范围：`2026-05-08` ~ `2026-05-11`
- 处理交易日：2
- 入库：`10360` 行
- 覆盖代码数：`5183`


## 已完成：筹码表现表与同步链路

### 新增表

`stock_chip_daily`

字段来自 Tushare `cyq_perf`：

- `his_low` / `his_high`
- `cost_5pct` / `cost_15pct` / `cost_50pct` / `cost_85pct` / `cost_95pct`
- `weight_avg`
- `winner_rate`
- `source='tushare_cyq_perf'`

唯一键：`code + trade_date`

### 新增代码

- `app/data_ingestion/chip_sync.py`
- `scripts/run_chip_daily_update.py`

### 实跑验证

- 日期范围：`2026-05-08` ~ `2026-05-11`
- 处理交易日：2
- 入库：`10982` 行
- 最新日 `2026-05-11` 覆盖：`5491 / 5516`，约 `99.55%`

### 系统页接入

`/api/system/status` 数据基准卡新增：`筹码数据`。

## 已完成：回测支持复权收益计算

文件：`app/backtest/service.py`

- `BacktestRequest.use_adjusted_price` 从占位字段变为可用字段。
- `_fetch_future_bars()` 读取 `adj_factor_daily.adj_factor`。
- `_build_trades()` 在 `use_adjusted_price=true` 时按：

```text
return = exit_price * exit_adj_factor / (entry_price * entry_adj_factor) - 1
```

计算 1d / 3d 收益。

- 最大涨幅/最大回撤在复权模式下也按 entry factor 转换到同一价格尺度后计算。

### API 验证

公网 API smoke：

- `POST /api/backtest/run`
- V13
- `2026-05-08` 单日
- `return_mode=1d`
- `use_adjusted_price=true`
- `save=false`

结果：

- HTTP 200
- `status=success`
- `sample_days=1`
- `total_picks=3`
- `total_trades=3`
- `total_return_pct=-0.8809`

## 已完成：页面与系统状态接入

### `/backtest` 页面

- 新增“使用复权收益”复选框。
- 提交回测时传递 `use_adjusted_price`。
- 回测详情 `run_id` 行标注“复权收益 / 不复权”。
- 最近回测列表中对复权任务显示“复权”。

验证：

- `https://www.yzysstock.cloud/backtest` HTTP 200。
- 页面 HTML 已包含 `backtest-use-adjusted-price`。
- `node --check app/api/web/js/backtest.js` 通过。

### `/system` 数据状态

- 数据基准卡新增：
  - `复权因子`
  - `个股资金流`
- 当前覆盖：
  - 复权因子：`5521 / 5516`，展示百分比封顶为 `100%`
  - 个股资金流：`5180 / 5516`，约 `93.91%`

## 已完成：moneyflow 进入候选 raw metrics

已在当前选股候选构建和回测候选构建中左联 `stock_moneyflow_daily`，并写入 raw metrics：

- `net_mf_amount`
- `net_mf_vol`
- `buy_lg_amount`
- `sell_lg_amount`
- `buy_elg_amount`
- `sell_elg_amount`

验证：公网 V13 选股返回的 3 只股票 raw metrics 已包含资金流字段，例如：

- `sh.600428`：`net_mf_amount=-5324.31`
- `sh.600054`：`net_mf_amount=-2175.79`
- `sh.600873`：`net_mf_amount=-3725.94`

## 已接入 cron

新增：

```cron
10 2 * * * scripts/run_adj_factor_daily_update.py --recent-trade-days 5
20 2 * * * scripts/run_moneyflow_daily_update.py --recent-trade-days 5
```

保留：

- `15:10` AkShare 快速日 K
- `15:25` 当天 factor input 快速补齐
- `02:00` Tushare 官方日 K 校准
- `03:20` factor input 日常补齐

## 已完成：筹码数据进入解释层

- 当前选股候选和回测候选已左联 `stock_chip_daily`。
- raw metrics 已新增：
  - `chip_his_low` / `chip_his_high`
  - `chip_cost_5pct` / `chip_cost_15pct` / `chip_cost_50pct` / `chip_cost_85pct` / `chip_cost_95pct`
  - `chip_weight_avg`
  - `chip_winner_rate`
- `/api/stocks/{code}` 已新增 `chip` 对象，包含：获利比例、加权平均成本、成本集中带、价格偏离平均成本、筹码状态 label。
- 个股详情页新增“筹码成本”卡片。

公网验证：

- `/api/stocks/sh.600428` HTTP 200，返回 `chip.winner_rate=20.33`、`label=套牢盘偏多`。
- `/stocks/sh.600428` HTTP 200，HTML 已包含 `stock-detail-chip`。
- V13 选股返回的 3 只股票 raw metrics 均包含筹码字段。

## 当前边界

1. `moneyflow` 和 `chip` 已进入 raw metrics/个股解释，但尚未作为策略因子参与打分。
2. 财报字段仍未做严格 point-in-time 生效日期映射。
3. 交易成本、滑点、涨跌停/停牌成交约束仍未进入回测模型。

## 下一步建议

### V2.1-P1

1. 在个股详情页展示资金流解释模块。
2. 设计 `moneyflow` 因子：例如净流入强度、特大单净流入占比、连续净流入。
3. 接入 `cyq_perf` 筹码数据。

### V2.1-P2

1. 增加交易成本/滑点/停牌涨跌停成交约束。
2. 把基本面字段升级为严格财报 point-in-time 映射。

## 已完成：资金流进入个股解释层

- `/api/stocks/{code}` 新增 `moneyflow` 对象。
- 读取 `stock_moneyflow_daily` 最新记录，并计算：
  - `large_net_amount`：大单 + 特大单净额
  - `retail_net_amount`：小单 + 中单净额
  - `net_flow_intensity_pct`：净流入金额 / 当日成交额
  - `large_flow_ratio_pct`：大/特大单净额 / 当日成交额
  - `label`：主力净流入、主力净流出、整体净流入、整体净流出、资金分歧
- 个股详情页新增“资金流”卡片。
- Tushare `moneyflow` 金额字段按“万元”口径展示；成交额强度计算时已把行情成交额从“元”换算为“万元”。

公网验证：

- `/api/stocks/sh.600428` HTTP 200，返回 `net_mf_amount=-5324.31` 万、`net_flow_intensity_pct=-13.49`、`large_flow_ratio_pct=2.71`、`label=整体净流出`。
- `/stocks/sh.600428` HTTP 200，HTML 已包含 `stock-detail-moneyflow`。

## 已完成：回测真实化第一步

`BacktestRequest` / `/api/backtest/run` 已新增：

- `commission_bps`：手续费，bps/边，默认 0
- `slippage_bps`：滑点，bps/边，默认 0
- `apply_execution_constraints`：成交约束，默认关闭

收益计算：

- 买入侧成本：`entry * (1 + commission + slippage)`
- 卖出侧收益：`exit * (1 - commission - slippage)`
- 复权收益同样兼容成本口径。

成交约束：

- 涨停开盘不买入。
- 跌停开盘不卖出。
- 停牌或无开盘价不成交。
- `_fetch_future_bars` 已补 `prev_close` 用于判断涨跌停。

`/backtest` 高级设置新增：

- 手续费 bps/边
- 滑点 bps/边
- 启用成交约束

默认不改变现有回测结果。

公网验证：

- `/backtest` HTTP 200，页面已包含手续费、滑点、成交约束控件。
- `POST /api/backtest/run`：V13、`2026-05-08`、1d、手续费 3bps、滑点 5bps、成交约束开启、`save=false` 返回 success，3 trades，总收益 `-1.0393%`。
