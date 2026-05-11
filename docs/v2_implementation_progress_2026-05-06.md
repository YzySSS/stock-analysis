# V2 实施进展（2026-05-06）

## 本轮目标

按 V2-P0 顺序启动：

1. `factor_input_daily` 正式 schema
2. `backtest_*` 表
3. 最小回测服务/API
4. 先支持 `lowvol_reversal` 的 1日 / 3日收益验证

## 已完成

### 1. `factor_input_daily` schema 正式化

新增迁移模块：

- `app/orchestration/v2_schema.py`

已确认实库 `factor_input_daily` 补齐以下 V2 字段：

- `turnover_rate`
- `turnover_rate_f`
- `volume_ratio`
- `total_mv`
- `circ_mv`
- `fundamental_publish_date`
- `valuation_source`
- `fundamental_source`
- `valuation_updated_at`
- `fundamental_updated_at`
- `completeness_score`
- `idx_factor_input_period`

### 2. `backtest_*` 表确认可用

实库已具备：

- `backtest_run`
- `backtest_pick`
- `backtest_trade`
- `backtest_summary_daily`

### 3. `factor_input_history_sync` 升级

文件：`app/data_ingestion/factor_input_history_sync.py`

本轮升级：

- `ensure_table()` 改为调用 `ensure_v2_schema()`
- Tushare `daily_basic` 字段从 `pe/pb` 扩展到：
  - `turnover_rate`
  - `turnover_rate_f`
  - `volume_ratio`
  - `total_mv`
  - `circ_mv`
- 写入 `valuation_source / fundamental_source / completeness_score`

已做小批量实跑验证：

```bash
PYTHONPATH=$PWD .venv/bin/python scripts/run_factor_input_history_backfill.py \
  --start-date 2026-04-27 \
  --end-date 2026-04-27 \
  --limit-per-day 5 \
  --offset 0
```

结果：`rows_synced=5`，并已确认换手率、市值、量比等字段真实落库。

### 4. 最小回测服务/API 接入

新增模块：

- `app/backtest/service.py`
- `app/backtest/__init__.py`

更新接口：

- `POST /api/backtest/run`
- `GET /api/backtest/results`
- `GET /api/backtest/trades`
- `GET /api/backtest/runs`
- `GET /api/factor-input/status`

V2-P0 当前限制：

- 只支持 `lowvol_reversal`
- 只支持 `use_adjusted_price=false`
- 单次最多 260 个交易日
- 收益口径：
  - `1d`：选股当日开盘买入，下一交易日开盘卖出
  - `3d`：选股当日开盘买入，第三个后续交易日收盘卖出

## 已完成验证

### Python 编译

已通过：

```bash
PYTHONPATH=$PWD .venv/bin/python -m py_compile \
  app/backtest/service.py \
  app/api/routes/backtest.py \
  app/orchestration/v2_schema.py \
  app/data_ingestion/factor_input_history_sync.py
```

### Schema 迁移

已通过：

```bash
PYTHONPATH=$PWD .venv/bin/python -m app.orchestration.v2_schema
```

返回：

```json
{"status": "ok", "applied": [...]}
```

### API 验证

已重启线上服务：

```bash
systemctl restart stock-analysis-api.service
```

公网验证通过：

```bash
POST https://www.yzysstock.cloud/api/backtest/run
```

请求：

```json
{
  "strategy_id": "lowvol_reversal",
  "start_date": "2026-04-24",
  "end_date": "2026-04-27",
  "return_mode": "1d",
  "max_picks": 3,
  "score_threshold": 60
}
```

返回成功：

- `run_id`: `backtest_lowvol_reversal_20260506_161419`
- `sample_days`: 2
- `total_picks`: 6
- `total_trades`: 6
- `avg_return_pct`: -2.0886
- `win_rate_pct`: 16.6667

同时验证：

- `GET /api/backtest/results`
- `GET /api/backtest/trades?run_id=...`
- `GET /api/backtest/runs?limit=3`
- `GET /api/factor-input/status`
- `GET /api/health`

## 当前注意事项

1. `factor_input_daily` 目前历史覆盖只有 500 只股票、317 个交易日、158500 行；V2 后续需要扩大覆盖。
2. 新字段 `turnover_rate` 等只有本轮小批量验证的 5 行有值，后续需要正式跑批补历史。
3. 回测第一版复用了 V1 `StockSelector._build_candidate()`，后续应抽出公共 candidate builder，避免私有方法依赖。
4. 还没有 `/backtest` 页面；下一步可以进入页面最小版。
5. 当前仍不要 push GitHub，需先做 secret 清理。

## 下一步建议

1. 扩大 `factor_input_daily` 新字段补数：先按 500 只覆盖范围补 2025-01-02 ~ 2026-04-27。
2. 补 `/backtest` 页面最小版：配置区 + 结果总览 + 日级曲线表 + 个股明细。
3. 把候选构建逻辑从 `StockSelector` 中拆成公共模块。
4. 再考虑接入 `adj_factor / moneyflow / cyq_perf`。

## `/backtest` 页面最小版（2026-05-06 16:22）

已新增回测中心页面最小版：

- 页面：`app/api/web/pages/backtest.html`
- JS：`app/api/web/js/backtest.js`
- 路由：`GET /backtest`
- 导航：已加入首页、选股中心、跟踪复盘、策略管理、数据状态、个股详情页侧边栏

页面当前包含：

1. 回测配置区
   - 策略：当前固定 `lowvol_reversal`
   - 开始日期 / 结束日期
   - 收益口径：`1d` / `3d`
   - 每日最大入选数量
   - 分数底线
2. 历史输入层状态
   - 覆盖日期
   - 覆盖股票数
   - 覆盖行数
   - 关键字段覆盖率
3. 回测结果总览
   - 状态
   - 交易日
   - 交易数
   - 平均收益
   - 胜率
4. 日级结果表
5. 最近回测任务列表
6. 个股明细表

已完成验证：

- `node --check app/api/web/js/backtest.js` 通过
- `https://www.yzysstock.cloud/backtest` 返回 `200 OK`
- `https://www.yzysstock.cloud/static/js/backtest.js` 返回 `200 OK`
- 页面依赖接口均返回 `200`：
  - `/api/backtest/results`
  - `/api/backtest/runs?limit=5`
  - `/api/factor-input/status`
  - `/api/backtest/trades?run_id=...`

当前页面仍是 V2-P0 最小可用版，没有复杂图表；后续可补收益曲线图、按日期/代码筛选、回测参数持久化展示。

## 异步回测任务与进度列表（2026-05-06 16:56）

根据大X对回测交互的反馈，已将回测从“页面同步等待结果”升级为异步任务模型：

### 后端改动

- `backtest_run` 新增进度与核心收益字段：
  - `progress_total_days`
  - `progress_done_days`
  - `progress_pct`
  - `current_trade_date`
  - `estimated_seconds_left`
  - `total_return_pct`
  - `avg_return_pct`
  - `max_drawdown_pct`
  - `win_rate_pct`
- `POST /api/backtest/run` 默认行为改为：
  1. 校验历史输入层是否覆盖目标区间
  2. 创建 `queued` 状态的 `backtest_run`
  3. 立即返回 `run_id`
  4. 使用 FastAPI `BackgroundTasks` 后台执行逐日回测
- 后台执行过程中逐日写入：
  - `backtest_pick`
  - `backtest_trade`
  - `backtest_summary_daily`
  - `backtest_run` 进度字段
- 完成后计算并写入：
  - 总收益率：按每日组合收益复利计算
  - 平均收益率：按交易收益平均
  - 最大回撤：按权益曲线计算
  - 胜率
  - `summary_json.equity_curve`
- 如果回测区间没有 `factor_input_daily` 历史输入，会返回明确错误：`该区间缺少历史输入数据，请先补 factor_input_daily 后再回测`。

### 前端改动

- `/backtest` 页面提交回测后不再等待最终结果，而是显示“任务已创建”。
- 最近回测任务列表显示：
  - 状态
  - 进度 `done/total`
  - 预计剩余时间
  - 总收益率
  - 平均收益率
  - 最大回撤
  - 胜率
  - 交易数
- 存在 `queued/running` 任务时，页面每 4 秒轮询刷新列表和当前详情。
- 完成后的任务才允许点击“查看详情”。
- 顶部总览卡片新增：总收益率、最大回撤。

### 验证

公网 HTTPS 已验证：

- `POST /api/backtest/run` 立即返回 `queued` run：`backtest_lowvol_reversal_20260506_165608`
- 后台快速完成后：
  - `status=success`
  - `progress_done_days=2`
  - `progress_total_days=2`
  - `progress_pct=100`
  - `total_return_pct=-4.1414`
  - `avg_return_pct=-2.0886`
  - `max_drawdown_pct=-4.1414`
  - `win_rate_pct=16.6667`
  - `total_trades=6`
- `GET /api/backtest/runs?limit=3` 已返回进度与收益字段。
- `https://www.yzysstock.cloud/backtest` 返回 200，页面包含“总收益率 / 最大回撤”。

### 后续注意

当前异步执行使用 FastAPI `BackgroundTasks`，适合 V2-P0 轻量回测。后续如果单次回测时间明显变长，建议升级为独立 worker/队列，避免受 API 进程重启影响。

## 独立回测 worker / DB 队列（2026-05-06 17:00）

由于真实回测可能覆盖一两年数据，FastAPI `BackgroundTasks` 不适合作为长期执行容器，本轮已升级为独立 worker / 数据库队列模式。

### 架构调整

- `POST /api/backtest/run`：只负责创建 `backtest_run(status='queued')` 并立即返回 `run_id`。
- `stock-analysis-backtest-worker.service`：常驻 systemd 服务，每 3 秒轮询 queued 任务。
- Worker 使用数据库原子 claim：
  - 查询最早的 `queued` run
  - `UPDATE ... WHERE status='queued'` 抢占成功后执行
  - 执行中写入 `running`、进度、收益和完成状态

### 新增文件

- `app/backtest/worker.py`

### systemd 服务

- service：`stock-analysis-backtest-worker.service`
- 工作目录：`/root/.openclaw/workspace/stock-analysis`
- 启动命令：

```bash
/root/.openclaw/workspace/stock-analysis/.venv/bin/python -m app.backtest.worker --poll-seconds 3
```

常用命令：

```bash
systemctl status stock-analysis-backtest-worker.service
systemctl restart stock-analysis-backtest-worker.service
journalctl -u stock-analysis-backtest-worker.service -n 100 --no-pager
```

### 验证

已验证公网任务：

- 创建 run：`backtest_lowvol_reversal_20260506_165933`
- API 首次返回：`status=queued`
- worker 日志显示：
  - `claimed backtest run backtest_lowvol_reversal_20260506_165933`
  - `finished backtest run backtest_lowvol_reversal_20260506_165933`
- 结果：
  - `status=success`
  - `progress_done_days=2`
  - `progress_total_days=2`
  - `progress_pct=100`
  - `total_return_pct=-4.1414`
  - `avg_return_pct=-2.0886`
  - `max_drawdown_pct=-4.1414`
  - `win_rate_pct=16.6667`
  - `total_trades=6`

### 后续可增强

- 处理 API/worker 重启后长时间卡在 `running` 的任务恢复逻辑。
- 增加取消任务接口。
- 如果未来并发回测需要更高吞吐，可以支持多个 worker 实例和更明确的锁字段。

## 回测队列稳定性增强：取消 / 心跳 / 卡住恢复（2026-05-06 17:06）

在独立 worker / DB 队列基础上，继续补齐长回测稳定性能力。

### Schema 增强

`backtest_run` 新增：

- `worker_id`：当前消费任务的 worker 标识
- `locked_at`：任务被 worker 抢占时间
- `worker_heartbeat_at`：worker 最近心跳时间
- `cancel_requested`：取消请求标记

### Worker 增强

- Worker 启动时生成 `worker_id = hostname:pid`。
- claim queued run 时写入 `worker_id / locked_at / worker_heartbeat_at`。
- 每处理一个交易日，更新进度和 `worker_heartbeat_at`。
- 每轮轮询前调用 `recover_stale_running_runs()`：如果 running 任务心跳超过 30 分钟未更新，自动恢复为 queued，等待 worker 重新消费。
- 执行过程中每个交易日前检查 `cancel_requested`，如已取消则写入 `status='cancelled'` 并退出。

### API / 页面增强

新增取消接口：

```http
POST /api/backtest/runs/{run_id}/cancel
```

行为：

- queued 任务：立即改为 `cancelled`，写入 `finished_at`。
- running 任务：写入 `cancel_requested=1`，worker 在下一个交易日边界中断。

`/backtest` 页面最近任务列表中，`queued/running` 任务会显示“取消”按钮。

### 验证

已验证：

1. queued 取消：
   - 停止 worker 后创建 run `backtest_lowvol_reversal_20260506_170442`
   - 调用取消接口后状态变为 `cancelled`
   - `cancel_requested=true`
   - `estimated_seconds_left=0`
   - `finished_at` 正常写入
2. 正常 worker 执行：
   - run `backtest_lowvol_reversal_20260506_170407` 成功完成
   - `worker_id` 和 `worker_heartbeat_at` 正常写入
3. 卡住恢复：
   - 手动构造 stale running run `backtest_lowvol_reversal_20260506_170500`
   - worker `--once` 启动后日志显示 `recovered 1 stale running backtest run(s)`
   - 该任务随后被重新 claim 并成功完成
4. 页面与 JS：
   - `/backtest` 页面 200
   - JS 中已包含取消按钮逻辑
   - `/api/backtest/runs?limit=3` 返回 `worker_id / cancel_requested / progress_pct / finished_at`

### 当前策略

- 单 worker 串行执行，适合 V2-P0。
- 通过 `UPDATE ... WHERE status='queued'` 做轻量原子抢占，后续若启用多 worker，需要继续加强锁字段和超时策略。

## 回测中心支持 V13 / V12 基础版（2026-05-10 16:25）

本轮按顺序完成多策略回测接入：

1. 先打通 `v13_three_factor`
2. 再接入 `v12_legacy` 基础回测
3. 更新 `/backtest` 页面策略选择

### 后端改动

文件：`app/backtest/service.py`

- `_validate_request()` 从仅允许 `lowvol_reversal`，扩展为允许：
  - `lowvol_reversal`
  - `v13_three_factor`
  - `v12_legacy`
- 回测候选池 `_load_candidates()` 已升级为按历史交易日构造当时可见输入：
  - `factor_input_daily.turnover_rate / volume_ratio / total_mv / completeness_score`
  - `stock_basic.is_st`
  - 当日 `daily_kline.open / close / amount`
  - 截至当前回测日的 20 日窗口：`ma20 / close_20d / max_close_20 / min_close_20 / avg_amount_20 / kline_count_20`
  - V12 基础所需：`stock_sentiment_daily.sentiment_score / news_count`、`market_context_daily.market_strength / market_state`
- 20 日窗口查询限定 `daily_kline.trade_date <= 当前回测日`，避免未来函数。
- V12 回测基础版只使用已入库历史舆情，不在回测过程中实时调用 Tavily。

### 前端改动

文件：

- `app/api/web/pages/backtest.html`
- `app/api/web/js/backtest.js`

更新：

- 策略下拉新增：`三因子策略`、`多因子策略`
- 切换策略自动调整默认参数：
  - `lowvol_reversal`: threshold=60, max_picks=3
  - `v13_three_factor`: threshold=65, max_picks=3
  - `v12_legacy`: threshold=50, max_picks=3
- 页面提示 V12 基础回测使用已入库历史舆情，不实时调用 Tavily。
- JS 版本更新到 `backtest.js?v=20260510a`。

### 验证

已通过：

```bash
.venv/bin/python -m py_compile app/backtest/service.py app/api/routes/backtest.py
node --check app/api/web/js/backtest.js
```

直接 API smoke（`save=false`，2026-04-24~2026-04-27，2 个交易日）：

- `v13_three_factor`: `sample_days=2`, `total_picks=6`, `total_return_pct=-4.412`
- `v12_legacy`: `sample_days=2`, `total_picks=6`, `total_return_pct=1.5886`

队列/worker 验证（`save=true`）：

- `backtest_v13_three_factor_20260510_162452`：success，`total_trades=6`，`total_return_pct=-4.412`
- `backtest_v12_legacy_20260510_162452`：success，`total_trades=6`，`total_return_pct=1.5886`

公网页面验证：

- `https://www.yzysstock.cloud/backtest` 返回 200
- 页面加载 `backtest.js?v=20260510a`

### 后续注意

- V12 当前是基础历史舆情版，不等同于线上选股的实时 Tavily Top40 精排；若要严格复刻，需要先做 V12 历史 Top40 舆情回填任务。
- 当前回测仍是非复权价格；复权回测需接入 `adj_factor` 后再升级。

## 暂停开放 V12 回测（2026-05-10 16:29）

大X确认：V12 回测如果要严格复刻线上 Top40 舆情精排，会消耗大量 Tavily 搜索次数；因此先不开放 V12 回测。

调整：

- `BacktestService.SUPPORTED_STRATEGIES` 移除 `v12_legacy`，当前仅开放：
  - `lowvol_reversal`
  - `v13_three_factor`
- `/backtest` 页面策略下拉移除 V12 选项。
- 页面文案改为：V12 因 Tavily 搜索成本暂不开放回测。
- JS 版本更新到 `backtest.js?v=20260510b`。

说明：V12 选股中心仍可正常使用，并会在当前选股时按 Top40 流程补充 Tavily 舆情；本次只关闭 V12 的历史回测入口。

## 回测高级设置短期计划（2026-05-10 17:09）

当前 `/backtest` 的“高级设置”仍是占位入口。短期不展开复杂参数，先改为说明型提示，避免用户误解当前回测已经是完整资金组合回测。

建议短期文案：

> 当前为信号收益评估：不含手续费、滑点、复权和真实资金占用。高级资金模型后续开放。

当前收益率口径：

- `1d`：买入日开盘价买入，下一交易日开盘价卖出。
- `3d`：买入日开盘价买入，第三个后续交易日收盘价卖出。

后续高级设置候选项：

- 交易成本：手续费、印花税、滑点。
- 成交规则：开盘买/收盘买，次日开盘卖/收盘卖，停牌/涨跌停无法成交处理。
- 价格口径：不复权、前复权、后复权。
- 资金模型：信号收益评估、等权组合、固定资金投入、满仓滚动持仓、最大同时持仓数。
- 过滤条件：排除 ST、最低成交额、最低/最高价格、行业过滤。

## 2026-05-10 - lowvol_reversal_v2 开发计划

DeepSeek v4 pro 完整审查确认旧 `lowvol_reversal` 存在“伪因子 + 大量同分 + 股票代码排序锁死”问题。当前计划保留策略入口，升级实现为真实行情因子版本：

- `turnover`: 使用真实换手率/流动性横截面百分位。
- `lowvol`: 使用 20 日振幅或后续收益率标准差，波动越低分越高。
- `reversal`: 使用 20 日收益率取反，跌幅越深反转分越高。
- 每日候选池内做 percentile rank，输出 0~100 因子分。
- 加过滤：ST、`kline_count_20 < 15`、`avg_amount_20` 太低、`close <= 1`。
- 加 tie-breaker 与同分诊断，避免回测被 `ORDER BY sb.code` 污染。
- 验证口径：跑 2026-01-01~2026-05-10 回测，唯一股票数、得分分布、同分池规模必须明显改善。
