# 股票分析系统架构审计与整改计划（2026-07-15）

> 状态：**已完成并归档为执行台账（2026-07-18）**。批次 A～E、DQ1～DQ5、冻结策略诊断与第一阶段 Tracking 性能治理均已部署验收；本文保留逐批证据和历史决策，不再承载当前待办。当前运行基线、版本路线与剩余整改队列统一见 [`CURRENT_VERSION_PLAN.md`](./CURRENT_VERSION_PLAN.md)。
>
> 本文保留 `docs/IMPROVEMENT_PLAN.md` 中“个人纪律型投研工作台、模块化单体、异步任务驱动、不做自动交易、不急拆微服务”的产品与架构定位，只更新当前整改优先级。
>
> 具体目录边界、依赖规则、任务状态机和迁移落点见 `docs/target_code_architecture_2026-07-15.md`；工作区原有未提交改动继续保留。

## 1. 本轮结论

当前系统已经是一个可运行的个人投研研究平台，但还不能定义为“生产级策略系统”。

更准确的现状是：

```text
单机模块化单体
  + FastAPI / 静态多页 Web
  + 共享 MySQL 数据总线
  + cron 数据同步任务
  + 独立 backtest / selection / portfolio advice worker
```

当前最重要的不是继续增加策略数量或页面功能，而是解决以下四类基础问题：

1. 页面/API 宣称可用，但数据和策略并不支持，例如 ETF、指数选股。
2. 回测工程链路可运行，但存在前视偏差、非 point-in-time 基本面和幸存者偏差。
3. 三类长任务、worker 租约、readiness 与任务保留周期已统一；分钟行情和舆情也已具备明确分层、保留期与收盘后清理任务。
4. schema 入口和五个核心 Repository 边界已经收敛；下一阶段的主要结构风险转为脚本与应用层的反向依赖、旧入口和失联原型。

整改总原则：

```text
先保证“不误导、不错算、不丢任务”
再优化“性能、结构和新标的支持”
```

## 2. 当前真实架构

```text
浏览器
  -> Nginx HTTPS
    -> 单进程 FastAPI
       |- Dashboard / System / Stocks
       |- Strategies / Selection / Tracking
       |- Portfolio / AI Advice
       |- Backtest API
       `- MySQL 直连查询与写入

MySQL portfolio_advice_run queue
  -> stock-analysis-portfolio-worker.service
  -> DeepSeek -> 结构化建议与输入快照

MySQL selection_run queue
  -> stock-analysis-selection-worker.service
  -> StrategyService -> StockSelector
  -> preview result_json（用户按条保存到 selection_result）

MySQL backtest_run queue
  -> stock-analysis-backtest-worker.service
  -> BacktestService / StockSelector
  -> backtest_pick / trade / summary

root crontab（当前约 25 条）
  -> AkShare / Tushare / Tavily / DeepSeek / NewsNow 等
  -> 日线、因子、资金流、筹码、舆情、实时快照、任务日志
```

当前优点：

- 主业务已经基本迁入 `app/`，比旧 `src/versions` 主线清晰。
- 策略协议、YAML 注册、动态加载和统一候选评分已真实落地。
- 回测已有独立 worker、任务 claim、取消、心跳和 stale recovery。
- 选股已有独立 worker、active 幂等、任务 claim、取消、心跳、重试和 stale recovery。
- 持仓建议已有独立 worker、单持仓 active 幂等、任务 claim、取消、心跳、重试和 stale recovery。
- 核心数据表普遍有唯一键和幂等 upsert。
- Web 和 API 同源，单机部署成本低，符合个人投研工具定位。

当前边界问题：

- Router、Service、SQL、外部调用和指标计算仍高度混合。
- `PortfolioService`、`StockSelector`、Dashboard、BacktestService 已成为大文件。
- 每次 `mysql_conn()` 都建立新物理连接，没有连接池或 repository 层。
- 策略可用性分散在 YAML、两个静态白名单、Backtest 白名单和未实际使用的 DB registry。
- `app` 仍反向导入 `src/` 和 `scripts/` 中的业务函数。
- Schema 初始化散落在 API 请求、Service 构造器和不同脚本中。

## 3. 功能与策略可用性基线

### 3.1 功能矩阵

| 功能 | 当前状态 | 整改前口径 |
| --- | --- | --- |
| 首页、个股、市场概览 | 基本可用 | 继续使用，但补数据新鲜度和真实健康检查 |
| A股选股页面/API | 部分可用 | 仅代表链路能运行，不代表策略有效 |
| ETF选股 | 不可用 | 必须禁用或明确标记未支持，不能继续 success + 0 |
| 指数选股 | 不可用 | 必须禁用或明确标记未支持 |
| ETF持仓行情 | 部分可用 | 仅覆盖当前持仓 ETF，不等于 ETF 全市场选股 |
| 跟踪复盘 | 部分可用 | 保留股票真实结果，删除市场快照假兜底 |
| 策略详情页 | 展示可用 | readiness 改为动态数据事实，不再用静态绿灯 |
| 股票回测 | 研究态 | 工程链可用，现有结果暂不可作为交易证据 |
| ETF回测 | 不可用 | 前后端均应明确拒绝 |
| 持仓 AI 建议 | 工程链可恢复 | 已迁出 API；模型建议仍只作为人工决策辅助 |
| 系统状态 | 部分可用 | 必须覆盖全部实际任务和数据新鲜度 |
| 选股 CLI | 不可用 | 修复或删除，不能继续作为文档入口 |
| 自动化测试 / CI | 不可用 | 先补高风险链路的最小回归测试 |

### 3.2 策略矩阵

| 策略 | 当前判断 | 真实运行证据 |
| --- | --- | --- |
| `a_share_sentiment` | 股票链路部分可用，策略仍为实验态 | 有持续选股记录；截至 7/15 的 58 条纳入统计样本均值约 -3.98%、胜率约 31.03% |
| `lowvol_reversal` | 高风险实验态 | 少量选股/回测；关键低波特征样本异常少且不新鲜 |
| `v13_three_factor` | 研究态 | 少量选股、8 次回测；CLI 失败，代表回测明显为负 |
| `v12_legacy` | 遗留兼容 | 有历史结果；当前回测已禁用 |
| `fund_chip_repair` | 原型 | 有 CI 数据，无真实选股/回测记录 |
| `quality_lowvol` | 原型 | 有 CI 数据，无真实运行记录 |
| `leader_tactics` | 原型 | 有 CI 数据，无真实运行记录 |
| `low_position_resonance` | 原型 | 有 CI 数据，无真实运行记录 |
| `multi_timeframe_resonance` | 原型 | 有 CI 数据，无真实运行记录 |
| `chan_structure_watch` | 原型 | 无选股记录，回测禁用 |
| `limitup_reversal` | 原型 | 无 CI、无选股、无回测 |

整改前注册表实际为 10 个 `experimental`、1 个 `legacy`，API 却把 11 个策略全部标记为 `runtime_ready`。B1 已改为分别返回：

- 代码可加载。
- 当前标的类型兼容。
- 数据覆盖满足要求。
- 最近运行成功。
- 策略通过回测/样本外验证。

这五者不再合并成一个“可执行”。当前实测为 11 条可加载、7 条数据就绪、4 条实时可执行、2 条研究回测可执行、0 条通过交易有效性验证。

### 3.3 ETF / 指数数据事实

截至 2026-07-15：

- `stock_basic` 中有 1474 个 ETF。
- 只有 4 个 ETF 代码有任何 `daily_kline`，共约 317 行。
- ETF 的 `factor_input_daily` 为 0。
- 没有 ETF 专用策略、CI 或完整回测链。
- 唯一 ETF selection run 为 `success`，但 `result_count=0`。
- ETF 结果查询在没有真实结果时会错误退化为市场快照，并硬编码为 lowvol 策略。
- 当前 ETF 行情任务只更新活跃持仓 ETF，不是全市场数据链。

指数情况类似：417 个指数仅约 18 个有日线，因子和实时行情均不足。

### 3.4 回测可信度

旧口径回测至少存在三类方法论问题：

1. 使用 T 日收盘、资金流、筹码、舆情等信息选股，却假设在同一 T 日开盘成交。
2. 历史因子曾使用当前基本面快照回填历史日期，没有严格按公告日 point-in-time。
3. 历史股票池使用当前退市状态过滤，存在幸存者偏差。

因此旧结果继续保持：

- 当前回测页面和历史记录保留作研究与排错。
- 在口径修复、回归测试和重新跑数前，不得用于宣传策略有效或决定策略上线。
- 代表性旧结果为负，只能说明执行链能运行，不能证明策略有效。

B2 后新建任务统一使用 `close_signal_next_open_v2`：T 日收盘后形成信号、T+1 开盘成交；不再把 T 日收盘信息用于 T 日开盘。对 lowvol/v13 的历史回测明确排除没有公告日保障的基本面字段，并保存数据截止日、策略配置 hash 与方法论快照。历史 ST/退市主数据仍不完整，因此新结果也继续是 `research_only + validation_pending`，不能直接升级为策略有效性证据。

2026-07-17 的 DQ3 已在 B2 之上补齐独立的 point-in-time 生命周期、名称/ST 区间、停复牌分区和历史退市行情层，新任务方法论升级为 `close_signal_next_open_pit_universe_v3`。回测区间内 101 只历史退市股票的行情/因子缺口已归零；仍有 `sh.689009` 无上游历史名称记录。

同日 DQ4 又补齐按公告日和修订版本生效的基本面真相层，回测方法论升级为 `close_signal_next_open_pit_fundamentals_v4`。2022 年报至 2026 中报落库 100,423 个公告版本，12 个代表交易日 as-of 覆盖 100%；旧历史快照污染不再进入候选。

随后 DQ5 建立四个核心宽基指数的月度历史成分真相层，显式指数回测按信号日读取最近快照并 fail-closed，默认全 A 口径保持不变。数据真相主线已完成；样本外验证仍未完成，因此 `research_only + validation_pending + unvalidated` 的结论不变。

### 3.5 任务与存储基线

- `stock_realtime_snapshot`：约 5592 行，约 3.7 MB。
- `stock_realtime_intraday`：单日约 127 万行，约 418 MB。
- `stock_realtime_moneyflow_intraday`：约 16 万行，约 55 MB。
- `task_run_log`：约 8.3 万行，约 283 MB，存在 35 条长期 `running`。
- `sector_opinion_daily`：约 12.9 万行，但约 4.49 GB，存在严重 JSON 重复。
- 本地日志目录约 379 MB，未发现统一 logrotate。
- `stock_popularity_update` 当前因上游返回结构变化持续失败，但系统状态页未显式展示。

实时行情、板块资金、实时资金流、同花顺热点、热度五个脚本的 MySQL advisory lock 生命周期错误，不能真正防止 cron 重叠执行。

## 4. 整改目标架构

近期继续采用单机模块化单体，不引入微服务，也不急着增加 Redis/Celery。

```text
Nginx
  |- HTTPS
  |- 认证 / 限流
  `- FastAPI
      |- Query API -> Application Service -> Repository -> MySQL
      `- Command API -> job_run -> 立即返回 job_id

独立 Worker
  |- selection
  |- backtest
  `- portfolio_advice

Scheduler / cron
  -> 统一 job registry / lock / task status
  -> ingestion services
  -> snapshot / raw / rollup / daily tables

Strategy Registry（唯一真相源）
  |- supported_instrument_types
  |- required_datasets
  |- data_freshness
  |- runtime_status
  |- backtest_status
  `- validation_status
```

## 5. 按优先级排列的整改计划

### P0：停止误导与立即风险收敛

目标：先让系统诚实、安全、不会重复执行或继续产出不可解释结果。

#### P0-1 禁用未支持的 ETF / 指数选股与回测

改动：

- 选股页禁用 ETF、指数按钮，显示“数据链建设中”。
- 选股 API 对 ETF/指数返回 `422 unsupported_instrument`。
- 回测 API 同样拒绝未支持的 instrument type。
- 在候选加载、Tavily、DeepSeek 调用之前执行兼容性与数据覆盖预检。
- 0 候选且原因是无数据时标记为 `no_data`，不能继续记为普通 success。

验收：

- ETF/指数请求不会产生外部 API/AI 调用。
- 不再出现 `success + 0` 的假成功。
- 前端、API、任务记录给出一致的不可用原因。

#### P0-2 删除选股结果的假兜底

改动：

- `selection/results` 无持久化结果时返回明确空列表和 `no_history`。
- 市场快照如需展示，使用独立字段/接口，不伪装为策略结果。
- 删除 fallback 中硬编码 lowvol 策略的逻辑。
- 修复 latest run 跨 instrument type、策略详情漏传 instrument type 等问题。

验收：

- 没有真实 `selection_result` 时页面明确显示“暂无历史结果”。
- 返回的每条选股结果都有真实 run、strategy 和 instrument type。

#### P0-3 建立策略 capability 与动态 readiness

YAML 作为唯一策略能力真相源，至少增加：

```yaml
supported_instrument_types: [stock]
required_datasets: [daily_kline, factor_input_daily]
minimum_coverage: 0.95
maximum_data_age_days: 1
runtime_status: enabled
backtest_status: disabled
validation_status: unvalidated
```

运行前检查：

- 标的类型。
- 数据最新日期。
- 有效候选样本量。
- 必需特征覆盖率。
- 最近成功运行与错误原因。

验收：

- “代码可加载”和“生产可用”分开显示。
- lowvol 特征恢复前不得显示 ready。
- 原型策略默认不进入普通运行入口。

#### P0-4 回测风险隔离

立即措施：

- 页面和 API 显示 `research_only` / `validation_pending`。
- 历史结果增加“当前口径存在前视偏差，不可作为交易证据”提示。
- 禁止把回测成功状态直接映射成策略 ready。

口径修复：

- 统一为 T-1 收盘前已知数据生成信号，T 日开盘成交；或明确采用 T 日收盘信号、T+1 开盘成交。
- 基本面按公告日期 point-in-time。
- 历史股票池按当时上市、退市和 ST 状态构建。
- 增加“修改未来数据不得改变过去选股”的回归测试。

验收：

- 任一选股日只能读取信号时点之前的数据。
- 回测结果保存 data cutoff、策略配置 hash 和交易口径版本。
- 修复后全部旧结果标记为旧口径，重新运行基准回测。

#### P0-5 修复高频任务锁、热度失败和状态漏报

改动：

- 将六个高频脚本统一改为持久连接 advisory lock，或在 cron 外使用 `flock -n`。
- 修复百度/AkShare 热度返回结构变化，源不可用时明确 degraded/failed。
- 系统状态由统一任务注册表生成，不再维护不完整的硬编码列表。
- 清理/归档长期 `running`，新增任务超时回收。
- 增加重复运行、锁竞争和上游结构变化烟测。

验收：

- 同一高频任务不能并发执行两份。
- 系统页能看到全部约 25 条任务、最近状态、耗时、错误和新鲜度。
- `stock_popularity_update` 不再每 5 分钟静默失败。

#### P0-6 公网保护、证书和可复现部署

改动：

- 对页面/写接口/AI/回测增加 Nginx 或应用层认证。
- 对回测、选股和 AI 接口增加限流。
- 2026-08-03 前续签并验证 `yzysstock.cloud` 证书。
- 把 systemd、Nginx、cron 模板和缺失的直接依赖纳入仓库。
- 将线上已依赖但仍未跟踪的选股任务文件纳入版本管理。

验收：

- 未认证访问者不能修改持仓/复盘或触发付费/重型任务。
- 空环境可按文档完成依赖安装、数据库迁移、服务启动和健康检查。

### P1：建立真正可恢复的任务系统

目标：把长任务从 API 进程迁出，统一队列语义。

#### P1-1 复用 backtest worker 模式

先支持三类 job：

- `selection`
- `backtest`
- `portfolio_advice`

任务至少包含：

- `job_type`
- `payload_json`
- `idempotency_key`
- `status`
- `worker_id`
- `locked_at`
- `heartbeat_at`
- `cancel_requested`
- `attempt_count`
- `max_attempts`
- `progress_stage`
- `progress_current / progress_total`
- `error_code / error_message`

原则：

- API 只创建任务并立即返回。
- worker 原子 claim。
- 支持取消、重试、心跳和 stale recovery。
- 相同策略、标的、日期和参数使用幂等键防重复提交。
- 进度按真实阶段更新，不再按耗时制造假进度。

单机阶段继续使用 MySQL 队列，不引入 Redis/Celery。

#### P1-2 修复 worker 数据库连接稳定性

- 排查 backtest worker 日志中的 PyMySQL packet sequence 异常。
- 禁止跨线程共享同一 PyMySQL connection/cursor。
- 为长任务设置连接超时、断线重连和阶段性短事务。
- 评估引入轻量连接池，但不把连接池作为任务可靠性的替代品。

#### P1-3 统一任务状态与健康检查

- `/api/health` 保留轻量存活检查。
- 新增 readiness：DB、worker 心跳、关键任务、数据最新日期。
- 系统页直接消费统一 job/task registry。
- 配置任务日志和结构化错误保留周期。

验收：

- 重启 API 不影响已排队或执行中的 selection/advice。
- worker 异常退出后任务可恢复或明确失败。
- 用户可取消 queued/running 任务。

### P2：实时、舆情与日志数据生命周期治理

目标：保留产品需要的数据价值，减少全市场分钟写入、DELETE 和 JSON 重复。

#### P2-1 实时行情分层

建议默认口径：

| 层 | 内容 | 建议保留 |
| --- | --- | --- |
| current snapshot | 每个代码最新行情 | 长期仅保留最新一行 |
| full-market 1m raw | 全市场分钟原始快照 | 2 个交易日 |
| 5m / 15m rollup | OHLCV 聚合 | 60-90 个交易日 |
| tracked 1m | 持仓、选股、观察池分钟数据 | 可按产品需求延长 |
| daily kline | 日线主数据 | 长期 |

改动：

- snapshot 增加 batch id、received_at、freshness/stale 标记。
- 1m 表按 `trade_date` 分区；用分区删除替代每分钟范围 DELETE。
- 日终聚合 5m/15m，再执行原始数据 retention。
- 如果确认不需要全市场历史 1m，可只持久化持仓/跟踪/观察池，市场全量只保存 current snapshot。

#### P2-2 舆情数据去重

- `sector_opinion_daily` 的 top news/source 改为 raw/news ID 引用。
- 盘中快照只短期保留。
- 每个交易日生成一份 EOD summary 供长期复盘。
- 历史新闻正文和行业汇总解耦，避免每个 15 分钟快照复制整段 JSON。

#### P2-3 日志与任务表治理

建议默认口径：

- `task_run_log` 明细保留 90 天。
- 每日/每任务汇总长期保留。
- 应用日志保留 14-30 天并压缩轮转。
- 错误日志按 error code 聚合，避免同一上游错误每 5 分钟无限堆积。

验收：

- 分钟表不会因每日 DELETE 长期膨胀。
- `sector_opinion_daily` 不再重复存储大段相同 JSON。
- 系统能够说明每张高增长表的用途、保留期限和清理责任人/任务。

### P3：迁移、模块边界和自动化回归

目标：在不大重写的前提下，降低耦合并保证干净部署。

#### P3-1 唯一 schema migration 入口

- 合并散落的 core、V2 和各模块 schema。
- 部署阶段执行 migrate。
- 移除 GET 接口、Service 构造器和普通查询路径中的 DDL/seed。
- API 数据库账户逐步降为 DML 权限。
- 增加空数据库 migration + startup smoke test。

#### P3-2 按垂直切片抽 repository

推荐顺序：

1. `PortfolioRepository`：先解决列表 N+1 和 Service 过大。（E2 已完成）
2. `TrackingRepository`：避免分页同时重新加载全量结果。（E3 已完成）
3. `DashboardRepository`：统一市场快照查询和缓存。（E4 已完成）
4. `SelectionRepository` / `BacktestRepository`：复用任务与结果查询。（E5/E6 均已完成）

原则：

- 保留现有 SQL，先移动边界，不同时重写业务规则。
- 一个垂直切片一个提交、一个验证闭环。

#### P3-3 清理反向依赖和旧入口

- 把 `scripts` 中可复用同步逻辑迁入 `app/data_ingestion`。（已完成本轮扫描发现的全部生产反向依赖）
- 脚本只保留 CLI 参数和任务启动。（ETF/日更舆情/策略舆情三个相关入口已完成）
- 迁移或归档 `app -> src` 依赖。（已完成，静态边界测试锁定为 0）
- 修复或删除失效选股 CLI。（已改为只提交 selection worker 任务；Selector 直接执行明确拒绝）
- 归档旧 `/static/index.html` 和失联 ETF grid 原型。（已完成，legacy import 兼容保留）

#### P3-4 最小测试体系

第一批必须覆盖：

- strategy registry/capability。
- ETF/指数明确拒绝。
- selection 无历史结果不伪造。
- selection/advice 队列 claim、取消、超时恢复。
- backtest 防未来数据。
- schema 空库初始化。
- 高频任务防重入。

以上最小测试均已有对应回归；真实空数据库双执行 smoke 工具已完成，但仍等待数据库侧提供独立测试库，不能拿生产库模拟。

### P4：真正支持 ETF（独立项目）

只有在大X确认 ETF 选股是近期主目标后才启动。

ETF 不能直接复用股票的 PE、ROE、筹码和个股舆情策略，应单独建设：

1. ETF universe、上市/退市、基金类型和跟踪指数。
2. 全量 ETF 日线、复权、成交额、规模和流动性。
3. 资产类别、行业/主题、境内/跨境、商品/债券分类。
4. 折溢价、跟踪误差、AUM、换手、成交额、动量和波动因子。
5. ETF 专用策略、CI、回测、结果解释和 readiness。
6. 前端 ETF 策略入口与风险说明。

在这条链完整前，ETF 只保留持仓行情与人工分析能力。

## 6. 推荐交付批次

### 批次 A：真实性与安全护栏

包含：

- P0-1 ETF/指数禁用和 422。
- P0-2 删除假兜底。
- P0-4 回测 research-only 提示。
- P0-5 高频锁、热度失败、系统状态漏报。
- P0-6 认证/限流和证书安排。

完成标准：页面不再宣称不真实的能力，公网重型/写接口得到保护，主要任务状态可信。

### 批次 B：策略 readiness 与回测口径

包含：

- P0-3 capability/readiness。
- P0-4 point-in-time 和交易时点修复。
- 对 lowvol、v13、a_share_sentiment 重建验证基线。

完成标准：系统能解释每个策略为什么可运行、不可运行或只适合研究。

### 批次 C：任务队列统一

包含：

- selection worker。
- portfolio advice worker。
- worker DB 稳定性。
- 统一任务状态、取消、恢复和幂等。

完成标准：API 重启不再丢长任务。

### 批次 D：数据生命周期

包含：

- 实时 1m / 5m / 15m 分层。
- 舆情 JSON 去重。
- task/log retention 和 logrotate。

完成标准：高增长数据有明确用途、保留期和清理任务。

### 批次 E：模块边界和可复现部署

包含：

- 唯一 migrate。
- Repository 垂直拆分。
- 依赖与部署模板。
- 自动化 smoke/regression tests。

完成标准：空环境可重建，关键语义不会被后续修改悄悄回退。

### 批次 F：ETF 正式支持（可选）

只有在前五批稳定或大X明确把 ETF 提升为近期主目标后启动。

## 7. 实施与变更控制

每个批次遵守：

1. 先记录当前接口、数据库和服务基线。
2. 不覆盖工作区原有未提交改动。
3. 一个主题一个 diff，先审查再重启。
4. 修改接口就打接口，修改页面就验证页面，修改任务就做重启/恢复测试。
5. 生产数据迁移先备份、支持 dry-run，并提供回滚办法。
6. 每批结束更新本文、`docs/IMPROVEMENT_PLAN.md` 当前优先级和长期记忆。

## 8. 已确认的默认决策（2026-07-15）

### 决策 1：ETF 近期定位

- 已确认：立即禁用 ETF/指数选股与回测，只保留 ETF 持仓行情；后续把 ETF 支持作为独立项目。
- 备选：把 ETF 正式数据/策略链提前到 P1，但会明显延后队列、回测口径和数据治理。

### 决策 2：公网访问保护范围

- 已确认默认口径：除 `/api/health` 外，整个股票站使用 Basic Auth；回测、AI、选股再叠加限流。实施前需要确认登录凭据和切换时间。
- 备选：页面公开只读，只保护写接口和重型计算接口。
- 备选：股票站只允许 Tailscale/IP allowlist 访问。

### 决策 3：全市场分钟历史需求

- 已确认：全市场 1m 原始保留 2 个交易日，生成 5m/15m 长期汇总；持仓/跟踪股 1m 可延长。
- 备选：全市场只保存 current snapshot，1m 仅保存持仓/跟踪/观察池。
- 不推荐：全市场 1m 原始长期保留，除非确认有明确回测用途并接受存储成本。

## 9. 当前执行基线

当前执行口径为：

1. ETF/指数立即禁用，ETF 持仓行情保留。
2. 股票站整体 Basic Auth，健康检查例外。
3. 回测保留页面和历史数据，但明确 research-only；修复口径后再恢复策略验证功能。
4. 全市场 1m 保留 2 个交易日，5m/15m 保留 90 个交易日，持仓/跟踪股分钟历史可延长。
5. 单机继续使用 MySQL 队列，不引入 Redis/Celery，不拆微服务。

## 10. 实施进度

### 2026-07-15：A1 真实性护栏已完成

已落地：

- 新增统一 instrument policy，selection 与 backtest 目前只接受 `stock`。
- 选股 Route、任务提交 Service、策略 Service 和回测 Service 均有预检，ETF/指数不会进入候选加载或外部 AI/API 调用。
- ETF/指数选股与回测返回结构化 `422 unsupported_instrument`。
- 选股页禁用 ETF/指数入口并明确“数据链建设中”；ETF 持仓行情链保持不变。
- 删除选股结果对市场快照的假兜底；无真实结果返回 `no_history + []`。
- latest selection run 查询按 instrument type 隔离。
- 回测 API、历史任务与页面统一显示 `research_only`、`validation_pending`、旧方法论版本和风险说明。
- 新增 `tests/test_p0_architecture_guards.py`，8 个轻量回归测试全部通过。
- 本机和公网 HTTPS 烟测通过；API、backtest worker、Nginx 均正常。

本切片没有改表、没有删除历史数据，也没有触发全市场选股或回测。

后续批次：

1. P0-6 Basic Auth/限流与部署模板已在 A2.2 完成。
2. 证书续签仍须在 2026-08-03 到期前闭环。

### 2026-07-15：A2.1 高频任务与状态可信度已完成

已落地：

- 新增共享持久连接 MySQL advisory lock；实时快照、市场资金流、个股实时资金流、同花顺概念热度、个股热度、ETF 持仓行情六条高频链路统一复用。
- 修复原实现获取锁后立即关闭连接、锁实际已释放的问题；真实 MySQL 双连接竞争和释放后重获验证通过。
- 个股热度在百度源返回 `403` 时自动降级至东方财富单请求榜单，任务明确记录 `partial_success`、实际来源和上游错误，不再静默失败。
- 修复 `stock_popularity_snapshot` 只 upsert 不清理导致旧榜单长期残留的问题；同事务清理退出榜单的代码，分钟历史仍保留。
- 系统状态改为统一 24 项任务注册表；展示已登记数、有状态数、来源、降级原因，并将超过一小时仍为 `running` 的记录暴露为 `stale`。
- ETF 持仓行情和股票基础资料同步补齐 `task_run_log` 记录，便于后续统一观测。
- 新增锁、热度源、快照替换和任务注册表测试；连同 A1 回归共 19 项全部通过。

线上验证：

- API 本机与公网 `/api/system/status` 均返回 `status=ok`、已登记任务 24 项。
- 热度任务以东方财富降级成功，当前快照 100 条且来源唯一；一次性清理历史残留 486 条。
- 六把 advisory lock 均已释放，六条原 cron 调度未改动并继续有效。
- 本切片没有新增表或改动历史分钟数据，也没有触发全市场选股或回测。

### 2026-07-15：A2.2 公网保护与部署模板已完成

已落地：

- HTTPS 站点除 `/api/health` 外统一启用 Nginx Basic Auth；用户名为 `dax`，随机密码只保存在 root-only 文件，仓库和文档均不含明文或哈希。
- 选股、回测、DeepSeek 深度复盘、持仓建议刷新/评估使用按 IP 限流：每分钟 6 次、突发 2 次，超限返回 429。
- 健康检查保持免认证，便于 systemd、Nginx 和外部监控继续探活。
- 新增 `deploy/nginx/` 与 `deploy/systemd/` 无密钥现网模板，并补充依赖、安装、验证与回滚说明。
- `scripts/setup_kline_cron.sh` 补齐现网股票任务、保留其他系统 cron，并新增 `--print-only` 审查模式；生成结果与现网逐行集合一致且无重复。
- 上线前已备份原 Nginx 配置；配置通过 `/usr/sbin/nginx -t` 后平滑 reload，Nginx 主进程未重启。

线上验证：

- 未认证健康检查返回 200；未认证首页和受保护 API 返回 401，并带 `Basic realm="Stock Analysis"`。
- 正确凭据访问首页与受保护 API 返回 200。
- 使用 ETF 预检请求做无副作用限流烟测：前三次返回业务 422，第四、五次返回 Nginx 429，没有创建选股任务或调用外部服务。
- 两份 Nginx 模板和两份 systemd 模板与现网文件一致；19 项回归测试继续全部通过。

剩余 A2 工作只有证书续签。当前 TrustAsia 证书于 `2026-08-03 23:59:59 GMT` 到期，续签需要新的证书材料或另行确认切换到 ACME/Let's Encrypt；大X已确认本轮暂不处理，继续后续架构整改。

### 2026-07-15：B1 策略 capability/readiness 已完成

已落地：

- `strategies.yaml` 成为策略能力唯一真相源，11 条策略均声明支持标的、必需数据集、覆盖率、新鲜度、实时状态、回测状态、验证状态与运行证据。
- 新增动态 capability 服务，分别计算 `loadable`、`instrument_compatible`、`data_ready`、`runtime_ready`、`backtest_ready` 与 `validated`；未知数据集或缺少声明默认关闭，不再乐观放行。
- 最新数据覆盖以 A 股最新交易日为基准，避免周末/休市误报；状态快照缓存 60 秒，首次真实读取由约 12 秒优化到约 0.68 秒。
- 当前线上真实口径：11 条可加载、7 条数据就绪、4 条实时可执行（lowvol、v13、v12、A股舆情）、2 条仅允许研究回测（lowvol、v13）、0 条通过交易有效性验证。
- 7 条无真实运行证据的策略统一标为 `prototype`，不再出现在选股运行入口；selection 在写入任务前预检并返回 422，避免生成注定失败的 queued 任务。
- BacktestService 删除静态七策略白名单，改为读取注册表能力；回测页动态只展示两条 research-only 策略，原型策略在创建 run 前返回 400。
- 策略页删除伪造的固定时间绿灯，改为展示真实数据集日期、覆盖率、实时/回测/验证状态及阻塞原因。
- 新增 5 项 capability/预检测试；连同已有护栏共 24 项轻量回归全部通过。

线上验证：

- `/api/strategies` 返回 `11 / 7 / 4 / 2 / 0` 五层状态；策略页和回测页新静态资源均已生效。
- 未认证保持 401、认证访问为 200；原型选股返回 422、原型回测返回 400，均未创建任务。
- API 与 backtest worker 重启后 active，未触发全市场选股、回测或外部 AI/API 调用。

B 批次下一步是 B2：修正回测信号时点、point-in-time 基本面、历史股票池与 methodology/data cutoff/config hash。当前所有回测仍保持 `research_only + unvalidated`。

### 2026-07-16：B2 回测时点与页面性能优化已完成

回测口径已落地：

- 新建任务统一使用 `close_signal_next_open_v2`：T 日收盘后形成信号，首个后续交易日开盘成交，不再用 T 日收盘信息模拟 T 日开盘买入。
- `1d` 持有期在入场后的下一交易日开盘退出；`3d` 持有期在含入场日的第三个持有交易日收盘退出。前端持有期标签统一从真实 `entry_date` 起算。
- 旧任务继续保留 `legacy_pre_point_in_time_v1`，接口按任务记录展示方法论，不会因代码升级被伪装成新口径。
- lowvol/v13 历史候选明确排除缺少公告日保障的 PE/PB、ROE/ROA、利润、营收增长、EPS 等当前基本面快照；同时不再用当前 `is_st`、`is_delisted` 状态过滤历史日期，并增加上市日期不晚于信号日的约束。
- `backtest_run` 新增 `methodology_version`、`data_cutoff_date`、`strategy_config_hash` 和 `methodology_json`，新任务可以追溯数据截止日、策略配置与交易语义。
- 历史 ST、退市和成分变更主数据仍不完整，幸存者偏差尚未彻底消除；因此所有新回测继续标记为 `research_only + validation_pending + unvalidated`。

页面性能已同步优化：

- 首页、跟踪复盘和回测任务列表增加面向页面的 compact 投影，不再传输页面没有使用的完整因子、舆情上下文、交易计划原始结构和权益曲线。
- 首页 compact JSON 从约 143 KB 降到约 21.8 KB；跟踪复盘从约 404 KB 降到约 8.9 KB；20 条回测任务列表从约 118 KB 降到约 28.5 KB。
- 首页 compact 响应增加 30 秒进程内缓存；跟踪汇总增加 60 秒带数量校验的缓存，保存、删除和统计状态变更会主动失效。
- Nginx 已为 JSON、JavaScript、CSS、XML 和 SVG 启用 gzip，并为 `/static/` 增加 1 小时浏览器缓存。公网实测首页 JSON gzip 后约 6.1 KB，首页 JS 约 6.8 KB，CSS 约 28.6 KB。
- 首次查询仍受 MySQL 聚合影响，但重复首页请求已降到毫秒级；跟踪页冷请求约 1.2 秒，避免了原先同一请求内重复构建全量跟踪记录。

验证结果：

- 新增回测方法论和页面 compact 契约测试，连同既有护栏共 33 项轻量回归全部通过。
- Python 编译、前端 JavaScript 语法、Nginx 配置和公网 Basic Auth/gzip 烟测均通过。
- 本切片只迁移方法论元数据列，没有运行全市场回测；历史结果仍按旧口径保留。

B 批次下一步为 B3：受控重建 lowvol/v13 新口径基线并对照旧结果。重建前仍需限定日期和样本规模，避免在 2 vCPU / 3.6 GiB 主机上制造资源尖峰。

### 2026-07-16：B3 受控验证基线已完成

基线工具与隔离规则：

- 新增 `scripts/run_backtest_validation_baseline.py`，默认只做 dry-run；只有显式 `--execute` 才提交任务。
- 仅允许 lowvol/v13，限制 1d/3d、最多 20 个交易日和最多 10 只入选；执行前检查回测队列、可用内存和 Swap。
- 两个策略严格串行，通过持久连接 MySQL advisory lock 防止重复基线脚本并发；单任务超时会请求取消。
- 新任务使用 `is_system_test=1 + validation_baseline_id` 隔离，默认回测列表和“最新正式结果”不会展示或选中这些记录。
- 报告会检查日期窗口、请求参数、策略版本与配置 hash，分为严格方法论对比、同版本但配置不可验证、仅方向参考三档；样本少于 20 个交易日固定标记 `engineering_baseline_only + statistical_validation=false`。
- `backtest_run` 新增 `validation_baseline_id` 及组合索引，报告可从数据库随时重新生成，不依赖一次性的终端输出。

本次受控运行：

- 基线 ID：`b3_20260716_20260424_20260427_1d_v2`。
- 窗口：`2026-04-24～2026-04-27`，共 2 个交易日；每策略 3 只、6 笔交易，仅用于与现有同窗口旧任务做工程比较。
- lowvol 新口径总收益 `+1.2606%`，旧任务 `-2.0904%`；但策略版本从 `v1` 变为 `v2.1-risk-filtered` 且旧任务无配置 hash，因此只能标记 `directional_only`。
- v13 新口径总收益 `+0.4114%`，旧任务 `-4.4120%`；策略版本相同，但旧任务无配置 hash，因此标记 `directional_same_version_unverifiable_config`。
- 上述收益差异不能当作策略提升证据；两天样本远不足以评价胜率、回撤或交易有效性，策略 readiness 仍为 `research_only + unvalidated`。

执行中暴露并修复一处 worker 韧性问题：

- 第一个任务提交时，常驻 worker 尚未重启，仍加载旧的请求数据类；领取新字段后退出，systemd 自动拉起新进程。
- 沿用 stale recovery 将同一任务放回队列并成功完成，没有重复插入基线任务。
- 请求反序列化已移入单任务失败边界，worker 循环增加兜底异常捕获；以后单条不兼容任务会失败并记录，不再带崩整个 worker。

验证结果：

- 40 项轻量回归、Python 编译与 diff 检查通过。
- 默认公网回测列表不含系统测试；显式 `include_system_tests=true` 可查到两条成功新口径记录；最新正式结果仍为旧任务且明确显示旧方法论。
- API 与 backtest worker 已加载最新代码并保持 active；完成后可用内存约 2.4 GiB、Swap 0。

批次 B 到此完成的是“可信工程链路”，不是策略有效性认证。下一步进入批次 C：先抽取共享 MySQL job 状态操作，再把 selection 从 FastAPI 进程内后台任务迁到独立 worker。

### 2026-07-16：C1 selection 独立 worker 已完成

任务生命周期已落地：

- 新增 `app/jobs/mysql_state.py`，抽取短事务的原子 claim、worker ownership、heartbeat、queued/running cancel、stale recovery 与重试耗尽分流；动态表名/字段名只允许受控标识符。
- `selection_run` 在保留原表和历史记录的前提下，增量补齐 `idempotency_key / active_idempotency_key / idempotency_date`、worker 锁与心跳、取消、attempt/max_attempts 和稳定 `error_code`，没有迁移到高风险统一大表。
- active 幂等键使用唯一索引保证并发双击只有一个活跃任务；key 覆盖 job type、最新数据交易日和所有影响结果的请求参数。任务进入终态后释放 active key，允许用户重新运行。
- `POST /api/selection/run` 只做标的/策略 readiness 预检并写入 queued，返回 HTTP 202；`async_run=false` 被明确拒绝，不再保留绕过 worker 的同步重计算入口。
- 新增 `stock-analysis-selection-worker.service`，API 重启不影响 queued/running 任务；worker 单条 payload 解析、业务执行和兜底异常都有独立失败边界。
- 选股任务固定只生成 preview，不直接批量保存跟踪结果；用户继续通过页面按条保存。因此运行中取消不会在任务完成边界后误标 success，queued 可立即取消，running 在当前同步计算边界协作取消。
- 进度不再按墙钟时间伪造多个阶段；worker 只写真实的 queued / strategy calculation / terminal 阶段并独立更新心跳。
- 页面增加当前选股任务取消入口，支持 queued/running 状态与 cancelled 文案。

验证结果：

- 49 项轻量回归、Python 编译、JavaScript 语法、systemd unit 校验与 diff 检查通过。
- 使用无外部调用的坏 payload 验证 worker 原子 claim 后仅将该任务标成 `failed + invalid_request`，进程不中断；验证 active 双提交复用同一 run、queued 取消和测试记录清理，最终 active 队列为 0。
- 公网健康检查 200、未认证选股页 401、认证页面/API/静态资源 200、ETF 和同步选股入口 422；API、selection worker、backtest worker 均 active 且 `NRestarts=0`。
- 迁移首次使用了当前数据库不支持的 `ADD COLUMN ... BEFORE`，在服务切换前被验证拦下；改用兼容的 `AFTER result_json` 后幂等重跑成功，所有列/索引齐全且 20 条历史 success 记录未改变。

C1 完成的是 selection 的可恢复执行闭环；当时批次 C 的下一步是复用同一状态操作迁移 `portfolio_advice_run`。

### 2026-07-16：C2 portfolio advice 独立 worker 已完成

持仓 AI 建议不再依赖 FastAPI 进程内 `BackgroundTasks`：

- `portfolio_advice_run` 在保留 6 条历史成功建议的前提下增量补齐 idempotency、worker ownership、锁/心跳、取消、attempt/max_attempts、真实阶段/进度和稳定 `error_code`；没有搬表或改写历史结果。
- `POST /api/portfolio/{position_id}/advice/refresh` 只固化提交时输入快照并写入 queued；同一持仓同时最多一个活跃任务，并发双击复用同一 run，终态后释放 active key。
- 新增 `stock-analysis-portfolio-worker.service`，worker 从 MySQL 原子 claim 后使用已持久化快照调用 DeepSeek；API 重启不影响 queued/running 任务，也不会在等待期间占用 API worker。
- queued 可立即取消；running 记录取消请求，在当前外部 AI 调用边界结束后进入 cancelled，取消后不会落成 succeeded。持仓被修改或删除时也会让对应活跃建议任务进入取消流程。
- 新增 advice run 查询和取消接口；页面按 run 轮询真实状态、显示生成/取消状态，并在终态后刷新缓存建议。
- `ensure_portfolio_schema()` 改为进程内只执行一次，避免持仓页面每次 API 调用重复执行 DDL/schema 检查。

验证结果：

- 55 项轻量回归、Python 编译、JavaScript 语法和 systemd unit 校验通过。
- 无外部调用的坏快照被原子 claim 后只标记为 `failed + invalid_request`，worker 未退出；重复提交复用同一 run，queued 取消后 active key 释放，所有烟测记录均已清理。
- 本机 health、持仓 API 和新任务查询接口分别返回 200/200/404（不存在任务）；公网 health 200、未认证持仓页 401、认证页面/API 200，新任务查询不存在记录返回 404。
- API、portfolio advice worker、selection worker 和 backtest worker 均 active；portfolio advice worker `NRestarts=0`，active advice 队列为 0，部署验证没有调用 DeepSeek。

C2 完成后三类重任务都已迁出 FastAPI。批次 C 剩余 C3：统一 worker/readiness 健康检查、任务状态展示和任务/结构化错误保留周期。

### 2026-07-16：C3 worker/readiness 与任务保留治理已完成

运行健康不再依赖“最近是否恰好有任务执行”来猜测：

- 新增 `worker_runtime_heartbeat` 进程租约。backtest、selection、portfolio advice worker 在空闲和运行状态都会每 10 秒续租；`45s` 未续租才判为 stale，当前任务 ID 与最近任务起止时间可追踪。
- 新增轻量 `GET /api/readiness`。它检查 MySQL、三类 worker、队列积压/失联任务、三项关键日更和关键数据最新日期；`not_ready` 返回 503，`/api/health` 继续保持不访问数据库的轻量 liveness。
- 系统页新增 Worker/队列健康和结构化错误区，直接消费同一 readiness；删除了原先并不存在的“连接池 18/50”静态文案。
- 三类队列按真实 stale 边界展示：backtest 30 分钟、selection 15 分钟、portfolio advice 5 分钟；进程租约与任务心跳分开，不再把“空闲”误判成“worker 已死”。

backtest 也补齐到共享任务契约：

- `backtest_run` 增量增加 active idempotency、attempt/max_attempts、phase 和稳定 `error_code`；原子 claim、取消、stale recovery 与重试耗尽复用 `MySQLJobStateRepository`。
- 同参数活跃回测只保留一个任务；终态释放 active key。`save=false` 同步回测入口返回 422，API 不再保留绕过 worker 的长计算路径。
- 无外部调用的坏 payload 实际烟测被 worker claim 后得到 `attempt_count=1 + failed + invalid_request`，active key 正常释放，worker 继续存活；测试任务和测试错误汇总随后清理。

任务与错误保留口径已落库并定时执行：

- `task_run_log` 明细保留 90 天，最新一条任务状态始终保留；`task_run_daily_summary` 长期保存每日/任务/状态计数。
- 错误消息脱敏、归一化 fingerprint 后进入 `job_error_daily_summary`，按日期、任务和 error code 聚合，默认保留 365 天，避免同一上游错误每几分钟堆一整条详情。
- selection 任务壳保留 90 天；正式回测、验证基线、已保存选股结果、持仓建议摘要与 outcome 明确保留。仅非基线 system test 可在 90 天后删除；AI 原始响应 30 天后清理，输入快照 90 天后清理。
- `scripts/run_job_retention.py` 默认 dry-run，只有 `--apply` 修改数据；每天 04:15 串行运行。首次受控执行收口 33 条超过 24 小时的历史 abandoned task、聚合 5,805 次历史错误为 140 个日级分组，并只清理了预览中的 2 条过期 AI 原始响应。
- 仓库日志接入系统 `logrotate.timer`：按日轮转 14 份、压缩、单文件 50 MiB 提前轮转。

执行中首次历史错误聚合触发 MySQL `GROUP_CONCAT` 1260；由于各阶段均为可重跑短事务，尚未进入删除阶段。随后改为有界代表消息聚合并幂等重跑成功，问题已记录到 `.learnings/ERRORS.md`。同轮还修复了 `strategy_factor_ci_daily_update` 对已移除静态 `RUNTIME_READY_IDS` 的引用，改为读取动态 capability，实测返回当前四个 runtime-ready 策略。

验证结果：

- 63 项回归、Python 编译、JavaScript 语法、shell 语法、systemd unit 与 `git diff --check` 通过。
- 本机/公网 `/api/health` 200；公网未认证 readiness 401、认证 readiness 200 且为 `ready`；系统页及新版 JS 200。
- API 与三个 worker 均 active、`NRestarts=0`；队列 0、失联任务 0，日线和 factor input 最新交易日均为 `2026-07-15`。
- 部署后可用内存约 2.1 GiB，Swap 约 0.5 MiB，没有资源持续上升。

批次 C 到此完成。下一批次是 D：先处理全市场 1m 数据分层/rollup，再做舆情 JSON 去重；日志轮转与通用 task retention 已在 C3 提前完成。

### 2026-07-16：D1 全市场分钟行情分层已部署

分钟行情已从“每分钟写全量、热路径顺手 DELETE”改为明确的冷热分层：

- `stock_realtime_snapshot` 继续只保留每只代码最新一行，并新增 `batch_id / received_at / freshness_seconds / is_stale`，页面可区分抓取时间和源行情是否陈旧。
- `stock_realtime_intraday` 已通过 shadow copy + 原子改名迁为按交易日分区的全市场 1m 原始层；默认只保留最近 2 个交易日。迁移前后均为 367,231 行，旧表保留为 `stock_realtime_intraday_legacy_20260716124449` 供回滚。
- 新增 5m/15m OHLCV 聚合表，默认保留 90 个交易日；新增持仓、已保存选股和跟踪标的 1m 层，默认保留 90 个交易日。
- `scripts/run_realtime_lifecycle.py` 默认 dry-run，工作日 15:20 才以 `--apply` 生成 rollup、复制 tracked raw 并按分区做 retention。只有 5m/15m manifest 都成功且源会话覆盖到 14:55 后，才允许删除当天以前的过期 raw 分区。
- 新增 `GET /api/stocks/{code}/realtime-rollups?interval=5|15`；系统页展示 raw/rollup/tracked 占用、分区状态和每个 interval 的 manifest。

现场迁移后全市场 raw 从 information_schema 旧估算约 413 MiB 收敛为约 125 MiB，午间受控聚合生成 82,728 条 5m、38,524 条 15m 和 8,460 条 tracked raw；因午间数据只到 11:34，manifest 正确标为 `partial`，没有误删 raw。15:20 的收盘任务负责把当天 manifest 转为完整状态。

执行中发现一个 09:43 启动的 AkShare 调用卡在 TCP 读取超过 3 小时，且连接失效后 MySQL advisory lock 已随连接消失。现已给每次 `stock_zh_a_spot` 调用增加默认 50 秒硬超时，清理陈旧进程并把原任务记为 `killed + upstream_timeout`；后续每分钟任务可继续运行。AkShare 的 tqdm 进度默认也已静默，避免每分钟把 70 段进度条写进日志；发现时 58 MiB 噪声日志已按 logrotate 策略轮转。

### 2026-07-16：D2 舆情 JSON 去重已切换新写入

现场审计确认 `sector_opinion_daily` 约 14.5 万行、46 个交易日，却分配约 4.49 GiB；最近 2,000 行的 `top_stocks_json` 平均约 25.9 KiB，且股票对象继续嵌套新闻正文，单纯按整段 JSON hash 去重收益很低。

本轮采用“父快照 + 关系化明细 + raw news 引用”的兼容迁移：

- 新增 `sector_opinion_stock / sector_opinion_news_ref / sector_opinion_source_ref`，股票明细不再嵌入 `matched_news`，新闻只保存 `stock_news.raw_id` 引用及该快照自己的衰减指标。
- 父表新增 `payload_version / payload_migrated_at`。V2 writer 先写子表，再把父表三段 JSON 置空并标记版本 2；旧读路径由 repository hydration 还原成原响应结构，Router/策略代码无需同时重写产品语义。
- 13:15 现网任务已成功写入 122 条纯 V2 父快照；抽取一条历史快照迁移后，股票代码/条数、新闻 raw ID 和来源均与迁移前一致。
- 生命周期口径为：最近 5 个交易日保留盘中全部快照，更早日期每个交易日只保留最后一批 EOD 快照，长期最多 90 个交易日。
- `scripts/run_market_opinion_lifecycle.py` 默认 dry-run；工作日 16:05 后台串行执行。它先分批归一化所有保留快照，只有校验全部 `payload_version=2` 且父 JSON 为空后才允许裁剪旧快照；中断可幂等续跑，不会出现“迁了一半先删源数据”。

最新 dry-run 为 145,097 条父记录：保留 20,552、可裁剪 124,545、待归一化 20,429。逻辑删除/置空会停止后续 4 GiB 级重复增长并释放 InnoDB 内部页，但不会立即缩小表空间文件；物理空间回收留给后续独立维护窗口，不在盘中执行大表 rebuild。

本轮全量回归增至 72 项（其中首轮 71 项、进度静默补丁增加 1 项），Python 编译、JavaScript/shell 语法、`git diff --check` 和本机接口烟测通过；API 与三个 worker 均 active，readiness 为 `ready`，可用内存约 2.1 GiB、Swap 约 0.5 MiB。D 批次的代码、在线新写入和调度已落地；历史舆情初次迁移由 16:05 后台任务继续完成。

### 2026-07-16：E1 唯一 migration 入口与运行时 DDL 清理已部署

数据库结构变更现统一由 `python -m app.orchestration.migrate` 管理：

- 新增 `schema_migration`，登记 16 个有序版本、名称、checksum、running/success/failed、执行耗时和结果；默认命令只输出 plan，`--apply` 才执行，`--check` 有 pending/checksum mismatch 时非零退出。
- 整轮 migration 使用持久 MySQL advisory lock；每个 step 保留现有幂等 schema 函数作为内部 runner，失败可重跑。现库首次登记 16/16 用时约 1 秒，第二次执行 `applied_now=0`。
- API、PortfolioService、StockSelector、sentiment refresh、各数据 ingestion 和 cron writer 已移除 schema ensure/CREATE/ALTER；普通请求和同步任务现在只做 DML。旧独立 schema 模块不再提供 `__main__` 执行入口，`init_project.py` 直接提示改用统一 migrate。
- `stock_basic` 过去散落在 fundamental/valuation/basic sync 中的列升级已回收到 core migration；lowvol 特征缓存表也从计算脚本移到内部 schema step。
- API、backtest/selection/portfolio worker 的 systemd unit 都增加 `ExecStartPre ... migrate --check`；cron 安装模式也先 check。readiness 和系统页展示 migration target/applied/pending。

首次上线后发现 `0001` checksum 错把 Python `runner.__module__` 当稳定身份：CLI `python -m` 下是 `__main__`，API import 下是包路径，导致 readiness 误报一个 pending。已固定 core runner 的 canonical identity、增加回归并记录 `.learnings/ERRORS.md`；CLI 与 import 两条路径现在都返回 16/16 ready。

新增强护栏空库 smoke：只允许名称精确为 `stock_migration_smoke` 或以 `stock_migration_smoke_` 开头、连接库与参数完全一致且首次零表的独立数据库，执行两遍 migration 并要求第二遍零变更。数据库侧 provision 后已完成真实空库验收：首次应用 16 个 migration、生成 61 张表，第二遍 `applied_now=0`，最终 16/16 ready；生产库迁移快照哈希在执行前后保持一致。测试库保留，工具不会自动清表或删库。

验证结果：79 项回归、编译、JavaScript/shell、systemd unit、diff 检查通过；四个 unit 的 ExecStartPre 实际退出码均为 0，API 与三个 worker active、`NRestarts=0`，readiness `schema_migrations=16/16 + ready`。E1 完成后，E 批次下一步按垂直切片抽 PortfolioRepository，再处理 Tracking/Dashboard。

### 2026-07-16：E2 PortfolioRepository 垂直切片已部署

`PortfolioService` 原来同时承载产品规则、AI 建议编排和约 18 处直接 MySQL 访问；其中持仓列表按 `1 + 6N + 2` 查询加载持仓、行情、日线、舆情、资金流、筹码、AI 建议和建议结果。E2 保留全部产品与 AI 规则，只移动持久化边界：

- 新增 `app/portfolio/repository.py`，统一持仓、建议任务、建议结果和市场上下文 SQL；`PortfolioService` 不再导入或调用 `mysql_conn`。
- 市场上下文按全部持仓代码一次批量加载，日线使用 MySQL 8 `ROW_NUMBER() OVER (PARTITION BY code ...)` 每只取最近 120 条；持仓列表总查询数由随 N 增长改为固定 9 条。
- ETF 历史兜底、技术指标、支撑压力、纪律规则、AI 缓存失效、任务取消与结果评分均留在 Service，未改变产品语义。
- 新增 repository 批量查询数、批量调用和 Service 无直连数据库的回归测试。真实数据库影子校验中，新旧链路对同一行情快照逐叶差异为 0。

验证结果：全量回归 84 项通过；线上 `/api/portfolio` 连续 3 次返回 200、2 个现有持仓，耗时约 40-49ms；API 与 portfolio worker 的 migration `ExecStartPre` 均退出 0，服务 active、`NRestarts=0`，公网 health 200。下一垂直切片进入 `TrackingRepository`。

### 2026-07-16：E3 TrackingRepository 与分页查询收敛已部署

整改前 `GET /api/tracking?limit=10` 虽然只展示 10 条，却在冷缓存时先富化全部 176 条记录，再在 Python 切页；缓存命中后，富化 SQL 的日线/分钟极值子查询仍会为整张 selection_result 计算。现场基线为冷请求约 7.77 秒、缓存分页约 5.49 秒。

- 新增 `app/tracking/repository.py`，统一 tracking 列表、计数、run/filter、统计开关和删除 SQL；route 与 `SelectionResultTracker` 均不再直连 MySQL或保存 SQL。
- 富化查询先在 `target_selection` CTE 中按 run/date/strategy/latest、limit/offset 圈定目标 ID，日线和 raw/tracked 分钟极值只对本页 ID 计算。
- 页面记录始终按 limit/offset 单独读取；全局 filtered summary 只富化 `include_in_stats=1` 的 61 条记录，不再读取 115 条明确排除样本。60 秒缓存继续保留，产品统计口径不变。
- latest-only count 从“先查 runs、再递归 count”收敛为单条 SQL；删除由预查 + 删除两次往返收敛为一次 DELETE rowcount。route/Tracker 的业务映射、交易计划冻结和 AI 复盘输入未改。

验证结果：run/date/strategy/latest 四条真实库分支均通过；filters 与旧响应完全一致，页 1/页 2 的代码顺序、分页和字段树一致，行情值差异仅来自实时更新。全量回归 88 项通过。上线后冷请求约 0.313 秒、缓存页约 0.049 秒、第 2 页约 0.050 秒，相比基线约快 25 倍/112 倍；Dashboard 和 selection results 继续 200。API active、`NRestarts=0`、migration check 退出 0，公网 health 200、tracking 未认证仍 401。下一垂直切片进入 `DashboardRepository`。

### 2026-07-16：E4 DashboardRepository 已部署

新增 `app/dashboard/repository.py`，将首页市场概览、热点主题和短线情绪榜三块 SQL 全部从 route 收口到 Repository；`app/api/routes/dashboard.py` 只保留字段投影、评分、标签、榜单排序和 30 秒页面缓存，不再导入 `mysql_conn` 或保存 SQL。

审计发现市场概览固定 7 条、热点主题固定 3 条，但情绪榜会对涨停/反包候选逐只查询日线，单次达到 66 条 SQL。现先批量读取候选，再用一个 window query 按 code 各取最近 9 根 K 线，同时批量读取分钟开板记录；情绪榜固定为 7 条，整个 Dashboard Repository 固定 17 条，旧 route 总计约 76 条。

旧线上进程与新磁盘代码在同一行情窗口完成两轮影子验证：情绪榜逐叶差异 0，完整 Dashboard 逐叶差异也为 0。情绪榜单模块约从 0.69 秒降到 0.34 秒；完整页的主要收益是数据库往返由约 76 降到 17，业务输出和缓存语义保持不变。

最终 readiness 验收先发现一处跨标的误报：`daily_kline` 的 2026-07-16 起初只有 2 条 ETF `tencent_quote`，不能用全表 `MAX(trade_date)` 判断股票因子层落后。健康检查已改为取最近 45 日内达到股票池 95% 覆盖的完整日线日期，同时保留 `latest_available` / `is_partial` 字段；查询约 11ms。

15:10 收盘回填随后写入 5,522 条 2026-07-16 股票日线，此时因子层落后一天变成真实状态；现场只读验证 Tushare 当日 `daily_basic` 尚为 0 行，不能强写空因子。为闭合依赖，factor input 新增交易日 18:30 补跑，03:20 保留兜底；任务先按日期预取并要求至少 80% 源覆盖，未发布日期跳过并记 `partial_success`。同一任务的 Tushare 调用由“5 日 × 12 批≈60 次”降为“每日期一次≈5 次”。

验证结果：全量回归 97 项通过；上线后 Dashboard compact 冷请求约 0.54 秒、缓存约 0.003 秒、完整响应约 0.43 秒。API 与三个 worker active、`NRestarts=0`、migration 16/16，三类队列均为 0；当前 readiness=`degraded` 但 `accepting_jobs=true`，唯一原因是当日上游待发布，18:30 将自动补跑。系统状态、readiness、health 和公网 health 均 200。下一垂直切片进入 `SelectionRepository / BacktestRepository`。

### 2026-07-16：E5 SelectionRepository 已部署

新增 `app/stock_selection/repository.py`，把选股结果元数据、候选池、舆情上下文、结果保存以及 selection run 的创建/查询/阶段完成 SQL，从 route、`StockSelector` 和 `SelectionRunService` 收口到 Repository。三者均不再导入 `mysql_conn`、不再直接执行 SQL；选股规则、候选映射、评分解释、任务状态机和用户按条保存语义仍留在原业务层。

为避免边界移动顺手改口径，部署前冻结三组基线：selection results、selection runs，以及 `lowvol_reversal + 2026-07-16 15:05 + limit=10` 的候选包。新磁盘代码与旧线上进程在真实 ASGI 序列化下逐字段一致，三组最终 SHA-256 均相同；候选代码顺序和全部诊断字段未变化。Repository 对动态日期运算符和市场板块只接受白名单，并新增候选 SQL、保存去重事务及“业务层不得直连 MySQL”的回归。

验证结果：全量回归增至 102 项，编译、migration 16/16、diff 检查通过；上线窗口三类队列为 0，可用内存约 2.2 GiB。selection worker 与 API 串行重启后均 active、`NRestarts=0`，本地 health/readiness/results/runs 均 200，selection worker healthy/idle、队列 0；公网 health 200、未认证 selection 401。readiness 仍为 `degraded + accepting_jobs=true`，唯一原因仍是当日 factor input 等待 18:30 上游补跑。下一垂直切片进入 `BacktestRepository`。

### 2026-07-16：D2 历史舆情生命周期已完整收口

16:05 首次后台生命周期任务已归一化 16,875 条保留快照，但在一个同时含“已完成”和“待处理”记录的混合批次中，UPDATE 错误复用了原批次占位符，触发 `TypeError: not enough arguments for format string`。异常发生在裁剪前，因此该失败任务没有删除历史快照。

现已改为按 SELECT 实际返回的 `normalized_ids` 生成 UPDATE 占位符，并增加混合批次回归。恢复任务保持幂等，继续归一化剩余 3,554 条后先完成保留集校验，再删除 124,545 条冗余快照。最终保留 21,772 条、46 个交易日；`normalized_rows=21772`、`legacy_json_rows=0`、`pending_normalization_rows=0`、`prunable_rows=0`。任务日志状态为 success，D2 不再有后台历史迁移尾项。

### 2026-07-16：E6 BacktestRepository 已部署

新增 `app/backtest/repository.py`，将回测 run/result/curve/trade/factor status 读取、候选与历史窗口加载、run 创建/进度/结果/完成写入，以及验证基线查询，从 route、`BacktestService` 和 `validation_baseline` 收口到 Repository。三层均不再导入 `mysql_conn` 或保存 SQL；研究披露、能力预检、策略执行、收益计算、分页字段映射和验证结论仍留在原业务边界。

部署前冻结一个正式历史 run 的四组 API 响应、2026-04-24 的 5,201 条候选及 2026-04-24/27 两个交易日。旧线上进程与新磁盘代码逐字段一致；部署后 results/runs/trades/factor-status 四个 SHA-256 再次全部命中冻结值。没有提交新回测任务，也没有制造正式或系统测试数据。

验证结果：全量回归增至 109 项，编译、migration 16/16、diff 检查通过；上线前后三类队列均为 0。backtest worker 与 API 串行重启后 active、`NRestarts=0`，三个 worker 均 healthy/idle；公网 health 200、未认证 backtest 401。readiness 仍为 `degraded + accepting_jobs=true`，唯一原因是 2026-07-16 日线已完整而 factor input 等待 18:30 上游补跑。E2-E6 的五个核心 Repository 垂直切片至此完成，下一步进入 P3-3：清理反向依赖和旧入口。

### 2026-07-16：P3-3 反向依赖与旧入口清理已部署

静态扫描确认生产 `app` 只有两条真实反向依赖：`PortfolioService` 动态导入 ETF 同步脚本，以及选股舆情链同时导入 `scripts` 持久化函数和 `src` 新闻模块。现已完成以下收敛：

- ETF 抓取、日线/快照写入和任务逻辑迁入 `app/data_ingestion/portfolio_etf_quote_sync.py`；原脚本只保留启动器，Service 改为显式 app 内依赖。顺带修复 Service 调用 `save_snapshot` 未传 batch ID 导致 best-effort 快照静默失败的问题，缺省 ID 现在由同步模块生成。
- 新闻 provider、可信度、质量过滤迁入 `app/data_ingestion`；`save_news` / `save_daily` 与确定性本地情绪打分收口到 `sentiment_sync.py`；日更舆情和策略候选舆情任务迁入 app job，两个脚本退化为薄启动器。`src` 只保留 legacy compatibility import，生产 app 不再修改 `sys.path` 或依赖 `src/scripts`。
- `app.stock_selection.run_selection` 从 demo 特征直跑改为只提交可恢复 selection worker 任务；`selector.py` 直接执行明确退出，不能再绕过队列同步扫全市场。
- 未被路由引用的 `app/api/web/index.html` 迁到 `archive/legacy_web_index.html`；ETF grid 原型迁到 `archive/legacy_grid_trader.py`，旧研究版本通过薄兼容导入继续可读。

迁移前后 ETF 归一化、新闻来源/日期/可信度/质量和本地情绪打分冻结包 SHA-256 完全一致；旧线上 Portfolio 与新磁盘代码逐叶差异为 0，部署后哈希继续命中。新增 6 项依赖边界/CLI/归档回归，全量回归增至 115 项。selection/portfolio worker 与 API 串行重启后，API 和三个 worker 均 active、`NRestarts=0`，三类队列为 0，公网 health 200、未认证 portfolio 401。P3-3 完成；P3-4 的七类最低回归也已覆盖。随后独立测试库已完成真实空库 smoke，首次 16 个 migration 全部应用、第二遍零变更。

16:53 再次只读探测到 Tushare 2026-07-16 `daily_basic` 已发布 5,524 行、覆盖 99.69%，随后通过独立后台任务只补最新交易日：12 批写入 `factor_input_daily` 5,541 行，无不可用日期，任务日志 success。最终 `/api/readiness` 从 degraded 恢复为 `ready + accepting_jobs=true`，三个 worker healthy/idle、三类队列为 0。真实空库 smoke 随后也已完成。至此本轮代码内架构整改、空库重建和当日数据新鲜度验收均完成；剩余项仅为需要独立维护窗口的舆情物理表空间回收，以及按原决定暂缓的证书续签。

### 2026-07-16：P2-2 舆情存储维护判定已收口

生命周期完成后的真实数据为 21,772 条父快照，21,772 条均为 V2，三个 legacy JSON 字段非空行与字节数均为 0。数据库为腾讯云 CynosDB MySQL 8.0.30，`@@innodb_file_per_table=0`；统计刷新前 `information_schema.tables` 仍把父表估成 132,670 行/约 4.71 GB，新闻关系表则把真实 942,047 行低估为 32,343 行，说明原“4.49 GB 单表占用”主要混入了严重过期的 engine statistics，不能直接当作物理文件大小。

因此取消原先“直接 `OPTIMIZE TABLE` 或 shadow rebuild 回收单表物理文件”的设想：共享表空间释放页已经可以被实例内其他 InnoDB 数据复用，但单表重建不能缩小共享物理文件，还会增加临时空间、I/O 和元数据锁风险。新增 `scripts/inspect_market_opinion_storage.py`，默认只读输出实际行数、engine 统计、表空间模式和维护判定；显式 `--analyze-statistics` 也只刷新四张舆情表的优化器统计，并复用舆情 advisory lock，脚本没有 `OPTIMIZE` 或 rebuild 入口。

18:56 在收盘后无舆情任务运行时，通过新工具显式执行一次 `--analyze-statistics`，四表均返回 `status / OK`，没有执行 `OPTIMIZE` 或 rebuild。刷新后父表估算为 21,945 行/约 160.6 MB，新闻关系表估算为 935,759 行/约 311.2 MB，已与真实数量同一量级；共享 `DATA_FREE` 约 10.17 GB，只能解释为实例级可复用页，不能归因到单表或直接等同云端账单空间。维护任务 `market_opinion_storage_20260716_185628` 已记 success。新增 3 项维护判定回归后全量 118 项通过，migration 16/16、readiness ready、三 worker healthy/idle、三队列 0，服务 `NRestarts=0`；本切片没有重启 API/worker。

若未来云端存储账单/配额仍需要真实下降，只能在数据库管理侧确认 CynosDB 的物理回收语义，并另开 provider-approved 的实例迁移或全库重建窗口；不扩大应用账号权限，也不由业务服务器自行执行。UI 设计评估已转入 `IMPROVEMENT_PLAN.md` 的后续独立阶段，不属于本轮架构整改。

### 2026-07-17：整改后 DQ1 核心数据质量闭环已落地

架构主线完成后进入数据可信度阶段。新增 `app/data_quality` 垂直切片和 `scripts/run_data_quality_audit.py`：Repository 只读取股票主数据、最新日线/因子、状态快照和未来日期的有界切片，Service 统一输出 11 条 `pass/warn/fail` 规则；任务结果复用 `task_run_log.metadata_json`，`/api/system/status` 和数据状态页只读最近快照，不增加在线大表扫描。

缺口不再只报一个覆盖率：停牌/暂停上市、当日新股和待处理源缺口分别统计，PE 缺失不再作为因子硬故障。同步修复 `StockBasicSync` 长期只读取 `list_status=L` 导致退市旧行留在有效池的问题：额外读取 D 集合但只标记库内已有代码，同时将行业 `NaN` 归一化为 NULL。13 只历史退市旧行退出有效池后，有效股票由 5,542 降为 5,529；日线缺口由 20 降为 7，因子市场字段缺口由 20 降为 7，最终审计为 `8 pass / 3 warn / 0 fail`。剩余告警保留真实样本，不写猜测值。详细记录见 `docs/data_quality_audit_2026-07-17.md`。

验收结果：全量 125 项、Python/JavaScript/shell/diff 检查通过，migration 16/16；两条质量 cron 已安装且无重复。API 串行重启后 active、`NRestarts=0`，本地 health/readiness/system status 与公网 health 均 200，readiness `ready / accepting_jobs=true`；三个 worker 未重启且继续 active。

同日继续完成 DQ2 来源缺口追溯：离线审计对每类最多 10 个样本回看最近 60 个交易日，补充连续缺失交易日、最后成功来源/时间，以及 `task_run_log` 中最近一次相关上游尝试。系统页直接展示这份持久化快照，未增加在线大表扫描、schema 或运行时 DDL；持续性分类暂不改变 DQ1 的 11 条严重度规则。真实样本已区分 `sh.689009` 的 5 日持续日线缺口、`bj.920685` 的 2 日日线缺口和 `bj.920081` 的 5 日因子市场字段缺口，而对应上游任务整体仍为成功。

DQ2 全量回归增至 127 项，migration 仍为 16/16 ready；真实 `data_quality_audit` 快照已持久化并由系统状态页读取。API 串行重启后本地/公网 health 200、`NRestarts=0`，三个 worker 健康空闲且队列为 0。收盘后的 readiness 暂为 degraded 仅因 7 月 17 日因子输入等待既定 18:30 日更，仍允许接收任务。

### 2026-07-17：DQ3 point-in-time 历史股票池已落地

新增 migration `0017`，建立 `stock_instrument_lifecycle`、`stock_name_history`、`stock_suspension_daily` 与 `stock_status_pit_manifest`。Tushare 生命周期共落地 5,866 只；回测区间相关 5,629 只中名称/ST 区间覆盖 5,628 只，唯一缺口 `sh.689009` 经逐股接口复核仍为 0 条。停复牌区间改为分页抓取，2024-01-02 至 2026-07-16 的 613/613 个交易日全部成功，共 9,951 条事件。

历史股票池基线原有 101 只区间内退市股票，仅 14 只有行情/因子覆盖。后台回填后 98 只有日线与 `daily_basic`，另 3 只由两个接口同时确认区间内无市场活动，DQ3 待补缺口归零。历史行情 upsert 只更新市场列并保留已有基本面；开发过程中发现的 8,007 条存量基本面空写已按原 `stock_basic_snapshot` 口径恢复 7,967 条，剩余 40 条原本没有主数据，未猜值。最终字段级复核显示关键市场字段 `24,763 / 25,181` 行可用（98.34%），并将 95% 覆盖门槛写入 DQ3，避免“有行无值”被误判为 ready。

BacktestRepository 现在按信号日连接生命周期和历史名称区间，退市前保留真实候选、退市日退出，且未知 ST 不回退当前状态。方法论版本升级为 `close_signal_next_open_pit_universe_v3`。真实 SQL 验证退市前/退市日边界符合预期；最终 DQ 为 `7 pass / 6 warn / 0 fail`，历史退市行情检查 pass，PIT 真相层仅因 `sh.689009` 保持 warn。全量 141 项回归、生产 migration 17/17、独立 smoke 库 0017 增量与第二遍幂等均通过。所有回测继续 `research_only / validation_pending / unvalidated`，本切片不声明策略有效。

### 2026-07-17：DQ4 基本面公告日真相层已落地

新增 migration `0018`，建立 `stock_fundamental_pit` 与 `fundamental_pit_manifest`。同步器通过 Tushare `fina_indicator_vip` 按报告期分页，保留报告期、公告日和修订版本；成熟报告期首刷需达到历史股票池 50%，重复刷新不得低于已有覆盖 80%。2022 年报至 2026 中报共落库 100,423 个版本、5,766 只股票，日期硬异常为 0；12 个代表交易日 as-of 覆盖 `61,970 / 61,970`。

回测只使用信号日 `daily_basic` PE/PB 与公告日不晚于信号日的最新报告期，Service 再次 fail-closed 清除未知或未来财务版本；方法论升级为 `close_signal_next_open_pit_fundamentals_v4`。全市场 fallback SQL 相比原查询新增约 0.74 秒，主要耗时仍是既有 90 日窗口。最终 DQ4 为 `8 pass / 6 warn / 0 fail`，公告日检查 pass；149 项回归、生产/独立 smoke migration 18/18 与第二遍幂等均通过。所有回测继续 `research_only / validation_pending / unvalidated`。

### 2026-07-17：DQ5 历史指数成分真相层已落地

新增 migration `0019`，建立 `index_constituent_pit` 与 `index_constituent_pit_manifest`。Tushare `index_weight` 按月串行同步上证 50、沪深 300、中证 500 和中证 1000；2023-12 至 2026-06 的 124 个指数/月分区全部成功，落库 57,350 条成分，成员数、权重、代码映射和日期硬异常均通过守卫。12 个代表交易日共 48 个 as-of 快照覆盖 `22,200 / 22,200`，最大滞后 17 天。

回测请求新增显式 `universe_code`，默认 `ALL_A` 不变；只有选择指数时才按信号日读取最近月度快照，缺失、成员数或权重异常都会 fail-closed。方法论升级为 `close_signal_next_open_pit_index_universe_v5`，并明确记录“月度权重快照不是精确调仓事件”的限制。最终 DQ5 为 `10 pass / 5 warn / 0 fail`；159 项回归、生产/独立 smoke migration 19/19 与第二遍幂等均通过。沪深 300 两日工程 smoke 成功，但所有回测继续 `research_only / validation_pending / unvalidated`。

### 2026-07-18：冻结策略受控验证已落地

新增 migration `0020` 和 `strategy_validation_protocol`，将一次验证的策略配置、方法论、请求参数、成本/成交约束以及真实执行源码指纹固化为不可静默覆盖的协议。首轮 V1 在发现未冻结源码后于 54/242 主动取消并标记 superseded；V2 对策略实现、Selector、Backtest Service/Repository 与股票池政策逐文件计算 SHA-256，配置、方法论或源码任一漂移均 fail-closed。验证 run 以 system test 隔离并受 retention 保护，API/回测页可读取协议与报告。

历史全 A 区间 2025-07-01 至 2026-06-30 的两条 V2 诊断均完整跑完 242 个样本日、714 笔交易，基准覆盖 100%、收益缺失 0、全部结构检查通过。低波反转扣费后收益 `-43.4248%`、超额 `-54.1446%`、最大回撤 `-44.4163%`；三因子收益 `-39.0495%`、超额 `-50.5983%`、最大回撤 `-47.2742%`。两条策略六项表现门槛均失败，结论为 `historical_diagnostic_fail`，不能归咎于工程或数据缺口，也不会自动调参或升级验证状态。

真正样本外协议已冻结到 2026-07-20 至 2027-01-31，窗口闭合前不执行。当前结论继续是 `research_only / validation_pending / unvalidated`；下一策略工作不再围绕证明现版本有效，而应先分析失败归因并另开新版本研究，旧冻结证据保持不可变。

### 2026-07-18：冻结策略失败归因已落地

新增独立只读归因 Repository/Service/CLI，不修改两条冻结策略的执行指纹文件。工具从不可变 run 重建 1/3/5/10 日毛净收益、因子秩相关、相邻日留存、保留持仓成本近似、市场阶段和所有非重叠再平衡 offset；前瞻收益缺失显式保留，禁止补零或只选最佳 offset。

低波 1 日毛收益复利已为 `-6.7309%`，总分对 1/3/5 日未来收益 IC 分别为 `-0.084299 / -0.133208 / -0.144282`，归类为因子方向失败。V13 1 日毛收益只有 `+0.4824%`，相邻日留存 `7.6072%`，成本后为 `-39.0498%`；3 日三个 offset 中仅一个为正，5/10 日同样分裂，因此“只延长持有期”不具备稳健性。

12 个代表日候选池复核显示原门槛通过面过宽、顶部几乎同分。结论是不创建执行修补型 V14；下一版必须使用独立信号家族、允许持币并预先冻结研究协议。全量 176 项、migration 20/20、真实 CLI 和前瞻协议指纹复核通过，旧冻结协议继续保持原样。

### 2026-07-18：OPS-1 HTTPS 自动续期已收口

原 TrustAsia 手工证书在到期前完成可回滚切换。现网改用 Let’s Encrypt HTTP-01 webroot，证书同时覆盖根域与 `www`，有效期至 2026-10-16；`certbot.timer` 每日运行两轮，续期 deploy hook 只在 Nginx 配置校验成功后 reload。真实签发、远端指纹、HTTP→HTTPS、健康检查、认证边界和 `certbot renew --dry-run --run-deploy-hooks` 均已通过。

旧证书、私钥和 Nginx 配置保留在 root-only 恢复目录，旧 SSL 文件未删除；仓库同步提供 ACME bootstrap、最终 Nginx 模板、deploy hook、安装与回滚说明。另设 2026-09-18 的一次性 OpenClaw 复核任务，在首次进入续期窗口后检查远端到期日、timer、日志、dry-run 与公网健康。P0-6 的证书尾项至此关闭。
