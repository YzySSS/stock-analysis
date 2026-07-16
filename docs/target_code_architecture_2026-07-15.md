# 股票分析系统目标代码架构（2026-07-15）

> 定位：这是当前整改的代码落地合同，服务于单机模块化单体，不是微服务拆分方案。
>
> 执行基线见 `docs/architecture_remediation_plan_2026-07-15.md`。本文负责回答“模块放在哪里、依赖怎么走、任务状态怎么定义、每一批如何迁移”。

## 1. 目标与非目标

本轮目标：

1. API 入口先做能力预检，不让未支持的标的进入数据库、外部 API 或 AI 调用。
2. Query 与 Command 分开：查询只读真实结果，长任务只入队并返回任务 ID。
3. 路由、应用服务、算法、SQL 和外部数据适配器形成单向依赖。
4. selection、backtest、portfolio advice 复用同一套 MySQL 任务语义。
5. Schema 只由部署迁移入口管理，普通请求与 Service 构造器不执行 DDL。
6. 每个高风险语义都有不依赖线上数据的最小回归测试。

本轮不做：

- 不拆微服务，不引入 Redis、Celery 或消息中间件。
- 不一次性重写现有 SQL、策略算法或页面。
- 不把 ETF 持仓行情等同于 ETF 选股能力。
- 不在回测口径修复前把任何策略标为“已验证”。

## 2. 目标目录边界

```text
app/
  api/
    routes/                    # HTTP 参数、鉴权、状态码；不承载 SQL/长计算
  shared/
    db.py                      # 连接与事务基础设施
    instrument_policy.py       # 全局标的类型护栏
    errors.py                  # 后续统一业务错误协议
  strategies/
    registry/                  # 策略能力唯一真相源
    capability.py              # 数据覆盖/新鲜度/readiness 预检
    service.py                 # 策略编排，不直接成为 HTTP 层
  selection/
    service.py                 # 提交任务、查询真实结果
    repository.py              # selection_run/result SQL
    worker.py                  # claim、执行、心跳、取消、恢复
  stock_selection/             # 纯选股算法与评分；不处理 HTTP/队列
  backtest/
    policy.py                  # research-only 与口径版本
    service.py                 # 回测业务计算
    repository.py              # backtest_run/result/trade SQL
    worker.py                  # 回测队列消费者
  portfolio/
    service.py                 # 持仓业务编排
    repository.py              # 持仓与建议 SQL
    worker.py                  # AI 建议任务消费者
  jobs/
    models.py                  # 通用状态、错误、进度模型
    repository.py              # 通用 claim/heartbeat/cancel/retry
    service.py                 # 幂等提交和状态流转
  data_ingestion/
    ...                        # 可复用同步逻辑；scripts 只做 CLI 适配
  migrations/
    ...                        # 唯一 Schema 迁移入口
scripts/
  ...                          # 参数解析、调度启动，不被 app 反向导入
tests/
  unit/                        # 策略、policy、状态机
  integration/                 # MySQL repository、迁移和接口烟测
```

这是最终边界，不要求一次搬完。迁移期间允许保留现有 `app/stock_selection/run_tasks.py`、`app/backtest/worker.py` 等文件，但新增逻辑必须遵守下面的依赖方向。

## 3. 允许的依赖方向

```text
HTTP Route
  -> Application Service
     -> Capability / Domain Policy
     -> Repository
        -> shared.db
     -> Domain Algorithm
     -> External Adapter

Worker
  -> Application Service（与 HTTP 共用）

scripts CLI
  -> data_ingestion / Application Service
```

禁止方向：

- `app -> scripts`、`app -> src/versions`。
- Route 直接执行大段 SQL、外部 API 或全市场计算。
- Repository 反向调用 Route、页面或外部 API。
- 策略算法自行创建任务、修改 HTTP 状态或执行 Schema DDL。
- Service 构造器和 GET 请求执行 `CREATE/ALTER/seed`。

## 4. 核心调用流程

### 4.1 选股 Command

```text
POST /api/selection/run
  -> instrument policy（stock only）
  -> strategy capability（标的、数据集、覆盖率、新鲜度）
  -> SelectionService.submit(payload, idempotency_key)
  -> MySQL job/selection_run: queued
  -> 202 + run_id

selection worker
  -> 原子 claim
  -> heartbeat / 真实阶段进度
  -> StrategyService.run_strategy
  -> 持久化 preview result_json
  -> 用户按条保存 selection_result
  -> success / no_data / failed / cancelled
```

能力预检必须发生在候选加载、Tavily、DeepSeek 等付费或重型调用之前。

### 4.2 选股 Query

```text
GET /api/selection/results
  -> SelectionRepository.find_results(...)
  -> 有真实 selection_result：success + items
  -> 没有真实结果：no_history + []
```

市场快照只能通过独立接口/字段展示，不能伪装成策略结果。

### 4.3 回测

```text
POST /api/backtest/run
  -> instrument policy（stock only）
  -> strategy/backtest capability
  -> BacktestService.submit
  -> queued

backtest worker
  -> point-in-time 数据切片
  -> T-1 已知信号 / T 日成交（最终口径）
  -> 保存 cutoff、配置 hash、methodology_version
  -> 完成结果仍携带 research disclosure
```

旧口径统一标记 `legacy_pre_point_in_time_v1`，不得和修复后的结果混算。

### 4.4 持仓 AI 建议

```text
POST /api/portfolio/advice
  -> 鉴权、限流、参数校验
  -> job queued
  -> portfolio worker 调用 DeepSeek
  -> 保存结构化建议与模型/提示词版本
```

API/Gateway 重启不得丢失 queued/running 任务。

## 5. 统一任务状态机

```text
queued -> running -> success
                  -> no_data
                  -> failed
                  -> cancelled

queued  --cancel--> cancelled
running --cancel_requested--> cancelled
running --heartbeat stale--> queued（可重试）或 failed（达到 max_attempts）
```

语义约束：

- `success + 0` 只允许表示“数据充分、算法正常执行，但没有标的达到阈值”。
- 因缺少必要数据无法执行必须是 `no_data`，并保存 `error_code/data_gap`。
- 从未有真实持久化结果的查询使用 `no_history`，它不是任务状态。
- `failed` 必须有稳定的 `error_code` 和适合用户阅读的 `error_message`。
- claim 必须原子完成；worker 只执行自己持有锁的任务。
- 幂等键至少覆盖 job_type、策略、instrument、交易日与影响结果的参数。

通用任务最小字段：

```text
job_id, job_type, payload_json, idempotency_key
status, worker_id, locked_at, heartbeat_at
cancel_requested, attempt_count, max_attempts
progress_stage, progress_current, progress_total
error_code, error_message
created_at, started_at, finished_at
```

短期继续兼容 `selection_run` 与 `backtest_run`；先抽取公共状态操作，再决定是否迁移到统一 `job_run`，避免为了表名统一先做高风险数据迁移。

## 6. 策略 capability 合同

YAML 注册表是唯一能力真相源，每个策略至少声明：

```yaml
supported_instrument_types: [stock]
required_datasets: [daily_kline, factor_input_daily]
minimum_coverage: 0.95
maximum_data_age_days: 1
runtime_status: enabled
backtest_status: disabled
validation_status: unvalidated
```

运行时动态计算并分别返回：

- `loadable`：代码能否导入。
- `instrument_compatible`：当前标的是否支持。
- `data_ready`：覆盖率与新鲜度是否达标。
- `runtime_ready`：当前是否允许普通选股运行。
- `backtest_ready`：是否允许回测执行。
- `validation_status`：是否通过样本外验证。

页面不能再用单个绿色 `runtime_ready` 代表以上全部含义。

## 7. Repository 接口边界

Repository 只封装持久化，不决定产品语义。目标最小接口示例：

```python
class SelectionRepository:
    def create_run(self, payload, idempotency_key): ...
    def claim_next(self, worker_id): ...
    def heartbeat(self, run_id, worker_id, progress): ...
    def save_results(self, run_id, items): ...
    def find_results(self, filters): ...

class BacktestRepository:
    def create_run(self, payload): ...
    def load_candidate_rows(self, trade_date, instrument_type, windows): ...
    def update_progress(self, run_id, progress): ...
    def save_results(self, run_id, results, curve, trades): ...
    def load_run_results(self, run_id): ...

class JobRepository:
    def request_cancel(self, job_id): ...
    def recover_stale(self, stale_before): ...
    def finish(self, job_id, worker_id, status, error=None): ...
```

事务边界：

- claim、结果保存、finish 分别使用短事务。
- 长计算不持有数据库事务。
- connection/cursor 不跨线程共享。
- 外部 API 调用不在数据库事务中进行。

## 8. 迁移批次与文件落点

### A1：真实性护栏（当前批次）

- `app/shared/instrument_policy.py`：统一拒绝 ETF/指数。
- selection/backtest Route 与 Service：双层预检。
- selection tracker：删除市场快照假兜底。
- `app/backtest/policy.py`：API 与页面统一 research-only。
- `tests/test_p0_architecture_guards.py`：高风险语义回归。

完成后仍保留股票选股、股票研究回测和 ETF 持仓行情。

### A2：运行与公网保护

- 高频 cron advisory lock 修复。（已完成：六条链路统一为持久连接锁）
- 热度任务错误语义与系统状态注册。（已完成：24 项注册表、降级/陈旧状态可见）
- Basic Auth（`/api/health` 例外）与重型接口限流。（已完成：全站认证、五类重型入口按 IP 限流）
- systemd、Nginx、cron 部署模板。（已完成：无密钥模板和 cron 审查模式）
- 证书续签。（待完成：当前证书 2026-08-03 到期）

运行时凭据只保存在服务器 root-only 文件中，不进入仓库、文档或任务日志。

### B：capability 与可信回测

- YAML 能力字段与动态 readiness。（B1 已完成并部署）
- 信号时点、非 point-in-time 基本面隔离和历史股票池约束。（B2 已完成；历史 ST/退市快照仍是已知限制）
- methodology/data cutoff/config hash 入库。（B2 已完成）
- lowvol/v13 小窗口系统测试基线、资源护栏和可比性分级。（B3 已完成；仅工程验证，不升级 validation 状态）

### C：MySQL 任务统一

- 抽 `jobs` 公共状态操作。（C1 已完成）
- selection 迁出 FastAPI 进程。（C1 已完成：独立 systemd worker）
- selection 取消、心跳、stale recovery、active 幂等与重试测试。（C1 已完成）
- portfolio advice 迁出 FastAPI 进程。（C2 已完成：独立 systemd worker、active 幂等、取消/心跳/stale recovery）
- worker/readiness 健康检查与任务保留周期统一。（C3 已完成：进程租约、`/api/readiness`、系统页、错误日聚合与安全 retention 已部署）

### D：数据生命周期（已部署，历史迁移后台收口）

- 全市场 1m raw 按交易日分区并保留 2 个交易日；5m/15m rollup 和 tracked 1m 保留 90 个交易日。
- 高频 writer 只写 current/raw，不再执行 retention；收盘后 lifecycle 以 manifest 完整性作为删分区前置条件。
- 舆情父快照改为关系化 stock/news/source 子表；news 使用 raw ID 引用，legacy reader 由 repository hydration 兼容。
- 舆情最近 5 个交易日保留盘中快照，更早日期只保留每日 EOD，最长 90 个交易日；只有保留集全部完成 V2 归一化才允许裁剪。
- task/error retention 与 logrotate 已在 C3 提前完成；系统页现在能说明三类高增长数据的用途、保留期和任务状态。

### E：模块边界与可复现部署（E1-E6 已完成）

- 唯一 migration 入口和普通请求/Service/同步器运行时 DDL 清理已完成；16 个版本现库 ready，systemd/cron 均有启动前 check。
- 空库 smoke 已提供生产库拒绝护栏和双执行幂等校验；因当前远端应用账号不能 provision 独立数据库，真实空库实跑等待测试库。
- `PortfolioRepository` 已完成：Service 不再直连 MySQL，列表市场上下文改为固定 6 条批量查询，完整列表固定 9 条 SQL，新旧响应影子对比零差异。
- `TrackingRepository` 已完成：route/Tracker 不再直连 MySQL，分页先圈定 ID 再计算本页极值，全局汇总只读取纳入统计样本；冷/缓存分页约从 7.77s/5.49s 降到 0.313s/0.049s。
- `DashboardRepository` 已完成：route 不再含 SQL，市场概览/热点/情绪榜统一走 read repository；逐候选日线 N+1 批量化后 Dashboard SQL 约从 76 降到固定 17，完整响应影子对比零差异。
- `SelectionRepository` 已完成：route、`StockSelector`、`SelectionRunService` 不再直连 MySQL，候选/舆情/结果/run SQL 统一收口；结果、任务和固定时点候选三组影子响应零差异。
- `BacktestRepository` 已完成：route、`BacktestService`、验证基线不再直连 MySQL，run/result/trade/factor status、候选窗口、结果写入和验证查询统一收口；四组 API、5,201 条固定候选和交易日影子响应零差异。
- readiness 数据新鲜度已按业务口径校准：历史输入层只与达到股票池 95% 覆盖的完整日线日期比较；ETF/零星盘中日线只记为 partial available，不再触发股票因子落后误报。
- factor input 改为 18:30 收盘后补跑、03:20 兜底；Tushare `daily_basic` 每交易日只抓一次，低于 80% 覆盖的日期不写入并以 `partial_success` 暴露上游未就绪。
- P3-3 已完成：生产 app 对 `scripts/src` 的直接依赖为 0，ETF/舆情可复用任务进入 `app/data_ingestion`，相关脚本为薄启动器；旧选股 CLI 改为排队，未路由首页和 ETF grid 原型已归档。
- P3-4 七类最低回归已覆盖，全量 115 项；真实空库 migration smoke 等待数据库侧提供独立测试库，禁止借生产库模拟。

## 9. 验证与回滚要求

每个切片至少验证：

1. 单元测试：policy、状态语义、结果不伪造。
2. 接口烟测：支持与不支持路径的 HTTP 状态及响应结构。
3. 页面烟测：禁用入口、风险提示、空态文案。
4. 服务烟测：API/worker active、health 200、无新增 failed unit。
5. 资源检查：重启后 CPU、内存、swap 无持续异常增长。

回滚遵循“代码可回滚、数据向后兼容”：

- 第一阶段只增加共享 policy/响应字段，不删库、不改历史结果。
- 新响应字段保持向后兼容；前端先支持再切换语义。
- Schema 变更必须有备份、dry-run 与反向脚本。
- 任何会触发付费外部调用的烟测均使用不支持标的预检或 stub，不跑全市场计算。
