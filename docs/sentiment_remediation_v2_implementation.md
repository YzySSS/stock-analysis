# 舆情选股整改 v2：实施与云端启用手册

## 1. 范围和基线

- OpenClaw 清理基线：`1a99f85`。
- 保留页面：Dashboard、Selection、Tracking、Stocks、System、Portfolio、Backtest、Trade Strategies。
- 新建选股只保留 `a_share_sentiment 0.4.4` 与 `a_share_sentiment_v05 0.5.0`。
- 退役策略不能创建新任务，API 返回 `410 STRATEGY_RETIRED`；历史结果和通用回测结构继续保留。
- `0.4.4` 是冻结稳定对照；`0.5.0` 初始为 `shadow_only/prototype`，不自动晋级。
- 本地只完成代码、migration、配置模板与离线测试，不安装 Redis 服务，不访问生产库或云服务器。

## 2. 已实现的运行边界

```text
HTTP 页面/API
  ├─ 轻量 GET：MySQL 已发布数据或统一缓存
  ├─ Selection POST：MySQL 队列，202 + run_id
  ├─ Save item：只保存，分钟线补全写 MySQL 持久队列
  ├─ Tracking review：202 + review_job_id，业务记录与持久任务同事务创建
  └─ SSE：Redis ready 时启用；否则前端自动轮询

Worker/后台任务
  ├─ Selection Worker：确定性策略计算
  ├─ Durable Task Worker：分钟线刷新/补全和 Tracking AI 复盘
  ├─ Technical feature refresh：预计算日频技术字段
  ├─ Snapshot publisher：校验后原子发布候选快照
  ├─ Operational read models：实时榜单、Tracking 摘要和运维状态
  └─ External providers：仅在后台适配器中调用

事实和加速
  ├─ MySQL：唯一事实源、任务幂等和完整快照
  ├─ In-memory TTL：默认缓存
  └─ Redis：可选热点缓存/SSE 门控，故障自动回落
```

API route 不直接导入 `requests/akshare/tushare/tavily` 等外部客户端。DeepSeek provider 位于 Tracking 服务层，失败只更新任务状态，不影响本地选股结果。

## 3. 缓存和数据库配置

默认值适合 2C4G 单机首次部署：

```env
DB_CONNECT_TIMEOUT_SECONDS=3
DB_READ_TIMEOUT_SECONDS=10
DB_WRITE_TIMEOUT_SECONDS=10
DB_POOL_ENABLED=true
DB_POOL_SIZE=4
DB_POOL_MAX_OVERFLOW=0
DB_POOL_TIMEOUT_SECONDS=3

CACHE_ENABLED=true
CACHE_BACKEND=memory
REDIS_CACHE_ENABLED=false
REDIS_URL=redis://127.0.0.1:6379/0
CACHE_REDIS_FALLBACK_TO_MEMORY=true
USE_SENTIMENT_READ_MODEL=false
```

- 每个 API 进程最多使用 4 个常驻 MySQL 连接，不允许无限溢出。
- 高频纯读链路使用只读连接上下文，结束时 rollback，不产生无意义 commit。
- Redis 包延迟导入；禁用、连接失败或运行时异常均可回落内存缓存。
- `/api/health` 不主动创建首个 Redis 连接，避免存活检查被故障缓存拖慢。
- `/api/health/performance` 提供请求延迟、数据库事务、连接池等待和缓存诊断。

## 4. 一致性快照

当前 schema 共 25 个有序 migration。已应用 migration 不修改、不删除，部署统一通过 `app.orchestration.migrate` 执行 plan、`--apply` 与 `--check`。

Migration `0023` 新增：

- `source_batch_manifest`
- `sentiment_candidate_snapshot_manifest`
- `sentiment_candidate_snapshot`
- `stock_realtime_rank_snapshot`
- `tracking_summary_daily`
- `operational_status_snapshot`
- `ai_advice_snapshot`

Migration `0024` 新增中性日技术特征读模型 `stock_technical_feature_daily`，并为选股结果高频读取补充组合索引。首次应用 `0024` 后、启动 API/Selection Worker 前，执行一次初始化刷新：

```bash
.venv/bin/python scripts/refresh_stock_technical_feature_daily.py
```

命令只从本地 MySQL `daily_kline` 幂等计算，不访问外部数据源。确认输出成功且 `published_rows` 大于 0；若日线尚未同步，先补齐日线再重跑。日常刷新由 Cron 在日线任务后执行；需要指定日期时可追加 `--trade-date YYYY-MM-DD`。

Migration `0025` 新增通用 `durable_task` 队列。股票分钟线刷新、选股保存后的分钟线补全与 Tracking 深度复盘都只在 API 内持久化任务，随后由 `python -m app.jobs.durable_worker` 独立执行。队列使用 MySQL 原子 claim、active idempotency、10 秒任务心跳、最大尝试次数和 5 分钟 stale recovery；Redis 不参与任务正确性。深度复盘的 `ai_advice_snapshot` 和队列行在同一事务创建，旧 worker 丢失所有权后不能覆盖新结果。AkShare 分钟线 Provider 在不继承连接池的 `spawn` 子进程中运行，默认 120 秒硬超时后终止进程，不会留下继续写库的后台线程。

`0023` 的三个运维读模型通过统一入口只从已落库的 MySQL 数据生成：

```bash
.venv/bin/python scripts/refresh_operational_read_models.py --models all
```

每个模型在独立事务内幂等替换；实时榜固定同一行情批次，Tracking 计算 1/3/5/20 日成熟度、胜率与平均收益，缺少可靠基准回放时超额收益保持 `NULL`。实时榜单固定保留 3 天，运维状态固定保留 7 天，Tracking 日摘要不自动清理。

快照并非只有仓储接口：正式物化入口为：

```bash
.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py --strategy-id a_share_sentiment
```

物化器用 MySQL 命名锁避免同策略并发生产，在单个 `REPEATABLE READ` 事务中实算活跃股票全集、必需数据集交集覆盖率并运行 `StockSelector` 本地确定性核心。所有日线、舆情、板块资金、实时行情、实时资金和人气查询都绑定同一 `decision_as_of`；实际评分读取的可选表也生成独立 source manifest 和 lineage，但部分覆盖不会冒充核心 98% 覆盖。它不调用外部 Provider、不运行会改变正式排名的 progressive/DeepSeek 包装、不写普通选股结果；完整 selector 输入视图、配置、实现和输出均带内容哈希。低于 98%、必需来源缺时间戳、候选级必需数据不完整、出现时间穿越、混合交易时钟或时区不一致时，不会让对应股票进入 v0.5 交易级，也不会替换上一个完整快照。v0.5 影子物化必须显式增加 `--strategy-id a_share_sentiment_v05 --allow-shadow`，不会改变注册表状态。

同批对照使用 `.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py --dual-run`。该入口把 0.4.4 与 v0.5 的必需数据集取并集，只打开一次 MySQL 一致性快照，两个策略分别保存自己的 read-view hash 和候选快照，并共享 `dual_input_hash`；不写正式 `selection_result`，也不改变 v0.5 的 `shadow_only/prototype` 状态。两个版本各自原子发布；对照分析只接受同一 `dual_input_hash` 的完整配对，任一侧失败时必须重跑，不能拿新旧快照拼接比较。

候选快照发布规则：

1. 先写 `building/pending` manifest 和同一 `snapshot_id` 的候选行。
2. 覆盖率必须 `>= 0.98`。
3. 候选关键字段完整率必须为 `100%`。
4. 每条 lineage 必须包含 `provider/batch_id/source_time/received_at`。
5. `source_time` 不得晚于 `decision_as_of`。
6. 发布时在单个 MySQL 事务中 `FOR UPDATE` 重验并切换为 `ready/passed`。
7. 失败批次只标记 `rejected/failed`，不会覆盖上一个可用快照。
8. 读取先锁定一个完整 manifest，再按同一 `snapshot_id` 读取并校验行数，禁止混批。

发布成功后写一个可丢弃的轻量缓存指针；缓存失败不影响 MySQL 发布。进程内缓存不跨 API/worker 共享，因此提交任务必须在 pointer miss 时通过 `latest_complete_manifest()` 从 MySQL 轻量读取并固定 `input_snapshot_id`，不能通过加载候选行或同步外部抓取来补 miss。

`USE_SENTIMENT_READ_MODEL=false` 是首次上线和回滚默认值。快照生产连续稳定后改为 `true`，Selection Worker 将只读取最近完整快照；没有完整快照时明确失败，不退化为同步外部抓取。

## 5. 舆情 v0.5 固定策略合同

候选分为直接催化轨和主题龙头轨，两轨统一排名。正式分数固定为：

| 模块 | 权重 |
|---|---:|
| 催化质量 | 25% |
| 事件或主题持续性 | 15% |
| 个股关系及辨识度 | 15% |
| 个股和板块资金确认 | 15% |
| 价格、成交量和分时确认 | 20% |
| 筹码、流动性和成交容量 | 10% |

分级和市场硬门：

- `<60` 淘汰；`60～67.99` 观察；`>=68` 才可能成为交易级。
- `risk_on` 最多 3 只，最低 68；`cautious` 最多 1 只，最低 72。
- `defensive/unknown/stale` 全部只能观察。
- 同主题最多 1 只交易级，同一行业最多 2 只。
- ST、退市、停牌、生命周期未知、上市不足 60 交易日、流动性不足和高可信重大负面事件走硬过滤。
- 盘中最新成交额只认同批有效 `realtime_amount`；收盘后使用最新完整日成交额。20 日成交额中位数缺失时不以均值替代。
- 直接催化必须有一个权威原始来源或两个独立可信发布方；同一 `source_id` 或同一注册域名的多个链接只计一个来源。
- AI 只提供解释、风险、证据和失效条件；不改变正式分数、排序或等级。

结果字段包含 `signal_grade/validation_status/score_breakdown/gate_results/evidence_ids/ai_status`。

## 6. 本地验证

本轮代码基于 OpenClaw 清理提交 `1a99f85575f85ee74b267a9b5725a1ab85167de1` 完成，并于 2026-07-21 做过以下离线验收：

- `python -m unittest discover -s tests -v`：383 项全部通过。
- Python `app/`、`scripts/` 全量字节码编译通过。
- Selection、Stock Detail、Tracking、Backtest 四个改动过的浏览器脚本通过 `node --check`。
- 未提供 Token 的 Tushare 探针返回 `skipped`，确认 `network_attempted=false`。
- `git diff --check` 通过。

上述结果只证明代码、合同测试和无基础设施降级路径通过；没有连接真实 MySQL、Redis、Tushare/AkShare/新闻源/DeepSeek，也没有执行 Linux Bash/systemd 校验或云端压测。

Windows PowerShell 示例：

```powershell
$env:DB_HOST='127.0.0.1'
$env:DB_PORT='3306'
$env:DB_USER='test'
$env:DB_PASSWORD='test'
$env:DB_NAME='test'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\probe_tushare_realtime_permissions.py
```

未提供显式 Token 环境变量时，Tushare 探针应返回 `skipped`，不得联网或输出密钥。本地测试不代表云服务器性能 SLO。

## 7. 云服务器上线顺序

可直接勾选执行的云侧任务清单见 `docs/cloud_deployment_task_plan.md`。该清单包含负责人、命令、进入下一阶段的门槛和逐阶段回滚项。

1. 备份 MySQL、生产环境文件、Cron 和 systemd 配置。
2. 部署代码，保持 `REDIS_CACHE_ENABLED=false`、`USE_SENTIMENT_READ_MODEL=false`、单个 Uvicorn worker。
3. 执行全部 25 个 migration 的 plan、`--apply`、`--check`，确认空库/增量状态均为 25/25。
4. 执行 `.venv/bin/python scripts/refresh_stock_technical_feature_daily.py`，确认最新交易日技术特征已发布。
5. 执行 `.venv/bin/python scripts/refresh_operational_read_models.py --models all`，确认三个本地读模型可生成。
6. 启动 API 与全部必要 worker（包括 `stock-analysis-durable-task-worker.service`），逐一验证全部保留页面、历史查询和三类持久异步任务。
7. 用服务器上的真实 Token 运行 Tushare `rt_min/rt_min_daily` 只读探针。
8. 运行 `0.4.4` 烟雾测试、稳定快照物化和固定快照对照。
9. 安装 `deploy/redis/stock-analysis.conf`，启用 Redis 后验证命中、故障回落和 SSE 轮询降级。
10. 快照生产连续稳定后才开启 `USE_SENTIMENT_READ_MODEL=true`。
11. 完成 20 并发、30 分钟压测后，才评估从 1 个 API worker 调整到 2 个。
12. v0.5 先以 `--dual-run` 影子运行至少 20 个交易日；120/252 个交易日分别形成中期/完整报告，晋级必须人工执行。

## 8. 云端验收

生产性能只能在 2C4G 云服务器上判定。压测使用 20 个并发页面会话持续 30 分钟，并同时运行数据同步和一次完整舆情选股：

| 指标 | 目标 |
|---|---:|
| Health P95 | `<50ms` |
| Readiness/System P95 | `<100ms` |
| 选股提交 P95 | `<200ms` |
| 任务状态 P95 | `<100ms` |
| 结果、Dashboard、Tracking 热读 P95 | `<200ms` |
| 冷读 P95 | `<500ms` |
| 个股 Overview P95 | `<250ms` |
| 个股详情 P95 | `<400ms` |
| 本地确定性选股 P95 | `<10s` |
| API 错误率 | `<0.5%` |

资源约束为 CPU P95 低于 75%、总内存低于 3.2GB、无持续 Swap、Redis 不超过 192MB。分别记录 Redis 关闭与开启时的热点接口 P95；没有明确改善则保持 Redis 关闭。两个 API worker 造成内存不足或数据库连接超过 70% 时回退单 worker。验收记录同时包含数据快照 ID、新鲜度、缓存模式和 Redis 降级结果。

## 9. 回滚

按影响从小到大回滚：

1. Redis 异常：设置 `REDIS_CACHE_ENABLED=false`，重启 API；MySQL 业务不受影响。
2. 快照读路径异常：设置 `USE_SENTIMENT_READ_MODEL=false`，切回稳定旧查询路径。
3. v0.5 异常：保持 `shadow_only` 或从影子任务列表移除，不修改 0.4.4 和历史证据。
4. 新代码异常：部署上一个代码版本；新增表保留，不回改已应用 migration。
5. 数据批次异常：读回上一个 `ready/passed` 快照，不发布不完整批次。

回滚后必须执行页面/API 冒烟、Selection Worker 检查和 `/api/health`、`/api/readiness` 验证。旧策略历史表至少保留 90 天，只有备份恢复演练成功后才讨论删除。
