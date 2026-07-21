# 舆情选股整改：云侧部署执行任务计划

## 1. 目标与执行边界

本计划用于将本地已完成的舆情选股整改部署到 2C4G 云服务器。云侧必须按阶段启用，不能在同一次发布中同时开启 Redis、双 API Worker、快照读路径和 v0.5。

- 部署对象：合并后的不可变 Git 提交 SHA，禁止直接部署未提交工作区。
- 稳定策略：`a_share_sentiment / 0.4.4`。
- 影子策略：`a_share_sentiment_v05 / 0.5.0`，仅允许 `shadow_only`。
- MySQL 是唯一事实源；Redis 只保存缓存、SSE 门控和短期协同状态。
- 首次上线保持 `REDIS_CACHE_ENABLED=false`、`USE_SENTIMENT_READ_MODEL=false`、单个 Uvicorn Worker。
- 任何阶段验收失败都停止后续任务，并执行本阶段回滚。

建议维护一份发布记录，至少填写：

```text
release_sha=
operator=
started_at=
database_backup=
env_backup=
migration_before=
migration_after=
stable_snapshot_id=
tushare_probe_result=
redis_enabled=false
api_workers=1
rollback_sha=
```

## 2. Phase 0：发布前准备

负责人：运维/部署人员。

- [ ] 确认发布提交已经合并到目标分支，记录完整 `release_sha`。
- [ ] 阅读 `deploy/README.md` 与 `docs/sentiment_remediation_v2_implementation.md`。
- [ ] 确认服务器剩余磁盘、内存和 MySQL 最大连接数满足发布要求。
- [ ] 确认服务器 `.env`、Tushare Token、DeepSeek Key 不在 Git 仓库中。
- [ ] 确认当前稳定版本仍为 `a_share_sentiment / 0.4.4`。
- [ ] 记录当前 API/Worker systemd 状态、Cron 内容和最近一次完整快照。

只读检查示例：

```bash
cd /root/.openclaw/workspace/stock-analysis
git status --short
git rev-parse HEAD
systemctl status stock-analysis-api.service --no-pager
systemctl status stock-analysis-selection-worker.service --no-pager
crontab -l
free -h
df -h
```

进入下一阶段条件：工作区无未提交修改，发布 SHA 明确，当前服务基线已记录。

## 3. Phase 1：备份

负责人：数据库/运维人员。

- [ ] 备份生产 MySQL；备份文件名包含日期和发布前 SHA。
- [ ] 备份服务器 `.env`，权限保持 `600` 或更严格。
- [ ] 备份 `/etc/systemd/system/stock-analysis-*.service`。
- [ ] 备份当前 crontab、Nginx 配置和 Redis 配置（如果已有）。
- [ ] 验证数据库备份可以读取，并记录恢复命令和存放位置。

示例命令需按服务器实际凭据文件调整，不得把密码写入命令历史：

```bash
install -d -m 700 /root/stock-analysis-backups
mysqldump --defaults-extra-file=/root/.my.cnf \
  --single-transaction --routines --triggers stock_analysis \
  | gzip > /root/stock-analysis-backups/stock_analysis_before_release.sql.gz
cp --preserve=mode,ownership .env /root/stock-analysis-backups/app.env.before_release
crontab -l > /root/stock-analysis-backups/crontab.before_release
```

进入下一阶段条件：数据库、环境文件、Cron 和 systemd 均有可定位备份。

## 4. Phase 2：部署代码与依赖，保持所有新开关关闭

负责人：部署人员。

```bash
cd /root/.openclaw/workspace/stock-analysis
git fetch --prune origin
git checkout <target-branch>
git pull --ff-only origin <target-branch>
git checkout <release_sha>
.venv/bin/pip install -r requirements.txt
```

服务器环境先保持：

```env
DB_POOL_ENABLED=true
DB_POOL_SIZE=4
DB_POOL_MAX_OVERFLOW=0
DB_POOL_TIMEOUT_SECONDS=3
DB_CONNECT_TIMEOUT_SECONDS=3
DB_READ_TIMEOUT_SECONDS=10
DB_WRITE_TIMEOUT_SECONDS=10

CACHE_ENABLED=true
CACHE_BACKEND=memory
REDIS_CACHE_ENABLED=false
USE_SENTIMENT_READ_MODEL=false
DURABLE_INTRADAY_TIMEOUT_SECONDS=120
```

- [ ] 不安装或启用 Redis。
- [ ] 不把 API Worker 数调整为 2。
- [ ] 不启用 v0.5 API 运行权限。
- [ ] 执行 Python 编译检查。

```bash
.venv/bin/python -m compileall -q app scripts
```

进入下一阶段条件：依赖安装成功、编译检查通过、所有高风险开关仍关闭。

## 5. Phase 3：Migration 25/25

负责人：数据库/部署人员。

严格使用统一 migration 入口，不手工执行生产改表 SQL，不修改已应用 migration：

```bash
.venv/bin/python -m app.orchestration.migrate
.venv/bin/python -m app.orchestration.migrate --apply
.venv/bin/python -m app.orchestration.migrate --check
```

- [ ] plan 中只有预期的 pending migration。
- [ ] apply 成功，无 checksum mismatch。
- [ ] check 显示 `25/25`。
- [ ] 确认新增表存在：候选快照/Manifest、运维读模型、技术特征、`durable_task`。
- [ ] 确认原有历史选股结果、页面相关表和已应用 migration 未被删除。

失败处理：停止部署，不启动新代码；保留新增表并切回旧代码 SHA。只有确认 migration 本身造成不可接受影响时，才按数据库备份恢复方案处理，禁止临时回改 migration 文件。

## 6. Phase 4：初始化本地读模型和稳定版快照

负责人：数据/部署人员。

```bash
.venv/bin/python scripts/refresh_stock_technical_feature_daily.py
.venv/bin/python scripts/refresh_operational_read_models.py --models all
.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py \
  --strategy-id a_share_sentiment
```

- [ ] 技术特征 `published_rows > 0`；如果日线为空，先补齐日线再重跑。
- [ ] 实时排名、Tracking 摘要、运维状态三个读模型均成功生成。
- [ ] 稳定版快照为 `ready/passed`。
- [ ] 覆盖率 `>= 0.98`，关键字段完整率为 `100%`。
- [ ] 记录 `snapshot_id`、`decision_as_of`、`freshness_seconds` 和输入批次。
- [ ] 未通过质量门的批次没有覆盖上一个完整快照。

此阶段仍保持 `USE_SENTIMENT_READ_MODEL=false`。

## 7. Phase 5：安装并启动服务

负责人：运维人员。

```bash
install -o root -g root -m 644 deploy/systemd/stock-analysis-api.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-backtest-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-selection-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-portfolio-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-durable-task-worker.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now \
  stock-analysis-api.service \
  stock-analysis-backtest-worker.service \
  stock-analysis-selection-worker.service \
  stock-analysis-portfolio-worker.service \
  stock-analysis-durable-task-worker.service
```

- [ ] `/api/health` 返回 200，`cache_mode=memory`、`redis_status=disabled`。
- [ ] `/api/readiness` 返回 ready，五个必要 Worker 均为 healthy。
- [ ] Dashboard、Selection、Tracking、Stocks、System、Portfolio、Backtest、Trade Strategies 页面均可打开。
- [ ] 历史策略结果仍可查询。
- [ ] 退役策略新建运行返回 `410 STRATEGY_RETIRED`。
- [ ] 稳定舆情选股提交返回 `202 + run_id`，Worker 可以完成任务。
- [ ] 分钟线刷新返回 `202 + job_id`，持久任务 Worker 可以完成或记录失败。
- [ ] Tracking 深度复盘返回 `202 + review_job_id`，API 重启不丢任务。
- [ ] GET 接口缓存未命中时不直接调用外部 Provider。

失败处理：停止新 unit，恢复上一个代码 SHA 与旧 systemd 文件；`REDIS_CACHE_ENABLED` 和 `USE_SENTIMENT_READ_MODEL` 继续保持 false。

## 8. Phase 6：数据源权限探测

负责人：数据/运维人员。

Token 只能通过临时环境变量提供，不写入命令参数、日志或仓库：

```bash
read -rsp 'Tushare token: ' TUSHARE_RT_PROBE_TOKEN && printf '\n'
export TUSHARE_RT_PROBE_TOKEN
.venv/bin/python scripts/probe_tushare_realtime_permissions.py \
  --token-env TUSHARE_RT_PROBE_TOKEN --ts-code 600000.SH --freq 1MIN
unset TUSHARE_RT_PROBE_TOKEN
```

- [ ] 分别记录 `rt_min` 与 `rt_min_daily` 探测结果。
- [ ] 探测通过：配置 Tushare 为实时分钟主源、AkShare 为备用源。
- [ ] 探测不通过：保持 AkShare 实时主源，Tushare 继续负责日线/PIT/基础数据。
- [ ] Provider 切换前后各观察至少一个完整交易日。

Provider 切换只修改配置，不修改策略业务代码。

## 9. Phase 7：启用快照读路径

只有稳定版快照连续生成并通过质量门后执行：

```env
USE_SENTIMENT_READ_MODEL=true
```

重启 API 和 Selection Worker 后验证：

- [ ] 提交任务固定 `input_snapshot_id`。
- [ ] Worker 只读取同一个完整快照，不混用批次。
- [ ] 缓存指针缺失时从 MySQL 读取最新完整 Manifest。
- [ ] 没有完整快照时明确失败，不同步调用外部数据源。

回滚：设置 `USE_SENTIMENT_READ_MODEL=false` 并重启 API/Selection Worker。

## 10. Phase 8：Redis 可选启用

稳定运行一轮无 Redis 冒烟后，再部署仓库模板：

```bash
apt-get install -y redis-server
install -o root -g root -m 644 deploy/redis/stock-analysis.conf /etc/redis/stock-analysis.conf
systemctl restart redis-server.service
redis-cli -h 127.0.0.1 ping
```

确认 Redis 只监听 `127.0.0.1`、`maxmemory 192mb`、`allkeys-lfu`，且 RDB/AOF 已关闭，再设置：

```env
CACHE_BACKEND=memory
REDIS_CACHE_ENABLED=true
REDIS_URL=redis://127.0.0.1:6379/0
```

- [ ] 健康检查显示 `cache_mode=redis`、`redis_status=ready`。
- [ ] 热点接口出现缓存命中。
- [ ] Selection SSE 可用。
- [ ] 主动停止 Redis 后，API 自动回落内存缓存，SSE 前端退回轮询。
- [ ] Redis 故障不影响选股提交、任务执行、保存和 Tracking。

回滚：设置 `REDIS_CACHE_ENABLED=false`，重启 API；无需恢复 Redis 数据。

## 11. Phase 9：性能压测与 Worker 数决策

压测条件：20 个并发页面会话持续 30 分钟，同时运行数据同步和一次完整舆情选股。

| 指标 | 验收目标 |
|---|---:|
| Health P95 | `<50ms` |
| Readiness/System P95 | `<100ms` |
| 选股提交 P95 | `<200ms` |
| 任务状态 P95 | `<100ms` |
| 结果/Dashboard/Tracking 热读 P95 | `<200ms` |
| 冷读 P95 | `<500ms` |
| 个股 Overview P95 | `<250ms` |
| 个股详情 P95 | `<400ms` |
| 本地确定性选股 P95 | `<10s` |
| API 错误率 | `<0.5%` |

资源门：CPU P95 `<75%`、总内存 `<3.2GB`、无持续 Swap、Redis `<=192MB`、数据库连接使用率 `<70%`。

- [ ] 分别记录 Redis 关闭和开启时的 P50/P95/P99。
- [ ] Redis 没有明确改善则关闭。
- [ ] 单 Worker 达标时保持单 Worker。
- [ ] 只有内存和数据库连接余量充足时，才试运行两个 API Worker。
- [ ] 两 Worker 导致内存不足或数据库连接超过 70% 时立即回退一个。

## 12. Phase 10：v0.5 影子观察

v0.5 不通过 API 开放，不自动晋级。使用同一输入执行双版本影子物化：

```bash
.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py --dual-run
```

- [ ] 只比较相同 `dual_input_hash` 的完整配对。
- [ ] 任一版本失败时重跑，不拼接新旧快照。
- [ ] 比较候选、排名、硬门、证据、数据新鲜度和执行耗时。
- [ ] 前 20 个交易日只验收工程稳定性。
- [ ] 120 个交易日生成中期策略报告。
- [ ] 252 个交易日完成策略验证后，才允许人工讨论晋级。
- [ ] AI 建议与正式分数、排名、交易等级分开保存。

## 13. 最终交付与回滚表

发布完成后保存：发布 SHA、migration 25/25 结果、稳定快照 ID、各 Worker 状态、数据源探针结果、Redis 前后压测报告和全部回滚命令。

| 故障 | 首选回滚 |
|---|---|
| Redis 不稳定 | `REDIS_CACHE_ENABLED=false`，重启 API |
| 快照读路径异常 | `USE_SENTIMENT_READ_MODEL=false`，重启 API/Selection Worker |
| v0.5 异常 | 停止双跑，保持 `shadow_only` |
| Durable Worker 异常 | 停止该 Worker，修复后由 MySQL stale recovery 重排；API 页面读路径继续可用 |
| 新代码异常 | 切回上一个代码 SHA，保留新增表 |
| 数据批次异常 | 固定读取上一个 `ready/passed` 快照，不发布坏批次 |

任何回滚后都必须重新执行页面/API 冒烟、`/api/health`、`/api/readiness` 和稳定舆情策略检查。
