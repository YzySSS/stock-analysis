# 当前服务器部署模板

这些文件复现当前单机部署边界：FastAPI 只监听 loopback，Nginx 提供 HTTPS
和重型接口限流，FastAPI 提供单管理员登录页与签名会话，`/api/health`
保持免认证。

逐阶段执行、验收和回滚清单见 `docs/cloud_deployment_task_plan.md`；云端操作应以该清单记录发布 SHA、备份、migration、快照、探针和压测证据。

模板不包含数据库密码、API token、TLS 私钥或站点登录凭据。部署前确认项目
`.env` 和 MySQL 已独立准备；TLS 由 Certbot 在服务器上签发，证书文件不进入仓库。

## 依赖

- Ubuntu/Debian：Nginx、OpenSSL、Certbot、Python 3 venv。
- Python：在项目 `.venv` 内安装仓库依赖。
- 数据：MySQL 可用，应用 `.env` 只保存在服务器。
- TLS：Let’s Encrypt webroot 为 `/var/www/letsencrypt`，证书位于 `/etc/letsencrypt/live/yzysstock.cloud/`，私钥由 Certbot 限制为 root。

## 两阶段上线顺序

首次部署不要同时启用所有变量，按下面顺序隔离风险：

1. 备份数据库、服务器 `.env`、Cron 与 systemd unit。
2. 部署代码，保持 `REDIS_CACHE_ENABLED=false` 和单个 Uvicorn worker。
3. 运行 migration plan、`--apply`、`--check`，完成首次技术特征表刷新后再启动 API 和 worker。
4. 验证全部现有页面、历史结果查询及舆情选股队列。
5. 在服务器用真实 Token 执行 Tushare 只读权限探针。
6. 完成无 Redis 烟雾测试后，才安装并启用下面的 Redis 缓存配置。
7. Redis 故障降级和压测通过后，再评估是否将 API 调整为两个 worker。

应用默认使用进程内 TTL 缓存，不依赖 Redis 启动。`deploy/env/stock-analysis.env.example` 是无密钥模板；生产文件应为 root-only，不能提交仓库。

## 可选 Redis 缓存

Redis 只监听 loopback、上限 192 MiB、使用 LFU 淘汰，并关闭 RDB/AOF。MySQL 始终是事实源；Redis 数据可以随时清空或禁用。

```bash
apt-get install -y redis-server
install -o root -g root -m 644 deploy/redis/stock-analysis.conf /etc/redis/stock-analysis.conf
redis-server /etc/redis/stock-analysis.conf --test-memory 2
systemctl restart redis-server.service
redis-cli -h 127.0.0.1 ping
```

确认本机监听和内存策略后，在服务器环境文件中设置：

```text
CACHE_ENABLED=true
CACHE_BACKEND=memory
REDIS_CACHE_ENABLED=true
REDIS_URL=redis://127.0.0.1:6379/0
```

重启 API 后检查 `/api/health` 的 `cache_mode=redis`、`redis_status=ready`。停止 Redis 再访问热点接口，应自动回落进程内缓存且业务接口仍可用；若热点 P95 没有明显改善，恢复 `REDIS_CACHE_ENABLED=false`。

## systemd

```bash
install -o root -g root -m 644 deploy/systemd/stock-analysis-api.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-backtest-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-selection-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-portfolio-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-durable-task-worker.service /etc/systemd/system/
.venv/bin/python -m app.orchestration.migrate
.venv/bin/python -m app.orchestration.migrate --apply
.venv/bin/python -m app.orchestration.migrate --check
.venv/bin/python scripts/refresh_stock_technical_feature_daily.py
systemctl daemon-reload
systemctl enable --now stock-analysis-api.service stock-analysis-backtest-worker.service stock-analysis-selection-worker.service stock-analysis-portfolio-worker.service stock-analysis-durable-task-worker.service
```

`app.orchestration.migrate` 是唯一 schema 入口，默认只输出 plan，只有 `--apply` 修改数据库。当前 25 个有序 migration 使用持久 MySQL advisory lock、状态/checksum 和失败记录；再次执行不会重复应用。五个 systemd unit 都在 `ExecStartPre` 运行 `--check`，有 pending/checksum mismatch 时拒绝启动。cron 安装脚本在真正写入 crontab 前也执行同一检查。`0023` 只追加舆情数据一致性、候选快照与运维读模型表；`0024` 追加中性日技术特征读模型及选股结果高频组合索引；`0025` 新增通用 `durable_task` 持久队列，不修改旧 migration/checksum。

首次应用 `0024` 后、启动 API/selection worker 前，必须执行一次 `refresh_stock_technical_feature_daily.py`，从本地 `daily_kline` 幂等生成最新交易日的 `stock_technical_feature_daily`。检查命令输出为成功且 `published_rows` 大于 0；若尚无日线数据，先完成日线同步后重跑，技术特征表未初始化时不得开启 `USE_SENTIMENT_READ_MODEL=true`。需要指定历史交易日时可使用 `--trade-date YYYY-MM-DD`。

### 舆情候选快照物化

迁移和技术特征初始化完成后，先保持 `USE_SENTIMENT_READ_MODEL=false`，手动生成一份稳定版快照：

```bash
.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py \
  --strategy-id a_share_sentiment
```

该任务只在一个 MySQL `REPEATABLE READ` 一致性快照中读取全市场输入，并调用 `StockSelector` 的本地确定性核心；不会调用 Tushare、AkShare、Tavily 或 DeepSeek，也不会保存普通 `selection_result`。它按活跃股票全集与策略必需数据集在同一参考交易日的代码交集计算覆盖率，盘中还要求非陈旧实时行情覆盖：

- 覆盖率低于 `98%`、必需批次为空、时间晚于决策时点、应用/数据库时区不一致或运行跨越交易时钟边界时，命令非零退出，且不会创建或替换 `ready/passed` 快照。
- 每个输入批次、完整 selector 读视图和输出均写入内容哈希与来源链路；MySQL 是事实源，Redis/进程内 pointer 只作加速。
- 默认只生产冻结稳定版 `a_share_sentiment / 0.4.4`。影子版必须显式执行 `--strategy-id a_share_sentiment_v05 --allow-shadow`，该参数不会把 v0.5 开放给 API。
- 稳定版与影子版对照使用 `--dual-run`：两个策略共享同一个 MySQL 一致性读事务、决策时间和必需数据集覆盖审计，各自写独立快照，且都不写 `selection_result`。输出中的 `dual_input_hash` 用于证明同批输入。
- 命令成功后，先在 MySQL 确认最新 manifest 为 `ready/passed`、`coverage_ratio>=0.98`，再开启 `USE_SENTIMENT_READ_MODEL=true`。默认 memory 缓存跨进程不共享，因此 API/worker 必须以轻量 manifest 查询固定 `input_snapshot_id`，不能把缓存 pointer 当事实源。

Cron 安装脚本在交易时段于舆情与实时数据任务之后周期性物化，并在 `18:30` 因子更新、`18:40` 技术特征刷新、`18:45` 数据质量审计之后于 `18:55` 生成收盘最终快照，避免在 2C4G 服务器上叠加重任务。先检查而不安装：

```bash
bash scripts/setup_kline_cron.sh --print-only | grep materialize_sentiment_candidate_snapshot
```

手动生成同批影子对照：

```bash
.venv/bin/python scripts/materialize_sentiment_candidate_snapshot.py --dual-run
```

日志位于 `logs/sentiment_candidate_snapshot_materialize.log`；退出码 `2` 表示输入质量门未通过，旧完整快照仍可继续读取。

### 本地 MySQL 读模型物化

`0023` 中的 `stock_realtime_rank_snapshot`、`tracking_summary_daily` 和
`operational_status_snapshot` 由一个统一命令刷新。该命令只读取已经落库的
MySQL 表，不导入或调用 Tushare、AkShare、新闻源、Tavily、DeepSeek；每个
模型都在自己的数据库事务内删除同一业务快照并批量重建，因此失败会整体
回滚，同一输入或同一分钟重跑不会产生重复业务行。

```bash
.venv/bin/python scripts/refresh_operational_read_models.py --models all
.venv/bin/python scripts/refresh_operational_read_models.py \
  --models realtime-rank,operational-status --rank-limit 100
.venv/bin/python scripts/refresh_operational_read_models.py \
  --models tracking-summary --summary-date 2026-07-21
```

- 实时排名从 `stock_realtime_snapshot` 的单一最新 `batch_id`，以及本地最新的
  `stock_realtime_moneyflow_snapshot`、`stock_popularity_snapshot` 生成涨幅、
  成交额、净流入和热度四类榜单；`snapshot_id` 由来源内容确定。
- Tracking 日摘要从近 365 天去重后的 `selection_result` 与 `daily_kline`
  计算 1/3/5/20 日成熟数、胜率和平均收益。当前库没有可可靠回放且与每只
  入选股票同时间对齐的基准序列，因此 `avg_excess_*` 保持 `NULL`，原因写入
  `summary_json`；不得把这些空值解释为零超额收益。
- 运维快照只投影 `task_run_log` 和 `source_batch_manifest`。快照按捕获分钟
  幂等替换；如果两个来源表均为空，会发布一个 `unknown/warning` 骨架行，
  而不是伪造健康状态。
- 容量治理在成功物化的同一事务内执行固定策略：
  `stock_realtime_rank_snapshot` 删除早于最新物化交易日 3 天的数据，
  `operational_status_snapshot` 删除早于本次捕获时间 7 天的数据；两个
  `INTERVAL` 均硬编码在 SQL 中，不接受命令行覆盖。`tracking_summary_daily`
  作为每日策略观察历史不在该任务中清理。

Cron 在交易时段以 5 分钟周期、相对上游任务偏移 4 分钟刷新排名和运维
状态，并在交易日 19:00 运行一次三个模型的收盘物化。先检查而不安装：

```bash
bash scripts/setup_kline_cron.sh --print-only | grep refresh_operational_read_models
```

任务日志位于 `logs/operational_read_models_refresh.log`，System 页任务名为
`operational_read_models_refresh`。首次上线应在 migration 25/25、技术特征
初始化完成后手动执行一次 `--models all`，确认三个返回项无异常，再安装
cron；该命令不会证明生产 SLO，延迟仍需在云服务器验收。

选股 schema migration 必须先于 API/worker 切换执行。API 只写入 `queued` 任务，selection worker 负责原子 claim、心跳、取消与 stale recovery；重启 API 不会丢失排队任务。

持仓建议 schema 迁移也必须先于服务切换执行。API 只固化输入快照并写入 `queued`，portfolio worker 负责 DeepSeek 调用、心跳、取消与 stale recovery；同一持仓同时只允许一个活跃建议任务。

分钟线刷新、保存后分钟线补全和 Tracking 深度复盘统一写入 `durable_task`。API 不执行外部 Provider；`stock-analysis-durable-task-worker.service` 负责固定 job type 分发、10 秒任务心跳和 5 分钟 stale recovery。Tracking 的 `ai_advice_snapshot` 与对应队列行在同一 MySQL 事务创建；重排/重试耗尽时会同步恢复为 `queued` 或终结为 `failed`。Redis 关闭或故障不影响该队列。

AkShare 分钟线调用运行在 `spawn` 子进程中，不继承 API/worker 的 SQLAlchemy 连接池。`DURABLE_INTRADAY_TIMEOUT_SECONDS` 默认 120 秒（允许 10～600 秒）；超时后 worker 先 terminate、必要时 kill 子进程，再把任务记为失败，因此不会留下继续写库的后台线程。DeepSeek 请求仍使用 Provider 层 90 秒网络超时。

统一 migration 的 `0016` 增加原有三类 worker 的进程级租约、统一错误/日汇总表，并补齐 backtest 的 attempt、稳定错误码和 active 幂等列；`0025` 将 API 重型补全任务纳入同一租约/readiness 合同。四类 worker 启动后会持续更新空闲/运行心跳；`GET /api/readiness` 在 migration、数据库或任一必要 worker 不可用时返回 503，`GET /api/health` 仍保持不访问数据库的轻量存活检查。

空库重建 smoke 只允许显式配置、名称精确为 `stock_migration_smoke` 或以 `stock_migration_smoke_` 开头且首次零表的独立数据库；它会跑两遍 migration 并校验第二遍零变更，不会创建或删除数据库：

```bash
DB_NAME=stock_migration_smoke \
  .venv/bin/python -m app.orchestration.migration_smoke \
  --database stock_migration_smoke
```

当前生产数据库位于远端，应用账号不能自行创建数据库，独立 smoke 库仍需由数据库侧 provision。空库 smoke 应验证到 25/25，第二遍始终要求 `applied_now=0`。该工具要求首次零表；再次完整演练应重新 provision 空库或使用新的安全后缀库名，不会自动清表或删库。

Tushare 实时分钟权限可在部署前用只读探针检查。探针没有显式 `--token-env` 时不会导入 SDK 或访问网络，也不接受/输出命令行 token：

```bash
read -rsp 'Tushare token: ' TUSHARE_RT_PROBE_TOKEN && printf '\n'
export TUSHARE_RT_PROBE_TOKEN
.venv/bin/python scripts/probe_tushare_realtime_permissions.py \
  --token-env TUSHARE_RT_PROBE_TOKEN --ts-code 600000.SH --freq 1MIN
unset TUSHARE_RT_PROBE_TOKEN
```

完整安全边界和退出码见 `docs/sentiment_data_consistency_and_realtime_probe.md`。

模板按当前服务器路径 `/root/.openclaw/workspace/stock-analysis` 编写；迁移目录时先统一替换路径并运行 `systemd-analyze verify`。

## 云端验证

首次启用按“无 Redis 单 worker → Redis 故障降级 → 并发压测 → 可选双 worker”的顺序验收，不在本地开发机代替生产结论：

1. Redis 关闭时验证 `/api/health`、`/api/readiness`、全部保留页面、历史结果查询、Selection Worker 和 `0.4.4` 固定快照烟雾测试。
2. 确认 migration 为 25/25、技术特征初始化命令成功且最新交易日 `published_rows` 大于 0，并确认 durable task worker 在 readiness 中为 healthy。
3. 启用 Redis 后验证缓存命中；停止 Redis 验证自动回落内存缓存、SSE 退回轮询，选股和保存仍可用。
4. 使用 20 个并发页面会话持续 30 分钟，同时运行数据同步和一次完整舆情选股，记录请求 P95、错误率、CPU、内存、Swap、数据库连接和数据新鲜度。
5. Redis 未使热点 P95 明显改善时关闭 Redis；两个 API worker 导致内存不足或数据库连接超过 70% 时回退单 worker。

完整接口 SLO 与资源阈值见 `docs/sentiment_remediation_v2_implementation.md` 的“云端验收”。

## 应用登录凭据

站点使用一个管理员账号、PBKDF2 密码哈希和 HMAC 签名会话。配置脚本使用
隐藏交互输入，不接收命令行明文密码；更新密码时默认轮换会话密钥，让旧
浏览器会话全部失效。

```bash
.venv/bin/python scripts/configure_site_auth.py --username your_username
stat -c '%a %U:%G %n' .env
```

`.env` 必须为 `600 root:root`。生产必须设置
`SITE_AUTH_COOKIE_SECURE=true`。会话默认 7 天过期；写请求同时校验签名会话
和 CSRF token。Nginx 不再配置 `auth_basic`，否则浏览器原生弹窗会先于
应用登录页出现。

### 从 Basic Auth 安全切换到应用登录

切换顺序不能颠倒，避免在旧应用仍运行时先撤掉 Nginx 防线：

1. 备份生产 `.env`、Nginx 站点配置和原 `.htpasswd`，但暂时保留线上
   `auth_basic`。
2. 运行 `configure_site_auth.py` 配置凭据，确认 `.env` 为 `600 root:root`。
3. 部署并重启新 FastAPI 服务；此时公网仍由原 Basic Auth 保护。
4. 直接访问 loopback `127.0.0.1:8000`，验证匿名页面 303、匿名 API 401、
   正确登录 200、无 CSRF 写请求 403、退出后再次 303。
5. 上述应用层验证全部通过后，才安装不含 `auth_basic` 的新 Nginx 配置，
   运行 `/usr/sbin/nginx -t` 并 reload。
6. 最后从公网无旧凭据的新浏览器会话验证登录页、原路径回跳和退出登录。

回滚时顺序相反：先恢复并 reload 原 Nginx Basic Auth，让外围保护重新生效，
再回滚 FastAPI 代码或环境配置。

## TLS 自动签发与续期

首次部署先安装 Certbot、准备 webroot，并用临时 HTTP 配置完成 HTTP-01 验证。将示例邮箱替换为实际运维地址：

```bash
apt-get install -y certbot
install -d -o root -g root -m 755 /var/www/letsencrypt/.well-known/acme-challenge
install -o root -g root -m 644 deploy/nginx/stock-analysis-acme-bootstrap.conf /etc/nginx/sites-available/stock-analysis
ln -sfn /etc/nginx/sites-available/stock-analysis /etc/nginx/sites-enabled/stock-analysis
/usr/sbin/nginx -t
systemctl reload nginx.service
certbot certonly \
  --webroot --webroot-path /var/www/letsencrypt \
  --cert-name yzysstock.cloud \
  -d yzysstock.cloud -d www.yzysstock.cloud \
  --non-interactive --agree-tos \
  --email admin@example.com \
  --key-type rsa --rsa-key-size 2048
install -d -o root -g root -m 755 /etc/letsencrypt/renewal-hooks/deploy
install -o root -g root -m 755 deploy/certbot/reload-nginx.sh /etc/letsencrypt/renewal-hooks/deploy/stock-analysis-reload-nginx
systemctl enable --now certbot.timer
```

续期使用同一个 webroot；deploy hook 只在 `/usr/sbin/nginx -t` 成功后 reload。安装最终 Nginx 配置后做一次完整模拟：

```bash
certbot certificates
certbot renew --dry-run --run-deploy-hooks
systemctl list-timers certbot.timer --all --no-pager
```

不要把 `/etc/letsencrypt`、私钥、账户文件或应用登录凭据复制进仓库。

## Nginx

```bash
install -o root -g root -m 644 deploy/nginx/stock-analysis-rate-limit.conf /etc/nginx/conf.d/
install -o root -g root -m 644 deploy/nginx/stock-analysis.conf /etc/nginx/sites-available/stock-analysis
ln -sfn /etc/nginx/sites-available/stock-analysis /etc/nginx/sites-enabled/stock-analysis
/usr/sbin/nginx -t
systemctl reload nginx.service
```

验证口径：

- 未认证 `GET /api/health` 返回 200。
- 未认证页面返回 303 并跳转到 `/login?next=...`，其他 API 返回 JSON 401。
- 登录页、静态资源和 favicon 无需认证且不出现浏览器 Basic Auth 弹窗。
- 正确凭据登录后设置 `Secure + HttpOnly + SameSite` 会话 Cookie，原页面返回 200。
- 无 CSRF header 的已登录写请求返回 403，站内前端自动附加 CSRF token。
- 退出登录后会话 Cookie 被清除，再次访问受保护页面会回到登录页。
- 选股、回测、DeepSeek 深度复盘和持仓建议接口按 IP 限制为每分钟 6 次、突发 2 次，超限返回 429。
- HTTP-01 路径 `/.well-known/acme-challenge/` 免认证且不重定向，其余 HTTP 请求 301 到 HTTPS。
- 远端证书 SAN 同时包含 `yzysstock.cloud` 与 `www.yzysstock.cloud`，证书指纹与本机 Certbot live 文件一致。

## cron

`scripts/setup_kline_cron.sh` 是股票任务的唯一安装入口。它会保留不属于本项目的既有 cron，去重后写入当前项目任务：

```bash
bash scripts/setup_kline_cron.sh --print-only
bash scripts/setup_kline_cron.sh
```

先审查 `--print-only` 输出，再执行安装。
安装模式会先执行 migration `--check`；schema 未就绪时不会改写 crontab。
日线增量任务在 02:00 完成后，02:05 运行 `refresh_stock_technical_feature_daily.py`；工作日 `18:40` 在晚间因子数据完成后再次刷新，只从本地 MySQL `daily_kline` 幂等生成技术特征读模型。舆情快照物化任务在 `18:45` 数据质量审计之后于 `18:55` 发布收盘最终快照。日志分别写入 `logs/stock_technical_feature_daily_refresh.log` 和 `logs/sentiment_candidate_snapshot_materialize.log`。可先用以下命令只读确认 Cron 条目：

```bash
bash scripts/setup_kline_cron.sh --print-only | grep stock_technical_feature_daily_refresh
bash scripts/setup_kline_cron.sh --print-only | grep materialize_sentiment_candidate_snapshot
```

任务 retention 默认每天 04:15 串行执行：`task_run_log` 明细保留 90 天并长期保留日汇总，已终结 `durable_task` 队列行保留 30 天，结构化错误日汇总保留 365 天；正式回测、验证基线、持仓建议摘要/结果、Tracking AI 结果和已保存选股结果不被清理。手动执行时默认 dry-run，只有显式 `--apply` 才修改数据：

```bash
.venv/bin/python scripts/run_job_retention.py
.venv/bin/python scripts/run_job_retention.py --apply
```

## 日志轮转

```bash
install -d -o root -g root -m 755 /etc/logrotate.d
install -o root -g root -m 644 deploy/logrotate/stock-analysis /etc/logrotate.d/stock-analysis
/usr/sbin/logrotate --debug /etc/logrotate.d/stock-analysis
```

仓库日志按天轮转 14 份、压缩，单文件超过 50 MiB 也会提前轮转。systemd worker 日志继续由 journald 管理。

## 回滚

应用回滚按影响从小到大执行：

1. Redis 异常或无性能收益：设置 `REDIS_CACHE_ENABLED=false` 并重启 API，继续使用内存缓存。
2. 快照读路径异常：设置 `USE_SENTIMENT_READ_MODEL=false`，切回稳定旧查询路径和上一个 `ready/passed` 快照。
3. v0.5 异常：保持 `shadow_only` 或移出影子任务，不改变 `0.4.4` 与历史证据。
4. 新代码异常：部署上一个已验证代码版本；保留新增表，禁止修改、删除或反向执行已经应用的 migration。
5. 回滚后验证全部页面/API、Selection Worker、`/api/health` 和 `/api/readiness`；旧策略历史表至少保留 90 天。

每次覆盖现网配置前还要备份 `/etc/nginx/sites-available/stock-analysis`、原证书目录、`/etc/nginx/conf.d/stock-analysis-rate-limit.conf` 和 htpasswd。证书切换失败时恢复旧 SSL 路径和旧证书；回滚后必须先执行 `/usr/sbin/nginx -t`，通过后再 `systemctl reload nginx.service`。
