# 当前服务器部署记录（2026-05-06）

## 访问地址

- 公网首页：`https://www.yzysstock.cloud/`（Basic Auth）
- 健康检查：`https://www.yzysstock.cloud/api/health`（免认证）

## 运行方式

FastAPI 后端由 systemd 托管：

- service：`stock-analysis-api.service`
- 工作目录：`/root/.openclaw/workspace/stock-analysis`
- Python 虚拟环境：`/root/.openclaw/workspace/stock-analysis/.venv`
- 入口：`app.api.main:app`
- 监听：`127.0.0.1:8000`

关键命令：

```bash
systemctl status stock-analysis-api.service
systemctl restart stock-analysis-api.service
journalctl -u stock-analysis-api.service -n 100 --no-pager
```

## 反向代理

Nginx 负责公网 HTTPS 访问，HTTP 自动跳转：

- 配置文件：`/etc/nginx/sites-available/stock-analysis`
- 启用链接：`/etc/nginx/sites-enabled/stock-analysis`
- 监听：`80` / `443`
- 域名：`www.yzysstock.cloud` / `yzysstock.cloud`
- upstream：`http://127.0.0.1:8000`

关键命令：

```bash
/usr/sbin/nginx -t
systemctl status nginx
systemctl reload nginx
```

## 已完成验证

2026-05-06 重装系统后重新部署并验证：

- `stock-analysis-api.service`：active + enabled
- `nginx`：active + enabled
- DNS：`www.yzysstock.cloud` / `yzysstock.cloud` 解析到当前服务器公网 IP `43.159.168.45`
- 本地 API：`http://127.0.0.1:8000/api/health` 返回 `{"status":"ok"}`
- 公网 API：`http://www.yzysstock.cloud/api/health` 返回 `200 OK`
- 公网页面：`http://www.yzysstock.cloud/` 返回股票分析控制台首页
- 关键前端接口已验证：
  - `/api/dashboard/summary?limit=5`
  - `/api/strategies`
  - `/api/strategies/detail`
  - `/api/system/status`
  - `/api/tracking/latest?limit=5&instrument_type=stock`
  - `/api/tracking/filters`
  - `/api/selection/results?strategy_id=lowvol_reversal`

## 注意事项

- 当前 HTTPS 已启用；除 `/api/health` 外的页面与 API 均需 Basic Auth。
- `nginx` 二进制在 `/usr/sbin/nginx`，当前 shell PATH 里可能没有 `/usr/sbin`。
- 本次仅做部署恢复，没有推送 GitHub；仓库仍需先清理 token/password 等敏感内容后再考虑 push。

## HTTPS 配置更新（2026-05-06 16:05）

已使用大X提供的证书完成 HTTPS 配置：

- 原始证书目录：`/root/.openclaw/workspace/yzysstock.cloud_nginx/`
- 证书：`yzysstock.cloud_bundle.pem` / `yzysstock.cloud_bundle.crt`
- 私钥：`yzysstock.cloud.key`
- Nginx 使用路径：
  - `/etc/nginx/ssl/yzysstock.cloud/fullchain.pem`
  - `/etc/nginx/ssl/yzysstock.cloud/privkey.key`
- 证书域名：`yzysstock.cloud`、`www.yzysstock.cloud`
- 有效期：`2026-05-06` 到 `2026-08-03`
- HTTP 80 已配置为自动 301 跳转到 HTTPS。

已验证：

- `https://www.yzysstock.cloud/api/health` 返回 `200 OK`，内容 `{"status":"ok"}`
- `https://www.yzysstock.cloud/` 返回首页 HTML
- `http://www.yzysstock.cloud/api/health` 返回 `301` 到 HTTPS
- TLS 证书链可读取，SAN 包含 `yzysstock.cloud` 和 `www.yzysstock.cloud`

历史说明：这张 TrustAsia 手工证书原定于 `2026-08-03 23:59:59 GMT` 过期，已在 2026-07-18 切换为下节记录的 Let’s Encrypt 自动续期链路；Nginx 不再引用这里的旧路径。

## HTTPS 自动续期切换（2026-07-18）

现网已从手工 TrustAsia 证书切换为 Let’s Encrypt webroot 自动续期：

- 证书域名：`yzysstock.cloud`、`www.yzysstock.cloud`。
- 当前 issuer：`Let's Encrypt / YR2`。
- 当前有效期：`2026-07-18 07:01:29 GMT`～`2026-10-16 07:01:28 GMT`。
- 当前证书：`/etc/letsencrypt/live/yzysstock.cloud/fullchain.pem`。
- 当前私钥：`/etc/letsencrypt/live/yzysstock.cloud/privkey.pem`，目标文件权限 `0600 root:root`。
- HTTP-01 webroot：`/var/www/letsencrypt`；挑战路径免认证、不跳转，其余 HTTP 请求仍 301 到 HTTPS。
- 自动续期：系统 `certbot.timer` 已 enabled/active，每日运行两轮。
- deploy hook：`/etc/letsencrypt/renewal-hooks/deploy/stock-analysis-reload-nginx`；只有 Nginx 配置校验成功才 reload。
- Certbot 账户未登记运维邮箱；失败兜底依赖 systemd 日志和 2026-09-18 09:30 的 OpenClaw 一次性续期复核任务。

上线前备份位于：

```text
/root/.openclaw/workspace/.restore-safety/stock-analysis-cert/20260718_155759/
```

旧 `/etc/nginx/ssl/yzysstock.cloud/` 文件暂未删除，可在紧急回滚时恢复原 Nginx SSL 路径。自动续期闭环已用下面的命令完整验证：

```bash
certbot renew --dry-run --run-deploy-hooks
/usr/sbin/nginx -t
systemctl list-timers certbot.timer --all --no-pager
```

最终验证：两域名远端证书指纹与本机 Certbot live 文件一致；HTTP→HTTPS 为 301；两个 HTTPS `/api/health` 均为 200；受保护页面未认证仍为预期 401；Nginx active。

## 公网保护更新（2026-07-15 23:02）

现网已启用：

- 全站 Nginx Basic Auth，`/api/health` 免认证。
- 用户名：`dax`。
- 明文随机密码：`/root/.config/stock-analysis/basic-auth-password`，权限 `0600`，仅 root 可读。
- Nginx 哈希文件：`/etc/nginx/.htpasswd-stock-analysis`，权限 `0640 root:www-data`。
- 限流 zone：`stock_analysis_heavy`，每 IP 每分钟 6 次、突发 2 次、超限 HTTP 429。
- 受限入口：选股运行、回测运行、DeepSeek 深度复盘、持仓建议刷新和结果评估。

现网文件：

- `/etc/nginx/sites-available/stock-analysis`
- `/etc/nginx/conf.d/stock-analysis-rate-limit.conf`
- `/etc/nginx/.htpasswd-stock-analysis`

无密钥部署模板已纳入：

- `deploy/nginx/`
- `deploy/systemd/`
- `deploy/README.md`
- `scripts/setup_kline_cron.sh --print-only`

上线备份位于 `/root/.openclaw/workspace/.restore-safety/stock-analysis-nginx-20260715_225950/`。

验证结果：健康检查免认证 200；首页/API 未认证 401；正确凭据 200；限流烟测依次为 `422, 422, 422, 429, 429`。Nginx 配置校验成功、平滑 reload 后仍为 active，`NRestarts=0`。

## 页面传输优化（2026-07-16）

现网 Nginx 与仓库模板已同步增加：

- 对 JSON、JavaScript、CSS、XML、SVG 等文本资源启用 gzip，压缩等级为 5，最小响应大小为 1 KiB。
- `/static/` 静态资源增加 1 小时浏览器缓存，继续由 FastAPI 提供文件、Nginx 负责传输缓存头和压缩。
- 现网配置：`/etc/nginx/sites-available/stock-analysis`。
- 无密钥模板：`deploy/nginx/stock-analysis.conf`。

页面接口改为请求精简投影：

- 首页：`/api/dashboard/summary?limit=8&compact=true`，响应缓存 30 秒。
- 跟踪复盘：`/api/tracking?compact=true&include_runs=false`，汇总缓存 60 秒，并在保存、删除或统计状态变更时主动失效。
- 回测任务列表：`/api/backtest/runs?limit=20&compact=true`，列表不再附带完整 summary/equity curve；查看单次任务详情时仍返回完整数据。

同一批数据下的本机实测：

- 首页 JSON：约 143 KB 降到 21.8 KB；缓存命中约 1 毫秒。
- 跟踪复盘 JSON：约 404 KB 降到 8.9 KB；冷请求约 1.2 秒。
- 20 条回测任务：约 118 KB 降到 28.5 KB，响应约 10 毫秒。

公网 gzip 传输实测：首页 JSON 约 6.1 KB、`home.js` 约 6.8 KB、`pages.css` 约 28.6 KB；静态资源返回 `Cache-Control: max-age=3600`。Basic Auth 与重型接口限流规则保持不变。

## 受控回测验证基线（2026-07-16）

管理脚本：

```bash
cd /root/.openclaw/workspace/stock-analysis

# 默认仅预演，不写库
PYTHONPATH=. .venv/bin/python scripts/run_backtest_validation_baseline.py \
  --baseline-id example_baseline \
  --start-date 2026-04-24 \
  --end-date 2026-04-27 \
  --return-mode 1d \
  --max-trade-days 2

# 明确确认后才提交系统测试任务
PYTHONPATH=. .venv/bin/python scripts/run_backtest_validation_baseline.py \
  --baseline-id example_baseline \
  --start-date 2026-04-24 \
  --end-date 2026-04-27 \
  --return-mode 1d \
  --max-trade-days 2 \
  --execute

# 重建已有基线报告，不创建任务
PYTHONPATH=. .venv/bin/python scripts/run_backtest_validation_baseline.py \
  --report-only example_baseline
```

规则：

- 基线任务固定写入 `is_system_test=1`，并以 `validation_baseline_id` 分组。
- 同一 baseline ID 不允许重复执行；已有任务使用 `--report-only` 查询。
- 默认页面/API 列表不包含系统测试；排障时显式使用 `include_system_tests=true`。
- 脚本检查队列空闲、内存、Swap、最大交易日数，并串行提交策略。
- 变更 `BacktestRequest` 或 worker 消费逻辑后，必须先重启 `stock-analysis-backtest-worker.service`，再提交新格式任务。
- 基线报告只用于工程验证，不能修改策略 `validation_status`。

本次基线 `b3_20260716_20260424_20260427_1d_v2` 已成功完成两条系统测试。执行中一次旧 worker 因请求 schema 未刷新而退出，同一任务经 stale recovery 恢复成功；worker 已增加单任务异常边界并重启加载最新代码。

## Selection 独立任务 Worker（2026-07-16）

现网新增服务：

- unit：`stock-analysis-selection-worker.service`
- 代码入口：`app.stock_selection.worker`
- 工作目录：`/root/.openclaw/workspace/stock-analysis`
- 轮询间隔：3 秒
- 部署模板：`deploy/systemd/stock-analysis-selection-worker.service`

部署/升级顺序：

```bash
cd /root/.openclaw/workspace/stock-analysis
.venv/bin/python -m app.orchestration.selection_run_schema
install -o root -g root -m 644 deploy/systemd/stock-analysis-selection-worker.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stock-analysis-selection-worker.service
systemctl restart stock-analysis-api.service
```

运行语义：

- `POST /api/selection/run` 只写入 queued 并返回 HTTP 202；同步执行已禁用。
- active 同参数请求按最新数据交易日和完整参数去重，终态后释放 active key。
- worker 原子 claim，记录 worker/锁/心跳；默认最多尝试 2 次，心跳超过 15 分钟按 attempt 分流为重新排队或失败。
- `POST /api/selection/runs/{run_id}/cancel`：queued 立即取消；running 设置取消标记，在当前同步计算边界结束后进入 cancelled。
- 选股任务只保存 preview `result_json`；正式跟踪记录继续由用户在页面按条保存。
- API 重启不影响 queued/running selection；修改任务 payload schema 后要先确保 worker 加载新代码，再提交新格式任务。

当前验证：schema 必需列/索引齐全，历史 20 条 success 未改变；API、selection worker 与 backtest worker 均 active、`NRestarts=0`，active selection 队列为 0。

## Portfolio Advice 独立任务 Worker（2026-07-16）

现网新增服务：

- unit：`stock-analysis-portfolio-worker.service`
- 代码入口：`app.portfolio.worker`
- 工作目录：`/root/.openclaw/workspace/stock-analysis`
- 轮询间隔：3 秒
- 部署模板：`deploy/systemd/stock-analysis-portfolio-worker.service`

部署/升级顺序：

```bash
cd /root/.openclaw/workspace/stock-analysis
.venv/bin/python -m app.orchestration.portfolio_schema
install -o root -g root -m 644 deploy/systemd/stock-analysis-portfolio-worker.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stock-analysis-portfolio-worker.service
systemctl restart stock-analysis-api.service
```

运行语义：

- `POST /api/portfolio/{position_id}/advice/refresh` 只固化提交时输入快照并创建 queued，不再启动 FastAPI `BackgroundTasks`。
- 同一持仓同时最多一个 active advice run；重复点击返回同一个 run，终态后释放 active key。
- worker 原子 claim，独立记录 worker/锁/心跳；默认最多尝试 2 次，心跳超过 5 分钟按 attempt 分流为重新排队或失败。
- `GET /api/portfolio/advice/runs/{run_id}` 查询真实任务状态；`POST /api/portfolio/advice/runs/{run_id}/cancel` 取消 queued/running 任务。
- queued 立即取消；running 在当前 DeepSeek HTTP 调用边界结束后协作取消，取消任务不会保存 succeeded 建议。
- API 重启不影响 queued/running advice；修改输入快照或提示词消费逻辑后，应先重启 portfolio worker，再允许新任务提交。

当前验证：55 项回归通过；坏快照、active 双提交和 queued 取消均使用无外部 AI 调用的烟测完成并清理。历史 6 条 succeeded 建议未改变；API、portfolio/selection/backtest worker 均 active，active advice 队列为 0。

## Worker Readiness 与任务保留治理（2026-07-16）

现网三个 worker 已统一接入进程租约和共享任务状态：

- `worker_runtime_heartbeat` 每 10 秒记录 worker 的 idle/running 状态、当前任务和最近任务时间；45 秒未续租才判为进程失联。
- `GET /api/health` 保持为不查数据库的 liveness；受 Basic Auth 保护的 `GET /api/readiness` 检查 MySQL、worker、队列、关键日更和数据日期，未就绪时返回 HTTP 503。
- backtest、selection、portfolio advice 的任务 stale 阈值分别为 30、15、5 分钟；进程租约与单任务心跳分开判定。
- backtest 已和另外两类任务统一 active 幂等、原子 claim、attempt/max_attempts、取消、stale recovery、phase 和 `error_code`；`save=false` 同步回测入口已禁用并返回 422。

数据库升级与检查：

```bash
cd /root/.openclaw/workspace/stock-analysis
PYTHONPATH=. .venv/bin/python -m app.jobs.schema
curl http://127.0.0.1:8000/api/health
curl -u '<user>:<password>' http://127.0.0.1:8000/api/readiness
```

任务保留脚本默认只预演，明确传入 `--apply` 才会修改数据：

```bash
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py --apply
```

当前策略为：任务明细和 selection 任务壳 90 天；非验证基线 system test 回测 90 天；AI 原始响应 30 天、输入快照 90 天；结构化错误日汇总 365 天。正式回测、验证基线、已保存选股结果、持仓建议摘要与 outcome 均受保护。cron 每天 04:15 串行执行，仓库日志由 `/etc/logrotate.d/stock-analysis` 按日/50 MiB 轮转、保留 14 份并压缩。

首次受控执行收口 33 条超过 24 小时的 abandoned task，把 5,805 次历史失败聚合为 140 个日级错误组，仅清理 2 条过期 AI 原始响应。首次聚合曾触发 MySQL `GROUP_CONCAT` 1260，删除阶段尚未开始；修为有界、幂等聚合后重跑成功。

部署验证：63 项回归通过；公网认证 readiness 返回 `ready`，三个 worker healthy，queued/running/stale 均为 0；API 与三个 worker 均 active、`NRestarts=0`，可用内存约 2.1 GiB、Swap 约 0.5 MiB。

## 分钟行情与舆情生命周期（2026-07-16）

现网生命周期口径：

- `stock_realtime_snapshot`：每只代码最新一行，含 batch/接收时间/新鲜度/陈旧标记。
- `stock_realtime_intraday`：按交易日分区，全市场 1m raw 保留 2 个交易日。
- `stock_realtime_bar_rollup`：5m/15m OHLCV 保留 90 个交易日。
- `stock_realtime_intraday_tracked`：持仓、已保存选股和跟踪标的 1m 保留 90 个交易日。
- `sector_opinion_daily`：V2 父快照不存重复 JSON，明细写入 `sector_opinion_stock / news_ref / source_ref`；最近 5 个交易日保留全部盘中批次，旧日期只保留每日最后一批，最长 90 个交易日。

运维命令均默认只读预演，明确传入 `--apply` 才修改数据：

```bash
cd /root/.openclaw/workspace/stock-analysis
PYTHONPATH=. .venv/bin/python scripts/run_realtime_lifecycle.py
PYTHONPATH=. .venv/bin/python scripts/run_realtime_lifecycle.py --apply
PYTHONPATH=. .venv/bin/python scripts/run_market_opinion_lifecycle.py
PYTHONPATH=. .venv/bin/python scripts/run_market_opinion_lifecycle.py --apply
```

crontab 已安装：工作日 15:20 运行 realtime lifecycle，工作日 16:05 运行 market opinion lifecycle。前者只有在 5m/15m manifest 均成功且源数据覆盖到 14:55 后才删除过期 raw 分区；后者只有在全部保留快照已转成 V2 且父 JSON 为空后才裁剪旧快照。

首次 raw 分区迁移前后均为 367,231 行，旧表 `stock_realtime_intraday_legacy_20260716124449` 暂留作回滚。`sector_opinion_daily` 历史迁移 dry-run 为 145,097 条：保留 20,552、可裁剪 124,545、待归一化 20,429；实际迁移安排在收盘后后台串行执行。逻辑裁剪不会立刻缩小 InnoDB 表空间文件，若后续需要回收物理磁盘，必须另开维护窗口做 shadow rebuild，不能盘中直接 `OPTIMIZE TABLE`。

现网 API 已切到新 writer/兼容 reader；13:15 的 122 条最新行业快照全部为 `payload_version=2`。D1/D2 回归共 72 项，API 与三个 worker active，readiness 为 `ready`，可用内存约 2.1 GiB、Swap 约 0.5 MiB。

## 统一 Schema Migration（2026-07-16）

唯一结构升级入口：

```bash
cd /root/.openclaw/workspace/stock-analysis
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --apply
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --check
```

默认只输出 plan；`--apply` 使用 MySQL advisory lock 串行应用 pending 版本；`--check` 在 pending/checksum mismatch 时非零退出。现库 `schema_migration` 为 `16/16 applied, pending=0`，首次基线登记约 1 秒，幂等重跑 `applied_now=0`。

四个现网 unit 已安装 `ExecStartPre=.venv/bin/python -m app.orchestration.migrate --check` 并实际验证退出码 0：

- `stock-analysis-api.service`
- `stock-analysis-backtest-worker.service`
- `stock-analysis-selection-worker.service`
- `stock-analysis-portfolio-worker.service`

普通 API、Service、选股候选加载、ingestion 和 cron writer 不再执行 DDL；只有 realtime lifecycle 仍按数据保留策略创建/删除日期分区。系统 readiness/page 展示 migration target/applied/pending，crontab 安装也会先 check。

空库 smoke 命令要求独立、首次零表且名称精确为 `stock_migration_smoke` 或以 `stock_migration_smoke_` 开头，并会执行两遍验证幂等。2026-07-16 已在数据库侧 provision 的 `stock_migration_smoke` 实跑成功：首次应用 16 个 migration、生成 61 张表，第二遍 `applied_now=0`，最终 16/16 ready；生产库迁移快照哈希在执行前后保持一致。测试库保留，不自动清表或删除。禁止改用生产库测试。

上线验证：79 项回归通过；API/三个 worker active、`NRestarts=0`，readiness 返回 `schema_migrations.health=healthy / 16 applied / 0 pending`；13:45 后的新一轮舆情、实时行情、热度与资金流任务在移除运行时 DDL 后继续成功/预期降级运行。

## DQ4 公告日基本面部署（2026-07-17）

- 生产与保留的独立 migration smoke 库均已增量应用 `0018`，最终 `18/18 ready`；smoke 库第二遍 `applied_now=0`。
- 生产库 `stock_fundamental_pit` 已落库 2022 年报至 2026 中报共 `100,423` 个公告版本、`5,766` 只股票；`fundamental_pit_manifest` 的 15 个报告期全部 success。
- crontab 新增每天 `04:40` 运行 `run_fundamental_pit_backfill.py --recent-periods 8`，核心 DQ 夜间审计顺延到 `04:55`；交易日 `18:45` 复核保持不变。
- API 与 backtest worker 已串行重启，两个 unit 的 `ExecStartPre` 均验证 migration 18/18；selection/portfolio worker 未重启且保持健康。
- system test `backtest_lowvol_reversal_20260717_180334_224827` 成功完成 2 个信号日、6 个 picks/6 笔交易，`methodology_version=close_signal_next_open_pit_fundamentals_v4`，并保持 `is_system_test=1 / research_only=true / validation_pending`。
- 本地 health、system status、system 页面与回测结果均为 HTTP 200；公网 health 200，公网 `/system` 未认证返回预期 401。
- DQ4 快照为 `8 pass / 6 warn / 0 fail`，公告日基本面检查自身为 pass；readiness 若在 18:30 因子日更前显示 degraded，仅表示 7 月 17 日日线已收盘而 factor input 暂停在 7 月 16 日，仍 `accepting_jobs=true`。

## DQ5 历史指数成分部署（2026-07-17）

- 生产与保留的独立 migration smoke 库均已增量应用 `0019`，最终 `19/19 ready`；smoke 库第二遍 `applied_now=0`。
- 后台全量回填完成 2023-12 至 2026-06 的上证 50、沪深 300、中证 500 和中证 1000：`57,350` 条成分、31 个快照日，`124/124` 个指数/月 manifest 成功，partial/failed 和四类硬异常均为 0。
- crontab 新增每天 `04:45` 运行 `run_index_constituent_pit_backfill.py --recent-months 3`，核心 DQ 夜间审计顺延到 `05:00`；交易日 `18:45` 复核保持不变。指数任务 1 条、DQ 任务 2 条，无重复。
- API 与 backtest worker 已串行重启，两个 unit 的 `ExecStartPre` 均验证 migration 19/19；`NRestarts=0`。selection/portfolio worker 未重启且保持健康。
- system test `backtest_lowvol_reversal_20260717_203303_252683` 使用沪深 300，成功完成 2 个信号日、6 个 picks/6 笔交易；`methodology_version=close_signal_next_open_pit_index_universe_v5`，并保持 `is_system_test=1 / research_only=true / validation_pending`。窗口收益为负，只作工程 smoke。
- 本地 health、readiness、system status 和回测结果均为 HTTP 200；公网 health 200，公网 `/system` 未认证返回预期 401。readiness 为 `ready / accepting_jobs=true`，三个 worker healthy/idle、三类队列为 0。
- DQ5 快照为 `10 pass / 5 warn / 0 fail`；指数成分检查自身为 pass，12 个代表交易日成员覆盖 `22,200/22,200`，最大快照滞后 17 天。
- 全量 159 项 unittest、Python 编译、前端 JavaScript、cron shell 和 diff 检查均通过。
- 回测默认股票池继续是 `ALL_A`。用户显式选择指数时才读取信号日之前最近的月度权重快照；这不是精确到调仓公告时刻的事件流。

## 冻结策略受控验证部署（2026-07-18）

- migration `0020` 新增 `strategy_validation_protocol`，生产库已为 `20/20 ready`；协议锁定策略配置、方法论、请求/成本/成交约束与实际执行源码 SHA-256，任一漂移均 fail-closed。
- V1 四份协议因缺少源码指纹已保留审计记录并标记 `superseded`；有效历史协议为 `hist_diag_20250701_20260630_lowvol_v2` 和 `hist_diag_20250701_20260630_v13_v2`。
- 低波 run `backtest_lowvol_reversal_20260718_002337_693871` 完成 242 个样本日、714 笔交易，扣费后收益 `-43.4248%`、超额 `-54.1446%`、最大回撤 `-44.4163%`，结论 `historical_diagnostic_fail`。
- 三因子 run `backtest_v13_three_factor_20260718_010249_400303` 完成 242 个样本日、714 笔交易，扣费后收益 `-39.0495%`、超额 `-50.5983%`、最大回撤 `-47.2742%`，结论同为 `historical_diagnostic_fail`。
- 两条 run 基准覆盖均为 100%、收益缺失为 0，配置/方法论/请求/源码结构检查全部通过；失败来自六项表现门槛，不是工程或数据缺口。策略继续保持 `research_only / validation_pending / unvalidated`。
- 真正样本外协议 `prospective_20260720_20270131_lowvol_v2` 与 `prospective_20260720_20270131_v13_v2` 已冻结，2027-01-31 窗口闭合前不执行，也不会根据历史诊断修改参数。
- `GET /api/backtest/validations`、协议详情 API 和 `/backtest` 冻结验证卡片已上线；system validation run 继续从正式回测列表隔离并受 retention 保护。
- 全量 170 项 unittest、Python 编译、前端 JavaScript、migration 与 diff 检查通过。API 与三个 worker 均 active、`NRestarts=0`，readiness `ready / accepting_jobs=true`，migration `20/20`，三类队列为 0。
- 本地 health、验证 API、回测页及静态 JS 均为 HTTP 200；公网 health 200，公网 `/backtest` 未认证返回预期 401。

## 冻结策略失败归因工具（2026-07-18）

- 新增只读 `StrategyFailureAttributionRepository/Service` 和 CLI，不新增 migration、表、cron、API 或 worker，也未修改低波/V13、Selector、Backtest Service/Repository 与股票池政策文件。
- 两份前瞻协议实现指纹复核仍分别命中 `24d192...90e3` 与 `2a66e2...1b7f`，未被本切片污染。
- 低波 1 日毛/净复利为 `-6.7309% / -43.4252%`，1/3/5 日总分 IC 全为负，归类为因子方向失败。
- V13 1 日毛/净复利为 `+0.4824% / -39.0498%`，相邻日留存 `7.6072%`；3 日三个非重叠 offset 净收益为 `-5.7802% / -22.0879% / +12.0806%`，执行周期替换不稳健。
- 12 个代表日完整候选重算显示原阈值通过比例中位数为低波 `38.91%`、V13 `66.55%`，第三与第四名分差仅 `0.07 / 0.09`；本轮未按历史结果抬阈值或挑最佳 offset。
- 全量 176 项 unittest、Python 编译、真实 CLI、migration `20/20` 和冻结指纹检查通过。该工具为离线只读入口，无需重启在线服务。
- 详细记录见 `docs/strategy_failure_attribution_2026-07-18.md`。

## Portfolio Repository 垂直切片（2026-07-16）

持仓模块的 SQL 已从 `app/portfolio/service.py` 收口到 `app/portfolio/repository.py`。Service 继续负责行情兜底、技术指标、纪律规则、AI 建议、缓存失效与结果评分，不再直接打开 MySQL 连接。

`GET /api/portfolio` 现在先读取持仓，再按全部代码批量加载 stock basic、实时行情、每只最近 120 条日线、最新舆情、实时资金流和最新筹码，最后批量加载 AI 建议与结果。真实现网 2 个持仓的 SQL 数为固定 9 条；原实现为 `1 + 6N + 2`，持仓数增加时不再线性放大数据库往返。

上线前对同一实时快照执行旧在线链路与新影子链路逐叶比较，差异数为 0。上线后接口连续 3 次 200，耗时约 40-49ms、响应约 8.7KB、持仓代码保持 `sz.159660 / sh.518850`；全量 84 项回归通过。API 与 portfolio worker 均 active、`NRestarts=0`，migration `ExecStartPre` 退出 0，公网 health 200。

## Tracking Repository 与分页性能（2026-07-16）

Tracking 的 SQL 已从 `app/api/routes/tracking.py` 和 `app/error_learning/tracker.py` 收口到 `app/tracking/repository.py`。分页查询先在 `target_selection` CTE 中按筛选条件、limit/offset 圈定 selection_result ID，再只为目标 ID 计算日线及 raw/tracked 分钟极值；filtered summary 只富化 `include_in_stats=1` 的记录。

当前 stock 口径总记录 176 条、纳入统计 61 条。改造前 limit=10 的冷请求约 7.77 秒、缓存请求约 5.49 秒；上线后分别约 0.313 秒和 0.049 秒，第 2 页约 0.050 秒、filters 约 0.018 秒。旧/新页码和代码顺序一致，filters 完全一致；实时价格相关差异来自验证期间行情继续更新。

全量 88 项回归通过，run/date/strategy/latest 四种查询分支均在真实库验证。API active、`NRestarts=0`，migration `ExecStartPre` 退出 0；Dashboard、selection results 和公网 health 均 200，公网 tracking 未认证仍为 401。

### 跟踪统计 14 个自然日窗口（2026-07-20）

- `selection_result.created_at` 是统计窗口起点；第 14 天整仍纳入，超过完整 `14 × 24` 小时后，持久化把 `include_in_stats` 改为 `0`。页面原有 `tracking_days` 是交易日口径，继续只用于持有期展示，不参与到期判断。
- 到期记录不会删除，仍保留在复盘历史中，但不再进入平均收益、胜率、最大回撤和策略汇总；compact API 返回 `stats_window_expired / stats_age_days / stats_exclusion_reason`，页面显示“超14天自动排除”。
- 过期记录不能手工重新纳入统计，接口返回 `409`；未过期记录仍保留原手工开关。
- 每次读取跟踪复盘时即时补偿更新，现有每天 `04:15` 的 `job_retention` 也会主动执行一次，因此不依赖用户打开页面。
- 首次上线迁移把 `44` 行到期股票记录从纳入统计改为排除；迁移后股票记录共 `182` 行、纳入统计 `23` 行、逾期仍纳入 `0` 行。`201` 项回归、真实库 compact API、到期重纳入 `409`、每日任务 dry-run、本地静态资源和公网 health 均验证通过；API `active`、`NRestarts=0`。

## Dashboard Repository（2026-07-16）

首页三块 read model 已统一通过 `app/dashboard/repository.py` 访问 MySQL：市场概览 7 条、热点主题 3 条、短线情绪榜 7 条，Dashboard route 不再含 SQL。情绪榜过去逐只查询候选日线，单次 66 条；现以 window query 一次读取所有候选最近 9 根 K 线，并批量读取分钟开板数据。Dashboard 自身 SQL 总数约从 76 降到固定 17。

上线前对旧进程和新磁盘代码做了情绪榜及完整 Dashboard 两次逐叶影子对比，差异均为 0。情绪榜约从 0.69 秒降到 0.34 秒。上线后 compact 冷请求约 0.54 秒、缓存约 0.003 秒，完整响应约 0.43 秒。

最终 readiness 验收同时修正了一处数据口径：`daily_kline` 最新可用日可能只有 ETF/零星盘中数据，不能直接拿全表 `MAX(trade_date)` 与股票因子层比较。现改为最近 45 日内达到股票池 95% 覆盖的完整日线日，另返回 `daily_kline_latest_available_trade_date` 和 `daily_kline_latest_is_partial`。

15:10 收盘回填完成后，2026-07-16 已有 5,522 条完整股票日线，而 Tushare 当日 `daily_basic` 现场仍返回 0 行，因此 readiness 正确变为 degraded。factor input 已新增交易日 18:30 补跑、03:20 兜底；每个交易日的 Tushare 数据只预取一次（约从 60 次调用降到 5 次），覆盖不足 80% 的日期不会写空数据，任务记为 `partial_success`。

全量 97 项回归通过。API 与三个 worker 均 active、`NRestarts=0`，migration 16/16、三类队列 0；当前 readiness degraded 但 `accepting_jobs=true`，唯一等待项为当日上游数据和 18:30 自动补跑。系统状态、readiness、health、公网 health 均 200；available 约 2.3GiB、Swap 约 0.5MiB。
