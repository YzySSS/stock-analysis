# 当前服务器部署模板

这些文件复现当前单机部署边界：FastAPI 只监听 loopback，Nginx 提供 HTTPS、全站 Basic Auth 和重型接口限流，`/api/health` 保持免认证。

模板不包含数据库密码、API token、TLS 私钥或 Basic Auth 明文密码。部署前确认项目 `.env`、证书和 MySQL 已独立准备。

## 依赖

- Ubuntu/Debian：Nginx、OpenSSL、Python 3 venv。
- Python：在项目 `.venv` 内安装仓库依赖。
- 数据：MySQL 可用，应用 `.env` 只保存在服务器。
- TLS：证书安装到 `/etc/nginx/ssl/yzysstock.cloud/`，私钥权限限制为 root。

## systemd

```bash
install -o root -g root -m 644 deploy/systemd/stock-analysis-api.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-backtest-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-selection-worker.service /etc/systemd/system/
install -o root -g root -m 644 deploy/systemd/stock-analysis-portfolio-worker.service /etc/systemd/system/
.venv/bin/python -m app.orchestration.migrate
.venv/bin/python -m app.orchestration.migrate --apply
.venv/bin/python -m app.orchestration.migrate --check
systemctl daemon-reload
systemctl enable --now stock-analysis-api.service stock-analysis-backtest-worker.service stock-analysis-selection-worker.service stock-analysis-portfolio-worker.service
```

`app.orchestration.migrate` 是唯一 schema 入口，默认只输出 plan，只有 `--apply` 修改数据库。16 个有序 migration 使用持久 MySQL advisory lock、状态/checksum 和失败记录；再次执行不会重复应用。四个 systemd unit 都在 `ExecStartPre` 运行 `--check`，有 pending/checksum mismatch 时拒绝启动。cron 安装脚本在真正写入 crontab 前也执行同一检查。

选股 schema migration 必须先于 API/worker 切换执行。API 只写入 `queued` 任务，selection worker 负责原子 claim、心跳、取消与 stale recovery；重启 API 不会丢失排队任务。

持仓建议 schema 迁移也必须先于服务切换执行。API 只固化输入快照并写入 `queued`，portfolio worker 负责 DeepSeek 调用、心跳、取消与 stale recovery；同一持仓同时只允许一个活跃建议任务。

统一 migration 的 `0016` 增加三类 worker 的进程级租约、统一错误/日汇总表，并补齐 backtest 的 attempt、稳定错误码和 active 幂等列。三个 worker 启动后会持续更新空闲/运行心跳；`GET /api/readiness` 在 migration、数据库或任一必要 worker 不可用时返回 503，`GET /api/health` 仍保持不访问数据库的轻量存活检查。

空库重建 smoke 只允许显式配置、名称精确为 `stock_migration_smoke` 或以 `stock_migration_smoke_` 开头且首次零表的独立数据库；它会跑两遍 migration 并校验第二遍零变更，不会创建或删除数据库：

```bash
DB_NAME=stock_migration_smoke \
  .venv/bin/python -m app.orchestration.migration_smoke \
  --database stock_migration_smoke
```

当前生产数据库位于远端，应用账号不能自行创建数据库，独立 smoke 库仍需由数据库侧 provision。2026-07-16 已在 `stock_migration_smoke` 完成真实空库验收：首次应用 16 个 migration、生成 61 张表，工具内部第二遍 `applied_now=0`。该工具要求首次零表；再次完整演练应重新 provision 空库或使用新的安全后缀库名，不会自动清表或删库。

模板按当前服务器路径 `/root/.openclaw/workspace/stock-analysis` 编写；迁移目录时先统一替换路径并运行 `systemd-analyze verify`。

## Basic Auth 凭据

以下流程把随机密码保存在 root-only 文件中，并只把哈希交给 Nginx。不要把这两个运行时文件提交到仓库。

```bash
install -d -o root -g root -m 700 /root/.config/stock-analysis
umask 077
openssl rand -out /root/.config/stock-analysis/basic-auth-password -hex 18
password_hash=$(openssl passwd -apr1 -in /root/.config/stock-analysis/basic-auth-password)
install -o root -g www-data -m 640 <(printf 'dax:%s\n' "$password_hash") /etc/nginx/.htpasswd-stock-analysis
unset password_hash
```

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
- 未认证页面和其他 API 返回 401。
- 正确凭据访问页面和普通 API 返回 200。
- 选股、回测、DeepSeek 深度复盘和持仓建议接口按 IP 限制为每分钟 6 次、突发 2 次，超限返回 429。

## cron

`scripts/setup_kline_cron.sh` 是股票任务的唯一安装入口。它会保留不属于本项目的既有 cron，去重后写入当前项目任务：

```bash
bash scripts/setup_kline_cron.sh --print-only
bash scripts/setup_kline_cron.sh
```

先审查 `--print-only` 输出，再执行安装。
安装模式会先执行 migration `--check`；schema 未就绪时不会改写 crontab。

任务 retention 默认每天 04:15 串行执行：`task_run_log` 明细保留 90 天并长期保留日汇总，结构化错误日汇总保留 365 天；正式回测、验证基线、持仓建议摘要/结果和已保存选股结果不被清理。手动执行时默认 dry-run，只有显式 `--apply` 才修改数据：

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

每次覆盖现网配置前先备份 `/etc/nginx/sites-available/stock-analysis`、`/etc/nginx/conf.d/stock-analysis-rate-limit.conf` 和 htpasswd。回滚后必须先执行 `/usr/sbin/nginx -t`，通过后再 `systemctl reload nginx.service`。
