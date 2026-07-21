# 舆情数据一致性快照与 Tushare 实时分钟权限探针

## Schema migration

`0023` 只追加以下表，不修改 `0001`～`0022` 的 runner、顺序或 checksum：

- `source_batch_manifest`
- `sentiment_candidate_snapshot_manifest`
- `sentiment_candidate_snapshot`
- `stock_realtime_rank_snapshot`
- `tracking_summary_daily`
- `operational_status_snapshot`
- `ai_advice_snapshot`

部署顺序保持不变：

```bash
.venv/bin/python -m app.orchestration.migrate
.venv/bin/python -m app.orchestration.migrate --apply
.venv/bin/python -m app.orchestration.migrate --check
```

这些表当前只提供一致性、审计和预计算读模型的持久化边界。迁移不会启动采集、回填或外部请求，也不会改变现有策略/API 行为。

## Tushare `rt_min` / `rt_min_daily` 权限探针

探针只做内存中的最小查询，不写数据库或文件。它不接受命令行 token 值；必须显式传入保存 token 的环境变量名：

```bash
read -rsp 'Tushare token: ' TUSHARE_RT_PROBE_TOKEN && printf '\n'
export TUSHARE_RT_PROBE_TOKEN
.venv/bin/python scripts/probe_tushare_realtime_permissions.py \
  --token-env TUSHARE_RT_PROBE_TOKEN \
  --ts-code 600000.SH \
  --freq 1MIN
unset TUSHARE_RT_PROBE_TOKEN
```

未传 `--token-env`、环境变量不存在或为空时，命令返回 `skipped`、退出码 `0`，并且不会导入 Tushare 或发起网络请求。至少一个接口不可用时退出码为 `1`。输出只包含接口名、可用状态、行数和字段名；不输出 token、请求对象、原始异常或原始数据。

生产部署建议使用临时进程环境或 root-only EnvironmentFile，执行后立即清理 shell 变量；不要把 token 写入 unit、cron、仓库或命令行参数。
