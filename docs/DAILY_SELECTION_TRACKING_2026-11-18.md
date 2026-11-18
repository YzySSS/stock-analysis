# 每日自动选股 → 跟踪复盘（2026-11-18）

## 用户要求与边界

每日交易日09:35自动选股（A股舆情0.4.4、最多3只）的成功结果自动保存到跟踪复盘。不改变09:25前瞻研究配对、不修改选择器/策略/冻结依赖、不自动持仓或交易，人工普通选股仍按条保存。

## 实现

- 普通 `SelectionRunService` 明确拒绝 `save=true`，Worker始终生成预览；因此保持队列合同不变，在非冻结 `scripts/run_daily_stock_selection.py` 完成获授权的保存步骤。
- 提交后至多等待300秒，成功才复用原人工 `save_selection_item`：真实 `selection_result` 保存、分钟线补全入队、跟踪展示缓存失效均走原路径；固定原 `run_id`、候选与排名。既有 `(run_id, code)` 唯一键及日期/策略/代码去重防重复对象。
- 真实published结果顶层缺 `trade_date/selected_price_type`，顶层 `selected_price` 可能是昨日close；原盘中实时报价、日期、时间仍在 `explain.raw_metrics`。保存输入复制并明确固定这份**原已记录盘中报价**，与当时人工盘中保存一致，既不获取当前行情，也不修改原任务结果。原盘中报价证据缺失则明确报错，不猜值。
- 当日独立调度键查重不变，09:42/09:50各一次 `--save-only --wait-seconds 0` 仅补保存已有任务，绝不重算选择器或重建快照。没有任务跳过，仍运行则pending，失败/取消/空选不建跟踪。
- 保存任一项失败、补全入队degraded、Worker未完成均不能报完整成功；输出保存条数、代码、逐项错误并非零退出，系统任务日志记failed。`selection_run`保留原Worker计算状态，保存阶段另见 `daily_stock_selection`任务日志；手工可对已有当日任务重试save-only。

## 验证（实现阶段）

- 单进程定向unittest **139项全部通过**：daily selection、人工保存/异步补全、Worker队列、system registry、SelectionRepository去重、forward、tracking及sentiment snapshot/materializer。
- `bash -n scripts/setup_kline_cron.sh`、`git diff --check`。
- 12:16只读dry-run：今日自动任务 `selection_task_20261118_093512_899682` 状态success，2只：有研新材 `sh.600206`、伟测科技 `sh.688372`，该run尚无已保存行。可直接save-only补存，无需重新选股。
- 本文实现阶段未安装Cron、未补存生产、未重启服务、未提交推送；部署后应补充真实保存与页面API可读验证。已有两份无关docs改动保持原样。

## 部署与现场验收

12:21～12:23已完成部署与真实验收：

- 根独立复核 **110项全部通过**：`tests.test_daily_stock_selection`、`tests.test_selection_async_enrichment`、`tests.test_selection_repository`、`tests.test_selection_worker_queue`、`tests.test_forward_observation`、`tests.test_sentiment_snapshot_materializer`。故障分支测试中预期的模拟`db down`日志不代表现场失败。Shell语法与diff检查通过。
- 完整备份旧Cron，仅新增`42,50 9 * * 1-5`的save-only行，安装后逐字核对除该行外全部不变；保留09:35原任务。
- 仅补存今日原任务`selection_task_20261118_093512_899682`，没有重建快照、提交选股或修改原任务结果。2条保存成功、错误0：

| 股票 | 原盘中基准价 | 原报价时间 | 跟踪记录ID |
| --- | --- | --- | --- |
| 有研新材 sh.600206 | 39.74 | 2026-11-18 09:33:54 | 321 |
| 伟测科技 sh.688372 | 92.99 | 2026-11-18 09:34:12 | 322 |

- 再次save-only成功，核对记录ID、条数、创建时间、统计开关与原报价均不变，原任务结果不变。补存实际发生于12:22，未回写或伪造早盘保存时间。
- 本地与公网各4项GET共 **8/8通过**：health、选股页提示、指定原任务的跟踪复盘列表、原选股任务结果。跟踪API恰有2只、日期11/18、报价与原始记录一致；HTTP验证不等于请求者设备视觉验收。
- 只改调度脚本、Cron模板、HTML提示、相关测试与文档；冻结的`app/shared`、`app/strategies`、`app/stock_selection`源码/依赖无改动。无DDL、服务重启、持仓或交易写入。09:25研究及历史回测边界不变。
- 正常调度下首次自动保存预计为下一交易日11/19 09:35选股成功后；本次已用11/18既有真实结果验证保存与重跑去重，未冒称11/19自然运行已验收。

证据目录：`/root/.openclaw/workspace/output/daily-selection-tracking-20261118-122140/`，含`preflight.json`、完整Cron备份与安装结果、`save-first.json`、`save-retry.json`、`database-verification.json`及`http-verification.json`。目录中没有会话凭据。
