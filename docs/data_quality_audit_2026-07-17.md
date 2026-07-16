# 核心数据质量审计（2026-07-17）

## 1. 目标与边界

本阶段不继续盲目补字段，也不把大表聚合塞进 `/api/system/status`。第一批目标是建立一个每天可重复、结果可解释、不会把合法缺口误报为故障的质量闭环：

```text
cron
  -> scripts/run_data_quality_audit.py
  -> app/data_quality/repository.py（只读最新有界切片）
  -> app/data_quality/service.py（纯规则判定）
  -> task_run_log.metadata_json（离线快照）
  -> /api/system/status.data_quality
  -> 数据状态页“核心数据质量”
```

在线 API 只读取最近一次任务快照，不扫描 `daily_kline` / `factor_input_daily` 历史大表。快照随现有 `task_run_log` 保留策略保存，不新增 schema 和运行时 DDL。

## 2. 第一批 11 条规则

| 数据集 | 检查 | 主要语义 |
| --- | --- | --- |
| `stock_basic` | 身份字段 | 代码、名称、市场、instrument type 及市场前缀一致性 |
| `stock_basic` | 分类字段 | 上市日期、行业占位值、疑似退市但仍在有效池 |
| `daily_kline` | 值域/关联 | 重复、孤儿、空/非正 OHLC、高低价顺序、负成交量/金额、来源 |
| `daily_kline` | 缺口分层 | 停牌/暂停上市、当日新股、待处理源缺口分开统计 |
| `stock_status_snapshot` | 日期对齐 | 缺口解释快照不能长期落后日线 |
| `factor_input_daily` | 日期对齐 | 因子输入相对最新完整日线的新鲜度 |
| `factor_input_daily` | 覆盖缺口 | 合法非交易、新股与待处理缺口分开统计 |
| `factor_input_daily` | 市场字段 | 换手率、量比、总/流通市值；PE 不参与硬故障判定 |
| `factor_input_daily` | 来源/关联 | 重复、孤儿、完整度空值及 provenance 缺失 |
| `factor_input_daily` | 基本面可用性 | 六项核心基本面同时为空的覆盖比例 |
| 跨数据集 | 未来日期污染 | 日线、因子、状态快照不得出现未来交易日 |

判定分为 `pass / warn / fail`。小量可行动缺口为 `warn`；值域、孤儿、未来日期、来源字段丢失或超过 1% 的大面积可行动缺口为 `fail`。审计本身成功但发现告警时，任务日志记为 `partial_success`，不把“发现问题”错误等同为“任务执行失败”。

## 3. 退市主数据修正

原 `StockBasicSync` 只拉取 Tushare `list_status=L`，并把每条返回记录写成 `is_delisted=0`。已经从上市列表移除的旧行不会再被更新，也不会自动变成退市，因此 13 只名称已带“退市/退”的标的仍混在有效股票池中。

现改为：

1. 继续以 `L` 列表更新当前上市股票，并允许重新上市标的恢复为 `is_delisted=0`。
2. 额外读取 `D` 代码集合，但只对数据库里已经存在的股票执行 `is_delisted=1`。
3. 不把 Tushare 返回的 337 条历史退市记录重新导入项目库。
4. 将 Tushare 的 `NaN` 行业值归一化为 SQL `NULL`，不再写入字符串 `"nan"`。

## 4. 真实结果

首次只读审计（修正前）：

- 有效股票池 `5,542` 只，其中 13 只疑似已退市旧行。
- 最新日线 `5,524` 条，OHLC、成交量、金额、来源、重复和孤儿硬异常均为 `0`。
- 日线缺口 20 只：合法停牌/暂停上市 16、当日新股 1、待处理 3。
- 因子市场字段缺口 20 只：合法非交易 17、待处理 3。
- 质量规则 `7 pass / 4 warn / 0 fail`。

同步修正退市标记并刷新 2026-07-16 状态快照后：

- 有效股票池降为 `5,529`，13 只历史退市旧行已退出当前股票池，未新增历史 D 记录。
- 最新日线缺口降为 7 只：合法非交易 4、当日新股 1、待处理 2。
- 因子市场字段缺口降为 7 只：合法非交易 5、待处理 2。
- 状态快照与最新日线均为 `2026-07-16`。
- 质量规则改善为 `8 pass / 3 warn / 0 fail`。

当前剩余待处理样本：

- 日线：`bj.920685`、`sh.689009`。
- 因子市场字段：`bj.920081`、`bj.920685`。
- 主数据分类：仍有 2 只有效股票缺行业；暂无有效股票缺上市日期。

这些样本先保持为可解释告警，不直接用猜测值补齐。

## 5. 验收结果

- 全量 `125` 项 unittest 通过。
- Python 编译、系统页 JavaScript 语法、cron shell 语法和 `git diff --check` 通过。
- schema migration `16/16 ready`，没有新增 migration 或运行时 DDL。
- 两条审计 cron 已安装且无重复；API 串行重启后 active，`NRestarts=0`。
- 本地 `/api/health`、`/api/readiness`、`/api/system/status` 和公网 `/api/health` 均为 HTTP 200；readiness 为 `ready / accepting_jobs=true`。
- 回测、选股、Portfolio 三个 worker 未重启，继续 active；本切片不改变评分、选股和回测口径。

## 6. 调度与后续

- 每天 `04:05`：在夜间主数据、日线、状态、因子和舆情日更后审计。
- 交易日 `18:45`：在 Tushare `daily_basic` 晚间补跑后复核当天因子层。
- MySQL advisory lock 防止手动运行与 cron 重叠。

下一批数据质量工作建议按优先级推进：

1. 为上游缺失增加“最后成功来源/最后尝试时间/连续缺失天数”，把一次性 source gap 与持续故障分开。
2. 为历史股票池补 point-in-time 上市、ST、暂停/退市状态，解决回测幸存者偏差；当前所有回测仍保持 `research_only / unvalidated`。
3. 校准 `factor_input_daily.completeness_score`：PE 不适用不能继续与真正的源缺失同罚，但调整前必须冻结策略/回测响应，避免静默改变选股口径。
