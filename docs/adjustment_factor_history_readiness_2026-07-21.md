# 历史复权因子补齐与数据就绪证据（2026-07-21）

## 1. 本轮结论

历史复权因子前置条件已经完成，冻结开发区间达到股票口径 100% 覆盖，全历史达到 99.999745%，状态为 `ready`。

这只表示下一策略可以进入单独的 implementation lock 评审，不表示策略已经实现、回测、验证或可选。冻结 factor spec 仍保留当时“覆盖为 0、implementation blocked”的历史事实；后续就绪证据由新的不可变数据锁承载，不能回写旧 spec 改写历史。

不可变证据：

- [`pit_quality_trend_liquidity_adjustment_data_lock_v1.yaml`](../config/research_protocols/pit_quality_trend_liquidity_adjustment_data_lock_v1.yaml)
- [`pit_quality_trend_liquidity_adjustment_data_lock_v1.sha256`](../config/research_protocols/pit_quality_trend_liquidity_adjustment_data_lock_v1.sha256)
- 补齐与审计实现提交：`cdf2a380efd1668997b0a27f1e7aeeab0c6b9ff7`

## 2. 数据范围与验收结果

覆盖口径固定为 `daily_kline` 连接 `stock_basic.instrument_type='stock'`，版本为 `stock_instrument_type_v1`。ETF 不属于本策略全 A 股票池，也不进入 Tushare 股票复权接口的覆盖分母。

全历史（2024-01-02～2026-07-20）：

- 615 个交易日，615 个 manifest 分区全部 `success`；
- 股票 K 线 3,137,232 行，匹配因子 3,137,224 行；
- 总覆盖率 99.999745%，最差单日覆盖率 99.845649%，均高于 99.5% 门槛；
- `adj_factor_daily` 区间内 3,340,718 行；
- 非正因子 0 行，未来日期因子 0 行；
- manifest 缺失、非成功、口径版本不一致均为 0。

冻结开发区间（2024-01-02～2025-06-30）：

- 359 个交易日；
- 股票 K 线 1,819,694 行，匹配 1,819,694 行；
- 总覆盖率与最差单日覆盖率均为 100%；
- 缺失因子 0 行。

## 3. 已知 8 条源端缺口

全历史仅有 8 条股票行情没有同日 Tushare 因子，全部发生在 2026-02-03。现场核实：

- 8 只股票均在 `stock_suspension_daily` 标记为 `suspend_type='S'`；
- 对应 K 线成交量、成交额均为空，开高低收完全相同；
- Tushare `adj_factor` 当天不返回这些代码，但前后交易日均有因子。

因此不猜值、不沿用前一日因子。任何需要该日期的候选路径直接 fail-closed；禁止退回未复权收益。这 8 行使最差单日覆盖率为 99.845649%，仍高于预先固定的 99.5% 分区门槛，并且不会进入可交易候选路径。

## 4. 总回报口径

复权总回报统一使用：

```text
end_price * end_factor / (start_price * start_factor) - 1
```

模拟成交仍使用真实原始开盘价，复权因子只用于持有期总回报和趋势计算。候选所需任一日期缺少正且有限的因子时整条候选路径失败，不允许 `raw_unadjusted_return` 兜底。

抽查 10 个最近公司行动日期，原始价格跳空与调整后总回报已被明确区分。例如 `sh.603400` 原始收盘涨跌为 -30.118443%，因子变化 30.641190%，调整后总回报为 -8.705903%；这证明不能把除权跳空直接当作投资损失，也证明当前公式实际生效。

## 5. 工程闭环

- 迁移 `0022` 新增 `adj_factor_sync_manifest`，记录每个交易日的期望、源端、落库、匹配、缺失、覆盖率、尝试次数、错误和口径版本；
- `run_adj_factor_history_backfill.py` 支持日期范围、断点续跑、最大天数、暂停、失败上限、审计与公司行动样本；
- 已满足门槛且口径版本一致的日期会跳过上游；首个故障后的 2024-01-02 实跑已证明可从已有数据恢复，重复运行 `source_rows=0`；
- 历史补齐和每天 02:10 的近 5 日刷新共享 MySQL advisory lock `stock_analysis_adj_factor_sync`，不会互相重叠；
- 全量源端任务 `adj_factor_history_20260721_013455` 处理 564 天、写入 3,059,094 行、API 失败 0 天；
- 最终清单重对账 `adj_factor_history_20260721_015637` 跳过 615 天、上游调用 0，审计 `ready`；
- 系统状态页已跟踪“历史复权因子补齐”任务。

## 6. 下一步边界

下一步可以开始独立的 implementation lock：实现 `pit_quality_trend_liquidity_v1` 的 PIT 输入、完整因子路径、原始开盘成交和复权总回报，并冻结源码/配置/方法论指纹。

在 implementation lock 完成以前，仍然禁止创建历史诊断结果或数据库 validation protocol；完成以后也只能按冻结协议运行一次历史诊断，历史结果只能否决或判定证据不足，不能把策略升级为“已验证”。
