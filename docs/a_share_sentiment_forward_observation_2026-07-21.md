# A 股舆情策略前瞻观察协议（v0.3.1）

## 1. 定位

这条链路从 `2026-07-21` 开始，对 `a_share_sentiment 0.3.1` 做固定口径的日级前瞻观察。它回答的是“冻结版本在今后真实出现的样本上表现如何”，不是回填历史后再挑参数，也不是交易有效性认证。

协议 ID：

```text
a_share_sentiment_v0_3_1_after_close_v1
```

无论样本多少，API 和页面的 `validation_status` 始终保持 `unvalidated`。达到 20 个成功观察日或累计 50 个候选，只会从 `collecting` 进入 `preliminary_ready`，含义是可以开始阅读初步证据，不代表策略通过验证。

## 2. 冻结合同

| 项目 | 固定值 |
| --- | --- |
| 策略 | `a_share_sentiment` |
| 策略版本 | `0.3.1` |
| 每日执行 | 交易日收盘后 `16:20 Asia/Shanghai` |
| 分数底线 | 60 |
| 最大候选 | 3；允许 0 只 |
| 入场口径 | 信号日后的首个可交易日开盘价 |
| 观察周期 | 1 / 3 / 5 / 20 个交易日收盘 |
| 基准 | 沪深300、中证500、中证1000 |
| AI 口径 | 记录当次真实 progressive AI / partial AI / local fallback，不混写 |
| 参数政策 | 初步样本门槛前不改参数；若以后改动必须新建协议 |
| 不可变标签 | `a-share-sentiment-v0.3.1` |

正式任务要求部署工作树干净、不可变标签是当前部署提交的祖先，并逐个核对策略、配置、Selector、交易计划、AI rerank 和行情/舆情输入等冻结源码。冻结路径发生漂移时 fail-closed，不会继续把新代码写进旧协议。

## 3. 每日样本如何形成

`scripts/run_strategy_forward_observation.py` 每天只在最新完整日线等于当天时提交一次任务。每个协议和信号日只有一条 observation，selection run 与 observation 一一关联；重复执行会恢复或去重，不会重复制造候选。

任务仍走独立 selection worker：

```text
16:20 cron
  -> 冻结源与配置校验
  -> 预留 observation（包括未来可能的 0 只结果）
  -> selection_run queued
  -> selection worker 运行
  -> 成功时固化候选和当次 AI 模式
  -> 失败时保留稳定 error_code / message
```

每天 `05:10` 的 `scripts/run_strategy_forward_outcomes.py` 负责恢复未收口 observation，并刷新已经成熟的后续收益。零候选日也保留为成功观察日，禁止只统计“有票可选”的日子。

## 4. 收益与风险口径

每只候选从下一可交易日开盘入场，分别计算第 1、3、5、20 个交易日收盘收益；同时记录：

- 相对沪深300、中证500、中证1000的同期收益和超额；
- 5 日和 20 日最大有利变动（MFE）与最大不利变动（MAE）；
- 入场日、退出日、路径长度和最后可用交易日；
- `adjusted_total_return` 或显式的 `raw_fallback`。

有完整 `adj_factor` 时按调整后总回报计算。缺调整因子时不会伪装成复权结果，而是把该样本标为 `raw_fallback`，页面单独展示两种数量。

## 5. 建议质量与纪律执行分开评价

策略页新增“前瞻观察与执行复盘”：

- **建议质量**：全部候选的 1/3/5/20 日样本数、平均收益、胜率、基准和超额、MFE/MAE。
- **纪律执行**：用户对候选记录的 `看过 / 保存 / 买入 / 跳过 / 卖出` 动作、决策覆盖率，以及实际标记为买入的候选后续表现。

动作是追加流水，不会改写原始候选、信号日期或策略结果。这样可以区分“建议本身不好”和“建议不错但执行偏离”，避免把两个问题混成一个胜率。

接口：

```text
GET  /api/strategies/forward-evidence?strategy_id=a_share_sentiment
POST /api/strategies/forward-actions
```

## 6. 当前边界

- 这不是自动下单，也不把用户动作当成券商成交回报。
- AI 调用不可用时允许本地 fallback，但必须单独分段报告。
- 20 天或 50 个候选只是最低可读样本，不足以支持交易有效性结论。
- 参数、冻结路径或方法论变化后必须创建新策略版本、新标签和新协议，旧证据永久保留。

