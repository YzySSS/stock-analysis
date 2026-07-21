# 当前版本与整改计划

> 当前唯一执行计划。最后更新：2026-07-21。
>
> 这里的版本名是项目阶段标签，不是对外 API 的语义化版本号。历史审计、逐批实施日志和早期产品设想继续保留，但不再作为“下一步做什么”的依据。

## 1. 当前版本基线

当前阶段标签：

```text
Research Baseline 2026.07
可信研究基线，未验证交易系统
```

当前源代码基线已推进至提交 `c268311`；其后的版本计划文档提交不改变线上业务语义。

| 维度 | 当前事实 |
| --- | --- |
| 产品定位 | 个人纪律型投研工作台；辅助研究，不自动交易，不构成投资建议 |
| 部署形态 | 单机模块化单体；FastAPI + MySQL + Nginx + 3 个独立 worker |
| 数据库 | migration `0021`，生产与独立 smoke 库均为 `21/21` |
| 自动回归 | 279 项通过 |
| 运行状态 | readiness `ready`，3 个 worker healthy/idle，3 类队列均为 0 |
| 公网边界 | `/api/health` 公开；其余页面/API 由 Basic Auth 保护；重型入口限流 |
| TLS | Let’s Encrypt，当前有效期至 2026-10-16；Certbot 每日两轮自动续期，续期成功后校验并 reload Nginx |
| 数据真相 | DQ1～DQ5 已完成：行情、历史股票状态、公告日基本面、历史指数成分均按 point-in-time 使用 |
| 回测口径 | `close_signal_next_open_pit_index_universe_v5`；所有结果仍为 research-only |
| 策略状态 | 11 条可加载、7 条数据就绪、2 条普通选股可执行、2 条仅研究回测、0 条通过交易有效性验证；A 股舆情为 `0.4.3` |
| 默认策略 | 无；普通选股必须由用户显式选择 `strategy_id` |
| 页面性能 | Tracking、Dashboard、Portfolio、Selection、Backtest 均有统一冷/热预算证据；唯一超预算项 Dashboard 已从约 3.39s 降至 0.40s |

策略证据必须按下面的口径理解：

- `lowvol_reversal`、`v13_three_factor`：2025-07-01～2026-06-30 冻结历史诊断均未通过，退出普通选股，只保留研究回测。
- `v12_legacy`、`a_share_sentiment`：工程上可执行，但都未通过交易有效性验证；系统不会自动选中它们。
- `a_share_sentiment 0.4.3`：已修复股票—板块关系、局部新闻方向、主题资金映射、候选截断、深 V 分级、公告日基本面、股票简称中文子串伪命中、相邻子句串线、历史缓存绕过和旧催化冒充今日买点；旧版结果保留但退出有效统计，0.4.3 从新协议重新积累前瞻样本，仍保持 `unvalidated`。
- 其余策略：原型或仅展示，不进入普通运行入口。
- 两份 2026-07-20～2027-01-31 的真正前瞻协议保持冻结；实现指纹仍为 `MATCH`，窗口闭合前不执行、不调参。

## 2. 已收口的整改主线

以下项目不再作为开放整改项反复出现；后续只有发现真实回归才重新开启：

| 主线 | 状态 | 已形成的护栏 |
| --- | --- | --- |
| 能力真实性 | 已完成 | ETF/指数选股与回测明确拒绝；无真实结果不伪造；策略 capability 动态计算 |
| 回测可信工程 | 已完成 | 次日开盘成交、公告日基本面、历史生命周期/ST/停复牌、指数历史成分、方法论/配置/源码指纹 |
| 长任务可靠性 | 已完成 | selection/backtest/portfolio advice 独立 worker；幂等、取消、心跳、stale recovery、重试 |
| 公网保护 | 已完成 | Basic Auth、重型入口限流、无密钥部署模板 |
| HTTPS 续签 | 已完成 | TrustAsia 手工证书已切换为 Let’s Encrypt webroot；Certbot timer、deploy hook、dry-run 与到期复核提醒均已验证 |
| 数据生命周期 | 已完成 | raw 1m、rollup、tracked 1m、舆情快照、任务/错误日志均有保留与清理口径 |
| Schema 与代码边界 | 已完成 | 唯一 migration 入口；五个核心 Repository；生产 `app` 不反向依赖 `scripts/src` |
| 核心数据质量 | 已完成首轮 | DQ1～DQ5 离线审计、快照展示、已知缺口 fail-closed |
| 策略历史诊断 | 已完成 | 两条冻结诊断均失败，失败归因已完成，不创建执行修补型 V14 |
| 下一策略研究章程 | 已完成协议层 | `next_strategy_research_v1` 已冻结并加 SHA-256 锁；未写新策略、未创建验证协议、未运行回测 |
| A 股舆情前瞻观察 | 已完成语义整改 | 0.4.3 重新冻结关系、局部方向、实体识别、历史缓存、催化时效、资金、深 V 与 PIT 基本面口径；旧结果可追溯但不混入新统计 |
| 建议与执行复盘 | 已完成第一版 | 策略页分开呈现建议质量与用户纪律；支持看过/保存/买入/跳过/卖出动作流水，不冒充券商成交 |
| 下一策略 factor spec | 已冻结 | `pit_quality_trend_liquidity_factor_spec_v1` 已锁公式、阈值、候选日志和哈希；历史调整因子覆盖为 0，实施锁 fail-closed |
| 跟踪复盘性能 | 已完成第一阶段 | 冷请求从约 3.60s 降至 0.244s，热请求约 0.035s；真实交易日边界同时修正 |
| Dashboard 性能 | 已完成 | compact 冷请求从约 3.39s 降至 0.40～0.46s，热请求约 0.001s；输出哈希保持一致 |
| Portfolio 性能 | 已完成基线 | 完整持仓 API 首次约 0.047s、热约 0.033s；首屏只有一个数据请求，无需改代码或加缓存 |
| Selection 性能 | 已完成基线 | 最近/run/strategy 三条结果读取约 0.051～0.084s；详细字段均由页面实际使用，不裁剪产品信息 |
| Backtest 性能 | 已完成基线 | 包含最近结果与成交明细的 6 请求首屏串行链首次约 0.261s、热约 0.083s；保持串行以避免小主机并发查询尖峰 |

## 3. 版本路线

### R2026.07：可信研究基线（已完成）

完成标准已经满足：工程能力不冒充策略有效性，核心数据按 point-in-time 使用，长任务可恢复，关键状态可观测，历史策略结论有冻结证据。

### R2026.07.1：运维与响应性能维护版（已完成）

完成项：

1. 已完成：原 2026-08-03 到期的 TrustAsia 手工证书已切换为 Let’s Encrypt；新证书到期日为 2026-10-16，Certbot timer 每日两轮自动续期，webroot 和 reload hook 的完整 dry-run 已通过。
2. Tracking、Dashboard、Portfolio、Selection、Backtest 均已建立同一口径的冷/热响应基线和查询预算；Dashboard 真实冷慢点已经整改，其余页面均无需改运行代码。
3. 只优化有真实慢点的接口；不靠长期陈旧缓存、隐藏字段或一次性大重构制造“看起来很快”。

页面/API 的第一版预算：

| 类型 | 本机目标 |
| --- | --- |
| 页面 HTML / 静态资源 | 常规请求 `< 100ms` |
| 列表与 compact 汇总热请求 | `< 200ms` |
| 列表与 compact 汇总冷请求 | `< 800ms` |
| 重型计算 | 必须异步入队，不阻塞页面首屏 |

超出预算时按“查询次数 → 索引/边界 → 首屏与汇总拆分 → 有界缓存”的顺序处理，并保留优化前后基线。

### R2026.Q3-Research：下一代策略研究候选（协议优先）

研究章程 v1 已于 2026-07-18 冻结，协议 ID 为 `next_strategy_research_v1`。`factor_spec_lock` 已于 2026-07-21 完成；候选实现、数据库 validation protocol 和新回测仍未创建，不在旧 lowvol/V13 上做为了翻正结果的补丁。

协议最低要求：

- 新策略使用独立 ID、配置和实现文件，不修改旧冻结协议指纹覆盖的文件。
- 使用独立信号家族，允许无合格标的时持币。
- 删除与 alpha 无关的绝对股价加分。
- 在看结果前冻结训练、历史诊断和真正前瞻区间。
- 冻结股票池、基准、持有期、换仓频率、交易成本、最少样本数和所有非重叠 offset 的汇总门槛。
- 历史诊断只能排雷，不能把策略升级为“已验证”。
- 即使真正前瞻协议通过，也只进入人工评审候选，不自动成为默认策略或普通选股策略。

完整的区间、执行、成本、五个 offset、数据和晋级合同见 [`next_strategy_research_protocol_2026-07-18.md`](./next_strategy_research_protocol_2026-07-18.md)。历史调整因子与总回报核算前置条件已于 2026-07-21 通过并形成独立证据锁；下一步是单独完成 implementation lock，不能跳过实现冻结直接跑历史诊断。

### Validation Candidate：交易有效性候选（无固定日期）

只有满足冻结前瞻样本量、净收益/超额/回撤/稳定性门槛，且人工复核数据与方法论后，才允许单独讨论候选升级。现有 lowvol/V13 协议最早也要在 2027-01-31 窗口闭合后评估，不能提前下结论。

### Product Loop：纪律与建议复盘（第一版已完成）

当前已经形成第一版产品闭环：

1. 固定 A 股舆情候选后的 1/3/5/20 日表现，并同时展示三宽基超额与 MFE/MAE。
2. 分开评价“建议质量”和“纪律执行”。
3. 记录用户看过、保存、买入、跳过和卖出；当前是人工动作流水，不声称是券商成交回报。
4. 页面展示协议、样本、建议、用户动作与后续表现的完整链路。

## 4. 当前剩余整改队列

按实际执行顺序：

1. **FWD-OBS 前瞻样本积累**：从 `a_share_sentiment 0.4.3` 部署日起按新协议重新采集；窗口内只采集和展示，不调参数、不提前宣布验证通过。
2. **DQ-ONGOING 已知缺口观察**：可行动缺口已归零；保留 `sh.689009` 历史名称未知、`bj.920305` 上游分类矛盾和指数月度快照近似等事实警告，不猜值。
3. **STR-4 下一策略 implementation lock**：按已冻结 factor spec 实现独立策略、PIT 输入、原始开盘成交和复权总回报，冻结源码/配置/方法论指纹；锁定前不跑历史诊断。

`PERF-2` 已从开放队列移出：Tracking、Dashboard、Portfolio、Selection、Backtest 全部验收完成。若以后某页真实冷/热请求越过预算，再按独立切片重开，不做无基线的泛化优化。

`STR-2 factor spec lock` 已完成并加 SHA-256 锁。任何公式、权重、阈值或候选日志变化必须创建新 spec ID，不能原地修改 v1。

`STR-3 调整收益前置条件` 已完成：冻结开发区间股票 K 线 1,819,694/1,819,694 全匹配；全历史 3,137,224/3,137,232，唯一 8 条缺口均为有真实停牌证据且上游无同日因子的记录，候选路径 fail-closed。证据见 [`adjustment_factor_history_readiness_2026-07-21.md`](./adjustment_factor_history_readiness_2026-07-21.md)。

以下项目保持可选，不进入当前承诺：

- ETF 全市场选股/回测正式支持。
- 大规模 UI 重做或切换前端框架。
- Redis/Celery、微服务拆分。
- 自动下单。
- 微信/其他通知通道接入。

## 5. 每一步的完成门槛

每个后续切片都必须满足：

1. 只改与本步目标直接相关的代码和文档。
2. 修改接口就打真实接口；修改页面就验证静态资源与页面状态；修改任务就验证队列、取消、恢复和 worker 健康。
3. 跑全量 `unittest`、Python 编译、相关 JavaScript 语法和 `git diff --check`。
4. 上线前确认三类队列为空；服务串行重启，避免 2 vCPU / 3.6 GiB 主机产生并发资源尖峰。
5. 验收 readiness、migration、worker、队列、本地与公网健康状态。
6. 更新本计划；一个步骤一个提交，并在完成后向大X报告结果。

## 6. 历史文档定位

- [`architecture_remediation_plan_2026-07-15.md`](./architecture_remediation_plan_2026-07-15.md)：已完成整改的逐批执行台账，保留审计证据，不再承载当前待办。
- [`target_code_architecture_2026-07-15.md`](./target_code_architecture_2026-07-15.md)：已落地的目标边界与迁移合同，保留作架构约束参考。
- [`IMPROVEMENT_PLAN.md`](./IMPROVEMENT_PLAN.md)：长期产品方向、数据质量、验证和性能背景；当前执行顺序以本文为准。
- [`strategy_oos_validation_2026-07-17.md`](./strategy_oos_validation_2026-07-17.md)：冻结历史诊断与真正前瞻协议。
- [`strategy_failure_attribution_2026-07-18.md`](./strategy_failure_attribution_2026-07-18.md)：失败原因与下一策略研究边界。
- [`next_strategy_research_protocol_2026-07-18.md`](./next_strategy_research_protocol_2026-07-18.md)：下一策略不可变研究章程、时间分区、执行成本与人工晋级合同。
- [`a_share_sentiment_forward_observation_v0_4_3_2026-07-21.md`](./a_share_sentiment_forward_observation_v0_4_3_2026-07-21.md)：A 股舆情 0.4.3 的完整语义整改、生产复核与当前固定前瞻观察合同。
- [`a_share_sentiment_forward_observation_v0_4_2_2026-07-21.md`](./a_share_sentiment_forward_observation_v0_4_2_2026-07-21.md)：0.4.2 历史冻结合同；首个正式观察前已被 0.4.3 替换，仅作追溯。
- [`a_share_sentiment_forward_observation_v0_4_1_2026-07-21.md`](./a_share_sentiment_forward_observation_v0_4_1_2026-07-21.md)：0.4.1 历史冻结合同；首个正式观察前已被 0.4.2 替换，仅作追溯。
- [`a_share_sentiment_forward_observation_v0_4_0_2026-07-21.md`](./a_share_sentiment_forward_observation_v0_4_0_2026-07-21.md)：0.4.0 历史冻结合同；首个正式观察前已被 0.4.1 替换，仅作追溯。
- [`a_share_sentiment_forward_observation_2026-07-21.md`](./a_share_sentiment_forward_observation_2026-07-21.md)：A 股舆情 0.3.1 的历史合同，仅作追溯，不代表当前口径。
- [`adjustment_factor_history_readiness_2026-07-21.md`](./adjustment_factor_history_readiness_2026-07-21.md)：历史复权因子补齐、股票口径覆盖率、停牌缺口与不可变数据证据锁。
- [`page_response_performance_2026-07-19.md`](./page_response_performance_2026-07-19.md)：四个主要页面的统一预算、实测基线和逐页整改证据。

如果历史文档中的“下一步”与本文冲突，以本文为准。
