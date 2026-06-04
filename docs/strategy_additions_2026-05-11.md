# 选股/交易策略新增记录（2026-05-11）

## 背景

根据对 A 股因子研究、低波/反转/因子动量/舆情方向的调研，以及当前项目已经补齐的 `moneyflow`、`cyq_perf/chip`、`stock_sentiment_daily` 数据层，本轮新增 4 个选股策略和 1 个交易策略。

命名原则：页面直接展示人能理解的策略名，不再使用 V14/V15 作为产品名称。

## 新增选股策略

### 1. 资金筹码修复选股

- `strategy_id`: `fund_chip_repair`
- 核心语义：寻找“价格尚未明显拉升，但资金回流、筹码压力改善”的股票。
- 因子：
  - 资金回流 `fund_flow`
  - 筹码修复 `chip_repair`
  - 温和回撤 `pullback`
  - 流动性 `liquidity`
  - 低波约束 `lowvol`
- 数据依赖：`stock_moneyflow_daily`、`stock_chip_daily`、`lowvol_reversal_feature_daily`、`daily_kline`。

### 2. 质量低波选股

- `strategy_id`: `quality_lowvol`
- 核心语义：偏稳健，寻找“盈利质量较好、估值不过分、波动较低”的股票。
- 因子：
  - 盈利质量 `quality`
  - 合理估值 `value`
  - 低波动 `lowvol`
  - 流动性 `liquidity`
  - 规模稳定性 `size_stability`
- 数据依赖：`factor_input_daily`、`stock_basic`、`daily_kline`。

### 3. 龙头战法选股

- `strategy_id`: `leader_tactics`
- 核心语义：贴合 A 股题材和龙头交易，关注强势程度、成交额、资金确认和市场热度。
- 因子：
  - 强势程度 `strength`
  - 放量确认 `volume_confirm`
  - 资金确认 `fund_flow`
  - 龙头辨识度 `leadership`
  - 舆情热度 `sentiment`
  - 市场热度 `market_heat`
  - 换手健康度 `turnover`
- 数据依赖：`daily_kline`、`factor_input_daily`、`stock_moneyflow_daily`、`stock_sentiment_daily`、`market_context_daily`。

### 4. A股舆情选股

- `strategy_id`: `a_share_sentiment`
- 核心语义：承认 A 股短线涨跌与新闻/公告/题材舆情关系较强，但不让页面实时等待外部 API。
- 当前实现：优先使用本地 `stock_sentiment_daily` 缓存，并允许读取最近一个不晚于交易日的舆情快照。
- DeepSeek TopN 精排：已接入 `deepseek_rerank`，默认仅对本地舆情初筛 Top10 做一次批量 DeepSeek 分析，按 `base_score * 70% + ai_sentiment_score * 30%` 重新排序；若 DeepSeek 不可用或超时，自动 fallback 到本地缓存排名。
- 因子：
  - 舆情方向 `sentiment`
  - 新闻热度 `news_heat`
  - 资金确认 `fund_flow`
  - 价格确认 `price_confirm`
  - 成交确认 `volume_confirm`
  - 市场环境 `market_context`

## 新增交易策略

### 五日止盈止损

- `trade_strategy_id`: `triple_barrier_5d`
- 中文名：五日止盈止损
- 买入：入选日开盘买入。
- 卖出：最多持有 5 个交易日；触及 +6% 止盈、-3% 止损或到期收盘，谁先发生就退出。
- 当前回测粒度：日 OHLC。盘中触发用 high/low 穿越 barrier 近似；退出价使用 barrier 价格或到期收盘价。

## 已验证

- 本地 `py_compile` 通过。
- 本地 `StrategyService.run_strategy(save=False)`：4 个新增选股策略均能返回 3 条结果。
- 本地短区间回测：4 个新增选股策略均能完成 2026-05-08 单日回测。
- 本地 `fund_chip_repair + triple_barrier_5d` 回测成功。
- 公网验证：
  - `/api/selection/run` 4 个新增选股策略均返回 3 条结果。
  - `/api/trade-strategies` 返回 `triple_barrier_5d`。
  - `/api/backtest/run` 使用 `fund_chip_repair + triple_barrier_5d` 成功。
  - `/selection`、`/backtest`、`/trade-strategies`、`/strategies` 均 200。

## 注意事项

1. 舆情策略当前只对本地初筛 TopN 做一次批量 DeepSeek 精排，不做全市场实时 LLM 调用；后续如需审计留痕，可把 DeepSeek 结果写回 `stock_sentiment_daily` 或新增 `stock_sentiment_ai_review`。
2. 龙头战法是实验策略，适合结合市场热度/题材资金流观察，不宜直接视为稳定实盘策略。
3. 五日止盈止损是交易策略第一版，当前按日线 OHLC 近似触发；若后续要更严谨，应接分钟线/真实交易约束。

## 2026-05-11 22:23 追加验证

- `A股舆情选股` 已新增 TopN DeepSeek 精排模块：`app/stock_selection/deepseek_sentiment_rerank.py`。
- 配置位于 `app/strategies/registry/configs/a_share_sentiment.yaml`：`top_n=10`、`model=deepseek-chat`、`ai_weight=0.30`、`max_news_per_stock=2`、`timeout_seconds=35`。
- 公网 `/api/selection/run` 验证：`a_share_sentiment` 返回 3 条，`deepseek_rerank.available=true`、`error=null`、`model=deepseek-chat`、`requested=10`，耗时约 15.4s。
- 为了支撑新增策略数据完整性，已刷新 `lowvol_reversal_feature_daily` 到 `2026-05-11`，并补跑 `market_context_daily` 到 `2026-05-11`。
- 最新交易日 `2026-05-11` 数据覆盖：
  - 资金筹码修复：moneyflow 94.3%、chip 100%、std_return_20 94.3%、turnover 100%。
  - 质量低波：ROE/利润/营收/EPS 等约 92%~94%，PB 93.6%，PE 70.1%，std_return_20 94.3%。
  - 龙头战法：pct_chg_1d 94.3%、volume_ratio 99.9%、moneyflow 94.3%、total_mv 100%、market_strength 100%；舆情覆盖约 6.2%，因此舆情只作为加分项。
  - A股舆情：舆情缓存覆盖约 6.2%，moneyflow 94.3%、volume_ratio 99.9%、market_strength 100%；该策略天然依赖新闻覆盖，适合 TopN 精排而非全市场覆盖。
- 回测支持验证：4 个新增选股策略均可在 `2026-05-08~2026-05-11` 完成回测；`leader_tactics + triple_barrier_5d` 公网回测成功，2 个交易日、6 笔交易。

## 2026-05-11 22:53 数据补充

- 新增脚本 `scripts/run_strategy_sentiment_refresh.py`，按新增策略的打分 TopN 构建候选池，批量补 `stock_news` / `stock_sentiment_daily`。
- 本轮补数：策略候选池 180 只，AkShare 个股新闻 180 次，Tavily 精搜 25 次，raw_news=1990，有效新闻=500，failed=0。
- `2026-05-11` 直接舆情日表：192 行，其中 sentiment_score 非空 136 行。
- 按“不晚于当前交易日的最近舆情快照”join 到全市场后：sentiment_score 覆盖 393/5491=7.16%，news_count 覆盖 450/5491=8.20%。
- 更关键的策略候选覆盖：
  - 资金筹码修复 Top80 舆情覆盖 61.3%。
  - 质量低波 Top80 舆情覆盖 76.2%。
  - 龙头战法 Top80 舆情覆盖 38.8%。
  - A股舆情 Top80 舆情覆盖 100%。
- 同步跑了估值补数：3 批共 scanned=1500，updated=311，missing_source=1189；随后刷新 `factor_input_daily` 最新日 5516 行。
- 最新 PE/PB：`factor_input_daily.pe_tushare` 4041/5516=73.26%；按股票 universe join 后 PE 4109/5491=74.83%，PB 5452/5491=99.29%。PE 仍有缺口主要是 Tushare 源无 PE 或亏损口径，不强行造值。
- 补数后 4 个新增选股策略均已用 `triple_barrier_5d` 完成 2026-05-08~2026-05-11 回测 smoke。
