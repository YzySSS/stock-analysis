# A股舆情选股 V2 设计草案（2026-05-14）

## 目标

将当前“个股新闻 + 资金/价格确认”的舆情选股，升级为“舆情主导的热点板块/主题发现 + 板块内选股”。

核心语义：

> 舆情决定方向，资金确认可交易性，价格判断是否追高。

## 已确认设计

### 1. 数据源层

不直接集成 TrendRadar；借鉴其多平台热榜/RSS 聚合思路，独立实现本项目的数据采集模块。

- TrendRadar 参考链接：<https://github.com/sansan0/TrendRadar>
- NewsNow 直接数据源：<https://github.com/ourongxing/newsnow>

推荐先使用 NewsNow API：

```text
https://newsnow.busiyi.world/api/s?id=<source>&latest
```

后续如稳定性/频次需要，可自部署 NewsNow，并通过 `NEWSNOW_BASE_URL` 切换。

首批推荐源：

```text
财经强相关：
- cls
- cls-hot
- wallstreetcn
- wallstreetcn-hot
- xueqiu
- jin10
- mktnews
- gelonghui
- fastbull

泛舆情辅助：
- baidu
- toutiao
- weibo
- zhihu
- thepaper
- ifeng
```

### 2. 数据链路

```text
NewsNow / RSS / Google News / AkShare 原始新闻热榜
→ market_opinion_raw 原始落库
→ 新闻清洗、去重、来源权重、时间衰减
→ 主题 / 板块 / 股票映射
→ sector_opinion_daily 板块舆情评分
→ 板块内个股评分
→ A股舆情选股结果
```

### 3. 评分骨架

大X提出的核心骨架：

```text
信源评分 source_score
信息重要度 importance_score
```

建议补充第三层：

```text
传播热度 amplification_score
```

三者职责分离：

- `source_score`：消息来源靠不靠谱。
- `importance_score`：消息本身对股票/板块是否重要。
- `amplification_score`：市场有没有关注、扩散、持续讨论。

单条新闻可先用：

```text
news_impact_score =
  source_score × 30%
+ importance_score × 45%
+ amplification_score × 25%
```

若为负面事件，保留方向：

```text
direction = positive / neutral / negative
signed_news_score = news_impact_score × direction_factor
```

```text
positive = +1
neutral = 0
negative = -1
```

## 可以开始落地的范围

可以先进入 P0 开发，不必等所有细节完全定稿。

P0 目标不是一次做完完美策略，而是跑通闭环：

```text
多源舆情采集 → 原始落库 → 基础评分 → 板块聚合 → 选股策略读取
```

P0 可先使用规则评分，DeepSeek 只作为可选解释/复核，不进入全量主链路。

## 仍需补齐的设计口径

### 1. 信源基础分表

需要明确每类来源的默认分：

```text
监管/交易所/公司公告：95-100
财联社/证券时报/上证报/中证报：85-95
华尔街见闻/金十/格隆汇/MKTNews：75-90
雪球热门股票/东方财富/同花顺：60-80
百度/微博/头条/知乎泛平台：45-70
未知/自媒体/低质转载：20-50
```

P0 可先配置在代码/YAML 中，后续落表动态调整。

### 2. 信息重要度事件分类

需要定义事件类型与基础分：

```text
强正面催化：政策落地、重大订单、并购重组、业绩大超预期、涨价、技术突破
中正面催化：行业景气、产品发布、机构调研、产能扩张、供应链合作
弱催化：普通新闻、品牌曝光、泛行业讨论、概念提及
强负面：监管处罚、问询函、减持、业绩暴雷、安全事故、诉讼、造假风险
```

P0 可先用关键词规则 + source 类型判断；P1 再引入 LLM 小范围分类。

### 3. 主题/板块映射规则

需要从新闻标题映射到：

- 行业
- 概念
- 股票代码/名称
- 自定义主题词

P0 可先用：

```text
stock_basic.name / industry
概念成分股表（如后续补齐）
人工维护主题词典
```

### 4. 板块聚合公式

建议：

```text
sector_opinion_score =
  加权新闻影响分 × 45%
+ 覆盖股票数量/扩散度 × 20%
+ 来源多样性 × 15%
+ 热度持续性 × 10%
+ 主题一致性 × 10%
```

### 5. 板块内个股评分公式

建议：

```text
stock_score =
  所属板块舆情分 × 30%
+ 个股直接舆情分 × 25%
+ 主题匹配度 × 15%
+ 资金确认 × 15%
+ 价格不过热 × 10%
+ 流动性 × 5%
```

### 6. 防噪音和风险口径

必须避免：

- 娱乐/社会新闻误映射到股票。
- 泛关键词误伤，例如“机器人”“AI”“芯片”在非财经语境中泛滥。
- 单一低质来源把板块拉高。
- 负面新闻被错误当成热度利好。

P0 至少应有：

```text
财经相关性过滤
负面事件方向识别
低质来源降权
同标题/相似标题去重
```

## 建议实施顺序

### P0：采集与基础评分闭环

1. 新增 `NewsNowClient`。
2. 新增 `market_opinion_raw` 表。
3. 定时/手动脚本 `run_market_opinion_update.py`。
4. 完成标题去重、source_score、rank/amplification 基础分。
5. 先按行业/股票名称做最小映射。

### P1：板块舆情聚合

1. 新增 `sector_opinion_daily` 表。
2. 实现主题/行业/股票聚合。
3. 输出 Top 舆情板块和解释字段。

### P2：接入 A股舆情选股策略

1. 改造 `a_share_sentiment`：从“全市场个股舆情打分”改为“先板块、再个股”。
2. 保留资金/价格/流动性作为确认因子。
3. 前端展示板块来源、热点原因、个股入选原因。

### P3：LLM 精排与解释

1. DeepSeek 只对 Top 板块/Top 股票做摘要和风险识别。
2. Tavily 只作为手动或 Top 主题低频增强。

## 当前结论

可以开始落地 P0。

仍需细化的是评分常量和映射词典，但这些可以边做边校准，不需要阻塞数据采集和基础评分闭环。

## P0 落地进展（2026-05-14 01:38）

已完成第一版近期数据验证闭环：

- 新增 `app/data_ingestion/newsnow_client.py`
- 新增 `app/orchestration/market_opinion_schema.py`
- 新增 `scripts/run_market_opinion_update.py`

新增表：

```text
market_opinion_raw
market_opinion_stock_match
market_opinion_sector_match
sector_opinion_daily
```

本轮后台分两批抓取 NewsNow 近期源，覆盖 12 个源、288 条近期原始舆情，聚合出 27 个板块/主题候选。

最新验证样例：

```text
AI算力：20 条新闻、8 个来源；候选股票含 工业富联 / 中际旭创 / 天孚通信 / 新易盛
半导体：9 条新闻、5 个来源；候选股票含 澜起科技 / 寒武纪 / 立讯精密 / 胜宏科技
有色金属：6 条新闻、3 个来源；候选股票含 紫金矿业 / 云南锗业 / 北方稀土 / 洛阳钼业
化工原料：直接新闻命中 东方碳素 / 比亚迪
```

无未来函数约束已落入代码：

- 新闻可用时间统一使用 `COALESCE(published_at, crawl_time)`。
- 若 NewsNow/source 提供的 `pubDate` 晚于本系统实际抓取时间，则 capped 到 `crawl_time`。
- 板块聚合查询强制：`COALESCE(published_at, crawl_time) <= as_of_datetime`。
- 候选股票行情只读取 `daily_kline.trade_date <= as_of.date()` 的最新记录。
- 当前 SQL 校验：聚合进 `sector_opinion_daily` 的新闻未来行数为 `0`。

后续需继续优化：

- 泛新闻源噪音过滤仍需加强，特别是非财经语境的关键词误映射。
- 主题到行业/股票的映射目前是规则版，可先用于验证，不应当成最终生产评分。
- 下一步可把 `sector_opinion_daily` 接入 `a_share_sentiment` 策略，但必须继续保留 `as_of_datetime` / `trade_date <= as_of` 约束。

## 12:35 - 正式接入选股中心 P0

本轮已把前面验证通过的 `market_opinion_raw -> sector_opinion_daily` 链路正式接入 `A股舆情选股` 可执行策略：

- `StockSelector` 在 `strategy_id=a_share_sentiment` 时，会读取最新 `sector_opinion_daily`，将 `top_stocks_json` 中的热点板块/主题候选映射回全市场候选股。
- `AShareSentimentStrategy` 改为优先使用 `market_opinion_v2` 模式：先要求候选股命中热点板块/主题，再计算 `sector_heat / source_credibility / info_importance / amplification / stock_match` 等舆情主因子。
- 资金、价格、成交量、市场环境降级为交易确认因子，避免旧版“资金/技术主导”反客为主。
- 若未来没有可用 `sector_opinion_daily` 数据，策略会自动回退旧版 `stock_sentiment_daily` 个股舆情模式，避免页面直接不可用。
- `a_share_sentiment.yaml` 已关闭默认 DeepSeek rerank，P0 日常运行不消耗 DeepSeek/Tavily；后续可只对 Top 板块/Top 股票做解释增强。
- `scripts/setup_kline_cron.sh` 已加入 `run_market_opinion_update.py`，交易日 09:00-15:59 每 15 分钟刷新；系统页任务也已展示“热点舆情聚合”。

验证：

- Python 语法检查通过：`selector.py`、`thematic_strategies.py`、`system.py`、`run_market_opinion_update.py`。
- 本地策略运行通过：`A股舆情选股` 返回 3 条，模式为 `market_opinion_v2`，当前 Top 为 `工业富联 / 天孚通信 / 新易盛`，命中主题 `AI算力`。
- 公网接口通过：`POST https://www.yzysstock.cloud/api/selection/run` 返回 200。
- 公网页面通过：`https://www.yzysstock.cloud/selection` 返回 200。
- 系统页任务通过：`/api/system/status` 已出现 `market_opinion_update`，最近 run 为 success。

## P1 补充说明（2026-05-14）

- P0 已在回测入口禁用 `a_share_sentiment`：当前策略依赖实时 `market_opinion_v2` 聚合，尚未注入按历史 `as_of_datetime` 重建的舆情快照。
- 因此当前选股页展示的是实时/准实时舆情选股解释，不代表历史回测可用；历史舆情回测需要补齐可复现的历史新闻源、源失败状态、板块聚合快照与反未来函数校验，归入后续 P2/P3。

## P1 修复进展（2026-05-16）

已先修复回测可信度相关的基础问题：

- `sector_opinion_daily` 的唯一键改为 `trade_date + as_of_datetime + sector_type + sector_name`，同一交易日可以保留多个舆情快照，不再每次刷新覆盖整天快照。
- `market_opinion_raw` 增加 `first_seen_at / last_seen_at`，重复标题保留最早可见时间，避免无稳定发布时间的新闻被后续抓取刷新成“新新闻”。
- `StockSelector` 支持通过 `market_opinion_as_of` 指定历史舆情快照；实时模式下若最新舆情快照与最新行情交易日一致，即使超过 `max_age_minutes` 也继续使用 `market_opinion_v2`，避免夜间/周末静默退回旧个股舆情口径。
- `load_sector_candidate_stocks()` 对盘中 `as_of` 不再读取当日日线收盘数据，15:05 前只使用前一交易日数据，降低历史盘中回测的未来函数风险。
- `top_stocks_json` 区分 `direct_news_match` 与 `sector_candidate`；策略对板块候选池股票的 `stock_match` 做上限保护，避免把“板块内候选”解释成强个股舆情命中。

## 2026-05-28 补充：板块内辨识度/涨停潜力口径

本轮校准后，`a_share_sentiment` 的目标不再只是“舆情相关股票”，而是“热点主题中更容易被资金识别和接力的前排标的”。股票可以不在全市场热榜最前，但必须在对应主题/板块内有辨识度。

实现口径：

- `load_sector_candidate_stocks()` 的板块候选池从偏成交额排序，改为优先按涨幅、换手、成交额排序，避免大市值/大成交但不够前排的股票挤占名额。
- `StockSelector` 为每个 `top_stocks_json` 候选新增 `opinion_stock_recognition_score / label / reason / rank`，综合板块内排名、涨幅、成交额、直接新闻命中和涨停记忆。
- `AShareSentimentStrategy` 新增因子 `stock_recognition`（板块内辨识度），权重高于普通传播热度；`sector_candidate` 若辨识度低于配置阈值会被过滤，直接新闻命中仍可保留。
- 前端 `sentiment_context` 展示“板块辨识度”，让用户能看出某只票是主题前排、活跃候选，还是低辨识度跟风。

仍未开放正式历史回测按钮。下一步应做最小 P1 回测：只读取已存在的历史 `as_of` 快照，以“信号日后一个可交易开盘价”为入场口径，先验证最近少量交易日。

## 运行时渐进式精排（2026-05-19）

大X确认 `A股舆情选股` 不应直接从全市场股票做一次 TopN，而应先判断市场热点主题，再在主题里选股票。

当前运行链路调整为：

- 本地 `sector_opinion_daily` 先按热点聚合分取 Top10 板块/主题。
- 对 Top10 板块/主题运行 Tavily 搜索，补充外部新闻证据。
- DeepSeek 结合本地热点和 Tavily 新闻精排出 Top3 板块/主题。
- 只在 Top3 板块/主题映射出的股票池内，用本地舆情/资金/价格/成交因子排出 Top30 股票。
- 对 Top30 股票运行 Tavily 个股新闻搜索并写入 `stock_news` / `stock_sentiment_daily`。
- DeepSeek 结合本地热点、股票因子和 Tavily 新闻做最终精排。
- 最终结果仍遵循选股页面传入的分数阈值和数量上限 `limit`。

实现约束：

- Tavily/DeepSeek 只在运行 `a_share_sentiment` 时触发，回测仍不开启该链路。
- 若 Tavily 或 DeepSeek 不可用，返回 summary 中会显式标记，并尽量 fallback 到本地热点分，不让基础选股直接失败。
- 中间结果通过 API 返回 `progressive_rerank`，用于页面展示板块/股票精排漏斗。

## 舆情时效性评分（2026-05-19）

热点排序不能只看新闻命中数量和来源权重，还要按新闻事件类型动态衰减时效分。

当前规则：

- 每条原始舆情入库时记录 `timeliness_score / timeliness_level / effective_until`，聚合板块时会按当前 `as_of_datetime` 重新计算，避免旧新闻永久保留高分。
- 普通热点按 `1 天内 / 3 天内 / 7 天内 / 一个月内 / 失效` 衰减。
- 政策性文件、重大技术突破等长效事件使用更长有效期，最高可保留一年，但分数会随时间下降，不会一直满分。
- 企业火灾、爆炸、事故、传闻、澄清、短线异动等短效风险事件使用更短有效期，通常 3-14 天内快速失去时效分。
- 板块聚合分新增 `avg_timeliness` 权重，最终 `sector_score` 中时效性占 10%；单条新闻 `impact_score` 也乘以时效衰减因子。
- 定时任务默认抓取近 30 天新闻；聚合查询会按最长事件有效期保留必要历史，但低时效或已失效新闻不会继续贡献分数。
