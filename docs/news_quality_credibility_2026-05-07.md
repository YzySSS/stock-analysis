# 新闻质量过滤与可信度评分（2026-05-07）

## 目标

先提升舆情输入质量，避免低质/无关/乱码新闻污染 `stock_sentiment_daily.sentiment_score`。

## 已实现

### 1. 新闻质量过滤

接入位置：`scripts/run_sentiment_daily_update.py`

过滤规则复用并升级 `src/news_filter.py`：
- 最低来源可信度：`0.35`
- 最大新闻年龄：`7` 天
- 最低质量分：`45`
- 按股票隔离标题去重，避免跨股票误杀
- 过滤明显无效标题、重复标题、弱相关内容、过期内容、明显乱码文本
- 标题党/情绪词不再直接一刀切，而是通过质量分降权，减少误杀有效公告/财经新闻

### 2. 可信度评分

接入位置：`src/news_credibility.py` + `scripts/run_sentiment_daily_update.py`

单条新闻写入：
- `credibility_score`：0~1
- `credibility_level`：S/A/B/C/D
- `credibility_reason`：评级原因

评分组合：
- URL 域名评级优先识别真实媒体来源
- 来源名规则作为补充兜底
- Tavily 返回结果已尽量保留真实域名作为 `source`，不再统一写成 Tavily 聚合源

### 3. 入库字段

`stock_news` 新增：
- `credibility_level`
- `credibility_reason`
- `quality_score`
- `quality_level`

`stock_sentiment_daily` 新增：
- `raw_news_count`
- `filtered_news_count`
- `quality_avg`

说明：
- `news_count` 继续表示最终用于舆情计算的有效新闻数
- `raw_news_count` 表示过滤前抓到的新闻数
- `filtered_news_count` 当前等同有效新闻数，便于页面/接口后续展示过滤口径
- `quality_avg` 表示有效新闻平均质量分

## 验证结果

命令：

```bash
./.venv/bin/python scripts/run_sentiment_daily_update.py --limit 2 --sleep-seconds 0
```

结果：

```json
{"trade_date":"2026-05-06","requested":2,"updated":2,"failed":0,"raw_news":20,"total_news":15,"filtered_out":5}
```

样例：
- `sh.600000`：raw `10`，有效 `7`，`credibility_avg=0.6286`，`quality_avg=72.8571`
- `sh.600004`：raw `10`，有效 `8`，`credibility_avg=0.7625`，`quality_avg=73.1250`

补充验证：乱码标题检测已通过，明显编码错误标题会被过滤。

## 后续建议

1. 在舆情数据页面或系统状态页展示 raw/effective/filtered 口径。
2. 后续如果要进一步提升质量，可把 `credibility_score` 与 `quality_score` 同时用于舆情加权，而不是只按可信度加权。
3. 对 DuckDuckGo redirect URL 可再做一次真实目标 URL 解码，提升兜底源可信度识别。

## 页面展示补充（13:32）

已在 `/api/system/status` 增加 `sentiment_quality` 轻量统计，并在 `/system` 数据状态页新增“舆情质量”卡片。

当前展示字段：
- 最新舆情交易日
- 覆盖股票数
- 原始新闻数、有效新闻数、过滤数、过滤比例
- 平均可信度
- 平均质量分
- 近 7 天新闻质量分布
- 近 7 天新闻可信度等级分布

公网验证：
- `https://www.yzysstock.cloud/system` 返回 200
- `https://www.yzysstock.cloud/api/system/status` 返回 `sentiment_quality`

## 个股详情页最近舆情（13:36）

按大X建议，个股详情页已直接展示最近舆情新闻：
- 接口：`GET /api/stocks/{code}?news_limit=12`
- 字段：`recent_news[]`
- 排序：`COALESCE(published_at, created_at) DESC, id DESC`
- 页面：`/stocks/{code}` 新增“最近舆情新闻”卡片

每条新闻展示：
- 发布时间/入库时间
- 标题，支持直接点击原文链接
- 来源
- 可信度等级与分数
- 可信度原因
- 质量分
- 情绪分

验证：
- `https://www.yzysstock.cloud/stocks/sh.600000` 返回 200
- `https://www.yzysstock.cloud/api/stocks/sh.600000?news_limit=3` 返回 200，包含 3 条 `recent_news`

## 新闻源分层策略更新（15:18）

按大X确认，舆情采集改为“两层模型”：

1. **AkShare 主源**
   - `ak.stock_news_em(symbol=code)` 作为个股新闻主源。
   - 优点：结构化、免费、财经垂直、适合批量覆盖。
   - 当前标准化字段：`新闻标题/新闻内容/发布时间/文章来源/新闻链接` → `title/content/datetime/source/url`。

2. **Tavily 精搜**
   - 只对每日候选池得分靠前的股票做精细网页搜索。
   - 默认 `--tavily-top-n 50`。
   - 用于补 AkShare 之外的网页新闻、公告解读和跨媒体信息。

`scripts/run_sentiment_daily_update.py` 新增参数：

- `--universe selection_score|stock_basic`：默认 `selection_score`，优先取最新 `selection_result` 中按分数排序的股票；无结果时 fallback 到 `stock_basic`。
- `--tavily-top-n N`：默认 50，只对前 N 只做 Tavily。
- `--akshare-only`：禁用 Tavily，只跑 AkShare 主源。

验证：

```bash
./.venv/bin/python scripts/run_sentiment_daily_update.py --limit 3 --tavily-top-n 2 --sleep-seconds 0.1
```

结果：`updated=3`、`failed=0`、`raw_news=48`、`total_news=17`、`akshare_runs=3`、`tavily_runs=2`。
