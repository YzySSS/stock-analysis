# 历史舆情资料与不可变新闻版本 · 2026-11-17

本轮继续补数据，不重做已有选股快照诊断，也不开放完整历史选择器。

## 真实来源核验

- Tushare `news`（正文）和 `cctv_news`：当前账户明确无接口权限。未购买、升级或更换凭证。
- Tushare `major_news`：可查历史 `title / pub_time / src / url`，**没有正文**。2026-11-16 和 2025-11-17 全天探针各返回恰800条，故不能把全天单次响应当完整库存。新工具先按6小时分窗，达到800条继续二分；最小窗口仍触顶明确缺口。
- 真实账户同时限制20次/分钟及30次/小时。首轮触发分钟限额，恢复时发现小时限额。现已加入3.2秒串行节流、遇限流立即停止后续网络请求、保留所有未取窗口以及按内容寻址manifest仅恢复缺窗。不能因“能重试”宣称已补齐。

## 已封存的真实数据

独立目录 `logs/sentiment-news-history`，只写本地证据；没有把后补新闻写进生产选股/策略/event表。

| 材料 | 数量与范围 | 可证明的事实 |
| --- | --- | --- |
| 历史标题资料 | 8,055条；11/10=1,967、11/11=2,019、11/12=1,717、11/13=1,604、11/14=748 | 七家媒体的provider历史标题、发布时间声明和链接；非正文，非当时系统已知证据 |
| 旧 `stock_news` 当前版本 | 8,487条（按主键有界遍历，非只限最近七天） | 当前库中仍存在的正文/摘要与raw_json版本；不能重建被覆盖的旧正文 |
| 旧 `market_opinion_raw` 当前版本 | 8,555条，trade_date为11/10～16 | 保留整行原始材料；其旧published_at可能包含crawl兜底，只作为legacy原始字段保留，不升级为真实发布日期 |
| 当前真实NewsNow财联社读取 | 30条，源响应为cache，全部headline_only | 封存观察前严格as-of为0条、观察后30条；不把上游缓存日期或旧发布日期当系统首次可见时间 |

11/15～16共8个6小时窗口仍受账户小时限额阻挡，最新manifest状态为`partial`。标题覆盖并不代表全市场新闻覆盖，即使所有请求成功也固定`provider_coverage_complete_proven=false`。

关键证据（workspace/output）：

- `sentiment-news-history-backfill-20261117.json`：原首轮窗口与失败记录。
- `sentiment-news-history-backfill-resumed-20261117.json`：旧证据不改，恢复后的独立manifest；SHA `d1ae9403080a47fbafa463b01461c511b88c0f9f274d1cfe42eaf6c010a82a6a`。
- `sentiment-news-history-inventory-20261117.json`：17,042条旧库当前版本；manifest SHA `4d587f1c741dace2d6b6480ff9d47b3fe806d4bfaf3f7929b70d13c19e9d1cf9`。
- `sentiment-news-history-live-smoke-20261117.json`：真实30条读取与严格时钟验证。
- `sentiment-news-history-validation-20261117.json`：逐文件hash验证、来源/日期统计、后补与legacy严格as-of均为0、封存大小。

## 新数据链路

`NewsAggregator.get_stock_news` 在去重/过滤之前封存每个真实返回的adapter payload；保留未入选和同标题不同内容版本。NewsNow市场采集在打分/关系覆盖之前按源批量封存原始item与时钟。任务摘要保存封存条数/批数/警告，不把大正文塞进task日志。

这保存的是采集adapter实际传来的payload，不声称是媒体网页全文或完整HTTP响应。现有adapter会截取摘要，NewsNow很多源只有标题。`content_kind`明确区分`headline_only / provider_excerpt / legacy_current_excerpt`，不会用标题冒充正文。

- 每个版本保留稳定URL优先的article_id、内容version SHA、原provider ID（无ID时标synthetic）、provider发布时间原文与精度、真实observed/available时钟以及完整adapter payload。
- `historical_backfill`和`legacy_inventory`始终退出严格as-of。当前重新取得旧标题/正文不证明过去就可得。`live_observation`只能从本次真实封存观察时点以后可见；日期精度发布时间保守到当日23:59:59，同一时钟冲突版本fail-closed，不擅定修订顺序。
- 文件内容寻址+gzip、0600文件/0700目录、fsync+不可覆盖发布，2GiB/50,000文件总限、64MiB单文件限；达到预算不删旧证据、不阻断正常新闻/选股链，明确记录封存警告。
- 本地封存不等于异地备份，hash只能证明封存后不变，不能追认历史内容正确或完整。

## 命令

所有命令从仓库执行，使用项目解释器。日期范围最多7自然日，结束日期右开，接口调用预算最多300。

```sh
PYTHONPATH=. .venv/bin/python scripts/archive_sentiment_news_history.py backfill --start-date 2026-11-10 --end-date 2026-11-17 --max-calls 50 --resume-manifest-sha256 d1ae9403080a47fbafa463b01461c511b88c0f9f274d1cfe42eaf6c010a82a6a
PYTHONPATH=. .venv/bin/python scripts/archive_sentiment_news_history.py inventory --start-date 2026-11-10 --end-date 2026-11-17
PYTHONPATH=. .venv/bin/python scripts/archive_sentiment_news_history.py inspect --manifest-sha256 d1ae9403080a47fbafa463b01461c511b88c0f9f274d1cfe42eaf6c010a82a6a
```

`inspect`完全离线；`--news-sha256 HASH --as-of DATETIME`可对单份封存批次核对严格可见计数。恢复命令必须在小时限额窗口恢复以后运行，不能连续重试；工具也读取上一manifest的`retry_not_before`，冷却未到时不发网络请求，保留缺窗。新一轮返回新的manifest SHA，不覆盖旧记录。

## 验证与边界

新增14项有意义测试：版本修订/as-of、同钟冲突、发布日期精度、历史倒填排除、篡改、配额不删证据、触顶二分、边界去重、最小窗仍截断、调用预算/越界、空窗、缺窗恢复、小时限流即停、采集在去重过滤之前。完整846项执行845通过1skip（4.288秒），日志`output/sentiment-news-history-unittest-20261117.log`。

独立审查追加修复：NewsNow的`item_id`可能由适配器用标题合成，封存层改从真实`raw.id/key`判断provider原ID，适配器合成值仅留在原payload；发布时间也改保留`raw.pubDate/date/createdAt`声明，避免经过适配器解析后丢失日期精度或时区。封存层支持epoch秒/毫秒、ISO时区和RSS RFC时间，不改既有生产选股时间逻辑。增加4项测试覆盖这些语义与冷却前/后恢复；18项定向测试通过，完整850项执行849通过1skip（4.619秒），日志`output/sentiment-news-history-independent-unittest-20261117.log`，`git diff --check`通过。

无MySQL DDL、无策略参数/冻结cohort/交易权限变化；不修改`app/shared`、`app/strategies`、`app/stock_selection`或旧回测service。完整舆情历史回测仍返回原409。原始历史正文、当时首次可见、选择器全部辅助输入仍存在缺口；本轮标题与旧版本保全不得作为完整历史回测已可用的依据。

## 根复核与缺窗续采

- 根串行复跑完整850项，849通过、1项跳过（4.307秒）；日志 `output/sentiment-news-history-root-unittest-20261117.log`。
- 独立逐一读取61份已存文件，内容hash和0600文件/0700目录权限通过；历史标题8,055条、legacy17,042条均确认严格as-of为0。现存压缩文件合计10,139,052字节，不等于去重后的唯一新闻篇数。证据 `output/sentiment-news-history-root-validation-20261117.json`。
- 根重新真实读取财联社30条并封存，源状态`cache`也保留在payload；观察前0条、观察后30条，SHA `90eeaa8cfa64d25902949516d2fa6c7ee6cdfde47a188226437ca5d7b2beab17`。生产封存同时保留adapter的来源元数据、缓存状态、原排名和移动链接；不改变原选股数据。
- 当前研究source SHA仍为 `a1d5ba02d1f0046cf886a4af29d42318d5c5b4001728eedaf17ccc1e3db786db`，依赖SHA仍为 `f15258898a4afc76ef6101904ac4e25a8ee8c2b3daaadc02e1a41557aa7b0248`。
- 11/15～16的8个6小时窗口受小时额度约束，最近失败发生16:10；**未连续重试，也未宣称补齐**。已创建上海时间17:15一次性续采 `69bbc09c-fca3-4e81-bdf8-cf55d2733fcc`，从上述manifest仅取缺窗，本次最多20调用，结果写新的output文件与新manifest；如再限流按新`retry_not_before`恢复同一任务，不覆盖旧证据。
- 原manifest生成时尚未加入cooldown字段，所以首次恢复明确不早于17:15；之后的新manifest包含冷却时钟。

## 生产发布验收

- 实现提交 `883b81a` 已推送GitHub main，服务器运行同份代码。当前checkout为 `codex/sentiment-v06-implementation`，本地main不是部署来源；通过非强制 `HEAD:main` 快进发布，远端hash已比对。
- 发布前readiness ready、四队列queued/running均0；仅串行重启导入新闻聚合器的API、selection及backtest Worker，等待本地health成功后再验公网。portfolio/durable Worker未重启。
- 正常舆情采集脚本人工运行 `--sources cls --lookback-days 30 --sleep-seconds 0`：success、failed_sources=0、封存1批30条、warnings=0、23.43秒。没有历史时点override，不写后补资料到生产策略表；本次是正常实时采集路径验证。证据 `output/sentiment-news-history-production-task-20261117.json`。
- 根本地/公网14项HTTP检查通过：health/readiness 200、三版历史回测请求均为原409、记录快照列表200且保持`selector_recomputed=false / full_history_backtest_available=false`、匿名readiness401；40/40迁移，五服务active/NRestarts0，四队列queued/running均0。证据 `output/sentiment-news-history-root-http-20261117.json`，可复用probe同名前缀 `.py`；探针含合法内存CSRF会话，不输出凭证。
- source/dependency双指纹与根复核前完全一致，无DDL、策略参数、历史研究cohort或交易权限改变；发布时工作树干净。最终文档提交仅补充上述验收，不需再重启。

下一自然市场采集为下一交易日09:00、个股采集为次日03:50；不可把人工smoke当自然Cron验收。缺窗自动续采仍待17:15，正文权限及历史首次可见证明仍缺，不宣称完整历史回测可用。
