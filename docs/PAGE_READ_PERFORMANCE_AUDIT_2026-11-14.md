# 全页面读取性能审计与简化方案（2026-11-14）

状态：**审计完成；第一批五项已实现并上线，第二、三批仍待逐项实施。** 进展和真实验收见 [实施记录](PAGE_READ_OPTIMIZATION_IMPLEMENTATION_2026-11-14.md)。下文审计样本和“建议”保留原现场含义，不代表后续批次已完成。

审计代码：`04a835c`。覆盖 9 个正式导航页面、个股详情、登录，共 **11/11 页**；另追踪共享认证请求、首屏、翻页、筛选、手动刷新、自动刷新及任务轮询路径。历史设计预览不属于生产业务页，不列入性能改造范围。保持 [UI_DESIGN_BASELINE.md](UI_DESIGN_BASELINE.md) 的布局与真实数据/研究语义。

## 结论

用户提出的方向成立：**减少读取范围、拆开不同更新频率的数据、把重复聚合移到有版本的读模型，通常比继续压缩单条暖缓存 SQL 更接近实际体验。** 但不是所有页面都需要分表，更不是每个交易日建立一批新物理表。

当前最值得优先处理的五条链路：

1. **个股详情**：默认图只看近一个月，却取 600 根日 K；详情与概览重复读取基础/报价/历史，每 60 秒整包重拉。舆情回退还会先完整还原 40 个板块再找当前股票，存在已经证实的过读。
2. **跟踪复盘**：本页列表确实在 SQL 分页，但筛选项每次翻页重拉，冷的全范围统计仍会加载全部有效样本；持有期分钟极值仍按原始分钟读取，值得建精确日级中间层。
3. **选股中心**：只为显示风险警示，先取约 84KB 首页整包，再串行获取策略和最近任务。应复用相同风险结果的轻量投影，而不是再算一套风险。
4. **首页**：Redis 命中很快，但 summary 仍约 84KB，其中 market_timing 约 66KB。首屏与展开证据应分开，不把大量研究证据当作每次必传字段。
5. **数据状态**：Redis 命中约 8ms，但返回约 152KB，约 107KB 是 43 个任务的完整记录/metadata，约 25KB 是 DQ 明细。需要摘要与证据分层，而非再加同义缓存。

回测还有确证的每页 N+1 查询，但本次十条交易页仅约 18ms，优先级低于前述范围/请求链问题。ETF 已有物化结果，主要需拆包；不宜再建重复轮动表。交易策略只有 5 条、注册选股策略只有 3 条，强行给这些目录做分页反而会增加往返。

## 1. 范围、方法和证据边界

- 串行静态追踪 `app/api/web/js/`、`app/api/routes/`、repository/service/read-model 层；串行执行 27 个 API 路径/参数变体的本地认证 GET 抽样，主要每条两次。
- 会话仅在诊断进程内签发，不打印或保存凭据；没有运行选股、回测、AI 分析或分钟补拉写操作。未关闭 Redis、清空缓存、清数据库 buffer pool 或重启服务。
- 对三个函数做独立诊断进程 SQL 剖面：个股详情、回测十条交易页、单代码持仓市场上下文；对持仓历史查询做普通 `EXPLAIN`，并核实相关生产索引。无全表文本长度排序、无生产大范围 `EXPLAIN ANALYZE`。
- **没有调用 `/api/portfolio`**：其服务代码会在挂接旧 AI 建议时触发过期状态写入。本轮只调用纯 repository 的市场上下文读取，不把有潜在写副作用的 GET 当作只读探测。
- 没有启动 Chromium 或执行全量测试，避免在约 714MiB 可用内存、swap 已用约 1.2GiB 的主机上追加重负载。本轮无应用代码改动，无需用旧浏览器截图或旧测试数充当新验收。
- 本次耗时是**应用服务器经 loopback、复用连接、自然暖 DB/既有缓存**的少量完整 HTTP GET；不是请求者电脑、互联网 RTT、真正冷库、P95 或浏览器渲染耗时。字节是解压后的 JSON body，不是公网压缩传输量。
- 旧优化结果与约束见 [API_READ_OPTIMIZATION_2026-11-14.md](API_READ_OPTIMIZATION_2026-11-14.md)、[REDIS_DISPLAY_CACHE.md](REDIS_DISPLAY_CACHE.md)。前轮浏览器审计位于 workspace `output/browser-cache-audit-20261114.md`；本报告对其中持仓“策略→持仓串行网络请求”的判断做了纠正，见逐页表。

## 2. 逐页请求、查询与缓存地图

所有正式页面的 JS/CSS 有现有静态资源 HTTP 缓存；受保护业务 API 当前统一 `private, no-store`。`common.js:51` 的 `fetchJson` 没有业务数据 TTL/inflight 去重。带登录壳的页面另独立请求 `/api/auth/session`，不在业务初始化的 await 链上；认证结果不建议长期缓存。

| 页面 / 路径 | 首屏、翻页与刷新真实链路 | 后端范围 / 当前缓存 | 优先级与最小方向 |
| --- | --- | --- | --- |
| 首页 `/` | `/dashboard/summary?limit=8&compact=true` 与 `/dashboard/indices` 各自加载；分钟主图区复用 indices 内序列，切日线才拉 `/index-series`。没有每秒行情 GET，1s 定时器只更新本地时钟；供应商缓存正在刷新时有重试 | summary 固定 Redis 投影；指数 7 固定键；miss 才拼行情宽度/择时/主题/情绪/最近跟踪，多层聚合 | **高：拆首屏/证据。** 保留真实风险、指数、主线与近期跟踪等关键区，研究证据展开再取；不复制指数序列、不增加无关串行请求 |
| 选股中心 `/selection` | 风险 summary → 策略目录 → 最近20任务；保持已选策略时目录后还会拉 detail。初始不自动拉全结果；执行/选任务后才取有 limit 的 results。运行主要 SSE，失败才 polling；500ms 定时器只是本地进度展示 | 风险借用首页整包；策略数据检查复用 dataset Redis；results 默认3/上限200，不是全市场回传，但还附带完整策略详情 | **高：风险轻量投影、独立区域有限并发、结果compact。** 状态与结果分开；提交前/Worker仍用真实硬门，不能用缓存决定可执行性 |
| 跟踪复盘 `/tracking` | 首次 deep-review/status → filters → list；**每次翻页/刷新仍 filters → list**，初始10条，列表 compact/include_runs=false | SQL CTE先LIMIT/OFFSET本页ID；本页持有期日线+tracked分钟聚合。默认第一页Redis预热；任意页/筛选live；全范围汇总另有短缓存，miss加载全部统计范围 | **高：移除重复filters；列表与全范围统计解耦；精确分钟日极值读模型。** 不把全范围统计改成本页统计；不丢14日窗口、去重/排除与策略版本语义 |
| 持仓管理 `/portfolio` | 一个 `/api/portfolio`；`await loadStrategies()`只是本地HOLDING_STRATEGIES填选项，**没有多一次策略HTTP**。AI刷新异步提交与按任务状态轮询；无固定整页分钟轮询 | 全部active持仓，代码集合批量取6类市场上下文，批量AI/outcomes，已非市场N+1；每只最多120历史输出，但窗口函数先扫其全部日线。GET可能物化旧AI建议过期 | **低/规模触发：** 不先拆表。静态技术上下文按日线批次复用、动态行情/风控单独刷新；证据按需。持仓多时再服务端分页+独立总汇总；先去掉GET写副作用再考虑HTTP缓存 |
| 回测中心 `/backtest` | 策略 → factor-input/status → 20条compact runs → 默认run结果 → 十条交易页，最多5个业务串行往返。活跃任务每4s runs→results→trades，即使交易未变化也会重拉 | runs SELECT * 后丢summary/methodology；结果读现成daily summary画完整曲线；交易有DB COUNT+LIMIT/OFFSET，但为每页每条交易循环读5根价格/复权条，十条页13SQL | **中：** 状态轮询变轻、完成批次未变不重拉结果；结果和交易按所选run并发/分区渲染；N+1批量化或复用不可变结果读模型；深页/长曲线再按证据加cursor/分辨率 |
| 选股策略 `/strategies` | 目录 → detail → factor-evaluation → forward-evidence，detail先渲染但两类证据仍串行加载；换策略/手动刷新重拉 | dataset Redis只覆盖数据检查，不覆盖全部结果；detail读现成每日factor stats。factor-evaluation读评估表+group/ablation与manifest汇总；forward-evidence读整份协议观测/pick/action后汇总，再切最后20/50/100条 | **中：** spec/状态/研究证据分层，按需/有限并发。证据有增长风险：协议级累计汇总+明细DB分页，不是只在Python尾部切片；不能用截断明细重新计算全协议指标 |
| 交易策略 `/trade-strategies` | 一个 `/api/trade-strategies`，5条本地选择渲染；无轮询 | 小目录 SELECT *，含规则/参数，约3.8KB；无业务浏览器缓存 | **低：** 按规则版本/更新时间私有缓存即可，不值得建中间表/分页/分库 |
| ETF轮动 `/etf-rotation` | 一个 `/api/etf-rotation/latest`；展开目前仅CSS显示隐藏，候选证据与outcomes已提前下载；无每5分钟浏览器刷新 | 最新run指针+run/candidate/outcome现成表，非现场重算市场。latest每次附同一spec，候选/evidence/outcomes整包 | **中：** 首屏compact候选第一页，详情/forward outcomes按需，spec按hash缓存；复用已有run模型，不新建同义“今日ETF轮动表” |
| 数据状态 `/system` | 一个 `/api/system/status`，手动刷新；摘要与大量证据必须同包到齐才能渲染 | 固定Redis投影，后台30s刷新；miss构建live readiness、各latest/覆盖、最近43任务、DQ、生命周期等。任务payload含完整metadata | **高（载荷/渲染）：** 摘要/任务第一页/证据分层；计划配置版本缓存；DQ/metadata展开取。可复用operational_status展示投影，但不能替代实时readiness/提交守卫 |
| 个股 `/stocks/{code}` | detail 与 overview并发，却Promise.all等两者才一起渲染；每60s可见时整包重拉。分时不足会自动POST异步补全，再GET轮询，即使分时证据未展开；不应把补拉耗时算作轻量报价读取 | detail默认600日K/240分钟/12新闻+财务/资金/筹码/选股历史/完整分钟缓存；overview另读基础/报价/60日K/5精确排名/行业统计，已优化到7SQL但非整个页面。detail无payload Redis | **最高：** 报价优先、静动拆分；首屏只读可见日K窗口，扩范围/周月切换按需读，新闻/分时独立；命中板块先定位再hydrate；行业/因子上下文按源批次共享，热点详情预热，非热点有界回退 |
| 登录 `/login` | 静态HTML/CSS/JS；仅提交时POST/login，认证后跳转 | 页面private/no-store；静态资源可缓存；密码校验本来就应有成本 | **保留。** 不缓存密码/会话响应，不为提速降低认证成本，不做数据中间表 |

### 2.1 代码定位

- 页面请求链：`home.js:761,795,838,847`；`selection.js:10,1274,1320,1444,1514`；`tracking.js:450,456,735`；`portfolio.js:177,532,723`；`backtest.js:576,633,646,657,671`；`strategies.js:712,735`；`etf-rotation.js:156`；`system.js:1`；`stock-detail.js:536,1059,1195,1257`。
- 分页/汇总：`app/tracking/repository.py:118`、`app/api/routes/tracking.py:358`；`app/backtest/repository.py:127`；`app/api/routes/selection.py:392`。
- 板块过读：`app/api/routes/stocks.py:826` 的TOP40→hydrate→按code挑选；`app/data_ingestion/market_opinion_repository.py:303`。
- 研究证据增长：`app/stock_selection/forward_observation.py:993,1280,1450`；`app/stock_selection/factor_evaluation_v2.py:1696`。
- 系统证据：`app/api/routes/system.py:859,1253`；持仓范围/潜在GET写：`app/portfolio/repository.py:123`、`app/portfolio/service.py:918,1615`。
- HTTP缓存边界：`app/api/auth.py:414,478`；页面资源/HTML：`app/api/routes/web.py:14`；浏览器共享fetch：`app/api/web/js/common.js:51`。

## 3. 本次少量运行证据

### 3.1 完整本地HTTP抽样

每个表内路径两次，第一次仅指**本轮首次访问**，不等于空缓存/冷库。所有返回200，不使用401作为延迟样本；全部仍是 `private, no-store`。

| API / 条件 | 两次完整GET ms | 解压JSON字节 | 说明 |
| --- | ---: | ---: | --- |
| dashboard summary compact8 | 4.19 / 3.58 | 83,764 | 已预热，仍每次传完整summary |
| dashboard indices | 3.63 / 3.25 | 39,444 | 三指数与真实分钟序列 |
| tracking filters default | 2.41 / 2.51 | 14,634 | 59日期/4策略/72runs，本页面只用前两项 |
| tracking 第1页10条 | 3.39 / 3.28 | 20,946 | 默认固定投影命中 |
| tracking 第2页10条 | 118.80 / 124.19 | 21,099 | 非默认整页，仍聚合持有期分钟 |
| selection runs20 | 9.78 / 8.08 | 7,763 | 当前返回5任务，不是20条全市场结果 |
| strategies目录 | 29.25 / 9.96 | 10,224 | 三策略；运行状态不能当不可变spec |
| detail v0.4.4 | 45.89 / 21.10 | 10,114 | 后续v0.5.1/v0.6.0约17～19ms |
| factor-evaluation v0.4.4/5d | 116.34 / 18.67 | 22,853 | 一次首次探测较慢，不能报为稳定116ms瓶颈 |
| forward-evidence v0.4.4 | 19.22 / 14.58 | 5,868 | 当前样本小，长期全协议读需受控 |
| factor-input/status | 19.29 / 18.02 | 704 | 最新因子日期覆盖统计 |
| backtest runs20 compact | 7.61 / 6.66 | 31,938 | 20任务；DTO仍有较多诊断/请求字段 |
| backtest 最新成功run results | 6.03 / 5.33 | 3,537 | 本次仅4曲线点，不能推断长区间同样小 |
| backtest 同run trades第1页10条 | 17.41 / 17.62 | 24,627 | 有N+1，但当前暖读不慢 |
| trade-strategies | 4.38 / 4.17 | 3,844 | 5条规则目录 |
| etf-rotation latest | 9.54 / 9.63 | 46,214 | 已物化；item约37,736B/spec约8,461B |
| system status | 9.25 / 7.53 | 152,336 | 后续另一次取组件大小时cache年龄字段略有变化 |
| stock sh.600519 detail | 73.26 / 71.63 | 76,346 | 600日K、111实时采样分钟、选股历史/新闻等 |
| 同股overview | 46.01 / 45.28 | 1,490 | 仅概览，不是整个页面 |

`system/status` 各组件重新紧凑JSON序列化：`task_runs` 107,104B、`data_quality` 25,092B、`scheduled_tasks` 5,353B、`readiness` 4,436B。组件字节之和与原始HTTP长度会因外层键/序列化格式稍有差异，不用它冒充线上压缩字节。

`dashboard/summary` 同法：`market_timing` 65,908B，`market_overview` 5,421B，`emotion_board` 6,174B，`hot_themes` 4,484B，最近跟踪预览1,153B。因此选股页为了一个risk_alert拿整份summary明显过宽。

### 3.2 函数与SQL剖面（不含HTTP）

- **个股详情 sh.600519**：约69.88ms，15SQL，SQL执行合计约46.2ms。其中板块hydrate读取480条关联股、694条关联新闻；新闻查询约17.9ms、关联股约6.5ms。最后只显示命中板块最多5条新闻。范围被TOP40限制，并不是无界全表，但仍比当前股票需要的结果宽很多。
  - 生产已存在 `sector_opinion_stock(code,snapshot_id)` 与 `(snapshot_id,code)`，`sector_opinion_news_ref(snapshot_id,scope_type,stock_code)`。优先利用现有关联表先选命中板块及该板块sector新闻，不必为这个问题先加新表/新索引。需保持“最新批次TOP40中按sector_score首个命中”的选择顺序，以及旧payload JSON兼容回退；不能简单改为另一板块全集中的最高分。
- **回测十条交易页**：约15.18ms，13SQL，执行合计13.73ms；10次为每个交易取5日价格/复权。批量化应以页内精确 `(code,entry_date)` 集合为范围，保留每条样本的5个交易日、复权缺失规则，不扫描全市场窗口来代替N个小查询。
- **单代码持仓市场上下文（非持仓接口）**：约11.07ms，6SQL；日线窗口输出120条约4.45ms。普通EXPLAIN显示由`uniq_code_date`按代码读、估计689行，再derived窗口过滤/排序，并非只读120行。它是可改进范围，不是当前首要慢点；未用虚构大持仓负载宣称大收益。
- **复盘分钟**：没有再跑重型冷读或EXPLAIN ANALYZE。前轮已证明第二页约9万有效分钟、现有覆盖索引自动覆盖读取。本轮生产SHOW INDEX再次确认`idx_tracked_code_minute_price(code,quote_minute,latest_price)`仍存在；剩下的工作量不是再给相同查询堆一个近义索引。

## 4. 中间表 / 分表 / 缓存如何选

### 4.1 先复用现有，不重复造表

| 现成能力 | 当前接入情况 | 适用方案 / 不可直接代替 |
| --- | --- | --- |
| `stock_realtime_snapshot` 一代码一最新行 | 报价接口已使用 | 保留实时读取，不复制一套同义“今日行情表” |
| `stock_realtime_rank_snapshot` 每批每类TOP100 | 全仓API搜索只发现writer/schema，未发现业务reader；生产表确实存在，信息模式约10万历史行，**不是只有当天400行** | 先接入最新完整snapshot的热点列表/预热集合，按snapshot_id+rank_type+rank_no有界读取。不要误读全部历史批次；不能用TOP100伪造任意股票精确排名 |
| `sector_opinion_stock/news_ref` 已规范化 | 个股仍先hydrate整个TOP40 | 先按code命中snapshot再读必要新闻；无需新增关系中间表 |
| `tracking_summary_daily` | 现有writer，未见页面reader；生产估计45行 | 它是1/3/5/20日分版本研究绩效，不是当前filtered_summary，不能直接替换 |
| `operational_status_snapshot` | 已有5分钟任务写入，未见页面reader；生产信息模式约2.9万历史行 | 适合轻量展示组件，不拿5分钟前worker/队列状态替代live readiness；仍只查一批/一组件最新记录 |
| `etf_rotation_signal_run/candidate/forward_outcome` | latest已经读它们 | 拆compact/detail/outcomes与spec即可；已有不可变策略结果不是实时ETF行情，不每5分钟重算/覆盖历史run |
| 原始分钟/5m与15m rollup/tracked分钟分层与日期分区 | 已有生产生命周期 | 保留现有体系。分区主要有利于日期裁剪/保留清理，当前复盘按代码跨多日仍要算很多行；追加分片不能替代预聚合 |

表行数是information_schema估计，不是本轮精确COUNT。不能由这些数推导具体磁盘扩容或承诺物化收益。

### 4.2 真正值得新增/扩展的读模型

下面是**方案，不是已建表合同**，字段与索引需在实施前结合最终SQL确定。

1. **tracked分钟精确日极值**（优先的持久化中间层）
   - 粒度：`code × trade_date × source_scope/算法版本`，记录有效价格min/max、有效分钟数、首末分钟、源revision/hash、完整性/修订状态、生成时间。
   - 已结束日可按已验证源版本稳定读取；当天可按行情批次增量更新，或单独读取当天原始分钟，不能把盘中变化统一冻5分钟。迟到/更正/历史补拉需重建受影响日，不能只做append或永不回头的MAX/MIN。
   - 复盘仍把日线high/low与tracked分钟极值按**同一天**合并，再跨持有期聚合并COUNT DISTINCT日期。保留无正价/NULL排除、选择日之后范围、当前报价合并、缺源行为；日线不能假装分钟完整源。
   - 预期把“一代码×若干万分钟”降为“一代码×若干交易日”读取；收益需同条件EXPLAIN、源行数、完整JSON等价证明，暂不写毫秒承诺。
2. **复盘全范围统计投影**（与列表解耦，不无限复制每种filter）
   - 优先基于选股business key/有效记录ID和上述精确日模型形成可聚合薄层；策略、日期、run过滤走索引，保留统计窗口及同股/同日/同策略去重规则。
   - 列表先返回第一页；全范围统计独立按需或后台刷新，明确as_of/refreshing，未构建显示“统计计算中”，不可展示本页值冒充总统计。排除/删除/纳入复盘、源行情/日线/分钟修订和自然日跨界均要失效。
   - 只预热有上限的公共组合，不为每个任意参数生成永久表/无限Redis键。若暂不需要持久化全维度汇总，可先用独立汇总API+有界短缓存。
3. **共享慢变股票上下文 / 精确排名批次**（条件扩展）
   - 基本资料/财务、因子、技术历史和行业均值按各自source revision/date共享；不为每次打开一股重新算同一行业。先拆接口/按需与现有索引优化，再评估是否需要持久化快照。
   - 5分钟热点compact详情可由现有rank集合驱动预热；不在热点的代码走原有有界查询，并保留完整搜索能力。
   - 若要全市场精确rank共享，需完整人口同批次计算，保存样本分母、NULL/并列RANK、factor_date和quote批次。**目标股票自己的最新因子日期**合同仍不变；某股落后日期不能被“全市场最新日”偷换。混合批次/缺覆盖应回退或明确标记，不将热榜名次冒充精确百分位。
4. **协议级前向证据累计摘要**（增长后按需）
   - 现有factor evaluation本来就有结果表，不重算研究。forward-evidence则可在观测/outcome/action提交后更新版本化协议摘要，同时为recent observations/picks/actions做数据库分页。
   - 完整协议指标与分页明细分开；保留零候选、失败天数、成熟分母、复权/benchmark、污染版本隔离及人工动作去重。不能只拿最后50条样本重算累计胜率。

### 4.3 不推荐的起手式

- 每天建`hot_stock_YYYYMMDD`物理表、用动态表名查询，或以只存热股为由删除原始历史。固定表+日期/批次+必要日期分区即可，原始研究证据保留。
- 先分库分片、复制全量行情/ETF/因子大表，或给每个页面加一份同义表。当前最大问题是请求范围、过读与聚合粒度，不是已证明单库容量或连接成为瓶颈。
- 把最新、历史、来源失败、合法空结果都压成一个“今日缓存”；把Redis/浏览器命中当数据就绪。
- 把所有API缓存五分钟，特别是登录/认证、任务状态、风险、当前报价、提交前检查和私有持仓。

## 5. 分层缓存与新鲜度合同

| 数据类 | 建议读取/更新方式 | 必须失效或拒绝复用的条件 |
| --- | --- | --- |
| 不可变策略/ETF spec、交易规则、目录静态部分 | 内容hash/显式版本缓存；私有浏览器fresh TTL或客户端内存/session缓存+inflight合并 | 版本/hash/应用构建变化；不能把runtime_ready放进长缓存spec |
| 基本资料、财务、日K、技术/行业上下文 | 按基础资料revision、公告/报表版本、日线/复权批次取；日K按可见窗口+游标/日期范围加载 | 日线修订/补拉、除权复权变更、公告版本或身份/ST状态变化；旧窗口不得被称实时 |
| 热点集合/榜单 | 复用最新完整5分钟snapshot，小型列表与热点预热 | 交易日期/批次/覆盖/硬TTL；休市显示真实最后交易日与生成时间，不能改写为“今日实时” |
| 当前价格/资金、市场风险、运行就绪 | 独立动态小payload；遵守现有报价与风险时钟；选股提交/worker真实重验 | 来源过期、批次混用、缺数据、日历硬门异常；不沿用5分钟热点TTL |
| 任务进度/队列/worker | 状态小payload、有限轮询/SSE；结果仅revision变化后拉 | 完成/失败/取消即时更新；不重发不变曲线/新闻/方法论 |
| 系统计划/DQ/元数据 | 计划按配置版本缓存；DQ按审计run_id；任务详情按run/revision按需 | 新审计/新任务/错误状态变化；summary必须仍暴露红灯，不能因懒加载隐藏错误 |
| 私有持仓/人工动作 | 默认内存级、按当前认证身份隔离；变更后精准失效 | 写入/退出/401/会话epoch变化清除；不跨用户、不同登录会话持久复用 |

HTTP缓存要在认证middleware的明确白名单落实，不能只在路由加header然后被全局no-store覆盖。`ETag/304`主要省body，仍有网络RTT；fresh cache/inflight复用才减少请求。动态风险等继续no-store。静态资源应有可靠构建版本/内容hash，避免旧JS与新API不兼容。

## 6. 建议实施批次（本轮未执行）

### 第一批：首屏瘦身与去重复，不新增数据库表

1. 复盘filters同页面复用，翻页不重拉；去除其未用available_runs，AI可用状态不阻塞主列表；写操作或交易日期/版本变化后刷新filters。
2. 选股风险独立轻量读取，主列表/任务/策略展示有限并发、分别容错；静态spec与runtime状态拆开；不能为了并发绕开筛选真实依赖。
3. 个股报价优先显示，detail拆成行情/可见日K/慢变上下文/新闻/选股历史/分时；自动刷新仅动态部分，避免并发请求重入和每次重绑图表事件；变更区间再拉历史。不只是把history_limit改小而丢掉原有历史浏览能力。
4. 个股舆情先按现有关系索引找到原排序范围内的命中板块，再加载需展示的sector新闻；不动舆情选股输入或冻结研究逻辑。
5. 首页与system的可见摘要/证据分层，ETF spec缓存+compact候选/详情分离。保留11页布局基准、红灯和合法空结果，不把“简化查询”变成删功能。

### 第二批：精确分钟日级中间层与全范围统计解耦

1. 冻结旧SQL/返回样本及源语义；明确raw tracked与daily合并合同、跨日/迟到/修订、覆盖与回退。
2. 在预置smoke库验证新模型、幂等/并发发布/更新失败，再做限速小批历史构建；不直接生产全库重算。
3. 先影子比较，再切复盘非默认页读取；列表与filtered summary独立，新增可观测revision/as_of/覆盖信息。
4. 评估读行数与额外写放大/存储/后台周期，不能让小主机因预计算变成持续满负载。既有Redis继续作为最外层可丢弃展示缓存。

### 第三批：按实测规模再扩展

- 回测N+1批量化、状态轮询只读变化、runs真正列投影；长曲线按分辨率读取但保留原始点查询/导出，不改变收益/回撤计算。
- 热股共享上下文/精确排名批次；热点ETF行情若新增，复用行情snapshot链路，和冻结ETF策略run严格分开。
- 策略前向证据累计摘要/分页；持仓数量或CPU剖面证明必要时再分页/缓存技术上下文。
- 小型交易规则目录和登录不做表结构改造。

## 7. 实施验收门槛

以下是之后每批要交付的证据，**不是本轮已经达到的性能承诺**：

- **同条件可比**：固定代码SHA、源批次/交易日/时钟与查询参数，分别记录应用本地、公网、实际请求者浏览器；首次进程/空展示缓存、自然暖读、页面返回、翻页分开，不把服务器8ms冒充用户端8ms。
- **用户侧分段**：Resource Timing记录DNS/TCP/TLS/TTFB/下载、API数量/关键串行深度、压缩transferSize与解压body、JSON解析/首个关键区渲染及全部初始GET完成；记录设备/网络/缓存状态。至少同设备重复若干轮并保留原样本，样本不足不报P95。
- **范围确实变小**：分页DB LIMIT在昂贵join/聚合前生效；深页、空页、日期/策略/run筛选验证；SQL条数、实际/估计读取行数、返回body/列、预热时间与存储写放大可核对。只减少payload而SQL仍过读不能算查询完成优化。
- **结果等价**：固定时钟比较旧/新完整字段；日线+分钟max/min、天数、NULL/零/负价、停牌/缺交易日、跨版本/去重/排除统计、非热点、历史修订/迟到、跨日、来源失败/合法空结果均覆盖。分页不能改变总汇总分母。
- **加载正确**：首屏不再取隐藏证据/600根不可见日K；翻页不再请求不变filters；自动刷新不重拉慢变spec/财务/历史；部分证据失败不抹掉已成功的行情/主表，但必须在相应区显示错误/陈旧状态。
- **缓存正确**：身份/会话/构建隔离、401/logout清理、mutation/revision失效、硬TTL、源日期保留；Redis/读模型失败有界回退或明确pending，不无限旧数据续命。
- **上线**：新DDL按最小权限合同、smoke、备份/容量、在线约束与短锁等待验证；既有冻结研究指纹/策略/自动选股保持不变，回归与五服务/readiness/队列/静态资源版本核对。每批单独可回滚，不为性能改研究历史。

最终优先顺序：**第一批先让页面少等、少传、少重复；第二批再让复盘少读几个数量级的分钟；第三批按观察到的规模消除剩余增长风险。** 本轮没有证据支持直接分库分片或每日新物理表。
