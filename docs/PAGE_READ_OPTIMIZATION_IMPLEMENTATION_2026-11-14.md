# 全页面读取优化实施记录 · 2026-11-14

## 状态与范围

**第一批五项已实施并上线；第二、三批仍未完成，不是全页面方案全部完成。** 固定旧代码 SHA：`04a835cfbdeb132ca670f28864a842b53759711e`。实现 `2cb7778`，可见日期窗口兼容修正 `56b49f6`，隔离测试环境补齐 `901b2f1`，独立区域一致性及资源版本修正 `d9d0d5b`。生产路由代码于首提交只重启 API 加载；后续仅浏览器 JS/测试/版本变化，不重启四 Worker。

执行边界来自 [全页面审计](PAGE_READ_PERFORMANCE_AUDIT_2026-11-14.md) 第一批前四项：跟踪 filters、选股风险与初始化、个股分区读取与历史窗口、舆情匹配范围。没有 DDL、数据清理、研究重算或交易/自动选股改动。冻结研究源码/依赖双指纹与基线相同，migration 保持 39/39。

## 已实施

### 跟踪复盘

- `filters?include_runs=false` 保持旧默认响应兼容，页面不再下载无用 runs；本次默认响应 **14,634 → 1,340B**。
- 同 tab 内存缓存 60 秒，按 instrument/strategy/上海自然日区分；并发去重。60 秒内翻页仅请求列表，不再 filters→list。TTL 到期仍会重新验证，不能说“永不再请求”。手动刷新、写操作前后清除；跨自然日键不复用。
- AI 可用性探测不再阻塞主列表。列表本身的全范围统计/分钟聚合尚未改变，不能把去掉一次网络请求说成已解决所有分页慢查询。

### 选股中心

- 新 `/api/dashboard/risk-alert` 复用首页同一市场风险结果；正常命中同一有效投影。冷 miss 只读原 `_dashboard_market_overview()`，不为一个警报构建择时、情绪、板块和跟踪整包。
- 同库风险对象一致，解压 JSON **83,764 → 1,127B**；动态风险仍 `private, no-store`。
- 风险、策略目录、最近任务独立启动/容错；不改变提交硬门、Worker 重验、强提醒非阻断语义或原始选股参数。

### 个股详情

- 旧 `/api/stocks/{code}` 默认 full 保持兼容；新增 `section=context/dynamic/news/intraday`，在 SQL 执行前分区，不是取全量后丢字段。
- 初始 context、35 根日 K、新闻、overview 独立加载；报价不再等 overview 排名和新闻/图表齐备。初始请求数从两个宽接口变为四个小接口，**不声称请求数全面减少**。动态刷新通常只有 `dynamic + overview` 两次请求。
- `/history` 只返回可见近月所需 35 根 OHLC；返回原 600 个交易日浏览边界，扩日期时在 SQL 绑定 start/end 和 LIMIT，确证可按需取回完整 600 根。日/周/月切换保留，不偷减历史能力。600 日边界仍需读取/聚合最多 600 条日期元数据；不是完全零元数据扫描。
- 保留旧默认月窗口算法，视觉复核发现时区边界相差一天后已修正，避免隐式改变默认可见 K 线。图表事件只绑定一次；动态刷新不重置用户区间或重绑事件。
- 60 秒定时器正常只重拉报价/实时资金与 overview；历史 source revision 改变才重读，并保留用户扩展区间。静态 context、新闻最多 5 分钟复用，源修订/跟踪变化可提前刷新；不是永久冻结。缓存的筹码成本与当前报价本地重新合成价格偏离，避免把旧价格派生值冒充实时。
- context、新闻彼此失败不抹掉已成功的行情/图表；新闻空态在上下文晚到时重渲染，避免并发顺序改变原研究解释。
- 隐藏分时不再 GET 或自动 POST 补拉；只有展开才加载/按原异步合同补全，单飞防重入，失败显示缓存/实时尾段及失败标记。没有用日线伪造分钟数据。
- 新浏览器缓存仅是**当前 tab 的内存缓存**，不做 localStorage/sessionStorage 持久私有数据缓存，也不意味着关闭页面后可离线复用。白名单只允许 filters、个股 context/news/history；报价、风险、认证、任务、持仓默认不缓存。最多 64 项；身份 cookie 变化、401、logout、写操作前后和 BFCache 恢复清理。失效前的在途响应不能重新发布旧数据；错误不缓存。构建更新重载 JS 清空内存，所有受影响资源版本已提升。

### 个股舆情范围

- 保持“最新批次 theme TOP40 按 sector_score 排序，第一个包含当前代码的板块”的原语义。
- 用现有 `(code,snapshot_id)` 索引先找命中，再 hydrate 唯一被选板块；旧 JSON payload 仍走原解析路径。
- sh.600519 同条件剖面：关联股返回 **480→30** 行，新闻引用 **694→55** 行，来源 **176→12** 行。额外一条 code 匹配 SQL，所以 full SQL 数 **14→15**，不是 SQL 条数下降；减少的是每条 SQL/JSON 的范围。未修改冻结选股输入或历史研究内容。

## 验证与证据

本机证据目录：`/root/.openclaw/workspace/output/page-read-batch1-20261114/`。凭据只在进程内签发，不存报告/截图发布、不记录 Cookie。全程单工人、串行命令、单 Chromium，未清 Redis/buffer pool。

- `comparison.json` / `compare.py`：从固定旧 SHA 加载旧模块，与当前同库对比 sh.600519、sz.000725、bj.920001、sh.512480。四份 full JSON 排序字节/长度/SHA 相同；各分区去除新增 revision 后逐字段相同；35 根与旧末 35 根一致，600 根全窗口一致。非热点、无因子/无历史样本均未丢查询入口。
- `sql-profile.json`：真实 SQL 和返回行数；context 8 SQL、dynamic 5 SQL、news 9 SQL（该样本）；历史普通 EXPLAIN 保留。拆分会增加首屏若干小查询，未宣称首屏总 SQL 一定下降。没有用返回行数假冒 rows examined。
- `unittest.log`：**735 项执行，734 通过、1 个既有 skip**。新增隔离合同覆盖缓存单飞、TTL、身份、revision、失败、写期旧结果拒绝、风险缓存 miss、范围绑定/排序、动态不读大包、初始可见窗口及缓存筹码与新报价合成。JS 四文件语法检查通过。旧风险 URL/cachebuster 的断言随真实新合同更新。
- `browser-before/after.json`、三页桌面截图：同 1672×941、单 Chromium 经真实公网。选股、复盘、个股全部初始 GET 200、没有未捕获 JS 异常或整页横向溢出。复盘第二页只有列表 GET；个股日期扩展取到 600 根、周 K 可切，刷新只有 dynamic/overview，仍保留已扩区间。
- 旧浏览器样本拦截一个隐藏分时 POST（409），因此旧补拉工作流不算完整通过，也不把其失败后的耗时当完整分钟数据就绪。新版初始没有该 POST；本轮未实际执行供应商补拉写任务。
- 三页首轮原始样本含 HTML/资源冷暖/启动差异，stock/tracking **未证实稳定整页总耗时下降**；不把单轮波动、函数毫秒数或服务器浏览器冒充请求者电脑/P95。可确认收益是读取/响应范围缩小、串行依赖解除与重复请求消失。仍需请求者端同设备分段采样。
- `deployment.json` / `readiness-after.json`：本地/公网 health/readiness 200，实际 `ready/accepting_jobs=true`，四队列 queued/running=0，五服务 active/NRestarts=0。匿名新接口与 readiness 401；非法 section、日期、过大 limit 422。三 HTML/四 JS 本地、公网、磁盘 SHA 完全一致，API仍 private/no-store。未调用有过期写副作用的持仓 GET。

## 尚未完成的后续批次

1. 第一批第五项已实施，见本文后附章节；静态ETF spec按hash缓存，小目录runtime能力不盲目长缓存。
2. 第二批已实施，见本文后附章节：逐日源revision/完整性合同、tracked分钟日极值、列表与全范围统计解耦、影子等价与限速回填。
3. 第三批：回测 N+1 批量化/轻状态轮询/投影、研究证据数据库分页与全协议累计指标分离、热点快照复用；持仓规模触发优化。
4. 真正请求者设备首屏/返回/翻页分段验收。

不新建每日物理表、不删历史，不用 TOP100 热榜替代任意股票全市场排名。后续不能把本页状态误读为全 11 页优化完成。

## 回滚

无新增表或业务写入。首轮前 `rollback-files.tar` 保存旧基线对应文件。优先反向撤销本轮应用提交并发布新的静态资源版本、重启 API，保留后续无关更改，不对生产工作树直接 reset。内存缓存可丢弃；默认旧 full 接口仍在，回滚前端即可回到宽读取。不能把存档恢复覆盖当作无需冲突检查的通用命令。

## 第一批第五项：首页、系统、ETF 分层（已上线）

基线 `b6ff9ff`；实现 `d80f11a`，可见主线兼容修正 `a39e691`。只重启 API 加载 Python display/read 路径，没有重启 Worker。无 DDL，保持 39/39；策略阈值、冻结研究源码/依赖指纹和既有不可变 ETF run 未改变。

### 范围与结果

| 页面/API | 原解压 JSON | 新首屏解压 JSON | 实际变化 |
| --- | ---: | ---: | --- |
| 首页 compact8 | 83,764B | **19,248B** | 保留风险、行情宽度、资金板块、近期跟踪、**可见市场主线**；隐藏择时/情景与热点/情绪按原 disclosure 展开分别读取 |
| system status | 152,337B | **37,673B** | 保留全部 43 张任务卡、最新状态、readiness、原可见 DQ 告警/追溯；不传页面不用的任务 metadata 和通过规则明细 |
| ETF latest | 46,214B | **11,502B** | 保留全部 14 候选、周期、条件、综合分与结论；不带完整 spec、源血缘、隐藏评分列和整段证据 |

这是解压 body，不是公网压缩传输字节，也不是三个页面总传输量。旧默认 full API 均保持兼容，没有通过截断可见任务卡/候选条数来获得上述缩减。

- **首页**：`section=summary/timing/emotion`，默认 full 不变。有效固定 dashboard Redis 投影命中时从同一快照投影；miss 的 summary 不构建隐藏 timing/hot-themes/emotion，独立读取可见主线。主线轻读只取 forecast 日期/spec 元数据与原排序最多24行必要 leadership 字段，仍先对24行按原规则算市场主线，再显示原16行；绝不先截16行再决定主线。隐藏 forecast JSON 不读取。原显式 `none`、父子去重、资格与顺序不变。原 disclosure 打开请求对应分区，同页面60秒/上海日期复用、单飞，失败重开可重试。
- **修正记录**：首次拆分误把可见主线与隐藏情景的共享 render 一起延后。公网截图发现“加载中”后，已立即把可见主线独立保留在首屏并修复，新增合同与旧新 HTML 等价证据。`7,150B` 是这次修正前的中间值，**不能作为最终收益**。最终截图与旧首屏同视口逐区核对；主线正常显示。
- **system**：前端用 `compact=true`，保留43卡不强加分页。缓存 hit 缩小展示投影；miss 在 SQL 用 JSON 字段投影读取卡片指标，DQ/舆情两行仍保留用于推导原可见摘要的数据。DQ仍显示原非pass规则及优先5条 actionable 样本，不把提示/失败过滤掉；原全量检查计数/证据日期保留。手动刷新单飞，失败不留下永久 busy。此处没有虚称为任务历史分页；本页面原来只展示每类任务最近一行，不是所有历史。
- **ETF**：新只读 display 模块复用现有 run/candidate/outcome 表，latest 按当前 model/version 固定一批，candidate 先 `WHERE run_id` 再读必要列；周期只读 `evidence_json.sector_cycle_label`，不先 hydrate 整段 evidence。14候选本来全可见，因此仍全部展示。勾选“评分与明细”才取同 run 的评分列；完整固定口径在原 disclosure 展开时按 spec hash 加载，tab内存缓存，hash不符409提示重载，不把不同版本混用。前向观察进入视口附近再读同run结果（不读metadata）；本次1672×941视口该区域靠近首屏，故触发额外一个 outcomes GET，**不是所有视口都只发一个初始业务请求**。合法零候选/无成熟观察仍是真实空态，不补0收益。
- **目录缓存边界**：本批仅新增不可变 ETF spec 的 hash 白名单，沿用 common 的身份/401/logout/写入/BFCache清理与最多64项tab内存缓存，不增加私有 localStorage/sessionStorage。小型交易目录仅约3.8KB、页面单次请求，动态选股目录混有runtime能力/可执行状态，未为凑缓存覆盖把它们长期缓存；没有新增同义表或扩大动态风险/readiness缓存。

### 证据与验证

证据目录 `/root/.openclaw/workspace/output/page-read-batch1b-20261114/`。

- `comparison.json` / `compare.py`：固定基线用 `git show b6ff9ff:<path>` 加载，不移动HEAD。首页三个原分区重新合并（剔除新增独立主线投影）与旧 compact JSON 排序 SHA 相同；模拟cache miss summary也与原可见投影一致。ETF旧默认full的旧新SHA一致，14候选各可见字段、勾选评分列、前向结果与旧run对应字段相同。未伪造空缓存/冷库测量，未清Redis或buffer pool。
- `render-equality.json`、`leadership-render-equality.json`：真实43任务卡/DQ HTML及可见主线 HTML/文案/title，与固定基线逐字相同。SQL轻读的mainline仍保留原data_quality语义，不能把缺失/NULL偷换为False或True。
- `sql-profile.json`：系统最近任务同为43行，返回JSON度量约124,020→62,135B；可见主线的3条forecast行约8,631→456B，24条leadership行约160,022→85,726B。ETF compact仅两条SQL，分别1run/14candidate，候选普通EXPLAIN有界按run索引。以上是诊断序列化的**返回行/字节**，不是rows examined或磁盘IO；任务、DQ及主线冷路径仍有必要聚合/元数据读取，未声称全部SQL消失。
- `unittest.log`：**743项执行，742通过，1个既有skip**；新合同覆盖隐藏builder不执行、可见主线保留/缺字段资格、SQL列和run范围、43卡与告警不丢、spec hash错配。四JS语法通过；旧资源版本固定断言已随真实版本更新。
- `browser-before/after.json`、三页before/after截图：单Chromium、真实公网、同1672×941；初始GET全200、无未捕获JS错误、无整页横向溢出。初始首页没有timing/emotion GET且可见主线已完成；展开两个disclosure各出现对应请求。ETF勾选评分/完整口径各按需GET，14行不丢；前向观察使用独立run端点。所有非GET/HEAD均在发送前拦截，本轮没有写请求/供应商任务。
- 本轮浏览器原始样本混有启动/静态资源冷暖，**不能宣称三页整页总时长稳定下降**：首页有减少，system有波动；ETF增加独立outcomes往返，该视口全部初始GET可能晚于原整包。可确证的是首屏传输范围、隐藏证据读取与冷读投影缩减。请求者电脑/P95仍未测。
- `deployment.json` / `readiness-after.json`：本地/公网health/readiness200且真实ready/accepting_jobs=true；39/39、四队列queued/running0、五服务active/NRestarts0。匿名新路径401、非法section422、spec hash错配409；三HTML/四JS磁盘/本地/公网SHA一致。源码/依赖双研究指纹与基线一致。未探测有过期状态写副作用的持仓GET。

### 后续与回滚

第一批五项均已完成（小目录可执行状态不做盲目缓存）；**第二批复盘日极值读模型/全范围统计解耦、第三批回测/研究分页/热点复用，及请求者端验收仍未完成**。

本批无新表或研究写入。`rollback-files.tar`保留基线文件；如需撤销，按依赖反向撤销本批应用提交、发布新静态版本并仅重启API，保留之后无关修改，不直接reset生产工作树。新完整默认接口仍可用；不要恢复首次主线缺失的中间版本。


## 第二批：复盘精确日极值与全范围统计解耦（已上线）

基线 `3d90743`，主实现 `4b81a91`，失败源保留保护 `b852257`，空筛选跳过价格读取 `ea81424`。migration **0040 / 40/40** 新增两张固定表，不按日新建物理表，不删除或改写任何原始分钟、日线、选股或研究历史。只重启 API；实际 tracked writer 是每天15:20既有 lifecycle Cron，每次从磁盘载入新代码，无需重启四 Worker。

### 读取及发布合同

- `tracking_minute_daily` 粒度 **code×trade_date**，保存原tracked正价分钟的精确min/max、有效数、首末分钟；`tracking_minute_daily_manifest` 保存整日source_revision、scope `tracked_positive_price_v1`、源/有效行数、异常日期数、生成时间与发布状态。
- **不是用MAX时间/COUNT猜修订**：先锁manifest日行，copy/upsert tracked源、删除重建该日极值和发布新的UUID写代token全部在同一事务。旧分钟同时间、同COUNT改价，删除掉原最高/最低价，均可收缩极值；发布错误整笔回滚，不留下新源配旧聚合。重复重建值/行数幂等，发布token会更新（不是内容hash、不声称源没变会跳过）。
- 唯一已发现业务writer为`copy_tracked_trade_date`；新日期/迟到/原分钟修订由同一原writer自动维护。显式历史修复用 `PYTHONPATH=. .venv/bin/python scripts/rebuild_tracking_minute_daily.py --start YYYY-MM-DD --end YYYY-MM-DD --apply`，默认仅dry-run，每次最多181自然日、日间150ms限速。任何未来新增writer/手工直接SQL必须遵守同一manifest锁和发布合同；不宣称能自动侦测绕过合同的任意外部SQL。
- 分区DROP无法与InnoDB写事务原子提交，故retention**先提交retiring墓碑并移除日聚合**，再执行原删除。DDL失败仍回原源读取；retiring禁止并发重建“复活”旧聚合。日模型发布失败导致tracked copy回滚时，本轮retention额外保护该日期的全市场原始分钟以供重试，不因5m/15m已成功而先清掉源。
- 原数据库实际是 **READ-COMMITTED**。manifest日期列表与后续日表/raw混合读取使用**单次事务REPEATABLE READ**，不污染连接的后续事务，避免在两条SQL之间源提交产生缺失/混批。相同SQL快照中manifest与日行source_revision必须一致。
- ready日期改读日极值；未建/不支持/retiring日期保持原raw SQL，缺migration/table也回原查询。若quote_minute日期与trade_date不一致，该日不标ready、整体走原SQL以保留原时间下界语义。当前模型只声明`observed_only`，不冒充完整交易时段；generated_at仅生成时间，不冒充报价时钟。
- 日线high/low仍按原有效价规则读取，日线和分钟按**同日**合并、跨持有期取极值、COUNT DISTINCT日期。入选日之后、当前实时报价合并、14自然日统计及同股/同日/同策略去重、排除/删除语义完全保留。ETF/合法空结果不造行情。

### 列表、全范围统计及缓存

- 页面列表使用`include_summary=false`，只读本页及总条数；原接口默认完整行为不变。新`/api/tracking/summary`单独返回原筛选范围的filtered_summary与策略分组，**不是本页统计**。统计未到时卡片明确“统计计算中”，失败显式提示且表格继续保留，可刷新重试；迟到的旧filter响应和身份变化不能覆盖新状态。
- summary仍按窗口内**全部有效样本**计算；缓存miss时仍组装这些完整样本，不声称已经建立全维度持久化统计表。收益是由日模型减少分钟输入、专用API不阻塞列表、翻页不重算/重拉不变全范围统计。后续若样本增长，仍需按证据评估薄投影/累计物化。
- 服务端summary短缓存60秒，以filters、market/tracking revision、上海自然日和写generation隔离。构建期间revision变更不发布到当前key。客户端仅当前页内存、身份+同scope+server revision、固定60秒到期不因翻页无限续命；BFCache恢复清理，写操作后server generation失效。无localStorage/sessionStorage私有缓存。
- 空筛选总数为0时直接返回真实空态，不为0条结果额外规划77个日期的价格聚合。

### 生产构建与可核对收益

证据目录：`/root/.openclaw/workspace/output/page-read-batch2-20261114/`。

- 今晨全备重新完整解压/哈希验证：1,393,579,539压缩B、8,740,956,751解压B、115建表/8,460数据语句、一个完成标记；SHA-256 `67d0da942de4901b479c5cbefb96e6c438018a6bf4ee96f811a2c60c38f14ee9`，与已保存manifest一致。随后smoke与生产0040幂等通过，生产两表建表16ms，无大表ALTER。
- 从现存tracked分区/源日期枚举77日（7月16日～11月13日），串行限速构建：原源 **1,851,385** 行，其中有效正价 **1,767,162** 行，得到 **11,569** 个日行；77日均ready。两张小表逻辑DATA+INDEX约 **2.81MiB**（information_schema估算，不含共享可复用空间）。没有清生产Redis/buffer pool。
- 第二页10条入选记录所需的正价聚合输入 **89,521分钟记录→612日记录**。这不是rows examined或物理IO测量：SQL仍保留未覆盖日期的raw fallback；EXPLAIN显示raw日期索引range，估算78行，但**未裁掉全部历史分区**，不能声称“整个SQL只碰612物理行”或fallback零开销。

| 完整公网列表请求（数据库自然暖、服务器测） | 原重复样本 | 新重复样本 |
| --- | ---: | ---: |
| offset10、10条 | 126.38～126.39ms | **67.48～73.01ms** |
| offset50、10条 | 148.97～149.00ms | **65.57～69.65ms** |

原样本默认含全范围统计；新列表显式拆出统计，初始另有summary请求，不能把此表当作整页总耗时。首次原171.88/337.15ms、新98.10/65.57ms单独留在原始记录，不称冷库/P95收益；正负缓存状态与样本数量均有限。首屏/翻页改善还须请求者同设备验证。

### 验证

- `comparison.json`：固定旧SHA加载旧repository，同库六组SQL结果逐字段相等（两页、指定日/策略、ETF空、统计窗口空）；`full-json-comparison.json`四组原默认完整API的字节长度/排序SHA相等，未把新实现与自身比较。
- `smoke.json`：仅预置`stock_migration_smoke`，已验证同分钟同COUNT改价、删除后极值收缩、NULL/0排除、发布异常源与聚合一起rollback、两连接前后提交快照、发布者行锁超时、异常日期不ready、retention墓碑阻止重新发布。精确隔离测试行已清理，无新数据库、无FK、无生产源写探针。
- **751项执行、750通过、1既有skip**，JS语法通过；新增独立异步前端合同证明summary挂起/失败不挡表格、空scope不触发价格聚合。冻结源码/依赖双研究指纹与第一批/原基线完全一致。
- 单Chromium经真实公网、1672×941：旧前端在新兼容full后端上作**视觉**对照，非完整旧服务器性能基线。最终首屏旧/新PNG SHA均为 `f759fe1ad83766cf3f5b0ea786038865b38995b381056d57f610edcfb9080db6`，即同视口整张像素一致，无JS异常/整页溢出。首次列表完成约269ms、统计约289ms仅一个服务器浏览器样本，不是请求者设备；翻页只一个列表GET，不重复filters/summary。注入summary503后10行列表保留、错误明确，重试恢复。全部非GET/HEAD在发送前拦截，未实际删除/切统计/提交AI任务。
- `deployment.json`：本地/公网health与认证readiness200、真实ready/accepting_jobs，**40/40**、四队列queued/running0、五服务active/NRestarts0，tracking HTML/JS/common JS磁盘/本地/公网SHA一致，匿名新接口401、非法limit422。无供应商重拉、研究重算或自动交易改动。

### 回滚与余项

优先恢复原tracking前端（发布新资源版本）及将repository构造的`use_daily_model`默认关闭，API默认full仍兼容；**保留0040迁移登记、表和已部署writer合同**，无需删表/改原源/删除研究。不要把生产整体reset回旧SHA，或撤掉迁移登记造成后续0040复用冲突；重新启用前对可能未遵守新合同的源日期重新构建。

**本批完成后第三批回测批量化/轻轮询、研究证据数据库分页/全协议累计摘要及热点现有快照复用仍未做；请求者端验收仍未做。**
