# Redis 页面读取闭环（2026-11-13）

## 范围与边界

本轮复用已有192MiB Redis，不安装第二套、不改Redis全局配置、不触碰队列/工作锁。基线提交为 `d0114f059ab7a6cee87313c0d9f0eeafc0952b09`。

目标不是让所有接口都缓存，而是让多个页面共享的有界热点在访客到达前完成构建。新增 `app/display_cache/` 完全处于冻结研究源码 `app/shared + app/strategies + app/stock_selection` 以外；readiness、选股提交/Worker、09:25研究和09:35自动选股继续走原合同，不拿缓存判定是否允许执行。

| 固定投影 | API/用途 | 后台刷新间隔 | 最大证据年龄 |
| --- | --- | ---: | ---: |
| dashboard | 首页/选股 `summary?limit=8&compact=true` | 15s | 60s |
| dataset | 策略列表/详情的数据检查输入 | 30s | 60s |
| tracking | 默认复盘第一页10条，compact、不含runs | 15s | 30s |
| tracking-filters | 默认股票复盘日期/策略筛选 | 30s | 60s |
| system | 系统状态展示；非`/api/readiness` | 30s | 60s |
| 指数 | 三指数quote、各minute/daily共7键 | 30s既有刷新合同 | 900s既有展示回退上限 |

其他筛选/分页/非compact读取继续使用原始实现，不随请求参数创建无限多投影，也不缓存私有组合、写请求或认证结果。页面视觉/策略参数不变。

## 预热、更新和降级

- API lifespan只增加一个daemon线程：5秒检查一次，串行构建到期或被更新失效的固定投影，然后串行刷新指数。无新常驻进程，无浏览器后台任务，无线程池fan-out。
- 指数预热有轮转游标与8秒软预算：单个HTTP超时可能越过预算，但不会让一次供应商故障占满7个完整超时窗口；其余键下轮继续。真实空Redis/API刚启动时仍可能需要现场构建/外部请求，不能声称冷计算消失。
- 20个关键行情/日线/因子/资金/筹码/状态/舆情/候选/读模型更新脚本使用兼容TaskRunLogger子类，**原日志事务完成后**仅写3个固定revision令牌。partial/failed任务可能有已提交有效行，也会失效显示层；skipped不刷新。通知失败不把已经成功的数据任务改为失败。没有在数据任务内重算页面。
- 保存/删除/纳入复盘沿现有失效边界同步变更tracking令牌；首页/复盘投影同时依赖该令牌。复盘内部summary还包含market revision，避免外层刷新后混入旧行情统计。
- 令牌不匹配时立即拒绝旧投影；后台最迟下一次5s检查开始更新。两次更新合并成一次最新状态构建，不排队堆积。外部行情和实际SQL耗时另计，不把5s说成完整更新的硬上界。
- 构建开始记录revision和上海自然日；发布前再次核对。构建期间发生更新时不发布过时代际。所有SQL投影跨上海自然日强制失效；源交易日、quote_time、snapshot_now与覆盖/失败信息不被重写成当前行情。
- Redis owner-token租约 + 进程内单飞防止通常的并发击穿。120s租约有界，到期旧owner无法发布或删除新owner。另一个进程正在构建时请求最多等1s，之后允许live uncached read，保持Redis可选；极端慢构建下允许有界重复计算，不声称严格exactly-once。
- Redis不可用时SQL投影绕过进程内write-through fallback，直接live SQL，不复用可能错过跨进程失效的旧“就绪”。Redis恢复后后台自行重新发布。构建失败只记warm状态，不续TTL；过期后请求正常报错/实时读取，绝不无限stale-if-error。指数仍是display_only，保留原15分钟有标记回退/失败退避合同，不进入策略。
- 新投影最多5个固定payload键，各<=1MiB，另有4个固定revision、短租约、1个180s warmer状态；不SCAN、不FLUSH、不删除队列。旧缓存自然按原TTL退出。

## 运维

- 默认开启；环境变量 `DISPLAY_CACHE_WARMER_ENABLED=false` 可关闭后台预热。投影仍按正常read-through与短TTL工作。
- 认证 `/api/health/performance` 新增 `display_cache`：各投影可用性/年龄/TTL、warmer最近结果；不输出payload或凭证。匿名不可访问此诊断；`/api/health`保持便宜，`/api/readiness`始终实时构建。
- 手动一次串行预热：`PYTHONPATH=. .venv/bin/python -c 'from app.display_cache.service import warm_cycle; print(warm_cycle())'`。
- 回滚应用提交即可，Redis新增键全是可丢弃显示缓存，无schema迁移、业务数据变动或全局配置回滚。

## 测量与验证

证据目录为workspace `output/redis-optimization-20261113/`，固定baseline SHA，所有数据库/项目命令使用仓库`.venv/bin/python`。不清数据库buffer pool，不以401作延迟样本。

函数级实测（真实Redis，含Redis序列化，**不含ASGI、公网或浏览器**）：

| 构建 | 原版本空展示缓存 | 新版本空展示缓存 | 新版本命中 |
| --- | ---: | ---: | ---: |
| 首页summary | 477ms | 461ms | 1.68ms |
| 指数完整overview | 1633ms | 1638ms | 1.50ms |
| 策略数据检查 | 267ms | 235ms | 0.47ms |
| 默认复盘 | 523ms | 230ms | 0.96ms |
| 系统 | 938ms | 205ms | 4.35ms |

**后两项冷读差额不能全部归为本轮改进**：数据库/既有内部缓存已变暖。本轮没有宣称SQL本身数倍提速；收益是持续预热、跨进程共享与及时失效后用户通常命中。真正空指数仍约1.64s，上游未变快。

四份投影（首页84140B、策略1924B、复盘20949B、筛选14634B）原始构建与Redis读回SHA-256逐字节一致。跨进程首次策略读取约75ms（含本进程Redis驱动首次初始化），不再查询全套数据检查SQL；源日期仍11月13日。一次完整warm_cycle约1.78s。真实Redis使用约1.53MiB/192MiB，未改其配置。

717项执行（716通过、1既有skip）；后续调整另跑33项针对性测试通过。新增10项合同测试覆盖换代中发布竞争、跨日/时钟异常、过期失败不续命、Redis故障不假绿、租约所有权、payload上限、warm隔离及提交后事件失败隔离。

生产验收与公网完整初始GET结果在部署后追加，服务器公网浏览器不是请求者电脑体验的证明。

## 生产验收（22:46～22:48 上海时间）

实现提交 `f25afd2` 已受控重启API。原四Worker未重启；五服务均active、NRestarts=0。数据库migration保持38/38，无DDL。冻结研究源码/安装依赖两指纹与基线逐键完全一致。

- 本地/公网health、实时认证readiness均200，业务状态实际为 `ready / accepting_jobs=true`（与之前审计时间的degraded不同，是此时的实时结果，不由新缓存提供）。匿名readiness、performance、system仍401；公网home.js与仓库SHA一致。
- 公网认证API（应用服务器经公网域名，复用HTTP连接）：summary **8.9ms**、完整指数 **6.8ms**、策略列表 **12.3ms**、默认复盘 **5.5ms**、系统 **13.1ms**；本地分别4.3/3.7/9.7/3.1/8.5ms。真实指数源日期均11月13日。
- 真实Redis-only dataset revision测试：旧投影立即不可读，7秒后观察到API后台已重新构建，revision改变、源日期不变。未对生产Redis做停机注入，也未写业务行情/选股数据；Redis离线/恢复和DB构建失败由隔离测试验证。
- 服务器公网Chromium、相同视口/路径、全新浏览器profile、完成全部初始GET（非只看HTML/标题）：

| 页面 | 上线前自然缓存状态 | 上线后自动预热状态 |
| --- | ---: | ---: |
| 首页（含指数/4折线） | 859ms | 216ms |
| 选股 | 798ms | 219ms |
| 复盘 | 411ms | 140ms |
| 组合 | 184ms | 104ms |
| 回测 | 299ms | 204ms |
| 策略 | 275ms | 204ms |
| 交易策略 | 81ms | 84ms |
| ETF | 92ms | 94ms |
| 系统 | 344ms | 124ms |

这组比较体现“请求到达时缓存是否已准备好”的产品收益，不是相同空缓存SQL计算性能的比较，也不是用户电脑实测。9页均settled、无JS异常/失败GET/拦截写请求/整页溢出；首页4条折线均完成。未动页面CSS/JS、策略或自动选股配置。

Redis最终约1.54MiB/192MiB，evicted_keys=0。应用机可用内存约1GiB，未新增进程。证据：`baseline.json`、`projections-after.json`、`deployment-checks.json`、`browser-before.json`、`browser-after.json`、`unittest.log`、`targeted.log`。

仍保留的边界：真正首次空缓存/Redis故障/更新后的第一个请求可走live SQL，外部指数仍受供应商约1.6s冷读取影响；后台刷新降低发生频率，不承诺所有任意筛选请求都在10ms内。历史分钟的日级持久化读模型并未在本轮新增；默认复盘已通过有界预热消除反复计算，若未来任意历史筛选仍慢，再独立按来源/完整性/迟到修订合同建设日级读模型。
