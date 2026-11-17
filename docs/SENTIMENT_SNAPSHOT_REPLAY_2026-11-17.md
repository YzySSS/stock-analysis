# 舆情已记录决策重放与证据封存 · 2026-11-17

## 本轮可交付与不可恢复的边界

当前完整舆情历史选择器回测仍不可用，不能把本轮当成已开放任意历史区间。

- `sentiment_candidate_snapshot` 保存的是最终选中行，而不是全市场的原始候选输入。
- 原 `sentiment_selection_input` 只保存行数和 SHA-256，原始payload没有持久化；hash不能还原原文。
- `stock_news` 的创建时钟和 `market_opinion_raw` 的 first_seen/crawl 时钟不能使覆盖更新后的新闻内容变成不可变历史档案。
- 今天重新抓到旧新闻，只能记录今天真实收到的时间，不能倒填为过去已知。因此不进行历史新闻大回填，不修改注册表backtest_status和原 `STRATEGY_NOT_REPLAYABLE` 409。

新增的独立通道是**已记录决策的下一交易日假设收益诊断**：读取确切已发布快照，不重新计算选择器或新闻，不把观察级升级成可交易，也不用于冻结研究cohort、自动调参或晋级。

## 接口及使用

- `GET /api/backtest/recorded-snapshots?strategy_id=a_share_sentiment_v06&limit=20`：有界列出已发布快照，三种注册舆情策略均支持。列表不保证每份历史快照通过完整证据校验。
- `GET /api/backtest/snapshot-replay?snapshot_id=...&holding_days=3`：只读重放确切快照，持有期1/3/5；受原应用认证保护，不创建queue任务/selection结果/观察/持仓/交易。
- `PYTHONPATH=. .venv/bin/python scripts/replay_sentiment_snapshot.py --snapshot-id EXACT_ID --holding-days 3 --archive`：同时把原始DB证据、完整本次查询价格路径和独立日历封存到本地。
- `PYTHONPATH=. .venv/bin/python scripts/replay_sentiment_snapshot.py --sealed-report-sha256 SHA256 --holding-days 3`：完全不访问DB/网络，从已封存材料重复计算；使用封存报告的observed_through时钟，不能凭后来行情改变原报告。

新方法论 `recorded_sentiment_decision_next_session_v1`，返回 `selector_recomputed=false / full_history_backtest_available=false / research_only=true`。新接口不混入旧回测列表和累计曲线。

## 时间、收益和证据合同

1. 读取单份snapshot ID，最多50候选、64来源。快照必须ready/passed、覆盖≥98%、版本/config/implementation身份存在，候选数和来源ID严格匹配。
2. 按原来源manifest的event/received双时钟≤decision校验；必需来源passed，原本可选来源partial/missing保留原缺口而不洗成完整。候选lineage也必须匹配真实来源和截止时点。
3. 实际可得时点使用published_at（不得早于decision），统一保守从该发布日期之后的下一开市日开盘计算，不使用旧reference_trade_date倒推入场；这不是v0.6冻结盘中5分钟成交协议。
4. 独立SSE Tushare自然日日历逐日连续、开市标志合法。固定交易日持有窗口，缺一天不把后来复牌日移入窗口。
5. 强制完整复权OHLC路径、历史名称/ST执行状态、正价格/前收盘及保守日线端点涨跌停约束；买入检查开盘、卖出按实际退出端点检查（1日为开盘，3/5日为收盘），不是精确撮合或成交保证。任一缺口不计收益，未成熟明确pending。只在退出交易日18:45之后用完整日线计算，即使1日模式使用开盘退出也不拿不完整盘中日线冒充完整路径。
6. 默认每边佣金3bps、每边滑点5bps、卖出印花税5bps，仅固定研究成本假设，不是实际券商费用。1日模式在入场后的下一开市日开盘退出（T+1），3/5日模式入场日计第一日、目标日收盘退出。无资金账户、容量、精确撮合或真实订单。
7. 所有候选都保留原trade_grade；观察级收益只是反事实诊断。空决策保留空分母，缺失/排除/未成熟不会计作零收益。
8. 旧row_hash的规范化材料并不完整，不能仅凭旧hash声称重建原始全量输入。本次content hash证明的是**本次封存后内容不变**，不追认原始全市场新闻与输入完整性。

## 未来输入封存

新增独立 `app/backtest/sentiment_evidence.py` 包装原物化器，冻结的 `app/shared`、`app/strategies`、`app/stock_selection` 完全不改。

- `scripts/materialize_sentiment_candidate_snapshot.py` 及收盘依赖刷新脚本使用包装器；在原一致性事务内将全市场candidate read view规范化封存，其SHA必须与原物化器input_read_view_hash一致。发布后再封存manifest/最终候选/来源manifest。
- 全市场原始read-view只封存最新v0.6；0.4/0.5每15分钟高频物化仅封存选中结果，明确`status=snapshot_only`/input skipped，不伪称其完整输入已存。实测全量压缩约9.1MB，2GiB总预算约可容纳230份同等输入（不是保留期承诺）；达到预算明确告警，须审核导出，既不删旧证据也不阻断选股。
- 本地目录为 `logs/sentiment-evidence`（已gitignore），私有权限，gzip低压缩级别；content-addressed，临时文件fsync后link发布，不覆盖、不删除旧证据。
- 每份原始文件上限64MiB，目录总上限2GiB、最多10,000份；文件锁串行校验配额。超限不静默删掉旧证据。独立复核已将归档配额/I/O/序列化故障与选股数据门分离：选股仍按原数据质量合同执行，任务结果的`evidence_archive.status=warning`及事件列表明确记录未封存的阶段和原因，任务摘要显示“证据封存告警”，不冒称完整封存。输入文件同时校验原read-view行hash与封存元数据envelope hash；封存后时间字段被改动也会被发现。
- 这修复了“只有hash没有全市场payload”的一个缺口，但不声称已囊括选择器后续所有辅助查询、原始新闻版本和事件ledger。尚不能因此开放完整历史选择器回测。
- 无DDL、大区间回填、研究协议激活或生产参数变化。实现阶段未发布；最终发布验收见文末。

## 已完成验证

- 真实有界库存：仅11月1日起统计，v0.6共44份/127候选，0.4.4 308份/843候选，0.5.1 295份/671候选（截至11月17日13:30前查询）。不是全部历史的可用性证明，也不代表全部通过独立重放前置校验。
- 11月2日09:32 v0.6快照3只：使用11月3日开盘、11月5日收盘，真实三条完整复权路径可计算；原watch不变，仅工程smoke。封存本次价格/日历后完全离线重算，labels/path hash一致。
- 11月16日14:32的3只返回3 pending；11月17日凌晨快照因实际11月17日发布，保守入场是11月18日，不倒填为11月17日或更早。
- 69项关键定向通过；完整819项执行，818通过、1skip（4.192秒），日志`/root/.openclaw/workspace/output/sentiment-replay-unittest-20261117.log`。
- source SHA仍为`a1d5ba02d1f0046cf886a4af29d42318d5c5b4001728eedaf17ccc1e3db786db`，依赖SHA仍为`f15258898a4afc76ef6101904ac4e25a8ee8c2b3daaadc02e1a41557aa7b0248`。
- 本地原始快照证据46,225B，SHA `4129e1fcd970715a8cf9cb75267a3a31ff860a5bae173e7ce5d7377cfd83b46b`；完整封存报告48,927B，SHA `1dd7ecbaf097826ef13b0a66f269033fd88944d82d69b739acc992d81edb4225`。均位于上述logs目录，封存时间不冒充历史采集时间。
- 此处为首轮实现结果；生产HTTP、全市场封存实测与极简UI的最终结果见文末。未来自然Cron封存和请求者设备视觉仍未验收。

## 独立复核补充（待根发布验收）

- 修复证据归档失败阻断正常选股、收盘脚本重复封存、收盘退出误用开盘涨跌停判断，以及离线CLI未默认沿用归档持有期/费用四项问题。
- 独立日历重复日期、必需来源flag非法值及缺失策略版本进一步fail-closed；输入封存元数据篡改校验已补齐。
- 新增7项有意义的边界验证：收盘价与开盘价限价分歧、重复日历、非法来源flag、归档配额不回滚决策、归档I/O故障显式报告、采集时钟篡改、完全离线保留非默认费用/持有期。84项定向通过；完整826执行825通过1skip，4.342秒。日志`/root/.openclaw/workspace/output/sentiment-replay-review-unittest-20261117.log`。
- 未修改`app/shared`、`app/strategies`、`app/stock_selection`或`app/backtest/service.py`，未运行DDL/历史回填/发布/重启；既有冻结研究实现保留。

## 使用入口与发布前实测

- 现有回测参数卡内新增默认关闭的“已记录选股诊断”。仅展开后读取最多50份快照，不增加首屏HTTP；与旧回测任务/曲线隔离，不新增看板。用户可选择策略、已发布选股和1/3/5日假设持有期，查看简短计数、平均假设收益与候选状态。前端按请求代次、选项及登录身份丢弃过期响应，并转义所有动态内容。
- 首次根实际物化v0.6成功、数据质量passed，耗时16.39秒、峰值RSS364,180KiB；决策快照封存成功，但全市场输入超过初版24MiB上限，正确返回封存告警而不阻断选股。根随后把单份上限调至64MiB并保留2GiB目录总限；完整输入后续已实测通过，见根独立验收；不将小快照文件作为全市场验收证据。
- 未改CSS或整体布局；没有截图/请求者设备视觉一致性验收，不据此声称与概念图整页完全一致。
- `node --check`通过；5项Node轻量DOM/request测试覆盖默认关闭零请求、列表/诊断乱序、身份切换与HTML转义。UI与归档相关46项定向通过；最终完整831项执行830通过1skip，4.416秒，日志`/root/.openclaw/workspace/output/sentiment-replay-ui-unittest-20261117.log`。既有截取函数式前端测试同步纳入新增身份失效helper，静态asset版本断言同步更新。

## 根独立验收与正式上线

- 初次真实v0.6物化16.39秒、峰值RSS364,180KiB；选股ready/passed，初设24MiB不足产生显式归档warning，选择器仍成功。调整为64MiB后再次真实物化15.67秒、峰值RSS364,408KiB，5,224条全量输入原始53,655,571B/压缩9,140,021B，input SHA `399715eee07e3c07cf0f29d0ad9b68265d96baa693e109f8afac423b52c969ac`；选中快照40,678B，整个封存状态sealed。主机磁盘可用17GiB，目录预算2GiB，不修改DB容量或全备。
- 最终仅最新v0.6封存全量输入，旧版本高频运行保持snapshot_only；保持原研究源码/依赖和交易资格不变。根补回归覆盖旧版scope跳过但选中快照仍归档。
- 根独立读11/2成熟快照：3 ready；11/17新快照：3 pending/0 ready，未计零收益。封存报告后完全离线重算dict严格相等。证据`output/sentiment-replay-root-validation-20261117.json`。
- 实现提交`550ce11`已推GitHub main并常规重启API；其余4 Worker未重启。根最终完整回归832项执行831通过1skip（4.403秒），日志`output/sentiment-replay-root-unittest-20261117.log`；冻结路径及旧backtest service无diff。
- 本地/公网20项实际HTTP断言通过：health/readiness、最多50份快照、真实成熟3 ready、今日3 pending、缺快照404、匿名401、原舆情历史POST仍409 `STRATEGY_NOT_REPLAYABLE`；页面及`20261117replay1`脚本SHA与磁盘完全一致。证据`output/sentiment-replay-root-http-20261117.json`。这不是请求者浏览器整页渲染/视觉验收。
- 根从实际input/snapshot封存文件重新读取，5,224/3条内容hash与input envelope hash均通过。仅本地内容寻址封存，不等于异地备份或历史原始新闻追认。
- 5服务active/NRestarts0，migration40/40，4队列queued/running均0；无DDL、大回填、策略参数/冻结cohort/自动交易变化。
- 尚待自然验收：14:32/18:57等真实Cron的自动封存。已将既有19:05一次性只读验收`33cb94cf-ecd0-446c-99c9-86f81d5d8cc8`扩展为检查收盘链与同次输入/选中封存，必要时只读跟踪既有19:27/20:27重验；不以人工物化代替自然验收。完整舆情历史选择器仍不可恢复/未开放。
