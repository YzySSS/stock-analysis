# 剩余接口读取优化（2026-11-14）

固定基线：`7c68890a39a23e20c8c98950f2bd03dadb3aa364`。证据在工作区 `output/api-optimization-20261114/`；比较器从固定 SHA 加载旧代码，不从移动 HEAD 加载。所有项目命令使用仓库 `.venv/bin/python`，串行执行，不清空数据库 buffer pool。

## 个股概览：已实现

`GET /api/stocks/{code}/overview` 原先为每只股票单独做 5 次全市场 `RANK()` 排序和 2 次样本数计数；完整接口通常 12 条 SQL。现在使用 2 个有界人口聚合，按“严格大于目标值的数量 + 1”同时计算 2 个实时排名和 3 个因子排名，完整接口降到 7 条 SQL。没有新增 Redis 键或延长缓存 TTL。

保留以下合同：

- 并列值使用 `RANK` 而非 `DENSE_RANK`；NULL 指标不产生排名，样本数分母仍包含该指标为 NULL 的源行。
- 实时人口过滤条件不变；因子人口仍取**该股票自己的最新因子日期**，不是改为全市场最新日期。无因子时字段存在性、`factor_sample_size` 与原实现一致。
- 目标和人口在同一条 SQL 内读取、比较，数值使用数据库原始 DECIMAL。生产隔离级别是 `READ-COMMITTED`，不能把前一条报价查询读到的旧价格/金额当作排名阈值。
- 认证、行业均值、技术计算、UI、策略和 09:35 自动选股不变。冻结研究源码与依赖未改。

同条件函数级完整 JSON 比较（自然暖 DB，无 overview payload 缓存；不含 HTTP/ASGI）：

| 样本 | 原版 3 次 | 新版 3 次 |
| --- | --- | --- |
| sh.600519 | 102 / 98 / 98 ms | 42 / 42 / 41 ms |
| sz.000001 | 97 / 104 / 106 ms | 41 / 42 / 41 ms |
| sh.600000 | 97 / 102 / 101 ms | 41 / 43 / 41 ms |
| bj.920001 | 102 / 107 / 106 ms | 45 / 45 / 48 ms |
| sh.512480（无因子） | 31 / 32 / 30 ms | 11 / 10 / 10 ms |

15 对完整 JSON 的排序序列化字节、长度及 SHA-256 相同。真实 SQL 剖面为 7 条；一次新进程函数调用约 51ms，其中两条排名聚合约 11/13ms。`overview-comparison.json`、`overview-after-profile.json` 保留细节和实际执行计划。

4 项新增隔离 SQL 合同测试覆盖并列/NULL/范围外目标/因子日期、空人口、单样本百分位及更新时同语句读取目标。完整回归 721 项执行、720 通过、1 既有 skip。

## 复盘分页：根因与初步候选验证

`GET /api/tracking?limit=10&offset=10&instrument_type=stock&compact=true&include_runs=false` 的旧计划要读取约 **89,521 个有效分钟样本**，底层读取约 93,510 行，再汇总为日级极值与持有期天数。不能以“返回 10 行”推断它是主键查 10 行。

- 分区表约 179 万行、数据 443MiB、已有索引 360MiB（信息模式估计）。当前 `(code, quote_minute)` 索引**不含 `latest_price`**，每条有效分钟都需要回表读价格，这是主要成本。
- 试过静态代码/日期分支：分区由 78 个减为 62～65 个，但完整 SQL 原版 233～273ms、候选 209～267ms，收益不足以证明增加两阶段查询和多个 SQL 分支值得。
- 去除中间日级重复分组的候选在另一轮原版 340～433ms、候选 241～277ms，但 DB 存在自然波动，仍需读相同数量的原始价格。没有用它冒充稳定的亚百毫秒方案，也没有用日线替代真实分钟极值。
- 未部署上述复杂 SQL 候选；随后采用下节已验证上线的覆盖索引方案。另一条更重路线是带来源/覆盖/迟到修订合同的股票×日分钟极值读模型，本轮不需要引入。

以上初步候选、原始计划、schema 与 HTTP 基线在 `static-candidates.json`、`tracking-before-analyze.txt`、`schema.json`、`http-before.json`。

## 个股概览生产验收

实现 `147288b`、说明 `6c533d5` 已推送远端 main，当前生产工作树仍在历史部署分支 `codex/sentiment-v06-implementation`（`main` 本地引用较旧，因此推送明确使用 `HEAD:main`，未强推/重写历史）。仅重启 API，四 Worker 未重启；五服务 active/NRestarts=0，实时认证 readiness 为 `ready/accepting_jobs=true`，migration 38/38。本地/公网 health、认证/匿名边界与 home.js 文件哈希均通过。研究源码/依赖双指纹逐键不变。

同服务器经公网、复用 HTTP 连接、每路径 3 次完整认证 GET：

| 接口 | 上线前 | 上线后 |
| --- | --- | --- |
| sh.600519 overview | 130～145ms | 44～45ms |
| sz.000001 overview | 111～112ms | 44～45ms |
| bj.920001 overview | 109～119ms | 45～46ms |
| sh.512480 overview | 36～38ms | 13～14ms |

4 组公网/本地 overview 完整 JSON SHA 上线前后相同；这不是用户电脑端或 P95 的证据。测试未做浏览器整页重测：本轮没有改页面/静态资源，测量对象是完整概览 API，而非整个股票详情页。复盘 HTTP 自然缓存/数据状态有变化；本轮未修改复盘实现，不能将其上线前后波动宣称为复盘改进。证据 `http-before.json`、`http-after.json`、`deployment-checks.json`。

## 复盘覆盖索引：已上线

实现提交 `c7895e1`，migration `0039` 只给 `stock_realtime_intraday_tracked` 添加 `(code, quote_minute, latest_price)` 可见非唯一完整 BTREE 索引 `idx_tracked_code_minute_price`；保留旧索引，不改查询 SQL、源数据、分钟区间、有效价格过滤、日线/分钟合并和交易天数口径，也没有新增页面缓存或延长 TTL。分区主键隐含的 `trade_date` 足以使该读取覆盖。生产 EXPLAIN 自动选择新索引并报告 `Using index`，不需要硬编码索引提示。

### 迁移与数据验证

- 操作前确认当日 00:34 完成的全备 1,393,579,539 字节，重新流式 SHA-256 与 manifest 相同：`67d0da942de4901b479c5cbefb96e6c438018a6bf4ee96f811a2c60c38f14ee9`。保存生产 schema 与精确计数；共享可复用池约 8GiB，不跨表求和 `DATA_FREE`。
- 在预置、逐连接检查名称的 `stock_migration_smoke` 上，以相同字段/分区主键和 3 个 RANGE 分区、3,600 条含 NULL/零/负价的样本验证：在线 DDL 约 41ms，分钟聚合结果/行数不变，计划由回表改为覆盖；重复 runner 和统一 migration 均幂等。索引后的插入/唯一键 upsert 也已验证并回滚；3,600 条测试样本精确清理，smoke tracked 表恢复为空。
- 生产强制 `ALGORITHM=INPLACE, LOCK=NONE`，会话元锁等待上限 5 秒，无阻塞降级分支；DDL 约 7.24 秒成功，第二次 runner 不执行 DDL。生产精确 **1,851,385 行**、ID 边界与日期边界前后相同，数据内容不改写。操作前后采样行锁等待为 0；这是前后快照，不能据此声称 DDL 全程没有瞬时资源波动。
- 四项新增迁移合同测试覆盖在线约束/短元锁等待、幂等、错误索引拒绝、在线失败无阻塞回退；完整回归 **725 项执行、724 通过、1 既有 skip**。

### 同条件性能与等价性

下面是同一服务器经公网、复用 HTTP 连接的完整认证 GET，每个条件各 3 次；均为非默认页面、**没有整页投影缓存**。汇总子模块仍按原有缓存合同工作，数据库自然暖、未驱逐 buffer pool。

| 复盘条件 | 加索引前 | 加索引后 |
| --- | --- | --- |
| 第二页 offset=10 | 227～256ms | 114～143ms |
| 第三页 offset=20 | 237～270ms | 128～135ms |
| 较深历史页 offset=100 | 325～334ms | 163～164ms |
| 较深历史页 offset=210 | 320～324ms | 154～174ms |
| 指定 2026-08-11 | 74～75ms | 39～40ms |
| 超出末页 offset=9999 | 12～13ms | 11～12ms |

6 个条件、各 3 次完整原始 SQL 结果的排序 JSON SHA 全部前后一致。HTTP payload 含随时钟变化的 `stats_age_days`；跨时段有 6/12 组 HTTP hash 不同，**不宣称所有历史 HTTP 字节一致**。为消除时钟干扰，又在同一库上交替比较旧非覆盖索引与自然选择的新覆盖索引，仅在诊断进程固定统计年龄时钟：6 条件 × 3 轮的 **18 对完整 route payload 字节全部相同**，包括统计、交易计划、分钟极值和天数。此诊断中第二页重复调用约 263～271ms → 122ms，较深历史页约 365～390ms → 168～174ms；未在生产代码加入 FORCE INDEX 或固定时钟。

加索引后 SQL 新进程首次访问不同历史页仍出现约 225～334ms，重复约 113～139ms；API 重启后的本地首次第二页约 248ms，后续公网约 127ms。不能把重复读取成绩称作真正数据库冷读或保证所有请求亚百毫秒。本轮不做整页浏览器重测、不声称用户电脑端/P95；新增一个索引也意味着额外存储和写入维护，并未用短时测试证明交易时段持续写入的长期负载。

### 最终部署验收与证据

生产只重启 API 以加载第 39 项 migration registry，四 Worker 未重启。五服务 active/NRestarts=0，四队列 queued/running 均 0；本地/公网 health 200、认证 readiness `ready / accepting_jobs=true`，migration **39/39**，匿名 protected API 401，home.js 哈希和冻结研究源码/依赖双指纹不变。

证据在原目录下的 `tracking-index-preflight.json`、`tracking-index-smoke.json`、`tracking-index-production-migration.json`、`tracking-index-before.json`、`tracking-index-after.json`、`tracking-index-*-analyze.txt`、`tracking-index-paired-comparison.json`、`tracking-index-storage-after.json`、`tracking-index-smoke-write-and-cleanup.json`、`tracking-index-unittest.log`、`tracking-deployment-checks.json`。前一节 overview 的验收文件不覆盖。
