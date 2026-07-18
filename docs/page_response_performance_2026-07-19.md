# 页面响应性能基线与整改（2026-07-19）

## 1. 统一测量口径

本轮覆盖 Dashboard、Portfolio、Selection 和 Backtest 的首屏页面、静态 JS 与只读 API。预算沿用 `CURRENT_VERSION_PLAN.md`：

| 类型 | 首次观测/缓存未命中 | 热请求 |
| --- | ---: | ---: |
| HTML / 静态资源 | `< 100ms` | `< 100ms` |
| 列表与 compact 汇总 API | `< 800ms` | `< 200ms` |
| 重型计算 | 必须异步入队 | 不进入页面同步首屏 |

新增串行只读工具：

```bash
PYTHONPATH=. .venv/bin/python scripts/benchmark_page_responses.py \
  --profile dashboard \
  --repetitions 5 \
  --dashboard-local-cache-miss
```

工具只包含 GET 目标，不提交选股、回测或 AI 建议任务。`first_observed_ms` 是在线进程当前状态下的首次样本，可能已经命中缓存；Dashboard 另提供 fresh Python process 的应用缓存未命中探针。所有数据库和命令均串行执行，符合当前 2 vCPU / 3.6 GiB 主机资源红线。

选择 `--profile backtest` 时，工具还会按真实 `DOMContentLoaded` 顺序串行测量完整数据链：策略、因子状态、验证列表、任务列表，以及页面自动选中的最近回测结果和第一页成交明细。默认 run 的优先级与前端一致：运行中最高进度 → 排队中 → 最近成功。

## 2. 初始总览

2026-07-19 本机 `127.0.0.1:8000` 首轮五次串行测量：

| 页面/接口 | 首次观测 | 热中位数 | 响应大小 | 判断 |
| --- | ---: | ---: | ---: | --- |
| Dashboard HTML | 3.07ms | 1.51ms | 8.6 KiB | 达标 |
| Dashboard JS | 1.47ms | 1.37ms | 26.2 KiB | 达标 |
| Dashboard compact | 3394.20ms | 1.38ms | 21.3 KiB | 冷请求超预算 |
| Portfolio HTML / JS | 1.68ms / 1.71ms | 1.51ms / 1.49ms | 7.0 / 29.2 KiB | 达标 |
| Selection/Backtest Strategies | 205.59ms | 15.43ms | 20.0 KiB | 达标 |
| Portfolio API | 44.63ms | 32.50ms | 9.0 KiB | 达标 |
| Selection HTML / JS | 2.02ms / 1.59ms | 1.73ms / 1.43ms | 12.1 / 57.6 KiB | 达标 |
| Selection results | 54.30ms | 51.19ms | 64.1 KiB | 达标 |
| Backtest HTML / JS | 1.55ms / 1.41ms | 1.31ms / 1.41ms | 13.3 / 40.0 KiB | 达标 |
| Factor status | 34.40ms | 19.53ms | 705 B | 达标 |
| Validation list compact | 9.18ms | 8.27ms | 10.9 KiB | 达标 |
| Backtest runs compact | 13.91ms | 11.13ms | 31.2 KiB | 达标 |

该表是接口层初始基线，不等于浏览器完整可用时间；Backtest 的完整首屏串行 waterfall 已在第 6 节单独复核。

## 3. Dashboard 切片

缓存未命中的模块拆分显示：tracking 约 14ms、市场概览约 126ms、市场时机约 11ms、热点主题约 66ms，短线情绪板约 1261ms。情绪板最重查询从 `stock_realtime_intraday` 为约 30 只涨停股读取整日 1 分钟明细，单次返回约 6205 行；数据库查询在热状态仍约 452ms，首次进程连接/缓存状态下会进一步放大。

整改保持页面字段和产品语义不变：

1. 开板次数、首次封板和最后开板由 MySQL 8 window query 在库内聚合，不再把 6205 条分钟明细搬回 Python；
2. 查询使用当前实时快照交易日形成明确的 `trade_date + quote_minute` 边界；
3. 在有界日期范围内显式使用既有 `code + quote_minute` 索引，避免按整日全市场约百万级分钟行扫描；
4. 不新增 schema、长期缓存或陈旧快照，不改变 30 秒 compact 缓存口径。

优化前后真实情绪板 JSON SHA-256 均为 `a68094209aad6295096b0deb3ac8a5e0a9b3bdc679719304cfacfdabbac7187d`，开板次数/时间、榜单顺序和全部字段一致。

新磁盘代码的首轮五次应用缓存未命中样本为 `515.63 / 418.38 / 387.41 / 466.30 / 462.05ms`，中位数 `462.05ms`、最大 `515.63ms`。部署后在线首次/热中位数为 `401.85ms / 1.24ms`；独立进程连续五次强制缓存未命中的中位数 `412.65ms`、最大 `457.53ms`。相比旧线上首次 `3394.20ms` 约快 8.4 倍，稳定进入 `<800ms` 预算。

API 串行重启后 `NRestarts=0`，页面和 API 均为 200，响应字段哈希不变。Dashboard 切片完成，下一页进入 Portfolio。

## 4. Portfolio 切片

线上五次串行复核：HTML 首次/热中位数 `3.30 / 1.38ms`，JS `1.53 / 1.37ms`，`GET /api/portfolio` 为 `47.41 / 32.61ms`，均远低于预算。

前端 `DOMContentLoaded` 虽写成先 `loadStrategies()` 再 `loadPortfolio()`，但 Portfolio 的 `loadStrategies()` 只在浏览器内填充“短期/波段/长期”三个固定选项，不发网络请求，也不读取共享策略 API。首屏只有一次真实数据请求 `/api/portfolio`，不存在需要并行化的网络 waterfall。

因此本切片不修改 Portfolio Service/Repository、前端加载顺序或缓存。已有 Repository 将持仓市场上下文固定为 9 条批量 SQL，当前两个持仓的完整响应约 9.0 KiB；继续优化只会增加复杂度，没有真实收益。测量工具已同步移除误归类到 Portfolio 的 `/api/strategies` 目标。Portfolio 判定完成，下一页进入 Selection。

## 5. Selection 切片

线上五次串行基线：HTML 首次/热中位数 `3.09 / 1.27ms`，JS `1.37 / 1.24ms`，初始策略列表 `184.34 / 15.08ms`，最近选股结果 `57.16 / 52.86ms`，全部达标。

结果读取继续覆盖三个真实产品分支：

| 分支 | 首次 | 热中位数 | 响应大小 |
| --- | ---: | ---: | ---: |
| 默认最近结果 | 51.94ms | 51.65ms | 64.1 KiB |
| 显式 `run_id` | 84.03ms | 50.67ms | 64.1 KiB |
| 按 `strategy_id` 最近结果 | 52.13ms | 51.67ms | 64.1 KiB |

三条结果约 64 KiB，其中每条主要由 `factor_scores`、`sentiment_context` 和 `trade_plan` 构成。Selection 页面会在主表/详情实际展示因子、舆情依据、原因风险和交易计划；这些不是无用 debug 字段，删除或另藏会回退已经确认的结果可读性。首屏默认只加载策略列表，不会自动拉历史结果；运行选股继续通过 worker 异步入队。

因此本切片不修改 Selection Repository、响应字段、前端加载或缓存。三个读取分支均远低于预算，Selection 判定完成，下一页进入 Backtest。

## 6. Backtest 切片

回测页真实首屏依次等待策略列表、历史输入状态、冻结验证列表和任务列表；若当前尚未选中 run，还会自动读取默认 run 的结果与第一页成交明细。因此完整链路是 6 个串行 GET，而不是只看任务列表一个接口。

各接口五次基线均在预算内：策略列表首次/热约 `222.31 / 17.49ms`，因子状态 `35.91 / 19.48ms`，冻结验证列表 `11.47 / 8.94ms`，任务列表 `17.18 / 11.08ms`，最近结果 `14.02 / 9.84ms`，第一页成交明细 `55.15 / 20.17ms`。最近结果约 3.5 KiB，十条成交明细约 24.1 KiB。

新增的完整链路自动测量得到：

| 指标 | 结果 | 预算 |
| --- | ---: | ---: |
| 首次观测 | 260.77ms | `< 800ms` |
| 热中位数 | 83.25ms | `< 200ms` |
| 最大观测 | 260.77ms | `< 800ms` |
| 六个响应合计 | 92,506 B | 仅保留页面真实使用数据 |

五次总耗时为 `260.77 / 79.56 / 91.84 / 81.56 / 84.94ms`，自动选中 `backtest_lowvol_reversal_20260512_011309_225521`，并按该结果的 `observe_t3_daily` 口径读取成交明细。

前三个信息请求理论上可以与任务链并发，但当前完整热链只有约 83ms，首次也只有约 261ms；为节省约几十毫秒而让 2 核服务器同时发起多条数据库查询，收益小于资源尖峰风险。因此本切片不修改前端串行语义、不加缓存，也不裁剪验证或成交字段。回测计算本身继续只通过 worker 异步执行，不阻塞首屏。

至此 Tracking、Dashboard、Portfolio、Selection、Backtest 均已有同一预算下的冷/热证据；PERF-2 页面响应基线与首轮整改完整收口。后续只有真实回归超预算时才重新开启对应切片。
