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

该表是接口层基线，不等于浏览器完整可用时间；尤其 Backtest 当前存在多个首屏请求串行等待，仍需在对应切片检查前端 waterfall。

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
