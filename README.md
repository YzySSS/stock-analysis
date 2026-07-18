# 股票分析管理台

这是一个面向 A 股研究和策略跟踪的管理台。当前主线是 FastAPI 后端、MySQL 数据层、Web 管理页面和后台数据任务，用来查看市场状态、运行选股策略、跟踪复盘、回测验证，以及观察舆情和主题轮动。

系统仅用于研究和辅助决策，不构成投资建议。

当前运行基线、版本路线与剩余整改队列见 [`docs/CURRENT_VERSION_PLAN.md`](docs/CURRENT_VERSION_PLAN.md)。历史架构整改文档已转为审计台账，不再作为当前待办来源。

## 项目结构

```text
app/
  api/               FastAPI 接口与 Web 页面
  backtest/          回测服务与任务模型
  jobs/              任务状态、worker 租约、readiness 与保留治理
  market_timing/     市场择时模型
  orchestration/     数据表初始化与同步编排
  strategies/        策略注册、配置与实现
  stock_selection/   选股、舆情、主题轮动辅助逻辑
  shared/            配置、数据库、日志等公共能力
scripts/             当前仍在使用的数据同步、补数和运维脚本
docs/                部署记录、设计记录、阶段性说明
config/              配置模板
requirements.txt     Python 依赖
```

## 使用方法

创建虚拟环境并安装依赖：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

配置环境变量：

```bash
cp .env.example .env
```

至少需要配置：

```bash
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=stock_user
DB_PASSWORD=your-password
DB_NAME=stock_analysis
TUSHARE_TOKEN=your-token
```

初始化或升级数据库表：

```bash
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate          # 默认只读计划
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --apply
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --check
```

`app.orchestration.migrate` 是唯一 schema 变更入口。API、Service、worker 和数据同步脚本只做查询/写入，不会在普通请求或 cron 中现场 `CREATE/ALTER TABLE`；实时 lifecycle 的分区创建/删除属于数据保留操作，不属于业务 schema migration。

持仓模块采用 `PortfolioService -> PortfolioRepository -> MySQL` 边界：Service 保留纪律规则和 AI 编排，Repository 统一 SQL；持仓列表的市场上下文一次批量加载，避免逐持仓 N+1 查询。

跟踪复盘采用 `Route -> SelectionResultTracker -> TrackingRepository -> MySQL` 边界：分页先圈定目标记录，再只计算本页行情极值；全局统计只读取纳入统计样本，避免 limit=10 仍重载全部历史结果。

首页采用 `Route -> DashboardRepository -> MySQL` read-model 边界：市场概览、热点主题和情绪榜 SQL 统一收口；候选股票的日线与分钟开板信息批量加载，不做逐股查询。

启动本地 API：

```bash
PYTHONPATH=. .venv/bin/uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

访问页面和健康检查：

```text
http://127.0.0.1:8000/
```

```bash
curl http://127.0.0.1:8000/api/health
```

常用后台脚本在 `scripts/` 下，建议通过 cron 或 systemd 运行：

```bash
PYTHONPATH=. .venv/bin/python scripts/run_kline_daily_update.py
PYTHONPATH=. .venv/bin/python scripts/run_fundamental_daily_update.py --batch-size 500
PYTHONPATH=. .venv/bin/python scripts/run_valuation_daily_update.py --batch-size 500
PYTHONPATH=. .venv/bin/python scripts/run_realtime_snapshot_update.py
PYTHONPATH=. .venv/bin/python scripts/run_market_fund_flow_update.py --force
PYTHONPATH=. .venv/bin/python scripts/run_market_opinion_update.py
PYTHONPATH=. .venv/bin/python scripts/run_market_timing_daily_update.py --lookback-days 120
PYTHONPATH=. .venv/bin/python scripts/run_realtime_lifecycle.py             # 默认 dry-run
PYTHONPATH=. .venv/bin/python scripts/run_realtime_lifecycle.py --apply
PYTHONPATH=. .venv/bin/python scripts/run_market_opinion_lifecycle.py       # 默认 dry-run
PYTHONPATH=. .venv/bin/python scripts/run_market_opinion_lifecycle.py --apply
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py       # 默认 dry-run
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py --apply
bash scripts/setup_kline_cron.sh
```

## 部署方法

线上建议让 FastAPI 只监听本机地址，再通过 Nginx 反代：

```text
FastAPI: 127.0.0.1:8000
API 服务: stock-analysis-api.service
回测 worker: stock-analysis-backtest-worker.service
选股 worker: stock-analysis-selection-worker.service
持仓建议 worker: stock-analysis-portfolio-worker.service
Nginx: 反代域名到 127.0.0.1:8000
```

常用部署命令：

```bash
sudo systemctl restart stock-analysis-api.service
sudo systemctl restart stock-analysis-backtest-worker.service
sudo systemctl restart stock-analysis-selection-worker.service
sudo systemctl restart stock-analysis-portfolio-worker.service
sudo systemctl status stock-analysis-api.service --no-pager
sudo systemctl status stock-analysis-backtest-worker.service --no-pager
sudo systemctl status stock-analysis-selection-worker.service --no-pager
sudo systemctl status stock-analysis-portfolio-worker.service --no-pager
sudo nginx -t
sudo systemctl reload nginx
```

线上检查：

```bash
curl https://www.yzysstock.cloud/api/health
curl -u '<user>:<password>' https://www.yzysstock.cloud/api/readiness
curl https://www.yzysstock.cloud/api/dashboard/summary?limit=3
```

`/api/health` 是不访问数据库的 liveness；`/api/readiness` 才会检查 MySQL、三个 worker 租约、队列、关键任务和数据新鲜度。股票因子层只与达到股票池 95% 覆盖的完整日线日期比较，ETF/零星盘中日线另标为 partial available。factor input 在交易日 18:30 补跑、03:20 兜底；Tushare 当日数据不足 80% 时跳过该日期，不写空因子。任务明细默认保留 90 天，结构化错误日汇总保留 365 天；正式回测、验证基线、已保存选股结果和持仓建议结论不在自动清理范围内。

API 和三个 worker 的 systemd unit 均在 `ExecStartPre` 执行 migration `--check`；只要存在待执行或 checksum 不一致的版本，进程就拒绝启动。系统页同时展示目标版本、已应用数和待执行版本。

分钟行情按用途分层：current snapshot 只保留最新值，全市场 1m raw 保留 2 个交易日，5m/15m rollup 与持仓/跟踪标的 1m 保留 90 个交易日。工作日 15:20 由 lifecycle 先校验聚合 manifest，再删除过期 raw 分区；个股聚合查询使用 `GET /api/stocks/{code}/realtime-rollups?interval=5|15`。

行业舆情 V2 不再在每个 15 分钟父快照中复制整段股票/新闻 JSON，而是写入关系化 stock/news/source 明细并引用 `stock_news.raw_id`。最近 5 个交易日保留盘中快照，更早日期只保留每日最后一批快照，最长 90 个交易日；工作日 16:05 先完成可恢复归一化，校验通过后才执行裁剪。

更具体的服务器部署记录见 `docs/current_server_deployment_2026-05-06.md`。
