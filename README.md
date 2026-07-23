# 股票分析管理台

面向 A 股研究、选股观察和复盘的 FastAPI + MySQL 管理台。项目包含 Web 页面、数据同步任务、可恢复 worker 队列、跟踪复盘、持仓管理、市场择时和通用历史回测基础设施。

系统仅用于研究和辅助决策，不构成投资建议。

## 当前策略范围

仓库只保留 A 股舆情选股系列。普通选股入口提供一个稳定基线和一个实验版本：

- `a_share_sentiment`：A 股舆情选股，当前版本 `0.4.4`
- 结合新闻/公告/题材、板块与主题资金、热门度、盘中交易确认和风险约束
- 默认最多返回 3 只；没有股票达到门槛时允许返回 0 只
- 当前状态为 `stable / frozen_baseline / enabled / unvalidated`
- 尚未通过正式样本外验证，不应把观察结果解释为收益承诺

整改版 `a_share_sentiment_v05 / 0.5.0` 的状态为
`experimental / shadow_only / enabled / unvalidated`。它可以在普通选股入口
手动选择、运行和按条保存，但页面会明确标记为“实验可执行”；它不会自动晋级
或替换 `0.4.4`。用户主动保存的结果按 `strategy_version=0.5.0` 进入手动
14 天跨版本统计。

从 `2026-07-24` 起，`0.4.4` 与 `0.5.0` 会在交易日 `09:25` 使用同一批
集合竞价输入做配对观察，累计 5 个成功交易日后停止该轮自动观察。自动结果只写入
`strategy_forward_*` 前瞻观察与执行复盘表，按当日开盘价计算后续表现；
不会写入 `selection_result`，也不会进入用户手动选股的 14 天统计。以后新增
的 `shadow_only` 策略必须带不可变版本标签，并自动继承同样的 5 交易日配对
观察规则，不能自行跳过。日常候选快照会继续配对物化，为两个手动策略提供
同口径的可读候选，但快照本身仍不等于用户保存的选股记录。

其他非舆情、历史和诊断失败的选股策略已从注册表与执行代码中移除。通用回测框架仍保留，用于查看历史任务和承接未来经过单独冻结、验证的策略；两个舆情版本的 `backtest_status` 均为 `disabled`，不能直接发起回测。

跟踪复盘按入选时间统计最近 14 个自然日内的已保存结果，不因策略升级排除旧版本；每条 `selection_result` 都会显式保存并展示当时实际执行的 `strategy_version`。超过 14 天或被手动标记为“不统计”的记录不进入汇总。

## 项目结构

```text
app/
  api/               FastAPI 接口与 Web 页面
  backtest/          通用回测服务、历史结果和 worker 任务模型
  data_ingestion/    行情、基本面、舆情、资金与主题数据同步
  jobs/              任务状态、worker 租约、readiness 与保留治理
  market_timing/     市场择时模型
  orchestration/     数据库 migration 与同步编排
  portfolio/         持仓管理与建议
  strategies/        舆情策略注册、配置与实现
  stock_selection/   选股候选、执行、队列与结果保存
  tracking/          选股跟踪复盘
  shared/            配置、数据库、日志等公共能力
config/              运行配置
deploy/              systemd 等部署文件
docs/                舆情整改实施、部署、验收与回滚 runbook
scripts/             数据同步、补数、审计和运维脚本
tests/               自动化测试
requirements.txt     Python 依赖
```

## 本地启动

创建虚拟环境并安装依赖：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

创建本地配置：

```bash
cp .env.example .env
```

至少配置以下环境变量：

```bash
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=stock_user
DB_PASSWORD=your-password
DB_NAME=stock_analysis
TUSHARE_TOKEN=your-token
```

初始化或升级数据库：

```bash
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate          # 只读计划
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --apply
PYTHONPATH=. .venv/bin/python -m app.orchestration.migrate --check
```

`app.orchestration.migrate` 是唯一 schema 变更入口。历史 migration 会继续保留，以保证已部署数据库的版本和 checksum 可验证；它们不代表对应的旧策略仍可执行。

启动 API：

```bash
PYTHONPATH=. .venv/bin/uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

访问管理台和健康检查：

```text
http://127.0.0.1:8000/
http://127.0.0.1:8000/selection
```

```bash
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:8000/api/readiness
curl http://127.0.0.1:8000/api/strategies?instrument_type=stock
```

## 运行舆情选股

选股统一进入可恢复的 MySQL worker 队列。CLI 提交示例：

```bash
PYTHONPATH=. .venv/bin/python -m app.stock_selection.run_selection \
  --strategy a_share_sentiment \
  --limit 3
```

也可以通过 API 提交：

```bash
curl -X POST http://127.0.0.1:8000/api/selection/run \
  -H 'Content-Type: application/json' \
  -d '{"strategy_id":"a_share_sentiment","limit":3,"max_picks":3,"instrument_type":"stock","save":false}'
```

返回的 `run_id` 用于查询任务和结果：

```bash
curl http://127.0.0.1:8000/api/selection/runs/<run_id>
```

候选池舆情补充任务默认也只服务于 `a_share_sentiment`：

```bash
PYTHONPATH=. .venv/bin/python scripts/run_strategy_sentiment_refresh.py
```

## 常用后台任务

数据同步和补数建议通过 cron 或 systemd 后台运行：

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
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py                  # 默认 dry-run
PYTHONPATH=. .venv/bin/python scripts/run_job_retention.py --apply
PYTHONPATH=. .venv/bin/python scripts/run_automatic_strategy_observation.py --dry-run
```

## 部署

建议让 FastAPI 只监听本机地址，由 Nginx 提供公网入口：

```text
FastAPI: 127.0.0.1:8000
API 服务: stock-analysis-api.service
回测 worker: stock-analysis-backtest-worker.service
选股 worker: stock-analysis-selection-worker.service
持仓建议 worker: stock-analysis-portfolio-worker.service
API 持久异步任务 worker: stock-analysis-durable-task-worker.service
```

常用检查命令：

```bash
sudo systemctl status stock-analysis-api.service --no-pager
sudo systemctl status stock-analysis-backtest-worker.service --no-pager
sudo systemctl status stock-analysis-selection-worker.service --no-pager
sudo systemctl status stock-analysis-portfolio-worker.service --no-pager
sudo systemctl status stock-analysis-durable-task-worker.service --no-pager
sudo /usr/sbin/nginx -t
curl https://www.yzysstock.cloud/api/health
```

API 和 worker 在启动前执行 migration `--check`；只要存在待执行版本或 checksum 不一致，进程就会拒绝启动。

## 仓库边界

GitHub 只保存当前运行代码、配置模板、部署文件、测试和本 README。以下内容仅保留在本地，不再纳入 Git：

- 设计稿、阶段记录和部署笔记：`docs/`
- 数据库与抓取缓存：`data_cache/`
- 回测输出：`backtest_results/`
- 本地备份：`backups/`
- UI 参考图和预览页
- `.env`、证书、密钥和其他敏感配置

不要使用 `git add -f` 绕过这些边界。

## 测试

```bash
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v
```

提交前至少执行 Python 编译、前端 JavaScript 语法检查、migration `--check` 和与改动相关的测试。
