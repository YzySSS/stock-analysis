# 股票分析管理台

这是一个面向 A 股研究和策略跟踪的管理台。当前主线是 FastAPI 后端、MySQL 数据层、Web 管理页面和后台数据任务，用来查看市场状态、运行选股策略、跟踪复盘、回测验证，以及观察舆情和主题轮动。

系统仅用于研究和辅助决策，不构成投资建议。

## 项目结构

```text
app/
  api/               FastAPI 接口与 Web 页面
  backtest/          回测服务与任务模型
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
PYTHONPATH=. .venv/bin/python app/orchestration/init_project.py
```

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
bash scripts/setup_kline_cron.sh
```

## 部署方法

线上建议让 FastAPI 只监听本机地址，再通过 Nginx 反代：

```text
FastAPI: 127.0.0.1:8000
API 服务: stock-analysis-api.service
回测 worker: stock-analysis-backtest-worker.service
Nginx: 反代域名到 127.0.0.1:8000
```

常用部署命令：

```bash
sudo systemctl restart stock-analysis-api.service
sudo systemctl restart stock-analysis-backtest-worker.service
sudo systemctl status stock-analysis-api.service --no-pager
sudo systemctl status stock-analysis-backtest-worker.service --no-pager
sudo nginx -t
sudo systemctl reload nginx
```

线上检查：

```bash
curl https://www.yzysstock.cloud/api/health
curl https://www.yzysstock.cloud/api/dashboard/summary?limit=3
```

更具体的服务器部署记录见 `docs/current_server_deployment_2026-05-06.md`。
