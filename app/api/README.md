# API 运行说明

## 1. 安装依赖

```bash
cd /root/.openclaw/workspace/stock-analysis
pip install -r requirements.txt
```

## 2. 配置环境变量

推荐准备：

```bash
cp .env.example .env
```

然后填写至少这些配置：

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

如果是新环境，建议先初始化 schema：

```bash
python3 -m app.orchestration.init_project
```

如果要跑 Tushare 相关同步，再补：

- `TUSHARE_TOKEN`

并建议至少执行一次：

```bash
python3 -m app.data_ingestion.valuation_sync
python3 -m app.data_ingestion.fundamental_sync
```

这样 `stock_basic` 会补齐 selector / tracking / API 需要的估值和基本面字段。

## 3. 启动 API

方式一：直接运行 uvicorn（推荐显式带上 `--app-dir`）

```bash
cd /root/.openclaw/workspace/stock-analysis
.venv/bin/python -m uvicorn app.api.main:app --app-dir /root/.openclaw/workspace/stock-analysis --host 0.0.0.0 --port 8000 --reload
```

如果你当前 shell 没有把项目根目录放进 Python 模块搜索路径，就会报：`ModuleNotFoundError: No module named 'app'`。
所以要么使用上面的 `--app-dir`，要么先执行：

```bash
export PYTHONPATH=/root/.openclaw/workspace/stock-analysis:$PYTHONPATH
```

方式二：使用脚本

```bash
cd /root/.openclaw/workspace/stock-analysis
bash run_api.sh
```

## 4. 接口访问

- 健康检查：`http://127.0.0.1:8000/api/health`
- Swagger 文档：`http://127.0.0.1:8000/docs`
- ReDoc：`http://127.0.0.1:8000/redoc`
