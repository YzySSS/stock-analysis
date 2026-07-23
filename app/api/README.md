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
- `SITE_AUTH_USERNAME`
- `SITE_AUTH_PASSWORD_HASH`
- `SITE_AUTH_SESSION_SECRET`

首次配置登录凭据时不要手写哈希，也不要把明文密码放进命令行。使用隐藏输入：

```bash
.venv/bin/python scripts/configure_site_auth.py --username your_username
```

该命令只把 PBKDF2 密码哈希和随机会话密钥写入权限为 `600` 的 `.env`。
再次执行可修改用户名或密码，并默认让全部旧会话失效。

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

线上服务由 systemd 托管：

```bash
systemctl status stock-analysis-api.service
systemctl restart stock-analysis-api.service
```

## 4. 接口访问

- 健康检查：`http://127.0.0.1:8000/api/health`
- 登录页面：`http://127.0.0.1:8000/login`
- Swagger 文档：`http://127.0.0.1:8000/docs`
- ReDoc：`http://127.0.0.1:8000/redoc`

除 `/login`、`/static/*`、`/favicon.ico` 和 `/api/health` 外，其余页面和 API
都需要应用会话。生产环境必须使用 HTTPS，并保持
`SITE_AUTH_COOKIE_SECURE=true`。
