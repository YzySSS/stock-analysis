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

如果要跑 Tushare 相关同步，再补：

- `TUSHARE_TOKEN`

## 3. 启动 API

方式一：直接运行 uvicorn

```bash
cd /root/.openclaw/workspace/stock-analysis
python3 -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
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
