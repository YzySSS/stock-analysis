# SECRETS_REMEDIATION_PLAN.md

## 结论

当前仓库存在**高风险 secrets 暴露问题**，且分布广、重复多、类型杂。不能把它视为“有几处不规范”，而应视为：

> 公开仓库中已有多类真实凭据、数据库配置和 webhook 被泄露。

建议默认以下凭据均已失效处理或应尽快轮换：

- Tushare Token
- DeepSeek API Key
- Tavily API Key
- Feishu Webhook
- MySQL 主机/用户名/密码

---

## 一、暴露类型分类

### 1.1 硬编码数据库配置
大量文件中直接写入：

- DB Host: `10.0.4.8`
- DB User: `openclaw_user`
- DB Password: `open@2026`
- DB Name: `stock`

问题：

- 暴露内网拓扑与数据库访问方式
- 导致仓库与真实运行环境强耦合
- 一旦网络边界配置失误，风险会被放大

### 1.2 硬编码 API Keys / Tokens
已发现真实值直接写入：

- Tushare Token
- DeepSeek API Key
- Tavily API Key
- Feishu Webhook

问题：

- 任何拉到仓库的人都可直接复用
- 无法判断是否已被第三方滥用
- 若继续使用，后续日志和调用费用都可能异常

### 1.3 Shell 启动脚本中 export 真实密钥
如：

- `run_postmarket.sh`
- `run_premarket_now.sh`
- `run_intraday.sh`
- `run_daily_update.sh`

问题：

- shell 文件经常被复制、转发、截图
- 运维脚本最容易在协作中扩散 secrets
- 后续排障时也容易继续暴露

### 1.4 README / 文档中暴露真实值
如：

- `README.md` 中写出真实 Tushare Token

问题：

- 文档往往是最容易被搜索引擎、缓存和第三方镜像收录的部分
- 即使后续删掉，历史记录和 fork 中也可能仍可见

---

## 二、暴露清单（按类型聚合）

## 2.1 Tushare Token 暴露点

已识别文件：

- `README.md`
- `update_pe_pb_tushare.py`
- `update_roe_smart.py`
- `scripts/screen_sector_v5.py`
- `scripts/screen_sector_v52.py`

处理建议：

1. 立刻在 Tushare 后台轮换 token
2. 所有脚本改为仅读取 `TUSHARE_TOKEN`
3. 删除代码里的默认 token 和 `os.environ[...] = '真实值'` 形式

---

## 2.2 DeepSeek API Key 暴露点

已识别文件：

- `call_deepseek_analysis.py`
- `call_deepseek_overfitting.py`
- `scripts/deepseek_strategy_reconstruction.py`
- `scripts/deepseek_cost_decision.py`
- `scripts/deepseek_cost_analysis.py`
- `run_postmarket.sh`
- `run_premarket_now.sh`
- `run_intraday.sh`
- `run_daily_update.sh`

处理建议：

1. 立刻轮换 DeepSeek API Key
2. 统一改为读取 `DEEPSEEK_API_KEY` 或兼容的 `OPENAI_API_KEY`
3. 禁止在 shell 启动脚本中直接 export 真实值

---

## 2.3 Tavily API Key 暴露点

已识别文件：

- `search_factors.py`
- `run_postmarket.sh`
- `run_premarket_now.sh`
- `run_intraday.sh`
- `run_daily_update.sh`

处理建议：

1. 立刻轮换 Tavily Key
2. 统一从环境变量 `TAVILY_API_KEY` 读取
3. 删除所有硬编码测试 key

---

## 2.4 Feishu Webhook 暴露点

已识别文件：

- `run_postmarket.sh`
- `run_premarket_now.sh`
- `run_intraday.sh`
- `run_daily_update.sh`

说明：

- `config/.env.example` 和 `env_config.sh` 里的占位写法可以保留
- 但真实 webhook URL 不能继续放在脚本中

处理建议：

1. 立刻更换 webhook
2. 后续只从 `.env` / 环境变量读取 `FEISHU_WEBHOOK`

---

## 2.5 MySQL 配置暴露点

暴露最广，涉及几十个文件。

典型文件包括：

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- `backtest.py`
- `daily_update_mysql.py`
- `init_stock_basic.py`
- `update_pe_pb_tushare.py`
- 大量 `v12_backtest_*`
- 大量 `ic_analysis_*`
- 多个 `backfill_*`
- 多个 `sentiment_*`
- 多个维护脚本

统一暴露内容为：

- Host: `10.0.4.8`
- User: `openclaw_user`
- Password: `open@2026`
- Database: `stock`

处理建议：

1. 立即轮换数据库密码
2. 评估是否需要更换数据库用户名
3. 统一改为：
   - `DB_HOST`
   - `DB_PORT`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_NAME`
4. 删除所有 `os.getenv(..., '真实默认值')` 中的真实默认值
5. shell 中的 mysql 命令禁止再使用 `-p'真实密码'`

---

## 三、风险等级排序

### P0，必须立刻处理

- DeepSeek API Key
- Tavily API Key
- Feishu Webhook
- Tushare Token
- MySQL 密码

原因：

- 都是可直接复用的真实凭据
- 公开暴露后无法假设“没人看到”

### P1，尽快处理

- MySQL host / user / database 信息
- 真实运行脚本中的 export 方式
- README 中的真实 token 痕迹

### P2，后续治理

- Git 历史清理
- 统一配置加载框架
- pre-commit / secret scan 机制

---

## 四、建议的整改顺序

### Step 1，立刻轮换外部凭据

优先级最高：

1. DeepSeek Key
2. Tavily Key
3. Tushare Token
4. Feishu Webhook
5. MySQL 密码

说明：

- 这一步优先于改代码
- 因为就算马上删掉仓库里的明文，旧值也已经可能被看到

### Step 2，仓库内替换真实值

目标：仓库中不再出现真实 secrets。

执行原则：

- Python 统一使用 `os.getenv`
- shell 统一 `source config/.env` 或独立安全配置文件
- 示例文件保留占位符，不保留真实值

### Step 3，建立统一配置入口

建议增加：

- `config/.env.example`
- `config/settings.py` 或 `src/config.py` 统一读取环境变量

避免每个脚本重复定义 `DB_CONFIG`。

### Step 4，增加自动扫描

建议后续加入：

- GitHub secret scanning
- pre-commit hooks
- gitleaks / trufflehog 之类的扫描工具

---

## 五、代码整改策略建议

### 5.1 不要继续使用这种写法

```python
os.getenv('DB_PASSWORD', 'open@2026')
```

原因：

- 这仍然把真实密码写进了代码
- 即使表面用了环境变量，本质还是泄露

### 5.2 推荐写法

```python
DB_PASSWORD = os.getenv('DB_PASSWORD')
if not DB_PASSWORD:
    raise RuntimeError('DB_PASSWORD is required')
```

### 5.3 shell 脚本不要这样写

```bash
export DEEPSEEK_API_KEY="真实key"
```

推荐改成：

```bash
source config/.env
```

或：

```bash
: "${DEEPSEEK_API_KEY:?DEEPSEEK_API_KEY not set}"
```

---

## 六、建议下一批实际改动

如果继续推进，建议按这个顺序直接动手：

1. 修改 README，删掉真实 token
2. 修改所有 shell 脚本，移除真实 export
3. 修改 Tushare / DeepSeek / Tavily 相关脚本，去掉真实 key 默认值
4. 抽出统一数据库配置模块，替换散落的 DB_CONFIG
5. 再考虑清 git 历史

---

## 七、最终判断

当前仓库最紧急的问题不是策略，而是 secrets 管理。

一句话：

> 在继续做策略开发之前，必须先完成 secrets 止血，否则后续任何迭代都建立在不安全基础上。
