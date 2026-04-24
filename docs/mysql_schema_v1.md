# MySQL Schema V1

## 目标

为股票分析项目建立第一版可落地的 MySQL 核心表结构，先覆盖最小主线：

1. 股票基础信息
2. 日线行情
3. 因子快照
4. 选股结果
5. 策略注册
6. 任务运行日志

## 设计原则

- 先服务主线，不追求一步到位
- 允许 SQLite 继续作为本地缓存和实验层
- MySQL 作为正式结构化存储
- 表名和字段名尽量稳定，为后续 `app/` 迁移提供锚点

## 核心表

### 1. stock_basic

存股票静态信息、估值字段、基本面字段和状态字段。

当前已包含：
- 基础字段：`code`, `name`, `instrument_type`, `market`, `industry`
- 估值字段：`pe_tushare`, `pb_tushare`, `valuation_updated_at`
- 基本面字段：`roe`, `roa`, `grossprofit_margin`, `netprofit_margin`, `revenue_yoy`, `profit_yoy`, `fundamental_period`, `fundamental_updated_at`
- 状态字段：`is_st`, `is_delisted`, `listing_date`, `updated_at`

### 2. daily_kline

存标准日线行情，按 `(code, trade_date)` 去重。

### 3. factor_snapshot

存某个交易日、某只股票、某个因子的数值，便于后续回溯和策略比较。

### 4. selection_result

存每次选股任务的输出结果，和 `run_id` 绑定。

### 5. strategy_registry

存策略登记信息，用于和 `app/strategies/registry` 协同。

### 6. task_run_log

存初始化、同步、选股、回测等任务运行状态。

## 当前落地文件

- `app/shared/settings.py`：统一配置入口
- `app/shared/db.py`：统一 MySQL / SQLite 连接入口
- `app/orchestration/init_project.py`：初始化第一版 MySQL 核心表结构
- `app/data_ingestion/valuation_sync.py`：补齐估值相关列并同步 PE / PB
- `app/data_ingestion/fundamental_sync.py`：补齐基本面相关列并同步 ROE / ROA / 毛利率 / 增长字段

## 下一步建议

1. 运行 `app/orchestration/init_project.py` 初始化腾讯云 MySQL 表结构
2. 运行 `stock_basic_sync.py` / `daily_kline_sync.py` / `valuation_sync.py` / `fundamental_sync.py` 补齐数据
3. 保持 `stock_basic` 字段与 selector / tracking / API 使用字段一致
4. 逐步把旧脚本逻辑迁入 `app/` 主线
