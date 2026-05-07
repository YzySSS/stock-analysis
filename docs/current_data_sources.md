# 当前数据源整理

## 一、当前可用的数据源

### 1. 本地 SQLite 历史库

用途：
- 历史行情缓存
- 技术因子计算基础
- 开发和本地实验底座

当前文件：
- `src/data_cache/stock_history.db`
- `src/data_cache/sentiment_cache.db`

当前状态：
- `stock_history.db`
  - 表：`stock_prices`, `last_update`
  - 记录数：390430
  - 股票数：6579
  - 日期范围：2025-12-30 ~ 2026-04-02
- `sentiment_cache.db`
  - 表：`sentiment_cache`
  - 记录数：996

### 2. 腾讯云 MySQL

用途：
- 新主线正式结构化存储
- 后续承接股票基础信息、日线、因子快照、选股结果、策略注册、任务日志

连接状态：
- 地址：`10.4.4.17:3306`
- 数据库：`stock`
- 已连通
- 已完成第一版建表

当前核心表：
- `stock_basic`
- `daily_kline`
- `factor_snapshot`
- `selection_result`
- `strategy_registry`
- `task_run_log`
- `market_context_daily`
- `stock_news`
- `stock_sentiment_daily`

当前舆情链路补充（2026-05-07）：
- 新闻源优先级：Tavily -> AkShare -> DuckDuckGo -> RSS
- `stock_news` 已记录单条新闻情绪、可信度等级/原因、质量分与质量等级
- `stock_sentiment_daily` 已记录过滤前新闻数、有效新闻数、平均可信度与平均质量分
- 详细实现记录：`docs/news_quality_credibility_2026-05-07.md`

### 3. AkShare

用途：
- A股历史数据
- 指数数据
- 涨停池等数据

当前状态：
- 已安装
- 可正常调用
- 实测 `stock_zh_a_hist` 成功

### 4. BaoStock

用途：
- 股票基础信息
- 历史 K 线
- 作为第一阶段 MySQL 数据接入主来源

当前状态：
- 代码已接入在新骨架中
- `stock_basic_sync.py` / `daily_kline_sync.py` 将优先依赖它

### 5. Tushare

用途：
- PE / PB 等估值数据
- ROE 等财务指标
- 基本面补充数据
- 某些更偏研究型的增强因子输入

当前状态：
- 仓库里已有多处 Tushare 使用痕迹
- 代表文件包括：
  - `src/data_source.py`
  - `update_pe_pb_tushare.py`
  - `update_roe_tushare.py`
  - `update_roe_smart.py`
- 说明它在旧项目中已经被用于“估值 + 财务指标补充”这条线
- 但当前接入方式仍存在问题：
  - token 曾明文出现在仓库里
  - 代码封装较散
  - 还没进入新的 `app/data_ingestion/` 主线

当前定位：
- 已纳入当前项目数据源体系
- 但作为第二阶段增强数据源使用，更适合承接：
  - `valuation_sync.py`
  - `fundamental_sync.py`
- 当前已新增 `app/data_ingestion/valuation_sync.py` 骨架，用于承接 Tushare 的 PE / PB 同步
- 当前已新增 `app/data_ingestion/fundamental_sync.py` 骨架，用于承接 Tushare 的 ROE 等基本面同步
- 接入原则：
  - 只从环境变量读取 `TUSHARE_TOKEN`
  - 不允许真实默认值
  - 不直接复用旧脚本里的明文 token 写法

---

## 二、当前部分可访问但未稳定封装的数据源

### 1. 东方财富龙虎榜
- 页面可访问
- 具备后续接入价值
- 适合作为增强数据源

### 2. 东方财富人气榜
- 页面可访问
- 当前未完成结构化解析

---

## 三、当前受限或不稳定的数据源

### 1. 北向资金接口
- 当前测试返回 502
- 需要重新找稳定接口或代理方案

### 2. Yahoo Finance SOX 接口
- 当前测试返回 429
- 存在限流或访问限制

### 3. 同花顺人气榜
- 当前测试返回 403
- 直接抓取受限

---

## 四、当前缺失的数据类型

以下数据还没有稳定进入新主线：

- 主力净流入/资金流向
- 稳定可复用的新闻舆情数据
- 板块轮动统一表结构
- 基本面统一表结构（Tushare 后续可承接其中一部分）

---

## 五、当前建议

第一阶段新主线只认：

1. SQLite 历史库（已有底座）
2. 腾讯云 MySQL（新正式库）
3. AkShare（补历史/行情）
4. BaoStock（补基础信息/历史K线）
5. Tushare（补估值/财务指标/基本面）

其中 Tushare 已正式纳入数据源设计，但放在第二阶段增强接入，不抢第一阶段主线底座角色。
