# PROJECT_AUDIT.md

## 项目审计结论

本仓库当前不应被视为“已经收敛完成的 V13 股票分析系统”，而应被视为一个长期迭代形成的**研究型仓库**。README 展示的是目标形态，但代码现实仍处于多版本并存、数据流并存、自动化脚本堆叠的状态。

一句话判断：

> 这是一个有研究积累、但尚未完成工程收敛的量化选股仓库。

适合作为后续重构和提纯的基础，不适合直接按 README 理解为稳定主线产品。

---

## 一、仓库现状概览

### 1.1 当前真实状态

仓库内同时存在以下几类内容：

- 多版本策略实验（V9 / V10 / V11 / V12 / V13相关）
- 本地 SQLite 历史数据流
- 远端 MySQL 回测与分析流
- 飞书推送与日报流程
- DeepSeek / Tavily / 新闻舆情增强
- 定时任务、回填脚本、一次性修复脚本

这说明仓库既承担了“研究试验场”的角色，也承担了“半生产运行脚本集合”的角色。

### 1.2 README 与实际代码不一致

README 中声称的若干关键文件，在当前仓库中并不存在，例如：

- `v13_strategy.py`
- `v13_backtest.py`
- `src/stock_database.py`

实际存在的相关文件为：

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- `src/stock_history_db.py`
- 大量 `v12_*`、`v11_*` 文件

结论：

> README 更像目标架构或阶段性总结，而不是当前代码的准确地图。

因此，后续接手必须以仓库真实结构为准，而不能以 README 为准。

---

## 二、当前可识别的三条主线

### 2.1 主线 A，本地 SQLite 历史数据流

代表文件：

- `init_history_db.py`
- `daily_update.py`
- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`

特点：

- 使用本地 SQLite (`src/data_cache/stock_history.db`)
- 用于缓存股票历史价格
- 适合开发、调试、轻量实验
- 依赖相对少，运行门槛较低

判断：

> 这是目前最适合作为“最小可运行主线”的部分。

### 2.2 主线 B，远端 MySQL 策略研究 / 回测流

代表文件：

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- 大量 `v12_backtest_*`
- 大量 `ic_analysis_*`
- `daily_update_mysql.py`
- `init_stock_basic.py`

特点：

- 依赖远端 MySQL
- 用于更完整的数据分析与回测
- 与真实运行环境耦合较深
- 配置大量散落在脚本中

判断：

> 这是研究能力更强的一条线，但目前工程卫生较差，不适合作为第一接手入口。

### 2.3 主线 C，自动化分析 / 推送 / AI 增强流

代表文件：

- `analyze_with_deepseek.py`
- `call_deepseek_*`
- `send_feishu_*`
- `stock_tracker_feishu.py`
- `intraday_scheduler.py`
- `run_*.sh`

特点：

- 面向盘前、盘中、盘后自动分析
- 接入飞书 webhook
- 使用 DeepSeek / Tavily / 新闻分析
- 更偏“自动汇报与辅助分析”层

判断：

> 这部分不应作为第一阶段维护重点，应建立在主策略内核稳定之后再接回。

---

## 三、当前核心问题

### 3.1 工程主线不清晰

当前仓库没有明确区分：

- 哪些是正式主线
- 哪些是历史实验
- 哪些是一次性分析脚本
- 哪些是生产辅助脚本

结果是：

- 新接手者难以判断从哪里开始
- 版本演化路径难以追踪
- 容易误改历史脚本
- 维护成本高

### 3.2 配置与密钥管理极差

仓库中存在大量敏感信息暴露问题，包括但不限于：

- README 中直接写出真实 token
- Python 文件中硬编码 DB 密码
- shell 脚本中直接 export API key
- 多处真实密钥和数据库配置重复散落

这已经不是“配置不规范”，而是明确的安全问题。

结论：

> 应将当前仓库中的相关 key / token / 数据库密码视为已泄露，必须尽快轮换。

### 3.3 README 描述领先于代码收敛

README 中已经呈现出较清晰的 V13 项目形态，但仓库真实结构仍明显停留在：

- 多版本策略并行
- 历史脚本未归档
- 数据流未统一
- 模块边界未明确

因此，当前项目最大的问题不是“策略有没有想法”，而是“仓库是否真正收敛到统一主线”。

### 3.4 存在研究资产，但尚未产品化

当前仓库并不是无序垃圾堆。它有明显的研究积累：

- 多版本因子试错
- 多轮回测
- IC 分析
- 市场状态检测思路
- 自动报告与推送链路

问题在于这些研究资产还没有被提炼成稳定可维护的产品结构。

---

## 四、接手时的主线判定

### 4.1 第一阶段应认定的最小主线

建议第一阶段只认以下文件为“基础主线”：

#### 数据层
- `src/stock_history_db.py`
- `init_history_db.py`
- `daily_update.py`

#### 数据源层
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/etf_datasource.py`

#### 因子 / 选股相关候选层
- `src/strategies/`
- `src/stock_screener.py`
- `src/technical_analysis.py`
- `src/v12_factor_model.py`

说明：

当前仓库里真正干净、容易跑通、利于提纯的起点，仍然是本地 SQLite 历史数据线，而不是 V13_Hybrid 远端 MySQL 线。

### 4.2 第二阶段再接回的增强层

以下能力建议在主线内核稳定后再逐步纳入：

- 远端 MySQL 统一存储
- V13 Hybrid 检测器 / 优化器
- 飞书推送
- DeepSeek 分析
- 舆情 / Tavily 新闻
- 定时任务调度

---

## 五、建议的目录重构方向

建议后续逐步重构为如下结构：

```text
stock-analysis/
├── app/
│   ├── data/
│   ├── factors/
│   ├── strategies/
│   ├── backtest/
│   ├── reports/
│   ├── integrations/
│   └── utils/
├── scripts/
│   ├── init/
│   ├── update/
│   ├── backtest/
│   ├── reports/
│   └── maintenance/
├── configs/
├── docs/
├── archive/
├── tests/
└── PROJECT_AUDIT.md
```

其中原则如下：

### app/
放真正长期维护的核心代码。

### scripts/
放命令入口与运维脚本，不再把大量主逻辑散落在根目录。

### archive/
放历史版本、一次性分析、废弃脚本，避免继续污染主目录。

### docs/
放架构文档、研究结论、指标说明，而不是让 README 承担全部事实来源。

---

## 六、建议的阶段性改造顺序

### Phase 1，止血

目标：让仓库变得“可以安全接手”。

优先事项：

1. 清理 secrets
2. 轮换已暴露 key / token / DB 密码
3. 将真实配置迁移到 `.env` / 环境变量
4. 修正文档中的虚假或过时主线描述

### Phase 2，提纯最小主线

目标：形成可跑通、可理解、可维护的最小股票分析内核。

优先事项：

1. 固定一条主数据流
2. 固定一个主策略入口
3. 固定一个主回测入口
4. 为主线写运行说明

### Phase 3，结构重组

目标：将历史实验、生产脚本、核心逻辑分层。

优先事项：

1. 将历史版本归档
2. 将根目录脚本迁入 `scripts/`
3. 将核心模块迁入统一包结构
4. 为关键流程补最少量测试

### Phase 4，恢复增强能力

目标：在干净主线之上，逐步恢复：

- 飞书推送
- AI 诊断
- 市场状态自适应
- MySQL 统一存储
- 定时自动运行

---

## 七、第一批建议保留 / 观察 / 归档

### 建议保留（高优先级）

- `init_history_db.py`
- `daily_update.py`
- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/strategies/`

### 建议观察（暂不删）

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- `daily_update_mysql.py`
- `init_stock_basic.py`
- `ic_analysis_*`
- `v12_backtest_*`

### 建议后续归档

- 历史版本主程序
- 一次性分析脚本
- 已废弃回测实验
- 临时修复脚本
- 重复 shell 启动脚本

---

## 八、对后续接手工作的建议

当前最合理的推进方式不是直接“继续写功能”，而是：

1. 先明确主线
2. 先做安全清理
3. 先形成最小可运行内核
4. 再逐步恢复复杂功能

一句话建议：

> 先把这个仓库从“研究仓库”提纯成“可维护主线”，再谈策略升级和自动化扩展。

---

## 九、下一步建议

建议下一轮直接执行以下工作之一：

### 选项 A，做 secrets 清点与整改方案
输出具体文件清单和整改顺序。

### 选项 B，做主线文件清单与归档计划
输出哪些是主线，哪些进 archive。

### 选项 C，开始搭一个 `app/` 最小内核骨架
先不迁全部逻辑，只建立后续重构承接结构。

推荐顺序：A → B → C。
