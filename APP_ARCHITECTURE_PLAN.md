# APP_ARCHITECTURE_PLAN.md

## 目标

为仓库建立一个新的 `app/` 承接骨架，用于后续把历史脚本逐步迁移成可维护的模块化系统。

本轮设计遵循两个核心原则：

1. 模块职责清晰
2. 选股策略必须可插拔、可切换、可归档

---

## 一、模块划分

新的应用骨架拆分为以下模块：

```text
app/
├── data_ingestion/
├── stock_selection/
├── error_learning/
├── backtesting/
├── orchestration/
├── shared/
└── strategies/
    ├── active/
    ├── registry/
    └── archive/
```

---

## 二、模块职责

### 2.1 `app/data_ingestion/`

负责：

- 行情数据接入
- 基本面数据接入
- 新闻 / 舆情数据接入
- 数据缓存与持久化
- 数据清洗、统一字段、标准化输出

建议后续迁入：

- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/etf_datasource.py`
- `src/data_source.py`
- `src/datasource_v2.py`

---

### 2.2 `app/stock_selection/`

负责：

- 因子计算
- 股票打分
- 选股流程
- 市场状态判断
- 选股结果输出

建议后续迁入：

- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/strategy_factory.py`
- `src/strategy_manager.py`
- 与选股直接相关的核心逻辑

说明：

该模块不直接保存策略实现细节，而是调用 `app/strategies/` 中注册的策略。

---

### 2.3 `app/error_learning/`

负责：

- 推荐结果追踪
- 复盘分析
- 错误归因
- 失败经验沉淀
- 模型改进建议产出

建议后续迁入：

- `src/recommendation_tracker.py`
- `src/postmarket_analyzer.py`
- `analyze_with_deepseek.py`
- 部分 `call_deepseek_*` 能力（需去敏后重构）

说明：

该模块是“策略学习闭环”的核心，不只是分析结果，更要沉淀为什么错、如何修正。

---

### 2.4 `app/backtesting/`

负责：

- 历史回测
- 因子有效性验证
- 参数优化
- 策略版本对比
- 回测指标输出

建议后续迁入：

- `src/backtest.py`
- 选中的主回测入口
- 经过筛选后的 `ic_analysis_*`
- 后续保留的一份主回测实现

说明：

后续应避免几十个 `v12_backtest_*` 并存，最终只保留统一入口。

---

### 2.5 `app/orchestration/`

负责：

- 配置装配
- 任务编排
- 定时任务入口
- 报告投递
- 模块之间的调用顺序管理

建议后续迁入：

- `daily_update.py`
- 调度相关脚本
- 报告触发入口
- 飞书推送入口（去敏后）

说明：

这一层是“应用层”，不是策略层。目的是避免业务流程继续散落在 shell 脚本中。

---

### 2.6 `app/shared/`

负责：

- 公共配置
- 公共数据结构
- 公共工具函数
- 统一日志
- 公共异常定义

说明：

所有模块共享的底层能力都应逐步沉淀到这里，而不是各脚本各写一份。

---

## 三、策略独立存储设计

这是本轮最关键的设计点。

你的要求是：

> 选股策略要有单独的存储位置，并且可以随时更换使用。

我完全同意，所以专门拆出：

```text
app/strategies/
├── active/
├── registry/
└── archive/
```

### 3.1 `app/strategies/active/`

用于存放当前可直接使用的策略实现。

原则：

- 每个策略单独目录或单独文件
- 每个策略有明确 ID / 名称 / 版本
- 可被 `stock_selection` 和 `backtesting` 模块调用

建议形态示例：

```text
app/strategies/active/
├── v13_three_factor/
│   ├── strategy.py
│   ├── config.yaml
│   └── README.md
├── v13_hybrid/
│   ├── strategy.py
│   ├── config.yaml
│   └── README.md
```

### 3.2 `app/strategies/registry/`

用于存放策略注册信息，而不是策略实现本身。

作用：

- 定义有哪些策略可用
- 标识默认策略是谁
- 记录策略描述、状态、版本、入口路径
- 支持运行时切换策略

建议示例：

```yaml
strategies:
  - id: v13_three_factor
    name: V13 三因子策略
    status: active
    entrypoint: app.strategies.active.v13_three_factor.strategy
    config: app/strategies/active/v13_three_factor/config.yaml
    tags: [alpha, short_holding, core]

  - id: v13_hybrid
    name: V13 混合市场状态策略
    status: review
    entrypoint: app.strategies.active.v13_hybrid.strategy
    config: app/strategies/active/v13_hybrid/config.yaml
    tags: [hybrid, regime]

default_strategy: v13_three_factor
```

### 3.3 `app/strategies/archive/`

用于归档旧策略和实验策略。

原则：

- 不删除历史策略
- 但历史策略不继续污染主线
- 回测仍可引用归档策略做对照

适合放入：

- V9
- V10
- 各类废弃或过拟合策略
- 重构前的旧版本策略脚本

---

## 四、策略切换机制建议

后续应该支持以下能力：

### 4.1 配置切换

通过配置指定当前启用策略，例如：

```yaml
current_strategy: v13_three_factor
```

### 4.2 运行时切换

命令或脚本入口可指定：

```bash
python run_selection.py --strategy v13_hybrid
```

### 4.3 回测与实盘共用同一策略入口

避免出现：

- 实时选股用一套逻辑
- 回测用另一套逻辑

后续要尽量保证：

> 同一策略实现，同时可用于选股与回测，只是输入数据和执行模式不同。

---

## 五、推荐的第一批迁移策略

第一批只做骨架承接，不做大规模逻辑迁移。

建议顺序：

### Step 1
先把主线文档落好，明确目录和职责。

### Step 2
优先迁入数据层和共享配置层。

### Step 3
为策略模块建立第一个标准模板，例如：

- `v13_three_factor`

### Step 4
再从现有 `src/` 中抽取选股逻辑接到 `stock_selection/`。

### Step 5
最后才接回回测、错误汲取、推送和自动调度。

---

## 六、对后续重构的实际意义

这样做之后，项目会从“脚本堆”逐步变成：

- 数据可独立维护
- 策略可独立替换
- 回测可独立验证
- 错误可独立沉淀
- 编排层只负责调用，不再挤满业务逻辑

一句话：

> 后续不再是“改某个大脚本”，而是“替换某个模块”或“切换某个策略”。

这对股票分析项目尤其重要，因为策略天然会迭代，而系统不应该因为策略切换而不断重构底座。
