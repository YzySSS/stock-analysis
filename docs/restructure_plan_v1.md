# Stock Analysis 重构路线图 V1

## 目标

将当前研究型仓库重构为四个明确模块，并把“选股策略可替换”作为核心设计原则。

四个模块固定为：

1. 数据接入
2. 股票选股分析
3. 错误纠正
4. 回测模块

目标不是一次性重写全仓，而是把旧仓库中的稳定能力逐步迁入新主线 `app/`，同时把历史实验脚本降级为素材库或归档区。

---

## 一、总体架构

```text
app/
├── data_ingestion/         # 数据接入
├── stock_selection/        # 股票选股分析
├── error_learning/         # 错误纠正
├── backtesting/            # 回测模块
├── orchestration/          # 编排层
├── shared/                 # 共享能力
└── strategies/            # 可替换策略系统
    ├── active/
    ├── registry/
    └── archive/
```

说明：

- 四个业务模块是核心
- `shared/` 提供配置、数据库、日志、模型等基础能力
- `orchestration/` 负责把模块串起来
- `strategies/` 独立于业务模块，作为可插拔策略层存在

---

## 二、四个功能模块定义

## 2.1 数据接入（data_ingestion）

负责：

- 股票基础信息同步
- 日线行情同步
- 基本面数据同步
- 板块/行业数据同步
- 舆情/新闻/资金流等增强数据同步
- 数据清洗、标准化、入库

建议承接来源：

- `init_history_db.py`
- `daily_update.py`
- `daily_update_mysql.py`
- `init_stock_basic.py`
- `fill_history_mysql.py`
- `fill_history_gap.py`
- `fill_sector_rotation.py`
- `sentiment_*`
- `update_*`

第一阶段只先承接：

- `stock_basic_sync.py`
- `daily_kline_sync.py`

第二阶段补入：

- `valuation_sync.py`（优先接 Tushare 的 PE / PB）
- `fundamental_sync.py`（优先接 Tushare 的 ROE / 财务指标）

---

## 2.2 股票选股分析（stock_selection）

负责：

- 因子计算
- 评分计算
- 候选股票筛选
- 市场状态判断
- 选股结果生成
- 选股结果入库

建议承接来源：

- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/strategy_factory.py`
- `src/strategy_manager.py`
- `src/v12_factor_model.py`
- 部分 `v13_hybrid_*` 中可提纯的逻辑

这一层不应直接写死某个策略，而应通过策略注册表加载策略。

可作为选股分析输入的增强数据，后续可接入：

- Tushare 估值数据
- Tushare 财务指标
- 行业/板块补充字段

---

## 2.3 错误纠正（error_learning）

负责：

- 追踪选股结果表现
- 复盘错误选股
- 归因分析（因子失效、市场环境误判、数据异常等）
- 记录修正建议
- 输出下一轮策略调整建议

建议承接来源：

- `src/recommendation_tracker.py`
- `src/postmarket_analyzer.py`
- `analyze_with_deepseek.py`
- `call_deepseek_*` 中去敏后可复用的部分

这一层是“闭环层”，不是简单日志系统。它要回答：

- 为什么这次选错了
- 是数据问题、模型问题，还是市场状态变了
- 下一轮应该如何调整

---

## 2.4 回测模块（backtesting）

负责：

- 统一回测入口
- 历史绩效验证
- 因子有效性分析
- 参数比较
- 策略版本对比
- 输出回测报告

建议承接来源：

- `backtest.py`
- `src/backtest.py`
- `ic_analysis.py`
- `ic_analysis_*`
- `v12_backtest_*`

这一层的关键目标不是“保留所有版本”，而是最终收敛为：

- 一个主回测入口
- 一套统一指标输出
- 一套策略版本对比逻辑

---

## 三、策略可替换设计（核心）

这是重构中最关键的要求。

你的要求是：

> 选股策略要可以自由替换。

我建议严格按“策略实现”和“选股流程”分离设计。

## 3.1 设计原则

- 选股流程不绑定某个具体策略
- 策略配置和策略实现分离
- 策略要能注册、切换、归档
- 回测与实盘选股都从同一策略注册表读取

## 3.2 策略目录

```text
app/strategies/
├── active/
│   ├── lowvol_reversal_strategy.py
│   ├── hybrid_regime_strategy.py
│   └── ...
├── registry/
│   ├── strategies.yaml
│   └── configs/
│       ├── lowvol_reversal.yaml
│       ├── hybrid_regime.yaml
│       └── ...
└── archive/
```

## 3.3 策略注册字段建议

每个策略至少要有：

- `id`
- `display_name`
- `entrypoint`
- `config_path`
- `version`
- `status` (`active` / `experimental` / `archived`)
- `tags`
- `description`

## 3.4 策略调用方式

`stock_selection` 和 `backtesting` 都不直接 import 某个具体策略文件，而是：

1. 读取注册表
2. 选择默认策略或指定策略
3. 动态加载策略类
4. 注入配置
5. 执行统一接口

## 3.5 策略接口建议

每个策略类至少提供：

- `prepare_context(data_bundle)`
- `compute_factors(data_bundle)`
- `score(stocks)`
- `select(scored_stocks)`
- `explain(stock)`

这样能保证：

- 实盘选股和回测复用同一策略逻辑
- 错误纠正模块也能读取同一策略输出做归因

---

## 附：2026-04-27 前端设计决策补充

当前 Web 前端已明确采用以下方向：

- 站点结构收敛为：`首页 / 选股中心 / 股票详情 / 跟踪复盘 / 数据状态`
- `选股中心` 下保留 `选股结果` 与 `因子分析`
- `策略说明`、`CI说明`、`因子说明` 不单独开页面，而改为小问号 tooltip / popover 形式就地解释
- `选股中心` 主视图不直接展示 JSON，改为前端正常条目/表格 + 展开详情
- 结果列表必须包含选股时间、入选价格、最新价格、区间涨跌幅、跟踪状态等轻量复盘字段
- `最新选股跟踪` 不再作为选股中心主模块长期保留，深度表现验证保留给独立 `跟踪复盘` 页
- 正式开发优先级：先改 `/selection` 结果页与对应 API，再补 `因子分析`，最后强化 `跟踪复盘`

更完整的前端设计文档见：`docs/frontend_selection_v1.md`

---

## 四、迁移原则

## 4.1 保留（Keep）

继续作为稳定主线来源：

- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/etf_datasource.py`
- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/v12_factor_model.py`

## 4.2 迁移（Migrate）

逐步迁入新主线：

- `init_stock_basic.py`
- `daily_update_mysql.py`
- `fill_history_mysql.py`
- `src/recommendation_tracker.py`
- `src/postmarket_analyzer.py`
- `src/strategy_factory.py`
- `src/strategy_manager.py`

## 4.3 观察（Review）

先保留研究价值，不立刻迁：

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- `ic_analysis_*`
- `v12_backtest_*`
- `sentiment_*`
- `call_deepseek_*`

## 4.4 冻结/归档（Freeze/Archive）

不再作为正式入口：

- 一次性修复脚本
- 历史 dual version 脚本
- 散落的 `run_*.sh`
- 带明文 secrets 的旧自动化脚本

---

## 五、当前阶段建议

第一阶段只做四件事：

1. 固定 `shared/` 基础能力
2. 固定 MySQL 核心表
3. 跑通 `data_ingestion`
4. 建立 `stock_selection` 的策略接口骨架
5. 将 Tushare 作为第二阶段增强数据源纳入 `valuation_sync.py` / `fundamental_sync.py` 规划

先不要急着：

- 大规模清洗所有历史脚本
- 把所有回测版本合并
- 先救飞书和 AI 推送链路

---

## 六、下一步落地建议

建议按顺序做：

1. `app/stock_selection/base.py`，定义统一策略接口
2. `app/strategies/registry/strategies.yaml`，建立策略注册表
3. `app/stock_selection/selector.py`，从注册表加载策略并执行选股
4. `app/error_learning/` 定义错误归因输入输出结构
5. `app/backtesting/runner.py` 建立统一回测入口

这样做的好处是：

- 四个模块边界会很快稳定下来
- 策略替换会从设计要求变成真实能力
- 旧仓库可以逐步迁，不需要一次性重写
