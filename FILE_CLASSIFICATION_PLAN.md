# FILE_CLASSIFICATION_PLAN.md

## 目标

本文件用于明确：

- 哪些文件属于后续要持续维护的主线
- 哪些文件暂时保留观察
- 哪些文件应逐步归档出主目录

核心原则：

> 后续重构不是“全仓重写”，而是先把主线从历史堆积中剥离出来。

---

## 一、分类原则

### 1.1 主线（Core）

定义：

- 后续会继续维护
- 直接服务于最小可运行股票分析内核
- 是重构后 `app/` 或统一模块结构的主要来源

### 1.2 观察区（Review）

定义：

- 当前仍可能有研究价值
- 但不是第一阶段主线
- 在未验证前先不删、不迁、不作为主入口

### 1.3 归档区（Archive）

定义：

- 历史版本
- 一次性分析脚本
- 重复或命名混乱的实验文件
- 已不适合作为当前项目入口

---

## 二、建议主线（Core）

## 2.1 数据底座主线

建议纳入主线：

- `init_history_db.py`
- `daily_update.py`
- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/etf_datasource.py`
- `src/data_source.py`
- `src/datasource_v2.py`

理由：

- 构成最小数据获取与存储闭环
- 相对独立
- 可作为本地开发、调试、回测的基础层

## 2.2 核心分析与筛选主线候选

建议纳入主线：

- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/stock_sector.py`
- `src/sector_first_screener.py`
- `src/breakout_screener.py`
- `src/dual_momentum_screener.py`
- `src/strategy_factory.py`
- `src/strategy_manager.py`
- `src/strategies/base.py`
- `src/strategies/v12_strategy.py`
- `src/v12_factor_model.py`

理由：

- 这些文件更接近“策略内核”和“筛选逻辑”
- 即使版本名较旧，也更像可提炼的逻辑资产

## 2.3 报告与结果层候选

建议纳入主线候选，但优先级次于数据层和策略层：

- `src/report_generator.py`
- `src/recommendation_tracker.py`
- `src/postmarket_analyzer.py`

理由：

- 对后续形成稳定输出有帮助
- 但不应早于主策略内核收敛

---

## 三、建议观察区（Review）

## 3.1 V13 / 市场状态相关

建议先保留观察：

- `v13_hybrid_market_detector.py`
- `v13_hybrid_optimizer.py`
- `v11_backtest_regime_v1.py`
- `v11_ic_regime_v1.py`
- `market_regime_analysis_request.md`

理由：

- 代表当前“V13/市场环境适配”的研究方向
- 但依赖远端 MySQL 和真实环境配置较多
- 不适合作为第一阶段最小主线入口

## 3.2 回测研究层

建议保留观察：

- `backtest.py`
- `backtest_simple.py`
- `backtest_premarket.py`
- `backtest_2025_full.py`
- `src/backtest.py`
- `ic_analysis.py`
- `ic_analysis_*`
- `v12_backtest_*`
- `v11_ic_parameter_optimization.py`

理由：

- 数量多，重复度高
- 含有大量研究资产
- 需要后续挑出“唯一主回测入口”后再归并

## 3.3 MySQL 数据链路

建议保留观察：

- `daily_update_mysql.py`
- `init_stock_basic.py`
- `fill_history_mysql.py`
- `backfill_*.py`
- `update_*` 中明显依赖 MySQL 的脚本
- `fill_*` 中明显依赖 MySQL 的脚本

理由：

- 很可能承载了更完整的数据流
- 但目前配置污染严重，且主线边界不清

## 3.4 AI / 新闻 / 舆情增强层

建议保留观察：

- `analyze_with_deepseek.py`
- `call_deepseek_*`
- `src/ai_sentiment_analyzer.py`
- `src/deepseek_analyzer.py`
- `src/news_provider.py`
- `src/news_filter.py`
- `src/news_credibility.py`
- `src/sentiment_factor.py`
- `sentiment_*`

理由：

- 有增强价值
- 但明显不属于第一阶段最小可运行闭环
- 需要等 secrets 和主线完成后再纳入

## 3.5 飞书与自动调度层

建议保留观察：

- `send_feishu_*`
- `stock_tracker_feishu.py`
- `intraday_scheduler.py`
- `run_intraday.sh`
- `run_postmarket.sh`
- `run_premarket_now.sh`
- `run_daily_update.sh`
- `setup_cron.sh`
- `CRON_CONFIG.md`

理由：

- 这是自动化交付层，不是策略主线
- 当前又夹带大量 secrets，短期不应优先接回

---

## 四、建议归档区（Archive）

## 4.1 历史版本主程序

建议优先归档：

- `versions/version_a/`
- `versions/version_b/`
- `versions/v10/`
- `versions/v11/`
- `README-1.0.md`
- `DUAL_VERSION_README.md`
- `run_dual_version.py`
- `run_dual_versions.py`
- `run_dual_simple.py`
- `run_v9_v11.py`

理由：

- 明确属于历史主程序
- 容易干扰当前主线判断

## 4.2 一次性分析 / 诊断 / 复盘脚本

建议逐步归档：

- `call_deepseek_analysis.py`
- `call_deepseek_ic_analysis.py`
- `call_deepseek_overfitting.py`
- `call_deepseek_regime_analysis.py`
- `analyze_yearly.py`
- `get_score_stats.py`
- `search_factors.py`
- `sample_report_with_levels.py`

理由：

- 更像临时研究工具，而非稳定模块

## 4.3 大量重复实验性回测文件

建议后续成批归档：

- `v12_backtest_v2_fixed.py`
- `v12_backtest_v3_p0.py`
- `v12_backtest_v4_2year.py`
- `v12_backtest_v4_2year_opt.py`
- `v12_backtest_v5_fixed.py`
- `v12_backtest_v6_full_opt.py`
- `v12_backtest_v7.py`
- `v12_backtest_v8.py`
- `v12_backtest_v8_fast.py`
- `v12_backtest_v8_eval.py`
- `v12_backtest_v8_run.py`
- `v12_backtest_v8_capital_curve.py`
- `v12_backtest_v9_run.py`
- `v12_backtest_v9_wan3_cost.py`
- `v12_backtest_v10_fast.py`
- `v12_backtest_v10_p0.py`
- `v12_backtest_v10_quick.py`
- `v12_backtest_v10_reconstruction.py`
- `v12_backtest_v11_2024.py`
- `v12_backtest_v11_2025.py`
- `v12_backtest_v11_2025q1.py`
- `v12_backtest_v11_2025q2.py`
- `v12_backtest_v11_2025q3.py`
- `v12_backtest_v11_2025q4.py`
- `v12_backtest_v11_batch.py`
- `v12_backtest_v11_fast.py`
- `v12_backtest_v11_final.py`
- `v12_backtest_v11_full.py`
- `v12_backtest_v11_ic_optimized.py`
- `v12_backtest_v11_vectorized.py`
- `v12_backtest_v12_market_adaptive.py`
- `v12_backtest_v12_ma_q1.py`
- `v12_backtest_stock_picker.py`

原则：

- 不立刻删除
- 先迁到 `archive/backtests/`
- 后续只选一份保留为“主回测参考实现”

## 4.4 旧 shell / cron / 运维脚本

建议后续清理归档：

- `crontab-1.0.txt`
- `crontab.config`
- `archive/crontab.txt`
- `archive/crontab-full.txt`
- `cron_backfill.sh`
- `check_*.sh`
- `run_*.sh` 中已失去主入口意义的脚本

理由：

- 当前数量过多
- 与现行主线关系弱
- 易继续传播旧配置和 secrets

---

## 五、第一批不建议动的部分

以下内容虽然不够整洁，但短期不建议急着改：

- `data_cache/`
- `backtest_results/`
- `docs/` 中研究文档
- `ARCHITECTURE_DIAGRAM.md`
- `ARCHITECTURE_V2.md`

原因：

- 它们更多是参考资产和结果产物
- 当前优先级低于主线梳理与 secrets 整改

---

## 六、建议的第一批实际动作

### Step 1
在仓库中建立明确目录：

- `archive/backtests/`
- `archive/legacy_versions/`
- `archive/one_off_scripts/`
- `archive/ops_scripts/`

### Step 2
先只迁移最明显的历史版本与重复脚本，不碰主线数据层。

### Step 3
保留一份“主线白名单”，后续仅围绕白名单做重构。

建议白名单初稿：

- `init_history_db.py`
- `daily_update.py`
- `src/stock_history_db.py`
- `src/eastmoney_datasource.py`
- `src/baostock_datasource.py`
- `src/technical_analysis.py`
- `src/stock_screener.py`
- `src/strategy_factory.py`
- `src/strategy_manager.py`
- `src/strategies/`
- `src/report_generator.py`

---

## 七、最终建议

当前不应再把整个仓库都当成“同等重要的活代码”。

正确做法是：

1. 先承认它是一个长期迭代堆起来的研究仓库
2. 给文件分层
3. 只对主线白名单持续投入维护
4. 其他内容进入观察区或归档区

一句话：

> 先建立文件边界，再做代码重构，否则后续每一步都会被历史脚本拖住。
