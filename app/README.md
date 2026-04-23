# app/

这是股票分析项目的新模块化承接骨架。

目标不是一次性重写整个仓库，而是逐步把现有主线逻辑迁入这里，最终形成：

- 数据接入
- 选股分析
- 错误汲取
- 回测验证
- 编排调度
- 策略可插拔切换

## 目录说明

- `data_ingestion/` 数据接入与标准化
- `stock_selection/` 选股分析与打分流程
- `error_learning/` 复盘、错误归因、经验沉淀
- `backtesting/` 回测、因子验证、策略比较
- `orchestration/` 调度、配置装配、任务入口
- `shared/` 共享配置、工具、数据结构
- `strategies/` 独立策略存储、注册与归档

## 策略目录

策略被独立放在：

- `strategies/active/` 当前可用策略
- `strategies/registry/` 策略注册表
- `strategies/archive/` 历史或废弃策略

后续选股与回测都应尽量从注册表读取策略入口，而不是把策略实现散落在各脚本里。
