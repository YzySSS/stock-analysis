# stock_selection/

该模块负责：

- 从策略注册表选择当前启用策略
- 组织输入数据
- 调用策略执行选股
- 输出标准化选股结果

## 当前状态

当前已经具备：

1. 策略注册表
2. 动态策略加载
3. 首个运行入口 `run_selection.py`

## 示例命令

使用默认策略运行：

```bash
python -m app.stock_selection.run_selection
```

指定策略运行：

```bash
python -m app.stock_selection.run_selection --strategy lowvol_reversal
```

指定日期和股票池：

```bash
python -m app.stock_selection.run_selection \
  --strategy lowvol_reversal \
  --date 2026-04-24 \
  --universe 000001.SZ,000002.SZ,600519.SH
```

## 说明

当前版本先使用 demo 特征输入，后续应接入：

- 数据接入模块
- 腾讯云 MySQL
- 真实三因子计算
- 结果落库 / 输出模块
