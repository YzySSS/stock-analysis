# 双版本并行运行说明

## 版本说明

### 版本A (5因子模型)
- **代码**: `versions/version_a/`
- **报告目录**: `daily_reports_version_a/`
- **因子权重**:
  - 技术: 25%
  - 情绪: 20%
  - 板块: 30%
  - 资金: 15%
  - 风险: 10%
- **特点**: 选出珠海中富的早期版本，简单直观

### 版本B (V11最新版)
- **代码**: `versions/version_b/`
- **报告目录**: `daily_reports_version_b/`
- **因子权重**: 动态调整
- **特点**: 包含技术位分析、动态阈值、舆情因子、ETF数据等

## 运行方式

### 手动运行

```bash
# 同时运行两个版本（盘前）
python3 run_dual_versions.py premarket

# 同时运行两个版本（盘中）
python3 run_dual_versions.py noon

# 同时运行两个版本（盘后）
python3 run_dual_versions.py postmarket
```

### 单独运行

```bash
# 只运行版本A
python3 versions/version_a/run_version_a.py --mode premarket

# 只运行版本B
python3 versions/version_b/run_version_b.py --mode premarket
```

## 定时任务配置

当前定时任务已配置，每天会自动运行：

- **8:50** - 盘前分析（双版本）
- **12:30** - 盘中简报（双版本）
- **15:50** - 盘后分析（双版本）

## 报告查看

### 版本A报告
- 盘前: `daily_reports_version_a/premarket/`
- 盘中: `daily_reports_version_a/intraday/`
- 盘后: `daily_reports_version_a/postmarket/`

### 版本B报告
- 盘前: `daily_reports_version_b/premarket/`
- 盘中: `daily_reports_version_b/intraday/`
- 盘后: `daily_reports_version_b/postmarket/`

## 飞书推送

两个版本的报告都会分别推送到飞书：
- 版本A: 🌅 盘前选股报告 - 版本A(5因子)
- 版本B: 🌅 盘前选股报告 - 版本B(V11)

方便对比两个版本的选股效果。
