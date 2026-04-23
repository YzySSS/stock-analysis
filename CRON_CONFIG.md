# 双版本定时任务配置说明

## 更新后的定时任务配置

### 1. 盘前分析（双版本）
- **时间**: 8:50 (工作日)
- **任务**: 同时运行版本A(5因子) + 版本B(V11)
- **命令**:
```bash
python3 run_dual_versions.py premarket
```
- **输出**:
  - 版本A报告: `daily_reports_version_a/premarket/`
  - 版本B报告: `daily_reports_version_b/premarket/`

### 2. 盘中简报（版本B V11）
- **时间**: 12:30 (工作日)
- **任务**: 只运行版本B(V11)
- **保持现有配置**: `股票分析1.0-盘中简报`

### 3. 盘后分析（版本B V11）
- **时间**: 15:50 (工作日)
- **任务**: 只运行版本B(V11)
- **保持现有配置**: `股票分析1.0-盘后分析`

## 配置更新命令

### 查看当前配置
```bash
openclaw cron list
```

### 更新盘前分析任务
```bash
# 删除旧任务
openclaw cron remove 股票分析1.0-盘前分析

# 添加新任务（双版本）
openclaw cron add \
  --name "股票分析-盘前双版本" \
  --schedule "50 8 * * 1-5" \
  --command "cd /workspace/projects/workspace/股票分析项目 && python3 run_dual_versions.py premarket" \
  --session isolated
```

### 验证配置
```bash
openclaw cron list
```

## 报告目录结构

```
daily_reports_version_a/          # 版本A (5因子)
├── premarket/                    # 盘前报告
├── intraday/                     # (不使用)
└── postmarket/                   # (不使用)

daily_reports_version_b/          # 版本B (V11)
├── premarket/                    # 盘前报告
├── intraday/                     # 盘中报告
└── postmarket/                   # 盘后报告
```

## 飞书推送

每个报告都会独立推送到飞书：
- 🌅 盘前选股报告 - 版本A(5因子)
- 🌅 盘前选股报告 - 版本B(V11)
- 📊 午间简报 - 版本B(V11)
- 🌇 盘后复盘 - 版本B(V11)
