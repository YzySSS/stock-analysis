# 股票分析项目 1.0

## 版本信息
- **版本**: 1.0
- **代号**: V8V10Plus
- **整合**: V8选股 + V10+报告

## 核心功能

### V8 选股器
- 全A股筛选（默认100只核心股，可扩展至4914只）
- 多因子评分系统：
  - 技术因子 (25%)
  - 情绪因子 (25%)
  - 行业轮动因子 (25%)
  - 风险因子 (25%)
- 板块轮动策略

### V10+ 报告生成器
- 盘前选股报告
- 盘后深度复盘
- AI诊断分析（需配置API Key）
- 策略改进建议

## 文件结构

```
股票分析项目/
├── main.py                 # 主入口 (1.0版本)
├── intraday_scheduler.py   # 定时任务调度器
├── crontab-1.0.txt        # 定时任务配置
├── src/                   # 源代码
│   └── (保留必要模块)
├── daily_reports/         # 报告存储
│   ├── premarket/
│   ├── intraday/
│   ├── postmarket/
│   └── summary/
├── archive/               # 归档旧版本
└── README-1.0.md         # 本文档
```

## 使用方法

### 手动运行
```bash
# 盘前分析
python3 main.py --mode premarket --top 5

# 盘后分析
python3 main.py --mode postmarket

# 午间简报
python3 main.py --mode noon

# 不发送推送
python3 main.py --mode premarket --no-send
```

### 定时任务
```bash
# 安装定时任务
crontab crontab-1.0.txt

# 查看定时任务
crontab -l
```

### 定时任务时间表
| 时间 | 任务 | 功能 |
|------|------|------|
| 8:50 | 盘前分析 | V8选股 + V10+报告 |
| 12:30 | 盘中简报 | 上午收盘总结 + 下午选股分析 |
| 15:50 | 盘后分析 | V10+深度复盘 |

## 环境变量

```bash
# 必需
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"

# 可选（用于AI分析）
export DEEPSEEK_API_KEY="sk-xxxxx"
export DEEPSEEK_BASE_URL="https://api.deepseek.com/v1"
export AI_MODEL="deepseek-chat"
```

## 版本历史

### 1.0 (当前)
- ✅ 整合 V8 选股器
- ✅ 整合 V10+ 报告生成器
- ✅ 统一代码架构
- ✅ 简化定时任务

### 旧版本（已归档）
- V1-V4: 早期分析系统
- V8: 板块优先选股器（独立版本）
- V10+: 盘后报告增强版（独立版本）

## 待优化

- [ ] 扩展股票池至4914只全A股
- [ ] 接入聚宽财务数据
- [ ] 完善AI诊断功能
- [ ] 添加回测验证
- [ ] 支持策略YAML配置

## 作者
OpenClaw AI 量化团队
