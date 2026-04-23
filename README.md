# 股票分析项目 V13 - 多因子选股系统

🤖 基于V13多因子模型的智能股票选股系统，专注Alpha选股策略研究与验证

---

## 🎯 项目定位

**选股策略（Alpha策略）**，核心目标是**选出会涨的股票**：
- ✅ 因子有效性验证（IC值、收益率、单调性）
- ✅ 对比不同因子组合（找出最优权重）
- ✅ 回测评估选股能力（胜率、盈亏比）
- ❌ 非交易策略（不关注仓位、止损、资金曲线）

> **策略区分**: 本项目专注选股层面，简化交易规则（固定持仓周期），纯评估选股能力

---

## 🏆 当前最优策略: V13 (3因子)

### 核心因子配置

| 因子 | 权重 | 定义 | IC验证 | 周期 |
|------|------|------|--------|------|
| **Turnover** (换手率) | 35% | 低换手率优先 | ✅ IC最高 | 日频 |
| **LowVol** (低波动) | 35% | -60日波动率 | ✅ 显著 | 60日 |
| **Reversal** (反转) | 30% | -5日收益率 | ✅ 3日最优 | 5日 |

### 关键参数

```python
{
    "选股阈值": "≥60分",
    "持仓周期": "3日 (T+1买入，T+4卖出)",
    "止损线": "-8%",
    "冷却期": "3天 (止损后)",
    "成交额门槛": "5000万",
    "最大持仓": "5只",
    "选股时间": "14:30 (收盘前)"
}
```

### 回测表现 (2年: 2024-2025)

| 指标 | 数值 | 评价 |
|------|------|------|
| 累计收益 | **+28.5%** | ✅ 正收益 |
| 年化收益 | +13.4% | ✅ 稳健 |
| 胜率 | 40.1% | ⚠️ 偏低 |
| 盈亏比 | 1.89:1 | ✅ 良好 |
| 最大回撤 | -25.3% | ⚠️ 可接受 |
| 夏普比率 | 0.68 | ⚠️ 一般 |
| 交易次数 | 412笔 | 样本充足 |

### 市场环境适配 (V13_Hybrid)

根据综合指数动态调整策略：

| 综合得分 | 市场状态 | 策略选择 | 持仓 |
|---------|---------|---------|------|
| ≥70分 | 强趋势市 | V13原版 | 5只，-8%止损 |
| 55-69分 | 弱趋势市 | V13保守版 | 3只，-8%止损 |
| 40-54分 | 震荡市 | V12精简版 | 2只，-5%止损 |
| <40分 | 熊市 | 空仓或1只 | -5%止损 |

**综合指数构成**:
- 趋势维度 (40%): 均线排列 + 趋势强度
- 波动率维度 (30%): ATR比率 + 布林带宽度
- 成交量维度 (20%): 量比 + 量价配合
- 市场宽度维度 (10%): 上涨家数比例

---

## 🗄️ 数据架构

### MySQL 数据库（腾讯云轻量）

| 表名 | 用途 | 数据规模 |
|------|------|----------|
| `stock_kline` | K线数据 | 30万+ 条 |
| `stock_basic` | 股票基础信息 | 5,400+ 只 |
| `strategies` | 策略配置 | 多版本 |
| `backtest_runs_v2` | 回测运行记录 | 按执行 |
| `backtest_daily` | 回测每日净值 | 按策略 |

### 数据源

| 优先级 | 数据源 | 用途 | 状态 |
|:------:|--------|------|------|
| 1 | **Tushare** | PE/PB/ROE等基本面数据 | ✅ 主力源 |
| 2 | **BaoStock** | 历史K线数据 | ✅ 主力源 |
| 3 | **AkShare** | 实时行情/板块数据 | ✅ 主力源 |
| 4 | **Bright Data代理** | 东方财富等受限数据源 | ✅ 备用 |

**Tushare Token**: `0faa52cf4350bede12c0cd302f5015f5a840c22ce3acb905393396a8`

---

## 🚀 快速开始

### 1. 安装依赖

```bash
# Python 3.10+
pip install pandas numpy requests pymysql sqlalchemy

# 数据源
pip install tushare baostock akshare
```

### 2. 配置环境变量

```bash
cd config
cp .env.example .env
# 编辑 .env 填入你的API密钥
source env.sh
```

**必需配置**:
```bash
# Tushare（基本面数据）
export TUSHARE_TOKEN="your-token"

# DeepSeek（策略评估）
export DEEPSEEK_API_KEY="your-api-key"

# 飞书Webhook（推送）
export FEISHU_WEBHOOK="your-webhook-url"

# MySQL 数据库
export MYSQL_HOST="your-host"
export MYSQL_USER="your-user"
export MYSQL_PASSWORD="your-password"
export MYSQL_DB="stock_analysis"
```

### 3. 初始化数据库

```bash
# 创建表结构
python3 init_database.py

# 首次数据填充
python3 daily_update.py --date $(date +%Y%m%d)
```

### 4. 运行策略

```bash
# V13 选股
python3 v13_strategy.py

# V13 回测
python3 v13_backtest.py --start 20240102 --end 20251231

# 市场环境检测
python3 v13_hybrid_market_detector.py

# 参数优化
python3 v13_hybrid_optimizer.py
```

---

## 📁 项目结构

```
股票分析项目/
├── 📄 核心策略
│   ├── v13_strategy.py                    # V13策略核心
│   ├── v13_backtest.py                    # V13回测引擎
│   ├── v13_hybrid_strategy.py             # V13混合策略
│   ├── v13_hybrid_market_detector.py      # 市场环境检测器
│   ├── v13_hybrid_optimizer.py            # 参数优化器
│   └── run_v13_hybrid_detector.sh         # 便捷运行脚本
│
├── 📄 历史策略（归档）
│   ├── v12_strategy_v10_reconstruction.py # V10重构
│   ├── v12_backtest_v10_reconstruction.py # V10回测
│   └── v9v10plus/                         # V9/V10+旧版本
│
├── 📂 src/                                # 核心模块
│   ├── stock_database.py                  # 数据库操作
│   ├── position_manager.py                # 持仓管理
│   ├── report_generator.py                # 报告生成
│   ├── tushare_datasource.py              # Tushare数据源
│   ├── baostock_datasource.py             # Baostock数据源
│   └── akshare_datasource.py              # AkShare数据源
│
├── 📂 docs/                               # 文档
│   ├── V12_PROJECT_PROGRESS.md            # 项目进展
│   ├── V12_FIX_PLAN.md                    # 修复计划
│   ├── V12_STRATEGY_RECONSTRUCTION_DEEPSEEK.md  # V10重构分析
│   ├── deepseek_evaluation_template.md    # 评估模板
│   └── v8_deepseek_evaluation.md          # V8评估
│
├── 📂 daily_reports/                      # 报告输出
│   ├── premarket/                         # 盘前报告
│   ├── intraday/                          # 盘中简报
│   └── postmarket/                        # 盘后报告
│
├── 📂 config/                             # 配置文件
│   └── env.sh                             # 环境变量
│
├── 📂 memory/                             # 迁移的记忆文件
│   └── STOCK_PROJECT_FOR_小Y.md           # 给小Y的技术细节
│
└── 📄 数据脚本
    ├── daily_update.py                    # 每日数据更新
    ├── update_pe_pb_tushare.py            # Tushare数据获取
    └── init_database.py                   # 数据库初始化
```

---

## 🧪 回测系统

### 回测配置

```python
{
    "start_date": "2024-01-02",
    "end_date": "2025-12-31",
    "factors": {
        "turnover": 0.35,     # 换手率因子
        "lowvol": 0.35,       # 低波动因子
        "reversal": 0.30      # 反转因子
    },
    "holding_period": 3,      # 持仓周期（日）
    "score_threshold": 60,    # 选股阈值
    "max_positions": 5,       # 最大持仓数
    "cost_rate": 0.0028       # 交易成本（0.28%）
}
```

### 回测输出

- 交易明细: `trades_v13_YYYYMMDD.csv`
- 汇总报告: `summary_v13_YYYYMMDD.json`
- 每日净值: `nav_v13_YYYYMMDD.csv`

### 关键指标定义

| 指标 | 计算方式 | 目标 |
|------|----------|------|
| **胜率** | 盈利交易数 / 总交易数 | > 40% |
| **盈亏比** | 平均盈利 / 平均亏损 | > 1.5 |
| **年化收益** | (1+累计收益)^(365/天数) - 1 | > 10% |
| **最大回撤** | 峰值到谷底最大跌幅 | < 30% |
| **IC值** | 因子得分与收益率相关系数 | > 0.03 |

---

## 📊 策略演进历史

| 版本 | 状态 | 收益 | 评价 |
|------|------|------|------|
| **V13** | ✅ 当前最优 | +28.5% (2年) | IC验证3因子，稳健 |
| V12_2Year | ❌ 废弃 | -34.69% (2年) | 5因子过复杂 |
| V10 | ⏸️ 暂停 | 开发中 | 4因子重构 |
| V9 | ⏸️ 归档 | - | 5因子基础版 |
| V8 | ⏸️ 归档 | - | DeepSeek 5.5/10 |
| V6 | ⏸️ 归档 | - | DeepSeek 5.5/10 |

**策略失败教训**:
1. V12_2Year: 因子过多引入噪声，Trend/Momentum在熊市失效
2. V10: 回测周期不足，过拟合风险高
3. V6/V8: 缺失市值因子，trend/momentum共线性

---

## 📝 重要决策记录

### 2026-04-14: V13确立为当前最优策略
- **决策**: 放弃V12_5因子复杂配置，回归V13并做参数优化
- **原因**: V13经过IC验证，3因子在3日持仓周期表现最优
- **表现**: 2年收益+28.5%，优于V12的-34.69%

### 2026-04-15: V13_Hybrid混合策略开发
- **思路**: 根据市场环境动态切换策略
- **互补性**: V13(牛市)+V12(震荡市)
- **检测器**: 综合指数法（趋势40%+波动30%+成交20%+宽度10%）

### 2026-04-10: 选股策略vs交易策略澄清
- **决策**: 专注选股策略（Alpha），简化交易规则
- **原则**: 固定持仓周期，不考虑止损/仓位/回撤控制
- **目的**: 纯评估选股能力，不被交易执行干扰

### 2026-04-03: 数据库架构统一
- **决策**: 所有数据统一使用腾讯云轻量MySQL
- **原因**: 避免SQLite数据分散，便于多脚本共享
- **原则**: 不再创建新的SQLite文件

---

## 🤝 团队协作

### 角色分工

| 角色 | 职责 | 对应文件 |
|------|------|----------|
| **大X** | 需求提出、决策确认、结果验证 | - |
| **小X** | 工作流协调、需求沟通、报告推送 | `memory/STOCK_PROJECT_FOR_小Y.md` |
| **小Y** | 代码开发、策略实现、回测分析 | `memory/STOCK_PROJECT_FOR_小Y.md` |

### 代码提交规范

```bash
# 功能开发
feat: V13策略核心实现

# 回测优化  
backtest: 优化查询性能

# 数据修复
data: 修复PE数据缺失

# 文档更新
docs: 更新README策略说明
```

---

## 🔧 常用命令

```bash
# 数据更新
python3 daily_update.py

# V13选股
python3 v13_strategy.py

# V13回测
python3 v13_backtest.py --start 20240102 --end 20251231

# 市场环境检测
python3 v13_hybrid_market_detector.py

# 参数优化
python3 v13_hybrid_optimizer.py

# 检查数据库
mysql -h your-host -u your-user -p stock_analysis -e "SHOW TABLES;"
```

---

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 💡 免责声明

本系统仅供学习研究，不构成投资建议。股市有风险，投资需谨慎。

---

**🎉 V13策略已就绪！**

**GitHub**: https://github.com/YzySSS/stock-analysis

**最新更新**: 2026-04-23
