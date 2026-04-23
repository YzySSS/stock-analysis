# 飞书多维表格集成 - 选股跟踪

## 功能
自动记录盘前选股，并跟踪后续3个交易日的表现

**特色功能：多策略支持**
- 每个策略版本一个独立数据表
- 未启用策略显示为「策略名（未启用）」
- 支持通过环境变量动态切换启用状态

## 文件结构
```
股票分析项目/
├── feishu_bitable_tracker.py   # 核心集成模块（支持多策略）
├── init_bitable_tables.py      # 初始化数据表脚本
├── update_pick_prices.py       # 每日更新收盘价脚本
├── test_feishu_bitable.py      # 连接测试脚本
└── main.py                     # 已集成记录功能
```

## 快速开始

### 1. 配置环境变量

编辑 `.env` 文件，添加飞书应用凭证：

```bash
# ============================================
# 飞书多维表格配置（选股跟踪）
# ============================================
# 从飞书开发者后台获取：https://open.feishu.cn/app
export FEISHU_APP_ID="cli_xxxxxxxx"
export FEISHU_APP_SECRET="xxxxxxxx"

# 启用的策略版本（逗号分隔）
export ENABLED_STRATEGIES="V10"

# 多维表格 Token（默认已配置）
export FEISHU_BITABLE_TOKEN="ZRR7bcleZanrLUsSG4zcYGoRnwh"
```

### 2. 初始化数据表

运行初始化脚本创建所有策略数据表：

```bash
cd /workspace/projects/workspace/股票分析项目
source .env
python init_bitable_tables.py
```

**输出示例：**
```
============================================================
飞书多维表格 - 策略数据表初始化
============================================================

1. 获取访问令牌...
✅ 令牌获取成功

2. 检查现有表格...

3. 创建策略数据表...
   启用的策略: V10
   🔄 创建: V9选股结果（未启用）... 成功
   🔄 创建: V10选股结果... 成功
   🔄 创建: V11选股结果（未启用）... 成功

4. 最终表格列表:
------------------------------------------------------------
   📊 V9选股结果（未启用）
   📊 V10选股结果
   📊 V11选股结果（未启用）
```

### 3. 测试连接

```bash
python test_feishu_bitable.py
```

### 4. 切换启用策略

修改 `ENABLED_STRATEGIES` 环境变量：

```bash
# 启用多个策略
export ENABLED_STRATEGIES="V9,V10"

# 仅启用V9
export ENABLED_STRATEGIES="V9"

# 仅启用V10（默认）
export ENABLED_STRATEGIES="V10"
```

重新运行初始化脚本更新表格名称：
```bash
python init_bitable_tables.py
```

## 数据表结构

每个策略一个独立数据表：
- ✅ **启用**: `V10选股结果`
- ⚪ **未启用**: `V9选股结果（未启用）`

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| 选股日期 | 日期 | 选股当天 |
| 股票代码 | 文本 | 如 000001 |
| 股票名称 | 文本 | 如 平安银行 |
| 选出时价格 | 数字 | 选股时价格 |
| 第1天收盘价 | 数字 | T+1 收盘 |
| 第2天收盘价 | 数字 | T+2 收盘 |
| 第3天收盘价 | 数字 | T+3 收盘 |
| 第1天涨幅% | 数字 | 相对选出价的涨幅 |
| 第2天涨幅% | 数字 | 相对选出价的涨幅 |
| 第3天涨幅% | 数字 | 相对选出价的涨幅 |
| 状态 | 单选 | 进行中/已完成 |

## 使用方式

### 自动记录（已集成到 main.py）

盘前选股时，只有**启用的策略**会记录到对应表格：

```python
# 自动调用，无需手动操作
from feishu_bitable_tracker import record_stock_pick

# 策略V10启用 -> 记录到「V10选股结果」表
record_stock_pick('V10', '000001', '平安银行', 10.5)

# 策略V9未启用 -> 跳过记录
record_stock_pick('V9', '000002', '万科A', 15.2)  # 不会记录
```

### 手动记录

```python
from feishu_bitable_tracker import record_stock_pick

# 记录今天的选股（只有启用状态的策略会成功）
record_stock_pick('V10', '000001', '平安银行', 10.5)

# 记录指定日期的选股
record_stock_pick('V10', '000001', '平安银行', 10.5, '2025-03-31')
```

### 更新收盘价

```python
from feishu_bitable_tracker import update_stock_prices

# 更新第1天收盘价
update_stock_prices('V10', '2025-03-31', '000001', day1=10.8)

# 批量更新
update_stock_prices('V10', '2025-03-31', '000001', 
                    day1=10.8, day2=10.6, day3=11.0)
```

## 定时任务

### 收盘后更新收盘价

添加到 crontab：
```bash
# 每天 15:35 运行（与 stock_daily_update 同时）
35 15 * * 1-5 cd /workspace/projects/workspace/股票分析项目 && python update_pick_prices.py >> logs/update_prices.log 2>&1
```

## 飞书权限配置

确保飞书应用有以下权限：
- `bitable:app` 或 `bitable:app:readonly`
- 多维表格已分享给应用（编辑权限）

### 获取 App ID 和 Secret

1. 访问 [飞书开放平台](https://open.feishu.cn/app)
2. 创建企业自建应用
3. 添加权限：`bitable:app`（多维表格管理）
4. 发布应用
5. 在「凭证与基础信息」中获取 App ID 和 Secret

### 分享多维表格给应用

1. 打开多维表格：https://my.feishu.cn/base/ZRR7bcleZanrLUsSG4zcYGoRnwh
2. 点击右上角「分享」
3. 添加应用为「可编辑」权限

## API 参考

### 获取策略状态

```python
from feishu_bitable_tracker import get_strategy_status

status = get_strategy_status()
# {'V9': False, 'V10': True, 'V11': False}
```

### 列出所有表格

```python
from feishu_bitable_tracker import list_all_tables

tables = list_all_tables()
# ['V9选股结果（未启用）', 'V10选股结果', 'V11选股结果（未启用）']
```

### 获取启用的策略

```python
from feishu_bitable_tracker import get_tracker

tracker = get_tracker()
enabled = tracker.get_enabled_strategies()
# ['V10']
```
