# 回测系统使用指南

## 概述

为了验证V10选股策略的有效性，我们建立了完整的回测数据体系。

## 数据库文件

| 数据库 | 文件路径 | 用途 |
|--------|---------|------|
| 回测数据库 | `src/data_cache/backtest.db` | 存储回测记录和绩效 |
| 历史数据库 | `src/data_cache/stock_history.db` | 存储60天历史价格 |
| 舆情数据库 | `src/data_cache/sentiment_cache.db` | 存储舆情数据 |

## 回测表结构

### 1. backtest_records - 选股记录表

记录每次选股的具体结果和因子得分。

```sql
CREATE TABLE backtest_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,                    -- 回测批次ID（如：20240324_premarket）
    run_date TEXT NOT NULL,                  -- 回测日期
    strategy_version TEXT NOT NULL,          -- 策略版本（如：v1.0_v10_full）
    mode TEXT NOT NULL,                      -- premarket/noon/postmarket
    
    -- 选股信息
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    rank INTEGER,                            -- 选股排名
    
    -- 因子得分（用于分析哪些因子有效）
    total_score REAL,                        -- 总评分
    technical_score REAL,                    -- 技术因子得分
    sentiment_score REAL,                    -- 情绪因子
    sector_score REAL,                       -- 板块因子
    money_flow_score REAL,                   -- 资金因子
    risk_score REAL,                         -- 风险因子
    consensus_score REAL,                    -- 一致预期
    news_sentiment_score REAL,               -- 舆情因子
    
    -- 选股时的市场数据
    entry_price REAL,                        -- 入选价格
    entry_change_pct REAL,                   -- 入选时涨跌幅
    market_status TEXT,                      -- 市场环境
    
    -- 所属板块
    sector TEXT,
    is_sector_leader INTEGER,                -- 是否板块龙头
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. backtest_performance - 绩效表

记录选股后的实际收益表现。

```sql
CREATE TABLE backtest_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    run_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    
    -- 次日表现（T+1）
    entry_price REAL,                        -- 买入价
    next_day_open REAL,                      -- 次日开盘价
    next_day_high REAL,                      -- 次日最高价
    next_day_low REAL,                       -- 次日最低价
    next_day_close REAL,                     -- 次日收盘价
    next_day_change_pct REAL,                -- 次日涨跌幅
    
    -- 5日表现（T+5）
    day5_close REAL,                         -- 5日后收盘价
    day5_change_pct REAL,                    -- 5日涨跌幅
    
    -- 风险指标
    max_drawdown REAL,                       -- 期间最大回撤
    
    -- 相对大盘
    index_change_pct REAL,                   -- 同期大盘涨跌幅
    alpha REAL,                              -- 超额收益
    
    -- 胜率标记
    is_win INTEGER                           -- 1=盈利, 0=亏损
);
```

### 3. strategy_versions - 策略版本表

记录策略的变更历史，方便对比不同版本。

```sql
CREATE TABLE strategy_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT NOT NULL UNIQUE,            -- 版本号
    version_name TEXT,                       -- 版本名称
    description TEXT,                        -- 版本描述
    weights TEXT,                            -- JSON格式因子权重
    threshold INTEGER,                       -- 选股阈值
    max_picks INTEGER,                       -- 最大选股数
    changes TEXT,                            -- 变更说明
    created_at TIMESTAMP
);
```

### 4. backtest_summary - 汇总统计表

按批次自动汇总统计。

```sql
CREATE TABLE backtest_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    run_date TEXT NOT NULL,
    strategy_version TEXT,
    mode TEXT,
    
    -- 选股统计
    total_picks INTEGER,                     -- 选股总数
    avg_score REAL,                          -- 平均得分
    
    -- 次日胜率
    next_day_win_rate REAL,                  -- 次日胜率
    next_day_avg_return REAL,                -- 次日平均收益
    next_day_max_return REAL,                -- 次日最大收益
    next_day_min_return REAL,                -- 次日最小收益
    
    -- 5日胜率
    day5_win_rate REAL,                      -- 5日胜率
    day5_avg_return REAL,                    -- 5日平均收益
    
    -- 相对大盘
    avg_alpha REAL,                          -- 平均超额收益
    
    -- 风险指标
    avg_max_drawdown REAL,                   -- 平均最大回撤
    
    -- 其他
    sector_distribution TEXT,                -- JSON格式板块分布
    market_status TEXT                       -- 市场环境
);
```

## 回测流程

### 阶段1：记录选股结果（每日运行）

在盘前/盘中选股完成后，自动将选股结果写入 `backtest_records` 表：

```python
# 伪代码
for pick in selected_stocks:
    insert_into_backtest_records(
        run_id=f"{date}_{mode}",           # 如：20240324_premarket
        run_date=date,
        strategy_version="v1.0_v10_full",   -- 当前策略版本
        mode="premarket",                   -- premarket/noon/postmarket
        stock_code=pick['code'],
        stock_name=pick['name'],
        rank=pick['rank'],
        total_score=pick['factors']['total'],
        technical_score=pick['factors']['technical'],
        sentiment_score=pick['factors']['sentiment'],
        sector_score=pick['factors']['sector'],
        money_flow_score=pick['factors']['money_flow'],
        risk_score=pick['factors']['risk'],
        consensus_score=pick['factors']['consensus'],
        news_sentiment_score=pick['factors']['news_sentiment'],
        entry_price=pick['price'],
        entry_change_pct=pick['change_pct'],
        market_status=market_context['reason'],
        sector=pick['sector'],
        is_sector_leader=pick['is_sector_leader']
    )
```

### 阶段2：获取实际收益（次日运行）

每天收盘后，获取前一日选股股票的实际收益：

```python
# 伪代码
for record in yesterday_records:
    # 获取次日数据
    next_day_data = get_stock_history(record['stock_code'], days=5)
    
    # 计算收益
    entry_price = record['entry_price']
    next_day_open = next_day_data[1]['open']
    next_day_high = next_day_data[1]['high']
    next_day_low = next_day_data[1]['low']
    next_day_close = next_day_data[1]['close']
    next_day_change = (next_day_close - entry_price) / entry_price * 100
    
    # 5日收益
    day5_close = next_day_data[5]['close']
    day5_change = (day5_close - entry_price) / entry_price * 100
    
    # 最大回撤
    max_drawdown = calculate_max_drawdown(next_day_data[:6], entry_price)
    
    # 相对大盘
    index_change = get_index_change(record['run_date'], days=1)
    alpha = next_day_change - index_change
    
    # 是否盈利
    is_win = 1 if next_day_change > 0 else 0
    
    insert_into_backtest_performance(...)
```

### 阶段3：生成汇总统计

自动汇总每个批次的绩效：

```python
# 伪代码
for run_id in unique_run_ids:
    records = get_performance_by_run_id(run_id)
    
    summary = {
        'run_id': run_id,
        'total_picks': len(records),
        'next_day_win_rate': sum(r['is_win'] for r in records) / len(records),
        'next_day_avg_return': sum(r['next_day_change_pct'] for r in records) / len(records),
        'avg_alpha': sum(r['alpha'] for r in records) / len(records),
        ...
    }
    
    insert_into_backtest_summary(summary)
```

## 分析维度

### 1. 整体胜率分析

```sql
-- 统计所有选股的历史胜率
SELECT 
    strategy_version,
    mode,
    COUNT(*) as total_picks,
    AVG(is_win) as win_rate,
    AVG(next_day_change_pct) as avg_return
FROM backtest_records r
JOIN backtest_performance p ON r.run_id = p.run_id AND r.stock_code = p.stock_code
GROUP BY strategy_version, mode;
```

### 2. 因子有效性分析

```sql
-- 分析哪些因子得分高的股票表现更好
SELECT 
    CASE 
        WHEN technical_score > 15 THEN 'high_technical'
        WHEN sector_score > 25 THEN 'high_sector'
        WHEN news_sentiment_score > 5 THEN 'high_sentiment'
        ELSE 'others'
    END as factor_group,
    AVG(p.next_day_change_pct) as avg_return,
    AVG(p.is_win) as win_rate
FROM backtest_records r
JOIN backtest_performance p ON r.run_id = p.run_id AND r.stock_code = p.stock_code
GROUP BY factor_group;
```

### 3. 板块胜率分析

```sql
-- 分析不同板块的选股胜率
SELECT 
    r.sector,
    COUNT(*) as pick_count,
    AVG(p.next_day_change_pct) as avg_return,
    AVG(p.is_win) as win_rate
FROM backtest_records r
JOIN backtest_performance p ON r.run_id = p.run_id AND r.stock_code = p.stock_code
GROUP BY r.sector
ORDER BY avg_return DESC;
```

### 4. 市场环境分析

```sql
-- 分析不同市场环境下的策略表现
SELECT 
    r.market_status,
    COUNT(*) as pick_count,
    AVG(p.next_day_change_pct) as avg_return,
    AVG(p.is_win) as win_rate
FROM backtest_records r
JOIN backtest_performance p ON r.run_id = p.run_id AND r.stock_code = p.stock_code
GROUP BY r.market_status;
```

## 关键指标说明

| 指标 | 说明 | 目标值 |
|------|------|--------|
| 次日胜率 | 选股次日盈利的概率 | > 55% |
| 次日平均收益 | 选股次日的平均涨跌幅 | > 1% |
| 5日胜率 | 持有5天后盈利的概率 | > 50% |
| 平均Alpha | 相对大盘的超额收益 | > 2% |
| 最大回撤 | 期间最大亏损幅度 | < 5% |

## 策略优化方向

基于回测数据，可以优化：

1. **因子权重调整** - 根据因子有效性调整权重
2. **阈值优化** - 找到最优选股阈值
3. **板块偏好** - 聚焦胜率高的板块
4. **市场环境适配** - 不同市场使用不同策略

## 后续计划

**Phase 4**：
- 开发回测数据自动收集脚本
- 运行1-2周收集数据
- 分析因子有效性
- 优化策略参数

**Phase 5**：
- 小资金实盘测试
- 持续跟踪记录
- 迭代优化策略
