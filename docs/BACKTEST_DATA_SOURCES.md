# 回测系统数据源说明

## 概述

回测系统使用**3个数据库**协同工作，分别负责：历史数据、舆情数据、回测记录。

```
┌─────────────────────────────────────────────────────────────────┐
│                        回测数据源架构                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────┐    ┌─────────────────────┐             │
│  │ 历史数据库          │    │ 舆情数据库          │             │
│  │ stock_history.db    │    │ sentiment_cache.db  │             │
│  ├─────────────────────┤    ├─────────────────────┤             │
│  │ stock_prices        │    │ sentiment_cache     │             │
│  │ - 60天历史价格      │    │ - 情感得分          │             │
│  │ - 用于验证收益      │    │ - 新闻数量          │             │
│  │ - 计算技术指标      │    │ - 用于舆情因子      │             │
│  └──────────┬──────────┘    └──────────┬──────────┘             │
│             │                          │                        │
│             └────────────┬─────────────┘                        │
│                          │                                       │
│                          ▼                                       │
│  ┌─────────────────────────────────────────┐                    │
│  │           选股 + 回测流程                │                    │
│  │                                          │                    │
│  │  1. 从API获取实时行情                    │                    │
│  │  2. 从stock_history读取历史数据          │                    │
│  │  3. 从sentiment_cache读取舆情因子        │                    │
│  │  4. V10评分选股                          │                    │
│  │  5. 写入backtest_records                 │                    │
│  │                                          │                    │
│  └────────────────────┬────────────────────┘                    │
│                       │                                          │
│                       ▼                                          │
│  ┌─────────────────────────────────────────┐                    │
│  │         回测数据库                       │                    │
│  │         backtest.db                      │                    │
│  ├─────────────────────────────────────────┤                    │
│  │ backtest_records    - 选股记录           │                    │
│  │ backtest_performance- 实际收益           │                    │
│  │ backtest_summary    - 汇总统计           │                    │
│  │ strategy_versions   - 策略版本           │                    │
│  └─────────────────────────────────────────┘                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 详细数据源说明

### 1️⃣ 历史数据库（stock_history.db）

**用途：** 提供历史价格数据，用于验证选股后的实际收益

**数据表：**

| 表名 | 用途 | 回测使用场景 |
|------|------|-------------|
| `stock_prices` | 存储60天历史价格 | 计算次日/5日收益、技术指标验证 |
| `last_update` | 记录最后更新时间 | 数据完整性检查 |

**回测使用示例：**
```sql
-- 查询某股票在选股后5天的价格变化
SELECT 
    date, close_price,
    (close_price - LAG(close_price, 1) OVER (ORDER BY date)) / LAG(close_price, 1) OVER (ORDER BY date) * 100 as daily_change
FROM stock_prices 
WHERE code = '000001' 
  AND date >= '2024-03-01'
ORDER BY date 
LIMIT 5;
```

**数据更新：**
- 频率：每日05:00
- 来源：Baostock
- 范围：5093只股票 × 60天

---

### 2️⃣ 舆情数据库（sentiment_cache.db）

**用途：** 提供选股时的舆情因子数据

**数据表：**

| 表名 | 用途 | 回测使用场景 |
|------|------|-------------|
| `sentiment_cache` | 存储情感得分和新闻数量 | V10评分中的舆情因子（7%权重） |

**字段说明：**
```sql
SELECT 
    code,                    -- 股票代码
    date,                    -- 日期
    sentiment_score,         -- 情感得分 (-10 ~ +10)
    news_count,              -- 新闻数量
    credibility_avg,         -- 平均可信度 (0-1)
    cached_at                -- 缓存时间
FROM sentiment_cache
WHERE date = '2024-03-24';
```

**数据更新：**
- 频率：每小时
- 来源：Coze Web Search + 本地情感分析
- 范围：全A股（5491只）

**回测价值：**
- 验证舆情因子对选股的影响
- 分析不同情感得分区间的胜率
- 优化舆情因子权重

---

### 3️⃣ 回测数据库（backtest.db）⭐ 核心

**用途：** 存储回测记录、实际收益、汇总统计

**数据表：**

#### 表1: backtest_records - 选股记录
**用途：** 记录每次选股的具体结果

```sql
-- 查询某天盘前选股的所有记录
SELECT 
    stock_code,
    stock_name,
    rank,
    total_score,
    technical_score,      -- 技术因子
    sentiment_score,      -- 情绪因子
    sector_score,         -- 板块因子
    money_flow_score,     -- 资金因子
    risk_score,           -- 风险因子
    consensus_score,      -- 一致预期
    news_sentiment_score, -- 舆情因子
    entry_price,          -- 入选价格
    sector,               -- 所属板块
    is_sector_leader      -- 是否板块龙头
FROM backtest_records
WHERE run_date = '2024-03-24' 
  AND mode = 'premarket'
ORDER BY rank;
```

#### 表2: backtest_performance - 实际收益
**用途：** 记录选股后的实际收益表现

**数据来源：** 从 stock_history.db 计算得出

```sql
-- 查询某批次选股的实际收益
SELECT 
    b.stock_code,
    b.stock_name,
    b.total_score,                    -- 选股时评分
    p.next_day_change_pct,            -- 次日涨跌幅
    p.day5_change_pct,                -- 5日涨跌幅
    p.max_drawdown,                   -- 最大回撤
    p.alpha,                          -- 超额收益
    p.is_win                          -- 是否盈利
FROM backtest_records b
LEFT JOIN backtest_performance p 
    ON b.run_id = p.run_id 
   AND b.stock_code = p.stock_code
WHERE b.run_date = '2024-03-24'
  AND b.mode = 'premarket';
```

**数据计算逻辑：**
```python
# 伪代码
next_day_change = (next_day_close - entry_price) / entry_price * 100
alpha = next_day_change - index_change
is_win = 1 if next_day_change > 0 else 0
```

#### 表3: backtest_summary - 汇总统计
**用途：** 按批次自动汇总统计

```sql
-- 查询策略整体表现
SELECT 
    run_date,
    strategy_version,
    mode,
    total_picks,                      -- 选股总数
    next_day_win_rate,                -- 次日胜率
    next_day_avg_return,              -- 次日平均收益
    day5_win_rate,                    -- 5日胜率
    avg_alpha,                        -- 平均超额收益
    avg_max_drawdown                  -- 平均最大回撤
FROM backtest_summary
ORDER BY run_date DESC
LIMIT 30;
```

#### 表4: strategy_versions - 策略版本
**用途：** 记录策略变更历史

```sql
-- 对比不同版本的胜率
SELECT 
    sv.version_name,
    sv.weights,                       -- JSON格式因子权重
    AVG(bs.next_day_win_rate) as avg_win_rate,
    AVG(bs.next_day_avg_return) as avg_return
FROM strategy_versions sv
JOIN backtest_summary bs ON sv.version = bs.strategy_version
GROUP BY sv.version
ORDER BY avg_win_rate DESC;
```

---

## 数据流向图

```
每日运行流程:

05:00 ──► 更新历史数据 ──► stock_history.db
  │
  ├───► stock_prices (新增昨日收盘价)
  │
  └───► last_update (更新最后日期)

每小时 ──► 更新舆情数据 ──► sentiment_cache.db
  │
  └───► sentiment_cache (全A股5491只)

08:50 ──► 盘前选股 ──► backtest.db
  │
  ├───► 读取 stock_history.db (历史数据)
  │
  ├───► 读取 sentiment_cache.db (舆情因子)
  │
  ├───► 腾讯财经API (实时行情)
  │
  └───► 写入 backtest_records (选股记录)

次日收盘后 ──► 计算收益 ──► backtest.db
  │
  ├───► 读取 stock_history.db (验证价格)
  │
  └───► 写入 backtest_performance (实际收益)
  │
  └───► 写入 backtest_summary (汇总统计)
```

---

## 典型查询示例

### 1. 查询策略整体胜率
```sql
-- 统计近30天盘前选股的胜率
SELECT 
    COUNT(*) as total_picks,
    AVG(p.is_win) as win_rate,
    AVG(p.next_day_change_pct) as avg_return,
    AVG(p.alpha) as avg_alpha
FROM backtest_records r
JOIN backtest_performance p 
    ON r.run_id = p.run_id AND r.stock_code = p.stock_code
WHERE r.mode = 'premarket'
  AND r.run_date >= date('now', '-30 days');
```

### 2. 分析因子有效性
```sql
-- 分析舆情因子高分股票的表现
SELECT 
    CASE 
        WHEN r.news_sentiment_score >= 5 THEN '高舆情分(>=5)'
        WHEN r.news_sentiment_score >= 0 THEN '中舆情分(0-5)'
        ELSE '低舆情分(<0)'
    END as sentiment_group,
    COUNT(*) as pick_count,
    AVG(p.next_day_change_pct) as avg_return,
    AVG(p.is_win) as win_rate
FROM backtest_records r
JOIN backtest_performance p 
    ON r.run_id = p.run_id AND r.stock_code = p.stock_code
GROUP BY sentiment_group
ORDER BY avg_return DESC;
```

### 3. 板块胜率分析
```sql
-- 分析不同板块的选股胜率
SELECT 
    r.sector,
    COUNT(*) as pick_count,
    AVG(p.next_day_change_pct) as avg_return,
    SUM(p.is_win) as win_count,
    ROUND(AVG(p.is_win) * 100, 2) as win_rate
FROM backtest_records r
JOIN backtest_performance p 
    ON r.run_id = p.run_id AND r.stock_code = p.stock_code
WHERE r.run_date >= date('now', '-30 days')
GROUP BY r.sector
HAVING pick_count >= 5
ORDER BY avg_return DESC;
```

---

## 总结

| 数据库 | 核心表 | 回测作用 |
|--------|--------|---------|
| **stock_history.db** | stock_prices | 提供历史价格，验证收益 |
| **sentiment_cache.db** | sentiment_cache | 提供舆情因子，选股评分 |
| **backtest.db** | backtest_records | 记录选股结果 |
| **backtest.db** | backtest_performance | 记录实际收益 |
| **backtest.db** | backtest_summary | 汇总统计数据 |

**3个数据库协同工作，形成完整的回测验证体系！** 🎯
