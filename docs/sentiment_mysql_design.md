# 舆情数据更新设计方案 (MySQL版)

## 1. 架构决策

**核心原则**: 所有数据统一使用 **腾讯云轻量MySQL**，不再使用SQLite

---

## 2. 数据表设计

### 2.1 主表: sentiment_daily (每日舆情数据)

```sql
CREATE TABLE IF NOT EXISTS sentiment_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
    code VARCHAR(10) NOT NULL COMMENT '股票代码',
    trade_date DATE NOT NULL COMMENT '交易日期',
    
    -- 核心舆情指标
    sentiment_score DECIMAL(5,2) DEFAULT 0 COMMENT '情感分数 -10~+10',
    sentiment_type TINYINT DEFAULT 0 COMMENT '情感类型: 0=中性, 1=正面, 2=负面',
    
    -- 新闻统计
    news_count INT DEFAULT 0 COMMENT '新闻总数',
    positive_news INT DEFAULT 0 COMMENT '正面新闻数',
    negative_news INT DEFAULT 0 COMMENT '负面新闻数',
    neutral_news INT DEFAULT 0 COMMENT '中性新闻数',
    
    -- 质量指标
    credibility_avg DECIMAL(3,2) DEFAULT 0.50 COMMENT '平均可信度 0~1',
    heat_score INT DEFAULT 0 COMMENT '热度分 0~100',
    
    -- 详细数据(JSON存储)
    top_keywords JSON COMMENT '关键词TOP10',
    sources_distribution JSON COMMENT '新闻来源分布',
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    UNIQUE KEY uk_code_date (code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_code (code),
    INDEX idx_sentiment_type (trade_date, sentiment_type),
    INDEX idx_heat (trade_date, heat_score)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日舆情数据表';
```

### 2.2 辅助表: sentiment_news_detail (新闻明细)

```sql
CREATE TABLE IF NOT EXISTS sentiment_news_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(10) NOT NULL COMMENT '股票代码',
    trade_date DATE NOT NULL COMMENT '交易日期',
    
    news_title VARCHAR(500) COMMENT '新闻标题',
    news_content TEXT COMMENT '新闻内容摘要',
    source VARCHAR(100) COMMENT '新闻来源',
    publish_time DATETIME COMMENT '发布时间',
    sentiment_score DECIMAL(4,2) DEFAULT 0 COMMENT '单条情感分 -1~+1',
    credibility DECIMAL(3,2) DEFAULT 0.5 COMMENT '可信度',
    url VARCHAR(500) COMMENT '原文链接',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_code_date (code, trade_date),
    INDEX idx_publish_time (publish_time)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '舆情新闻明细表';
```

### 2.3 日志表: sentiment_update_log (更新日志)

```sql
CREATE TABLE IF NOT EXISTS sentiment_update_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    update_date DATE NOT NULL COMMENT '更新日期',
    update_type VARCHAR(20) COMMENT '更新类型: daily=每日, batch=批量, backfill=回填',
    
    total_stocks INT DEFAULT 0 COMMENT '计划更新股票数',
    success_count INT DEFAULT 0 COMMENT '成功数',
    failed_count INT DEFAULT 0 COMMENT '失败数',
    skip_count INT DEFAULT 0 COMMENT '跳过数(已有数据)',
    
    start_time TIMESTAMP COMMENT '开始时间',
    end_time TIMESTAMP COMMENT '结束时间',
    duration_seconds INT COMMENT '耗时秒数',
    
    status VARCHAR(20) COMMENT '状态: success/partial/failed/skipped',
    message TEXT COMMENT '备注信息',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_update_date (update_date),
    INDEX idx_update_type (update_type)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '舆情更新日志表';
```

---

## 3. 数据来源

| 数据类型 | 来源 | 更新频率 | 说明 |
|---------|------|---------|------|
| 财经新闻 | AkShare/Coze搜索 | 每日 | 按股票代码搜索相关新闻 |
| 情感分析 | 本地关键词匹配 | 实时 | 不依赖外部API，稳定可靠 |
| 新闻可信度 | 规则引擎 | 实时 | 根据来源自动打分 |

---

## 4. 更新策略

### 4.1 每日更新 (sentiment_daily_update.py)

**执行时间**: 每天 22:30 (周一到周五)

**逻辑流程**:
1. 检查今天是否为交易日
2. 从 stock_basic 获取全A股列表（非退市）
3. 检查 sentiment_daily 表今天是否已有数据
4. 对无数据的股票，搜索当日新闻并分析情感
5. 批量写入 MySQL
6. 记录更新日志

**优先级**:
- 优先更新活跃股票（成交量大、波动大）
- 其次更新持仓股票
- 最后更新全市场

### 4.2 实时更新 (sentiment_realtime.py) - 可选

**触发条件**: 选股前调用

**逻辑**:
- 检查目标股票今日数据是否存在
- 如不存在，实时获取并更新
- 用于盘前/盘中选股时获取最新舆情

### 4.3 历史回填 (sentiment_history_fill.py)

**用途**: 补充历史日期的舆情数据

**逻辑**:
- 指定日期范围
- 使用历史新闻或模拟数据填充
- 用于回测和策略验证

---

## 5. 脚本设计

### 脚本1: sentiment_daily_update.py (每日更新)

```python
# 核心功能
- 连接MySQL数据库
- 获取全A股列表（stock_basic）
- 获取今日已有舆情数据的股票（去重）
- 分批处理（每批100只）
- 每只股票：
  1. 搜索今日新闻（AkShare/Coze）
  2. 情感分析（本地关键词匹配）
  3. 可信度评估
  4. 写入sentiment_daily表
- 输出结构化报告
```

### 脚本2: sentiment_history_fill.py (历史回填)

```python
# 核心功能
- 指定日期范围（如 2026-01-01 到 2026-03-31）
- 获取历史新闻数据或生成模拟数据
- 批量填充到sentiment_daily表
- 用于补全历史数据或回测
```

### 脚本3: sentiment_stats.py (统计分析)

```python
# 核心功能
- 查询指定日期的舆情统计
- 正面/负面股票列表
- 舆情热度排行
- 输出飞书报告
```

---

## 6. 定时任务配置

```cron
# 每日舆情数据更新 - 22:30 (周一到周五)
# 自动检查是否为交易日，非交易日跳过
30 22 * * 1-5 cd /root/.openclaw/workspace/股票分析项目 && python3 sentiment_daily_update.py >> /tmp/cron_sentiment_daily.log 2>&1

# 舆情统计报告 - 每天 23:00 (周一到周五)
0 23 * * 1-5 cd /root/.openclaw/workspace/股票分析项目 && python3 sentiment_stats.py >> /tmp/cron_sentiment_stats.log 2>&1
```

---

## 7. 与选股系统的集成

### V10因子评分中的舆情因子

```python
# 在 sentiment_factor.py 中修改
# 从MySQL读取而非SQLite

def get_sentiment_factor(code: str, date: str) -> Dict:
    """从MySQL获取舆情因子得分"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.execute('''
        SELECT sentiment_score, news_count, heat_score 
        FROM sentiment_daily 
        WHERE code = %s AND trade_date = %s
    ''', (code, date))
    row = cursor.fetchone()
    
    if row:
        return {
            'score': row[0],      # -10 ~ +10
            'news_count': row[1],
            'heat_score': row[2]
        }
    else:
        # 无数据返回中性
        return {'score': 0, 'news_count': 0, 'heat_score': 0}
```

### 权重分配

- **舆情因子权重**: 5% (V10多因子评分)
- **评分范围**: -10 ~ +10
- **影响因素**:
  - 情感分数 (正面/负面)
  - 新闻数量 (热度)
  - 可信度 (来源质量)

---

## 8. 现有数据迁移

### 从SQLite迁移到MySQL

```python
# 迁移脚本: migrate_sentiment_to_mysql.py

# 1. 读取SQLite sentiment_cache.db
# 2. 转换数据格式
# 3. 写入MySQL sentiment_daily表
# 4. 验证数据完整性
```

**当前SQLite数据**: 996条 (2026-04-02)
**目标**: 迁移到MySQL sentiment_daily表

---

## 9. 数据质量监控

### 每日检查项

- [ ] 更新股票数是否达标（>90%全市场）
- [ ] 正面/负面股票比例是否合理（通常70%中性）
- [ ] 新闻数量异常检测（某只股票新闻数>50需复核）
- [ ] 更新耗时是否正常（<30分钟）

### 告警规则

- 更新成功率 < 80% → 飞书告警
- 连续3天无数据 → 检查数据源
- 单只股票新闻数 > 100 → 人工复核

---

## 10. 实施计划

| 步骤 | 任务 | 预计时间 |
|-----|------|---------|
| 1 | 创建MySQL表结构 | 10分钟 |
| 2 | 开发 sentiment_daily_update.py | 2小时 |
| 3 | 迁移SQLite历史数据 | 30分钟 |
| 4 | 开发 sentiment_history_fill.py | 1小时 |
| 5 | 修改选股系统读取逻辑 | 30分钟 |
| 6 | 配置定时任务 | 10分钟 |
| 7 | 测试验证 | 1小时 |

**总计**: 约5-6小时

---

## 11. 待决策事项

1. **新闻来源**: 使用AkShare还是Coze搜索？或两者结合？
2. **更新范围**: 全市场6174只，还是仅活跃股/持仓股？
3. **历史回填**: 是否需要补充过去60天的舆情数据？
4. **实时性**: 盘中选股是否需要实时获取舆情？
