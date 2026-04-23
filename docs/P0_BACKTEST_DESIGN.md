# P0 阶段设计文档：数据回测系统

## 1. 需求分析

### 1.1 背景
当前选股策略缺乏历史验证手段，无法评估策略有效性。需要通过回测验证策略在历史数据上的表现。

### 1.2 目标
- 支持对V9/V10等选股策略进行历史回测
- 计算策略绩效指标（收益率、夏普比率、最大回撤等）
- 生成可视化回测报告
- 支持多策略对比

### 1.3 功能需求

| 编号 | 需求 | 优先级 | 描述 |
|------|------|--------|------|
| R1 | 历史数据加载 | P0 | 从**本地数据库**加载历史行情（SQLite） |
| R2 | 模拟交易 | P0 | 模拟买入、卖出、持仓管理 |
| R3 | 绩效计算 | P0 | 计算**单日收益**和**三日收益** |
| R4 | 滑点模拟 | P1 | 模拟实际交易滑点（默认0.1%） |
| R5 | 手续费模拟 | P1 | 模拟交易手续费（默认万3） |
| R6 | 报告生成 | P0 | 生成收益曲线图、交易记录表 |
| R7 | 策略对比 | P2 | 对比不同策略同一时期表现 |
| R8 | 参数敏感性 | P2 | 测试不同参数组合效果 |

### 1.3.1 收益口径定义

**单日收益（1-Day Return）**
- 买入价：选股当日开盘价
- 卖出价：次日开盘价
- 收益率：(次日开盘价 - 当日开盘价) / 当日开盘价

**三日收益（3-Day Return）**
- 买入价：选股当日开盘价
- 卖出价：第三日收盘价
- 收益率：(第三日收盘价 - 当日开盘价) / 当日开盘价

**适用场景**
- 单日收益：衡量隔夜/短线效果
- 三日收益：衡量短期趋势效果

### 1.4 非功能需求
- 支持回测最近5年历史数据
- 回测1000只股票1年数据在5分钟内完成
- 内存占用不超过2GB

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     Backtest Engine                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ Data Loader │  │  Strategy   │  │   Broker    │     │
│  │  (历史数据)  │  │  (选股策略)  │  │  (模拟交易)  │     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
│         │                │                │            │
│         └────────────────┼────────────────┘            │
│                          ▼                             │
│                 ┌─────────────────┐                   │
│                 │  Portfolio      │                   │
│                 │  (账户/持仓)     │                   │
│                 └────────┬────────┘                   │
│                          ▼                             │
│                 ┌─────────────────┐                   │
│                 │  Metrics        │                   │
│                 │  (绩效计算)      │                   │
│                 └────────┬────────┘                   │
│                          ▼                             │
│                 ┌─────────────────┐                   │
│                 │  Report         │                   │
│                 │  (报告生成)      │                   │
│                 └─────────────────┘                   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 模块说明

| 模块 | 职责 | 关键类 |
|------|------|--------|
| DataLoader | 从本地SQLite加载历史数据 | `LocalHistoryLoader` |
| Strategy | 执行选股策略 | `BacktestStrategy` |
| Broker | 模拟交易执行 | `SimulatedBroker` |
| Portfolio | 管理虚拟账户和持仓 | `Portfolio` |
| Metrics | 计算绩效指标 | `PerformanceMetrics` |
| Report | 生成回测报告 | `BacktestReport` |

---

## 3. 数据模型

### 3.1 核心数据结构

```python
# 历史K线数据
@dataclass
class Bar:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    code: str

# 交易记录
@dataclass  
class Trade:
    date: datetime
    code: str
    action: str  # 'buy' | 'sell'
    price: float
    shares: int
    fee: float
    reason: str

# 持仓
@dataclass
class Position:
    code: str
    shares: int
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float

# 账户
@dataclass
class Account:
    cash: float
    positions: Dict[str, Position]
    total_value: float
    daily_values: List[Dict]  # 每日总资产记录
```

### 3.2 回测配置

```python
@dataclass
class BacktestConfig:
    start_date: str          # 回测开始日期
    end_date: str            # 回测结束日期
    initial_capital: float   # 初始资金
    max_positions: int       # 最大持仓数
    position_size: float     # 单只股票仓位比例
    commission_rate: float   # 手续费率
    slippage: float          # 滑点
    stop_loss: float         # 止损比例
    take_profit: float       # 止盈比例
```

### 3.3 绩效指标

```python
@dataclass
class PerformanceMetrics:
    # 综合指标
    total_return: float           # 总收益率
    annualized_return: float      # 年化收益率
    sharpe_ratio: float           # 夏普比率
    max_drawdown: float           # 最大回撤
    max_drawdown_duration: int    # 最大回撤持续天数
    win_rate: float               # 胜率
    profit_factor: float          # 盈亏比
    total_trades: int             # 总交易次数
    avg_trade_return: float       # 平均每笔收益
    
    # 分段收益指标（新增）
    avg_1day_return: float        # 平均单日收益
    avg_3day_return: float        # 平均三日收益
    win_rate_1day: float          # 单日收益胜率
    win_rate_3day: float          # 三日收益胜率
    
@dataclass
class Trade:
    """交易记录（含双收益口径）"""
    date: datetime                # 选股日期
    code: str
    entry_price: float            # 买入价（当日开盘价）
    exit_price_1d: float          # 卖出价（次日开盘价）
    exit_price_3d: float          # 卖出价（第三日收盘价）
    return_1d: float              # 单日收益率
    return_3d: float              # 三日收益率
    shares: int
    fee: float
```

---

## 4. 接口定义

### 4.1 核心接口

```python
class IBacktestEngine(ABC):
    """回测引擎接口"""
    
    @abstractmethod
    def run(self, strategy: IStrategy, config: BacktestConfig) -> BacktestResult:
        """执行回测"""
        pass

class IStrategy(ABC):
    """策略接口"""
    
    @abstractmethod
    def on_bar(self, bar: Bar, portfolio: Portfolio) -> List[Signal]:
        """每根K线调用，返回交易信号"""
        pass
    
    @abstractmethod
    def on_day_start(self, date: datetime, portfolio: Portfolio):
        """每日开盘前调用"""
        pass

class IBroker(ABC):
    """交易接口"""
    
    @abstractmethod
    def buy(self, code: str, shares: int, price: float) -> Trade:
        """买入"""
        pass
    
    @abstractmethod
    def sell(self, code: str, shares: int, price: float) -> Trade:
        """卖出"""
        pass
```

### 4.2 回测结果

```python
@dataclass
class BacktestResult:
    config: BacktestConfig
    metrics: PerformanceMetrics
    trades: List[Trade]
    daily_values: pd.DataFrame
    positions_history: List[Dict]
    
    def plot(self, save_path: str = None):
        """绘制收益曲线"""
        pass
    
    def to_report(self) -> str:
        """生成文字报告"""
        pass

### 4.3 使用示例

```python
# 回测V9策略，使用双收益口径
from backtest import BacktestEngine, BacktestConfig
from backtest.strategies import V9Adapter

# 配置
config = BacktestConfig(
    start_date='20240101',
    end_date='20241231',
    initial_capital=1000000,
    max_positions=10,
    position_size=0.1,  # 每只股票10%仓位
    commission_rate=0.0003,  # 万3手续费
    slippage=0.001  # 0.1%滑点
)

# 初始化
engine = BacktestEngine()
strategy = V9Adapter()  # V9策略适配器

# 执行回测
result = engine.run(strategy, config)

# 查看结果
print(f"总交易次数: {result.metrics.total_trades}")
print(f"平均单日收益: {result.metrics.avg_1day_return:.2%}")
print(f"平均三日收益: {result.metrics.avg_3day_return:.2%}")
print(f"单日胜率: {result.metrics.win_rate_1day:.1%}")
print(f"三日胜率: {result.metrics.win_rate_3day:.1%}")

# 生成报告
result.plot(save_path='backtest_report.png')
report = result.to_report()
```
```

---

## 5. 执行流程

### 5.1 时序图

```
用户        BacktestEngine    DataLoader    Strategy    Broker    Portfolio    Metrics
 |               |                |            |          |          |            |
 |--run()------>|                |            |          |          |            |
 |               |--load_data()-->|            |          |          |            |
 |               |<--bars---------|            |          |          |            |
 |               |                |            |          |          |            |
 |               |------------------------------------------->       |            |
 |               |           初始化Portfolio(initial_capital)        |            |
 |               |<------------------------------------------|       |            |
 |               |                |            |          |          |            |
 |               |--loop(bars)-------------------------------------->|            |
 |               |                |            |          |          |            |
 |               |--on_day_start(date, portfolio)--------->|         |            |
 |               |                |<--signals--|          |          |            |
 |               |                |            |          |          |            |
 |               |--execute(signals)-------------------------------->|            |
 |               |                           |--buy/sell->|          |            |
 |               |                           |<--trade----|          |            |
 |               |<------------------------------------------|       |            |
 |               |                           |            |--update->|            |
 |               |<--------------------------------------------------|            |
 |               |                                                      |         |
 |               |--calculate_metrics()-------------------------------------------|>
 |               |<---------------------------------------------------------------|
 |<--result------|                                                      |         |
```

### 5.2 每日处理流程（双收益口径）

```python
def run_daily_iteration(date, bars, strategy, portfolio, broker, history_db):
    """每日回测迭代 - 支持单日/三日收益计算"""
    
    # 1. 盘前：策略选股（基于前一日数据）
    selected_stocks = strategy.select(date, history_db)
    
    # 2. 开盘：模拟买入（以当日开盘价买入）
    for stock in selected_stocks:
        bar = history_db.get_bar(stock.code, date)
        if bar:
            broker.buy(
                code=stock.code,
                shares=calculate_position_size(portfolio),
                price=bar.open  # 当日开盘价
            )
    
    # 3. 计算双收益口径（收盘后）
    for trade in portfolio.today_trades:
        # 获取次日数据（单日收益）
        next_day_bar = history_db.get_bar(trade.code, next_trading_day(date))
        if next_day_bar:
            trade.exit_price_1d = next_day_bar.open  # 次日开盘价
            trade.return_1d = (trade.exit_price_1d - trade.entry_price) / trade.entry_price
        
        # 获取第三日数据（三日收益）
        third_day_bar = history_db.get_bar(trade.code, third_trading_day(date))
        if third_day_bar:
            trade.exit_price_3d = third_day_bar.close  # 第三日收盘价
            trade.return_3d = (trade.exit_price_3d - trade.entry_price) / trade.entry_price
    
    # 4. 记录当日结果
    portfolio.record_daily_returns(date, trades)
```

### 5.3 收益计算流程

```
选股日(D)
    │
    ├── 买入价：D日开盘价
    │
    ├── 次日(D+1)
    │      └── 单日收益卖出价：D+1日开盘价
    │
    └── 第三日(D+3)
           └── 三日收益卖出价：D+3日收盘价

收益率计算：
    单日收益 = (D+1开盘价 - D开盘价) / D开盘价
    三日收益 = (D+3收盘价 - D开盘价) / D开盘价
```

---

## 6. 测试方案

### 6.1 单元测试

```python
class TestPortfolio(unittest.TestCase):
    def test_buy(self):
        """测试买入"""
        portfolio = Portfolio(initial_capital=100000)
        portfolio.buy('000001', 100, 10.0, fee=3)
        self.assertEqual(portfolio.positions['000001'].shares, 100)
        self.assertEqual(portfolio.cash, 99897)  # 100000 - 100*10 - 3

class TestMetrics(unittest.TestCase):
    def test_sharpe_ratio(self):
        """测试夏普比率计算"""
        returns = [0.01, 0.02, -0.01, 0.015, 0.01]
        sharpe = calculate_sharpe_ratio(returns, risk_free_rate=0.03)
        self.assertAlmostEqual(sharpe, expected_value, places=2)
```

### 6.2 集成测试

```python
def test_backtest_v9_strategy():
    """测试V9策略回测"""
    config = BacktestConfig(
        start_date='20240101',
        end_date='20240331',
        initial_capital=1000000
    )
    
    engine = BacktestEngine()
    strategy = V9BacktestStrategy()
    result = engine.run(strategy, config)
    
    assert result.metrics.total_return > -0.5  # 亏不超过50%
    assert result.metrics.total_trades > 0     # 有交易产生
    assert len(result.trades) > 0              # 有交易记录
```

### 6.3 验证测试

| 验证项 | 方法 | 通过标准 |
|--------|------|---------|
| 数据准确性 | 对比Baostock原始数据 | 收盘价误差<0.01% |
| 计算准确性 | 手工计算验证 | 收益率计算正确 |
| 边界条件 | 测试极端情况 | 空数据、单只股票、全仓等 |

---

## 7. 技术选型

| 组件 | 选型 | 理由 |
|------|------|------|
| 数据来源 | 本地SQLite数据库（腾讯云轻量） | 已存在、速度快、不依赖网络 |
| 计算库 | Pandas + NumPy | 成熟、性能好 |
| 可视化 | Matplotlib | 简单、够用 |
| 报告生成 | Jinja2 + HTML | 可导出为PDF |

### 7.1 数据来源说明

**主数据源**：`stock_history_db.py`（本地SQLite）

```python
# 从本地数据库加载历史数据
from stock_history_db import get_stock_history_db

db = get_stock_history_db()
prices = db.get_prices(code='000001', days=60)  # 获取60天历史数据
```

**数据覆盖**：
- 全A股日K线数据（开盘/收盘/最高/最低/成交量）
- 数据范围：2020年至今（约5年）
- 更新频率：每日收盘后自动更新

**优势**：
- ✅ 无需实时调用Baostock/AkShare API
- ✅ 回测速度快（本地读取）
- ✅ 数据稳定（已清洗入库）
- ✅ 支持离线回测 |

---

## 8. 项目结构

```
src/
├── backtest/
│   ├── __init__.py
│   ├── engine.py              # 回测引擎
│   ├── broker.py              # 模拟交易
│   ├── portfolio.py           # 账户管理
│   ├── metrics.py             # 绩效计算（含双收益口径）
│   ├── report.py              # 报告生成
│   └── strategies/            # 策略适配器
│       ├── __init__.py
│       ├── base.py            # 策略基类
│       ├── v9_adapter.py      # V9策略适配
│       └── v10_adapter.py     # V10策略适配
├── data/
│   └── local_loader.py        # 本地SQLite数据加载器
└── tests/
    └── backtest/              # 测试用例
```

---

## 9. 里程碑检查点

| 检查点 | 时间 | 验收标准 |
|--------|------|---------|
| CP1 | Week 1 结束 | 基础框架可运行，能加载数据 |
| CP2 | Week 2 结束 | V9策略接入，能生成交易记录 |
| CP3 | Week 3 结束 | 报告生成完整，性能达标 |

---

## 10. 下一步

1. **确认设计文档** - 是否有遗漏或需要调整？
2. **技术选型确认** - 是否接受推荐的技术栈？
3. **开始开发** - 按照Week 1计划开始编码

---

**文档版本**: v1.0  
**创建时间**: 2026-04-02  
**状态**: 待评审