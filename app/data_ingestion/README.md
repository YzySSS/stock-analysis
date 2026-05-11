# data_ingestion/

当前主线同步入口：

- `stock_basic_sync.py`：使用 Tushare 同步股票基础信息到 MySQL `stock_basic`，并用 AkShare 实时快照补充极新代码
- `daily_kline_sync.py`：使用 Tushare `daily` 同步官方日线行情到 MySQL `daily_kline`
- `valuation_sync.py`：使用 Tushare 同步 PE / PB 等估值字段到 `stock_basic`
- `fundamental_sync.py`：使用 Tushare 同步 ROE 等基本面字段到 `stock_basic`
- `adj_factor_sync.py`：使用 Tushare `adj_factor` 同步复权因子到 `adj_factor_daily`
- `moneyflow_sync.py`：使用 Tushare `moneyflow` 同步个股资金流到 `stock_moneyflow_daily`
- `chip_sync.py`：使用 Tushare `cyq_perf` 同步筹码表现到 `stock_chip_daily`

其中：
- Tushare 是基础股票表、官方日 K、估值、基本面、复权因子、个股资金流、筹码数据的主源
- AkShare 负责实时行情、盘后快速日 K、分钟线、资金流和新闻粗源
- BaoStock 已从生产同步链路下线，旧实现仅作为历史参考

后续再逐步吸收旧脚本能力。
