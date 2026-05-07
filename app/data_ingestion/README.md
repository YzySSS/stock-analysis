# data_ingestion/

当前主线先落四个同步入口：

- `stock_basic_sync.py`：同步股票基础信息到 MySQL `stock_basic`
- `daily_kline_sync.py`：同步日线行情到 MySQL `daily_kline`
- `valuation_sync.py`：使用 Tushare 同步 PE / PB 等估值字段到 `stock_basic`
- `fundamental_sync.py`：使用 Tushare 同步 ROE 等基本面字段到 `stock_basic`

其中：
- 前两个属于第一阶段基础底座
- `valuation_sync.py` / `fundamental_sync.py` 属于第二阶段增强数据接入

后续再逐步吸收旧脚本能力。
