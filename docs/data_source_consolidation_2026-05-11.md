# 数据源收敛方案（2026-05-11）

## 目标

当前项目已经接入 BaoStock、AkShare、Tushare、Tavily、DeepSeek、Sina/Eastmoney 等多个来源。短期能补功能，但长期会带来：

- 同一字段多来源口径不一致，例如 PE/PB、日 K、实时价；
- 定时任务和失败原因难排查；
- 页面状态解释复杂，用户不知道哪个数据可信；
- 外部接口越多，稳定性越差。

V2 阶段建议收敛为“少数主源 + 明确 fallback + 全部落库后页面只读库”。

## 推荐原则

1. **页面/API 不直接拉外部源**：页面只读 MySQL 缓存。
2. **每类数据只有一个主源**：除非主源明显缺失，否则不多源混写。
3. **实时与正式数据分层**：实时快照可用于盘中展示/快速 EOD，但官方日 K 保留二次校准。
4. **昂贵源只用于少量精排**：Tavily/AI 不做全市场常规任务。
5. **历史研究脚本归档**：DeepSeek 研究脚本、旧 screen/report 脚本不进入生产链路。

## 保留的数据源

### 1. AkShare：行情与新闻主源

**保留用途：**

- 全市场实时快照：`stock_zh_a_spot()`
- 盘后快速日 K：由 `stock_realtime_snapshot` 回填 `daily_kline`，source=`akshare_realtime_eod`
- 个股分钟线缓存：优先 `stock_zh_a_minute()`，必要时 Eastmoney minute fallback
- 行业/概念资金流：`stock_fund_flow_industry/concept`
- 个股新闻粗源：`stock_news_em`
- 股票状态快照：停牌/暂停上市等状态识别

**定位：** 交易日内与盘后快速更新的主源。

**注意：** AkShare 部分接口偶发 `RemoteDisconnected`，所以外部拉取必须在后台任务中完成，并写 task log；页面不能同步等待 AkShare。

### 2. Tushare：基础股票表、官方日 K、估值/基本面/因子输入主源

**保留用途：**

- `stock_basic`：A 股基础列表、行业、上市日期、北交所代码覆盖
- `daily`：官方日 K 校准源，source=`tushare_daily`
- `daily_basic`：PE/PB、换手率、量比、市值等
- `fina_indicator`：ROE、ROA、EPS、利润/营收同比等
- `factor_input_daily` 历史输入层补齐

**定位：** 中低频结构化数据与官方日线主源。

**实施状态：** 2026-05-11 已将 `stock_basic_sync.py` 与 `daily_kline_sync.py` 切到 Tushare；BaoStock 不再是生产同步依赖。

### 3. BaoStock：生产链路下线

**处理：**

- 不再作为股票基础表或官方日 K 的生产主源；
- 旧 BaoStock 方案仅作为历史参考，不进入 cron/API/页面；
- 若后续确认 Tushare 长期稳定，可删除 BaoStock 依赖包与旧文档引用。

### 4. Tavily：只保留 V12 小范围舆情精排

**保留用途：**

- V12 当前选股 Top N 候选的精搜舆情。

**限制：**

- 不用于全市场日更；
- 不用于历史回测批量回填；
- 默认只对 Top40 或更小候选池调用；
- 回测中心不开放 V12 严格历史回测，避免消耗大量搜索次数。

## 冻结或下线的数据源/路径

### 1. DeepSeek：退出生产链路

**处理：**

- DeepSeek 相关脚本只作为历史研究资料保留；
- 不再作为每日任务、页面 API、选股实时链路的一部分；
- 不新增 DeepSeek key，不在仓库/配置里继续依赖。

涉及脚本示例：

- `scripts/deepseek_cost_analysis.py`
- `scripts/deepseek_cost_decision.py`
- `scripts/deepseek_strategy_reconstruction.py`
- `scripts/report_postmarket_v10plus.py`
- `docs/V12_*DEEPSEEK*`

### 2. 旧版 screen/report 实验脚本：归档

例如：

- `scripts/screen_sector_v5.py`
- `scripts/screen_sector_v52.py`
- `scripts/comprehensive_report.py`

这些脚本可保留在仓库中作参考，但不应被 cron/API/页面调用。

### 3. Sina/Eastmoney 分钟线：降为 AkShare 内部 fallback

当前代码通过 AkShare 包装调用 Sina/Eastmoney minute 接口。对产品心智来说不再视为独立数据源，只作为“AkShare 分钟线缓存”的内部 fallback。

## 建议后的数据源矩阵

| 数据类别 | 主源 | fallback | 入库表 | 页面是否直连外部 |
|---|---|---|---|---|
| 实时全市场行情 | AkShare `stock_zh_a_spot` | 无 | `stock_realtime_snapshot`, `stock_realtime_intraday` | 否 |
| 盘后快速日 K | 本地实时快照聚合 | 无 | `daily_kline`, source=`akshare_realtime_eod` | 否 |
| 官方日 K 校准 | Tushare `daily` | 无 | `daily_kline`, source=`tushare_daily` | 否 |
| 个股分钟线 | AkShare/Sina minute | AkShare/Eastmoney minute | `stock_intraday_bar` | 否 |
| PE/PB/换手/量比/市值 | Tushare `daily_basic` | 无 | `stock_basic`, `factor_input_daily` | 否 |
| ROE/ROA/EPS/增长 | Tushare `fina_indicator` | 无 | `stock_basic`, `factor_input_daily` | 否 |
| 股票基础列表 | Tushare `stock_basic` | AkShare realtime snapshot supplement | `stock_basic` | 否 |
| 个股新闻粗源 | AkShare `stock_news_em` | 无 | `stock_sentiment_news` 等 | 否 |
| 舆情精搜 | Tavily TopN only | 无 | `stock_sentiment_news` / selection metadata | 否 |
| 行业/概念资金流 | AkShare fund flow | 本地实时快照行业聚合 | `market_sector_fund_flow_*` | 否 |

## 当前 cron 建议

保留：

- 每分钟交易时段：`run_realtime_snapshot_update.py`
- 15:10 交易日：`backfill_kline_from_realtime_snapshot.py`
- 15:25 交易日：`run_factor_input_daily_update.py` 补当天快速日 K 对应的因子输入层，避免选股切到新交易日但缺 factor input
- 01:30：`run_stock_basic_sync.py` Tushare 股票基础表日更
- 02:00：`run_kline_daily_update.py` Tushare 官方日 K 校准
- 02:40/02:50：fundamental / valuation 日更
- 03:20：factor input 日更
- 行业/概念资金流盘中低频更新
- 舆情日更：默认 AkShare，Tavily 只 TopN

避免新增：

- 全市场逐股实时接口；
- 页面请求触发外部源；
- 大规模 Tavily/DeepSeek 批处理；
- 同一个字段由多个源反复覆盖且不记录 source。

## 下一步执行建议

### P0：先定口径，不急删代码

1. 在系统页展示“数据源口径”：实时行情=AkShare，官方日 K/估值/基本面=Tushare。
2. 给 `daily_kline` 的 source 做更清晰解释：
   - `akshare_realtime_eod`：盘后快速 K；
   - `tushare_daily`：凌晨官方校准 K。
3. 检查 cron，确保没有 DeepSeek/旧 screen/report 脚本在跑。

### P1：代码层收敛

1. 把外部源调用集中到 `app/data_ingestion/providers/` 或现有 ingestion 模块，不让业务层直接 import 外部 SDK。
2. 对 DeepSeek 和旧实验脚本加 `archive/` 或 README 标记，避免误用。
3. 给每个入库任务统一写 `task_runs`，包括 source、行数、失败原因。

### P2：清理 BaoStock 遗留

1. 检查依赖文件中是否仍需要 `baostock` 包；若无生产脚本使用，可移除。
2. 将历史 BaoStock 排障说明标为 legacy，避免后续误以为仍是主链路。
3. 观察 3-5 个交易日 Tushare 日 K 与 AkShare 快速 EOD 的覆盖差异，再决定是否删除旧 BaoStock 代码备份。
