# 市场择时 V1.5 / V1.6 / V1.7 / V1.8 进展记录

日期：2026-06-09

## 背景

首页原“市场择时 V1”只使用已有实时市场概览做代理信号，包括市场强度、涨跌扩散、板块资金和涨停情绪。该版本能给出仓位提示，但不能清楚对应华泰金工文章里的具体择时因子。

V1.5 的目标是先接入 Tushare 中相对稳定、工程成本较低的数据，把首页择时升级为可落库、可复用、可回测接入的日频因子模型。

V1.6 继续补入文章情绪维度中的期权 PCR 和股指期货会员持仓。IV 偏斜暂不接入，因为它需要隐含波动率计算和更细的期权期限/价外程度口径。

V1.7 补入 AkShare 中债 10 年国债收益率作为 ERP fallback，并接入 QVIX 作为 IV 情绪代理。Pandas 用于收益率曲线筛选、QVIX 历史分位和新增聚合处理。

V1.8 补入 CFFEX 指数期权自算 IV 偏斜。当前先使用沪深300、上证50、中证1000指数期权，按最近到期月、流动性过滤、轻度虚值 put/call 反解 Black-Scholes IV，并以 put IV - call IV 作为偏斜压力。

## 已接入数据

新增表：

- `market_index_daily`：指数日线，来源 `tushare.index_daily`
- `market_index_valuation_daily`：指数估值，来源 `tushare.index_dailybasic`
- `market_margin_daily`：两融数据，来源 `tushare.margin`
- `market_bond_yield_daily`：十年国债收益率候选，来源 `tushare.yc_cb`
- `market_option_pcr_daily`：期权认沽/认购 PCR 聚合，来源 `tushare.opt_daily + opt_basic`
- `market_futures_holding_daily`：股指期货会员多空持仓聚合，来源 `tushare.fut_holding`
- `market_option_qvix_daily`：QVIX 波动率指数，来源 `akshare.qvix`
- `market_option_iv_skew_daily`：自算 IV 偏斜，来源 `tushare.opt_daily + opt_basic + self_calc`
- `market_timing_indicator_daily`：择时底层因子评分
- `market_timing_signal_daily`：合成择时信号

新增同步脚本：

- `scripts/run_market_timing_daily_update.py`

已加入 cron：

```cron
40 3 * * * cd /root/.openclaw/workspace/stock-analysis && PYTHONPATH=/root/.openclaw/workspace/stock-analysis /root/.openclaw/workspace/stock-analysis/.venv/bin/python scripts/run_market_timing_daily_update.py >> /root/.openclaw/workspace/stock-analysis/logs/market_timing_daily_update.log 2>&1
```

## 当前因子

已接入：

- 技术：指数布林带，基于 `index_daily`
- 估值：指数 PE_TTM 分位，基于 `index_dailybasic`
- 资金：融资买入额相对 20 日均值，基于 `margin`
- 情绪：期权成交/持仓 PCR，基于 `opt_daily + opt_basic`
- 情绪：QVIX 波动率代理，基于 AkShare QVIX
- 情绪：自算 IV 偏斜，基于 CFFEX 指数期权
- 情绪：股指期货会员多空持仓，基于 `fut_holding(exchange="CFFEX")`
- 微观结构：上涨/下跌股票成交额差，基于本地 `daily_kline`

部分接入：

- 估值：ERP/风险溢价。脚本会先尝试读取 `yc_cb`，若未拿到可识别收益率，则使用 `ak.bond_china_yield` 的“中债国债收益率曲线”10 年收益率兜底。

后续可优化：

- IV 偏斜当前是研究口径，可继续扩展 ETF 期权、严格 delta skew、分期限 term structure。

## 2026-06-09 实跑结果

运行：

```bash
.venv/bin/python scripts/run_market_timing_daily_update.py --lookback-days 120
```

输出摘要：

- 日期：`2026-06-09`
- 模型：`huatai_multidim_v15`
- 状态：`谨慎试探`
- 总分：`53.97`
- 仓位上限：`45%`
- 置信度：`0.82`

因子：

- 指数布林带：`29.8`，偏空
- 指数估值分位：`53.9`，中性
- ERP/风险溢价：待权限，不参与总分
- 融资买入额：`35.1`，偏空
- 上涨/下跌成交额差：`100.0`，偏多

## 2026-06-09 V1.6 实跑结果

运行：

```bash
.venv/bin/python scripts/run_market_timing_daily_update.py --lookback-days 120
```

输出摘要：

- 日期：`2026-06-09`
- 模型：`huatai_multidim_v16`
- 状态：`防守观望`
- 总分：`50.01`
- 仓位上限：`15%`
- 置信度：`0.86`

因子：

- 指数布林带：`29.8`，偏空
- 指数估值分位：`53.9`，中性
- ERP/风险溢价：待权限，不参与总分
- 融资买入额：`35.1`，偏空
- 期权 PCR：`45.2`，中性，成交 PCR `1.04`，持仓 PCR `0.74`
- 股指期货多空持仓：`39.8`，偏空，净多占比 `-6.4%`
- 上涨/下跌成交额差：`100.0`，偏多

## 2026-06-09 V1.7 实跑结果

运行：

```bash
.venv/bin/python scripts/run_market_timing_daily_update.py --lookback-days 120
```

输出摘要：

- 日期：`2026-06-09`
- 模型：`huatai_multidim_v17`
- 状态：`谨慎试探`
- 总分：`56.11`
- 仓位上限：`45%`
- 置信度：`1.0`

因子：

- 指数布林带：`29.8`，偏空
- 指数估值分位：`53.9`，中性
- ERP/风险溢价：`100.0`，偏多，ERP `5.29%`
- 融资买入额：`35.1`，偏空
- 期权 PCR：`45.2`，中性，成交 PCR `1.04`，持仓 PCR `0.74`
- QVIX 波动率：`43.8`，中性，均值 `15.91`，近 252 日分位 `56.2%`
- 股指期货多空持仓：`39.8`，偏空，净多占比 `-6.4%`
- 上涨/下跌成交额差：`100.0`，偏多

## 2026-06-09 V1.8 实跑结果

运行：

```bash
.venv/bin/python scripts/run_market_timing_daily_update.py --lookback-days 120
```

输出摘要：

- 日期：`2026-06-09`
- 模型：`huatai_multidim_v18`
- 状态：`防守观望`
- 总分：`54.05`
- 仓位上限：`15%`
- 置信度：`1.0`

因子：

- 指数布林带：`29.8`，偏空
- 指数估值分位：`53.9`，中性
- ERP/风险溢价：`100.0`，偏多，ERP `5.29%`
- 融资买入额：`35.1`，偏空
- 期权 PCR：`45.2`，中性
- QVIX 波动率：`43.8`，中性
- IV 偏斜：`31.5`，偏空，Put-Call `8.4pct`，Put IV `24.5%`，Call IV `16.1%`
- 股指期货多空持仓：`39.8`，偏空
- 上涨/下跌成交额差：`100.0`，偏多

## 产品语义

市场择时不是选股策略，也不是交易策略本身。它回答的是：

> 当前市场环境是否适合暴露 Beta 风险，以及选股结果应按多大仓位约束观察。

首页展示择时状态和因子评分；后续回测中心可将 `market_timing_signal_daily.position_upper` 接入交易策略的风险约束。

## 与文章因子对比

当前 V1.8 是“产品可用 + 研究可解释”版本，已经覆盖文章四维择时框架的主要方向，但不是逐字复刻文章原始回测口径。

| 文章维度 | 文章因子方向 | 当前实现 | 完成度 | 差异 |
| --- | --- | --- | --- | --- |
| 技术 | 指数趋势/布林带类指标 | `index_bollinger`，基于沪深 300 日线 20 日布林带位置 | 已完成 V1 | 当前只用沪深 300 主指数，后续可扩多指数确认 |
| 估值 | 指数估值分位 | `index_pe_percentile`，基于 `tushare.index_dailybasic` 的 PE_TTM 历史分位 | 已完成 V1 | 分位窗口按本地回看数据，尚未校准文章样本区间 |
| 估值 | ERP/风险溢价 | `erp = 100 / PE_TTM - 10Y 国债收益率`，国债优先 `yc_cb`，fallback `ak.bond_china_yield` | 已完成 V1 | 数据源与文章可能不同，但口径方向一致 |
| 资金 | 融资买入额/两融情绪 | `margin_buy_ratio`，融资买入额相对 20 日均值 | 已完成 V1 | 当前用全市场日频聚合，未拆交易所或行业 |
| 情绪 | 期权 PCR | `option_pcr`，基于 Tushare 期权成交量和持仓量 PCR | 已完成 V1 | 当前为全市场聚合，未按文章可能指定的单一品种单独复刻 |
| 情绪 | 波动率/IV | `qvix_volatility`，基于 AkShare QVIX 分位 | 已完成代理因子 | 这是 IV 情绪代理，不是文章原始 IV skew |
| 情绪 | IV 偏斜 | `iv_skew`，CFFEX 指数期权 Black-Scholes 自算 Put IV - Call IV | 已完成研究版 | 当前按近月、轻度虚值、流动性过滤；未做固定 Delta/期限 skew |
| 情绪 | 股指期货会员持仓 | `futures_holding_net`，基于 `tushare.fut_holding(exchange="CFFEX")` 多空持仓 | 已完成 V1 | 当前聚合 CFFEX 股指期货，未按文章指定合约族逐项校准 |
| 微观结构 | 上涨/下跌成交额差 | `up_down_amount_pressure`，基于本地 `daily_kline` 的上涨/下跌成交额压力 | 已完成 V1 | 当前为本地全 A 口径，可能与文章指数成分股口径不同 |

结论：

- 作为系统里的市场仓位风控层，V1.8 已经完成可用闭环。
- 作为“严格复现文章策略净值”的版本，还需要继续做因子口径校准、样本区间固定、权重阈值校准和历史回测对照。
- 下一步如果要验证文章效果，建议新增独立回测实验：`无择时基准`、`技术+估值`、`技术+估值+资金`、`V1.8 全因子`，比较收益、回撤、胜率和空仓天数。
