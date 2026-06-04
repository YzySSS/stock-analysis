# 全 A 实时行情分钟快照设计（2026-05-07）

## 目标

盘中获取全 A 实时行情，用于首页、跟踪复盘、个股详情和后续盘中曲线。

大X接受 1 分钟尝试频率，但要求有降级策略；盘中历史只保留当天，避免数据库无限膨胀。

## 数据源验证

接口：`ak.stock_zh_a_spot()`

短测：连续 3 轮，间隔 1 分钟。

| 轮次 | 结果 | 行数 | 耗时 |
|---|---|---:|---:|
| 1 | 成功 | 5512 | 34.06s |
| 2 | 成功 | 5512 | 34.44s |
| 3 | 成功 | 5512 | 35.75s |

结论：1 分钟频率短测可用，但单次耗时约 35 秒，必须防重入和失败退避。

## 表结构

### `stock_realtime_snapshot`

每只股票只保留最新一行，主键 `code`。

用途：
- 首页当前涨跌幅
- 个股详情当前价
- 跟踪复盘当前表现

核心字段：
- `code/source_code/name`
- `trade_date/quote_time`
- `latest_price/change_amount/pct_chg`
- `bid_price/ask_price`
- `pre_close/open_price/high_price/low_price`
- `volume/amount`
- `source/created_at/updated_at`

### `stock_realtime_intraday`

分钟级盘中历史，唯一键：`(trade_date, quote_minute, code)`。

用途：
- 个股盘中曲线
- 全 A 涨跌家数/市场热度曲线

当前保留策略：默认 `--retention-days 1`，只保留当天数据。

## 更新脚本

脚本：`scripts/run_realtime_snapshot_update.py`

默认行为：
- 交易日盘中才执行：`09:25-11:35`、`12:55-15:05`
- 使用 MySQL `GET_LOCK` 防重入
- 拉取 `ak.stock_zh_a_spot()`
- 覆盖写 `stock_realtime_snapshot`
- 追加/更新写 `stock_realtime_intraday`
- 清理过期盘中历史
- 写入 `task_run_log`

降级策略：
- 状态文件：`logs/realtime_snapshot_state.json`
- 连续失败达到阈值（默认 3）后，设置 `degraded_until = now + 5min`
- cron 仍每分钟触发，但脚本在降级窗口内直接 skip
- 下一次降级窗口结束后再尝试恢复 1 分钟频率
- 单次 AkShare 实时行情抓取默认重试 2 次，缓解开盘附近源头短暂返回异常内容导致的空窗。

## 调度

已接入 `scripts/setup_kline_cron.sh`：

```cron
* 9-15 * * 1-5 cd /root/.openclaw/workspace/stock-analysis && PYTHONPATH=/root/.openclaw/workspace/stock-analysis /root/.openclaw/workspace/stock-analysis/.venv/bin/python scripts/run_realtime_snapshot_update.py --retention-days 1 >> /root/.openclaw/workspace/stock-analysis/logs/stock_realtime_snapshot_update.log 2>&1
```

脚本内部会判断真实行情时段，因此 9:00-9:14、11:36-12:54、15:06-15:59 会自动跳过；9:15-9:29 会尝试捕获盘前集合竞价快照。

## 实跑验证

命令：

```bash
./.venv/bin/python scripts/run_realtime_snapshot_update.py --force --retention-days 1
```

结果：

```json
{
  "status": "success",
  "rows": 5512,
  "elapsed_seconds": 36.37,
  "latest_quote_time": "2026-05-07 14:24:06",
  "snapshot_rows": 5512,
  "intraday_rows": 5512,
  "deleted_old_rows": 0
}
```

数据库验证：
- `stock_realtime_snapshot`：5512 行
- `stock_realtime_intraday` 当日：5512 行
- 浦发银行样例：`latest_price=9.1500`，`pct_chg=-0.3270`，`quote_time=2026-05-07 14:23:32`

## 后续页面接入建议

V1 页面可优先接：
1. 首页最近跟踪：用实时价替换最新日线价。
2. 个股详情：展示当前价、涨跌幅、行情更新时间、今日分钟曲线。
3. 跟踪复盘：区间收益可并列展示“截至最近日线”和“截至实时行情”。

## 页面/API 接入补充（14:30）

已把实时快照接入主要页面读取链路：

### 首页 `/`

`/api/dashboard/summary` 的 `latest_tracking_preview[]` 已补充并优先使用：
- `current_price`：优先实时价，缺失时回退最新日线价
- `daily_current_price`
- `realtime_price`
- `realtime_pct_chg`
- `realtime_quote_time`
- `realtime_price_change_pct`

首页跟踪预览展示实时价、实时区间收益和行情时间。

### 跟踪复盘 `/tracking`

跟踪列表“最新价/区间收益”已改为实时价/实时区间收益，并显示实时行情时间；汇总统计也基于实时价计算。

### 个股详情 `/stocks/{code}`

`GET /api/stocks/{code}` 新增：
- `realtime`：最新实时快照
- `realtime_intraday[]`：当天分钟级行情，用于画今日分钟走势

页面新增“今日分钟走势”图，并把顶部价格/涨跌幅切为实时口径。

验证：
- `/api/dashboard/summary?limit=2` 返回实时行情时间与实时收益
- `/api/stocks/sh.600000?intraday_limit=20` 返回 `realtime` 与 `realtime_intraday`
- 公网 `/`、`/tracking`、`/stocks/sh.600000` 均返回 200

## 首页市场强度接入（14:35）

按大X要求，首页新增“今天市场强度 / 涨跌家数 / 强弱势板块”宏观信息。

### 数据源判断

- 尝试 AkShare 东方财富板块现货接口：
  - `stock_board_industry_spot_em()`
  - `stock_board_concept_spot_em()`
  - `stock_board_industry_name_em()`
- 当前服务器实测均出现 `RemoteDisconnected`，不适合放在页面请求链路里。
- 因此 V1 使用本地 MySQL 实时快照自行统计：`stock_realtime_snapshot + stock_basic.industry`。

### 首页口径

`/api/dashboard/summary` 新增 `market_overview`：
- `market_strength`：0~100 综合强度分，基于平均涨跌幅、上涨占比、涨停/跌停近似差计算。
- `market_state_label`：强势 / 偏强 / 震荡 / 偏弱 / 弱势。
- `up_count/down_count/flat_count`：全 A 涨跌平家数。
- `limit_up_like/limit_down_like`：按 ±9.8% 粗略近似涨跌停家数。
- `strong_up_count/strong_down_count`：按 ±5% 统计大涨/大跌家数。
- `strong_sectors/weak_sectors`：按 `stock_basic.industry` 分组，样本数 >= 5，只展示均幅最高/最低板块。

### 验证样例

2026-05-07 14:34 快照：
- 市场强度 `68.76`，状态 `偏强`
- 上涨/下跌/平盘：`3226 / 1821 / 152`
- 强势板块首位：装卸搬运和仓储业
- 弱势板块首位：石油和天然气开采业
- 公网首页 `/` 和 `/api/dashboard/summary?limit=2` 验证成功。

## 行业/概念资金流接入（15:09）

按大X反馈，`stock_basic.industry` 的证监会行业分类颗粒度太粗，不能代表同花顺常见的热点概念/细分板块。因此新增实时资金流链路：

### 新表

- `market_sector_fund_flow_snapshot`：行业/概念当前最新资金流快照。
- `market_sector_fund_flow_intraday`：当天分钟级行业/概念资金流历史。

字段包括：
- `sector_type`：`industry` / `concept`
- `sector_name`
- `pct_chg`
- `inflow_amount`
- `outflow_amount`
- `net_amount`
- `company_count`
- `leading_stock`
- `leading_stock_pct_chg`
- `source_unit='亿元'`

### 同步任务

新增 `scripts/run_market_fund_flow_update.py`：
- AkShare `stock_fund_flow_industry(symbol="即时")`
- AkShare `stock_fund_flow_concept(symbol="即时")`
- MySQL `GET_LOCK` 防重入
- 默认只保留当天 intraday 历史
- 已接入 cron：交易日 9-15 点每 3 分钟执行一次

### 首页口径

`/api/dashboard/summary.market_overview`：
- `strong_sectors` / `weak_sectors` 优先使用真实行业/概念资金流，按 `net_amount` 排序。
- 若资金流表为空，fallback 到 `stock_realtime_snapshot + stock_basic.industry` 的成交额加权行业统计。
- 新增 `sector_source='akshare_realtime_fund_flow'`、`sector_fund_flow_time`、`sector_fund_flow_rows`。

### 验证

2026-05-07 15:08 实跑成功：
- 写入 `477` 条资金流快照：行业 `90`、概念 `387`
- 首页强势首位：`5G`，净流入 `324.47 亿`
- 首页弱势首位：`锂电池概念`，净流入 `-328.35 亿`
- 公网 `/` 与 `/api/dashboard/summary` 验证成功。
