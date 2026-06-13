# A股舆情选股交易计划 V1 设计（2026-06-11）

## 背景

当前 `a_share_sentiment` 已经能回答“今天哪些股票值得关注”，但还没有结构化回答：

- 现在能不能追
- 更合适的入场区间在哪里
- 错了跌到哪里撤
- 涨了到哪里分批卖
- 什么条件下原计划失效

本设计只做交易计划建议，不做自动交易。

核心原则：

> 舆情决定方向，资金确认可交易性，技术指标决定买卖点和风控边界。

本轮设计参考了 DeepSeek API 的外部建议，但最终口径按当前项目已有数据结构、策略语义和大X确认过的产品方向整理。

## 目标

为 `a_share_sentiment` 每条入选结果生成一份可解释、可落库、可回测的交易计划：

```json
{
  "entry_style": "wait_pullback",
  "entry_zone": {"low": 11.8, "high": 12.1},
  "stop_loss": 11.45,
  "take_profit_1": 13.1,
  "take_profit_2": 14.0,
  "chase_warning": 12.8,
  "risk_reward_ratio": 1.8,
  "invalid_conditions": [],
  "reason": "..."
}
```

V1 重点是让页面给出“可执行计划”，而不是把技术指标堆成另一个选股模型。

## 适用范围

优先只接入 `a_share_sentiment`：

- 策略周期：短线，T+1 到 T+5
- 使用场景：舆情热点股、主题前排、盘中/收盘后交易计划
- 风险偏好：不接飞刀，不盲目追高，不自动下单

暂不扩展到所有策略。低波反转、趋势策略、缠论观察等可以后续各自有不同交易计划模板。

## 可用数据

当前项目已有或接近可用的数据：

- `daily_kline`
  - open / high / low / close / volume / amount
  - 可算 MA5、MA10、MA20、20日高低点、ATR14、量能均值
- `stock_realtime_snapshot`
  - latest_price / pct_chg / open_price / high_price / low_price / amount / quote_time
  - 可用于盘中当前价、日内位置、实时成交额
- `stock_realtime_intraday`
  - 分钟级快照
  - 可用于入选后浮盈回撤、粗略 VWAP、分时位置
- `stock_intraday_bar`
  - 已有 schema 和个股详情接口缓存链路
  - 后续可作为更稳定的分钟 K/VWAP 来源
- `a_share_sentiment` 结果中的现有因子
  - `sector_heat`
  - `source_credibility`
  - `info_importance`
  - `amplification`
  - `stock_recognition`
  - `fund_flow`
  - `price_confirm`
  - `volume_confirm`
  - `intraday_confirm`
  - `trade_signal_state / label / reason`

## V1 指标集

V1 不超过 8 个核心技术指标。

### 日线指标

1. `ma5`
   - 5日收盘均线
   - 用于短线强弱和回踩参考

2. `ma10`
   - 10日收盘均线
   - 用于短线趋势防线

3. `ma20`
   - 20日收盘均线
   - 用于中期支撑和趋势失效判断

4. `atr14`
   - 14日平均真实波幅
   - 用于止损和止盈距离

5. `high_20 / low_20`
   - 20日最高/最低
   - 用于突破确认、压力位和极端位置判断

6. `amount_ratio_5d`
   - 当前成交额 / 近5日平均成交额
   - 用于判断资金参与度

### 盘中指标

7. `intraday_position`
   - `(当前价 - 日内低点) / (日内高点 - 日内低点)`
   - 判断是否日内追高

8. `vwap`
   - 盘中成交均价
   - V1 若分钟成交量不稳定，可先用 `stock_realtime_intraday` 或快照近似；P1 再切到 `stock_intraday_bar`

## 交易状态

输出字段建议为 `entry_style`，枚举值固定，方便前端展示和回测统计。

### 1. `buyable_now` 立即可试

适合小仓位试。

条件建议：

- 舆情强度强：`sector_heat >= 70` 或 `stock_recognition >= 70`
- 资金确认不弱：`fund_flow >= 50`
- 分时确认较好：`intraday_confirm >= 60`
- 当前价在 `ma5` 上方，且未远离 `ma5 + 1.5 * atr14`
- 日内位置不过热：`intraday_position <= 0.80`
- 没有硬风险：实时跌幅未触发接飞刀过滤

产品文案：

```text
舆情与交易确认匹配，当前位置尚未明显追高，可小仓位试。
```

### 2. `wait_pullback` 等回踩

方向认可，但买点不舒服。

触发条件：

- 舆情和资金仍然强
- 但当前价明显高于 `ma5 + 1.5 * atr14`
- 或日内位置 `intraday_position > 0.80`
- 或盘中从高点回撤不充分，容易追在尖上

产品文案：

```text
方向仍然成立，但当前偏追高，等待回踩到入场区。
```

### 3. `breakout_confirm` 突破确认

适合突破型热点。

触发条件：

- 当前价接近或突破 `high_20`
- 成交确认较好：`volume_confirm >= 60` 或 `amount_ratio_5d >= 1.5`
- 分时不能高开低走
- 需要“站稳确认”，V1 可先用提示，P1 再接 15 分钟确认

产品文案：

```text
接近阶段压力位，需突破站稳后再考虑。
```

### 4. `do_not_chase` 暂不追

热点没坏，但当前买点风险大。

触发条件：

- 当前价高于追高警戒线
- 或 `intraday_confirm < 40`
- 或 `fund_flow < 40`
- 或放量但价格滞涨/回落

产品文案：

```text
舆情仍有热度，但交易确认不足，暂不追。
```

### 5. `avoid` 回避

不进入交易计划。

触发条件：

- 实时跌幅触发硬过滤，如 `realtime_pct_chg <= -5`
- `trade_signal_state == weak`
- 当前价跌破 `ma20` 且 `ma5 < ma10`
- 舆情强度退潮：`sector_heat < 45`
- 资金确认极弱：`fund_flow < 30`

产品文案：

```text
舆情或价格结构已失效，先回避。
```

## 买卖点计算

### 入场区间 `entry_zone`

按状态区别计算。

#### `buyable_now`

```text
entry_low = max(vwap, ma5)
entry_high = current_price
```

若没有可靠 `vwap`：

```text
entry_low = max(ma5, current_price - 0.5 * atr14)
entry_high = current_price
```

限制：

- 如果 `entry_high / entry_low - 1 > 3%`，说明区间过宽，转为 `wait_pullback`
- 如果当前价高于 `ma5 + 1.5 * atr14`，转为 `wait_pullback`

#### `wait_pullback`

```text
support_ref = max(ma5, ma10)
entry_low = support_ref - 0.3 * atr14
entry_high = support_ref + 0.2 * atr14
```

若当前价已经低于 `entry_high` 且分时确认未坏，可提示“接近低吸区”。

#### `breakout_confirm`

```text
breakout_price = high_20
entry_low = high_20 * 1.003
entry_high = high_20 * 1.015
```

V1 只提示“突破确认价”，不做自动触发。

P1 可加：

```text
突破后 15 分钟仍在 high_20 上方，且成交额继续放大，状态转 buyable_now。
```

#### `do_not_chase / avoid`

不输出可买入区间，或只输出观察区。

```text
entry_zone = null
watch_zone = [ma5, ma10]
```

### 止损 `stop_loss`

基础规则：

```text
risk_stop = entry_low - stop_atr_multiple * atr14
structure_stop = min(ma10, low_20)
stop_loss = max(risk_stop, structure_stop * 0.98)
```

不同状态建议倍数：

```text
buyable_now: 1.3
wait_pullback: 1.1
breakout_confirm: 1.6
```

还需要一个硬风控：

```text
hard_stop = entry_low * 0.94
stop_loss = max(stop_loss, hard_stop)
```

解释：

- 舆情短线不适合扛大亏
- 单票计划亏损尽量控制在 4%-6%
- 如果技术止损离入场太远，说明买点不合格，不应该硬买

### 止盈 `take_profit_1 / take_profit_2`

先用 R 倍数，再结合压力位截断。

```text
risk = entry_mid - stop_loss
sentiment_boost = clamp(sector_heat / 70, 0.8, 1.25)
take_profit_1 = entry_mid + 1.5 * risk * sentiment_boost
take_profit_2 = entry_mid + 2.5 * risk * sentiment_boost
```

压力位修正：

```text
if take_profit_1 > high_20 * 1.05:
    take_profit_1 = high_20 * 1.02

if take_profit_2 > high_20 * 1.12:
    take_profit_2 = high_20 * 1.08
```

若当前就是突破阶段，压力位要向上放宽：

```text
breakout_confirm 可允许 take_profit_2 到 high_20 * 1.15
```

### 追高警戒 `chase_warning`

```text
chase_warning = ma5 + 1.5 * atr14
```

若舆情极强且资金确认极强：

```text
chase_warning = ma5 + 2.0 * atr14
```

但不建议超过：

```text
ma5 + 2.5 * atr14
```

### 风险收益比 `risk_reward_ratio`

```text
entry_mid = (entry_low + entry_high) / 2
risk_reward_ratio = (take_profit_1 - entry_mid) / (entry_mid - stop_loss)
```

规则：

- `< 1.2`：性价比不足，转 `do_not_chase`
- `1.2 - 1.5`：可观察，不建议重仓
- `>= 1.5`：合格
- `>= 2.0`：较优

## 舆情/资金/分时联动

技术指标只决定交易位置，不反客为主。

### 舆情强度影响

影响：

- 是否允许追高警戒线放宽
- 止盈目标是否略微上移
- 持仓计划是否从 T+1 扩展到 T+3/T+5

建议口径：

```text
sector_heat >= 80 and stock_recognition >= 75:
    sentiment_level = strong
elif sector_heat >= 60:
    sentiment_level = normal
else:
    sentiment_level = weak
```

### 资金确认影响

影响：

- 是否从 `wait_pullback` 提升到 `buyable_now`
- 是否将 `do_not_chase` 改为 `avoid`

建议：

```text
fund_flow >= 60:
    allow_buyable_now = true
fund_flow < 40:
    no_chase = true
fund_flow < 30:
    avoid = true
```

### 分时确认影响

影响：

- 买点是否需要等回踩
- 当前价是否有效站稳
- 是否出现冲高回落风险

建议：

```text
intraday_confirm >= 65:
    可以使用当前价附近入场区
40 <= intraday_confirm < 65:
    等回踩到 ma5/vwap
intraday_confirm < 40:
    暂不追
```

## 输出字段设计

建议先不新增很多表，V1 可以随选股结果写入 `metadata_json.trade_plan`。后续如要做回测和历史版本追踪，再拆独立表。

### `trade_plan` JSON

```json
{
  "version": "a_share_sentiment_trade_plan_v1",
  "generated_at": "2026-06-11 14:30:00",
  "entry_style": "wait_pullback",
  "entry_label": "等回踩",
  "entry_zone": {
    "low": 11.8,
    "high": 12.1,
    "mid": 11.95,
    "method": "ma5_pullback_atr"
  },
  "watch_zone": {
    "low": 11.5,
    "high": 12.1
  },
  "stop_loss": {
    "price": 11.45,
    "method": "atr_and_ma10",
    "loss_pct_from_entry_mid": -4.18
  },
  "take_profit": [
    {
      "level": 1,
      "price": 13.1,
      "gain_pct_from_entry_mid": 9.62,
      "action": "减仓 1/3"
    },
    {
      "level": 2,
      "price": 14.0,
      "gain_pct_from_entry_mid": 17.15,
      "action": "再减 1/3 或移动止盈"
    }
  ],
  "chase_warning": {
    "price": 12.8,
    "reason": "高于 ma5 + 1.5 * atr14"
  },
  "risk_reward_ratio": 1.85,
  "holding_horizon": "T+1~T+3",
  "invalid_conditions": [
    "收盘跌破 11.45",
    "fund_flow < 30",
    "intraday_confirm < 40",
    "sector_heat < 45"
  ],
  "technical_snapshot": {
    "current_price": 12.3,
    "ma5": 12.0,
    "ma10": 11.6,
    "ma20": 10.9,
    "atr14": 0.42,
    "high_20": 13.4,
    "low_20": 9.8,
    "intraday_position": 0.66,
    "amount_ratio_5d": 1.35,
    "vwap": 12.08
  },
  "sentiment_snapshot": {
    "sector_heat": 78.5,
    "stock_recognition": 82.0,
    "fund_flow": 55.0,
    "intraday_confirm": 68.0,
    "trade_signal_state": "tradable"
  },
  "reason_summary": [
    "热点主题强度较高，个股处于板块前排",
    "价格仍在 MA5 上方且未明显偏离",
    "分时确认较强，当前可等待 MA5/VWAP 附近低吸"
  ],
  "risk_summary": [
    "若跌破 MA10 附近防线，短线交易计划失效",
    "若资金确认降至 30 以下，说明舆情未被资金继续验证"
  ]
}
```

## 页面展示

建议在选股中心和跟踪复盘都展示，但重点不同。

### 选股中心

展示“是否能买、怎么买”：

- 状态标签：立即可试 / 等回踩 / 突破确认 / 暂不追 / 回避
- 入场区间
- 止损价
- 止盈1 / 止盈2
- 追高警戒
- 风险收益比
- 简短理由

### 跟踪复盘

展示“计划是否有效”：

- 实际入选价是否落在建议区间
- 是否触发止损
- 是否到达止盈1 / 止盈2
- 最大浮盈/最大回撤与计划的偏差
- 计划失效原因

后续可做复盘字段：

```text
plan_hit_entry_zone
plan_hit_stop_loss
plan_hit_take_profit_1
plan_hit_take_profit_2
plan_max_gain_vs_target
plan_max_drawdown_vs_stop
```

## 最小落地顺序

### P0：只生成交易计划，不改选股结果

1. 新增技术指标计算 helper
   - 输入 code、trade_date、current realtime snapshot
   - 输出 MA/ATR/高低点/量比/日内位置

2. 新增 `a_share_sentiment` 专用 trade plan builder
   - 输入选股结果 item
   - 读取 item 中已有舆情/资金/分时因子
   - 输出 `trade_plan` JSON

3. 保存到 `selection_result.metadata_json.trade_plan`

4. 前端展示交易计划卡片

5. 跟踪复盘读取计划字段
   - 暂不做回测判定，只展示计划

### P1：计划表现复盘

接入已经修正过的秒级入选时间口径：

- 入选后最高价是否达到止盈
- 入选后最低价是否触发止损
- 实际最大浮盈/回撤相对计划是否合理
- 输出计划命中率

## 跟踪复盘判定逻辑

交易计划进入跟踪复盘后，不能只看“选股到现在涨跌多少”，还要判断当时给出的计划有没有被市场触发。

### 两种口径

V1 建议同时保留两种口径，但页面默认展示“计划触发口径”。

#### 1. 计划触发口径（推荐默认）

先判断入场区有没有被触及。

```text
如果入选后价格从未触及 entry_zone：
    entry_status = not_entered
    stop_loss / take_profit 不判定触发
    review_status = 未触发入场，计划继续观察或过期
```

这符合真实交易逻辑：没买进去，就不能说止盈或止损。

#### 2. 入选价买入口径（辅助复盘）

如果大X手动保存一条记录时，产品也可以提供一个辅助口径：

```text
assumed_entry_price = selected_price
```

这回答的是“如果我选股保存那一刻就买了，现在表现如何”。它适合统计策略信号质量，但不适合判断交易计划是否执行。

页面可以同时显示：

```text
计划口径：未触发入场
信号口径：入选价至今 +3.2%，最大浮盈 +5.8%，最大回撤 -1.4%
```

### 触发顺序

复盘需要按时间顺序判断：

1. `selection_datetime` 之后开始统计
2. 先判断是否触及 `entry_zone`
3. 触及入场后，才开始判断 `stop_loss / take_profit`
4. 若分钟数据可用，按分钟顺序判定先后
5. 若只有日线数据，且同一天同时触及止损和止盈，标记为 `ambiguous_same_day`

### 入场触发

```text
entry_touched = price_low_after_selection <= entry_zone.high
                AND price_high_after_selection >= entry_zone.low
```

如果入选价已经落在入场区内：

```text
entry_touched = true
entry_source = selected_price
entry_time = selection_datetime
```

如果之后才回踩/突破触发：

```text
entry_source = intraday_bar 或 daily_bar
entry_time = first_touch_time
```

### 止盈止损触发

入场后：

```text
stop_loss_hit = min_price_after_entry <= stop_loss.price
take_profit_1_hit = max_price_after_entry >= take_profit[0].price
take_profit_2_hit = max_price_after_entry >= take_profit[1].price
```

状态优先级建议：

```text
not_entered
entered_active
stop_loss_hit
take_profit_1_hit
take_profit_2_hit
ambiguous_same_day
expired
invalidated_by_signal
```

如果能确定时间顺序：

```text
先触发 stop_loss，再触发 take_profit，不算止盈
先触发 take_profit_1，再回落，可标记 take_profit_1_hit_then_pullback
```

### 计划过期

舆情短线不能无限等。

建议：

```text
buyable_now: 1 个交易日未入场则过期
wait_pullback: 3 个交易日未入场则过期
breakout_confirm: 2 个交易日未突破则过期
do_not_chase: 不进入计划，只观察
avoid: 不进入计划
```

过期不是失败，是“计划未执行”。

## 样例

以下样例用现有历史记录手工套用 V1 规则，目的是看页面展示和复盘语义，不代表最终参数已经固定。

### 样例 A：诺德股份 `sh.600110`

入选信息：

```text
入选时间：2026-06-10 14:47:26
入选价：12.21
当前价：12.10
入选至今收益：-0.90%
最大浮盈：+1.06%
最大回撤：-2.54%
舆情热度：78.05
资金确认：55.92
分时确认：56.12
```

技术快照：

```text
MA5 = 11.53
MA10 = 11.81
MA20 = 10.95
ATR14 = 0.95
20日高点 = 13.19
20日低点 = 9.20
5日成交额比 = 1.42
```

交易计划：

```json
{
  "entry_style": "wait_pullback",
  "entry_label": "等回踩",
  "entry_zone": {
    "low": 11.53,
    "high": 12.00,
    "method": "ma10_pullback_atr"
  },
  "stop_loss": {
    "price": 11.06,
    "method": "hard_stop_6pct_from_entry_mid"
  },
  "take_profit": [
    {"level": 1, "price": 12.96, "action": "减仓 1/3"},
    {"level": 2, "price": 13.56, "action": "再减 1/3 或移动止盈"}
  ],
  "chase_warning": {
    "price": 13.24,
    "reason": "MA5 + 1.8 * ATR14"
  },
  "risk_reward_ratio": 1.68,
  "invalid_conditions": [
    "收盘跌破 11.06",
    "fund_flow < 30",
    "intraday_confirm < 40",
    "sector_heat < 45"
  ],
  "reason_summary": [
    "舆情热度和板块辨识度较强",
    "资金确认中等，分时确认不足以支持追高",
    "入选价高于建议区间，适合等回踩"
  ]
}
```

跟踪复盘：

```text
计划口径：
- 入选价 12.21 高于 entry_zone.high 12.00，入选时不视为计划成交
- 入选后最大回撤 -2.54%，推算最低价约 11.90，触及 entry_zone
- 计划入场已触发
- 入场后未触发止损 11.06
- 入场后未触发止盈1 12.96
- 当前状态：entered_active，继续跟踪

信号口径：
- 如果按入选价 12.21 立即买入，当前 -0.90%
- 最大浮盈 +1.06%，最大回撤 -2.54%
```

页面展示建议：

```text
交易计划：等回踩
建议入场：11.53 - 12.00
当前复盘：已触及入场区，未触发止盈/止损
信号表现：-0.90%，最大浮盈 +1.06%，最大回撤 -2.54%
```

### 样例 B：宗申动力 `sz.001696`

入选信息：

```text
入选时间：2026-06-10 14:47:27
入选价：19.04
当前价：20.96
入选至今收益：+10.08%
最大浮盈：+10.08%
最大回撤：0.00%
舆情热度：84.22
资金确认：70.47
分时确认：66.41
```

技术快照：

```text
MA5 = 16.27
MA10 = 15.53
MA20 = 15.80
ATR14 = 0.77
20日高点 = 19.31
20日低点 = 13.98
5日成交额比 = 6.36
```

交易计划：

```json
{
  "entry_style": "breakout_confirm",
  "entry_label": "突破确认",
  "entry_zone": {
    "low": 19.37,
    "high": 19.60,
    "method": "high20_breakout_confirm"
  },
  "stop_loss": {
    "price": 18.32,
    "method": "hard_stop_6pct_from_entry_mid"
  },
  "take_profit": [
    {"level": 1, "price": 21.60, "action": "减仓 1/3"},
    {"level": 2, "price": 23.15, "action": "再减 1/3 或移动止盈"}
  ],
  "chase_warning": {
    "price": 17.81,
    "reason": "价格已显著高于 MA5 + 2 * ATR14，普通低吸口径不适用，只能按突破确认"
  },
  "risk_reward_ratio": 1.78,
  "invalid_conditions": [
    "跌回 19.31 下方且无法收回",
    "收盘跌破 18.32",
    "fund_flow < 30",
    "intraday_confirm < 40"
  ],
  "reason_summary": [
    "舆情热度强，资金和分时确认较强",
    "成交额明显放大，具备突破交易条件",
    "价格远离 MA5，不适合低吸，只适合突破确认"
  ]
}
```

跟踪复盘：

```text
计划口径：
- 入选价 19.04 低于突破确认区 19.37 - 19.60，入选时不视为计划成交
- 入选后最高价达到 20.96，已触及突破入场区
- 入场后未触发止损 18.32
- 入场后未触发止盈1 21.60
- 当前状态：entered_active，浮盈中但未到计划止盈

信号口径：
- 如果按入选价 19.04 立即买入，当前 +10.08%
- 最大浮盈 +10.08%，最大回撤 0.00%
```

页面展示建议：

```text
交易计划：突破确认
建议入场：19.37 - 19.60
当前复盘：已触发入场，未到止盈1，未触发止损
信号表现：+10.08%，最大浮盈 +10.08%，最大回撤 0.00%
```

### 样例 C：科大讯飞 `sz.002230`

入选信息：

```text
入选时间：2026-05-29 09:52:04
入选价：50.02
当前价：40.61
入选至今收益：-18.81%
最大浮盈：0.00%
最大回撤：-18.81%
舆情热度：81.54
资金确认：20.09
分时确认：68.59
```

技术快照：

```text
MA5 = 44.56
MA10 = 46.47
MA20 = 47.41
ATR14 = 1.86
20日高点 = 50.99
20日低点 = 42.11
5日成交额比 = 1.03
```

交易计划：

```json
{
  "entry_style": "do_not_chase",
  "entry_label": "暂不追",
  "entry_zone": null,
  "watch_zone": {
    "low": 46.0,
    "high": 47.5,
    "method": "ma10_ma20_watch"
  },
  "stop_loss": null,
  "take_profit": [],
  "chase_warning": {
    "price": 49.78,
    "reason": "资金确认过弱，且价格接近阶段高位"
  },
  "risk_reward_ratio": null,
  "invalid_conditions": [
    "fund_flow < 30，交易计划不成立",
    "跌破 MA20 后仍无法收回"
  ],
  "reason_summary": [
    "舆情热度强，但资金确认只有 20.09",
    "不满足舆情短线的资金验证要求",
    "不应给买入计划，只给观察区"
  ]
}
```

跟踪复盘：

```text
计划口径：
- 原计划为暂不追，没有有效 entry_zone
- 不判定止盈止损
- 当前状态：plan_not_actionable，原计划避免了后续大幅回撤

信号口径：
- 如果按入选价 50.02 立即买入，当前 -18.81%
- 最大浮盈 0.00%，最大回撤 -18.81%
- 说明“资金确认过弱时不追”的规则有价值
```

页面展示建议：

```text
交易计划：暂不追
当前复盘：原计划未给入场，避免追高亏损
信号表现：-18.81%，最大回撤 -18.81%
```

### P2：交易计划回测

按历史 `selection_result` 重放：

- 每条记录使用当时的日线/分钟线
- 生成当时的交易计划
- 用后续 T+1/T+5 行情检验
- 统计各 `entry_style` 的胜率、盈亏比、止损率、止盈率

### P3：盘中动态刷新

每 5 分钟刷新：

- `entry_style`
- `entry_zone`
- `chase_warning`
- `invalid_conditions`

仍不自动下单，只更新 UI。

## 风险与约束

### 1. 不要让技术指标反客为主

舆情策略的主因子仍是舆情和主题辨识度。技术指标只负责：

- 买点
- 风控
- 追高警戒

不能因为技术形态漂亮，就让弱舆情股票进入舆情策略。

### 2. 不要过早做复杂模型

V1 不做：

- 复杂形态识别
- 缠论买卖点
- 机器学习价格预测
- 自动仓位优化
- AI 自由生成买卖点

先用固定规则跑出可回测样本。

### 3. 分钟数据质量要明确降级

如果 `vwap` 或分钟数据不足：

- 可以降级到日线 MA/ATR
- `entry_style` 不应给 `buyable_now`
- 文案提示“分钟数据不足，按日线计划参考”

### 4. 涨停/停牌/流动性特殊处理

需要额外状态：

```text
limit_up_unbuyable
suspended
liquidity_insufficient
```

涨停不是“立即可买”，而是“强但不可执行”。

### 5. 止损必须机械

舆情短线最怕逻辑还在、价格先崩。交易计划里止损不能写成模糊文案，必须有明确价格和失效条件。

## 推荐 V1 默认规则

可以先从这组参数开始，不调太复杂：

```yaml
trade_plan:
  enabled: true
  strategy_scope:
    - a_share_sentiment
  ma_short: 5
  ma_mid: 10
  ma_long: 20
  atr_period: 14
  high_low_period: 20
  min_risk_reward_ratio: 1.2
  good_risk_reward_ratio: 1.5
  max_single_trade_loss_pct: 6
  chase_atr_multiple: 1.5
  strong_chase_atr_multiple: 2.0
  stop_atr_multiple:
    buyable_now: 1.3
    wait_pullback: 1.1
    breakout_confirm: 1.6
  state_thresholds:
    strong_sector_heat: 70
    strong_stock_recognition: 70
    min_fund_flow_buyable: 50
    weak_fund_flow: 40
    avoid_fund_flow: 30
    min_intraday_buyable: 60
    weak_intraday: 40
    chase_intraday_position: 0.80
```

## 下一步建议

下一步先不急着开发完整链路，建议先做两件事：

1. 用最近 20 条 `a_share_sentiment` 结果手算/脚本生成一版 `trade_plan` 样例，检查文案和点位是否符合直觉。
2. 再定前端展示形态：选股中心卡片里展示精简版，跟踪复盘展示计划命中情况。

确认后再进入 P0 实现。
