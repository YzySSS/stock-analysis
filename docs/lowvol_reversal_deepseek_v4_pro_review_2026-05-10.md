# lowvol_reversal DeepSeek v4 pro 策略审查（2026-05-10）

## 一、结论

当前 `lowvol_reversal` 策略**完全不能信任，回测结果没有策略含义，只是一种“伪信号＋代码排序”的假象**。

- 四个月的回测只选出 3 只股票，且每天得分完全相同（63.10），这并不是因为这三只股票真的符合“低波动 + 反转”逻辑，而是因子公式本身让大量股票获得相同总分，而后端排序缺乏打破平局的机制，导致固定取到 `ORDER BY sb.code` 的前 3 只股票。  
- 策略语义严重扭曲：名字叫“低波动 + 短期反转”，实际使用的三个因子 `turnover_score`、`lowvol_score`、`reversal_score` 全部由 PE/PB/ROE、价格区间、数据完整性等**静态基本面条件**合成，与价格波动、反转幅度、换手活跃度等时间序列行为几乎无关。  
- 因此，无论是回测绩效还是选股结果，都只是机械的数据拍平结果，**不存在任何策略超额效果**，继续使用会导致严重误导。

---

## 二、根因：四层偏差的叠加

### 1. 因子语义错位（最根本问题）

策略配置文件宣称的因子：

| 因子    | 宣称含义           | 实际所用字段           | 实际含义               | 与宣称的差距                                           |
| ------- | ------------------ | ---------------------- | ---------------------- | ------------------------------------------------------ |
| turnover | 换手活跃度         | `turnover_score`       | 30% 稳定性 + 20% 数据质量 ｜ 与换手率无关              |
| lowvol  | 低波稳定性         | `lowvol_score`        | 35% 估值 + 20% 稳定性    ｜ 与价格波动幅度无关          |
| reversal| 短期反转弹性       | `reversal_score`      | 35% 常数 + 25% 质量 + 15% 估值 ｜ 与过去涨跌幅无关     |

这些合成分数的计算完全来自 `_build_candidate()`，其输入都是**同一交易日**的 PE、PB、ROE、收盘价区间、数据是否缺失等，没有任何窗口滚动或时间序列计算。  
因此：

- **turnover_score** ≈ 数据是否齐全 + 股价是否在 5~60 元 + PE/PB 是否合理 ⇒ 本质是“数据质量 + 估值稳定性”得分。  
- **lowvol_score** ≈ ROE、毛利率、营收增速等（价值 + 稳定性），与波动率无关。  
- **reversal_score** ≈ 常数 0.35 + 基本面质量，完全不是反转（过去跌得多才期待反弹）。

### 2. 因子离散化导致大量同分

`value_score`、`quality_score`、`stability_score`、`data_quality_score` 都是通过少数几个离散条件累加的阶梯函数（例如 PE 0~20 加 0.30，20~35 加 0.18 等）。全市场 5000+ 股票最终只能产生少数几种分数组合。在一个交易日，**63.10 分的股票高达 206 只**，top15 全是同分。这是候选 Score 同质化的直接原因。

### 3. 排序无 tie‑breaker，被候选顺序锁死

策略 `score()` 只按总分降序排列，未定义二级排序：

```python
return sorted(scored, key=lambda x: x.get("score", 0), reverse=True)
```

回测候选加载 SQL 尾部是 `ORDER BY sb.code`，因此每天具有相同总分的股票会按代码字母排列。`select()` 仅取前 3（`max_picks=3`），导致永远选出代码最小的三只股票（600039、600062、600566）。这就是 80 个交易日完全固定选股的机制。

### 4. 数据口径不一致

回测 SQL 和策略并没有利用任何时序字段去计算低波 / 反转 / 换手，却仍把这些标签性得分当作因子使用。这导致整个回测仿佛在一条“磨平的”横截面数据上运行，完全没有策略空间。

---

## 三、继续使用的风险

1. **回测结论欺诈性**  
   现有回测会展示一条“3 只股票持续持有”的净值曲线，看起来很稳健，实际上只是按代码顺序选取了基本面评分稍高的几只是庄股。任何将此回测与实盘、策略比较的行为都会产生严重误导。

2. **线上选股完全随机**  
   若将相同逻辑部署到实盘生产，每天会固定选出“同分池中代码最小的 3 只”，并非基于任何预期收益或风险特征，选股结果与市场状态无关。

3. **策略归因错误**  
   若有人据此认为“低波动+反转有效”，会误把基本面代理分数当作真正的因子，影响整个因子库的后续研究和资金分配。

4. **系统信誉受损**  
   一旦被审查或审计发现这种低劣的因子构造，平台可解释性、合规性将受到严重质疑。

---

## 四、短期修复方案（最小改动，立即止血）

在完全重构因子之前，必须至少打破“同分固定选股”并暴露异常。

### 4.1 策略层添加 tie‑breaker

修改 `select()`，在同分时引入次级排序（例如 `total_mv` 从小到大，或 `turnover_rate` 从大到小），同时加入**随机扰动**防止完全按候选顺序。

```python
def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    threshold = float(self.config.get("score_threshold", 0))
    max_picks = int(self.config.get("max_picks", 5))
    # 过滤阈值
    qualified = [x for x in scored_stocks if float(x.get("score", 0)) >= threshold]
    # tie‑breaker：score 降序，total_mv 升序，再加随机哈希
    import hashlib
    qualified.sort(
        key=lambda x: (
            -float(x.get("score", 0)),
            float(x.get("total_mv") or 1e9),
            hashlib.md5(x["code"].encode()).hexdigest()
        )
    )
    return qualified[:max_picks]
```

同时，在 `explain()` 中加入 `"tie_break": ...` 以便后续观察。

### 4.2 增加同分诊断日志/页面

在策略 `score()` 返回之前，统计不同得分的股票数量并写入日志或返回给调度层：

```python
from collections import Counter
score_counts = Counter(round(x["score"], 4) for x in scored)
top_scores = score_counts.most_common(3)
logging.info(f"同分诊断 top3: {top_scores}")
```

在回测报告/前端展示中增加“同分数候选数”字段，若同分池规模大于某个阈值（如 50）则标红警告。

### 4.3 临时禁止继续运行

在根本重构完成前，**应下线该策略的回测与选股入口**，避免产生更多虚假结果。可临时改名称 `lowvol_reversal` 为 `fundamental_proxy_deprecated`，并禁止在 Web 界面新建任务。

---

## 五、根本重构方案：真正的 lowvol_reversal 因子体系

我们要设计三个**以行情时间序列为基础的因子**，并保证横截面上的区分度。

### 5.1 因子定义

#### 5.1.1 换手活跃度 `turnover`

- **原始值**：当日 `turnover_rate`（已存在）。
- **稳健处理**：取最近 5 日平均换手率，若缺失用当日值。
- **方向**：越高代表资金关注度越高（正向）。
- **处理**：横截面 Winsorize（1% / 99% 分位）后 rank 百分比。

#### 5.1.2 低波动 `lowvol`

- **原始值**：过去 20 个交易日收益率的标准差（日波动率）。  
  现有候选表未提供标准差，但可以通过近似：  
  `amplitude = (max_close_20 - min_close_20) / close` 是 20 日振幅，它与波动率高度正相关。  
  若后续能扩展 SQL，可直接计算 `STDDEV(daily return)`。
- **方向**：波动越低越好 ⇒ 因子值应为 **振幅的负值**（或 `1 / (1 + amplitude)`）。
- **处理**：截面 rank 百分比，高分对应低波动。

#### 5.1.3 短期反转 `reversal`

- **原始值**：过去 5 个交易日的累计收益率（或 20 日）。  
  候选表中有 `close_20d`（20 天前的收盘价），可用 20 日收益率 `ret_20d = (close - close_20d) / close_20d`。  
  为避免长窗口过于钝化，可再结合短期信号：通过 `close` 和 `ma20` 也能计算近期动量，但数据更全后可以取 5 日或 10 日收益。
- **方向**：反转意味“过去跌得多，未来大概率反弹” ⇒ 取 **负的收益率** 作为因子值，即 `-ret_20d`。
- **处理**：截面 rank 百分比，高分给下跌最深的股票。

### 5.2 归一化方式：百分位排名（Percentile Rank）

每天在全市场候选股票（或满足流动性过滤的子集）中，对每个因子计算**百分位排名**（0~1），取代 min‑max 归一化。  
优点：

- 分布均匀，最大限度避免同分；
- 对异常值不敏感；
- 各因子量纲统一。

实现伪代码（对因子值列表 `values`）：

```python
import numpy as np
def percent_rank(values):
    arr = np.array(values)
    # 处理缺失值：置为中位数
    med = np.median(arr)
    arr = np.where(np.isnan(arr), med, arr)
    # 用排序编号
    order = arr.argsort()
    rank = np.empty_like(order)
    rank[order] = np.arange(1, len(arr)+1)
    # 最大 1，最小 0
    return (rank - 1) / (len(arr) - 1) if len(arr) > 1 else np.zeros_like(arr)
```

### 5.3 综合得分

```python
score = w_t * rank_turnover + w_l * rank_lowvol + w_r * rank_reversal
```

权重沿用配置文件的值，但在新策略中建议调优，例如 `w_t=0.3, w_l=0.4, w_r=0.3`。

### 5.4 过滤条件（必须）

在计算秩之前，应剔除不合格股票，避免它们污染百分位排名：

- 剔除 `is_st=True`；
- 剔除 `kline_count_20 < 15`（停牌过多）；
- 剔除 `avg_amount_20 < 1e6`（日均成交额低于 100 万，流动性极差）；
- 剔除 `close <= 1`（仙股或异常）；
- 可选：`total_mv < threshold` 过滤小盘垃圾。

---

## 六、推荐 V1 可落地算法（直接实现）

基于现有候选数据（无需修改 SQL），我们可以立即构建一个**真实且合理**的 lowvol_reversal 策略。

### 6.1 策略 `compute_factors` 改造

在 `LowVolReversalStrategy.compute_factors()` 中，放弃使用 `item` 里携带的伪 factor，重新从原始行情字段计算。

```python
def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = data_bundle.get("candidates", [])
    # ---------- 过滤 ----------
    valid = []
    for s in candidates:
        if s.get("is_st"):
            continue
        if s.get("kline_count_20", 0) < 15:
            continue
        if (s.get("avg_amount_20") or 0) < 1e6:
            continue
        if (s.get("close") or 0) <= 1:
            continue
        valid.append(s)

    # ---------- 原始因子计算 ----------
    raws = []
    for s in valid:
        close = s["close"]
        turnover = s.get("turnover_rate")
        if turnover is None:
            turnover = np.nan
        # lowvol: 负振幅
        max_c = s.get("max_close_20")
        min_c = s.get("min_close_20")
        if close and max_c is not None and min_c is not None and max_c != min_c:
            amplitude = (max_c - min_c) / close
            raw_lowvol = -amplitude
        else:
            raw_lowvol = np.nan
        # reversal: 负20日收益率
        close_20d = s.get("close_20d")
        if close and close_20d and close_20d != 0:
            ret20 = (close - close_20d) / close_20d
            raw_reversal = -ret20
        else:
            raw_reversal = np.nan
        raws.append({
            "stock": s,
            "raw_turnover": turnover,
            "raw_lowvol": raw_lowvol,
            "raw_reversal": raw_reversal,
        })

    # ---------- 百分位排名 ----------
    def safe_rank(values):
        arr = np.array(values, dtype=float)
        med = np.nanmedian(arr)
        arr = np.where(np.isnan(arr), med, arr)
        order = arr.argsort()
        rank = np.empty_like(order, dtype=float)
        rank[order] = np.arange(1, len(arr)+1)
        return (rank - 1) / (len(arr) - 1) if len(arr) > 1 else np.ones(len(arr)) * 0.5

    t_vals = [r["raw_turnover"] for r in raws]
    l_vals = [r["raw_lowvol"] for r in raws]
    r_vals = [r["raw_reversal"] for r in raws]
    rank_t = safe_rank(t_vals)
    rank_l = safe_rank(l_vals)
    rank_r = safe_rank(r_vals)

    for i, entry in enumerate(raws):
        entry["factors"] = {
            "turnover": round(rank_t[i], 4),
            "lowvol": round(rank_l[i], 4),
            "reversal": round(rank_r[i], 4),
        }
    return [ {**e["stock"], "factors": e["factors"]} for e in raws ]
```

### 6.2 得分计算与 tie‑breaker

沿用已有 `score()`，但评分对象已是百分位排名。在 `select()` 中增加二级、三级排序：

```python
def select(self, scored_stocks):
    threshold = float(self.config.get("score_threshold", 0))
    max_picks = int(self.config.get("max_picks", 5))
    qualified = [x for x in scored_stocks if x["score"] >= threshold]
    qualified.sort(
        key=lambda x: (
            -x["score"],
            float(x.get("total_mv") or 1e20),      # 市值小优先（避免权重过度集中）
        )
    )
    # 可选：市值相似时按换手率降序
    return qualified[:max_picks]
```

### 6.3 配置文件调整

- `score_threshold` 可设 0（因为已是百分位合成得分，阈值意义不大），或者设为 0.6，保留过滤。
- 权重保持或调整为 0.3/0.4/0.3。

---

## 七、回测验证方案（续）

重构后必须系统验证同质化是否消失，以及因子是否真正有效。

### 7.1 必看指标（续）

2. **每日得分分布**（续）  
   - 绘制每日所有候选股票的得分直方图或核密度图，观察是否集中在某个区间（如 0.4–0.6），若分布极度集中，说明因子区分度低，组合实际依赖 tie‑breaker（如市值），容易再度陷入同质化。  
   - 计算每日得分的标准差，理想状态下应在 0.15 以上（百分位合成得分理论标准差约 0.2）。若持续低于 0.1，则该复合因子无效。

3. **同分池大小**  
   统计每日满足 `score >= threshold` 的股票数量，如果经常低于 5，意味着阈值可能过严或因子失效；若常高于 100，则组合筛选主要依赖二级排序（市值），需分析二级排序的逻辑合理性。

4. **行业集中度**  
   - 按申万一级行业（或交易所行业分类）统计组合的行业分布，计算每日持仓的行业赫芬达尔指数（HHI），并与市场等权基准对比。  
   - 经验阈值：单行业占比不应连续超过 40%，否则需要检查因子是否隐含了行业偏好。

5. **换手率（组合层面）**  
   - 每日计算 `换手率 = 卖出只数 / 持仓只数`，并观察分布。  
   - 如果平均换手率 > 0.8（即几乎每天都全换），可能表明信号不稳定，需要调高因子权重或引入动量平滑。

6. **收益归因**  
   - 使用 Brinson 或因子归因模型（如 Fama‑French 五因子 + 波动率因子 + 反转因子），将组合超额收益拆分为因子配置贡献和个股选择贡献。  
   - 重点确认：组合超额收益确实来源于预期的低波和反转，而非指数风格暴露或行业押注。若反转因子贡献显著为负，说明重构后反转因子可能被过度降权或定义失效。

7. **对照组设置**  
   - **基准策略**：原始 `lowvol_reversal`（未重构）回测结果。  
   - **单因子对照组**：仅使用 `lowvol` 百分位排名（权重1）、仅使用 `reversal` 百分位排名（权重1）、仅使用 `turnover` 百分位排名（权重1）的等权组合。  
   - **等权100股票组合**：从候选池等权抽取100只，用于剔除市场微盘效应。  
   - 比较指标：年化收益率、夏普比率、最大回撤、月度胜率、信息比率。

---

## 八、数据需求

### 8.1 当前候选池 `candidates` 可用字段
（假设 `data_bundle.get("candidates")` 返回字典列表，每个字典包含以下字段，需实际核对）：

- 基础：`code`, `name`, `close`, `total_mv`, `circ_mv`
- 技术：`turnover_rate`（或 `turnover`）, `max_close_20`, `min_close_20`, `close_20d`, `avg_amount_20`, `kline_count_20`
- 状态：`is_st`, `listed_days`
- 可能包含：`pe`, `pb`, `roe`（若系统默认输出）

**这些字段已足够支撑重构后的因子计算：**
- 振幅：`max_close_20`, `min_close_20`
- 20日收益率：`close`, `close_20d`
- 换手率：`turnover_rate`
- 候选池规模过滤：`avg_amount_20`
- tie‑breaker 市值：`total_mv`

**无需新增数据源。**

### 8.2 如需更精细化（可选）
- 添加 `industry_code`（申万一级）以执行行业集中度监控，可从 `daily_kline` 或基础信息表关联获取。
- 若需计算历史波动率（替代振幅），需要每日收益率序列，可从 `daily_kline` 表计算窗口波动率。
- 推荐在 `factor_input_daily` 中预计算并缓存窗口波动率、历史收益、平均换手率等，避免每个策略重复计算。

---

## 九、命名建议

当前名称 `lowvol_reversal` 虽概括了原始两个因子，但在重构后已加入换手率，本质上是一个**三因子百分位合成策略**，且逻辑发生根本变化。

**建议：**
- **保留原名**：如果团队习惯于识别该因子组合，且文档已注明重构，可继续使用 `lowvol_reversal`，但在代码注释和策略说明中明确“已重构为百分位排名复合因子（低波 + 反转 + 换手率）”。
- **更清晰的替代名**：`percentile_lowvol_reversal` 或 `composite_lvr`（low vol + reversal），强调百分位合成，避免与原始线性组合混淆。
- **反对使用 proxy/deprecated**：该策略仍有保留价值（尤其是重构后可能有效），不应标记为废弃；除非回测证明其无法产生稳定超额，再考虑废弃并移至 `archive/`。

**最终推荐：** 使用 `lowvol_reversal` 并追加版本号，如 `lowvol_reversal_v2`，便于回溯和对比。

---

## 十、实施优先级

### 10.1 短期（1‑2 周内）
1. 按照第六章提供的重构代码实现因子计算与选择逻辑，完成初次部署。
2. 运行回测，检查 7.1 中的每日得分标准差、同分池大小、行业集中度，快速判断因子有效性。
3. 若标准差 < 0.1，考虑调整权重或剔除无效子因子（如 reversal 的 20 日窗口可能太短）。
4. 设置简单的监控 dashboard，展示每日组合持仓、得分分布。

### 10.2 中期（1‑3 个月）
1. 如果短期验证有效，进行参数敏感性测试：  
   - 不同回溯窗口（10 日 vs 20 日 vs 60 日）  
   - 权重组合优化（等权、IC 加权、波动率倒数加权）  
   - 阈值和最大持仓数调整。
2. 引入行业中性处理：在 `select()` 阶段加入行业分散约束（例如单行业最多 2 只），避免过度集中。
3. 实现收益归因的自动化脚本，集成到回测报告中。

### 10.3 长期（3‑6 个月）
1. 如果策略持续有效，将其纳入实盘模拟，并考虑使用日内 VWAP 滑点模型。
2. 探索引入风险因子（如 Beta 中性），使用多因子回归分离 alpha。
3. 若策略表现不佳或市场结构变化，考虑回收该策略的计算资源，并归档为示例策略，在文档中说明其失效原因。

---

## 十一、最终建议摘要

- **重构核心**：将原始低波和高反转的线性得分替换为各自在候选池中的百分位排名，并引入换手率百分位排名，三者加权求和得到最终得分，市值作为 tie‑breaker。  
- **验证关键**：必须通过每日得分标准差、行业集中度和收益归因，确认同质化得到解决且 alpha 来源清晰。  
- **数据就绪**：现有候选池字段完全满足重构需要，无需额外数据。  
- **命名**：保留 `lowvol_reversal` 并添加版本号 `v2`，明确重构标记，不建议废弃。  
- **路线**：快速上线验证 → 参数优化与行业中性 → 长期根据表现决定去留。  

**一句话**：这不是小修小补，而是因子合成逻辑的根本升级，能在不增加数据成本的前提下，显著改善策略的分散度和信号有效性。
