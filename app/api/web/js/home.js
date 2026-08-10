function formatAmount(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  const n = Number(value);
  if (Math.abs(n) >= 100000000) return `${(n / 100000000).toFixed(1)}亿`;
  if (Math.abs(n) >= 10000) return `${(n / 10000).toFixed(1)}万`;
  return n.toFixed(0);
}

function formatCompactAmount(value) {
  const num = Number(value);
  if (value == null || Number.isNaN(num)) return '-';
  if (Math.abs(num) >= 100000000) return `${(num / 100000000).toFixed(1)}亿`;
  if (Math.abs(num) >= 10000) return `${(num / 10000).toFixed(1)}万`;
  return num.toFixed(0);
}

function formatSectorAmount(value, unit) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  if (unit === '亿元') return `${Number(value).toFixed(1)}亿`;
  return formatAmount(value);
}

function setText(selector, value) {
  const element = qs(selector);
  if (element) element.textContent = value;
}

function setWidth(selector, value) {
  const element = qs(selector);
  if (element) element.style.width = `${Math.max(0, Math.min(100, Number(value) || 0))}%`;
}

function setLeftPercent(selector, value) {
  const element = qs(selector);
  if (element) element.style.left = `${Math.max(0, Math.min(100, Number(value) || 0))}%`;
}

function renderSectorList(selector, sectors, emptyText) {
  const container = qs(selector);
  if (!container) return;
  if (!sectors || !sectors.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }
  container.innerHTML = sectors.slice(0, 5).map((item, index) => {
    const typeLabel = item.sector_type_label || '行业';
    const isFundFlow = item.net_amount != null;
    const mainValue = isFundFlow ? formatSectorAmount(item.net_amount, item.source_unit) : formatPercent(item.amount_weighted_pct_chg ?? item.avg_pct_chg);
    const extraLine = isFundFlow
      ? `领涨 ${escapeHtml(item.leading_stock || '-')} ${formatPercent(item.leading_stock_pct_chg)} · 涨幅 ${formatPercent(item.avg_pct_chg)}`
      : `上涨 ${escapeHtml(item.up_count ?? 0)} · 下跌 ${escapeHtml(item.down_count ?? 0)}`;
    return `
      <a class="home-sector-row" href="/selection">
        <span class="sector-rank">${index + 1}</span>
        <span class="sector-name">
          <strong>${escapeHtml(item.name || '-')}</strong>
          <small>${escapeHtml(typeLabel)} · ${escapeHtml(item.stock_count ?? '-')} 只</small>
        </span>
        <span class="sector-flow">
          <b>${mainValue}</b>
          <small>${extraLine}</small>
        </span>
      </a>
    `;
  }).join('');
}

function formatHotThemeStock(stock) {
  if (!stock) return '';
  if (typeof stock === 'string') return stock;
  return stock.name || stock.stock_name || stock.code || '';
}

function renderHotThemes(data = {}) {
  const summary = qs('#home-hot-theme-summary');
  const container = qs('#home-hot-themes');
  const items = data.items || [];
  if (summary) {
    summary.textContent = data.as_of ? `更新 ${data.as_of} · 每 15 分钟` : '暂无热点主题快照';
  }
  if (!container) return;
  if (!items.length) {
    container.innerHTML = '<div class="empty-state">暂无热点主题数据</div>';
    return;
  }
  container.innerHTML = items.slice(0, 8).map((item, index) => {
    const stocks = (item.top_stocks || []).map(formatHotThemeStock).filter(Boolean).slice(0, 3).join(' / ') || '-';
    const firstNews = (item.top_news || [])[0] || {};
    const fundSummary = item.net_amount != null
      ? `资金 ${formatCompactAmount(item.net_amount * 100000000)} · 涨幅 ${formatPercent(item.pct_chg)} · 领涨 ${item.leading_stock || '-'}`
      : '';
    const newsTitle = typeof firstNews === 'string' ? firstNews : (firstNews.title || firstNews.summary || '');
    const positive = Number(item.positive_news_count || 0);
    const negative = Number(item.negative_news_count || 0);
    const toneClass = positive >= negative ? 'up' : 'down';
    const displayScore = item.hot_score ?? item.sector_score;
    const detailLine = item.fund_flow_score != null
      ? `资金 ${formatNumber(item.fund_flow_score, 1)}${item.opinion_match_score != null ? ` · 舆情共振 ${formatNumber(item.opinion_match_score, 1)}` : ''}${item.ths_score != null ? ` · 同花顺 ${formatNumber(item.ths_score, 1)}` : ''}`
      : item.ths_score != null
        ? `同花顺 ${formatNumber(item.ths_score, 1)} · 成分 ${item.ths_member_count ?? item.stock_count ?? '-'}`
        : `来源 ${item.source_count ?? '-'} · 新闻 ${item.news_count ?? '-'}`;
    return `
      <article class="home-hot-theme-card">
        <div class="hot-theme-rank">${index + 1}</div>
        <div class="hot-theme-main">
          <div class="hot-theme-title-row">
            <strong>${escapeHtml(item.sector_name || '-')}</strong>
            <span>${escapeHtml(item.sector_type_label || item.sector_type || '-')}</span>
          </div>
          <div class="hot-theme-news">${escapeHtml(newsTitle || fundSummary || '暂无热点新闻摘要')}</div>
          <div class="hot-theme-stocks">关联个股：${escapeHtml(stocks)}</div>
        </div>
        <div class="hot-theme-metrics">
          <b>${formatNumber(displayScore, 1)}</b>
          <span>热度分</span>
          <small class="${toneClass}">正 ${positive} / 负 ${negative}</small>
          <small>${escapeHtml(detailLine)}</small>
        </div>
      </article>
    `;
  }).join('');
}

function splitDateTime(value) {
  if (!value) return { date: '-', time: '--:--:--' };
  const parts = String(value).split(' ');
  return { date: parts[0] || '-', time: parts[1] || parts[0] || '--:--:--' };
}

function shanghaiNow() {
  const formatter = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = Object.fromEntries(formatter.formatToParts(new Date()).map((part) => [part.type, part.value]));
  return new Date(`${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}+08:00`);
}

function formatHms(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const hours = String(Math.floor(total / 3600)).padStart(2, '0');
  const minutes = String(Math.floor((total % 3600) / 60)).padStart(2, '0');
  const seconds = String(total % 60).padStart(2, '0');
  return `${hours}:${minutes}:${seconds}`;
}

function marketTimeAt(base, hour, minute, second = 0) {
  const d = new Date(base);
  d.setHours(hour, minute, second, 0);
  return d;
}

function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

function nextTradingMorning(date) {
  const d = marketTimeAt(date, 9, 30, 0);
  if (date >= d || isWeekend(date)) d.setDate(d.getDate() + 1);
  while (isWeekend(d)) d.setDate(d.getDate() + 1);
  return d;
}

function marketSessionInfo(now) {
  const preOpenAuction = marketTimeAt(now, 9, 15);
  const morningOpen = marketTimeAt(now, 9, 30);
  const lunchBreak = marketTimeAt(now, 11, 30);
  const afternoonOpen = marketTimeAt(now, 13, 0);
  const close = marketTimeAt(now, 15, 0);
  if (isWeekend(now)) {
    const target = nextTradingMorning(now);
    return { label: '休市中', targetLabel: '早盘', target, className: 'closed' };
  }
  if (now < preOpenAuction) return { label: '未开盘', targetLabel: '盘前竞价', target: preOpenAuction, className: 'closed' };
  if (now < morningOpen) return { label: '盘前竞价中', targetLabel: '连续竞价', target: morningOpen, className: 'paused' };
  if (now < lunchBreak) return { label: '交易中', targetLabel: '午盘', target: lunchBreak, className: 'trading' };
  if (now < afternoonOpen) return { label: '午间休市', targetLabel: '午后开盘', target: afternoonOpen, className: 'paused' };
  if (now < close) return { label: '交易中', targetLabel: '收盘', target: close, className: 'trading' };
  const target = nextTradingMorning(now);
  return { label: '已收盘', targetLabel: '早盘', target, className: 'closed' };
}

function updateMarketTimeCard() {
  const now = shanghaiNow();
  const weekday = ['星期日', '星期一', '星期二', '星期三', '星期四', '星期五', '星期六'][now.getDay()];
  const dateText = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
  const timeText = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
  const session = marketSessionInfo(now);
  setText('#home-market-clock', timeText);
  setText('#home-market-date', dateText);
  setText('#home-market-weekday', weekday);
  setText('#home-market-session', session.label);
  setText('#home-market-countdown', `距离${session.targetLabel} ${formatHms(session.target - now)}`);
  const sessionEl = qs('#home-market-session');
  if (sessionEl) sessionEl.className = `market-session ${session.className}`;
  const hourHand = qs('#home-clock-hour');
  const minuteHand = qs('#home-clock-minute');
  const hourDeg = ((now.getHours() % 12) + now.getMinutes() / 60) * 30;
  const minuteDeg = (now.getMinutes() + now.getSeconds() / 60) * 6;
  if (hourHand) hourHand.style.transform = `translateX(-50%) rotate(${hourDeg}deg)`;
  if (minuteHand) minuteHand.style.transform = `translateX(-50%) rotate(${minuteDeg}deg)`;
}

function setClass(selector, className) {
  const element = qs(selector);
  if (element) element.className = className;
}

function renderMarketOverview(overview) {
  if (!overview) return;
  const quote = splitDateTime(overview.latest_quote_time);
  const strength = Number(overview.market_strength || 0);
  const total = Number(overview.total || 0);
  const upPct = total ? (Number(overview.up_count || 0) / total) * 100 : 0;
  const downPct = total ? (Number(overview.down_count || 0) / total) * 100 : 0;
  const flatPct = total ? (Number(overview.flat_count || 0) / total) * 100 : 0;
  setText('#home-market-strength', overview.market_strength == null ? '-' : `${strength.toFixed(1)}`);
  setClass('.market-strength-thermo', `market-strength-thermo ${strength > 60 ? 'hot' : 'cool'}`);
  setLeftPercent('#home-strength-pointer', strength);
  const change = overview.market_strength_change;
  const changeText = change == null ? '较昨日 -' : `较昨日 ${Number(change) >= 0 ? '+' : ''}${Number(change).toFixed(1)}`;
  setText('#home-market-strength-change', changeText);
  setText('#home-market-state', `${overview.market_state_label || '-'} · 额加权 ${formatPercent(overview.amount_weighted_pct_chg ?? overview.avg_pct_chg)}`);
  setText('#home-market-breadth', `${overview.up_count ?? '-'} / ${overview.down_count ?? '-'} / ${overview.flat_count ?? '-'}`);
  setText('#home-market-up-ratio', overview.up_ratio == null ? '-' : `上涨占比 ${(Number(overview.up_ratio) * 100).toFixed(1)}%`);
  setText('#home-market-active', `涨 ${overview.up_count ?? '-'} · 平 ${overview.flat_count ?? '-'} · 跌 ${overview.down_count ?? '-'} · 样本 ${overview.total ?? '-'} 只`);
  setWidth('#home-breadth-up', upPct);
  setWidth('#home-breadth-flat', flatPct);
  setWidth('#home-breadth-down', downPct);
  setText('#home-market-limit', `${overview.limit_up_like ?? '-'} / ${overview.limit_down_like ?? '-'}`);
  const bjLimit = overview.limit_breakdown?.board30;
  const bjLimitText = bjLimit ? ` · 北证 ${bjLimit.up ?? 0}/${bjLimit.down ?? 0}` : '';
  setText('#home-market-big-move', `大涨 ${overview.strong_up_count ?? '-'} · 大跌 ${overview.strong_down_count ?? '-'}${bjLimitText}`);
  updateMarketTimeCard();
  renderSectorList('#home-strong-sectors', overview.strong_sectors, '暂无强势板块统计');
  renderSectorList('#home-weak-sectors', overview.weak_sectors, '暂无弱势板块统计');
}

function timingSignalClass(signal) {
  const n = Number(signal || 0);
  if (n > 0) return 'positive';
  if (n < 0) return 'negative';
  return 'neutral';
}

function formatTimingValue(signal) {
  if (!signal) return '-';
  if (signal.value_label != null) return signal.value_label;
  const value = signal.value;
  if (value == null) return '-';
  if (typeof value === 'object') {
    if (value.limit_up != null && value.limit_down != null) return `${value.limit_up}/${value.limit_down}`;
    return '-';
  }
  if (signal.dimension === 'capital') return `${(Number(value) * 100).toFixed(1)}%`;
  return Number(value).toFixed(signal.dimension === 'trend' ? 1 : 1);
}

function renderFactorCoverage(items = []) {
  if (!items.length) return '';
  const statusClass = (status) => {
    if (status === '已接入') return 'ready';
    if (status === '待接入' || status === '待数据' || status === '待权限') return 'pending';
    if (String(status || '').startsWith('V2')) return 'future';
    return 'missing';
  };
  return `
    <div class="home-market-timing-coverage">
      ${items.slice(0, 8).map((item) => `
        <span class="${statusClass(item.status)}"
          title="${escapeHtml(item.reason || '')}">
          ${escapeHtml(item.factor || '-')}：${escapeHtml(item.status || '-')}
        </span>
      `).join('')}
    </div>
  `;
}

function researchEvidenceLabel(status) {
  const labels = {
    baseline_pass: '已优于基线 · 仍属研究',
    provisional_baseline_pass: '初步优于基线',
    not_better_than_baseline: '未优于朴素基线',
    insufficient_evidence: '证据不足',
  };
  return labels[status] || status || '证据不足';
}

function renderTimingV20(shadow) {
  const status = qs('#home-timing-v20-status');
  const body = qs('#home-timing-v20-body');
  if (!status || !body) return;
  if (!shadow?.model_id) {
    status.textContent = '等待首个影子快照';
    status.className = 'research-status muted';
    body.innerHTML = '<div class="empty-state">V1.9 继续作为正式信号；V2.0 尚未形成影子日快照。</div>';
    return;
  }
  const range = shadow.position_range || {};
  const low = range.low_pct ?? (range.low == null ? null : Number(range.low) * 100);
  const high = range.high_pct ?? (range.high == null ? null : Number(range.high) * 100);
  status.textContent = `${shadow.state_label || '-'} · 研究影子`;
  status.className = `research-status ${shadow.emergency ? 'risk' : 'active'}`;
  const dimensions = shadow.dimensions || [];
  body.innerHTML = `
    <div class="home-timing-v20-summary">
      <div>
        <span>建议仓位区间</span>
        <strong>${low == null || high == null ? '-' : `${Number(low).toFixed(0)}–${Number(high).toFixed(0)}%`}</strong>
        <small>目标 ${shadow.position_target_pct == null ? '-' : `${Number(shadow.position_target_pct).toFixed(0)}%`} · 上限 ${shadow.position_upper_pct == null ? '-' : `${Number(shadow.position_upper_pct).toFixed(0)}%`}</small>
      </div>
      <div>
        <span>择时分 / 置信度</span>
        <strong>${formatNumber(shadow.timing_score, 1)}</strong>
        <small>${shadow.confidence == null ? '-' : `${(Number(shadow.confidence) * 100).toFixed(0)}%`} · ${escapeHtml(shadow.hysteresis_action || '-')}</small>
      </div>
    </div>
    <div class="home-timing-v20-dimensions">
      ${dimensions.map((item) => `
        <div class="${item.available === false ? 'missing' : ''}">
          <span>${escapeHtml(item.dimension_label || item.dimension || '-')}</span>
          <b>${formatNumber(item.score, 1)}</b>
          <i style="--dimension-score:${Math.max(0, Math.min(100, Number(item.score || 0)))}%"></i>
        </div>
      `).join('') || '<div class="empty-state">暂无维度数据</div>'}
    </div>
    <p>${escapeHtml(shadow.action_label || '-')}</p>
    ${(shadow.risk_notes || []).length ? `<small class="home-research-warning">${(shadow.risk_notes || []).map(escapeHtml).join('；')}</small>` : ''}
  `;
}

function renderScenarioForecast(scenario) {
  const status = qs('#home-scenario-status');
  const container = qs('#home-scenario-forecast');
  const leadershipStatus = qs('#home-leadership-status');
  const leadershipContainer = qs('#home-leadership-grid');
  if (!status || !container || !leadershipContainer) return;
  if (!scenario?.model_id) {
    status.textContent = '等待择时 V2.0 样本';
    status.className = 'research-status muted';
    container.innerHTML = '<div class="empty-state">尚无概率情景快照。模型不会使用大模型自由生成概率。</div>';
    if (leadershipStatus) leadershipStatus.textContent = '市场主线：暂无';
    leadershipContainer.innerHTML = '<div class="empty-state">尚无行业强度与价格周期快照。</div>';
    return;
  }
  const forecasts = scenario.forecasts || [];
  const allowedCount = forecasts.filter((item) => item.probability_display_allowed).length;
  status.textContent = allowedCount === forecasts.length && forecasts.length
    ? '通过朴素基线门槛'
    : '证据不足时隐藏精确概率';
  status.className = `research-status ${allowedCount === forecasts.length && forecasts.length ? 'active' : 'warn'}`;
  container.innerHTML = forecasts.map((item) => {
    const probabilities = item.probabilities || {};
    const quantiles = item.return_quantiles_pct || {};
    const displayAllowed = Boolean(item.probability_display_allowed);
    const probabilityMarkup = displayAllowed ? `
      <div class="scenario-probability down"><span>跌</span><b>${probabilities.down == null ? '-' : formatPercent(Number(probabilities.down) * 100, 0)}</b></div>
      <div class="scenario-probability range"><span>震荡</span><b>${probabilities.range == null ? '-' : formatPercent(Number(probabilities.range) * 100, 0)}</b></div>
      <div class="scenario-probability up"><span>涨</span><b>${probabilities.up == null ? '-' : formatPercent(Number(probabilities.up) * 100, 0)}</b></div>
    ` : `
      <div class="scenario-evidence-gate">
        <b>${escapeHtml(researchEvidenceLabel(item.validation_status || item.evidence_status))}</b>
        <span>已保存内部研究结果，但不展示虚假精度</span>
      </div>
    `;
    return `
      <article class="home-scenario-card ${displayAllowed ? 'validated' : 'gated'}">
        <header><strong>${item.horizon_days ?? '-'}日</strong><span>情景</span></header>
        <div class="scenario-probabilities">${probabilityMarkup}</div>
        <small>收益区间 P10/P50/P90：${quantiles.p10 == null ? '-' : formatNumber(quantiles.p10, 1)} / ${quantiles.p50 == null ? '-' : formatNumber(quantiles.p50, 1)} / ${quantiles.p90 == null ? '-' : formatNumber(quantiles.p90, 1)}%</small>
        <p>${escapeHtml(Object.values(item.action_plan || {})[0] || '等待更多前向证据')}</p>
      </article>
    `;
  }).join('') || '<div class="empty-state">暂无情景期限数据</div>';

  const leadership = scenario.leadership || [];
  const strengthClass = (state) => {
    const normalized = state === 'seed' ? 'watch' : state === 'decay' ? 'fading' : state;
    return ['watch', 'confirmed', 'core', 'crowded', 'fading'].includes(normalized)
      ? normalized
      : 'watch';
  };
  const cycleClass = (state) => [
    'base',
    'impulse_watch',
    'first_impulse',
    'main_up',
    'late_acceleration',
    'pullback',
    'rebound_candidate',
    'oversold_rebound',
    'secondary_decline_risk',
    'downtrend',
    'range',
    'stale_data',
    'insufficient_data',
  ].includes(state) ? state : 'insufficient_data';
  const cycleDisplayLabel = (item) => ({
    insufficient_data: '待补证',
    base: '筑底观察',
    impulse_watch: '短线转强·启动待确认',
    first_impulse: '多周期启动确认',
    main_up: '主升阶段',
    late_acceleration: '加速末段',
    pullback: '主升回踩',
    rebound_candidate: '持续修复·B浪候选',
    oversold_rebound: '超跌反弹·趋势未确认',
    secondary_decline_risk: '二次下探·C浪风险',
    downtrend: '下降趋势',
    range: '震荡整理',
    stale_data: '数据待对齐',
  }[item.cycle_state] || item.cycle_label || '待补证');
  const constructiveCycles = new Set(['first_impulse', 'main_up', 'late_acceleration', 'pullback']);
  const mainlineStates = new Set(['confirmed', 'core', 'crowded']);
  const fallbackMainline = leadership
    .filter((item) => (
      mainlineStates.has(item.leadership_state)
      && constructiveCycles.has(item.cycle_state)
      && item.price_evidence_status === 'ready'
      && item.breadth_metrics?.status === 'ready'
      && Number(item.confidence || 0) >= 0.8
    ))
    .sort((left, right) => Number(right.leadership_score || 0) - Number(left.leadership_score || 0))[0] || null;
  const mainlineSummary = scenario.market_mainline || {};
  const marketMainline = mainlineSummary.status === 'present'
    ? mainlineSummary.sector
    : fallbackMainline;
  const isMarketMainline = (item) => Boolean(
    marketMainline
    && item.sector_type === marketMainline.sector_type
    && item.sector_name === marketMainline.sector_name
  );
  const fallbackStrengtheningCount = leadership.filter((item) => (
    constructiveCycles.has(item.cycle_state)
    && !['fading', 'decay'].includes(item.leadership_state)
  )).length;
  const strengtheningCount = Number.isFinite(Number(mainlineSummary.price_strengthening_count))
    ? Number(mainlineSummary.price_strengthening_count)
    : fallbackStrengtheningCount;
  if (leadershipStatus) {
    leadershipStatus.textContent = marketMainline
      ? `市场主线：${marketMainline.sector_name} · 价格转强 ${strengtheningCount}`
      : `市场主线：暂无 · 价格转强 ${strengtheningCount}`;
    leadershipStatus.title = mainlineSummary.qualification_note || '市场主线最多一条，未达完整门槛时允许为空';
    leadershipStatus.className = `research-status ${marketMainline ? 'active' : 'muted'}`;
  }
  const orderedLeadership = [...leadership].sort((left, right) => (
    Number(isMarketMainline(right)) - Number(isMarketMainline(left))
    || Number(right.leadership_score || 0) - Number(left.leadership_score || 0)
  ));
  leadershipContainer.innerHTML = orderedLeadership.map((item) => {
    const primary = isMarketMainline(item);
    const evidence = (item.evidence || [])
      .slice(0, 4)
      .map((text) => String(text).replace(/^主线强度/, '行业综合强度'))
      .join(' · ');
    return `
      <article class="home-leadership-card ${strengthClass(item.leadership_state)} cycle-${cycleClass(item.cycle_state)} ${primary ? 'market-mainline' : ''}">
        <div><span>${primary ? '市场主线' : '行业强度'} · ${escapeHtml(item.state_label || '-')}</span><b>${formatNumber(item.leadership_score, 1)}</b></div>
        <strong>${escapeHtml(item.sector_name || '-')}</strong>
        <em class="home-leadership-cycle ${cycleClass(item.cycle_state)}">价格周期 · ${escapeHtml(cycleDisplayLabel(item))}</em>
        <small>${escapeHtml(evidence || '等待证据')}</small>
        ${(item.contradictions || []).length ? `<p>${escapeHtml((item.contradictions || []).join('；'))}</p>` : ''}
      </article>
    `;
  }).join('') || '<div class="empty-state">暂无行业强度与价格周期数据</div>';
}

function renderMarketTiming(timing) {
  const summary = qs('#home-market-timing-summary');
  const main = qs('#home-market-timing-main');
  const signals = qs('#home-market-timing-signals');
  const reasons = qs('#home-market-timing-reasons');
  if (!timing) {
    if (summary) summary.textContent = '暂无择时信号';
    if (signals) {
      signals.style.removeProperty('--timing-signal-columns');
      signals.removeAttribute('data-signal-count');
      signals.innerHTML = '<div class="empty-state">暂无择时信号</div>';
    }
    renderTimingV20(null);
    renderScenarioForecast(null);
    return;
  }

  const stateClass = timing.state || 'cautious';
  if (summary) summary.textContent = `${timing.model_name || '市场择时'} · ${timing.as_of || '-'}`;
  if (main) main.className = `home-market-timing-main ${stateClass}`;
  setText('#home-market-timing-position', timing.position_upper_pct == null ? '-' : `${Number(timing.position_upper_pct).toFixed(0)}%`);
  setText('#home-market-timing-state', `${timing.state_label || '-'} · ${formatNumber(timing.timing_score, 1)}分`);
  setText('#home-market-timing-action', timing.action_label || '-');

  if (signals) {
    const items = timing.signals || [];
    const columns = items.length > 1 ? Math.ceil(items.length / 2) : 1;
    signals.style.setProperty('--timing-signal-columns', String(columns));
    signals.dataset.signalCount = String(items.length);
    signals.innerHTML = items.length ? items.map((item) => `
      <article class="home-market-timing-signal ${timingSignalClass(item.signal)}">
        <span>${escapeHtml(item.label || item.dimension || '-')}</span>
        <strong>${formatNumber(item.score, 1)}</strong>
        <b>${escapeHtml(item.signal_label || '-')}</b>
        <small>${escapeHtml(item.article_dimension || '')} · 原始值 ${escapeHtml(formatTimingValue(item))} · ${escapeHtml(item.source_status || '已接入')}</small>
      </article>
    `).join('') : '<div class="empty-state">暂无维度信号</div>';
  }

  if (reasons) {
    const reasonList = (timing.reasons || []).slice(0, 4);
    const riskList = (timing.risk_notes || []).slice(0, 2);
    reasons.innerHTML = `
      <div class="home-market-timing-reason-list">
        ${reasonList.map((item) => `<span>${escapeHtml(item)}</span>`).join('') || '<span>暂无解释</span>'}
      </div>
      ${renderFactorCoverage(timing.article_factor_coverage || [])}
      ${riskList.length ? `<div class="home-market-timing-risk">${riskList.map(escapeHtml).join('；')}</div>` : ''}
    `;
  }
  renderTimingV20(timing.shadow_v20 || null);
  renderScenarioForecast(timing.scenario_forecast || null);
}

function renderTrackingCards(items = []) {
  if (!items.length) return '<div class="empty-state">暂无跟踪数据。可以先去选股中心运行一次策略。</div>';
  return items.map((item) => {
    const pct = item.price_change_pct;
    const pctClass = getPctClass(pct);
    const detailUrl = `/stocks/${encodeURIComponent(item.code || '')}`;
    return `
      <article class="home-tracking-card">
        <a href="${detailUrl}" class="home-tracking-card-main">
          <div class="home-tracking-card-head">
            <div>
              <strong>${escapeHtml(item.name || item.code || '-')}</strong>
              <span>${escapeHtml(item.code || '-')}</span>
            </div>
            ${item.rank_no != null ? `<em>#${escapeHtml(item.rank_no)}</em>` : ''}
          </div>
          <div class="home-tracking-return ${pctClass}">${formatPercent(pct)}</div>
          <div class="home-tracking-meta">
            <span>入选 ${escapeHtml(item.selection_datetime || item.selection_date || '-')}</span>
            <span>跟踪 ${escapeHtml(item.tracking_days ?? '-')} 日</span>
          </div>
          <div class="home-tracking-price-row">
            <span>入选价 <b>${formatPrice(item.selected_open_price ?? item.selected_close_price)}</b></span>
            <span>实时价 <b>${formatPrice(item.current_price)}</b></span>
            <span>分数 <b>${formatNumber(item.score, 2)}</b></span>
          </div>
          <small>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')} · ${escapeHtml(item.realtime_quote_time || '暂无实时快照')}</small>
        </a>
      </article>
    `;
  }).join('');
}

function renderRiskTags(tags = []) {
  if (!tags.length) return '<span class="emotion-risk-tag muted-tag">暂无风险标签</span>';
  return tags.slice(0, 3).map((tag) => `<span class="emotion-risk-tag">${escapeHtml(tag)}</span>`).join('');
}

function renderLimitUpPool(items = []) {
  if (!items.length) return '<div class="empty-state">暂无涨停/连板观察数据</div>';
  const rows = items.slice(0, 10).map((item, index) => {
    const detailUrl = `/stocks/${encodeURIComponent(item.code || '')}`;
    const openBoardLine = item.open_board_count
      ? `${item.open_board_label || `开板${item.open_board_count}次`}${item.last_open_time ? ` · 末次 ${splitDateTime(item.last_open_time).time || item.last_open_time}` : ''}`
      : '';
    return `
      <tr>
        <td><span class="emotion-rank">${index + 1}</span></td>
        <td>
          <a class="emotion-stock-link" href="${detailUrl}">${escapeHtml(item.name || item.code || '-')}</a>
          <div class="muted">${escapeHtml(item.code || '-')} · ${escapeHtml(item.industry || '-')}</div>
        </td>
        <td>
          <span class="emotion-height-badge">${escapeHtml(item.board_height_label || '-')}</span>
          ${item.recent_pattern_label ? `<div class="muted">${escapeHtml(item.recent_pattern_label)}</div>` : ''}
        </td>
        <td>
          <div>${escapeHtml(item.status_label || '-')}</div>
          ${openBoardLine ? `<div class="muted">${escapeHtml(openBoardLine)}</div>` : ''}
          <div class="muted">${formatCompactAmount(item.amount)}</div>
        </td>
        <td class="up">${formatPercent(item.pct_chg)}</td>
        <td>${formatPercent(item.turnover_rate)}</td>
        <td><div class="emotion-tags">${renderRiskTags(item.risk_tags || [])}</div></td>
      </tr>
    `;
  }).join('');
  return `
    <div class="home-emotion-table-wrap">
      <table class="home-emotion-table">
        <thead>
          <tr>
            <th>#</th>
            <th>股票</th>
            <th>高度</th>
            <th>状态/成交额</th>
            <th>涨幅</th>
            <th>换手</th>
            <th>风险</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderHotLimitWatchPool(items = []) {
  if (!items.length) return '<div class="empty-state">暂无强势冲板观察数据</div>';
  const rows = items.slice(0, 6).map((item, index) => {
    const detailUrl = `/stocks/${encodeURIComponent(item.code || '')}`;
    const limitText = item.limit_gap_pct == null ? '水上' : `距板 ${formatPercent(item.limit_gap_pct)}`;
    const themeLine = item.theme_name
      ? `${item.theme_name} · 热度 ${formatNumber(item.theme_score, 1)}`
      : '热点映射待确认';
    const watchLine = (item.watch_points || []).join(' · ') || themeLine;
    return `
      <tr>
        <td><span class="emotion-rank">${index + 1}</span></td>
        <td>
          <a class="emotion-stock-link" href="${detailUrl}">${escapeHtml(item.name || item.code || '-')}</a>
          <div class="muted">${escapeHtml(item.code || '-')} · ${escapeHtml(item.industry || '-')}</div>
        </td>
        <td>
          <span class="emotion-status-badge">${escapeHtml(item.status_label || '-')}</span>
          <div class="muted">${escapeHtml(limitText)}</div>
        </td>
        <td>
          <div>${escapeHtml(themeLine)}</div>
          <div class="muted">${escapeHtml(item.theme_match_reason || watchLine)}</div>
        </td>
        <td class="up">${formatPercent(item.pct_chg)}</td>
        <td>${formatNumber(item.hot_score, 1)}</td>
        <td>
          <div>${item.net_amount_yi == null ? '-' : `${formatNumber(item.net_amount_yi, 2)}亿`}</div>
          <div class="muted">${item.popularity_rank ? `人气 #${escapeHtml(item.popularity_rank)}` : '人气 -'}</div>
        </td>
      </tr>
    `;
  }).join('');
  return `
    <div class="home-emotion-table-wrap">
      <table class="home-emotion-table">
        <thead>
          <tr>
            <th>#</th>
            <th>股票</th>
            <th>状态</th>
            <th>热点依据</th>
            <th>涨幅</th>
            <th>冲板分</th>
            <th>资金/人气</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderReversalPool(items = []) {
  if (!items.length) return '<div class="empty-state">暂无分歧反包观察数据</div>';
  const rows = items.slice(0, 6).map((item, index) => {
    const detailUrl = `/stocks/${encodeURIComponent(item.code || '')}`;
    const limitGap = item.is_limit_up ? '已封板' : (item.limit_gap_pct == null ? '' : `距板 ${formatPercent(item.limit_gap_pct)}`);
    return `
      <tr>
        <td><span class="emotion-rank">${index + 1}</span></td>
        <td>
          <a class="emotion-stock-link" href="${detailUrl}">${escapeHtml(item.name || item.code || '-')}</a>
          <div class="muted">${escapeHtml(item.code || '-')} · ${escapeHtml(item.industry || '-')}</div>
        </td>
        <td>
          <span class="emotion-status-badge">${escapeHtml(item.status_label || '-')}</span>
          ${item.previous_board_label ? `<div class="muted">${escapeHtml(item.previous_board_label)}</div>` : ''}
          ${limitGap ? `<div class="muted">${escapeHtml(limitGap)}</div>` : ''}
        </td>
        <td>${formatPercent(item.prev_divergence_pct)}</td>
        <td class="up">${formatPercent(item.reversal_pct)}</td>
        <td>${formatNumber(item.support_score, 1)}</td>
        <td><div class="emotion-tags">${renderRiskTags(item.risk_tags || [])}</div></td>
      </tr>
    `;
  }).join('');
  return `
    <div class="home-emotion-table-wrap">
      <table class="home-emotion-table">
        <thead>
          <tr>
            <th>#</th>
            <th>股票</th>
            <th>前连板/状态</th>
            <th>昨日分歧</th>
            <th>今日修复</th>
            <th>承接</th>
            <th>风险</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderStrongWatchPool(hotLimitItems = [], reversalItems = []) {
  return `
    <div class="home-emotion-subsection">
      <div class="home-emotion-subhead">
        <strong>强势冲板观察</strong>
        <span>水上强势 / 冲刺涨停 / 热度与资金确认</span>
      </div>
      ${renderHotLimitWatchPool(hotLimitItems)}
    </div>
    <div class="home-emotion-subsection">
      <div class="home-emotion-subhead">
        <strong>分歧反包观察</strong>
        <span>昨日分歧 / 今日修复 / 承接</span>
      </div>
      ${renderReversalPool(reversalItems)}
    </div>
  `;
}

function renderEmotionBoard(board = {}) {
  const summary = qs('#home-emotion-summary');
  const limitPool = board.limit_up_pool || [];
  const hotLimitPool = board.hot_limit_watch_pool || [];
  const reversalPool = board.reversal_watch_pool || [];
  if (summary) {
    summary.textContent = `观察池：连板 ${limitPool.length} · 冲板潜力 ${hotLimitPool.length} · 反包 ${reversalPool.length} · 非正式策略`;
  }
  const limitContainer = qs('#home-limit-up-pool');
  if (limitContainer) limitContainer.innerHTML = renderLimitUpPool(limitPool);
  const reversalContainer = qs('#home-reversal-watch-pool');
  if (reversalContainer) reversalContainer.innerHTML = renderStrongWatchPool(hotLimitPool, reversalPool);
}

async function loadHomePage() {
  const trackingSummary = qs('#home-tracking-summary');
  const trackingPreview = qs('#home-tracking-preview');

  try {
    const data = await fetchJson('/api/dashboard/summary?limit=8&compact=true');
    const items = data.latest_tracking_preview || [];
    renderMarketOverview(data.market_overview);
    renderMarketTiming(data.market_timing);
    renderHotThemes(data.hot_themes || {});
    renderEmotionBoard(data.emotion_board || {});

    const avgText = data.latest_tracking_avg_price_change_pct == null
      ? '平均涨跌幅 -'
      : `平均涨跌幅 ${formatPercent(data.latest_tracking_avg_price_change_pct)}`;
    trackingSummary.textContent = `共 ${data.latest_tracking_count ?? items.length ?? 0} 条 · ${avgText}`;
    trackingPreview.innerHTML = renderTrackingCards(items);
  } catch (error) {
    trackingSummary.textContent = '加载失败';
    trackingPreview.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    setText('#home-emotion-summary', '加载失败');
    setText('#home-hot-theme-summary', '加载失败');
    setText('#home-market-timing-summary', '加载失败');
    const timingSignals = qs('#home-market-timing-signals');
    if (timingSignals) timingSignals.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    const hotThemeContainer = qs('#home-hot-themes');
    if (hotThemeContainer) hotThemeContainer.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    const limitContainer = qs('#home-limit-up-pool');
    const reversalContainer = qs('#home-reversal-watch-pool');
    if (limitContainer) limitContainer.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    if (reversalContainer) reversalContainer.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  updateMarketTimeCard();
  setInterval(updateMarketTimeCard, 1000);
  loadHomePage();
});
