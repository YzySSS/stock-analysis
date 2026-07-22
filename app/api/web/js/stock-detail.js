function formatChartTime(value, fallback = '-') {
  if (!value) return fallback;
  const text = String(value);
  if (text.includes('T')) return text.replace('T', ' ').slice(0, 16);
  return text.length > 16 ? text.slice(0, 16) : text;
}

function formatMoneyCN(value) {
  const num = Number(value);
  if (value == null || Number.isNaN(num)) return '-';
  if (Math.abs(num) >= 100000000) return `${(num / 100000000).toFixed(2)}亿`;
  if (Math.abs(num) >= 10000) return `${(num / 10000).toFixed(2)}万`;
  return num.toFixed(2);
}

function formatMoneyWan(value) {
  const num = Number(value);
  if (value == null || Number.isNaN(num)) return '-';
  if (Math.abs(num) >= 10000) return `${(num / 10000).toFixed(2)}亿`;
  return `${num.toFixed(2)}万`;
}

const intradayRefreshRequestedAt = new Map();
const INTRADAY_REFRESH_COOLDOWN_MS = 5 * 60 * 1000;

function intradayMinuteKey(value) {
  const text = String(value || '').trim().replace('T', ' ');
  const fullMatch = text.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2})/);
  if (fullMatch) return `${fullMatch[1]} ${fullMatch[2]}:${fullMatch[3]}`;
  const timeMatch = text.match(/(\d{2}):(\d{2})/);
  return timeMatch ? `${timeMatch[1]}:${timeMatch[2]}` : null;
}

function intradayPointMinuteKey(item) {
  return intradayMinuteKey(item?.minute_time || item?.quote_minute);
}

function latestIntradayMinuteKey(items = []) {
  return (items || []).reduce((latest, item) => {
    const key = intradayPointMinuteKey(item);
    return key && (!latest || key > latest) ? key : latest;
  }, null);
}

function isIntradayCacheStale(cachedBars = [], realtimePoints = []) {
  if ((cachedBars || []).length < 2) return true;
  const latestRealtime = latestIntradayMinuteKey(realtimePoints);
  if (!latestRealtime) return false;
  const latestCached = latestIntradayMinuteKey(cachedBars);
  return !latestCached || latestCached < latestRealtime;
}

function mergeIntradayChartPoints(cachedBars = [], realtimePoints = [], meta = {}) {
  const merged = new Map();
  (realtimePoints || []).forEach((item) => {
    const key = intradayPointMinuteKey(item);
    if (key) merged.set(key, { ...item, quote_minute: item.quote_minute || item.minute_time });
  });
  normalizeIntradayBars(cachedBars || [], meta).forEach((item) => {
    const key = intradayPointMinuteKey(item);
    if (key) merged.set(key, item);
  });
  return [...merged.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, item]) => item);
}

function waitFor(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

async function refreshAndLoadIntradayBars(code, tradeDate, expectedLatestMinute = null) {
  const encodedCode = encodeURIComponent(code);
  const encodedDate = encodeURIComponent(tradeDate || '');
  const refreshKey = `${code}:${tradeDate || ''}`;
  const now = Date.now();
  const lastRequestedAt = intradayRefreshRequestedAt.get(refreshKey) || 0;
  const shouldQueueRefresh = now - lastRequestedAt >= INTRADAY_REFRESH_COOLDOWN_MS;
  if (shouldQueueRefresh) {
    intradayRefreshRequestedAt.set(refreshKey, now);
    try {
      await fetchJson(
        `/api/stocks/${encodedCode}/intraday-bars/refresh?trade_date=${encodedDate}`,
        { method: 'POST' },
      );
    } catch (error) {
      intradayRefreshRequestedAt.delete(refreshKey);
      throw error;
    }
  }
  const expectedPoints = expectedLatestMinute ? [{ quote_minute: expectedLatestMinute }] : [];
  const maxAttempts = shouldQueueRefresh ? 8 : 1;
  let latest = { items: [], count: 0, source_status: 'empty' };
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    latest = await fetchJson(`/api/stocks/${encodedCode}/intraday-bars?trade_date=${encodedDate}`);
    if (!isIntradayCacheStale(latest.items || [], expectedPoints)) return latest;
    if (attempt < maxAttempts - 1) await waitFor(1500);
  }
  return latest;
}

function formatRank(rank, percentile) {
  if (rank == null) return '-';
  const pct = percentile == null ? '' : ` · 前${Math.max(0, Math.min(100, 100 - Number(percentile))).toFixed(1)}%`;
  return `#${rank}${pct}`;
}

function renderOverviewConsole(overview) {
  if (!overview) return;
  const quote = overview.quote || {};
  const status = overview.market_status || {};
  const rankings = overview.rankings || {};
  const capital = overview.capital_flow || {};
  const technical = overview.technical_summary || {};
  const fundamental = overview.fundamental_summary || {};
  const peVs = fundamental.valuation_position?.pe_vs_industry_pct;

  qs('#stock-overview-status').textContent = status.label || '-';
  qs('#stock-overview-open').textContent = formatPrice(quote.open_price);
  qs('#stock-overview-high').textContent = formatPrice(quote.high_price);
  qs('#stock-overview-low').textContent = formatPrice(quote.low_price);
  qs('#stock-overview-amount').textContent = formatMoneyCN(quote.amount);
  qs('#stock-overview-amount-rank').textContent = formatRank(rankings.amount_rank, rankings.amount_percentile);
  qs('#stock-overview-pct-rank').textContent = formatRank(rankings.pct_chg_rank, rankings.pct_chg_percentile);
  qs('#stock-overview-mv-rank').textContent = formatRank(rankings.total_mv_rank, rankings.total_mv_percentile);
  qs('#stock-overview-capital-score').textContent = formatNumber(capital.score, 1);
  qs('#stock-overview-tech-score').textContent = formatNumber(technical.trend_score, 1);
  qs('#stock-overview-pe-vs').textContent = peVs == null ? '-' : `${peVs > 0 ? '+' : ''}${Number(peVs).toFixed(1)}%`;

  ['#stock-overview-capital-score', '#stock-overview-tech-score'].forEach((selector) => {
    const el = qs(selector);
    const value = Number(el.textContent);
    el.classList.remove('up', 'down');
    if (!Number.isNaN(value) && value >= 60) el.classList.add('up');
    if (!Number.isNaN(value) && value < 45) el.classList.add('down');
  });
}

function renderChipPanel(chip) {
  const container = qs('#stock-detail-chip');
  if (!container) return;
  if (!chip) {
    container.innerHTML = '<div class="muted">暂无筹码数据</div>';
    return;
  }
  const labelClass = chip.winner_rate >= 70 ? 'up' : chip.winner_rate <= 35 ? 'down' : '';
  container.innerHTML = `
    <div><strong>筹码状态</strong></div>
    <div class="${labelClass}">${escapeHtml(chip.label || '-')}</div>
    <div><strong>获利比例</strong></div>
    <div>${formatPercent(chip.winner_rate)}</div>
    <div><strong>加权平均成本</strong></div>
    <div>${formatPrice(chip.weight_avg)}</div>
    <div><strong>价格偏离成本</strong></div>
    <div>${formatPercent(chip.price_vs_weight_avg_pct)}</div>
    <div><strong>中位成本</strong></div>
    <div>${formatPrice(chip.cost_50pct)}</div>
    <div><strong>成本集中带</strong></div>
    <div>${formatPrice(chip.cost_15pct)} ~ ${formatPrice(chip.cost_85pct)}</div>
    <div><strong>集中带宽度</strong></div>
    <div>${formatPercent(chip.cost_band_width_pct)}</div>
    <div><strong>数据日期</strong></div>
    <div>${escapeHtml(chip.trade_date || '-')}</div>
  `;
}

function renderMoneyflowPanel(moneyflow, realtimeMoneyflow) {
  const container = qs('#stock-detail-moneyflow');
  if (!container) return;
  if (!moneyflow && !realtimeMoneyflow) {
    container.innerHTML = '<div class="muted">暂无资金流数据</div>';
    return;
  }
  const realtimeNet = Number(realtimeMoneyflow?.net_amount);
  const realtimeClass = !Number.isNaN(realtimeNet) && realtimeNet > 0 ? 'up' : !Number.isNaN(realtimeNet) && realtimeNet < 0 ? 'down' : '';
  const realtimeHtml = realtimeMoneyflow ? `
    <div><strong>今日实时状态</strong></div>
    <div class="${realtimeClass}">${escapeHtml(realtimeMoneyflow.label || '-')}</div>
    <div><strong>实时净额</strong></div>
    <div class="${realtimeClass}">${formatMoneyCN(realtimeMoneyflow.net_amount)}</div>
    <div><strong>实时流入 / 流出</strong></div>
    <div>${formatMoneyCN(realtimeMoneyflow.inflow_amount)} / ${formatMoneyCN(realtimeMoneyflow.outflow_amount)}</div>
    <div><strong>净额占成交额</strong></div>
    <div>${formatPercent(realtimeMoneyflow.net_flow_intensity_pct)}</div>
    <div><strong>实时换手率</strong></div>
    <div>${formatPercent(realtimeMoneyflow.turnover_rate)}</div>
    <div><strong>实时报价时间</strong></div>
    <div>${escapeHtml(formatChartTime(realtimeMoneyflow.quote_time))}</div>
  ` : `
    <div><strong>今日实时资金流</strong></div>
    <div class="muted">当前股票暂无新鲜实时记录</div>
  `;

  const dailyNet = Number(moneyflow?.net_mf_amount);
  const dailyClass = !Number.isNaN(dailyNet) && dailyNet > 0 ? 'up' : !Number.isNaN(dailyNet) && dailyNet < 0 ? 'down' : '';
  const dailyHtml = moneyflow ? `
    <div><strong>完整日资金状态</strong></div>
    <div class="${dailyClass}">${escapeHtml(moneyflow.label || '-')}</div>
    <div><strong>完整日净流入</strong></div>
    <div class="${dailyClass}">${formatMoneyWan(moneyflow.net_mf_amount)}</div>
    <div><strong>完整日净流入强度</strong></div>
    <div>${formatPercent(moneyflow.net_flow_intensity_pct)}</div>
    <div><strong>大/特大单净额</strong></div>
    <div>${formatMoneyWan(moneyflow.large_net_amount)}</div>
    <div><strong>大单占成交额</strong></div>
    <div>${formatPercent(moneyflow.large_flow_ratio_pct)}</div>
    <div><strong>特大单买 / 卖</strong></div>
    <div>${formatMoneyWan(moneyflow.buy_elg_amount)} / ${formatMoneyWan(moneyflow.sell_elg_amount)}</div>
    <div><strong>大单买 / 卖</strong></div>
    <div>${formatMoneyWan(moneyflow.buy_lg_amount)} / ${formatMoneyWan(moneyflow.sell_lg_amount)}</div>
    <div><strong>最近完整交易日</strong></div>
    <div>${escapeHtml(moneyflow.trade_date || '-')}</div>
  ` : `
    <div><strong>完整日资金拆单</strong></div>
    <div class="muted">暂无日频资金流数据</div>
  `;

  container.innerHTML = `${realtimeHtml}${dailyHtml}`;
}

function factorLabel(key) {
  const labelMap = {
    trend: '趋势',
    momentum: '动量',
    quality: '质量',
    sentiment: '舆情',
    value: '估值',
    liquidity: '流动性',
    turnover_score: '换手',
    turnover: '换手',
    lowvol_score: '低波',
    lowvol: '低波',
    reversal_score: '反转',
    reversal: '反转',
    sector_heat: '主题热度',
    source_credibility: '信源可信度',
    info_importance: '信息重要度',
    amplification: '传播热度',
    stock_match: '个股匹配',
    fund_flow: '资金确认',
    daily_trend: '日线趋势',
    chip_structure: '筹码结构',
    price_confirm: '价格确认',
    volume_confirm: '成交确认',
    intraday_confirm: '分时确认',
    market_context: '市场环境',
    deepseek_sentiment: 'AI舆情精排',
    divergence: '分歧强度',
    reversal_strength: '反包强度',
    recognition: '辨识度',
    turnover_heat: '换手热度',
    sentiment_heat: '情绪热度',
    risk_control: '风险控制',
  };
  return labelMap[key] || String(key || '')
    .replace(/_score$/, '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function getDisplayFactorEntries(factorScores) {
  const preferredOrder = [
    'trend', 'momentum', 'quality', 'sentiment', 'value', 'liquidity',
    'turnover_score', 'turnover', 'lowvol_score', 'lowvol', 'reversal_score', 'reversal',
    'sector_heat', 'source_credibility', 'info_importance', 'amplification', 'stock_match',
    'fund_flow', 'daily_trend', 'chip_structure', 'price_confirm', 'volume_confirm', 'intraday_confirm', 'market_context', 'deepseek_sentiment',
  ];
  const rawMetricKeys = new Set([
    'open', 'high', 'low', 'close', 'volume', 'amount', 'trade_date',
    'pe_tushare', 'pb_tushare', 'roe', 'roa', 'eps',
    'grossprofit_margin', 'netprofit_margin', 'revenue_yoy', 'profit_yoy',
    'completeness_score', 'data_quality_score', 'value_score', 'quality_score', 'stability_score',
    'market_strength', 'market_state', 'news_count', 'sentiment_score', 'sentiment_source',
    'chip_his_low', 'chip_his_high', 'chip_cost_5pct', 'chip_cost_15pct', 'chip_cost_50pct',
    'chip_cost_85pct', 'chip_cost_95pct', 'chip_weight_avg', 'chip_winner_rate',
    'raw_news_count', 'filtered_news_count', 'credibility_avg', 'quality_avg',
    'fundamental_missing_fields', 'source_credibility_level', 'source_credibility_reason',
    'trade_signal_state', 'trade_signal_label', 'trade_signal_reason',
  ]);
  const scores = factorScores || {};
  const orderedKeys = preferredOrder.filter((key) => Object.prototype.hasOwnProperty.call(scores, key));
  const extraKeys = Object.keys(scores)
    .filter((key) => !preferredOrder.includes(key) && !rawMetricKeys.has(key))
    .sort();
  return [...orderedKeys, ...extraKeys]
    .map((key) => ({ key, label: factorLabel(key), value: Number(scores[key]) }))
    .filter((item) => !Number.isNaN(item.value) && item.value >= 0 && item.value <= 100);
}

function renderFactorScorePills(factorScores, latestSelection = {}) {
  const container = qs('#stock-factor-score-pills');
  if (!container) return;
  const strategyName = latestSelection.strategy_display_name || latestSelection.strategy_id || '未关联策略';
  const scoreLabel = qs('#stock-strategy-score-label');
  const caption = qs('#stock-strategy-factor-caption');
  if (scoreLabel) scoreLabel.textContent = strategyName;
  if (caption) caption.textContent = `因子得分 · ${strategyName}`;

  const entries = getDisplayFactorEntries(factorScores);

  if (!entries.length) {
    container.innerHTML = '<span class="muted">暂无因子得分</span>';
    return;
  }

  container.innerHTML = entries.map((item) => {
    const level = item.value >= 70 ? 'strong' : item.value < 45 ? 'weak' : 'neutral';
    const width = Math.max(4, Math.min(100, item.value));
    return `
      <div class="factor-score-pill ${level}">
        <div class="factor-score-row">
          <span>${escapeHtml(item.label)}</span>
          <strong>${formatNumber(item.value, item.value % 1 === 0 ? 0 : 1)}</strong>
        </div>
        <i style="width:${width}%"></i>
      </div>
    `;
  }).join('');
}

function renderInteractiveLineChart(svgSelector, rawPoints, options) {
  const svg = qs(svgSelector);
  if (!svg) return;
  const points = (rawPoints || [])
    .map((item) => ({
      label: options.label(item),
      value: Number(options.value(item)),
      pct: options.pct ? options.pct(item) : null,
    }))
    .filter((item) => item.value != null && !Number.isNaN(item.value));

  if (points.length < 2) {
    svg.innerHTML = `<text x="18" y="28" fill="#94a3b8" font-size="13">${escapeHtml(options.emptyText || '暂无图表数据')}</text>`;
    return;
  }

  const width = 640;
  const height = 220;
  const padding = { left: 44, right: 24, top: 28, bottom: 34 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const values = points.map((item) => item.value);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const extra = Math.max((rawMax - rawMin) * 0.12, rawMax * 0.002, 0.01);
  const min = rawMin - extra;
  const max = rawMax + extra;
  const range = max - min || 1;
  const chartPoints = points.map((item, index) => {
    const x = padding.left + (index * plotWidth) / (points.length - 1);
    const y = padding.top + ((max - item.value) / range) * plotHeight;
    return { ...item, x, y };
  });
  const polyline = chartPoints.map((item) => `${item.x},${item.y}`).join(' ');
  const stroke = points[points.length - 1].value >= points[0].value ? '#ef4444' : '#22c55e';
  const gridYs = [0, 0.25, 0.5, 0.75, 1].map((ratio) => padding.top + ratio * plotHeight);
  const lastIndex = chartPoints.length - 1;

  svg.innerHTML = `
    <g class="chart-grid">
      ${gridYs.map((y) => `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" />`).join('')}
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" />
    </g>
    <polyline class="chart-line" fill="none" stroke="${stroke}" stroke-width="2.4" points="${polyline}" />
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${formatPrice(rawMax)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${formatPrice(rawMin)}</text>
    <text x="${padding.left}" y="${height - 6}" fill="#94a3b8" font-size="11">${escapeHtml(formatChartTime(points[0].label))}</text>
    <text x="${width - padding.right - 86}" y="${height - 6}" fill="#94a3b8" font-size="11">${escapeHtml(formatChartTime(points[lastIndex].label))}</text>
    <g class="chart-focus" style="display:none">
      <line data-focus-x x1="0" y1="${padding.top}" x2="0" y2="${height - padding.bottom}" />
      <line data-focus-y x1="${padding.left}" y1="0" x2="${width - padding.right}" y2="0" />
      <circle data-focus-dot r="4.5" cx="0" cy="0" />
      <text data-focus-label class="chart-focus-info" x="${width - padding.right}" y="20" text-anchor="end"></text>
      <text data-focus-price class="chart-focus-info" x="${width - padding.right}" y="38" text-anchor="end"></text>
    </g>
    <rect data-chart-hit-area x="${padding.left}" y="${padding.top}" width="${plotWidth}" height="${plotHeight}" fill="transparent" />
  `;

  const focus = svg.querySelector('.chart-focus');
  const focusX = svg.querySelector('[data-focus-x]');
  const focusY = svg.querySelector('[data-focus-y]');
  const focusDot = svg.querySelector('[data-focus-dot]');
  const focusLabel = svg.querySelector('[data-focus-label]');
  const focusPrice = svg.querySelector('[data-focus-price]');
  const hitArea = svg.querySelector('[data-chart-hit-area]');

  const showPoint = (index) => {
    const point = chartPoints[Math.max(0, Math.min(index, chartPoints.length - 1))];
    focus.style.display = 'block';
    focusX.setAttribute('x1', point.x);
    focusX.setAttribute('x2', point.x);
    focusY.setAttribute('y1', point.y);
    focusY.setAttribute('y2', point.y);
    focusDot.setAttribute('cx', point.x);
    focusDot.setAttribute('cy', point.y);
    focusLabel.textContent = formatChartTime(point.label);
    const pctText = point.pct == null || Number.isNaN(Number(point.pct)) ? '' : ` · ${Number(point.pct).toFixed(2)}%`;
    const carryText = point.carriedFrom ? ` · 延续${point.carriedFrom}` : '';
    focusPrice.textContent = `价格 ${formatPrice(point.value)}${pctText}${carryText}`;
  };

  const handleMove = (event) => {
    const rect = svg.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * width;
    const ratio = (x - padding.left) / plotWidth;
    const index = Math.round(ratio * (chartPoints.length - 1));
    showPoint(index);
  };

  hitArea.addEventListener('pointermove', handleMove);
  hitArea.addEventListener('pointerdown', handleMove);
  hitArea.addEventListener('pointerleave', () => showPoint(lastIndex));
  showPoint(lastIndex);
}

function getDateParts(dateText) {
  const [year, month, day] = String(dateText || '').slice(0, 10).split('-').map(Number);
  if (!year || !month || !day) return null;
  return { year, month, day, date: new Date(Date.UTC(year, month - 1, day)) };
}

function getWeekKey(dateText) {
  const parts = getDateParts(dateText);
  if (!parts) return String(dateText || '-');
  const date = new Date(parts.date);
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

function aggregateKlineHistory(history, period = 'day') {
  const points = (history || [])
    .map((item) => ({
      label: item.trade_date || '-',
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
    }))
    .filter((item) => [item.open, item.high, item.low, item.close].every((value) => value != null && !Number.isNaN(value)))
    .sort((a, b) => String(a.label).localeCompare(String(b.label)));

  if (period === 'day') return points;

  const groups = new Map();
  points.forEach((item) => {
    const key = period === 'week' ? getWeekKey(item.label) : String(item.label).slice(0, 7);
    if (!groups.has(key)) {
      groups.set(key, { ...item, key, start: item.label, end: item.label });
      return;
    }
    const group = groups.get(key);
    group.high = Math.max(group.high, item.high);
    group.low = Math.min(group.low, item.low);
    group.close = item.close;
    group.end = item.label;
    group.label = period === 'week' ? item.label : key;
  });

  return [...groups.values()].map((item) => ({
    label: period === 'week' ? item.end : item.key,
    periodLabel: period === 'week' ? `${item.start}~${item.end}` : item.key,
    open: item.open,
    high: item.high,
    low: item.low,
    close: item.close,
  }));
}

function setupKlineTabs(history) {
  const periodButtons = [...document.querySelectorAll('[data-kline-period]')];
  const startInput = qs('#kline-start-date');
  const endInput = qs('#kline-end-date');
  const title = qs('#stock-kline-title');
  if (!periodButtons.length) return;

  const orderedHistory = [...(history || [])].sort((a, b) => String(a.trade_date || '').localeCompare(String(b.trade_date || '')));
  const availableDates = orderedHistory.map((item) => String(item.trade_date || '').slice(0, 10)).filter(Boolean);
  const minDate = availableDates[0] || '';
  const maxDate = availableDates[availableDates.length - 1] || '';
  const oneMonthAgo = maxDate ? (() => {
    const date = new Date(`${maxDate}T00:00:00`);
    date.setMonth(date.getMonth() - 1);
    return date.toISOString().slice(0, 10);
  })() : '';
  const defaultStart = oneMonthAgo && oneMonthAgo > minDate ? oneMonthAgo : minDate;
  const state = { period: 'day', startDate: defaultStart, endDate: maxDate };

  if (startInput) {
    startInput.min = minDate;
    startInput.max = maxDate;
    startInput.value = state.startDate;
  }
  if (endInput) {
    endInput.min = minDate;
    endInput.max = maxDate;
    endInput.value = state.endDate;
  }

  const render = () => {
    if (state.startDate && state.endDate && state.startDate > state.endDate) {
      [state.startDate, state.endDate] = [state.endDate, state.startDate];
      if (startInput) startInput.value = state.startDate;
      if (endInput) endInput.value = state.endDate;
    }
    periodButtons.forEach((button) => button.classList.toggle('active', button.dataset.klinePeriod === state.period));
    const titleMap = { day: '日K走势', week: '周K走势', month: '月K走势' };
    if (title) title.textContent = titleMap[state.period] || 'K线走势';
    const filtered = orderedHistory.filter((item) => {
      const date = String(item.trade_date || '').slice(0, 10);
      return (!state.startDate || date >= state.startDate) && (!state.endDate || date <= state.endDate);
    });
    renderPriceChart(filtered, state.period);
  };

  periodButtons.forEach((button) => {
    button.addEventListener('click', () => {
      state.period = button.dataset.klinePeriod || 'day';
      render();
    });
  });
  if (startInput) {
    startInput.addEventListener('change', () => {
      state.startDate = startInput.value || minDate;
      render();
    });
  }
  if (endInput) {
    endInput.addEventListener('change', () => {
      state.endDate = endInput.value || maxDate;
      render();
    });
  }
  render();
}

function renderPriceChart(history, period = 'day') {
  const svg = qs('#stock-price-chart');
  if (!svg) return;
  const points = aggregateKlineHistory(history, period);
  const periodLabel = period === 'week' ? '周K' : period === 'month' ? '月K' : '日线';

  if (points.length < 2) {
    svg.innerHTML = `<text x="18" y="28" fill="#94a3b8" font-size="13">暂无${periodLabel}数据</text>`;
    return;
  }

  const width = 640;
  const height = 220;
  const padding = { left: 42, right: 18, top: 22, bottom: 24 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const highs = points.map((item) => item.high);
  const lows = points.map((item) => item.low);
  const rawMax = Math.max(...highs);
  const rawMin = Math.min(...lows);
  const extra = Math.max((rawMax - rawMin) * 0.12, rawMax * 0.002, 0.01);
  const max = rawMax + extra;
  const min = rawMin - extra;
  const range = max - min || 1;
  const yOf = (value) => padding.top + ((max - value) / range) * plotHeight;
  const step = plotWidth / points.length;
  const candleWidth = Math.max(4, Math.min(12, step * 0.56));
  const chartPoints = points.map((item, index) => {
    const x = padding.left + step * index + step / 2;
    return {
      ...item,
      x,
      highY: yOf(item.high),
      lowY: yOf(item.low),
      openY: yOf(item.open),
      closeY: yOf(item.close),
    };
  });
  const gridYs = [0, 0.25, 0.5, 0.75, 1].map((ratio) => padding.top + ratio * plotHeight);
  const lastIndex = chartPoints.length - 1;
  const candles = chartPoints.map((item) => {
    const isUp = item.close >= item.open;
    const color = isUp ? '#ef4444' : '#22c55e';
    const bodyY = Math.min(item.openY, item.closeY);
    const bodyH = Math.max(Math.abs(item.closeY - item.openY), 2);
    return `
      <g class="chart-candle ${isUp ? 'up-candle' : 'down-candle'}">
        <line x1="${item.x}" y1="${item.highY}" x2="${item.x}" y2="${item.lowY}" stroke="${color}" stroke-width="1.5" stroke-linecap="round" />
        <rect x="${item.x - candleWidth / 2}" y="${bodyY}" width="${candleWidth}" height="${bodyH}" fill="${color}" fill-opacity="${isUp ? '0.72' : '0.86'}" stroke="${color}" stroke-width="1.7" rx="1.4" />
      </g>
    `;
  }).join('');

  svg.innerHTML = `
    <g class="chart-grid">
      ${gridYs.map((y) => `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" />`).join('')}
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" />
    </g>
    ${candles}
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${formatPrice(rawMax)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${formatPrice(rawMin)}</text>
    <text x="${padding.left}" y="${height - 6}" fill="#94a3b8" font-size="11">${escapeHtml(formatChartTime(points[0].label))}</text>
    <text x="${width - padding.right - 86}" y="${height - 6}" fill="#94a3b8" font-size="11">${escapeHtml(formatChartTime(points[lastIndex].label))}</text>
    <g class="chart-focus" style="display:none">
      <line data-focus-x x1="0" y1="${padding.top}" x2="0" y2="${height - padding.bottom}" />
      <line data-focus-y x1="${padding.left}" y1="0" x2="${width - padding.right}" y2="0" />
      <circle data-focus-dot r="4.2" cx="0" cy="0" />
      <text data-focus-label class="chart-focus-info" x="${width - padding.right}" y="20" text-anchor="end"></text>
      <text data-focus-price class="chart-focus-info" x="${width - padding.right}" y="38" text-anchor="end"></text>
      <text data-focus-extra class="chart-focus-info chart-focus-extra" x="${width - padding.right}" y="56" text-anchor="end"></text>
    </g>
    <rect data-chart-hit-area x="${padding.left}" y="${padding.top}" width="${plotWidth}" height="${plotHeight}" fill="transparent" />
  `;

  const focus = svg.querySelector('.chart-focus');
  const focusX = svg.querySelector('[data-focus-x]');
  const focusY = svg.querySelector('[data-focus-y]');
  const focusDot = svg.querySelector('[data-focus-dot]');
  const focusLabel = svg.querySelector('[data-focus-label]');
  const focusPrice = svg.querySelector('[data-focus-price]');
  const focusExtra = svg.querySelector('[data-focus-extra]');
  const hitArea = svg.querySelector('[data-chart-hit-area]');

  const showPoint = (index) => {
    const point = chartPoints[Math.max(0, Math.min(index, chartPoints.length - 1))];
    focus.style.display = 'block';
    focusX.setAttribute('x1', point.x);
    focusX.setAttribute('x2', point.x);
    focusY.setAttribute('y1', point.closeY);
    focusY.setAttribute('y2', point.closeY);
    focusDot.setAttribute('cx', point.x);
    focusDot.setAttribute('cy', point.closeY);
    focusLabel.textContent = point.periodLabel || formatChartTime(point.label);
    const pct = point.open ? ((point.close - point.open) / point.open) * 100 : null;
    focusPrice.textContent = `收 ${formatPrice(point.close)} · ${pct == null ? '-' : pct.toFixed(2)}%`;
    focusExtra.textContent = `开 ${formatPrice(point.open)} 高 ${formatPrice(point.high)} 低 ${formatPrice(point.low)}`;
  };

  const handleMove = (event) => {
    const rect = svg.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * width;
    const ratio = (x - padding.left) / plotWidth;
    const index = Math.floor(ratio * points.length);
    showPoint(index);
  };

  hitArea.addEventListener('pointermove', handleMove);
  hitArea.addEventListener('pointerdown', handleMove);
  hitArea.addEventListener('pointerleave', () => showPoint(lastIndex));
  showPoint(lastIndex);
}

function normalizeIntradayBars(bars = [], meta = {}) {
  const prevClose = Number(meta.prevClose);
  const hasPrevClose = Number.isFinite(prevClose) && prevClose > 0;
  const calcPct = (price) => {
    const value = Number(price);
    if (!hasPrevClose || !Number.isFinite(value)) return null;
    return ((value - prevClose) / prevClose) * 100;
  };
  const normalized = bars.map((item) => ({
    quote_minute: item.minute_time,
    latest_price: item.close,
    pct_chg: calcPct(item.close),
    volume: item.volume,
    amount: item.amount,
  }));

  const first = bars[0];
  const firstOpen = Number(first?.open);
  const firstClose = Number(first?.close);
  const firstMinute = String(first?.minute_time || '');
  if (
    first
    && Number.isFinite(firstOpen)
    && Number.isFinite(firstClose)
    && Math.abs(firstOpen - firstClose) >= 0.001
    && /09:31(?::\d{2})?$/.test(firstMinute)
  ) {
    normalized.unshift({
      quote_minute: firstMinute.replace('09:31', '09:30'),
      latest_price: firstOpen,
      pct_chg: calcPct(firstOpen),
      volume: null,
      amount: null,
      is_open_reference: true,
    });
  }

  return normalized;
}

function renderIntradayChart(points, meta = {}) {
  const svg = qs('#stock-intraday-chart');
  if (!svg) return;

  const minuteKey = (value) => {
    if (!value) return null;
    const text = String(value);
    const match = text.match(/(\d{2}:\d{2})/);
    return match ? match[1] : null;
  };
  const buildTradingTimeline = () => {
    const result = [];
    const pushRange = (startHour, startMinute, endHour, endMinute, session) => {
      let current = startHour * 60 + startMinute;
      const end = endHour * 60 + endMinute;
      while (current <= end) {
        const hour = String(Math.floor(current / 60)).padStart(2, '0');
        const minute = String(current % 60).padStart(2, '0');
        result.push({ label: `${hour}:${minute}`, session });
        current += 1;
      }
    };
    pushRange(9, 30, 11, 30, 'am');
    pushRange(13, 0, 15, 0, 'pm');
    return result;
  };

  const prevClose = Number(meta.prevClose);
  const hasPrevClose = Number.isFinite(prevClose) && prevClose > 0;
  const pctFromPrice = (price) => {
    const value = Number(price);
    if (!hasPrevClose || !Number.isFinite(value)) return null;
    return ((value - prevClose) / prevClose) * 100;
  };
  const byMinute = new Map((points || []).map((item) => [minuteKey(item.quote_minute), item]).filter(([key]) => key));
  const timeline = buildTradingTimeline().map((slot) => {
    const item = byMinute.get(slot.label);
    const value = item ? Number(item.latest_price) : null;
    const rawPct = item?.pct_chg == null ? pctFromPrice(value) : Number(item.pct_chg);
    const pct = rawPct != null && !Number.isNaN(rawPct) ? rawPct : null;
    return {
      ...slot,
      value: value != null && !Number.isNaN(value) ? value : null,
      pct,
      hasData: value != null && !Number.isNaN(value),
      carriedFrom: null,
    };
  });

  const morningClose = timeline.find((item) => item.label === '11:30' && item.hasData);
  const afternoonOpen = timeline.find((item) => item.label === '13:00');
  if (morningClose && afternoonOpen && !afternoonOpen.hasData) {
    afternoonOpen.value = morningClose.value;
    afternoonOpen.pct = morningClose.pct;
    afternoonOpen.hasData = true;
    afternoonOpen.carriedFrom = '11:30';
  }

  // 实时快照来自每分钟全市场采样，偶尔会因为采集耗时/上游波动漏掉 1-3 个分钟点。
  // 分时图的主心智是走势连续性，所以对同一交易时段内的短缺口用上一笔价格补齐；
  // 午休和长时间采集故障仍然保留断点，避免制造不存在的行情。
  timeline.forEach((item, index) => {
    if (item.hasData) return;
    const previousIndex = [...timeline].slice(0, index).reverse().findIndex((point) => point.hasData && point.session === item.session);
    if (previousIndex < 0) return;
    const actualPreviousIndex = index - previousIndex - 1;
    const previous = timeline[actualPreviousIndex];
    const nextIndex = timeline.findIndex((point, pointIndex) => pointIndex > index && point.hasData && point.session === item.session);
    if (nextIndex < 0) return;
    const gapSize = nextIndex - actualPreviousIndex;
    if (gapSize > 4) return;
    item.value = previous.value;
    item.pct = previous.pct;
    item.hasData = true;
    item.carriedFrom = previous.label;
  });

  const valid = timeline.filter((item) => item.hasData);
  if (valid.length < 2) {
    svg.innerHTML = '<text x="18" y="28" fill="#94a3b8" font-size="13">暂无今日分钟数据</text>';
    return;
  }

  const width = 640;
  const height = 220;
  const padding = { left: 44, right: 24, top: 30, bottom: 36 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const percentMode = valid.some((item) => item.pct != null && !Number.isNaN(Number(item.pct)));
  const values = valid.map((item) => percentMode ? Number(item.pct) : item.value).filter((value) => Number.isFinite(value));
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const domainMin = percentMode ? Math.min(rawMin, 0) : rawMin;
  const domainMax = percentMode ? Math.max(rawMax, 0) : rawMax;
  const extra = percentMode
    ? Math.max((domainMax - domainMin) * 0.08, 0.18)
    : Math.max((rawMax - rawMin) * 0.16, rawMax * 0.002, 0.01);
  const min = domainMin - extra;
  const max = domainMax + extra;
  const range = max - min || 1;
  const xOf = (index) => padding.left + (index * plotWidth) / (timeline.length - 1);
  const yOf = (value) => padding.top + ((max - value) / range) * plotHeight;
  const chartPoints = timeline.map((item, index) => ({
    ...item,
    index,
    x: xOf(index),
    chartValue: percentMode ? item.pct : item.value,
    y: item.hasData && (percentMode ? item.pct != null : item.value != null) ? yOf(percentMode ? item.pct : item.value) : null,
  }));
  const segments = [];
  let current = [];
  chartPoints.forEach((item) => {
    if (item.hasData && item.y != null) {
      current.push(`${item.x},${item.y}`);
    } else if (current.length) {
      if (current.length > 1) segments.push(current.join(' '));
      current = [];
    }
  });
  if (current.length > 1) segments.push(current.join(' '));
  const stroke = percentMode
    ? (Number(valid[valid.length - 1].pct) >= 0 ? '#ef4444' : '#22c55e')
    : (valid[valid.length - 1].value >= valid[0].value ? '#ef4444' : '#22c55e');
  const gridYs = [0, 0.25, 0.5, 0.75, 1].map((ratio) => padding.top + ratio * plotHeight);
  const zeroY = percentMode ? yOf(0) : null;
  const formatAxisValue = (value) => percentMode ? value.toFixed(2) + '%' : formatPrice(value);
  const sessionMarks = [
    { label: '09:30', index: 0, anchor: 'start' },
    { label: '11:30 / 13:00', index: 120.5, anchor: 'middle' },
    { label: '15:00', index: timeline.length - 1, anchor: 'end' },
  ];
  const lastDataIndex = chartPoints.map((item, index) => item.hasData ? index : -1).filter((index) => index >= 0).pop();

  svg.innerHTML = `
    <g class="chart-grid">
      ${gridYs.map((y) => `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" />`).join('')}
      ${percentMode ? `<line class="chart-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}" />` : ''}
      <line class="chart-session-gap" x1="${xOf(120.5)}" y1="${padding.top}" x2="${xOf(120.5)}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" />
    </g>
    ${segments.map((segment) => `<polyline class="chart-line" fill="none" stroke="${stroke}" stroke-width="2.4" points="${segment}" />`).join('')}
    <text class="intraday-axis-label" x="8" y="${padding.top + 4}">${formatAxisValue(rawMax)}</text>
    <text class="intraday-axis-label" x="8" y="${height - padding.bottom}">${formatAxisValue(rawMin)}</text>
    ${percentMode ? `<text class="intraday-axis-label intraday-zero-label" x="${width - padding.right + 4}" y="${zeroY + 3}">0%</text>` : ''}
    ${sessionMarks.map((mark) => `<text class="intraday-axis-label intraday-time-label" x="${xOf(mark.index)}" y="${height - 10}" text-anchor="${mark.anchor}">${mark.label}</text>`).join('')}
    <g class="chart-focus" style="display:none">
      <line data-focus-x x1="0" y1="${padding.top}" x2="0" y2="${height - padding.bottom}" />
      <line data-focus-y x1="${padding.left}" y1="0" x2="${width - padding.right}" y2="0" />
      <circle data-focus-dot r="4.5" cx="0" cy="0" />
      <text data-focus-label class="chart-focus-info" x="${width - padding.right}" y="20" text-anchor="end"></text>
      <text data-focus-price class="chart-focus-info" x="${width - padding.right}" y="38" text-anchor="end"></text>
    </g>
    <rect data-chart-hit-area x="${padding.left}" y="${padding.top}" width="${plotWidth}" height="${plotHeight}" fill="transparent" />
  `;

  const focus = svg.querySelector('.chart-focus');
  const focusX = svg.querySelector('[data-focus-x]');
  const focusY = svg.querySelector('[data-focus-y]');
  const focusDot = svg.querySelector('[data-focus-dot]');
  const focusLabel = svg.querySelector('[data-focus-label]');
  const focusPrice = svg.querySelector('[data-focus-price]');
  const hitArea = svg.querySelector('[data-chart-hit-area]');

  const showPoint = (index) => {
    const point = chartPoints[Math.max(0, Math.min(index, chartPoints.length - 1))];
    focus.style.display = 'block';
    focusX.setAttribute('x1', point.x);
    focusX.setAttribute('x2', point.x);
    focusLabel.textContent = point.label;
    if (!point.hasData || point.y == null) {
      focusY.setAttribute('y1', height - padding.bottom);
      focusY.setAttribute('y2', height - padding.bottom);
      focusDot.setAttribute('cx', point.x);
      focusDot.setAttribute('cy', height - padding.bottom);
      focusPrice.textContent = '暂无分钟快照';
      return;
    }
    focusY.setAttribute('y1', point.y);
    focusY.setAttribute('y2', point.y);
    focusDot.setAttribute('cx', point.x);
    focusDot.setAttribute('cy', point.y);
    const pctText = point.pct == null || Number.isNaN(Number(point.pct)) ? '' : ` · ${Number(point.pct).toFixed(2)}%`;
    const carryText = point.carriedFrom ? ` · 延续${point.carriedFrom}` : '';
    focusPrice.textContent = `价格 ${formatPrice(point.value)}${pctText}${carryText}`;
  };

  const handleMove = (event) => {
    const rect = svg.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * width;
    const ratio = (x - padding.left) / plotWidth;
    const index = Math.round(ratio * (chartPoints.length - 1));
    showPoint(index);
  };

  hitArea.addEventListener('pointermove', handleMove);
  hitArea.addEventListener('pointerdown', handleMove);
  hitArea.addEventListener('pointerleave', () => showPoint(lastDataIndex ?? chartPoints.length - 1));
  showPoint(lastDataIndex ?? chartPoints.length - 1);
}

function renderSelectionHistory(items) {
  const container = qs('#stock-detail-history');
  if (!items || !items.length) {
    container.innerHTML = '<div class="empty-state">暂无历史入选记录</div>';
    return;
  }

  container.innerHTML = `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>交易日</th>
            <th>策略</th>
            <th>分数</th>
            <th>排名</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(item.trade_date || '-')}</td>
              <td>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</td>
              <td>${formatNumber(item.score, 4)}</td>
              <td>${escapeHtml(item.rank_no ?? '-')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderRecentNews(items, latestSelection = {}) {
  const container = qs('#stock-detail-news');
  if (!container) return;
  if (!items || !items.length) {
    const rawMetrics = latestSelection.raw_metrics || {};
    const factorScores = latestSelection.factor_scores || {};
    const sentimentScore = factorScores.sentiment ?? rawMetrics.sentiment_score;
    const source = latestSelection.sentiment_source || rawMetrics.sentiment_source || 'fallback_price_volume';
    const newsCount = latestSelection.news_count ?? rawMetrics.news_count ?? 0;
    container.innerHTML = `
      <div class="empty-state sentiment-empty-state">
        <strong>暂无最近舆情新闻</strong>
        <div class="muted">当前新闻库没有该股可展示新闻；多因子情绪分已使用${escapeHtml(source === 'fallback_price_volume' ? '价格/成交量回退信号' : source)}计算。</div>
        <div class="muted">情绪因子：${formatNumber(sentimentScore, 2)} · 新闻数：${escapeHtml(newsCount)}</div>
      </div>
    `;
    return;
  }

  container.innerHTML = items.map((item) => {
    const hasCredibilityScore = item.credibility_score != null;
    const credibilityLevel = item.credibility_level || (hasCredibilityScore ? '-' : 'C');
    const credibilityScore = hasCredibilityScore ? Number(item.credibility_score).toFixed(2) : null;
    const qualityScore = item.quality_score == null ? '-' : Number(item.quality_score).toFixed(0);
    const qualityLevel = item.quality_level || '-';
    const sentimentValue = item.sentiment_score == null ? null : Number(item.sentiment_score);
    const sentiment = sentimentValue == null
      ? '中性/未明确'
      : Math.abs(sentimentValue) < 0.05
        ? '中性'
        : `${sentimentValue > 0 ? '偏正面' : '偏负面'} ${sentimentValue.toFixed(2)}`;
    const source = item.source || '未知来源';
    const publishedAt = item.published_at || '发布时间未知';
    const title = item.title || '(无标题)';
    const url = item.url || '';
    const titleHtml = url
      ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(title)}</a>`
      : escapeHtml(title);
    const summary = item.summary ? `<div class="muted news-summary">${escapeHtml(String(item.summary).slice(0, 140))}</div>` : '';
    const credibilityBadge = hasCredibilityScore
      ? `<span class="badge ${credibilityBadgeClass(credibilityLevel)}">可信度 ${escapeHtml(credibilityLevel)} · ${escapeHtml(credibilityScore)}</span>`
      : `<span class="badge ${credibilityBadgeClass(credibilityLevel)}">可信度 ${escapeHtml(credibilityLevel)} · 来源评级</span>`;
    return `
      <div class="preview-item stock-news-item">
        <div class="preview-main">
          <div class="status-row news-title-row">
            <strong>${titleHtml}</strong>
            ${credibilityBadge}
            <span class="badge status-muted">质量 ${escapeHtml(qualityLevel)} · ${escapeHtml(qualityScore)}/100</span>
          </div>
          <div class="muted">${escapeHtml(publishedAt)} · ${escapeHtml(source)}</div>
          ${summary}
          <div class="muted">情绪 ${escapeHtml(sentiment)} · ${item.credibility_reason ? escapeHtml(item.credibility_reason) : '按信源评级表估算'}</div>
        </div>
      </div>
    `;
  }).join('');
}

function credibilityBadgeClass(level) {
  if (level === 'S' || level === 'A') return 'status-ok';
  if (level === 'B') return 'status-warn';
  if (level === 'C' || level === 'D') return 'status-error';
  return 'status-muted';
}

async function loadStockDetail() {
  const code = decodeURIComponent(window.location.pathname.split('/').pop() || 'UNKNOWN');
  qs('#stock-detail-title').textContent = `个股详情: ${code}`;

  try {
    const [data, overview] = await Promise.all([
      fetchJson(`/api/stocks/${encodeURIComponent(code)}`),
      fetchJson(`/api/stocks/${encodeURIComponent(code)}/overview`),
    ]);
    const latestSelection = data.latest_selection || {};
    const factorScores = latestSelection.factor_scores || {};
    const realtime = data.realtime || {};

    qs('#stock-detail-title').textContent = `${escapeHtml(data.name || code)} (${escapeHtml(data.code || code)})`;
    qs('#stock-stat-close').textContent = formatPrice(realtime.latest_price ?? data.latest_kline?.close);
    qs('#stock-stat-change').textContent = formatPercent(realtime.pct_chg ?? data.latest_kline?.intraday_change_pct);
    qs('#stock-stat-change').classList.remove('up', 'down');
    const stockChangeClass = getPctClass(realtime.pct_chg ?? data.latest_kline?.intraday_change_pct);
    if (stockChangeClass) {
      qs('#stock-stat-change').classList.add(stockChangeClass);
    }
    qs('#stock-stat-score').textContent = formatNumber(latestSelection.score, 4);
    qs('#stock-stat-date').textContent = escapeHtml(realtime.quote_time || data.latest_kline?.trade_date || '-');
    renderOverviewConsole(overview);
    renderFactorScorePills(factorScores, latestSelection);
    renderChipPanel(data.chip);
    renderMoneyflowPanel(data.moneyflow, data.realtime_moneyflow);

    qs('#stock-detail-basic').innerHTML = `
      <div><strong>股票代码</strong></div>
      <div>${escapeHtml(data.code || '-')}</div>
      <div><strong>市场</strong></div>
      <div>${escapeHtml(data.market || '-')}</div>
      <div><strong>行业</strong></div>
      <div>${escapeHtml(data.industry || '-')}</div>
      <div><strong>类型</strong></div>
      <div>${escapeHtml(data.instrument_type || '-')}</div>
      <div><strong>上市日期</strong></div>
      <div>${escapeHtml(data.listing_date || '-')}</div>
      <div><strong>ST / 退市</strong></div>
      <div>${data.flags?.is_st ? 'ST' : '正常'} / ${data.flags?.is_delisted ? '已退市' : '未退市'}</div>
      <div><strong>更新时间</strong></div>
      <div>${escapeHtml(data.updated_at || '-')}</div>
      <div><strong>实时行情时间</strong></div>
      <div>${escapeHtml(realtime.quote_time || '-')}</div>
    `;

    const peStatusLabel = data.valuation?.pe_status_label || '-';
    const peStatusReason = data.valuation?.pe_status_reason || '';
    qs('#stock-detail-factors').innerHTML = `
      <div><strong>PE</strong></div>
      <div>${formatNumber(data.valuation?.pe_tushare, 2)}</div>
      <div><strong>PE状态</strong></div>
      <div>${escapeHtml(peStatusLabel)}${peStatusReason ? ` · ${escapeHtml(peStatusReason)}` : ''}</div>
      <div><strong>PB</strong></div>
      <div>${formatNumber(data.valuation?.pb_tushare, 2)}</div>
      <div><strong>换手率</strong></div>
      <div>${formatNumber(overview.capital_flow?.turnover_rate ?? factorScores.turnover, 2)}</div>
      <div><strong>量比</strong></div>
      <div>${formatNumber(overview.capital_flow?.volume_ratio, 2)}</div>
      <div><strong>20日位置</strong></div>
      <div>${formatPercent(overview.technical_summary?.position_20d_pct)}</div>
      <div><strong>估值更新时间</strong></div>
      <div>${escapeHtml(data.valuation?.valuation_updated_at || '-')}</div>
    `;

    qs('#stock-detail-fundamentals').innerHTML = `
      <div><strong>ROE</strong></div>
      <div>${formatNumber(data.fundamentals?.roe, 2)}</div>
      <div><strong>ROA</strong></div>
      <div>${formatNumber(data.fundamentals?.roa, 2)}</div>
      <div><strong>毛利率</strong></div>
      <div>${formatNumber(data.fundamentals?.grossprofit_margin, 2)}</div>
      <div><strong>净利率</strong></div>
      <div>${formatNumber(data.fundamentals?.netprofit_margin, 2)}</div>
      <div><strong>营收同比</strong></div>
      <div>${formatNumber(data.fundamentals?.revenue_yoy, 2)}</div>
      <div><strong>利润同比</strong></div>
      <div>${formatNumber(data.fundamentals?.profit_yoy, 2)}</div>
      <div><strong>报告期</strong></div>
      <div>${escapeHtml(data.fundamentals?.fundamental_period || '-')}</div>
    `;

    qs('#stock-detail-selection').innerHTML = `
      <div><strong>最近策略</strong></div>
      <div>${escapeHtml(latestSelection.strategy_display_name || latestSelection.strategy_id || '-')}</div>
      <div><strong>交易状态</strong></div>
      <div>${escapeHtml((latestSelection.sentiment_context || {}).trade_signal_label || factorScores.trade_signal_label || '-')}</div>
      <div><strong>最近分数</strong></div>
      <div>${formatNumber(latestSelection.score, 4)}</div>
      <div><strong>最近排名</strong></div>
      <div>${escapeHtml(latestSelection.rank_no ?? '-')}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(latestSelection.trade_date || '-')}</div>
      <div><strong>策略版本</strong></div>
      <div>${escapeHtml(latestSelection.strategy_version || '-')}</div>
      <div><strong>记录创建时间</strong></div>
      <div>${escapeHtml(latestSelection.created_at || '-')}</div>
    `;

    const riskTexts = [];
    if (data.fundamentals?.roe == null) riskTexts.push('缺少 ROE 数据');
    if (data.fundamentals?.revenue_yoy == null) riskTexts.push('缺少营收同比数据');
    if (data.valuation?.pe_status && data.valuation.pe_status !== 'valid') {
      riskTexts.push(`${data.valuation.pe_status_label || 'PE 状态待确认'}：${data.valuation.pe_status_reason || '估值因子按中性处理'}`);
    }
    if (data.flags?.is_st) riskTexts.push('股票处于 ST 状态');
    const positiveReasons = [];
    if (data.fundamentals?.roe != null && Number(data.fundamentals.roe) >= 10) positiveReasons.push(`ROE 良好 (${formatNumber(data.fundamentals.roe, 2)})`);
    if (data.valuation?.pb_tushare != null && Number(data.valuation.pb_tushare) <= 2) positiveReasons.push(`PB 偏低 (${formatNumber(data.valuation.pb_tushare, 2)})`);
    if (latestSelection.score != null) positiveReasons.push(`最近入选分数 ${formatNumber(latestSelection.score, 4)}`);

    qs('#stock-detail-reasons').innerHTML = `
      <div><strong>当前可见正向信号：</strong>${positiveReasons.length ? escapeHtml(positiveReasons.join('；')) : '暂无明显正向信号'}</div>
      <div class="muted"><strong>当前主要风险：</strong>${riskTexts.length ? escapeHtml(riskTexts.join('；')) : '暂无明显风险提示'}</div>
    `;

    qs('#stock-detail-tracking-summary').innerHTML = `
      <div><strong>最近复盘入口</strong></div>
      <div>${latestSelection.run_id ? escapeHtml(latestSelection.strategy_display_name || latestSelection.strategy_id || '最近入选记录') : '暂无可关联复盘'}</div>
      <div><strong>建议动作</strong></div>
      <div>${latestSelection.run_id ? '可直接跳转到跟踪复盘页查看整轮表现' : '当前暂无可关联复盘记录'}</div>
      <div><strong>当前价格</strong></div>
      <div>${formatPrice(realtime.latest_price ?? data.latest_kline?.close)}</div>
      <div><strong>实时涨跌幅</strong></div>
      <div>${formatPercent(realtime.pct_chg)}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(data.latest_kline?.trade_date || '-')}</div>
    `;

    const trackingLink = qs('#stock-detail-tracking-link');
    if (trackingLink && latestSelection.run_id) {
      trackingLink.href = `/tracking?run_id=${encodeURIComponent(latestSelection.run_id)}`;
    }

    setupKlineTabs(data.price_history || []);
    const intradayChartMeta = {
      prevClose: realtime.pre_close || data.latest_kline?.prev_close || null,
    };
    const cachedIntradayBars = data.intraday_bars || {};
    const cachedIntradayItems = cachedIntradayBars.items || [];
    const realtimeIntradayItems = data.realtime_intraday || [];
    const cacheIsStale = isIntradayCacheStale(cachedIntradayItems, realtimeIntradayItems);
    const mergedIntradayItems = mergeIntradayChartPoints(
      cachedIntradayItems,
      realtimeIntradayItems,
      intradayChartMeta,
    );
    const latestRealtimeMinute = latestIntradayMinuteKey(realtimeIntradayItems);

    if (cachedIntradayItems.length >= 2 && !cacheIsStale) {
      renderIntradayChart(normalizeIntradayBars(cachedIntradayItems, intradayChartMeta), {
        ...intradayChartMeta,
        label: `完整分钟线 · ${cachedIntradayBars.count || 0} 点 · 数据库缓存`,
      });
    } else {
      renderIntradayChart(mergedIntradayItems, {
        ...intradayChartMeta,
        label: cachedIntradayItems.length >= 2
          ? `分钟线 · 缓存 ${cachedIntradayItems.length} 点 + 实时尾段`
          : '实时采样线 · 正在按需补全完整分钟线',
      });
      refreshAndLoadIntradayBars(
        code,
        realtime.trade_date || data.latest_kline?.trade_date || '',
        latestRealtimeMinute,
      )
        .then((intradayBars) => {
          const refreshedItems = intradayBars.items || [];
          const refreshedIsStale = isIntradayCacheStale(refreshedItems, realtimeIntradayItems);
          const refreshedChartItems = mergeIntradayChartPoints(
            refreshedItems,
            realtimeIntradayItems,
            intradayChartMeta,
          );
          if (refreshedChartItems.length >= 2) {
            renderIntradayChart(refreshedChartItems, {
              ...intradayChartMeta,
              label: refreshedIsStale
                ? `分钟线 · 缓存 ${refreshedItems.length} 点 + 实时尾段 · 缓存暂未追平`
                : `完整分钟线 · ${intradayBars.count || refreshedItems.length} 点 · 数据库缓存`,
            });
          } else {
            renderIntradayChart(realtimeIntradayItems, { ...intradayChartMeta, label: '完整分钟线暂无数据，展示实时采样线' });
          }
        })
        .catch(() => renderIntradayChart(mergedIntradayItems, { ...intradayChartMeta, label: '完整分钟线补全失败，已展示实时尾段' }));
    }
    renderRecentNews(data.recent_news || [], latestSelection);
    renderSelectionHistory(data.selection_history || []);
  } catch (error) {
    ['#stock-detail-basic', '#stock-detail-factors', '#stock-detail-fundamentals', '#stock-detail-chip', '#stock-detail-moneyflow', '#stock-detail-selection', '#stock-detail-reasons', '#stock-detail-tracking-summary', '#stock-detail-news', '#stock-detail-history'].forEach((selector) => {
      qs(selector).innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadStockDetail();
  window.setInterval(() => {
    if (!document.hidden) loadStockDetail();
  }, 60 * 1000);
});
