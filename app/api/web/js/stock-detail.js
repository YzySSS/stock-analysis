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
  qs('#stock-overview-open').textContent = formatNumber(quote.open_price, 2);
  qs('#stock-overview-high').textContent = formatNumber(quote.high_price, 2);
  qs('#stock-overview-low').textContent = formatNumber(quote.low_price, 2);
  qs('#stock-overview-amount').textContent = formatMoneyCN(quote.amount);
  const trendLabelEl = qs('#stock-overview-trend');
  if (trendLabelEl) trendLabelEl.textContent = `趋势 ${technical.trend_label || '-'}`;
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
  ];
  const rawMetricKeys = new Set([
    'open', 'high', 'low', 'close', 'volume', 'amount', 'trade_date',
    'pe_tushare', 'pb_tushare', 'roe', 'roa', 'eps',
    'grossprofit_margin', 'netprofit_margin', 'revenue_yoy', 'profit_yoy',
    'completeness_score', 'data_quality_score', 'value_score', 'quality_score', 'stability_score',
    'fundamental_missing_fields',
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
  const strategyVersion = latestSelection.strategy_version ? ` · ${latestSelection.strategy_version}` : '';
  const scoreLabel = qs('#stock-strategy-score-label');
  const scoreSubtitle = qs('#stock-strategy-score-subtitle');
  const caption = qs('#stock-strategy-factor-caption');
  if (scoreLabel) scoreLabel.textContent = strategyName;
  if (scoreSubtitle) scoreSubtitle.textContent = latestSelection.strategy_id ? `${latestSelection.strategy_id}${strategyVersion}` : '策略 -';
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
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${rawMax.toFixed(2)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${rawMin.toFixed(2)}</text>
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
    focusPrice.textContent = `价格 ${point.value.toFixed(2)}${pctText}`;
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

function renderPriceChart(history) {
  const svg = qs('#stock-price-chart');
  if (!svg) return;
  const points = (history || [])
    .map((item) => ({
      label: item.trade_date || '-',
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
    }))
    .filter((item) => [item.open, item.high, item.low, item.close].every((value) => value != null && !Number.isNaN(value)));

  if (points.length < 2) {
    svg.innerHTML = '<text x="18" y="28" fill="#94a3b8" font-size="13">暂无日线数据</text>';
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
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${rawMax.toFixed(2)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${rawMin.toFixed(2)}</text>
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
    focusLabel.textContent = formatChartTime(point.label);
    const pct = point.open ? ((point.close - point.open) / point.open) * 100 : null;
    focusPrice.textContent = `收 ${point.close.toFixed(2)} · ${pct == null ? '-' : pct.toFixed(2)}%`;
    focusExtra.textContent = `开 ${point.open.toFixed(2)} 高 ${point.high.toFixed(2)} 低 ${point.low.toFixed(2)}`;
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

function renderIntradayChart(points) {
  const svg = qs('#stock-intraday-chart');
  if (!svg) return;

  const minuteKey = (value) => {
    if (!value) return null;
    const text = String(value);
    const match = text.match(/(\d{2}:\d{2})/);
    return match ? match[1] : null;
  };
  const buildTradingMinutes = () => {
    const result = [];
    const pushRange = (startHour, startMinute, endHour, endMinute) => {
      let current = startHour * 60 + startMinute;
      const end = endHour * 60 + endMinute;
      while (current <= end) {
        const hour = String(Math.floor(current / 60)).padStart(2, '0');
        const minute = String(current % 60).padStart(2, '0');
        result.push(`${hour}:${minute}`);
        current += 1;
      }
    };
    pushRange(9, 30, 11, 30);
    pushRange(13, 0, 15, 0);
    return result;
  };

  const byMinute = new Map((points || []).map((item) => [minuteKey(item.quote_minute), item]).filter(([key]) => key));
  const timeline = buildTradingMinutes().map((label) => {
    const item = byMinute.get(label);
    const value = item ? Number(item.latest_price) : null;
    return {
      label,
      value: value != null && !Number.isNaN(value) ? value : null,
      pct: item?.pct_chg == null ? null : Number(item.pct_chg),
      hasData: value != null && !Number.isNaN(value),
    };
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
  const values = valid.map((item) => item.value);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const extra = Math.max((rawMax - rawMin) * 0.16, rawMax * 0.002, 0.01);
  const min = rawMin - extra;
  const max = rawMax + extra;
  const range = max - min || 1;
  const xOf = (index) => padding.left + (index * plotWidth) / (timeline.length - 1);
  const yOf = (value) => padding.top + ((max - value) / range) * plotHeight;
  const chartPoints = timeline.map((item, index) => ({
    ...item,
    index,
    x: xOf(index),
    y: item.hasData ? yOf(item.value) : null,
  }));
  const segments = [];
  let current = [];
  chartPoints.forEach((item) => {
    if (item.hasData) {
      current.push(`${item.x},${item.y}`);
    } else if (current.length) {
      if (current.length > 1) segments.push(current.join(' '));
      current = [];
    }
  });
  if (current.length > 1) segments.push(current.join(' '));
  const stroke = valid[valid.length - 1].value >= valid[0].value ? '#ef4444' : '#22c55e';
  const gridYs = [0, 0.25, 0.5, 0.75, 1].map((ratio) => padding.top + ratio * plotHeight);
  const sessionMarks = [
    { label: '09:30', index: 0, anchor: 'start' },
    { label: '11:30 / 13:00', index: 120.5, anchor: 'middle' },
    { label: '15:00', index: timeline.length - 1, anchor: 'end' },
  ];
  const lastDataIndex = chartPoints.map((item, index) => item.hasData ? index : -1).filter((index) => index >= 0).pop();

  svg.innerHTML = `
    <g class="chart-grid">
      ${gridYs.map((y) => `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" />`).join('')}
      <line class="chart-session-gap" x1="${xOf(120.5)}" y1="${padding.top}" x2="${xOf(120.5)}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" />
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" />
    </g>
    ${segments.map((segment) => `<polyline class="chart-line" fill="none" stroke="${stroke}" stroke-width="2.4" points="${segment}" />`).join('')}
    ${chartPoints.filter((item) => !item.hasData).length ? `<text x="${padding.left + 8}" y="${padding.top - 8}" fill="#64748b" font-size="10">空白区间表示暂无分钟快照</text>` : ''}
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${rawMax.toFixed(2)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${rawMin.toFixed(2)}</text>
    ${sessionMarks.map((mark) => `<text x="${xOf(mark.index)}" y="${height - 10}" text-anchor="${mark.anchor}" fill="#94a3b8" font-size="11">${mark.label}</text>`).join('')}
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
    if (!point.hasData) {
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
    focusPrice.textContent = `价格 ${point.value.toFixed(2)}${pctText}`;
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
            <th>run_id</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(item.trade_date || '-')}</td>
              <td>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</td>
              <td>${formatNumber(item.score, 4)}</td>
              <td>${escapeHtml(item.rank_no ?? '-')}</td>
              <td>${escapeHtml(item.run_id || '-')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderRecentNews(items) {
  const container = qs('#stock-detail-news');
  if (!container) return;
  if (!items || !items.length) {
    container.innerHTML = '<div class="empty-state">暂无最近舆情新闻</div>';
    return;
  }

  container.innerHTML = items.map((item) => {
    const credibilityLevel = item.credibility_level || '-';
    const credibilityScore = item.credibility_score == null ? '-' : Number(item.credibility_score).toFixed(2);
    const qualityScore = item.quality_score == null ? '-' : Number(item.quality_score).toFixed(0);
    const qualityLevel = item.quality_level || '-';
    const sentimentValue = item.sentiment_score == null ? null : Number(item.sentiment_score);
    const sentiment = sentimentValue == null
      ? '待分析'
      : Math.abs(sentimentValue) < 0.05
        ? '中性'
        : `${sentimentValue > 0 ? '偏正面' : '偏负面'} ${sentimentValue.toFixed(2)}`;
    const source = item.source || '未知来源';
    const publishedAt = item.published_at || item.created_at || '-';
    const title = item.title || '(无标题)';
    const url = item.url || '';
    const titleHtml = url
      ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(title)}</a>`
      : escapeHtml(title);
    const summary = item.summary ? `<div class="muted news-summary">${escapeHtml(String(item.summary).slice(0, 140))}</div>` : '';
    return `
      <div class="preview-item stock-news-item">
        <div class="preview-main">
          <div class="status-row news-title-row">
            <strong>${titleHtml}</strong>
            <span class="badge ${credibilityBadgeClass(credibilityLevel)}">可信度 ${escapeHtml(credibilityLevel)} · ${escapeHtml(credibilityScore)}</span>
            <span class="badge status-muted">质量 ${escapeHtml(qualityLevel)} · ${escapeHtml(qualityScore)}/100</span>
          </div>
          <div class="muted">${escapeHtml(publishedAt)} · ${escapeHtml(source)}</div>
          ${summary}
          <div class="muted">情绪 ${escapeHtml(sentiment)} · ${item.credibility_reason ? escapeHtml(item.credibility_reason) : '可信度待评分'}</div>
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
    qs('#stock-detail-subtitle').textContent = `${escapeHtml(data.industry || '未分类行业')} · ${escapeHtml(data.market || '-')} · ${escapeHtml(data.instrument_type || '-')}`;

    qs('#stock-stat-close').textContent = formatNumber(realtime.latest_price ?? data.latest_kline?.close, 2);
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

    qs('#stock-detail-factors').innerHTML = `
      <div><strong>PE</strong></div>
      <div>${formatNumber(data.valuation?.pe_tushare, 2)}</div>
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
    if (data.valuation?.pe_tushare == null) riskTexts.push('缺少 PE 数据');
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
      <div>${formatNumber(realtime.latest_price ?? data.latest_kline?.close, 2)}</div>
      <div><strong>实时涨跌幅</strong></div>
      <div>${formatPercent(realtime.pct_chg)}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(data.latest_kline?.trade_date || '-')}</div>
    `;

    const trackingLink = qs('#stock-detail-tracking-link');
    if (trackingLink && latestSelection.run_id) {
      trackingLink.href = `/tracking?run_id=${encodeURIComponent(latestSelection.run_id)}`;
    }

    renderPriceChart(data.price_history || []);
    renderIntradayChart(data.realtime_intraday || []);
    renderRecentNews(data.recent_news || []);
    renderSelectionHistory(data.selection_history || []);
  } catch (error) {
    qs('#stock-detail-subtitle').textContent = '加载详情失败';
    ['#stock-detail-basic', '#stock-detail-factors', '#stock-detail-fundamentals', '#stock-detail-selection', '#stock-detail-reasons', '#stock-detail-tracking-summary', '#stock-detail-news', '#stock-detail-history'].forEach((selector) => {
      qs(selector).innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    });
  }
}

document.addEventListener('DOMContentLoaded', loadStockDetail);
