let currentBacktestRunId = null;
let backtestPollTimer = null;
let currentBacktestCurve = [];
let currentBacktestChartMode = 'equity';
let currentBacktestReturnMode = '1d';
let currentBacktestTradesPage = 1;
let currentBacktestTradesTotalPages = 0;
let currentBacktestTradesLimit = 10;


const BACKTEST_STRATEGY_LABELS = {
  lowvol_reversal: '低波反转策略 v2.1',
  v13_three_factor: '三因子策略',
  v12_legacy: '多因子策略',
  fund_chip_repair: '资金筹码修复选股',
  quality_lowvol: '质量低波选股',
  leader_tactics: '龙头战法选股',
  low_position_resonance: '低位共振修复',
  multi_timeframe_resonance: '多周期共振',
  a_share_sentiment: 'A股舆情选股',
};

function backtestStrategyLabel(strategyId) {
  return BACKTEST_STRATEGY_LABELS[strategyId] || strategyId || '-';
}

function backtestRunStrategyLabel(item) {
  return item?.strategy_display_name || backtestStrategyLabel(item?.strategy_id);
}

const BACKTEST_UNIVERSE_LABELS = {
  ALL_A: '历史全A',
  '000016.SH': '上证50',
  '000300.SH': '沪深300',
  '000905.SH': '中证500',
  '000852.SH': '中证1000',
};

function backtestUniverseLabel(item) {
  const code = item?.universe_code || item?.request?.universe_code || 'ALL_A';
  return item?.universe_label || BACKTEST_UNIVERSE_LABELS[code] || code;
}

async function loadBacktestStrategies() {
  const select = qs('#backtest-strategy-id');
  const submit = qs('#backtest-form button[type="submit"]');
  if (!select) return [];
  const previousValue = select.value;
  const data = await fetchJson('/api/strategies?instrument_type=stock');
  const items = (data.strategies || []).filter((item) => item.backtest_ready === true);
  items.forEach((item) => {
    BACKTEST_STRATEGY_LABELS[item.id] = item.display_name || item.id;
  });
  if (!items.length) {
    select.innerHTML = '<option value="">当前没有达到研究回测门槛的策略</option>';
    select.disabled = true;
    if (submit) submit.disabled = true;
    return [];
  }
  select.innerHTML = items.map((item) => (
    `<option value="${escapeHtml(item.id)}">${escapeHtml(item.display_name || item.id)}（研究态 · ${item.validation_status === 'validated' ? '已验证' : '未验证'}）</option>`
  )).join('');
  const preferred = items.some((item) => item.id === previousValue)
    ? previousValue
    : items.some((item) => item.id === data.default_strategy)
      ? data.default_strategy
      : items[0].id;
  select.value = preferred;
  select.disabled = false;
  if (submit) submit.disabled = false;
  syncBacktestStrategyDefaults();
  return items;
}

const BACKTEST_TRADE_STRATEGY_LABELS = {
  next_open_1d: '次日开盘卖出',
  hold_3d_close: '持有3日收盘',
  triple_barrier_5d: '五日止盈止损',
  observe_t3_daily: '选股专属回测',
};

function backtestTradeStrategyLabel(item) {
  const id = item?.trade_strategy_id || item?.request?.trade_strategy_id || (item?.return_mode === '3d' ? 'hold_3d_close' : item?.return_mode === 'triple_barrier_5d' ? 'triple_barrier_5d' : item?.return_mode === 'observe_t3_daily' ? 'observe_t3_daily' : 'next_open_1d');
  return BACKTEST_TRADE_STRATEGY_LABELS[id] || id || '-';
}

const BACKTEST_STRATEGY_DEFAULTS = {
  lowvol_reversal: {
    threshold: 60,
    maxPicks: 3,
  },
  v13_three_factor: {
    threshold: 65,
    maxPicks: 3,
  },
  fund_chip_repair: {
    threshold: 60,
    maxPicks: 3,
  },
  quality_lowvol: {
    threshold: 60,
    maxPicks: 3,
  },
  leader_tactics: {
    threshold: 60,
    maxPicks: 3,
  },
  low_position_resonance: {
    threshold: 60,
    maxPicks: 3,
  },
  multi_timeframe_resonance: {
    threshold: 60,
    maxPicks: 3,
  },
};

function syncBacktestStrategyDefaults() {
  const strategyId = qs('#backtest-strategy-id')?.value || 'lowvol_reversal';
  const defaults = BACKTEST_STRATEGY_DEFAULTS[strategyId];
  if (!defaults) return;
  const threshold = qs('#backtest-score-threshold');
  const maxPicks = qs('#backtest-max-picks');
  if (threshold) threshold.value = String(defaults.threshold);
  if (maxPicks) maxPicks.value = String(defaults.maxPicks);
}

function pctCell(value) {
  const cls = getPctClass(value) || '';
  return `<span class="${cls}">${formatPercent(value)}</span>`;
}

function formatRatio(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return Number(value).toFixed(2);
}

const BACKTEST_REJECTION_LABELS = {
  missing_entry_price: '缺少入场价',
  missing_bar: '缺少行情',
  suspended_or_no_open: '停牌/无开盘价',
  buy_blocked_limit_up: '涨停买不进',
  sell_blocked_limit_down: '跌停卖不出',
};

function renderExecutionSummary(data) {
  const container = qs('#backtest-execution-summary');
  if (!container) return;
  const summary = data?.summary || {};
  const counts = summary.rejection_counts || {};
  const rules = summary.execution_rule_summary || {};
  const entries = Object.entries(counts).filter(([, value]) => Number(value || 0) > 0);
  if (!entries.length && !rules.a_share_realistic) {
    container.hidden = true;
    container.innerHTML = '';
    return;
  }
  container.hidden = false;
  const reasonText = entries.length
    ? entries.map(([key, value]) => `<span class="badge status-warn">${escapeHtml(BACKTEST_REJECTION_LABELS[key] || key)} ${value}</span>`).join('')
    : '<span class="badge status-ok">无成交拒绝</span>';
  container.innerHTML = `
    <div class="card-header">
      <div>
        <p class="card-kicker">Execution</p>
        <h3>A 股真实化执行摘要</h3>
      </div>
    </div>
    <div class="backtest-execution-chips">
      <span class="badge ${rules.a_share_realistic ? 'status-ok' : 'status-muted'}">${rules.a_share_realistic ? '真实化已启用' : '研究口径'}</span>
      <span class="badge status-muted">佣金 ${Number(rules.commission_bps || 0)}bps</span>
      <span class="badge status-muted">印花 ${Number(rules.stamp_tax_bps || 0)}bps</span>
      <span class="badge status-muted">滑点 ${Number(rules.slippage_bps || 0)}bps</span>
      ${reasonText}
    </div>
    <p class="muted">${escapeHtml(rules.lot_size_rule || 'A 股整手规则将在仓位模型接入后影响交易数量。')}</p>
  `;
}

function setBacktestStats(data) {
  const summary = data?.summary || {};
  const adjustedLabel = data?.request?.use_adjusted_price ? ' · 复权收益' : ' · 不复权';
  const costLabel = (Number(data?.request?.commission_bps || 0) || Number(data?.request?.stamp_tax_bps || 0) || Number(data?.request?.slippage_bps || 0) || data?.request?.apply_execution_constraints)
    ? ` · 成本${Number(data?.request?.commission_bps || 0)}bps/印花${Number(data?.request?.stamp_tax_bps || 0)}bps/滑点${Number(data?.request?.slippage_bps || 0)}bps${data?.request?.apply_execution_constraints ? ' · 成交约束' : ''}`
    : '';
  const tradeStrategyLabel = data?.request?.trade_strategy_id ? ` · ${data.request.trade_strategy_id}` : '';
  qs('#backtest-detail-section').style.display = '';
  qs('#backtest-stat-days').textContent = data?.sample_days || summary.trade_days || `${data?.progress_done_days ?? 0}/${data?.progress_total_days ?? 0}`;
  qs('#backtest-stat-picks').textContent = data?.total_picks ?? summary.total_picks ?? data?.total_trades ?? summary.trade_count ?? '-';
  qs('#backtest-stat-total-return').innerHTML = pctCell(data?.total_return_pct ?? summary.total_return_pct);
  qs('#backtest-stat-avg-return').innerHTML = pctCell(data?.avg_return_pct ?? summary.avg_return_pct);
  qs('#backtest-stat-max-drawdown').innerHTML = pctCell(data?.max_drawdown_pct ?? summary.max_drawdown_pct);
  qs('#backtest-stat-win-rate').textContent = formatPercent(data?.win_rate_pct ?? summary.win_rate_pct);
  qs('#backtest-stat-sharpe').textContent = formatRatio(summary.sharpe_ratio);
  qs('#backtest-stat-sortino').textContent = formatRatio(summary.sortino_ratio);
  qs('#backtest-stat-calmar').textContent = formatRatio(summary.calmar_ratio);
  qs('#backtest-run-id').textContent = data?.run_id ? `run_id: ${data.run_id}${tradeStrategyLabel}${adjustedLabel}${costLabel}` : '暂无 run';
  renderExecutionSummary(data);
}

function formatEta(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return '-';
  const value = Number(seconds);
  if (value < 60) return `${Math.max(0, Math.round(value))} 秒`;
  return `${Math.floor(value / 60)} 分 ${Math.round(value % 60)} 秒`;
}

function statusBadgeClass(status) {
  if (status === 'success') return 'status-ok';
  if (status === 'failed') return 'status-error';
  if (status === 'running' || status === 'queued') return 'status-warn';
  return 'status-muted';
}

function renderCurve(curve = []) {
  const body = qs('#backtest-curve-body');
  currentBacktestCurve = curve || [];
  renderBacktestChart(currentBacktestCurve);
  if (!curve.length) {
    body.innerHTML = renderEmptyRow(4, '暂无日级结果');
    return;
  }
  body.innerHTML = curve.slice(-10).reverse().map((item) => `
    <tr>
      <td>${escapeHtml(item.trade_date || '')}</td>
      <td>${item.pick_count ?? '-'}</td>
      <td>${pctCell(item.avg_return_1d_pct)}</td>
      <td>${pctCell(item.avg_return_3d_pct)}</td>
    </tr>
  `).join('');
}

function renderBacktestChart(curve = []) {
  const svg = qs('#backtest-equity-chart');
  if (!svg) return;
  const points = [];
  let equity = 1;
  const returnField = currentBacktestReturnMode === '3d' || currentBacktestReturnMode === 'triple_barrier_5d' || currentBacktestReturnMode === 'observe_t3_daily' ? 'avg_return_3d_pct' : 'avg_return_1d_pct';
  (curve || []).forEach((item) => {
    const daily = Number(item[returnField] ?? item.daily_return_pct ?? 0);
    if (!Number.isNaN(daily)) equity *= (1 + daily / 100);
    points.push({
      label: item.trade_date || '-',
      value: currentBacktestChartMode === 'return' ? daily : equity,
      equity,
      dailyReturn: daily,
      pickCount: item.pick_count,
    });
  });
  if (points.length < 2) {
    svg.innerHTML = '<text x="22" y="42" fill="#64748b" font-size="13">暂无曲线数据，请点击已完成任务查看</text>';
    return;
  }

  const width = 720;
  const height = 336;
  const padding = { left: 62, right: 26, top: 28, bottom: 44 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const values = points.map((p) => p.value);
  const rawMin = Math.min(...values, currentBacktestChartMode === 'return' ? 0 : 1);
  const rawMax = Math.max(...values, currentBacktestChartMode === 'return' ? 0 : 1);
  const pad = Math.max((rawMax - rawMin) * 0.18, currentBacktestChartMode === 'return' ? 0.35 : 0.006);
  const min = rawMin - pad;
  const max = rawMax + pad;
  const range = max - min || 1;
  const yFor = (value) => padding.top + ((max - value) / range) * plotHeight;
  const xFor = (index) => padding.left + index * plotWidth / Math.max(points.length - 1, 1);
  const chart = points.map((p, index) => ({ ...p, x: xFor(index), y: yFor(p.value) }));
  const polyline = chart.map((p) => `${p.x},${p.y}`).join(' ');
  const area = `${padding.left},${height - padding.bottom} ${polyline} ${width - padding.right},${height - padding.bottom}`;
  const last = points[points.length - 1];
  const first = points[0];
  const stroke = last.value >= first.value ? '#ef4444' : '#22c55e';
  const fillId = stroke === '#ef4444' ? 'backtestPositiveFill' : 'backtestNegativeFill';
  const valueLabel = currentBacktestChartMode === 'return' ? '日收益' : '净值';
  const formatValue = (value) => currentBacktestChartMode === 'return' ? `${value.toFixed(2)}%` : value.toFixed(4);
  const yTicks = Array.from({ length: 5 }, (_, index) => min + (range * index / 4)).reverse();
  const xTickIndexes = [...new Set([0, Math.floor((points.length - 1) / 3), Math.floor((points.length - 1) * 2 / 3), points.length - 1])];
  const baselineValue = currentBacktestChartMode === 'return' ? 0 : 1;
  const baselineY = min < baselineValue && max > baselineValue ? yFor(baselineValue) : null;

  svg.innerHTML = `
    <defs>
      <linearGradient id="backtestPositiveFill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#ef4444" stop-opacity="0.30" />
        <stop offset="70%" stop-color="#ef4444" stop-opacity="0.07" />
        <stop offset="100%" stop-color="#ef4444" stop-opacity="0.00" />
      </linearGradient>
      <linearGradient id="backtestNegativeFill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#22c55e" stop-opacity="0.28" />
        <stop offset="70%" stop-color="#22c55e" stop-opacity="0.07" />
        <stop offset="100%" stop-color="#22c55e" stop-opacity="0.00" />
      </linearGradient>
      <filter id="backtestLineGlow" x="-20%" y="-20%" width="140%" height="140%">
        <feGaussianBlur stdDeviation="2.5" result="blur" />
        <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
      </filter>
    </defs>
    <rect x="0" y="0" width="${width}" height="${height}" fill="transparent" />
    <g class="chart-grid">
      ${yTicks.map((tick) => `<line x1="${padding.left}" y1="${yFor(tick)}" x2="${width - padding.right}" y2="${yFor(tick)}" />`).join('')}
      ${xTickIndexes.map((index) => `<line class="vertical" x1="${xFor(index)}" y1="${padding.top}" x2="${xFor(index)}" y2="${height - padding.bottom}" />`).join('')}
    </g>
    ${baselineY == null ? '' : `<line class="zero-line" x1="${padding.left}" y1="${baselineY}" x2="${width - padding.right}" y2="${baselineY}" />`}
    <g class="chart-axis-labels">
      ${yTicks.map((tick) => `<text x="${padding.left - 10}" y="${yFor(tick) + 4}" text-anchor="end">${formatValue(tick)}</text>`).join('')}
      ${xTickIndexes.map((index) => `<text x="${xFor(index)}" y="${height - 14}" text-anchor="middle">${escapeHtml(points[index].label.slice(5))}</text>`).join('')}
    </g>
    <polygon points="${area}" fill="url(#${fillId})" />
    <polyline class="equity-line glow" points="${polyline}" fill="none" stroke="${stroke}" />
    <polyline class="equity-line" points="${polyline}" fill="none" stroke="${stroke}" />
    ${chart.map((p, index) => `<circle class="chart-point ${index === chart.length - 1 ? 'latest' : ''}" cx="${p.x}" cy="${p.y}" r="${index === chart.length - 1 ? 4.5 : 2.8}" fill="${stroke}" />`).join('')}
    <g class="chart-focus" style="display:none">
      <line data-focus-x x1="0" y1="${padding.top}" x2="0" y2="${height - padding.bottom}" />
      <line data-focus-y x1="${padding.left}" y1="0" x2="${width - padding.right}" y2="0" />
      <circle data-focus-dot r="5" cx="0" cy="0" />
      <rect data-focus-bg x="${width - padding.right - 204}" y="10" width="204" height="72" rx="12" />
      <text data-focus-label class="chart-focus-info" x="${width - padding.right - 12}" y="29" text-anchor="end"></text>
      <text data-focus-value class="chart-focus-info strong" x="${width - padding.right - 12}" y="49" text-anchor="end"></text>
      <text data-focus-extra class="chart-focus-info" x="${width - padding.right - 12}" y="68" text-anchor="end"></text>
    </g>
    <rect data-chart-hit-area x="${padding.left}" y="${padding.top}" width="${plotWidth}" height="${plotHeight}" fill="transparent" />
  `;

  const focus = svg.querySelector('.chart-focus');
  const focusX = svg.querySelector('[data-focus-x]');
  const focusY = svg.querySelector('[data-focus-y]');
  const focusDot = svg.querySelector('[data-focus-dot]');
  const focusLabel = svg.querySelector('[data-focus-label]');
  const focusValue = svg.querySelector('[data-focus-value]');
  const focusExtra = svg.querySelector('[data-focus-extra]');
  const hitArea = svg.querySelector('[data-chart-hit-area]');
  const showPoint = (index) => {
    const point = chart[Math.max(0, Math.min(index, chart.length - 1))];
    focus.style.display = 'block';
    focusX.setAttribute('x1', point.x);
    focusX.setAttribute('x2', point.x);
    focusY.setAttribute('y1', point.y);
    focusY.setAttribute('y2', point.y);
    focusDot.setAttribute('cx', point.x);
    focusDot.setAttribute('cy', point.y);
    focusLabel.textContent = point.label;
    focusValue.textContent = `${valueLabel} ${formatValue(point.value)}`;
    focusExtra.textContent = `日收益 ${point.dailyReturn.toFixed(2)}% · 入选 ${point.pickCount ?? '-'} 只`;
  };
  const handleMove = (event) => {
    const rect = svg.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * width;
    const ratio = (x - padding.left) / plotWidth;
    showPoint(Math.round(ratio * (chart.length - 1)));
  };
  hitArea.addEventListener('pointermove', handleMove);
  hitArea.addEventListener('pointerdown', handleMove);
  hitArea.addEventListener('pointerleave', () => showPoint(chart.length - 1));
  showPoint(chart.length - 1);
}


const BACKTEST_FACTOR_LABELS = {
  turnover: '换手',
  lowvol: '低波',
  reversal: '反转',
  trend: '趋势',
  momentum: '动量',
  quality: '质量',
  sentiment: '情绪',
  value: '估值',
  liquidity: '流动性',
};

function buildBacktestFactorTooltip(item = {}) {
  const factors = item.factor_json?.factors || item.factors || {};
  const entries = Object.entries(factors || {}).filter(([, value]) => value !== null && value !== undefined && value !== '');
  const lines = [`入选总分：${formatNumber(item.entry_score, 2)}`];
  if (entries.length) {
    lines.push('因子分数：');
    entries.forEach(([key, value]) => lines.push(`${BACKTEST_FACTOR_LABELS[key] || key}: ${formatNumber(value, 2)}`));
  } else {
    lines.push('暂无因子明细');
  }
  return lines.join('\n');
}

function syncTradeReturnHeader(returnMode = currentBacktestReturnMode) {
  const header = qs('#backtest-trade-return-header');
  if (!header) return;
  header.textContent = returnMode === 'observe_t3_daily' ? 'T+3 收盘/收益' : returnMode === 'triple_barrier_5d' ? '止盈止损卖出/收益' : returnMode === '3d' ? '3 日卖出/收益' : '1 日卖出/收益';
}

function renderTradesPagination(meta = {}) {
  const container = qs('#backtest-trades-pagination');
  if (!container) return;
  const total = Number(meta.total || 0);
  const page = Number(meta.page || currentBacktestTradesPage || 1);
  const totalPages = Number(meta.total_pages || 0);
  currentBacktestTradesPage = page;
  currentBacktestTradesTotalPages = totalPages;
  if (!total) {
    container.innerHTML = '';
    return;
  }
  const start = (page - 1) * 10 + 1;
  const end = Math.min(page * 10, total);
  container.innerHTML = `
    <span class="muted">交易明细 ${start}-${end} / ${total}，按交易日倒序</span>
    <div class="actions">
      <label class="trades-page-size">每页
        <select id="backtest-trades-page-size">
          ${[10, 20, 50, 100].map((value) => `<option value="${value}" ${Number(meta.limit || currentBacktestTradesLimit) === value ? 'selected' : ''}>${value}</option>`).join('')}
        </select>
      </label>
      <button class="btn btn-secondary btn-small" type="button" data-trades-page="prev" ${page <= 1 ? 'disabled' : ''}>上一页</button>
      <span class="badge status-muted">${page} / ${Math.max(totalPages, 1)}</span>
      <button class="btn btn-secondary btn-small" type="button" data-trades-page="next" ${page >= totalPages ? 'disabled' : ''}>下一页</button>
    </div>
  `;
  container.querySelector('#backtest-trades-page-size')?.addEventListener('change', (event) => {
    currentBacktestTradesLimit = Number(event.target.value || 10);
    loadTrades(currentBacktestRunId, 1);
  });
  container.querySelector('[data-trades-page="prev"]')?.addEventListener('click', () => loadTrades(currentBacktestRunId, Math.max(1, page - 1)));
  container.querySelector('[data-trades-page="next"]')?.addEventListener('click', () => loadTrades(currentBacktestRunId, Math.min(totalPages, page + 1)));
}

function renderTradeHorizonDays(days = []) {
  if (!days.length) return '<span class="muted">入场日/入场+1/+2/+3/+4 数据不足</span>';
  return `
    <div class="backtest-horizon-grid">
      ${days.map((day) => `
        <div class="backtest-horizon-card">
          <div class="backtest-horizon-card-head">
            <strong>${escapeHtml(day.label || `T+${day.day_no || ''}`)}</strong>
            <span>${escapeHtml(day.trade_date || '-')}</span>
          </div>
          <div class="backtest-horizon-metrics">
            <div><span>收盘</span><b>${formatPrice(day.close_price)}</b><em>${pctCell(day.close_return_pct)}</em></div>
            <div><span>最大浮盈</span><b>${pctCell(day.max_gain_pct)}</b></div>
            <div><span>最大回撤</span><b>${pctCell(day.max_drawdown_pct)}</b></div>
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

function renderTrades(items = [], returnMode = currentBacktestReturnMode, meta = {}) {
  const body = qs('#backtest-trades-body');
  syncTradeReturnHeader(returnMode);
  renderTradesPagination(meta);
  if (!items.length) {
    body.innerHTML = renderEmptyRow(9, '暂无个股明细');
    return;
  }
  const isSelectionDiagnostics = returnMode === 'observe_t3_daily';
  const is3d = returnMode === '3d' || returnMode === 'triple_barrier_5d' || isSelectionDiagnostics;
  const horizonTitle = '选股专属回测：信号日后下一交易日入场，展示入场日 / +1 / +2 / +3 / +4 收盘与日内风险';
  body.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.trade_date || '')}</td>
      <td><a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.code || '')}</a></td>
      <td>${escapeHtml(item.name || '-')}</td>
      <td>
        <span>${formatNumber(item.entry_score, 2)}</span>
        <button class="icon-help backtest-score-help" type="button" data-tooltip="${escapeHtml(buildBacktestFactorTooltip(item))}">!</button>
      </td>
      <td>${formatPrice(item.entry_price)}</td>
      <td>${formatPrice(is3d ? item.exit_price_3d : item.exit_price_1d)}</td>
      <td>${escapeHtml((is3d ? item.exit_date_3d : item.exit_date_1d) || '-')} / ${pctCell(is3d ? item.return_3d_pct : item.return_1d_pct)}</td>
      <td>${pctCell(item.max_gain_pct)}</td>
      <td>${pctCell(item.max_drawdown_pct)}</td>
    </tr>
    ${isSelectionDiagnostics ? `
      <tr class="backtest-horizon-row">
        <td colspan="9">
          <div class="backtest-horizon-title">${horizonTitle}</div>
          ${renderTradeHorizonDays(item.horizon_days || [])}
        </td>
      </tr>
    ` : ''}
  `).join('');
  bindTooltips();
}

function renderRuns(items = []) {
  const container = qs('#backtest-runs-list');
  renderRecentRunsPanel(items);
  if (!items.length) {
    container.innerHTML = '<div class="empty-state">暂无历史回测任务</div>';
    return;
  }
  const activeItems = items.filter((item) => item.status === 'running' || item.status === 'queued');
  const sortedActiveItems = [...activeItems].sort((a, b) => {
    if (a.status !== b.status) return a.status === 'running' ? -1 : 1;
    return Number(b.progress_pct || 0) - Number(a.progress_pct || 0);
  });
  const primaryRunningRunId = sortedActiveItems[0]?.run_id || null;
  const featured = [...sortedActiveItems, ...items.filter((item) => !activeItems.includes(item))].slice(0, 2);
  if (featured.length === 1) {
    featured.push({
      strategy_id: '等待新任务',
      status: 'idle',
      start_date: '--',
      end_date: '--',
      return_mode: '--',
      progress_pct: 0,
      progress_done_days: 0,
      progress_total_days: 0,
      total_return_pct: null,
      avg_return_pct: null,
      max_drawdown_pct: null,
      win_rate_pct: null,
      total_trades: 0,
      run_id: '',
      estimated_seconds_left: null,
      placeholder: true,
    });
  }

  container.innerHTML = featured.map((item, index) => {
    const isWaiting = (item.status === 'queued') || (item.status === 'running' && primaryRunningRunId && item.run_id !== primaryRunningRunId);
    const displayStatus = isWaiting ? 'waiting' : item.status;
    const statusLabel = displayStatus === 'running' ? '运行中' : displayStatus === 'waiting' ? '等待中' : item.status === 'success' ? (index === 0 ? '最近完成' : '历史完成') : '待命中';
    const progress = Math.max(0, Math.min(100, Number(item.progress_pct || 0)));
    const picks = item.request?.max_picks ?? item.total_picks ?? '-';
    return `
    <div class="backtest-run-card ${escapeHtml(displayStatus || 'unknown')}">
      <div class="backtest-run-head">
        <div>
          <div class="backtest-run-state-line">
            <i class="run-dot ${escapeHtml(displayStatus || 'unknown')}"></i>
            <span>${statusLabel}</span>
          </div>
          <strong>${escapeHtml(backtestRunStrategyLabel(item))}</strong>
          <span>${escapeHtml(item.start_date || '-')} → ${escapeHtml(item.end_date || '-')}　${escapeHtml(backtestUniverseLabel(item))} · 每日入选：${escapeHtml(picks)}</span>
        </div>
        <div class="backtest-progress-percent">${formatNumber(progress, 0)}%</div>
      </div>
      <div class="backtest-progress-track"><i style="width:${progress}%"></i></div>
      <div class="backtest-run-foot compact">
        <span class="muted">开始时间：${escapeHtml(item.started_at || '--')}</span>
        <span class="muted">${item.status === 'success' ? `完成：${escapeHtml(item.finished_at || '--')}` : (displayStatus === 'waiting' ? '等待前序任务完成' : `预计剩余：${formatEta(item.estimated_seconds_left)}`)}</span>
        <div class="actions">
          <button class="btn btn-secondary btn-small" type="button" data-load-run="${escapeHtml(item.run_id || '')}" ${item.placeholder ? 'disabled' : ''}>${item.placeholder ? '暂无任务' : (displayStatus === 'waiting' ? '查看队列' : displayStatus === 'running' ? '查看已完成' : '查看详情')}</button>
          ${(item.status === 'queued' || item.status === 'running') ? `<button class="btn btn-secondary btn-small" type="button" data-cancel-run="${escapeHtml(item.run_id || '')}">取消</button>` : ''}
        </div>
      </div>
    </div>
  `}).join('');

  qsa('[data-load-run]').forEach((button) => {
    button.addEventListener('click', () => loadBacktestResult(button.dataset.loadRun));
  });
  qsa('[data-cancel-run]').forEach((button) => {
    button.addEventListener('click', () => cancelBacktestRun(button.dataset.cancelRun));
  });
}

function renderRecentRunsPanel(items = []) {
  const container = qs('#backtest-recent-runs-panel');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无最近回测';
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.slice(0, 20).map((item) => `
    <button class="recent-backtest-row" type="button" data-load-run="${escapeHtml(item.run_id || '')}">
      <span class="recent-selection-strategy" title="选股策略：${escapeHtml(backtestRunStrategyLabel(item))}">${escapeHtml(backtestRunStrategyLabel(item))}</span>
      <span class="recent-trade-strategy" title="交易策略：${escapeHtml(backtestTradeStrategyLabel(item))}">${escapeHtml(backtestTradeStrategyLabel(item))}</span>
      <span>${escapeHtml(item.start_date || '-')} → ${escapeHtml(item.end_date || '-')} · ${escapeHtml(backtestUniverseLabel(item))}${item.request?.use_adjusted_price ? ' · 复权' : ''}${(Number(item.request?.commission_bps || 0) || Number(item.request?.stamp_tax_bps || 0) || Number(item.request?.slippage_bps || 0) || item.request?.apply_execution_constraints) ? ' · 真实化' : ''}</span>
      <span><i class="badge ${statusBadgeClass(item.status)}">${escapeHtml(item.status || '-')}</i></span>
      <strong class="${getPctClass(item.total_return_pct) || ''}">${formatPercent(item.total_return_pct)}</strong>
    </button>
  `).join('');

  container.querySelectorAll('[data-load-run]').forEach((button) => {
    button.addEventListener('click', () => loadBacktestResult(button.dataset.loadRun));
  });
}

async function cancelBacktestRun(runId) {
  if (!runId) return;
  try {
    await fetchJson(`/api/backtest/runs/${encodeURIComponent(runId)}/cancel`, { method: 'POST' });
    await loadRuns();
    if (currentBacktestRunId === runId) await loadBacktestResult(runId);
  } catch (error) {
    const message = qs('#backtest-form-message');
    const messageShell = message?.closest('.backtest-terminal-foot');
    if (messageShell) messageShell.hidden = false;
    if (message) message.textContent = `取消失败：${error.message}`;
  }
}

function updatePolling(items = []) {
  const hasActive = items.some((item) => item.status === 'queued' || item.status === 'running');
  if (hasActive && !backtestPollTimer) {
    backtestPollTimer = setInterval(async () => {
      try {
        await loadRuns();
        if (currentBacktestRunId) await loadBacktestResult(currentBacktestRunId);
      } catch (error) {
        console.warn('backtest poll failed', error);
      }
    }, 4000);
  }
  if (!hasActive && backtestPollTimer) {
    clearInterval(backtestPollTimer);
    backtestPollTimer = null;
  }
}

function renderFactorStatus(data) {
  const container = qs('#factor-input-status');
  if (!container) return;
  const coverage = data?.coverage || {};
  const fields = coverage.fields || [];
  container.innerHTML = `
    <div class="status-row"><span>覆盖日期</span><strong>${escapeHtml(coverage.trade_date_start || '-')} ~ ${escapeHtml(coverage.trade_date_end || '-')}</strong></div>
    <div class="status-row"><span>覆盖股票</span><strong>${coverage.covered_stock_codes ?? '-'}</strong></div>
    <div class="status-row"><span>覆盖行数</span><strong>${coverage.covered_rows ?? '-'}</strong></div>
    <div class="status-detail">
      ${fields.map((item) => `<span class="badge status-ok">${escapeHtml(item.field)}: ${formatPercent(item.coverage_pct)}</span>`).join('')}
    </div>
    <div class="muted">最近任务：${escapeHtml(data?.latest_task?.run_id || '-')} · ${escapeHtml(data?.latest_task?.status || '-')}</div>
  `;
}

async function loadFactorStatus() {
  if (!qs('#factor-input-status')) return;
  try {
    const data = await fetchJson('/api/factor-input/status');
    renderFactorStatus(data);
  } catch (error) {
    renderError(qs('#factor-input-status'), `历史输入层状态加载失败：${error.message}`);
  }
}

function pickDefaultBacktestRun(items = []) {
  const running = items
    .filter((item) => item.status === 'running' && item.run_id)
    .sort((a, b) => Number(b.progress_pct || 0) - Number(a.progress_pct || 0))[0];
  if (running) return running;
  const queued = items.find((item) => item.status === 'queued' && item.run_id);
  if (queued) return queued;
  return items.find((item) => item.status === 'success' && item.run_id);
}

async function loadRuns({ autoLoadLatest = false } = {}) {
  const data = await fetchJson('/api/backtest/runs?limit=20&compact=true');
  const items = data.items || [];
  renderRuns(items);
  updatePolling(items);
  if (autoLoadLatest && !currentBacktestRunId) {
    const defaultRun = pickDefaultBacktestRun(items);
    if (defaultRun) {
      await loadBacktestResult(defaultRun.run_id, { scroll: false });
    }
  }
}

async function loadTrades(runId, page = currentBacktestTradesPage || 1) {
  if (!runId) {
    renderTrades([], currentBacktestReturnMode, { total: 0, page: 1, total_pages: 0 });
    return;
  }
  const returnMode = currentBacktestReturnMode || qs('#backtest-return-mode')?.value || '1d';
  const limit = currentBacktestTradesLimit || 10;
  const data = await fetchJson(`/api/backtest/trades?run_id=${encodeURIComponent(runId)}&limit=${encodeURIComponent(limit)}&page=${encodeURIComponent(page)}&return_mode=${encodeURIComponent(returnMode)}`);
  renderTrades(data.items || [], returnMode, data);
}

async function loadBacktestResult(runId, options = {}) {
  if (!runId) return;
  const data = await fetchJson(`/api/backtest/results?run_id=${encodeURIComponent(runId)}`);
  currentBacktestRunId = data.run_id;
  currentBacktestReturnMode = data.return_mode || data.request?.return_mode || qs('#backtest-return-mode')?.value || '1d';
  currentBacktestTradesPage = options.keepTradePage ? currentBacktestTradesPage : 1;
  setBacktestStats(data);
  renderCurve(data.curve || []);
  await loadTrades(data.run_id);
  if (options.scroll !== false) {
    qs('#backtest-detail-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

async function refreshBacktestPage() {
  await loadBacktestStrategies();
  await loadFactorStatus();
  await loadRuns({ autoLoadLatest: true });
}

async function runBacktest(event) {
  event.preventDefault();
  const message = qs('#backtest-form-message');
  const messageShell = message?.closest('.backtest-terminal-foot');
  if (messageShell) messageShell.hidden = true;
  if (message) message.textContent = '';
  const tradeStrategyId = qs('#backtest-trade-strategy-id')?.value || 'next_open_1d';
  const returnMode = tradeStrategyId === 'observe_t3_daily' ? 'observe_t3_daily' : tradeStrategyId === 'triple_barrier_5d' ? 'triple_barrier_5d' : tradeStrategyId === 'hold_3d_close' ? '3d' : '1d';
  if (qs('#backtest-return-mode')) qs('#backtest-return-mode').value = returnMode;
  const payload = {
    strategy_id: qs('#backtest-strategy-id').value,
    start_date: qs('#backtest-start-date').value,
    end_date: qs('#backtest-end-date').value,
    return_mode: returnMode,
    trade_strategy_id: tradeStrategyId,
    instrument_type: 'stock',
    universe_code: qs('#backtest-universe-code')?.value || 'ALL_A',
    use_adjusted_price: Boolean(qs('#backtest-use-adjusted-price')?.checked),
    commission_bps: Number(qs('#backtest-commission-bps')?.value || 0),
    stamp_tax_bps: Number(qs('#backtest-stamp-tax-bps')?.value || 0),
    slippage_bps: Number(qs('#backtest-slippage-bps')?.value || 0),
    apply_execution_constraints: Boolean(qs('#backtest-execution-constraints')?.checked),
    save: true,
    max_picks: Number(qs('#backtest-max-picks').value || 3),
    score_threshold: Number(qs('#backtest-score-threshold').value || 60),
  };

  try {
    const data = await fetchJson('/api/backtest/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    currentBacktestRunId = data.run_id;
    qs('#backtest-detail-section').style.display = '';
    renderCurve([]);
    renderTrades([], currentBacktestReturnMode, { total: 0, page: 1, total_pages: 0 });
    await loadRuns();
    await loadFactorStatus();
    if (messageShell) messageShell.hidden = true;
  } catch (error) {
    if (messageShell) messageShell.hidden = false;
    if (message) message.textContent = `回测失败：${error.message}`;
  }
}

function applyAShareRealisticPreset() {
  const adjusted = qs('#backtest-use-adjusted-price');
  const constraints = qs('#backtest-execution-constraints');
  const commission = qs('#backtest-commission-bps');
  const stamp = qs('#backtest-stamp-tax-bps');
  const slippage = qs('#backtest-slippage-bps');
  if (adjusted) adjusted.checked = false;
  if (constraints) constraints.checked = true;
  if (commission) commission.value = '2.5';
  if (stamp) stamp.value = '5';
  if (slippage) slippage.value = '5';
  const message = qs('#backtest-form-message');
  const messageShell = message?.closest('.backtest-terminal-foot');
  if (messageShell) messageShell.hidden = false;
  if (message) message.textContent = '已应用 A 股真实化预设：不复权、成交约束、佣金2.5bps、印花5bps、滑点5bps。';
}

qs('#backtest-form')?.addEventListener('submit', runBacktest);
qs('#refresh-backtest-page')?.addEventListener('click', refreshBacktestPage);
qs('#backtest-return-mode')?.addEventListener('change', () => {
  currentBacktestReturnMode = qs('#backtest-return-mode')?.value || '1d';
  currentBacktestTradesPage = 1;
  loadTrades(currentBacktestRunId, 1);
});
qs('#backtest-trade-strategy-id')?.addEventListener('change', () => {
  const tradeStrategyId = qs('#backtest-trade-strategy-id')?.value || 'next_open_1d';
  currentBacktestReturnMode = tradeStrategyId === 'observe_t3_daily' ? 'observe_t3_daily' : tradeStrategyId === 'triple_barrier_5d' ? 'triple_barrier_5d' : tradeStrategyId === 'hold_3d_close' ? '3d' : '1d';
  if (qs('#backtest-return-mode')) qs('#backtest-return-mode').value = currentBacktestReturnMode;
  currentBacktestTradesPage = 1;
  loadTrades(currentBacktestRunId, 1);
});
qs('#backtest-strategy-id')?.addEventListener('change', syncBacktestStrategyDefaults);
qs('#backtest-ashare-realistic-preset')?.addEventListener('click', applyAShareRealisticPreset);
qsa('[data-backtest-chart-mode]').forEach((button) => {
  button.addEventListener('click', () => {
    currentBacktestChartMode = button.dataset.backtestChartMode || 'equity';
    qsa('[data-backtest-chart-mode]').forEach((item) => item.classList.toggle('active', item === button));
    renderBacktestChart(currentBacktestCurve);
  });
});

syncBacktestStrategyDefaults();
refreshBacktestPage().catch((error) => {
  const message = qs('#backtest-form-message');
  const messageShell = message?.closest('.backtest-terminal-foot');
  if (messageShell) messageShell.hidden = false;
  if (message) message.textContent = `页面初始化失败：${error.message}`;
});
