let currentBacktestRunId = null;
let backtestPollTimer = null;
let currentBacktestCurve = [];
let currentBacktestChartMode = 'equity';

function pctCell(value) {
  const cls = getPctClass(value) || '';
  return `<span class="${cls}">${formatPercent(value)}</span>`;
}

function setBacktestStats(data) {
  const summary = data?.summary || {};
  qs('#backtest-detail-section').style.display = '';
  qs('#backtest-stat-days').textContent = data?.sample_days || summary.trade_days || `${data?.progress_done_days ?? 0}/${data?.progress_total_days ?? 0}`;
  qs('#backtest-stat-picks').textContent = data?.total_picks ?? summary.total_picks ?? data?.total_trades ?? summary.trade_count ?? '-';
  qs('#backtest-stat-total-return').innerHTML = pctCell(data?.total_return_pct ?? summary.total_return_pct);
  qs('#backtest-stat-avg-return').innerHTML = pctCell(data?.avg_return_pct ?? summary.avg_return_pct);
  qs('#backtest-stat-max-drawdown').innerHTML = pctCell(data?.max_drawdown_pct ?? summary.max_drawdown_pct);
  qs('#backtest-stat-win-rate').textContent = formatPercent(data?.win_rate_pct ?? summary.win_rate_pct);
  qs('#backtest-run-id').textContent = data?.run_id ? `run_id: ${data.run_id}` : '暂无 run';
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
  body.innerHTML = curve.slice(-8).reverse().map((item) => `
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
  (curve || []).forEach((item) => {
    const daily = Number(item.avg_return_1d_pct ?? item.avg_return_3d_pct ?? item.daily_return_pct ?? 0);
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
  const height = 260;
  const padding = { left: 48, right: 22, top: 24, bottom: 34 };
  const values = points.map((p) => p.value);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const pad = Math.max((rawMax - rawMin) * 0.16, currentBacktestChartMode === 'return' ? 0.5 : 0.01);
  const min = rawMin - pad;
  const max = rawMax + pad;
  const range = max - min || 1;
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const chart = points.map((p, index) => ({
    ...p,
    x: padding.left + index * plotWidth / (points.length - 1),
    y: padding.top + ((max - p.value) / range) * plotHeight,
  }));
  const polyline = chart.map((p) => `${p.x},${p.y}`).join(' ');
  const area = `${padding.left},${height - padding.bottom} ${polyline} ${width - padding.right},${height - padding.bottom}`;
  const stroke = points[points.length - 1].value >= points[0].value ? '#ef4444' : '#22c55e';
  const valueLabel = currentBacktestChartMode === 'return' ? '收益率' : '净值';
  const grid = [0, .25, .5, .75, 1].map((r) => padding.top + r * plotHeight);
  svg.innerHTML = `
    <defs>
      <linearGradient id="backtestEquityFill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="${stroke}" stop-opacity="0.32" />
        <stop offset="100%" stop-color="${stroke}" stop-opacity="0.02" />
      </linearGradient>
    </defs>
    <g class="chart-grid">${grid.map((y) => `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" />`).join('')}</g>
    <polygon points="${area}" fill="url(#backtestEquityFill)" />
    <polyline points="${polyline}" fill="none" stroke="${stroke}" stroke-width="2.6" />
    ${chart.map((p) => `<circle cx="${p.x}" cy="${p.y}" r="3" fill="${stroke}" opacity="0.9" />`).join('')}
    <text x="8" y="${padding.top + 4}" fill="#94a3b8" font-size="11">${currentBacktestChartMode === 'return' ? `${max.toFixed(2)}%` : max.toFixed(2)}</text>
    <text x="8" y="${height - padding.bottom}" fill="#94a3b8" font-size="11">${currentBacktestChartMode === 'return' ? `${min.toFixed(2)}%` : min.toFixed(2)}</text>
    <text x="${padding.left}" y="${height - 8}" fill="#94a3b8" font-size="11">${escapeHtml(points[0].label)}</text>
    <text x="${width - padding.right - 72}" y="${height - 8}" fill="#94a3b8" font-size="11">${escapeHtml(points[points.length - 1].label)}</text>
    <g class="chart-focus" style="display:none">
      <line data-focus-x x1="0" y1="${padding.top}" x2="0" y2="${height - padding.bottom}" />
      <line data-focus-y x1="${padding.left}" y1="0" x2="${width - padding.right}" y2="0" />
      <circle data-focus-dot r="4.5" cx="0" cy="0" />
      <rect data-focus-bg x="${width - padding.right - 188}" y="8" width="188" height="58" rx="10" />
      <text data-focus-label class="chart-focus-info" x="${width - padding.right - 10}" y="25" text-anchor="end"></text>
      <text data-focus-value class="chart-focus-info" x="${width - padding.right - 10}" y="43" text-anchor="end"></text>
      <text data-focus-extra class="chart-focus-info" x="${width - padding.right - 10}" y="60" text-anchor="end"></text>
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
    focusValue.textContent = `${valueLabel} ${currentBacktestChartMode === 'return' ? `${point.value.toFixed(2)}%` : point.value.toFixed(4)}`;
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

function renderTrades(items = []) {
  const body = qs('#backtest-trades-body');
  if (!items.length) {
    body.innerHTML = renderEmptyRow(7, '暂无个股明细');
    return;
  }
  body.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.trade_date || '')}</td>
      <td><a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.code || '')}</a></td>
      <td>${formatNumber(item.entry_price, 4)}</td>
      <td>${escapeHtml(item.exit_date_1d || '-')} / ${pctCell(item.return_1d_pct)}</td>
      <td>${escapeHtml(item.exit_date_3d || '-')} / ${pctCell(item.return_3d_pct)}</td>
      <td>${pctCell(item.max_gain_pct)}</td>
      <td>${pctCell(item.max_drawdown_pct)}</td>
    </tr>
  `).join('');
}

function renderRuns(items = []) {
  const container = qs('#backtest-runs-list');
  renderRecentRunsPanel(items);
  if (!items.length) {
    container.innerHTML = '<div class="empty-state">暂无历史回测任务</div>';
    return;
  }
  const activeItems = items.filter((item) => item.status === 'running' || item.status === 'queued');
  const featured = [...activeItems, ...items.filter((item) => !activeItems.includes(item))].slice(0, 2);
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
    const statusLabel = item.status === 'running' ? '运行中' : item.status === 'queued' ? '排队中' : item.status === 'success' ? (index === 0 ? '最近完成' : '历史完成') : '待命中';
    const progress = Math.max(0, Math.min(100, Number(item.progress_pct || 0)));
    const picks = item.request?.max_picks ?? item.total_picks ?? '-';
    return `
    <div class="backtest-run-card ${escapeHtml(item.status || 'unknown')}">
      <div class="backtest-run-head">
        <div>
          <div class="backtest-run-state-line">
            <i class="run-dot ${escapeHtml(item.status || 'unknown')}"></i>
            <span>${statusLabel}</span>
          </div>
          <strong>${escapeHtml(item.strategy_id || '-')}</strong>
          <span>${escapeHtml(item.start_date || '-')} → ${escapeHtml(item.end_date || '-')}　每日入选：${escapeHtml(picks)}</span>
        </div>
        <div class="backtest-progress-percent">${formatNumber(progress, 0)}%</div>
      </div>
      <div class="backtest-progress-track"><i style="width:${progress}%"></i></div>
      <div class="backtest-run-foot compact">
        <span class="muted">开始时间：${escapeHtml(item.started_at || '--')}</span>
        <span class="muted">${item.status === 'success' ? `完成：${escapeHtml(item.finished_at || '--')}` : `预计剩余：${formatEta(item.estimated_seconds_left)}`}</span>
        <div class="actions">
          <button class="btn btn-secondary btn-small" type="button" data-load-run="${escapeHtml(item.run_id || '')}" ${item.status === 'success' ? '' : 'disabled'}>${item.placeholder ? '暂无任务' : '查看详情'}</button>
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
  container.innerHTML = items.slice(0, 7).map((item) => `
    <button class="recent-backtest-row" type="button" data-load-run="${escapeHtml(item.run_id || '')}" ${item.status === 'success' ? '' : 'disabled'}>
      <span>${escapeHtml(item.strategy_id || '-')}</span>
      <span>${escapeHtml(item.start_date || '-')} → ${escapeHtml(item.end_date || '-')}</span>
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
    qs('#backtest-form-message').textContent = `取消失败：${error.message}`;
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

async function loadRuns({ autoLoadLatest = false } = {}) {
  const data = await fetchJson('/api/backtest/runs?limit=5');
  const items = data.items || [];
  renderRuns(items);
  updatePolling(items);
  if (autoLoadLatest && !currentBacktestRunId) {
    const latestSuccess = items.find((item) => item.status === 'success' && item.run_id);
    if (latestSuccess) {
      await loadBacktestResult(latestSuccess.run_id, { scroll: false });
    }
  }
}

async function loadTrades(runId) {
  if (!runId) {
    renderTrades([]);
    return;
  }
  const returnMode = qs('#backtest-return-mode')?.value || '1d';
  const data = await fetchJson(`/api/backtest/trades?run_id=${encodeURIComponent(runId)}&limit=20&return_mode=${encodeURIComponent(returnMode)}`);
  renderTrades(data.items || []);
}

async function loadBacktestResult(runId, options = {}) {
  if (!runId) return;
  const data = await fetchJson(`/api/backtest/results?run_id=${encodeURIComponent(runId)}`);
  currentBacktestRunId = data.run_id;
  setBacktestStats(data);
  renderCurve(data.curve || []);
  await loadTrades(data.run_id);
  if (options.scroll !== false) {
    qs('#backtest-detail-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

async function refreshBacktestPage() {
  await Promise.all([loadFactorStatus(), loadRuns({ autoLoadLatest: true })]);
}

async function runBacktest(event) {
  event.preventDefault();
  const message = qs('#backtest-form-message');
  message.textContent = '回测运行中...';
  const payload = {
    strategy_id: qs('#backtest-strategy-id').value,
    start_date: qs('#backtest-start-date').value,
    end_date: qs('#backtest-end-date').value,
    return_mode: qs('#backtest-return-mode').value,
    instrument_type: 'stock',
    use_adjusted_price: false,
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
    renderTrades([]);
    await Promise.all([loadRuns(), loadFactorStatus()]);
    message.textContent = `回测任务已创建：${data.run_id}，可在下方列表查看进度`;
  } catch (error) {
    message.textContent = `回测失败：${error.message}`;
  }
}

qs('#backtest-form')?.addEventListener('submit', runBacktest);
qs('#refresh-backtest-page')?.addEventListener('click', refreshBacktestPage);
qs('#backtest-return-mode')?.addEventListener('change', () => loadTrades(currentBacktestRunId));
qsa('[data-backtest-chart-mode]').forEach((button) => {
  button.addEventListener('click', () => {
    currentBacktestChartMode = button.dataset.backtestChartMode || 'equity';
    qsa('[data-backtest-chart-mode]').forEach((item) => item.classList.toggle('active', item === button));
    renderBacktestChart(currentBacktestCurve);
  });
});

refreshBacktestPage().catch((error) => {
  qs('#backtest-form-message').textContent = `页面初始化失败：${error.message}`;
});
