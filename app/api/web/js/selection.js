let lastSelectionResponse = null;
let hasExecutedSelection = false;
let selectionRunTimer = null;
let selectionRunStartedAt = null;
let selectionRunBackendStatus = null;
const savedSelectionKeys = new Set();
const SELECTION_RUN_DURATION_KEY = 'selection.run.durationMs';
const DEFAULT_SELECTION_RUN_ESTIMATE_MS = 110000;

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function formatRunDuration(ms) {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function getSelectionRunEstimateMs() {
  const stored = Number(window.localStorage?.getItem(SELECTION_RUN_DURATION_KEY));
  if (stored && !Number.isNaN(stored)) {
    return clampNumber(stored * 1.25, 15000, 120000);
  }
  return DEFAULT_SELECTION_RUN_ESTIMATE_MS;
}

function rememberSelectionRunDuration(ms) {
  if (!ms || Number.isNaN(ms)) return;
  const previous = Number(window.localStorage?.getItem(SELECTION_RUN_DURATION_KEY));
  const next = previous && !Number.isNaN(previous) ? previous * 0.65 + ms * 0.35 : ms;
  window.localStorage?.setItem(SELECTION_RUN_DURATION_KEY, String(Math.round(clampNumber(next, 8000, 120000))));
}

function updateSelectionRunProgress() {
  if (!selectionRunStartedAt) return;
  const liveRun = qs('#selection-live-run');
  const progress = qs('#selection-live-run-progress');
  const elapsedNode = qs('#selection-live-run-elapsed');
  const remainingNode = qs('#selection-live-run-remaining');
  const phaseNode = qs('#selection-live-run-phase');
  if (!liveRun || !progress || !elapsedNode || !remainingNode || !phaseNode) return;

  const backend = selectionRunBackendStatus;
  const elapsedMs = backend?.elapsed_seconds != null ? Number(backend.elapsed_seconds) * 1000 : Date.now() - selectionRunStartedAt;
  const estimateMs = getSelectionRunEstimateMs();
  const ratio = elapsedMs / Math.max(estimateMs, 1);
  const backendProgress = Number(backend?.progress_pct);
  const progressPct = Number.isFinite(backendProgress) && backendProgress > 0
    ? clampNumber(backendProgress, 0, backend?.status === 'success' ? 100 : 96)
    : ratio >= 1 ? 94 : clampNumber(8 + ratio * 78, 8, 90);
  const remainingMs = backend?.estimated_seconds_left != null ? Number(backend.estimated_seconds_left) * 1000 : estimateMs - elapsedMs;
  const phase = backend?.phase || (ratio < 0.18
    ? '正在提交任务'
    : ratio < 0.52
      ? '读取全市场数据'
      : ratio < 0.82
        ? '计算因子和排序'
        : '整理选股结果');

  liveRun.hidden = false;
  progress.style.width = `${progressPct}%`;
  elapsedNode.textContent = formatRunDuration(elapsedMs);
  phaseNode.textContent = phase;
  remainingNode.textContent = remainingMs > 0
    ? `预计还需约 ${formatRunDuration(remainingMs)}`
    : `已超过预计 ${formatRunDuration(Math.abs(remainingMs))}，仍在计算`;
}

function selectionRunStatusLabel(status) {
  return {
    queued: '排队中',
    running: '运行中',
    success: '运行成功',
    no_data: '数据不足',
    failed: '运行失败',
    cancelled: '已取消',
  }[status] || status || '运行中';
}

function renderSelectionTaskStatus(status) {
  if (!status) return;
  selectionRunBackendStatus = status;
  if (status.run_id && qs('#selection-run-id')) {
    qs('#selection-run-id').value = status.run_id;
  }
  const state = qs('#selection-run-state');
  if (state) {
    state.textContent = selectionRunStatusLabel(status.status);
    state.classList.remove('up', 'down', 'running');
    if (status.status === 'success') state.classList.add('up');
    else if (status.status === 'failed' || status.status === 'cancelled' || status.status === 'no_data') state.classList.add('down');
    else state.classList.add('running');
  }
  const cancelButton = qs('#selection-cancel-run');
  if (cancelButton) {
    const cancellable = status.status === 'queued' || status.status === 'running';
    cancelButton.hidden = !cancellable;
    cancelButton.disabled = Boolean(status.cancel_requested);
    cancelButton.textContent = status.cancel_requested ? '正在取消…' : '取消任务';
  }
  const runTime = qs('#selection-run-time');
  if (runTime) {
    runTime.textContent = status.started_at || status.created_at || '刚刚开始';
  }
  const count = qs('#selection-qualified-count');
  if (count) {
    count.textContent = status.status === 'success'
      ? String(status.result_count ?? 0)
      : status.status === 'queued'
        ? '排队中'
        : status.status === 'cancelled'
          ? '已取消'
          : '计算中';
  }
  updateSelectionRunProgress();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function applySelectionRunStatus(status) {
  renderSelectionTaskStatus(status);
  if (status.status === 'success') return true;
  if (status.status === 'cancelled') {
    throw new Error('选股任务已取消');
  }
  if (status.status === 'no_data') {
    throw new Error(status.error_message || '当前数据不足，任务未执行');
  }
  if (status.status === 'failed') {
    throw new Error(status.error_message || '选股运行失败');
  }
  return false;
}

async function waitForSelectionRunByPolling(runId, deadline) {
  while (Date.now() < deadline) {
    const status = await fetchJson(`/api/selection/runs/${encodeURIComponent(runId)}`);
    if (applySelectionRunStatus(status)) return status;
    await sleep(status.status === 'queued' ? 1000 : 2000);
  }
  throw new Error('选股运行超时，请稍后在历史 run_id 中查询结果');
}

function waitForSelectionRunByEvents(runId, deadline) {
  return new Promise((resolve, reject) => {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      reject(new Error('选股运行超时，请稍后在历史 run_id 中查询结果'));
      return;
    }

    const source = new EventSource(`/api/selection/runs/${encodeURIComponent(runId)}/events`);
    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(connectTimer);
      source.close();
      callback(value);
    };
    const fallBackToPolling = (message) => {
      const error = new Error(message || '选股状态流不可用');
      error.selectionSseFallback = true;
      finish(reject, error);
    };
    const connectTimer = setTimeout(
      () => fallBackToPolling('选股状态流连接超时'),
      Math.min(5000, remainingMs),
    );

    source.addEventListener('status', (event) => {
      clearTimeout(connectTimer);
      try {
        const status = JSON.parse(event.data);
        if (applySelectionRunStatus(status)) {
          finish(resolve, status);
        }
      } catch (error) {
        if (error instanceof SyntaxError) {
          fallBackToPolling('选股状态流数据异常');
        } else {
          finish(reject, error);
        }
      }
    });
    source.addEventListener('timeout', () => fallBackToPolling('选股状态流已超时'));
    source.onerror = () => fallBackToPolling('选股状态流暂不可用');
  });
}

async function waitForSelectionRun(runId) {
  const deadline = Date.now() + 10 * 60 * 1000;
  if (typeof EventSource !== 'undefined') {
    try {
      return await waitForSelectionRunByEvents(runId, deadline);
    } catch (error) {
      if (!error.selectionSseFallback) throw error;
    }
  }
  return waitForSelectionRunByPolling(runId, deadline);
}

async function fetchSelectionRunDetail(runId, allowMissing = false) {
  const response = await fetch(`/api/selection/runs/${encodeURIComponent(runId)}`);
  if (allowMissing && response.status === 404) return null;
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}${text ? `: ${text}` : ''}`);
  }
  return response.json();
}

async function resolveSelectionRunResult(completed) {
  if (completed?.result) return completed.result;
  if (!completed?.run_id) return null;
  const detailed = await fetchSelectionRunDetail(completed.run_id);
  return detailed?.result || null;
}

function setSelectionInputsDisabled(disabled) {
  qsa('#selection-form input, #selection-form select, #selection-form button, #refresh-results, #refresh-selection-page').forEach((element) => {
    element.disabled = disabled;
  });
}

function startSelectionRunFeedback(button) {
  selectionRunStartedAt = Date.now();
  selectionRunBackendStatus = null;
  clearInterval(selectionRunTimer);
  document.body.classList.add('selection-is-running');
  qs('.selection-run-panel')?.classList.add('is-running');
  qs('.selection-run-status-card')?.classList.add('is-running');
  if (button) {
    button.classList.add('is-running');
    button.setAttribute('aria-busy', 'true');
    const label = button.querySelector('.run-btn-label');
    if (label) label.textContent = '正在运行选股';
  }
  setSelectionInputsDisabled(true);
  const cancelButton = qs('#selection-cancel-run');
  if (cancelButton) {
    cancelButton.hidden = true;
    cancelButton.disabled = false;
    cancelButton.textContent = '取消任务';
  }
  const state = qs('#selection-run-state');
  if (state) {
    state.textContent = '运行中';
    state.classList.remove('up', 'down');
    state.classList.add('running');
  }
  qs('#selection-run-time').textContent = '刚刚开始';
  qs('#selection-qualified-count').textContent = '计算中';
  qs('#selection-avg-score').textContent = '-';
  qs('#selection-success-rate').textContent = '...';
  qs('#selection-score-sparkline').innerHTML = '<text x="16" y="44" fill="#38bdf8" font-size="12">正在计算评分分布</text>';
  updateSelectionRunProgress();
  selectionRunTimer = setInterval(updateSelectionRunProgress, 500);
}

function stopSelectionRunFeedback(button, status = 'success') {
  const elapsedMs = selectionRunStartedAt ? Date.now() - selectionRunStartedAt : 0;
  if (status === 'success') rememberSelectionRunDuration(elapsedMs);
  selectionRunStartedAt = null;
  selectionRunBackendStatus = null;
  clearInterval(selectionRunTimer);
  selectionRunTimer = null;
  document.body.classList.remove('selection-is-running');
  qs('.selection-run-panel')?.classList.remove('is-running');
  qs('.selection-run-status-card')?.classList.remove('is-running');
  setSelectionInputsDisabled(false);
  if (button) {
    button.classList.remove('is-running');
    button.removeAttribute('aria-busy');
    const label = button.querySelector('.run-btn-label');
    if (label) label.textContent = status === 'failed' ? '重新运行选股' : '开始运行选股';
  }
  const liveRun = qs('#selection-live-run');
  if (liveRun) liveRun.hidden = true;
}

async function cancelSelectionRun() {
  const runId = selectionRunBackendStatus?.run_id || (qs('#selection-run-id')?.value || '').trim();
  if (!runId) return;
  const button = qs('#selection-cancel-run');
  if (button) {
    button.disabled = true;
    button.textContent = '正在取消…';
  }
  try {
    const status = await fetchJson(`/api/selection/runs/${encodeURIComponent(runId)}/cancel`, { method: 'POST' });
    renderSelectionTaskStatus(status);
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.textContent = '重试取消';
    }
  }
}

function renderSelectionRunningPlaceholder() {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const cards = qs('#selection-result-cards');
  if (summaryLine) summaryLine.textContent = '选股运行中：已收到点击，正在读取全市场候选池和实时行情。';
  if (body) body.innerHTML = renderEmptyRow(14, '选股运行中，请稍等...');
  if (cards) {
    cards.classList.add('empty-state', 'selection-running-placeholder');
    cards.innerHTML = `
      <div class="selection-running-card">
        <span class="selection-running-ring" aria-hidden="true"></span>
        <div>
          <strong>正在运行选股</strong>
          <p>全市场候选池、实时行情、资金和舆情因子正在计算。</p>
        </div>
      </div>
    `;
  }
}

function renderSelectionRunError(error) {
  const message = error?.message || '运行失败';
  const cancelled = message.includes('任务已取消');
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const cards = qs('#selection-result-cards');
  const state = qs('#selection-run-state');
  if (state) {
    state.textContent = cancelled ? '已取消' : '运行失败';
    state.classList.remove('up', 'running');
    state.classList.add('down');
  }
  if (summaryLine) summaryLine.textContent = cancelled ? '本次选股任务已取消。' : `运行失败：${message}`;
  if (body) body.innerHTML = renderEmptyRow(14, cancelled ? '本次选股任务已取消' : `运行失败：${message}`);
  if (cards) {
    cards.classList.add('empty-state');
    cards.innerHTML = cancelled ? '本次选股任务已取消' : `运行失败：${escapeHtml(message)}`;
  }
}

function buildSelectionPersistKey(item) {
  return [
    item.selection_date || item.trade_date || '',
    item.strategy_id || '',
    item.code || '',
  ].join('::');
}

function compareSelectionItems(sortBy, a, b) {
  if (sortBy === 'score_desc') return (Number(b.score ?? -999) - Number(a.score ?? -999));
  if (sortBy === 'change_desc') return (Number(b.price_change_pct ?? -999) - Number(a.price_change_pct ?? -999));
  if (sortBy === 'change_asc') return (Number(a.price_change_pct ?? 999) - Number(b.price_change_pct ?? 999));
  if (sortBy === 'name_asc') return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN');
  return Number(a.rank_no ?? 9999) - Number(b.rank_no ?? 9999);
}

function fillIndustryOptions(items = []) {
  const select = qs('#selection-industry');
  if (!select) return;
  const current = select.value;
  const industries = [...new Set(items.map((item) => (item.industry || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  select.innerHTML = '<option value="">全部行业</option>' + industries.map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`).join('');
  if (industries.includes(current)) select.value = current;
}

function strategyBadgeClass(strategy) {
  if (strategy?.availability === 'runtime_ready') return 'status-ok';
  if (strategy?.availability === 'experimental' || strategy?.availability === 'research') return 'status-warn';
  return 'status-muted';
}

function renderStrategySummary(strategy) {
  const container = qs('#strategy-summary');
  if (!strategy) {
    container.innerHTML = '<div class="empty-state">暂无策略信息</div>';
    renderSelectionFactorBars(null);
    return;
  }

  const factors = strategy.factors || [];
  const historicalDiagnosticFailed = strategy.evidence_status === 'historical_diagnostic_fail';
  const helpText = [
    strategy.description || '暂无策略说明',
    `阈值：${strategy.score_threshold ?? '-'}，最多入选：${strategy.max_picks ?? '-'}`,
    `可用状态：${strategy.availability_label || '-'}`,
    strategy.evidence_note || '',
    `核心因子：${factors.map((item) => item.name || item.key || '-').join(' / ') || '暂无'}`,
  ].filter(Boolean).join('｜');

  container.innerHTML = `
    <div class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategy.display_name || strategy.id || '-')}</strong>
        <div>
          <span class="badge ${strategy.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${escapeHtml(strategy.mode || 'current')}</span>
          <span class="badge ${strategyBadgeClass(strategy)}">${escapeHtml(strategy.availability_label || '-')}</span>
          ${historicalDiagnosticFailed ? '<span class="badge status-error">历史诊断未通过</span>' : ''}
        </div>
      </div>
      <div class="muted">ID: ${escapeHtml(strategy.id || '-')} · 状态: ${escapeHtml(strategy.status || '-')} · 版本: ${escapeHtml(strategy.version || '-')}</div>
      <div>${escapeHtml(strategy.description || '')}</div>
      <div class="muted">当前运行阈值: ${strategy.score_threshold ?? '-'} 分 · 最大入选: ${strategy.max_picks ?? '-'}</div>
      <div class="muted">核心因子: ${factors.map((item) => escapeHtml(item.name || item.key || '-')).join(' / ') || '暂无'}</div>
      ${strategy.evidence_note ? `<div class="${historicalDiagnosticFailed ? 'strategy-warning-note' : 'muted'}">${escapeHtml(strategy.evidence_note)}</div>` : ''}
      <div class="muted">${escapeHtml(strategy.availability_note || '暂无状态说明')} · 完整因子分析请前往 <a href="/strategies">选股策略</a> · <button class="icon-help" type="button" data-tooltip="${escapeHtml(helpText)}">ⓘ</button></div>
    </div>
  `;
  renderSelectionFactorBars(strategy);
}

function renderSelectionFactorBars(strategy) {
  const container = qs('#selection-factor-bars');
  if (!container) return;
  const factors = strategy?.factors || [];
  if (!factors.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无因子配置';
    return;
  }
  container.classList.remove('empty-state');
  const weightFallback = Math.round(100 / Math.max(factors.length, 1));
  container.innerHTML = factors.slice(0, 6).map((factor, index) => {
    const rawWeight = factor.weight ?? factor.weight_pct ?? factor.ratio ?? null;
    const weight = rawWeight == null ? weightFallback : Number(rawWeight) <= 1 ? Number(rawWeight) * 100 : Number(rawWeight);
    const safeWeight = Math.max(6, Math.min(100, Number.isNaN(weight) ? weightFallback : weight));
    const label = factor.name || factor.label || factor.key || `因子${index + 1}`;
    return `
      <div class="selection-factor-bar">
        <div><span>${escapeHtml(label)}</span><b>${formatNumber(safeWeight, 0)}%</b></div>
        <i style="width:${safeWeight}%"></i>
      </div>
    `;
  }).join('');
}

function renderSelectionRunStatus(data = null, visibleItems = []) {
  const state = qs('#selection-run-state');
  if (!state) return;
  const summary = data?.summary || {};
  const originalCount = Number(summary.total_count ?? data?.items?.length ?? 0);
  const qualifiedCount = visibleItems.length;
  const successRate = originalCount ? (qualifiedCount / originalCount) * 100 : null;
  const scores = visibleItems.map((item) => Number(item.score)).filter((value) => !Number.isNaN(value));
  const avgScore = scores.length ? scores.reduce((sum, value) => sum + value, 0) / scores.length : null;
  qs('#selection-success-rate').textContent = successRate == null ? '-' : `${formatNumber(successRate, 0)}%`;
  const isNoHistory = data?.status === 'no_history';
  state.textContent = data ? (isNoHistory ? '暂无历史' : (qualifiedCount ? '运行成功' : '无达标股')) : '待运行';
  state.classList.remove('up', 'down', 'running');
  if (data && qualifiedCount) state.classList.add('up');
  if (data && !qualifiedCount && !isNoHistory) state.classList.add('down');
  qs('#selection-run-time').textContent = summary.run_created_at || summary.updated_at || summary.latest_trade_date || '-';
  qs('#selection-qualified-count').textContent = data ? `${qualifiedCount} / ${originalCount || 0}` : '-';
  qs('#selection-avg-score').textContent = formatNumber(avgScore, 1);
  renderSelectionScoreSparkline(scores);
}

function renderSelectionScoreSparkline(scores = []) {
  const svg = qs('#selection-score-sparkline');
  if (!svg) return;
  if (!scores.length) {
    svg.innerHTML = '<text x="16" y="44" fill="#64748b" font-size="12">暂无评分分布</text>';
    return;
  }
  const width = 320;
  const height = 88;
  const padding = { left: 14, right: 14, top: 12, bottom: 16 };
  const values = scores.slice(0, 12);
  const max = Math.max(100, ...values);
  const min = Math.min(0, ...values);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const step = plotWidth / Math.max(values.length - 1, 1);
  const points = values.map((value, index) => {
    const x = padding.left + index * step;
    const y = padding.top + ((max - value) / (max - min || 1)) * plotHeight;
    return `${x},${y}`;
  }).join(' ');
  svg.innerHTML = `
    <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" stroke="rgba(148,163,184,.22)" />
    <polyline points="${points}" fill="none" stroke="#38bdf8" stroke-width="2.5" />
    ${values.map((value, index) => {
      const x = padding.left + index * step;
      const y = padding.top + ((max - value) / (max - min || 1)) * plotHeight;
      return `<circle cx="${x}" cy="${y}" r="3" fill="${value >= 80 ? '#fb7185' : '#38bdf8'}" />`;
    }).join('')}
  `;
}

function syncInstrumentSegments() {
  const select = qs('#instrument-type');
  qsa('[data-instrument-value]').forEach((button) => {
    button.classList.toggle('active', button.dataset.instrumentValue === select.value);
  });
  const boardRow = qs('.board-row');
  if (boardRow) boardRow.hidden = select.value !== 'stock';
}

function syncMarketBoardSegments() {
  const select = qs('#market-board');
  qsa('[data-board-value]').forEach((button) => {
    button.classList.toggle('active', button.dataset.boardValue === select.value);
  });
}

function getMarketBoardValue() {
  const instrumentType = qs('#instrument-type')?.value || 'stock';
  return instrumentType === 'stock' ? (qs('#market-board')?.value || 'all') : 'all';
}

function marketBoardLabel(value) {
  return {
    all: '全市场',
    main: '主板',
    chinext: '创业板',
    star: '科创板',
    bse: '北交所',
  }[value || 'all'] || '全市场';
}

function syncScoreInputs(source) {
  const numberInput = qs('#selection-min-score');
  const rangeInput = qs('#selection-min-score-range');
  if (!numberInput || !rangeInput) return;
  const value = Math.max(0, Math.min(100, Number(source.value || 0)));
  numberInput.value = String(value);
  rangeInput.value = String(value);
}

function formatLocalDateTimeValue(date = new Date()) {
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function syncRunTimeDisplay() {
  const input = qs('#selection-run-time-input');
  const shell = qs('#selection-run-time-shell');
  const target = qs('#selection-run-time-display');
  if (!input || !shell || !target) return;
  const hasValue = Boolean(input.value);
  shell.classList.toggle('has-value', hasValue);
  target.textContent = hasValue ? input.value.replace('T', ' ').replaceAll('-', '/') : '自动使用最新交易日';
}

function updateRunTimeDisplay() {
  const input = qs('#selection-run-time-input');
  if (!input) return;
  input.value = formatLocalDateTimeValue(new Date());
  syncRunTimeDisplay();
}

function renderSelectionPlaceholder(message = '请先设置条件并点击“运行”，再查看本次选股结果') {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const cards = qs('#selection-result-cards');
  body.innerHTML = renderEmptyRow(14, message);
  summaryLine.textContent = message;
  if (cards) {
    cards.classList.add('empty-state');
    cards.innerHTML = message;
  }
  renderSelectionRunStatus(null, []);
}

const FACTOR_LABELS = {
  turnover: '换手',
  lowvol: '低波',
  reversal: '反转',
  trend: '趋势',
  momentum: '动量',
  quality: '质量',
  sentiment: '情绪',
  value: '估值',
  sector_heat: '主题热度',
  source_credibility: '信源可信度',
  info_importance: '信息重要度',
  amplification: '传播热度',
  stock_match: '个股匹配',
  stock_recognition: '板块辨识度',
  market_theme: '主线层级',
  fund_flow: '资金确认',
  daily_trend: '日线趋势',
  chip_structure: '筹码结构',
  price_confirm: '价格确认',
  volume_confirm: '成交确认',
  intraday_confirm: '分时确认',
  market_context: '市场环境',
  divergence: '分歧强度',
  reversal_strength: '反包强度',
  recognition: '辨识度',
  turnover_heat: '换手热度',
  sentiment_heat: '情绪热度',
  risk_control: '风险控制',
};

function formatFactorSummary(factors = {}, maxItems = 5) {
  const entries = Object.entries(factors || {}).filter(([, value]) => value !== null && value !== undefined && value !== '');
  if (!entries.length) return '-';
  return entries.slice(0, maxItems).map(([key, value]) => `${FACTOR_LABELS[key] || key} ${formatNumber(value, 2)}`).join(' / ');
}

function normalizeSentimentContext(item = {}) {
  const existing = item.sentiment_context || null;
  const metrics = {
    ...(item.factor_scores || {}),
    ...(item.explain?.raw_metrics || {}),
    ...(item.strategy_raw_metrics || {}),
  };
  const context = existing || {
    sector_name: metrics.opinion_sector_name,
    sector_type: metrics.opinion_sector_type,
    as_of: metrics.opinion_as_of_datetime,
    trade_date: metrics.opinion_trade_date,
    opinion_match_reason: metrics.opinion_match_reason,
    top_news: metrics.opinion_top_news || [],
    sources: metrics.opinion_sources || [],
    news_count: metrics.opinion_news_count,
    source_count: metrics.opinion_source_count,
    positive: metrics.opinion_positive_news_count,
    negative: metrics.opinion_negative_news_count,
    sector_score: metrics.opinion_sector_score,
    weighted_impact_score: metrics.opinion_weighted_impact_score,
    sentiment_mode: metrics.sentiment_mode,
    source_credibility_level: metrics.source_credibility_level,
    source_credibility_score: metrics.source_credibility_score,
    source_credibility_reason: metrics.source_credibility_reason,
    trade_signal_state: metrics.trade_signal_state,
    trade_signal_label: metrics.trade_signal_label,
    trade_signal_reason: metrics.trade_signal_reason,
    trade_grade_state: metrics.trade_grade_state,
    trade_grade_label: metrics.trade_grade_label,
    trade_grade_reason: metrics.trade_grade_reason,
    theme_trade_slot_state: metrics.theme_trade_slot_state,
    daily_trend_state: metrics.daily_trend_state,
    daily_trend_label: metrics.daily_trend_label,
    daily_trend_score: metrics.daily_trend,
    chip_structure_state: metrics.chip_structure_state,
    chip_structure_label: metrics.chip_structure_label,
    chip_structure_score: metrics.chip_structure,
    market_context_score: metrics.market_context,
    market_context_label: metrics.market_context_label,
    market_context_reason: metrics.market_context_reason,
    market_index_trend_score: metrics.market_index_trend_score,
    market_index_day_score: metrics.market_index_day_score,
    market_index_pct_chg: metrics.market_index_pct_chg,
    market_breadth_score: metrics.market_breadth_score,
    market_volume_score: metrics.market_volume_score,
    market_index_count: metrics.market_index_count,
    market_index_codes: metrics.market_index_codes,
    csi300_pct_chg: metrics.csi300_pct_chg,
    csi500_pct_chg: metrics.csi500_pct_chg,
    csi1000_pct_chg: metrics.csi1000_pct_chg,
    market_theme_tier: metrics.market_theme_tier,
    market_theme_label: metrics.market_theme_label,
    market_theme_trend_score: metrics.market_theme_trend_score,
    market_theme_rank: metrics.market_theme_rank,
    market_theme_score_delta: metrics.market_theme_score_delta,
    market_theme_reason: metrics.market_theme_reason,
    market_theme_fund_flow: metrics.market_theme_fund_flow,
    stock_rank: metrics.opinion_stock_rank,
    stock_pool_size: metrics.opinion_stock_pool_size,
    stock_recognition_score: metrics.opinion_stock_recognition_score,
    stock_recognition_label: metrics.opinion_stock_recognition_label,
    stock_recognition_reason: metrics.opinion_stock_recognition_reason,
    deepseek: metrics.deepseek || null,
  };
  return context?.sector_name || context?.sentiment_mode === 'market_opinion_v2' ? context : null;
}

function formatNewsTitle(news) {
  if (!news) return '';
  if (typeof news === 'string') return news;
  return news.title || news.summary || news.url || '';
}

function formatSourceName(source) {
  if (!source) return '';
  if (typeof source === 'string') return source;
  return source.source_name || source.name || source.source_id || source.id || '';
}

function renderSentimentContextInline(context) {
  if (!context) return '';
  const gradeLabel = context.trade_grade_label ? ` · ${context.trade_grade_label}` : '';
  const tradeLabel = context.trade_signal_label ? ` · ${context.trade_signal_label}` : '';
  const dailyLabel = context.daily_trend_label ? ` · ${context.daily_trend_label}` : '';
  const chipLabel = context.chip_structure_label ? ` · ${context.chip_structure_label}` : '';
  const themeLabel = context.market_theme_label ? ` · ${context.market_theme_label}` : '';
  const recognition = context.stock_recognition_label
    ? ` · ${context.stock_recognition_label}${context.stock_rank ? `#${context.stock_rank}` : ''}`
    : '';
  const counts = [
    context.news_count != null ? `新闻 ${context.news_count}` : '',
    context.source_count != null ? `来源 ${context.source_count}` : '',
    context.positive != null ? `正 ${context.positive}` : '',
    context.negative != null ? `负 ${context.negative}` : '',
  ].filter(Boolean).join(' · ');
  return `舆情主题：${context.sector_name || '-'}${context.sector_type ? `（${context.sector_type}）` : ''}${themeLabel}${recognition}${gradeLabel}${tradeLabel}${dailyLabel}${chipLabel}${context.as_of ? ` · ${context.as_of}` : ''}${counts ? ` · ${counts}` : ''}`;
}

function renderSentimentContextBlock(context) {
  if (!context) return '';
  const sources = (context.sources || []).map(formatSourceName).filter(Boolean).slice(0, 6).join(' / ') || '-';
  const news = (context.top_news || []).map(formatNewsTitle).filter(Boolean).slice(0, 3);
  return `
    <div class="muted">${escapeHtml(renderSentimentContextInline(context))}</div>
    <div class="muted">匹配说明：${escapeHtml(context.opinion_match_reason || '-')}</div>
    ${context.market_theme_label ? `<div class="muted">主线层级：${escapeHtml(context.market_theme_label)} · 趋势分 ${formatNumber(context.market_theme_trend_score, 1)} · ${escapeHtml(context.market_theme_reason || '-')}</div>` : ''}
    ${context.stock_recognition_label ? `<div class="muted">板块辨识度：${escapeHtml(context.stock_recognition_label)} · 分数 ${formatNumber(context.stock_recognition_score, 1)} · ${escapeHtml(context.stock_recognition_reason || '-')}</div>` : ''}
    ${context.trade_grade_label ? `<div class="muted">最终分级：${escapeHtml(context.trade_grade_label)} · ${escapeHtml(context.trade_grade_reason || '-')}</div>` : ''}
    ${context.trade_signal_label ? `<div class="muted">交易状态：${escapeHtml(context.trade_signal_label)} · ${escapeHtml(context.trade_signal_reason || '-')}</div>` : ''}
    ${context.daily_trend_label ? `<div class="muted">日线趋势：${escapeHtml(context.daily_trend_label)} · 分数 ${formatNumber(context.daily_trend_score, 1)}</div>` : ''}
    ${context.chip_structure_label ? `<div class="muted">筹码结构：${escapeHtml(context.chip_structure_label)} · 分数 ${formatNumber(context.chip_structure_score, 1)}</div>` : ''}
    ${context.market_context_label ? `<div class="muted">大盘环境：${escapeHtml(context.market_context_label)} · 分数 ${formatNumber(context.market_context_score, 1)} · ${escapeHtml(context.market_context_reason || '-')}</div>` : ''}
    ${context.market_index_count ? `<div class="muted">宽基指数：沪深300 ${formatPercent(context.csi300_pct_chg)} · 中证500 ${formatPercent(context.csi500_pct_chg)} · 中证1000 ${formatPercent(context.csi1000_pct_chg)}</div>` : ''}
    ${context.source_credibility_level ? `<div class="muted">信源评级：${escapeHtml(context.source_credibility_level)} · ${formatNumber(context.source_credibility_score, 2)} · ${escapeHtml(context.source_credibility_reason || '-')}</div>` : ''}
    ${context.deepseek?.summary ? `<div class="muted">DeepSeek：${escapeHtml(context.deepseek.summary)}${context.deepseek.confidence != null ? ` · 置信度 ${formatNumber(context.deepseek.confidence, 2)}` : ''}</div>` : ''}
    <div class="muted">新闻来源：${escapeHtml(sources)}</div>
    <div class="muted">Top新闻：${escapeHtml(news.join('；') || '-')}</div>
  `;
}

function formatTradePlanInline(plan = null, status = null) {
  if (!plan) return '';
  const entryZone = plan.entry_zone || {};
  const stopLoss = plan.stop_loss || {};
  const takeProfit = Array.isArray(plan.take_profit) ? plan.take_profit : [];
  const firstTakeProfit = takeProfit[0] || {};
  const riskReward = plan.risk_control?.take_profit_1_risk_reward_at_entry_high ?? plan.risk_control?.take_profit_1_risk_reward;
  const riskRewardText = riskReward != null ? ` · 盈亏比 ${formatNumber(riskReward, 2)}` : '';
  const statusText = status?.status_label ? ` · ${status.status_label}` : '';
  return `买入 ${formatNumber(entryZone.low ?? plan.entry_price, 3)}-${formatNumber(entryZone.high ?? plan.entry_price, 3)} · 止盈 ${formatNumber(firstTakeProfit.price, 3)} · 止损 ${formatNumber(stopLoss.price, 3)}${riskRewardText}${statusText}`;
}

function renderTradePlanBlock(plan = null, status = null) {
  const text = formatTradePlanInline(plan, status);
  if (!text) return '';
  const reasons = Array.isArray(plan.reasons) ? plan.reasons.slice(0, 2).join('；') : '';
  return `
    <div class="trade-plan-block">
      <div>${escapeHtml(text)}</div>
      ${reasons ? `<div class="muted">${escapeHtml(reasons)}</div>` : ''}
    </div>
  `;
}

function renderSelectionResultCards(items = [], emptyText = '暂无达标标的') {
  const container = qs('#selection-result-cards');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.innerHTML = escapeHtml(emptyText);
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.slice(0, 6).map((item) => {
    const factorSummary = formatFactorSummary(item.factors || item.factor_scores || {}, 4);
    const reasons = (item.reason_summary || []).slice(0, 2).join('；') || '暂无原因摘要';
    const risks = (item.risk_summary || []).slice(0, 1).join('；') || '暂无明显风险提示';
    const sentimentContext = normalizeSentimentContext(item);
    const tradeLabel = sentimentContext?.trade_grade_label || item.trade_grade_label || sentimentContext?.trade_signal_label || item.factor_scores?.trade_signal_label || '';
    const tradeState = sentimentContext?.trade_grade_state || item.trade_grade_state || sentimentContext?.trade_signal_state || item.factor_scores?.trade_signal_state || '';
    const tradeBadgeClass = tradeState === 'tradable' ? 'status-ok' : tradeState === 'weak' ? 'status-error' : tradeState === 'watch' ? 'status-warn' : 'status-muted';
    const pctClass = getPctClass(item.price_change_pct) || '';
    const tradePlanText = formatTradePlanInline(item.trade_plan, item.trade_plan_status);
    return `
      <article class="selection-stock-card">
        <div class="selection-stock-head">
          <div>
            <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '-')}</a>
            <span>${escapeHtml(item.code || '-')}</span>
          </div>
          <em>#${escapeHtml(item.rank_no ?? '-')}</em>
        </div>
        <div class="selection-stock-score-row">
          <strong>${formatNumber(item.score, 2)}</strong>
          <span class="${pctClass}">${formatPercent(item.price_change_pct)}</span>
        </div>
        ${tradeLabel ? `<div><span class="badge ${tradeBadgeClass}">${escapeHtml(tradeLabel)}</span></div>` : ''}
        <div class="selection-stock-meta">
          <span>${escapeHtml(item.industry_display || item.industry || '暂无行业')}</span>
          <span>${escapeHtml(item.selection_date || '-')}</span>
        </div>
        <div class="selection-stock-prices">
          <span>入选 ${formatPrice(item.selected_price ?? item.selected_open_price ?? item.selected_close_price)}</span>
          <span>最新 ${formatPrice(item.current_price)}</span>
        </div>
        ${tradePlanText ? `<div class="selection-card-note">${escapeHtml(tradePlanText)}</div>` : ''}
        <div class="selection-factor-mini">${escapeHtml(factorSummary)}</div>
        ${sentimentContext ? `<div class="selection-card-note">${escapeHtml(renderSentimentContextInline(sentimentContext))}</div>` : ''}
        <div class="selection-card-note">${escapeHtml(reasons)}</div>
        <div class="selection-card-risk">${escapeHtml(risks)}</div>
      </article>
    `;
  }).join('');
}

function normalizeRunResponse(result) {
  const runDate = result.run_id?.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/) ? `${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[1]}-${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[2]}-${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[3]}` : null;
  const items = (result.results || []).map((item) => {
    const explain = item.explain || {};
    const factorScores = {
      ...(explain.raw_metrics || {}),
      ...(item.factors || {}),
      ...(explain.summary || {}),
    };
    const selectedPrice = item.realtime_price ?? factorScores.realtime_price ?? item.close ?? null;
    const selectedPriceType = item.realtime_price != null || factorScores.realtime_price != null ? 'realtime' : 'daily_close';
    const selectedPriceTradeDate = item.realtime_trade_date || factorScores.realtime_trade_date || item.trade_date || runDate || '';
    const selectedPriceQuoteTime = item.realtime_quote_time || factorScores.realtime_quote_time || null;
    return {
      run_id: item.run_id || result.run_id || null,
      rank_no: item.rank_no ?? null,
      code: item.code,
      name: item.name,
      selection_date: selectedPriceTradeDate || runDate || item.trade_date || '',
      strategy_id: item.strategy_id,
      score: item.score,
      strategy_display_name: item.strategy_display_name,
      strategy_version: item.strategy_version,
      industry: item.industry || null,
      industry_display: item.industry || '暂无行业',
      factor_scores: factorScores,
      selected_price: selectedPrice,
      selected_price_type: selectedPriceType,
      selected_price_source: selectedPriceType === 'realtime' ? 'stock_realtime_snapshot' : 'daily_kline',
      selected_price_trade_date: selectedPriceTradeDate,
      selected_price_quote_time: selectedPriceQuoteTime,
      selected_open_price: selectedPrice,
      selected_close_price: item.close ?? null,
      current_price: selectedPrice,
      latest_trade_date: item.trade_date || null,
      price_change_pct: 0,
      reason_summary: explain.reasons || item.candidate_reasons || [],
      risk_summary: explain.risks || item.candidate_risks || [],
      tracking_days: 0,
      review_status: 'preview',
      max_gain_pct: 0,
      max_drawdown_pct: 0,
      instrument_type: item.instrument_type || qs('#instrument-type')?.value || 'stock',
      explain: explain,
      factors: item.factors || {},
      candidate_reasons: item.candidate_reasons || explain.reasons || [],
      candidate_risks: item.candidate_risks || explain.risks || [],
      missing_fields: item.missing_fields || explain.missing_fields || [],
      open: item.open ?? null,
      close: item.close ?? null,
      realtime_price: item.realtime_price ?? factorScores.realtime_price ?? null,
      realtime_pct_chg: item.realtime_pct_chg ?? factorScores.realtime_pct_chg ?? null,
      realtime_trade_date: item.realtime_trade_date || factorScores.realtime_trade_date || null,
      realtime_quote_time: item.realtime_quote_time || factorScores.realtime_quote_time || null,
      realtime_pre_close: item.realtime_pre_close ?? factorScores.realtime_pre_close ?? null,
      realtime_open: item.realtime_open ?? factorScores.realtime_open ?? null,
      realtime_high: item.realtime_high ?? factorScores.realtime_high ?? null,
      realtime_low: item.realtime_low ?? factorScores.realtime_low ?? null,
      realtime_amount: item.realtime_amount ?? factorScores.realtime_amount ?? null,
      pe_tushare: item.pe_tushare ?? null,
      pe_status: item.pe_status || explain.raw_metrics?.pe_status || null,
      pe_status_label: item.pe_status_label || explain.raw_metrics?.pe_status_label || null,
      pe_valid: item.pe_valid ?? explain.raw_metrics?.pe_valid ?? null,
      pe_status_reason: item.pe_status_reason || explain.raw_metrics?.pe_status_reason || null,
      pb_tushare: item.pb_tushare ?? null,
      roe: item.roe ?? null,
      roa: item.roa ?? null,
      grossprofit_margin: item.grossprofit_margin ?? null,
      netprofit_margin: item.netprofit_margin ?? null,
      revenue_yoy: item.revenue_yoy ?? null,
      profit_yoy: item.profit_yoy ?? null,
      value_score: item.value_score ?? explain.summary?.value_score ?? null,
      quality_score: item.quality_score ?? explain.summary?.quality_score ?? null,
      stability_score: item.stability_score ?? explain.summary?.stability_score ?? null,
      data_quality_score: item.data_quality_score ?? explain.summary?.data_quality_score ?? null,
      completeness_score: item.completeness_score ?? explain.summary?.completeness_score ?? null,
      run_diagnostics: item.run_diagnostics || result.diagnostics || null,
      sentiment_context: normalizeSentimentContext(item),
      trade_signal_state: item.strategy_raw_metrics?.trade_signal_state || explain.raw_metrics?.trade_signal_state || null,
      trade_signal_label: item.strategy_raw_metrics?.trade_signal_label || explain.raw_metrics?.trade_signal_label || null,
      trade_signal_reason: item.strategy_raw_metrics?.trade_signal_reason || explain.raw_metrics?.trade_signal_reason || null,
      trade_grade_state: item.trade_grade_state || item.strategy_raw_metrics?.trade_grade_state || explain.raw_metrics?.trade_grade_state || null,
      trade_grade_label: item.trade_grade_label || item.strategy_raw_metrics?.trade_grade_label || explain.raw_metrics?.trade_grade_label || null,
      trade_grade_reason: item.trade_grade_reason || item.strategy_raw_metrics?.trade_grade_reason || explain.raw_metrics?.trade_grade_reason || null,
      theme_trade_slot_state: item.theme_trade_slot_state || item.strategy_raw_metrics?.theme_trade_slot_state || explain.raw_metrics?.theme_trade_slot_state || null,
      trade_plan: item.trade_plan || null,
      trade_plan_status: item.trade_plan_status || null,
    };
  });

  return {
    run_id: result.run_id || null,
    requested_strategy_id: result.strategy?.id || result.strategy_id || null,
    strategy: result.strategy || null,
    diagnostics: result.diagnostics || items[0]?.run_diagnostics || null,
    progressive_rerank: result.progressive_rerank || null,
    summary: {
      selected_trade_date: items[0]?.selection_date || null,
      run_created_at: null,
      latest_trade_date: items[0]?.latest_trade_date || null,
      total_count: items.length,
      sample_size: null,
      instrument_type: items[0]?.instrument_type || qs('#instrument-type')?.value || 'stock',
      updated_at: items[0]?.latest_trade_date || null,
      result_strategy_id: result.strategy?.id || result.strategy_id || null,
    },
    items,
  };
}

function renderSelectionResults(data) {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const topSummary = qs('#selection-top-summary');
  const minScore = Number(qs('#selection-min-score')?.value || 60);
  const searchText = (qs('#selection-search')?.value || '').trim().toLowerCase();
  const industryValue = (qs('#selection-industry')?.value || '').trim();
  const sortBy = qs('#selection-sort')?.value || 'rank_asc';
  const summary = data.summary || {};
  const originalItems = data.items || [];
  const isNoHistory = data.status === 'no_history';
  let items = originalItems.filter((item) => Number(item.score ?? 0) >= minScore);

  fillIndustryOptions(originalItems);

  if (industryValue) {
    items = items.filter((item) => (item.industry || '').trim() === industryValue);
  }

  if (searchText) {
    items = items.filter((item) => [item.code, item.name, item.industry, item.strategy_display_name, item.strategy_id].some((value) => String(value || '').toLowerCase().includes(searchText)));
  }

  items = [...items].sort((a, b) => compareSelectionItems(sortBy, a, b));

  summaryLine.textContent = isNoHistory
    ? (data.message || '当前条件下暂无真实历史选股结果。')
    : `run_id：${data.run_id || '最新'} · 选股交易日：${summary.selected_trade_date || '-'} · 入库时间：${summary.run_created_at || '-'} · 最新交易日：${summary.latest_trade_date || '-'} · 达标展示：${items.length} / 原始入选 ${summary.total_count || 0} 条`;
  const progressiveSummary = data.progressive_rerank
    ? ` · 舆情精排：板块 ${data.progressive_rerank.local_sector_count ?? 0}→${data.progressive_rerank.selected_sector_count ?? 0}，股票 ${data.progressive_rerank.preliminary_stock_count ?? 0}→${data.count ?? items.length}，Tavily 板块 ${data.progressive_rerank.sector_tavily?.tavily_runs ?? 0} / 个股 ${data.progressive_rerank.stock_tavily?.tavily_runs ?? 0}`
    : '';
  const boardSummary = data.diagnostics?.market_board_filter_summary;
  const boardText = boardSummary?.market_board_label || marketBoardLabel(getMarketBoardValue());
  topSummary.textContent = `样本池：${summary.sample_size || '-'} · 股票池：${boardText} · 原始入选上限：${data.strategy?.max_picks ?? '-'} · 数据更新时间：${summary.updated_at || '-'} · 当前策略：${data.strategy?.display_name || data.strategy?.id || '-'} · 策略版本：${data.strategy?.version || '-'} · 当前运行阈值：${data.strategy?.score_threshold ?? '-'} 分${progressiveSummary}`;

  renderSelectionRunStatus(data, items);

  if (!originalItems.length) {
    const emptyText = isNoHistory ? '暂无历史选股结果' : '本次运行未产生任何入选结果';
    renderSelectionResultCards([], emptyText);
    body.innerHTML = renderEmptyRow(14, emptyText);
    return;
  }

  if (!items.length) {
    renderSelectionResultCards([]);
    body.innerHTML = renderEmptyRow(14, '无达标股：当前入选结果中没有股票达到设定分数底线');
    return;
  }

  renderSelectionResultCards(items);

  body.innerHTML = items.map((item, index) => {
    const reasonsList = item.reason_summary || [];
    const risksList = item.risk_summary || [];
    const reasons = reasonsList.slice(0, 2).join('；') || '-';
    const risks = risksList.slice(0, 2).join('；') || '-';
    const detailId = `selection-detail-${index}`;
    const factorScores = item.factor_scores || {};
    const sentimentContext = normalizeSentimentContext(item);
    const tradeLabel = sentimentContext?.trade_grade_label || item.trade_grade_label || factorScores.trade_grade_label || sentimentContext?.trade_signal_label || factorScores.trade_signal_label || item.trade_signal_label || '';
    const tradeState = sentimentContext?.trade_grade_state || item.trade_grade_state || factorScores.trade_grade_state || sentimentContext?.trade_signal_state || factorScores.trade_signal_state || item.trade_signal_state || '';
    const tradeBadgeClass = tradeState === 'tradable' ? 'status-ok' : tradeState === 'weak' ? 'status-error' : tradeState === 'watch' ? 'status-warn' : 'status-muted';
    const saveKey = buildSelectionPersistKey(item);
    const isSaved = savedSelectionKeys.has(saveKey);
    const factorSummary = formatFactorSummary(item.factors || factorScores);
    const fundamentalMissingFields = Array.isArray(factorScores.fundamental_missing_fields) ? factorScores.fundamental_missing_fields : [];
    const fundamentalCompleteness = factorScores.fundamental_completeness == null ? null : Number(factorScores.fundamental_completeness) * 100;
    const fundamentalHint = fundamentalMissingFields.length
      ? `基本面完整度 ${formatNumber(fundamentalCompleteness, 0)}% · 缺失 ${fundamentalMissingFields.join(', ')}`
      : `基本面完整度 ${formatNumber(fundamentalCompleteness, 0)}% · 关键字段齐全`;
    const tradePlanText = formatTradePlanInline(item.trade_plan, item.trade_plan_status);
    const peStatusHint = item.pe_status_label
      ? `PE状态：${item.pe_status_label}${item.pe_status_reason ? `（${item.pe_status_reason}）` : ''}`
      : null;
    const detailText = [
      `策略：${item.strategy_display_name || item.strategy_id || '-'}`,
      `策略版本：${item.strategy_version || '-'}`,
      `最新交易日：${item.latest_trade_date || '-'}`,
      `跟踪状态：${item.review_status || '-'}`,
      `执行入选价：${formatPrice(item.selected_price ?? item.selected_open_price)}`,
      `入选价来源：${item.selected_price_type === 'realtime' ? '盘中实时价' : '日线收盘价'}${item.selected_price_quote_time ? `（${item.selected_price_quote_time}）` : ''}`,
      `日线收盘价：${formatPrice(item.selected_close_price)}`,
      `最新价：${formatPrice(item.current_price)}`,
      `区间涨跌幅：${item.price_change_pct ?? '-'}%`,
      `最大浮盈：${item.max_gain_pct ?? '-'}%`,
      `最大回撤：${item.max_drawdown_pct ?? '-'}%`,
      `因子得分：${factorSummary}`,
      renderSentimentContextInline(sentimentContext),
      `交易状态：${tradeLabel || '-'}${sentimentContext?.trade_signal_reason ? `，${sentimentContext.trade_signal_reason}` : ''}`,
      `分级说明：${sentimentContext?.trade_grade_reason || item.trade_grade_reason || '-'}`,
      `买卖计划：${tradePlanText || '-'}`,
      `基础打分：value=${factorScores.value_score ?? '-'}, quality=${factorScores.quality_score ?? '-'}, stability=${factorScores.stability_score ?? '-'}, data=${factorScores.data_quality_score ?? '-'}, completeness=${factorScores.completeness_score ?? '-'}`,
      peStatusHint,
      `基本面：${fundamentalHint}`,
      `详细原因：${reasonsList.join('；') || '-'}`,
      `详细风险：${risksList.join('；') || '-'}`,
    ].join('\n');
    return `
      <tr>
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>
          <div>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</div>
          <div class="muted">${escapeHtml(item.strategy_version || '-')}</div>
        </td>
        <td>${escapeHtml(item.industry_display || '暂无行业')}</td>
        <td>${escapeHtml(item.selection_date || '-')}</td>
        <td>${formatPrice(item.selected_price ?? item.selected_open_price ?? item.selected_close_price)}</td>
        <td>${formatPrice(item.current_price)}</td>
        <td class="${getPctClass(item.price_change_pct) || ''}">${formatPercent(item.price_change_pct)}</td>
        <td>
          <div>${formatNumber(item.score, 2)}</div>
          <div class="muted">${escapeHtml(factorSummary)}</div>
          ${tradeLabel ? `<div><span class="badge ${tradeBadgeClass}">${escapeHtml(tradeLabel)}</span></div>` : ''}
        </td>
        <td>
          <div>${item.rank_no ?? '-'}</div>
          <div class="muted">第 ${item.rank_no ?? '-'} 名</div>
        </td>
        <td>${escapeHtml(item.review_status || '-')}</td>
        <td>
          <div>${escapeHtml(reasons)}</div>
          <div class="muted">共 ${reasonsList.length} 条</div>
          ${tradePlanText ? `<div class="muted">${escapeHtml(tradePlanText)}</div>` : ''}
        </td>
        <td>
          <div>${escapeHtml(risks)}</div>
          <div class="muted">共 ${risksList.length} 条</div>
          ${peStatusHint ? `<div class="muted">${escapeHtml(peStatusHint)}</div>` : ''}
          <div class="muted">${escapeHtml(fundamentalHint)}</div>
        </td>
        <td>
          <button class="btn ${isSaved ? 'btn-secondary' : 'btn-primary'}" type="button" data-selection-save="${escapeHtml(saveKey)}" ${isSaved ? 'disabled' : ''}>${isSaved ? '已保存' : '保存'}</button>
        </td>
        <td><button class="btn btn-secondary" type="button" data-selection-detail="${detailId}" data-tooltip="${escapeHtml(detailText)}">查看</button></td>
      </tr>
      <tr id="${detailId}" class="selection-detail-row" hidden>
        <td colspan="14">
          <div class="muted">策略：${escapeHtml(item.strategy_display_name || item.strategy_id || '-')} · 版本：${escapeHtml(item.strategy_version || '-')} · 最新交易日：${escapeHtml(item.latest_trade_date || '-')} · 跟踪状态：${escapeHtml(item.review_status || '-')}</div>
          <div class="muted">行业：${escapeHtml(item.industry_display || '暂无行业')} · 排名：第 ${escapeHtml(String(item.rank_no ?? '-'))} 名 · 总分：${escapeHtml(String(formatNumber(item.score, 2)))}</div>
          <div class="muted">价格跟踪：最新价 ${formatPrice(item.current_price)} · 涨跌幅 <span class="${getPctClass(item.price_change_pct) || ''}">${formatPercent(item.price_change_pct)}</span> · 最大浮盈 <span class="up">${formatPercent(item.max_gain_pct)}</span> · 最大回撤 <span class="down">${formatPercent(item.max_drawdown_pct)}</span></div>
          ${renderTradePlanBlock(item.trade_plan, item.trade_plan_status)}
          <div class="muted">因子得分：${escapeHtml(factorSummary)}</div>
          ${peStatusHint ? `<div class="muted">${escapeHtml(peStatusHint)}</div>` : ''}
          ${renderSentimentContextBlock(sentimentContext)}
          <div class="score-chip-list">
            <span class="score-chip">value ${escapeHtml(String(factorScores.value_score ?? '-'))}</span>
            <span class="score-chip">quality ${escapeHtml(String(factorScores.quality_score ?? '-'))}</span>
            <span class="score-chip">stability ${escapeHtml(String(factorScores.stability_score ?? '-'))}</span>
            <span class="score-chip">data ${escapeHtml(String(factorScores.data_quality_score ?? '-'))}</span>
            <span class="score-chip">complete ${escapeHtml(String(factorScores.completeness_score ?? '-'))}</span>
          </div>
          <div class="muted">详细原因：${escapeHtml(reasonsList.join('；') || '-')}</div>
          <div class="muted">详细风险：${escapeHtml(risksList.join('；') || '-')}</div>
        </td>
      </tr>
    `;
  }).join('');

  body.querySelectorAll('[data-selection-detail]').forEach((button) => {
    button.addEventListener('click', () => {
      const detailRow = qs(`#${button.getAttribute('data-selection-detail')}`);
      if (detailRow) detailRow.hidden = !detailRow.hidden;
    });
  });

  body.querySelectorAll('[data-selection-save]').forEach((button, index) => {
    button.addEventListener('click', async () => {
      const item = items[index];
      if (!item) return;
      button.disabled = true;
      try {
        const response = await fetchJson('/api/selection/save-item', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            run_id: data.run_id || item.run_id,
            strategy_id: item.strategy_id || data.strategy?.id,
            score_threshold: data.strategy?.score_threshold ?? Number(qs('#selection-min-score')?.value || 60),
            item,
          }),
        });
        const persistedKey = buildSelectionPersistKey(item);
        savedSelectionKeys.add(persistedKey);
        savedSelectionKeys.add(button.getAttribute('data-selection-save'));
        button.textContent = '已保存';
        button.classList.remove('btn-primary');
        if (!button.classList.contains('btn-secondary')) {
          button.classList.add('btn-secondary');
        }
      } catch (error) {
        button.disabled = false;
        throw error;
      }
    });
  });
}

async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const select = qs('#strategy-id');
  const previousValue = select.value;
  select.innerHTML = '';

  const strategies = (data.strategies || []).filter((item) => item.runtime_ready === true);

  const placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.textContent = strategies.length
    ? '请选择策略（当前均未通过交易有效性验证）'
    : '当前没有可执行策略';
  placeholder.selected = true;
  select.appendChild(placeholder);

  strategies.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.id;
    const validationLabel = item.validated ? '已验证' : '未验证';
    option.textContent = `${item.display_name || item.id} (${item.id} · ${validationLabel})`;
    select.appendChild(option);
  });

  if (strategies.some((item) => item.id === previousValue)) {
    select.value = previousValue;
  }

  if (select.value) {
    await loadStrategyDetail(select.value);
  } else {
    renderStrategySummary(null);
  }
}

async function loadStrategyDetail(strategyId) {
  if (!strategyId) {
    renderStrategySummary(null);
    return;
  }
  const instrumentType = qs('#instrument-type')?.value || 'stock';
  const data = await fetchJson(`/api/strategies/detail?strategy_id=${encodeURIComponent(strategyId)}&instrument_type=${encodeURIComponent(instrumentType)}&sample_limit=200`);
  renderStrategySummary(data.strategy);
}

async function loadSelectionResults(runIdOverride = null) {
  const instrumentType = qs('#instrument-type').value || 'stock';
  const limit = Number(qs('#limit').value || 3);
  const runIdInput = (qs('#selection-run-id')?.value || '').trim();
  const strategyId = qs('#strategy-id')?.value || '';
  const runId = runIdOverride || runIdInput;

  if (!runId && !hasExecutedSelection) {
    lastSelectionResponse = null;
    renderSelectionPlaceholder();
    return;
  }

  if (runId) {
    const taskRun = await fetchSelectionRunDetail(runId, true);
    if (taskRun) {
      if (!applySelectionRunStatus(taskRun)) {
        const summaryLine = qs('#selection-summary-line');
        if (summaryLine) {
          summaryLine.textContent = `任务 ${runId} 当前状态：${selectionRunStatusLabel(taskRun.status)}，请稍后刷新结果。`;
        }
        return;
      }
      const taskResult = await resolveSelectionRunResult(taskRun);
      if (!taskResult) {
        throw new Error('选股任务已完成，但任务详情中没有结果');
      }
      const normalized = normalizeRunResponse(taskResult);
      lastSelectionResponse = normalized;
      (normalized.items || []).forEach((item) => {
        if (item.persisted_key) savedSelectionKeys.add(item.persisted_key);
      });
      renderSelectionResults(normalized);
      if (normalized.strategy) {
        renderStrategySummary(normalized.strategy);
      }
      return;
    }
  }

  const query = new URLSearchParams({ instrument_type: instrumentType, limit: String(limit) });
  const marketBoard = getMarketBoardValue();
  if (marketBoard !== 'all') query.set('market_board', marketBoard);
  if (runId) {
    query.set('run_id', runId);
  } else if (strategyId) {
    query.set('strategy_id', strategyId);
  }
  const data = await fetchJson(`/api/selection/results?${query.toString()}`);
  lastSelectionResponse = data;
  if (data.run_id && !runIdOverride) {
    qs('#selection-run-id').value = data.run_id;
  }
  if (!runId && data.requested_strategy_id && qs('#strategy-id')) {
    qs('#strategy-id').value = data.requested_strategy_id;
  }
  (data.items || []).forEach((item) => {
    if (item.persisted_key) savedSelectionKeys.add(item.persisted_key);
  });
  renderSelectionResults(data);
  if (data.strategy) {
    renderStrategySummary(data.strategy);
  }
}

async function runSelection(event) {
  event.preventDefault();
  const button = event.submitter || qs('#selection-form button[type="submit"]');
  const strategyId = (qs('#strategy-id')?.value || '').trim();
  if (!strategyId) {
    renderSelectionRunError(new Error('请先明确选择一套策略；当前系统不设置默认策略'));
    return;
  }
  let runSucceeded = false;
  let finalResult = null;
  startSelectionRunFeedback(button);
  renderSelectionRunningPlaceholder();

  try {
    const submitted = await fetchJson('/api/selection/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: strategyId,
        instrument_type: qs('#instrument-type').value,
        market_board: getMarketBoardValue(),
        limit: Number(qs('#limit').value || 3),
        score_threshold: Number(qs('#selection-min-score').value || 60),
        save: false,
        async_run: true,
      }),
    });
    hasExecutedSelection = true;
    if (submitted.run_id) {
      qs('#selection-run-id').value = submitted.run_id;
    }
    if (submitted.status && submitted.run_id && !submitted.results && !submitted.result) {
      renderSelectionTaskStatus(submitted);
      const completed = await waitForSelectionRun(submitted.run_id);
      finalResult = await resolveSelectionRunResult(completed);
    } else {
      finalResult = submitted.result || submitted;
    }
    if (!finalResult) {
      throw new Error('选股任务已完成，但没有返回结果，请用 run_id 查询状态');
    }
    const normalized = normalizeRunResponse(finalResult);
    lastSelectionResponse = normalized;
    (normalized.items || []).forEach((item) => {
      if (item.persisted_key) savedSelectionKeys.add(item.persisted_key);
    });
    renderSelectionResults(normalized);
    if (normalized.strategy) {
      renderStrategySummary(normalized.strategy);
    }
    runSucceeded = true;
  } catch (error) {
    renderSelectionRunError(error);
  } finally {
    stopSelectionRunFeedback(button, runSucceeded ? 'success' : 'failed');
    bindTooltips();
  }
}

async function refreshSelectionPage() {
  await loadStrategies();
  if (hasExecutedSelection || (qs('#selection-run-id')?.value || '').trim()) {
    await loadSelectionResults();
  } else {
    renderSelectionPlaceholder();
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#selection-form').addEventListener('submit', runSelection);
  qs('#selection-cancel-run')?.addEventListener('click', cancelSelectionRun);
  qs('#refresh-strategies').addEventListener('click', async () => {
    await loadStrategies();
  });
  qs('#refresh-results').addEventListener('click', () => loadSelectionResults());
  qs('#refresh-selection-page').addEventListener('click', refreshSelectionPage);
  qs('#selection-min-score').addEventListener('change', (event) => {
    syncScoreInputs(event.target);
    lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : renderSelectionPlaceholder();
  });
  qs('#selection-min-score-range')?.addEventListener('input', (event) => {
    syncScoreInputs(event.target);
    lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : renderSelectionPlaceholder();
  });
  qs('#selection-search').addEventListener('input', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-sort').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-industry').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-run-time-input')?.addEventListener('change', syncRunTimeDisplay);
  qs('#selection-filter-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    await loadSelectionResults();
  });
  qs('#selection-run-id').addEventListener('change', async () => {
    hasExecutedSelection = Boolean((qs('#selection-run-id')?.value || '').trim());
    await loadSelectionResults();
  });
  qsa('[data-instrument-value]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (button.disabled) return;
      qs('#instrument-type').value = button.dataset.instrumentValue;
      syncInstrumentSegments();
      syncMarketBoardSegments();
      hasExecutedSelection = false;
      lastSelectionResponse = null;
      await loadStrategyDetail(qs('#strategy-id')?.value || '');
      renderSelectionPlaceholder();
    });
  });
  qs('#instrument-type').addEventListener('change', () => syncInstrumentSegments());
  qsa('[data-board-value]').forEach((button) => {
    button.addEventListener('click', async () => {
      qs('#market-board').value = button.dataset.boardValue;
      syncMarketBoardSegments();
      hasExecutedSelection = false;
      lastSelectionResponse = null;
      renderSelectionPlaceholder();
    });
  });
  qs('#market-board')?.addEventListener('change', () => syncMarketBoardSegments());
  qs('#strategy-id').addEventListener('change', async (event) => {
    hasExecutedSelection = false;
    lastSelectionResponse = null;
    qs('#selection-run-id').value = '';
    await loadStrategyDetail(event.target.value);
    renderSelectionPlaceholder();
  });

  try {
    updateRunTimeDisplay();
    syncScoreInputs(qs('#selection-min-score'));
    syncInstrumentSegments();
    syncMarketBoardSegments();
    await loadStrategies();
    renderSelectionPlaceholder();
    bindTooltips();
  } catch (error) {
    qs('#selection-summary-line').textContent = `页面初始化失败: ${error.message}`;
  }
});
