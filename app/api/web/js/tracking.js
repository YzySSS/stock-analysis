function updateTrackingStats(summary = {}, items = []) {
  qs('#tracking-stat-count').textContent = String(summary.count ?? items.length ?? 0);
  qs('#tracking-stat-active-count').textContent = String(summary.tracking_count ?? 0);
  qs('#tracking-stat-avg').textContent = formatPercent(summary.avg_return_pct);
  qs('#tracking-stat-win-rate').textContent = formatPercent(summary.win_rate_pct);
  qs('#tracking-stat-excess-return').textContent = formatPercent(summary.excess_return_pct);
  qs('#tracking-stat-excess-return').classList.remove('up', 'down');
  const excessReturnClass = getPctClass(summary.excess_return_pct);
  if (excessReturnClass) {
    qs('#tracking-stat-excess-return').classList.add(excessReturnClass);
  }
  qs('#tracking-stat-max-gain').textContent = formatPercent(summary.max_gain_pct);
  qs('#tracking-stat-max-drawdown').textContent = formatPercent(summary.max_drawdown_pct);
}

function renderReviewSummary(summary = {}, items = []) {
  const container = qs('#tracking-review-summary');
  const best = summary.best_item;
  const worst = summary.worst_item;
  const strategyName = items[0]?.strategy_display_name || items[0]?.strategy_id || '最新复盘快照';
  const selectionDate = items[0]?.selection_date || '-';
  container.innerHTML = `
    <article class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategyName)}</strong>
        <span class="badge status-ok">${summary.count ?? 0} 条</span>
      </div>
      <div class="muted">选股日期：${escapeHtml(selectionDate)} · 策略：${escapeHtml(items[0]?.strategy_display_name || items[0]?.strategy_id || '最新复盘快照')} · 仍在跟踪：${summary.tracking_count ?? 0} 条</div>
      <div class="muted">平均收益：${formatPercent(summary.avg_return_pct)} · 胜率：${formatPercent(summary.win_rate_pct)} · 超额收益：${formatPercent(summary.excess_return_pct)}</div>
      <div class="muted">最大浮盈：${formatPercent(summary.max_gain_pct)} · 最大回撤：${formatPercent(summary.max_drawdown_pct)}</div>
      <div class="muted">表现最好：${best ? `${escapeHtml(best.name || best.code || '-')} (${formatPercent(best.price_change_pct)})` : '暂无'}</div>
      <div class="muted">表现最弱：${worst ? `${escapeHtml(worst.name || worst.code || '-')} (${formatPercent(worst.price_change_pct)})` : '暂无'}</div>
    </article>
  `;
}

function renderReviewNotes(summary = {}, items = []) {
  const container = qs('#tracking-review-notes');
  const positive = items.filter((item) => (item.price_change_pct ?? -999) >= 0).length;
  const negative = items.filter((item) => (item.price_change_pct ?? 999) < 0).length;
  const flat = Math.max((summary.count ?? items.length ?? 0) - positive - negative, 0);
  const best = summary.best_item;
  const worst = summary.worst_item;
  container.innerHTML = `
    <div>当前复盘判断：正收益 ${positive} 只，负收益 ${negative} 只，持平 ${flat} 只，胜率 ${formatPercent(summary.win_rate_pct)}，超额收益 ${formatPercent(summary.excess_return_pct)}。</div>
    <div class="muted">当前成功特征：${best ? `${escapeHtml(best.name || best.code || '-')} 领跑，说明本轮至少有部分标的延续了正向表现。` : '暂无足够样本。'}</div>
    <div class="muted">当前失败特征：${worst ? `${escapeHtml(worst.name || worst.code || '-')} 偏弱，需结合回撤和基本面缺口继续复盘。` : '暂无明显失败样本。'}</div>
  `;
}

let lastTrackingState = {
  strategyId: 'lowvol_reversal',
  limit: 200,
  instrumentType: 'stock',
  runId: '',
  selectionDate: '',
};

function renderTrackingTable(items, summary = {}) {
  const body = qs('#tracking-results-body');
  if (!items.length) {
    body.innerHTML = renderEmptyRow(12, '暂无跟踪数据');
    return;
  }

  body.innerHTML = items.map((item) => {
    const pct = item.price_change_pct;
    const excessReturn = pct != null && summary.benchmark_return_pct != null
      ? Number(pct) - Number(summary.benchmark_return_pct)
      : null;
    const reviewNote = pct == null
      ? '缺少收益数据'
      : pct >= 0
        ? '已验证正收益'
        : '需重点复盘回撤';
    return `
      <tr data-tracking-key="${escapeHtml(`${item.code || ''}__${item.selection_date || ''}__${item.strategy_id || ''}`)}">
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>${escapeHtml(item.selection_date || '')}</td>
        <td>${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(pct)}">${formatPercent(pct)}</td>
        <td class="${getPctClass(excessReturn)}">${formatPercent(excessReturn)}</td>
        <td>${item.tracking_days ?? '-'}</td>
        <td class="up">${formatPercent(item.max_gain_pct)}</td>
        <td class="down">${formatPercent(item.max_drawdown_pct)}</td>
        <td>${escapeHtml(item.review_status || '-')}</td>
        <td>${escapeHtml(reviewNote)}</td>
        <td><button class="btn btn-danger btn-sm" type="button" data-action="delete-tracking-item" data-code="${escapeHtml(item.code || '')}" data-selection-date="${escapeHtml(item.selection_date || '')}" data-strategy-id="${escapeHtml(item.strategy_id || '')}">删除</button></td>
      </tr>
    `;
  }).join('');
}

function renderSelectionDateOptions(dates = [], selectedValue = '') {
  const select = qs('#tracking-selection-date');
  if (!select) return;
  select.innerHTML = '';
  const latestOption = document.createElement('option');
  latestOption.value = '';
  latestOption.textContent = '最新日期';
  select.appendChild(latestOption);

  dates.forEach((value) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = value;
    if (value === selectedValue) option.selected = true;
    select.appendChild(option);
  });

  select.value = selectedValue || '';
}

function renderRunOptions(runs = [], selectedRunId = '', selectedDate = '') {
  const select = qs('#tracking-run-id');
  if (!select) return;
  select.innerHTML = '';
  const latestOption = document.createElement('option');
  latestOption.value = '';
  latestOption.textContent = '自动按日期/最新匹配';
  select.appendChild(latestOption);

  const filteredRuns = selectedDate
    ? runs.filter((item) => String(item.trade_date || '') === String(selectedDate))
    : runs;

  filteredRuns.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.run_id || '';
    const tradeDate = item.trade_date || '-';
    const count = item.item_count ?? '-';
    option.textContent = `${tradeDate} · ${item.run_id} · ${count} 条`;
    if (item.run_id === selectedRunId) option.selected = true;
    select.appendChild(option);
  });

  select.value = selectedRunId || '';
}

async function loadTrackingFilters(strategyId, instrumentType = 'stock') {
  const query = new URLSearchParams({ instrument_type: instrumentType });
  if (strategyId) query.set('strategy_id', strategyId);
  return fetchJson(`/api/tracking/filters?${query.toString()}`);
}

async function loadTrackingData({ strategyId = 'lowvol_reversal', limit = 200, instrumentType = 'stock', runId = '', selectionDate = '' } = {}) {
  const summaryText = qs('#tracking-summary-text');
  summaryText.textContent = '加载中...';

  const filters = await loadTrackingFilters(strategyId, instrumentType);
  const selectionDates = filters.selection_dates || [];
  const availableRuns = filters.available_runs || [];
  renderSelectionDateOptions(selectionDates, selectionDate);
  renderRunOptions(availableRuns, runId, selectionDate);

  const query = new URLSearchParams({ limit: String(limit), instrument_type: instrumentType });
  if (strategyId) query.set('strategy_id', strategyId);
  if (selectionDate) query.set('selection_date', selectionDate);
  if (runId) query.set('run_id', runId);

  const url = runId
    ? `/api/tracking?${query.toString()}`
    : selectionDate
      ? `/api/tracking?${query.toString()}`
      : `/api/tracking/latest?${query.toString()}`;

  const data = await fetchJson(url);
  const items = data.items || [];
  const summary = data.summary || {};
  lastTrackingState = { strategyId, limit, instrumentType, runId, selectionDate };
  renderTrackingTable(items, summary);
  updateTrackingStats(summary, items);
  renderReviewSummary(summary, items);
  renderReviewNotes(summary, items);
  const modeText = runId
    ? `当前显示指定批次 ${runId}`
    : selectionDate
      ? `当前显示 ${selectionDate} 的复盘结果`
      : '当前显示最新复盘快照';
  summaryText.textContent = `${modeText}，共 ${items.length} 条`;
}

async function deleteTrackingItem(button) {
  const code = button?.dataset?.code || '';
  const selectionDate = button?.dataset?.selectionDate || '';
  const strategyId = button?.dataset?.strategyId || '';
  const instrumentType = lastTrackingState.instrumentType || 'stock';
  if (!code || !selectionDate || !strategyId) {
    qs('#tracking-summary-text').textContent = '删除失败：当前行缺少 code / selection_date / strategy_id';
    return;
  }

  if (!window.confirm(`确认删除 ${code} 在 ${selectionDate}（策略：${strategyId}）这条复盘记录吗？`)) {
    return;
  }

  button.disabled = true;
  try {
    const query = new URLSearchParams({ code, selection_date: selectionDate, strategy_id: strategyId, instrument_type: instrumentType });
    await fetchJson(`/api/tracking/item?${query.toString()}`, { method: 'DELETE' });
    qs('#tracking-summary-text').textContent = `已删除 ${code} 的复盘记录`;
    await loadTrackingData({
      strategyId: lastTrackingState.strategyId,
      limit: lastTrackingState.limit,
      instrumentType,
      runId: lastTrackingState.runId,
      selectionDate: lastTrackingState.selectionDate,
    });
  } finally {
    button.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  const strategySelect = qs('#tracking-strategy-id');
  const dateSelect = qs('#tracking-selection-date');
  const runSelect = qs('#tracking-run-id');
  const refreshBtn = qs('#refresh-tracking-page');

  qs('#tracking-results-body').addEventListener('click', async (event) => {
    const button = event.target.closest('[data-action="delete-tracking-item"]');
    if (!button) return;
    await deleteTrackingItem(button);
  });

  strategySelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect.value || 'lowvol_reversal',
      limit: lastTrackingState.limit,
      instrumentType: lastTrackingState.instrumentType,
      runId: '',
      selectionDate: '',
    });
  });

  dateSelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: lastTrackingState.limit,
      instrumentType: lastTrackingState.instrumentType,
      runId: '',
      selectionDate: dateSelect.value || '',
    });
  });

  runSelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: lastTrackingState.limit,
      instrumentType: lastTrackingState.instrumentType,
      runId: runSelect.value || '',
      selectionDate: dateSelect?.value || '',
    });
  });

  refreshBtn?.addEventListener('click', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: lastTrackingState.limit,
      instrumentType: lastTrackingState.instrumentType,
      runId: runSelect?.value || '',
      selectionDate: dateSelect?.value || '',
    });
  });

  try {
    if (strategySelect) strategySelect.value = 'lowvol_reversal';
    await loadTrackingData();
  } catch (error) {
    qs('#tracking-summary-text').textContent = `初始化失败: ${error.message}`;
    qs('#tracking-results-body').innerHTML = renderEmptyRow(12, error.message);
  }
});
