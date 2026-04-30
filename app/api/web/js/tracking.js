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
  strategyId: '',
  limit: 10,
  instrumentType: 'stock',
  selectionDate: '',
  offset: 0,
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
        <td>${formatNumber(item.selected_open_price ?? item.selected_close_price, 2)}</td>
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
  latestOption.textContent = '全部日期';
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

function renderPagination(pagination = {}, pageSize = 10) {
  const total = Number(pagination.total || 0);
  const offset = Number(pagination.offset || 0);
  const limit = Number(pagination.limit || pageSize || 10);
  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.max(Math.ceil(total / limit), 1);
  const start = total === 0 ? 0 : offset + 1;
  const end = Math.min(offset + limit, total);

  qs('#tracking-pagination-summary').textContent = `第 ${currentPage} / ${totalPages} 页 · 显示 ${start}-${end} / 共 ${total} 条`;
  qs('#tracking-prev-page').disabled = offset <= 0;
  qs('#tracking-next-page').disabled = offset + limit >= total;
}

async function loadTrackingFilters(strategyId, instrumentType = 'stock') {
  const query = new URLSearchParams({ instrument_type: instrumentType });
  if (strategyId) query.set('strategy_id', strategyId);
  return fetchJson(`/api/tracking/filters?${query.toString()}`);
}

async function loadTrackingData({ strategyId = '', limit = 10, instrumentType = 'stock', selectionDate = '', offset = 0 } = {}) {
  const summaryText = qs('#tracking-summary-text');
  summaryText.textContent = '加载中...';

  const filters = await loadTrackingFilters(strategyId, instrumentType);
  const selectionDates = filters.selection_dates || [];
  renderSelectionDateOptions(selectionDates, selectionDate);

  const query = new URLSearchParams({ limit: String(limit), offset: String(offset), instrument_type: instrumentType });
  if (strategyId) query.set('strategy_id', strategyId);
  if (selectionDate) query.set('selection_date', selectionDate);

  const url = selectionDate
    ? `/api/tracking?${query.toString()}`
    : `/api/tracking?${query.toString()}`;

  const data = await fetchJson(url);
  const items = data.items || [];
  const summary = data.summary || {};
  const pagination = data.pagination || {};
  lastTrackingState = { strategyId, limit, instrumentType, selectionDate, offset };
  renderTrackingTable(items, summary);
  updateTrackingStats(summary, items);
  renderReviewSummary(summary, items);
  renderReviewNotes(summary, items);
  renderPagination(pagination, limit);
  const modeText = selectionDate
      ? `当前显示 ${selectionDate} 的复盘结果`
      : strategyId
        ? '当前显示该策略全部历史复盘列表'
        : '当前显示全部策略历史复盘列表';
  summaryText.textContent = `${modeText}，本页 ${items.length} 条`;
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
      selectionDate: lastTrackingState.selectionDate,
      offset: 0,
    });
  } finally {
    button.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  const strategySelect = qs('#tracking-strategy-id');
  const dateSelect = qs('#tracking-selection-date');
  const pageSizeSelect = qs('#tracking-page-size');
  const refreshBtn = qs('#refresh-tracking-page');
  const prevBtn = qs('#tracking-prev-page');
  const nextBtn = qs('#tracking-next-page');

  qs('#tracking-results-body').addEventListener('click', async (event) => {
    const button = event.target.closest('[data-action="delete-tracking-item"]');
    if (!button) return;
    await deleteTrackingItem(button);
  });

  strategySelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect.value || '',
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: '',
      offset: 0,
    });
  });

  dateSelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect.value || '',
      offset: 0,
    });
  });

  pageSizeSelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: Number(pageSizeSelect.value || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect?.value || '',
      offset: 0,
    });
  });

  refreshBtn?.addEventListener('click', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect?.value || '',
      offset: lastTrackingState.offset || 0,
    });
  });

  prevBtn?.addEventListener('click', async () => {
    const nextOffset = Math.max((lastTrackingState.offset || 0) - (lastTrackingState.limit || 10), 0);
    await loadTrackingData({ ...lastTrackingState, offset: nextOffset });
  });

  nextBtn?.addEventListener('click', async () => {
    const nextOffset = (lastTrackingState.offset || 0) + (lastTrackingState.limit || 10);
    await loadTrackingData({ ...lastTrackingState, offset: nextOffset });
  });

  try {
    if (strategySelect) strategySelect.value = '';
    if (pageSizeSelect) pageSizeSelect.value = '10';
    await loadTrackingData({ limit: 10, offset: 0 });
  } catch (error) {
    qs('#tracking-summary-text').textContent = `初始化失败: ${error.message}`;
    qs('#tracking-results-body').innerHTML = renderEmptyRow(12, error.message);
  }
});
