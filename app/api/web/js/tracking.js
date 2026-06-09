function updateTrackingStats(summary = {}, items = []) {
  qs('#tracking-stat-count').textContent = String(summary.count ?? items.length ?? 0);
  qs('#tracking-stat-active-count').textContent = String(summary.tracking_count ?? 0);
  qs('#tracking-stat-avg').textContent = formatPercent(summary.avg_return_pct);
  qs('#tracking-stat-win-rate').textContent = formatPercent(summary.win_rate_pct);
  qs('#tracking-stat-excess-return').textContent = formatPercent(summary.excess_return_pct);

  ['#tracking-stat-avg', '#tracking-stat-win-rate', '#tracking-stat-excess-return', '#tracking-stat-max-gain', '#tracking-stat-max-drawdown'].forEach((selector) => {
    qs(selector).classList.remove('up', 'down');
  });

  const avgReturnClass = getPctClass(summary.avg_return_pct);
  if (avgReturnClass) {
    qs('#tracking-stat-avg').classList.add(avgReturnClass);
  }

  if (summary.win_rate_pct != null && !Number.isNaN(Number(summary.win_rate_pct))) {
    qs('#tracking-stat-win-rate').classList.add(Number(summary.win_rate_pct) >= 60 ? 'up' : 'down');
  }

  qs('#tracking-stat-excess-return').classList.remove('up', 'down');
  const excessReturnClass = getPctClass(summary.excess_return_pct);
  if (excessReturnClass) {
    qs('#tracking-stat-excess-return').classList.add(excessReturnClass);
  }
  qs('#tracking-stat-max-gain').textContent = formatPercent(summary.max_gain_pct);
  qs('#tracking-stat-max-drawdown').textContent = formatPercent(summary.max_drawdown_pct);
  const maxGainClass = getPctClass(summary.max_gain_pct);
  if (maxGainClass) {
    qs('#tracking-stat-max-gain').classList.add(maxGainClass);
  }
  const maxDrawdownClass = getPctClass(summary.max_drawdown_pct);
  if (maxDrawdownClass) {
    qs('#tracking-stat-max-drawdown').classList.add(maxDrawdownClass);
  }
}

function statsToggleLabel(includeInStats) {
  return includeInStats ? '纳入统计' : '不统计';
}

function getStrategyLabel(item) {
  return item?.strategy_display_name || item?.strategy_id || '-';
}

function summarizeStrategies(items = []) {
  const counts = new Map();
  items.forEach((item) => {
    const key = getStrategyLabel(item);
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  return Array.from(counts.entries()).map(([name, count]) => ({ name, count }));
}

function summarizeDates(items = []) {
  const values = Array.from(new Set(items.map((item) => item.selection_date).filter(Boolean)));
  values.sort().reverse();
  return values;
}

function formatDateRange(dates = []) {
  if (!dates.length) return '-';
  return dates.length > 1 ? `${dates[0]} ~ ${dates[dates.length - 1]}` : dates[0];
}

function renderSummaryCard({ title, summary, strategyText, dateText }) {
  const best = summary.best_item;
  const worst = summary.worst_item;
  return `
    <article class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(title)}</strong>
        <span class="badge status-ok">${summary.count ?? 0} 条</span>
      </div>
      <div class="muted">覆盖日期：${escapeHtml(dateText)} · 覆盖策略：${escapeHtml(strategyText)} · 仍在跟踪：${summary.tracking_count ?? 0} 条</div>
      <div class="muted">平均收益：${formatPercent(summary.avg_return_pct)} · 胜率：${formatPercent(summary.win_rate_pct)} · 超额收益：${formatPercent(summary.excess_return_pct)}</div>
      <div class="muted">最大浮盈：${formatPercent(summary.max_gain_pct)} · 最大回撤：${formatPercent(summary.max_drawdown_pct)}</div>
      <div class="muted">表现最好：${best ? `${escapeHtml(best.name || best.code || '-')} (${formatPercent(best.price_change_pct)})` : '暂无'}</div>
      <div class="muted">表现最弱：${worst ? `${escapeHtml(worst.name || worst.code || '-')} (${formatPercent(worst.price_change_pct)})` : '暂无'}</div>
    </article>
  `;
}

function renderReviewSummary(filteredSummary = {}, strategySummaries = [], pageItems = []) {
  const container = qs('#tracking-review-summary');
  if (!container) return;
  if (strategySummaries.length > 1) {
    container.innerHTML = strategySummaries.map((item) => renderSummaryCard({
      title: item.strategy_display_name || item.strategy_id || '-',
      summary: item,
      strategyText: item.strategy_display_name || item.strategy_id || '-',
      dateText: formatDateRange(item.selection_dates || []),
    })).join('');
    return;
  }

  const dates = summarizeDates(pageItems);
  const strategies = strategySummaries.length
    ? strategySummaries.map((item) => ({ name: item.strategy_display_name || item.strategy_id || '-', count: item.count ?? 0 }))
    : summarizeStrategies(pageItems);
  const strategyText = strategies.length
    ? strategies.map((item) => `${item.name}（${item.count}）`).join('、')
    : '暂无';
  const title = strategySummaries[0]?.strategy_display_name || strategies[0]?.name || '复盘概览';

  container.innerHTML = renderSummaryCard({
    title,
    summary: filteredSummary,
    strategyText,
    dateText: formatDateRange(strategySummaries[0]?.selection_dates || dates),
  });
}

function renderStrategyNoteCard(item) {
  const best = item.best_item;
  const worst = item.worst_item;
  return `
    <article class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</strong>
        <span class="badge status-ok">${item.count ?? 0} 条</span>
      </div>
      <div>当前策略表现：正收益 ${item.win_count ?? '-'} 只，负收益 ${item.loss_count ?? '-'} 只，持平 ${item.flat_count ?? '-'} 只，胜率 ${formatPercent(item.win_rate_pct)}，超额收益 ${formatPercent(item.excess_return_pct)}。</div>
      <div class="muted">成功侧：${best ? `${escapeHtml(best.name || best.code || '-')} 领跑，说明该策略在当前筛选范围内仍有较强延续性。` : '暂无足够样本。'}</div>
      <div class="muted">失败侧：${worst ? `${escapeHtml(worst.name || worst.code || '-')} 偏弱，需要重点复盘回撤来源与信号质量。` : '暂无明显失败样本。'}</div>
    </article>
  `;
}

function renderReviewNotes(filteredSummary = {}, strategySummaries = [], pageItems = []) {
  const container = qs('#tracking-review-notes');
  if (!container) return;
  if (strategySummaries.length > 1) {
    container.innerHTML = strategySummaries.map(renderStrategyNoteCard).join('');
    return;
  }

  const best = filteredSummary.best_item;
  const worst = filteredSummary.worst_item;
  const strategyLead = strategySummaries.length === 1
    ? `当前筛选结果对应 ${strategySummaries[0].strategy_display_name || strategySummaries[0].strategy_id || '单策略'}`
    : `当前筛选结果共 ${pageItems.length} 条`;
  container.innerHTML = `
    <div>${strategyLead}：正收益 ${filteredSummary.win_count ?? '-'} 只，负收益 ${filteredSummary.loss_count ?? '-'} 只，持平 ${filteredSummary.flat_count ?? '-'} 只，胜率 ${formatPercent(filteredSummary.win_rate_pct)}，超额收益 ${formatPercent(filteredSummary.excess_return_pct)}。</div>
    <div class="muted">当前成功特征：${best ? `${escapeHtml(best.name || best.code || '-')} 领跑，说明筛选结果中存在表现延续较强的标的。` : '暂无足够样本。'}</div>
    <div class="muted">当前失败特征：${worst ? `${escapeHtml(worst.name || worst.code || '-')} 偏弱，需结合回撤和基本面缺口继续复盘。` : '暂无明显失败样本。'}</div>
  `;
}

let lastTrackingState = {
  runId: '',
  strategyId: '',
  limit: 10,
  instrumentType: 'stock',
  selectionDate: '',
  offset: 0,
  deepReview: null,
  deepReviewAvailable: false,
};

function renderDeepReviewAnalysis(result) {
  const container = qs('#tracking-review-notes');
  if (!container) return;
  const analysis = result?.analysis || '暂无复盘结果';
  container.innerHTML = `
    <article class="strategy-item deep-review-result">
      <div class="strategy-item-head">
        <strong>DeepSeek 详细复盘</strong>
        <span class="badge status-ok">${escapeHtml(result?.model || '-')}</span>
      </div>
      <div class="muted">分析样本：${escapeHtml(result?.item_count ?? '-')} 条 · 模板：${escapeHtml(result?.prompt_template || '-')}</div>
      <pre class="deep-review-text">${escapeHtml(analysis)}</pre>
    </article>
  `;
}

async function loadDeepReviewStatus() {
  const button = qs('#tracking-deep-review');
  if (!button) return;
  try {
    const status = await fetchJson('/api/tracking/deep-review/status');
    lastTrackingState.deepReviewAvailable = Boolean(status.available);
    button.disabled = !status.available;
    button.textContent = status.available ? '详细复盘' : 'AI复盘未配置';
    button.title = status.available
      ? `使用 ${status.model || 'DeepSeek'} 生成详细复盘`
      : (status.message || '未配置 AI 复盘密钥');
  } catch (error) {
    lastTrackingState.deepReviewAvailable = false;
    button.disabled = true;
    button.textContent = 'AI复盘不可用';
    button.title = error.message;
  }
}

function renderTrackingTable(items, summary = {}) {
  const body = qs('#tracking-results-body');
  if (!items.length) {
    body.innerHTML = renderEmptyRow(14, '暂无跟踪数据');
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
    const includeInStats = item.include_in_stats !== false;
    return `
      <tr class="${includeInStats ? '' : 'tracking-excluded-row'}" data-tracking-key="${escapeHtml(`${item.code || ''}__${item.selection_date || ''}__${item.strategy_id || ''}`)}">
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>${escapeHtml(getStrategyLabel(item))}</td>
        <td>${escapeHtml(item.selection_date || '')}</td>
        <td>${formatNumber(item.selected_open_price ?? item.selected_close_price, 2)}</td>
        <td>
          ${formatNumber(item.current_price, 2)}
          <div class="muted">${escapeHtml(item.realtime_quote_time || '无实时')}</div>
        </td>
        <td class="${getPctClass(pct)}">${formatPercent(pct)}</td>
        <td class="${getPctClass(excessReturn)}">${formatPercent(excessReturn)}</td>
        <td>${item.tracking_days ?? '-'}</td>
        <td class="up">${formatPercent(item.max_gain_pct)}</td>
        <td class="down">${formatPercent(item.max_drawdown_pct)}</td>
        <td>${escapeHtml(item.review_status || '-')}</td>
        <td>${escapeHtml(reviewNote)}</td>
        <td>
          <button class="btn btn-sm ${includeInStats ? 'btn-secondary' : 'btn-warning'}" type="button" data-action="toggle-tracking-stats" data-code="${escapeHtml(item.code || '')}" data-selection-date="${escapeHtml(item.selection_date || '')}" data-strategy-id="${escapeHtml(item.strategy_id || '')}" data-include-in-stats="${includeInStats ? 'true' : 'false'}">
            ${escapeHtml(statsToggleLabel(includeInStats))}
          </button>
        </td>
        <td><button class="btn btn-danger btn-sm" type="button" data-action="delete-tracking-item" data-code="${escapeHtml(item.code || '')}" data-selection-date="${escapeHtml(item.selection_date || '')}" data-strategy-id="${escapeHtml(item.strategy_id || '')}">删除</button></td>
      </tr>
    `;
  }).join('');
}

function renderTrackingCards(items = [], summary = {}) {
  const container = qs('#tracking-record-cards');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无跟踪数据';
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.slice(0, 8).map((item) => {
    const pct = item.price_change_pct;
    const excessReturn = pct != null && summary.benchmark_return_pct != null
      ? Number(pct) - Number(summary.benchmark_return_pct)
      : null;
    const includeInStats = item.include_in_stats !== false;
    return `
      <article class="tracking-record-card ${includeInStats ? '' : 'tracking-excluded-row'}">
        <div class="tracking-record-head">
          <div>
            <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '-')}</a>
            <span>${escapeHtml(item.code || '-')} · ${escapeHtml(getStrategyLabel(item))}</span>
          </div>
          <em class="${getPctClass(pct) || ''}">${formatPercent(pct)}</em>
        </div>
        <div class="tracking-record-grid">
          <span>入选价 <b>${formatNumber(item.selected_open_price ?? item.selected_close_price, 2)}</b></span>
          <span>实时价 <b>${formatNumber(item.current_price, 2)}</b></span>
          <span>超额 <b class="${getPctClass(excessReturn) || ''}">${formatPercent(excessReturn)}</b></span>
          <span>跟踪 <b>${item.tracking_days ?? '-'} 天</b></span>
          <span>最大浮盈 <b class="up">${formatPercent(item.max_gain_pct)}</b></span>
          <span>最大回撤 <b class="down">${formatPercent(item.max_drawdown_pct)}</b></span>
        </div>
        <div class="tracking-record-foot">
          <span>${escapeHtml(item.selection_date || '-')} · ${escapeHtml(item.realtime_quote_time || '无实时')}</span>
          <span class="badge ${includeInStats ? 'status-ok' : 'status-muted'}">${includeInStats ? '纳入统计' : '不统计'}</span>
        </div>
      </article>
    `;
  }).join('');
}

function renderStrategyOptions(options = [], selectedValue = '') {
  const select = qs('#tracking-strategy-id');
  if (!select) return;
  select.innerHTML = '';
  const allOption = document.createElement('option');
  allOption.value = '';
  allOption.textContent = '全部策略';
  select.appendChild(allOption);

  options.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.strategy_id || '';
    option.textContent = item.strategy_display_name || item.strategy_id || '';
    if (option.value === selectedValue) option.selected = true;
    select.appendChild(option);
  });

  select.value = selectedValue || '';
}

function syncDateInputShell() {
  const input = qs('#tracking-selection-date');
  const shell = qs('#tracking-selection-date-shell');
  const display = qs('#tracking-selection-date-display');
  if (!input || !shell) return;
  const hasValue = Boolean(input.value);
  shell.classList.toggle('has-value', hasValue);
  if (display) {
    display.textContent = hasValue ? input.value.replaceAll('-', '/') : '全部日期';
  }
}

function renderSelectionDateOptions(dates = [], selectedValue = '') {
  const input = qs('#tracking-selection-date');
  if (!input) return;
  const sortedDates = [...dates].filter(Boolean).sort();
  input.min = sortedDates[0] || '';
  input.max = sortedDates[sortedDates.length - 1] || '';
  input.value = selectedValue || '';
  input.title = sortedDates.length
    ? `可选日期范围：${sortedDates[0]} ~ ${sortedDates[sortedDates.length - 1]}`
    : '暂无可选日期';
  syncDateInputShell();
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

async function loadTrackingData({ runId = '', strategyId = '', limit = 10, instrumentType = 'stock', selectionDate = '', offset = 0 } = {}) {
  const summaryText = qs('#tracking-summary-text');
  if (summaryText) summaryText.textContent = '加载中...';

  const filters = await loadTrackingFilters(strategyId, instrumentType);
  renderStrategyOptions(filters.strategy_options || [], strategyId);
  const selectionDates = filters.selection_dates || [];
  const effectiveSelectionDate = selectionDate && selectionDates.includes(selectionDate)
    ? selectionDate
    : '';
  renderSelectionDateOptions(selectionDates, effectiveSelectionDate);

  const query = new URLSearchParams({ limit: String(limit), offset: String(offset), instrument_type: instrumentType });
  if (runId) query.set('run_id', runId);
  if (strategyId) query.set('strategy_id', strategyId);
  if (effectiveSelectionDate) query.set('selection_date', effectiveSelectionDate);

  const data = await fetchJson(`/api/tracking?${query.toString()}`);
  const items = data.items || [];
  const filteredSummary = data.filtered_summary || data.summary || {};
  const pageSummary = data.summary || {};
  const strategySummaries = data.strategy_summaries || [];
  const pagination = data.pagination || {};
  lastTrackingState = {
    runId,
    strategyId,
    limit,
    instrumentType,
    selectionDate: effectiveSelectionDate,
    offset,
    deepReview: null,
    deepReviewAvailable: lastTrackingState.deepReviewAvailable,
  };
  renderTrackingTable(items, filteredSummary);
  updateTrackingStats(filteredSummary, items);
  renderReviewSummary(filteredSummary, strategySummaries, items);
  renderReviewNotes(filteredSummary, strategySummaries, items);
  renderPagination(pagination, limit);
  const modeText = runId
      ? '当前显示该轮选股的复盘结果'
      : effectiveSelectionDate
      ? `当前显示 ${effectiveSelectionDate} 的复盘结果`
      : strategyId
        ? '当前显示该策略全部历史复盘列表'
        : '当前显示全部策略历史复盘列表';
  const excludedText = filteredSummary.excluded_count ? `；已排除 ${filteredSummary.excluded_count} 条不参与统计` : '';
  if (summaryText) summaryText.textContent = `${modeText}，本页 ${items.length} 条，共 ${pagination.total || pageSummary.count || 0} 条${excludedText}`;
}

async function runDeepReview() {
  const button = qs('#tracking-deep-review');
  const summaryText = qs('#tracking-summary-text');
  if (!lastTrackingState.deepReviewAvailable) {
    if (summaryText) summaryText.textContent = '详细复盘不可用：未配置 DEEPSEEK_API_KEY 或 OPENAI_API_KEY';
    return;
  }
  if (button) {
    button.disabled = true;
    button.textContent = '复盘中...';
  }
  if (summaryText) summaryText.textContent = '正在调用 DeepSeek 进行详细复盘...';
  try {
    const result = await fetchJson('/api/tracking/deep-review', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: lastTrackingState.strategyId || null,
        selection_date: lastTrackingState.selectionDate || null,
        run_id: lastTrackingState.runId || null,
        instrument_type: lastTrackingState.instrumentType || 'stock',
        max_items: 80,
      }),
    });
    lastTrackingState.deepReview = result;
    renderDeepReviewAnalysis(result);
    if (summaryText) summaryText.textContent = `详细复盘完成：${result.item_count ?? '-'} 条，模型 ${result.model || '-'}`;
  } catch (error) {
    if (summaryText) summaryText.textContent = `详细复盘失败：${error.message}`;
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = '详细复盘';
    }
  }
}

async function toggleTrackingStats(button) {
  const code = button?.dataset?.code || '';
  const selectionDate = button?.dataset?.selectionDate || '';
  const strategyId = button?.dataset?.strategyId || '';
  const includeInStats = button?.dataset?.includeInStats !== 'false';
  const instrumentType = lastTrackingState.instrumentType || 'stock';
  if (!code || !selectionDate || !strategyId) {
    qs('#tracking-summary-text').textContent = '切换统计状态失败：当前行缺少 code / selection_date / strategy_id';
    return;
  }

  button.disabled = true;
  try {
    const nextValue = !includeInStats;
    const query = new URLSearchParams({ code, selection_date: selectionDate, strategy_id: strategyId, instrument_type: instrumentType });
    await fetchJson(`/api/tracking/item/stats?${query.toString()}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ include_in_stats: nextValue }),
    });
    qs('#tracking-summary-text').textContent = nextValue
      ? `已将 ${code} 重新纳入统计`
      : `已将 ${code} 标记为不统计`;
    await loadTrackingData({
      strategyId: lastTrackingState.strategyId,
      runId: lastTrackingState.runId,
      limit: lastTrackingState.limit,
      instrumentType,
      selectionDate: lastTrackingState.selectionDate,
      offset: lastTrackingState.offset,
    });
  } finally {
    button.disabled = false;
  }
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
      runId: lastTrackingState.runId,
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
  const deepReviewBtn = qs('#tracking-deep-review');
  const prevBtn = qs('#tracking-prev-page');
  const nextBtn = qs('#tracking-next-page');

  qs('#tracking-results-body').addEventListener('click', async (event) => {
    const statsButton = event.target.closest('[data-action="toggle-tracking-stats"]');
    if (statsButton) {
      await toggleTrackingStats(statsButton);
      return;
    }
    const deleteButton = event.target.closest('[data-action="delete-tracking-item"]');
    if (!deleteButton) return;
    await deleteTrackingItem(deleteButton);
  });

  strategySelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect.value || '',
      runId: '',
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect?.value || lastTrackingState.selectionDate || '',
      offset: 0,
    });
  });

  dateSelect?.addEventListener('change', async () => {
    syncDateInputShell();
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      runId: '',
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect.value || '',
      offset: 0,
    });
  });


  pageSizeSelect?.addEventListener('change', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      runId: '',
      limit: Number(pageSizeSelect.value || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect?.value || '',
      offset: 0,
    });
  });

  refreshBtn?.addEventListener('click', async () => {
    await loadTrackingData({
      strategyId: strategySelect?.value || lastTrackingState.strategyId,
      runId: lastTrackingState.runId,
      limit: Number(pageSizeSelect?.value || lastTrackingState.limit || 10),
      instrumentType: lastTrackingState.instrumentType,
      selectionDate: dateSelect?.value || '',
      offset: lastTrackingState.offset || 0,
    });
  });
  deepReviewBtn?.addEventListener('click', runDeepReview);

  prevBtn?.addEventListener('click', async () => {
    const nextOffset = Math.max((lastTrackingState.offset || 0) - (lastTrackingState.limit || 10), 0);
    await loadTrackingData({ ...lastTrackingState, offset: nextOffset });
  });

  nextBtn?.addEventListener('click', async () => {
    const nextOffset = (lastTrackingState.offset || 0) + (lastTrackingState.limit || 10);
    await loadTrackingData({ ...lastTrackingState, offset: nextOffset });
  });

  try {
    if (pageSizeSelect) pageSizeSelect.value = '10';
    await loadDeepReviewStatus();
    const params = new URLSearchParams(window.location.search);
    await loadTrackingData({
      runId: params.get('run_id') || '',
      strategyId: params.get('strategy_id') || '',
      selectionDate: params.get('selection_date') || '',
      limit: 10,
      offset: 0,
    });
  } catch (error) {
    qs('#tracking-summary-text').textContent = `初始化失败: ${error.message}`;
    qs('#tracking-results-body').innerHTML = renderEmptyRow(13, error.message);
  }
});
