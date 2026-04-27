function updateTrackingStats(summary = {}, items = []) {
  qs('#tracking-stat-count').textContent = String(summary.count ?? items.length ?? 0);
  qs('#tracking-stat-avg').textContent = formatPercent(summary.avg_return_pct);
  qs('#tracking-stat-win-rate').textContent = formatPercent(summary.win_rate_pct);
  qs('#tracking-stat-max-gain').textContent = formatPercent(summary.max_gain_pct);
  qs('#tracking-stat-max-drawdown').textContent = formatPercent(summary.max_drawdown_pct);
}

function renderReviewSummary(summary = {}, runId = '') {
  const container = qs('#tracking-review-summary');
  container.innerHTML = `
    <article class="strategy-item">
      <div class="strategy-item-head">
        <strong>${runId ? `run_id: ${escapeHtml(runId)}` : '最新复盘快照'}</strong>
        <span class="badge status-ok">${summary.count ?? 0} 条</span>
      </div>
      <div class="muted">平均收益：${formatPercent(summary.avg_return_pct)} · 胜率：${formatPercent(summary.win_rate_pct)}</div>
      <div class="muted">最大浮盈：${formatPercent(summary.max_gain_pct)} · 最大回撤：${formatPercent(summary.max_drawdown_pct)}</div>
      <div class="muted">这一区域后续可继续补策略稳定性、超额收益和失败归因。</div>
    </article>
  `;
}

function renderReviewNotes(summary = {}, items = []) {
  const container = qs('#tracking-review-notes');
  const positive = items.filter((item) => (item.price_change_pct ?? -999) >= 0).length;
  const negative = items.filter((item) => (item.price_change_pct ?? 999) < 0).length;
  container.innerHTML = `
    <div>当前复盘判断：正收益 ${positive} 只，负收益 ${negative} 只，胜率 ${formatPercent(summary.win_rate_pct)}。</div>
    <div class="muted">后续应补：基准超额、失败归因、因子失效观察、人工复盘备注。</div>
  `;
}

function renderTrackingTable(items) {
  const body = qs('#tracking-results-body');
  if (!items.length) {
    body.innerHTML = renderEmptyRow(9, '暂无跟踪数据');
    return;
  }

  body.innerHTML = items.map((item) => {
    const pct = item.price_change_pct;
    return `
      <tr>
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>${escapeHtml(item.selection_date || '')}</td>
        <td>${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(pct)}">${formatPercent(pct)}</td>
        <td>${item.tracking_days ?? '-'}</td>
        <td class="up">${formatPercent(item.max_gain_pct)}</td>
        <td class="down">${formatPercent(item.max_drawdown_pct)}</td>
        <td>${escapeHtml(item.review_status || '-')}</td>
      </tr>
    `;
  }).join('');
}

async function loadTrackingData({ runId = '', limit = 20, instrumentType = 'stock' } = {}) {
  const summaryText = qs('#tracking-summary-text');
  summaryText.textContent = '加载中...';

  const url = runId
    ? `/api/tracking?run_id=${encodeURIComponent(runId)}&limit=${limit}&instrument_type=${encodeURIComponent(instrumentType)}`
    : `/api/tracking/latest?limit=${limit}&instrument_type=${encodeURIComponent(instrumentType)}`;

  const data = await fetchJson(url);
  const items = data.items || [];
  const summary = data.summary || {};
  renderTrackingTable(items);
  updateTrackingStats(summary, items);
  renderReviewSummary(summary, runId);
  renderReviewNotes(summary, items);
  summaryText.textContent = runId
    ? `当前显示 run_id=${runId} 的复盘结果，共 ${items.length} 条`
    : `当前显示最新复盘快照，共 ${items.length} 条`;
}

async function handleTrackingFilter(event) {
  event.preventDefault();
  try {
    await loadTrackingData({
      runId: qs('#tracking-run-id').value.trim(),
      limit: Number(qs('#tracking-limit').value || 20),
      instrumentType: qs('#tracking-instrument-type').value,
    });
  } catch (error) {
    qs('#tracking-summary-text').textContent = `加载失败: ${error.message}`;
    qs('#tracking-results-body').innerHTML = renderEmptyRow(9, error.message);
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#tracking-filter-form').addEventListener('submit', handleTrackingFilter);
  qs('#tracking-latest-btn').addEventListener('click', async () => {
    qs('#tracking-run-id').value = '';
    await loadTrackingData({
      limit: Number(qs('#tracking-limit').value || 20),
      instrumentType: qs('#tracking-instrument-type').value,
    });
  });

  try {
    await loadTrackingData();
  } catch (error) {
    qs('#tracking-summary-text').textContent = `初始化失败: ${error.message}`;
    qs('#tracking-results-body').innerHTML = renderEmptyRow(9, error.message);
  }
});
