function updateTrackingStats(items) {
  const pctValues = items
    .map((item) => Number(item.price_change_pct))
    .filter((value) => !Number.isNaN(value));

  const upCount = pctValues.filter((value) => value >= 0).length;
  const downCount = pctValues.filter((value) => value < 0).length;
  const avg = pctValues.length ? pctValues.reduce((sum, value) => sum + value, 0) / pctValues.length : null;

  qs('#tracking-stat-count').textContent = String(items.length);
  qs('#tracking-stat-up').textContent = String(upCount);
  qs('#tracking-stat-down').textContent = String(downCount);
  qs('#tracking-stat-avg').textContent = formatPercent(avg);
}

function renderTrackingTable(items) {
  const body = qs('#tracking-results-body');
  if (!items.length) {
    body.innerHTML = renderEmptyRow(8, '暂无跟踪数据');
    return;
  }

  body.innerHTML = items.map((item) => {
    const pct = item.price_change_pct;
    return `
      <tr>
        <td><a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.code || '')}</a></td>
        <td>${escapeHtml(item.name || '')}</td>
        <td>${escapeHtml(item.selection_date || '')}</td>
        <td>${escapeHtml(item.strategy_display_name || item.strategy_id || '')}</td>
        <td>${formatNumber(item.score, 2)}</td>
        <td>${formatNumber(item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(pct)}">${formatPercent(pct)}</td>
      </tr>
    `;
  }).join('');
}

async function loadTrackingData({ runId = '', limit = 20, instrumentType = 'stock' } = {}) {
  const summary = qs('#tracking-summary-text');
  summary.textContent = '加载中...';

  const url = runId
    ? `/api/tracking?run_id=${encodeURIComponent(runId)}&limit=${limit}&instrument_type=${encodeURIComponent(instrumentType)}`
    : `/api/tracking/latest?limit=${limit}&instrument_type=${encodeURIComponent(instrumentType)}`;

  const data = await fetchJson(url);
  const items = data.items || [];
  renderTrackingTable(items);
  updateTrackingStats(items);
  summary.textContent = runId
    ? `当前显示 run_id=${runId} 的结果，共 ${items.length} 条`
    : `当前显示最新 tracking 快照，共 ${items.length} 条`;
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
    qs('#tracking-results-body').innerHTML = renderEmptyRow(8, error.message);
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
    qs('#tracking-results-body').innerHTML = renderEmptyRow(8, error.message);
  }
});
