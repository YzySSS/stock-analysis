async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const container = qs('#strategies');
  const select = qs('#strategy-id');
  container.innerHTML = '';
  select.innerHTML = '';

  const strategies = data.strategies || [];
  if (!strategies.length) {
    container.innerHTML = '<div class="empty-state">暂无可用策略</div>';
    return;
  }

  strategies.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'strategy-item';
    card.innerHTML = `
      <div class="strategy-item-head">
        <strong>${escapeHtml(item.display_name || item.id)}</strong>
        ${item.id === data.default_strategy ? '<span class="badge status-ok">默认策略</span>' : ''}
      </div>
      <div class="muted">ID: ${escapeHtml(item.id)} · 版本: ${escapeHtml(item.version || '-')} · 状态: ${escapeHtml(item.status || '-')}</div>
      <div>${escapeHtml(item.description || '')}</div>
    `;
    container.appendChild(card);

    const option = document.createElement('option');
    option.value = item.id;
    option.textContent = `${item.display_name || item.id} (${item.id})`;
    if (item.id === data.default_strategy) option.selected = true;
    select.appendChild(option);
  });
}

async function loadTracking() {
  const data = await fetchJson('/api/tracking/latest?limit=20&instrument_type=stock');
  const body = qs('#tracking-body');
  const items = data.items || [];
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

async function runSelection(event) {
  event.preventDefault();
  const output = qs('#selection-run-result');
  output.textContent = '运行中...';

  try {
    const data = await fetchJson('/api/selection/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: qs('#strategy-id').value || null,
        instrument_type: qs('#instrument-type').value,
        limit: Number(qs('#limit').value || 5),
        save: true,
      }),
    });
    output.textContent = JSON.stringify(data, null, 2);
    await loadTracking();
  } catch (error) {
    output.textContent = `运行失败: ${error.message}`;
  }
}

async function refreshSelectionPage() {
  await Promise.all([
    loadStrategies(),
    loadTracking(),
  ]);
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#selection-form').addEventListener('submit', runSelection);
  qs('#refresh-strategies').addEventListener('click', loadStrategies);
  qs('#refresh-tracking').addEventListener('click', loadTracking);
  qs('#refresh-selection-page').addEventListener('click', refreshSelectionPage);

  try {
    await refreshSelectionPage();
  } catch (error) {
    qs('#selection-run-result').textContent = `页面初始化失败: ${error.message}`;
  }
});
