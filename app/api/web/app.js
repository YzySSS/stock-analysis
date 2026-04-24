async function fetchJson(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const container = document.getElementById('strategies');
  const select = document.getElementById('strategy-id');
  container.innerHTML = '';
  select.innerHTML = '';

  (data.strategies || []).forEach((item) => {
    const div = document.createElement('div');
    div.className = 'strategy-item';
    div.innerHTML = `
      <strong>${item.display_name || item.id}</strong>
      <div class="muted">ID: ${item.id} · 版本: ${item.version || '-'} · 状态: ${item.status || '-'}</div>
      <div>${item.description || ''}</div>
    `;
    container.appendChild(div);

    const option = document.createElement('option');
    option.value = item.id;
    option.textContent = `${item.display_name || item.id} (${item.id})`;
    if (item.id === data.default_strategy) option.selected = true;
    select.appendChild(option);
  });
}

async function runSelection(event) {
  event.preventDefault();
  const strategyId = document.getElementById('strategy-id').value;
  const instrumentType = document.getElementById('instrument-type').value;
  const limit = Number(document.getElementById('limit').value || 5);
  const output = document.getElementById('selection-run-result');
  output.textContent = '运行中...';

  try {
    const data = await fetchJson('/api/selection/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: strategyId,
        instrument_type: instrumentType,
        limit,
        save: true,
      }),
    });
    output.textContent = JSON.stringify(data, null, 2);
    await loadTracking();
  } catch (error) {
    output.textContent = `运行失败: ${error.message}`;
  }
}

async function loadTracking() {
  const data = await fetchJson('/api/tracking/latest?limit=20&instrument_type=stock');
  const body = document.getElementById('tracking-body');
  body.innerHTML = '';

  (data.items || []).forEach((item) => {
    const tr = document.createElement('tr');
    const pct = item.price_change_pct;
    const pctClass = pct == null ? '' : pct >= 0 ? 'up' : 'down';
    tr.innerHTML = `
      <td>${item.code || ''}</td>
      <td>${item.name || ''}</td>
      <td>${item.selection_date || ''}</td>
      <td>${item.strategy_display_name || item.strategy_id || ''}</td>
      <td>${item.score ?? ''}</td>
      <td>${item.selected_open_price ?? ''}</td>
      <td>${item.current_price ?? ''}</td>
      <td class="${pctClass}">${pct == null ? '' : pct + '%'}</td>
    `;
    body.appendChild(tr);
  });
}

document.getElementById('refresh-strategies').addEventListener('click', loadStrategies);
document.getElementById('refresh-tracking').addEventListener('click', loadTracking);
document.getElementById('selection-form').addEventListener('submit', runSelection);

loadStrategies();
loadTracking();
