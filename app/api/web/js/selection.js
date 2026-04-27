let currentDefaultStrategy = null;

function renderStrategySummary(strategy) {
  const container = qs('#strategy-summary');
  if (!strategy) {
    container.innerHTML = '<div class="empty-state">暂无策略信息</div>';
    return;
  }

  const factors = strategy.factors || [];
  container.innerHTML = `
    <div class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategy.display_name || strategy.id || '-')}</strong>
        <span class="badge status-ok">${escapeHtml(strategy.version || '-')}</span>
      </div>
      <div class="muted">ID: ${escapeHtml(strategy.id || '-')} · 状态: ${escapeHtml(strategy.status || '-')}</div>
      <div>${escapeHtml(strategy.description || '')}</div>
      <div class="muted">阈值: ${strategy.score_threshold ?? '-'} · 最多入选: ${strategy.max_picks ?? '-'}</div>
      <div class="muted">核心因子: ${factors.map((item) => escapeHtml(item.name || item.key || '-')).join(' / ') || '暂无'}</div>
    </div>
  `;
}

function renderFactorAnalysis(strategy) {
  const body = qs('#factor-analysis-body');
  const items = strategy?.factors || [];
  if (!items.length) {
    body.innerHTML = renderEmptyRow(4, '暂无因子配置');
    return;
  }

  body.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.name || item.key || '')}</td>
      <td>${escapeHtml(item.direction || 'positive')}</td>
      <td>${formatNumber(item.weight, 2)}</td>
      <td>${escapeHtml(item.description || '')}</td>
    </tr>
  `).join('');
}

function renderSelectionResults(data) {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const items = data.items || [];
  const summary = data.summary || {};

  summaryLine.textContent = `选股日期：${summary.selected_trade_date || '-'} · 最新交易日：${summary.latest_trade_date || '-'} · 当前展示：${summary.total_count || 0} 条`;

  if (!items.length) {
    body.innerHTML = renderEmptyRow(9, '暂无选股结果');
    return;
  }

  body.innerHTML = items.map((item) => {
    const reasons = (item.reason_summary || []).slice(0, 2).join('；') || '-';
    const risks = (item.risk_summary || []).slice(0, 2).join('；') || '-';
    return `
      <tr>
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>${escapeHtml(item.selection_date || '-')}</td>
        <td>${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</td>
        <td>${formatNumber(item.score, 4)}</td>
        <td>${item.rank_no ?? '-'}</td>
        <td>${escapeHtml(reasons)}</td>
        <td>${escapeHtml(risks)}</td>
      </tr>
    `;
  }).join('');
}

async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const select = qs('#strategy-id');
  select.innerHTML = '';

  const strategies = data.strategies || [];
  currentDefaultStrategy = data.default_strategy || null;

  strategies.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.id;
    option.textContent = `${item.display_name || item.id} (${item.id})`;
    if (item.id === data.default_strategy) option.selected = true;
    select.appendChild(option);
  });

  if (select.value) {
    await loadStrategyDetail(select.value);
  }
}

async function loadStrategyDetail(strategyId) {
  const data = await fetchJson(`/api/strategies/detail?strategy_id=${encodeURIComponent(strategyId || currentDefaultStrategy || '')}`);
  renderStrategySummary(data.strategy);
  renderFactorAnalysis(data.strategy);
}

async function loadSelectionResults() {
  const instrumentType = qs('#instrument-type').value || 'stock';
  const limit = Number(qs('#limit').value || 20);
  const data = await fetchJson(`/api/selection/results?instrument_type=${encodeURIComponent(instrumentType)}&limit=${limit}`);
  renderSelectionResults(data);
  if (data.strategy) {
    renderStrategySummary(data.strategy);
    renderFactorAnalysis(data.strategy);
  }
}

async function runSelection(event) {
  event.preventDefault();
  const button = event.submitter || qs('#selection-form button[type="submit"]');
  if (button) button.disabled = true;

  try {
    await fetchJson('/api/selection/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: qs('#strategy-id').value || null,
        instrument_type: qs('#instrument-type').value,
        limit: Number(qs('#limit').value || 20),
        save: true,
      }),
    });
    await loadSelectionResults();
  } finally {
    if (button) button.disabled = false;
  }
}

async function refreshSelectionPage() {
  await loadStrategies();
  await loadSelectionResults();
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#selection-form').addEventListener('submit', runSelection);
  qs('#refresh-strategies').addEventListener('click', async () => {
    await loadStrategies();
  });
  qs('#refresh-results').addEventListener('click', loadSelectionResults);
  qs('#refresh-selection-page').addEventListener('click', refreshSelectionPage);
  qs('#strategy-id').addEventListener('change', async (event) => {
    await loadStrategyDetail(event.target.value);
  });

  try {
    await refreshSelectionPage();
  } catch (error) {
    qs('#selection-summary-line').textContent = `页面初始化失败: ${error.message}`;
  }
});
