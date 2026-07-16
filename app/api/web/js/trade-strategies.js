let selectedTradeStrategy = null;

function ruleSummary(rule = {}) {
  if (!rule || typeof rule !== 'object') return '-';
  const parts = [];
  if (rule.entry_day) {
    const entryDayLabel = rule.entry_day === 'selection_day'
      ? '入选日（旧口径）'
      : rule.entry_day === 'next_trading_day'
        ? '信号后下一交易日'
        : rule.entry_day;
    parts.push(`买入日：${entryDayLabel}`);
  }
  if (rule.entry_price) parts.push(`买入价：${rule.entry_price === 'open' ? '开盘价' : rule.entry_price}`);
  if (rule.exit_day_offset != null) parts.push(`卖出偏移：${rule.exit_day_offset} 个交易日`);
  if (rule.exit_price) parts.push(`卖出价：${rule.exit_price === 'open' ? '开盘价' : rule.exit_price === 'close' ? '收盘价' : rule.exit_price}`);
  if (Array.isArray(rule.observe_days)) parts.push(`观察窗口：${rule.observe_days.map((day) => day === 0 ? '入场日' : `入场+${day}`).join(' / ')}`);
  if (rule.summary_exit_day_offset != null) parts.push(`汇总口径：入场+${rule.summary_exit_day_offset} ${rule.summary_exit_price === 'close' ? '收盘价' : rule.summary_exit_price || ''}`);
  return parts.join('；') || JSON.stringify(rule);
}

function renderTradeStrategyCards(items = []) {
  const container = qs('#trade-strategy-list');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.textContent = '暂无交易策略';
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.map((item, index) => `
    <button class="trade-strategy-card ${index === 0 ? 'active' : ''}" type="button" data-trade-strategy-id="${escapeHtml(item.strategy_id)}">
      <span class="badge ${item.status === 'active' ? 'status-ok' : 'status-muted'}">${item.is_builtin ? '内置' : '自定义'} · ${escapeHtml(item.status || '-')}</span>
      <strong>${escapeHtml(item.display_name || item.strategy_id)}</strong>
      <small>${escapeHtml(item.description || '')}</small>
    </button>
  `).join('');
  qsa('[data-trade-strategy-id]').forEach((button) => {
    button.addEventListener('click', () => {
      qsa('.trade-strategy-card').forEach((item) => item.classList.remove('active'));
      button.classList.add('active');
      renderTradeStrategyDetail(items.find((item) => item.strategy_id === button.dataset.tradeStrategyId));
    });
  });
  renderTradeStrategyDetail(items[0]);
}

function renderTradeStrategyDetail(item) {
  selectedTradeStrategy = item;
  const title = qs('#trade-strategy-detail-title');
  const detail = qs('#trade-strategy-detail');
  if (!detail) return;
  if (!item) {
    if (title) title.textContent = '策略规则详情';
    detail.textContent = '请选择一个交易策略';
    return;
  }
  if (title) title.textContent = item.display_name || item.strategy_id;
  detail.innerHTML = `
    <div><strong>策略 ID</strong></div>
    <div>${escapeHtml(item.strategy_id || '-')}</div>
    <div><strong>版本</strong></div>
    <div>${escapeHtml(item.version || '-')}</div>
    <div><strong>状态</strong></div>
    <div>${escapeHtml(item.status || '-')}</div>
    <div><strong>买入规则</strong></div>
    <div>${escapeHtml(ruleSummary(item.buy_rule || {}))}</div>
    <div><strong>卖出规则</strong></div>
    <div>${escapeHtml(ruleSummary(item.sell_rule || {}))}</div>
    <div><strong>成本默认</strong></div>
    <div>${item.cost_rule?.enabled ? '开启' : '默认关闭'}</div>
    <div><strong>成交约束</strong></div>
    <div>${item.execution_rule?.enabled ? '开启' : '默认关闭'}</div>
  `;
}

async function loadTradeStrategies() {
  try {
    const data = await fetchJson('/api/trade-strategies');
    renderTradeStrategyCards(data.items || []);
  } catch (error) {
    renderError(qs('#trade-strategy-list'), `交易策略加载失败：${error.message}`);
  }
}

document.addEventListener('DOMContentLoaded', loadTradeStrategies);
