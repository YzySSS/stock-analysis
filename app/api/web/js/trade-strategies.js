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
  const rows = [
    ['入场条件', ruleSummary(item.buy_rule || {})],
    ['持有与退出', ruleSummary(item.sell_rule || {})],
    ['仓位管理', '持仓资金与候选数量由回测参数确定；本规则不自动下单或补仓。'],
    ['风险约束', `成本默认${item.cost_rule?.enabled ? '开启' : '关闭'}；成交约束默认${item.execution_rule?.enabled ? '开启' : '关闭'}。可在回测高级设置中显式调整，不直接下单。`],
  ];
  detail.innerHTML = rows.map(([title,text],i) => `<section class="trade-rule-row"><b>${String(i+1).padStart(2,'0')}</b><strong>${title}</strong><p>${escapeHtml(text)}</p></section>`).join('');
  const overview = qs('#trade-parameter-overview');
  if (overview) overview.innerHTML = [
    ['规则类型',item.display_name || item.strategy_id], ['版本',item.version || '-'],
    ['持有/退出偏移',item.sell_rule?.exit_day_offset != null ? `${item.sell_rule.exit_day_offset} 个交易日` : '按规则条件'],
    ['成交成本', item.cost_rule?.enabled ? '默认开启' : '默认关闭'], ['成交约束',item.execution_rule?.enabled ? '默认开启' : '默认关闭'],
  ].map(([label,value])=>`<div class="parameter-line"><span>${escapeHtml(label)}</span><b>${escapeHtml(value)}</b></div>`).join('') + `<details class="research-details"><summary>规则标识与原始参数</summary><p>${escapeHtml(item.strategy_id || '-')} · ${escapeHtml(item.status || '-')}</p><pre>${escapeHtml(JSON.stringify({buy:item.buy_rule,sell:item.sell_rule},null,2))}</pre></details>`;

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
