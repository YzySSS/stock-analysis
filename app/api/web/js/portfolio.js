let portfolioState = {
  positions: [],
  selectedId: null,
  editingId: null,
};

const HOLDING_STRATEGIES = [
  { id: 'short_term', name: '短期' },
  { id: 'swing', name: '波段' },
  { id: 'long_term', name: '长期' },
];

const LEGACY_STRATEGY_MAP = {
  a_share_sentiment: 'short_term',
  leader_tactics: 'short_term',
  lowvol_reversal: 'swing',
  quality_lowvol: 'swing',
};

function money(value, digits = 2) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function price(value) {
  return money(value, 3);
}

function pct(value) {
  return formatPercent(value, 2);
}

function signedClass(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return Number(value) >= 0 ? 'up' : 'down';
}

function actionClass(action) {
  if (['stop_loss', 'reduce'].includes(action)) return 'danger';
  if (['take_profit_1', 'take_profit_2'].includes(action)) return 'success';
  if (action === 'add_watch') return 'info';
  if (action === 'watch') return 'warn';
  return 'neutral';
}

function adviceLevelLabel(level) {
  const labels = {
    critical_exit: '强制止损',
    reduce: '减仓',
    hold_watch: '持有观察',
    add_allowed: '允许加仓',
    no_action: '不操作',
    data_insufficient: '数据不足',
  };
  return labels[level] || '-';
}

function adviceLevelClass(level) {
  if (level === 'critical_exit') return 'danger';
  if (level === 'reduce' || level === 'data_insufficient') return 'warn';
  if (level === 'add_allowed') return 'success';
  if (level === 'hold_watch') return 'info';
  return 'neutral';
}

function alertLevelClass(level) {
  if (level === 'critical') return 'danger';
  if (level === 'warning') return 'warn';
  return 'info';
}

function alertLevelLabel(level) {
  const labels = {
    critical: '必须处理',
    warning: '重点关注',
    info: '纪律提示',
  };
  return labels[level] || '纪律提示';
}

function outcomeLabel(label) {
  const labels = {
    hit: '命中',
    miss: '失效',
    neutral: '观察',
    invalidated: '失效',
    data_insufficient: '数据不足',
  };
  return labels[label] || '待复盘';
}

function outcomeClass(label) {
  if (label === 'hit') return 'success';
  if (label === 'miss' || label === 'invalidated') return 'danger';
  if (label === 'data_insufficient') return 'warn';
  return 'neutral';
}

function setDefaultBuyTime() {
  const input = qs('#portfolio-buy-time');
  if (!input || input.value) return;
  const now = new Date();
  now.setSeconds(0, 0);
  input.value = new Date(now.getTime() - now.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
}

function toDatetimeInput(value) {
  if (!value) return '';
  return String(value).replace(' ', 'T').slice(0, 16);
}

function holdingStrategyValue(value) {
  if (!value) return 'short_term';
  return LEGACY_STRATEGY_MAP[value] || value;
}

function setFormMode(mode, item = null) {
  const isEditing = mode === 'edit' && item;
  portfolioState.editingId = isEditing ? item.id : null;

  const title = qs('#portfolio-form-title');
  const submit = qs('#portfolio-form-submit');
  const reset = qs('#portfolio-form-reset');
  const codeInput = qs('#portfolio-code');
  const form = qs('#portfolio-form');
  const message = qs('#portfolio-form-message');

  if (!isEditing) {
    if (form) form.reset();
    if (codeInput) codeInput.disabled = false;
    if (title) title.textContent = '添加持仓';
    if (submit) submit.textContent = '保存持仓';
    if (reset) reset.textContent = '清空';
    if (message) message.textContent = '';
    setDefaultBuyTime();
    return;
  }

  if (title) title.textContent = '调整持仓';
  if (submit) submit.textContent = '保存调整';
  if (reset) reset.textContent = '取消调整';
  if (message) message.textContent = `正在调整 ${item.name || item.code}`;
  if (codeInput) {
    codeInput.value = item.code || '';
    codeInput.disabled = true;
  }
  if (qs('#portfolio-strategy')) qs('#portfolio-strategy').value = holdingStrategyValue(item.strategy_id);
  if (qs('#portfolio-cost')) qs('#portfolio-cost').value = item.cost_price ?? '';
  if (qs('#portfolio-quantity')) qs('#portfolio-quantity').value = item.quantity ?? '';
  if (qs('#portfolio-buy-time')) qs('#portfolio-buy-time').value = toDatetimeInput(item.buy_datetime);
  if (qs('#portfolio-max-loss')) qs('#portfolio-max-loss').value = item.max_loss_pct ?? 5;
  qs('.portfolio-form-card')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function loadStrategies() {
  const select = qs('#portfolio-strategy');
  if (!select) return;
  const current = holdingStrategyValue(select.value);
  select.innerHTML = HOLDING_STRATEGIES.map((item) => {
    return `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`;
  }).join('');
  select.value = HOLDING_STRATEGIES.some((item) => item.id === current) ? current : 'short_term';
}

function renderSummary(summary = {}) {
  qs('#portfolio-count').textContent = summary.count ?? '-';
  qs('#portfolio-market-value').textContent = money(summary.market_value);
  const pnl = qs('#portfolio-pnl');
  pnl.textContent = money(summary.pnl_amount);
  pnl.className = `stat-value ${signedClass(summary.pnl_amount)}`;
  const totalReturn = qs('#portfolio-return');
  totalReturn.textContent = pct(summary.return_pct);
  totalReturn.className = `stat-value ${signedClass(summary.return_pct)}`;
  qs('#portfolio-risk-count').textContent = summary.risk_count ?? 0;
  qs('#portfolio-take-profit-count').textContent = summary.take_profit_count ?? 0;
  const criticalCount = summary.critical_alert_count || 0;
  const warningCount = summary.warning_alert_count || 0;
  const alertText = criticalCount || warningCount ? `，纪律提醒 ${criticalCount} 个必须处理 / ${warningCount} 个关注` : '';
  qs('#portfolio-summary-text').textContent = `共 ${summary.count || 0} 只持仓，总盈亏 ${money(summary.pnl_amount)} 元，收益率 ${pct(summary.return_pct)}${alertText}`;
}

function renderOutcomeSummary(summary) {
  if (!summary) return '<p class="muted">暂无建议复盘结果，等待后续交易日或后台评价任务。</p>';
  return `
    <div class="portfolio-outcome-summary">
      <span class="portfolio-plan-badge ${outcomeClass(summary.latest_label)}">${escapeHtml(outcomeLabel(summary.latest_label))}</span>
      <strong>${escapeHtml(summary.latest_horizon_days || '-')}日后 ${pct(summary.latest_return_pct)}</strong>
      <span>质量分 ${formatNumber(summary.latest_quality_score, 1)}</span>
      <span>平均 ${formatNumber(summary.avg_quality_score, 1)}</span>
      <span>命中 ${summary.hit_count || 0} / 失效 ${summary.miss_count || 0}</span>
    </div>
  `;
}

function renderOutcomeTable(outcomes = []) {
  if (!outcomes.length) return '<p class="muted">暂无建议复盘结果。</p>';
  return `
    <div class="table-wrap compact-table">
      <table>
        <thead>
          <tr>
            <th>周期</th>
            <th>结果</th>
            <th>收益</th>
            <th>最大浮盈</th>
            <th>最大回撤</th>
            <th>质量分</th>
            <th>区间</th>
          </tr>
        </thead>
        <tbody>
          ${outcomes.map((item) => `
            <tr>
              <td>${escapeHtml(item.horizon_days)}日</td>
              <td><span class="portfolio-plan-badge ${outcomeClass(item.outcome_label)}">${escapeHtml(outcomeLabel(item.outcome_label))}</span></td>
              <td class="${signedClass(item.return_pct)}">${pct(item.return_pct)}</td>
              <td class="${signedClass(item.max_gain_pct)}">${pct(item.max_gain_pct)}</td>
              <td class="${signedClass(item.max_drawdown_pct)}">${pct(item.max_drawdown_pct)}</td>
              <td>${formatNumber(item.quality_score, 1)}</td>
              <td>${escapeHtml(item.evidence?.start_trade_date || item.base_trade_date || '-')} -> ${escapeHtml(item.evidence?.end_trade_date || '-')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderTable(positions = []) {
  const body = qs('#portfolio-table-body');
  if (!body) return;
  if (!positions.length) {
    body.innerHTML = renderEmptyRow(10, '暂无持仓，先在上方添加一只股票。');
    renderDetail(null);
    return;
  }

  body.innerHTML = positions.map((item) => {
    const plan = item.trade_plan || {};
    const alerts = item.discipline_alerts || [];
    const topAlert = alerts[0];
    const tp = (plan.take_profit || []).map((level) => price(level.price)).filter(Boolean).join(' / ') || '-';
    const selected = Number(item.id) === Number(portfolioState.selectedId) ? ' selected-row' : '';
    return `
      <tr class="portfolio-row${selected}" data-position-id="${item.id}">
        <td>
          <strong>${escapeHtml(item.name || item.code)}</strong>
          <div class="muted">${escapeHtml(item.code || '-')}</div>
        </td>
        <td>${escapeHtml(item.strategy_label || item.strategy_id || '-')}</td>
        <td>${price(item.cost_price)}</td>
        <td>
          <strong>${price(item.market?.quote?.latest_price)}</strong>
          <div class="${signedClass(item.market?.quote?.pct_chg)}">${pct(item.market?.quote?.pct_chg)}</div>
        </td>
        <td>${money(item.quantity, 0)}</td>
        <td>
          <strong class="${signedClass(item.pnl_amount)}">${money(item.pnl_amount)}</strong>
          <div class="${signedClass(item.return_pct)}">${pct(item.return_pct)}</div>
        </td>
        <td>
          <span class="portfolio-plan-badge ${actionClass(plan.action)}">${escapeHtml(plan.action_label || '-')}</span>
          ${topAlert ? `<div class="portfolio-row-alert ${alertLevelClass(topAlert.level)}">${escapeHtml(topAlert.title)}</div>` : ''}
        </td>
        <td>${price(plan.stop_loss?.price)}</td>
        <td>${escapeHtml(tp)}</td>
        <td>
          <button class="btn btn-secondary btn-small" type="button" data-view-position="${item.id}">查看</button>
          <button class="btn btn-secondary btn-small" type="button" data-edit-position="${item.id}">调整</button>
          <button class="btn btn-danger btn-small" type="button" data-delete-position="${item.id}">删除</button>
        </td>
      </tr>
    `;
  }).join('');
}

function renderDetail(item) {
  const title = qs('#portfolio-detail-title');
  const subtitle = qs('#portfolio-detail-subtitle');
  const badge = qs('#portfolio-detail-action');
  const container = qs('#portfolio-detail');
  if (!container || !title || !subtitle || !badge) return;
  if (!item) {
    title.textContent = '持仓诊断';
    subtitle.textContent = '选择一只持仓查看买卖点和建议。';
    badge.textContent = '-';
    badge.className = 'portfolio-action-badge';
    container.className = 'portfolio-detail empty-state';
    container.innerHTML = '暂无持仓';
    return;
  }

  const plan = item.trade_plan || {};
  const market = item.market || {};
  const quote = market.quote || {};
  const tech = market.technical || {};
  const sentiment = market.sentiment || {};
  const moneyflow = market.moneyflow || {};
  const chip = market.chip || {};
  const ai = item.ai_review || {};
  const outcomeSummary = ai.outcome_summary;
  const alerts = item.discipline_alerts || [];
  const tp = plan.take_profit || [];
  const entry = plan.entry_zone;
  const supportLevels = plan.support_levels || [];
  const resistanceLevels = plan.resistance_levels || [];
  const aiSummary = ai.summary || ai.operation_plan || (ai.status && ai.status !== 'ok' ? ai.message : '') || '暂无 AI 持仓建议，先参考本地规则摘要。';
  const aiMeta = ai.status === 'ok'
    ? `${ai.model ? `模型 ${ai.model}` : 'AI建议'} · 有效至 ${ai.expires_at || '-'}`
    : (ai.message || 'AI建议未生成');

  title.textContent = `${item.name || item.code} 持仓诊断`;
  subtitle.textContent = `${item.code} · ${item.strategy_label || item.strategy_id} · 买入 ${item.buy_datetime || '-'}`;
  badge.textContent = plan.action_label || '-';
  badge.className = `portfolio-action-badge ${actionClass(plan.action)}`;
  container.className = 'portfolio-detail';
  container.innerHTML = `
    <div class="portfolio-detail-grid">
      <div><span>成本价</span><strong>${price(item.cost_price)}</strong></div>
      <div><span>当前价</span><strong>${price(quote.latest_price)}</strong></div>
      <div><span>持仓盈亏</span><strong class="${signedClass(item.pnl_amount)}">${money(item.pnl_amount)}</strong></div>
      <div><span>收益率</span><strong class="${signedClass(item.return_pct)}">${pct(item.return_pct)}</strong></div>
    </div>

    <div class="portfolio-plan-grid">
      <article>
        <span>建议买入/加仓区</span>
        <strong>${entry ? `${price(entry.low)} - ${price(entry.high)}` : '-'}</strong>
      </article>
      <article>
        <span>止损价</span>
        <strong class="down">${price(plan.stop_loss?.price)}</strong>
      </article>
      <article>
        <span>关键支撑</span>
        <strong>${supportLevels.length ? supportLevels.slice(0, 2).map((level) => price(level.price)).join(' / ') : '-'}</strong>
      </article>
      <article>
        <span>关键压力</span>
        <strong>${resistanceLevels.length ? resistanceLevels.slice(0, 2).map((level) => price(level.price)).join(' / ') : '-'}</strong>
      </article>
      <article>
        <span>止盈1</span>
        <strong class="up">${price(tp[0]?.price)}</strong>
        <small>${escapeHtml(tp[0]?.suggestion || '')}</small>
      </article>
      <article>
        <span>止盈2</span>
        <strong class="up">${price(tp[1]?.price)}</strong>
        <small>${escapeHtml(tp[1]?.suggestion || '')}</small>
      </article>
    </div>

    <div class="portfolio-indicator-grid">
      <div><span>MA5</span><b>${price(tech.ma5)}</b></div>
      <div><span>MA10</span><b>${price(tech.ma10)}</b></div>
      <div><span>MA20</span><b>${price(tech.ma20)}</b></div>
      <div><span>ATR14</span><b>${price(tech.atr14)}</b></div>
      <div><span>舆情分</span><b>${formatNumber(sentiment.sentiment_score, 1)}</b></div>
      <div><span>资金净额</span><b>${money(moneyflow.net_amount, 0)}</b></div>
      <div><span>筹码胜率</span><b>${formatNumber(chip.winner_rate, 2)}%</b></div>
      <div><span>筹码中枢</span><b>${price(chip.cost_50pct || chip.weight_avg)}</b></div>
    </div>

    <div class="portfolio-discipline-block ${alerts.some((alert) => alert.level === 'critical') ? 'danger' : alerts.length ? 'warn' : ''}">
      <h3>纪律提醒</h3>
      ${renderAlertList(alerts)}
    </div>

    <div class="portfolio-advice-block">
      <h3>持仓建议</h3>
      <p><span class="portfolio-plan-badge ${adviceLevelClass(ai.decision_level)}">${escapeHtml(adviceLevelLabel(ai.decision_level))}</span></p>
      <p class="muted">${escapeHtml(aiSummary)}</p>
      <h3>建议复盘</h3>
      ${renderOutcomeSummary(outcomeSummary)}
      <div class="portfolio-advice-actions">
        <span>${escapeHtml(aiMeta)}</span>
        <button class="btn btn-secondary btn-small" type="button" data-refresh-advice="${item.id}">刷新分析</button>
        <button class="btn btn-secondary btn-small" type="button" data-advice-detail="${item.id}">查看详情</button>
      </div>
    </div>
  `;
}

function adviceList(items = []) {
  if (!items.length) return '<p class="muted">暂无</p>';
  return `<ul>${items.map((itemText) => `<li>${escapeHtml(itemText)}</li>`).join('')}</ul>`;
}

function renderAlertList(alerts = []) {
  if (!alerts.length) return '<p class="muted">当前没有触发纪律提醒。</p>';
  return `
    <ul class="portfolio-alert-list">
      ${alerts.map((alert) => `
        <li class="${alertLevelClass(alert.level)}">
          <span>${escapeHtml(alertLevelLabel(alert.level))}</span>
          <strong>${escapeHtml(alert.title || '-')}</strong>
          <p>${escapeHtml(alert.detail || '')}</p>
        </li>
      `).join('')}
    </ul>
  `;
}

function levelList(title, levels = []) {
  if (!levels.length) return '';
  return `
    <h3>${escapeHtml(title)}</h3>
    <ul>
      ${levels.map((level) => `<li>${price(level.price)}：${escapeHtml(level.reason || '')}</li>`).join('')}
    </ul>
  `;
}

function closeAdviceModal() {
  qs('#portfolio-advice-modal')?.remove();
}

function showAdviceModal(id) {
  const item = portfolioState.positions.find((position) => Number(position.id) === Number(id));
  if (!item) return;
  closeAdviceModal();
  const plan = item.trade_plan || {};
  const ai = item.ai_review || {};
  const alerts = item.discipline_alerts || [];
  const market = item.market || {};
  const quote = market.quote || {};
  const tech = market.technical || {};
  const chip = market.chip || {};
  const moneyflow = market.moneyflow || {};
  const supportLevels = plan.support_levels || [];
  const resistanceLevels = plan.resistance_levels || [];
  const tp = plan.take_profit || [];
  const shell = document.createElement('div');
  shell.id = 'portfolio-advice-modal';
  shell.className = 'portfolio-advice-modal';
  shell.innerHTML = `
    <div class="portfolio-advice-backdrop" data-close-advice="1"></div>
    <section class="portfolio-advice-panel" role="dialog" aria-modal="true" aria-label="持仓建议详情">
      <header>
        <div>
          <p class="card-kicker">DeepSeek Holding Review</p>
          <h2>${escapeHtml(item.name || item.code)} 持仓建议详情</h2>
          <p class="muted">${escapeHtml(item.code || '-')} · ${escapeHtml(item.strategy_label || item.strategy_id || '-')} · ${escapeHtml(ai.model ? `模型 ${ai.model}` : '本地规则')} · ${escapeHtml(ai.expires_at ? `有效至 ${ai.expires_at}` : '')}</p>
        </div>
        <button class="btn btn-secondary btn-small" type="button" data-close-advice="1">关闭</button>
      </header>
      <div class="portfolio-advice-modal-grid">
        <div><span>持仓成本</span><strong>${price(item.cost_price)}</strong></div>
        <div><span>实时价格</span><strong>${price(quote.latest_price)}</strong></div>
        <div><span>收益率</span><strong class="${signedClass(item.return_pct)}">${pct(item.return_pct)}</strong></div>
        <div><span>当前建议</span><strong>${escapeHtml(plan.action_label || '-')}</strong></div>
        <div><span>MA5 / MA10 / MA20</span><strong>${price(tech.ma5)} / ${price(tech.ma10)} / ${price(tech.ma20)}</strong></div>
        <div><span>ATR14</span><strong>${price(tech.atr14)}</strong></div>
        <div><span>筹码胜率</span><strong>${formatNumber(chip.winner_rate, 2)}%</strong></div>
        <div><span>资金净额</span><strong>${money(moneyflow.net_amount, 0)}</strong></div>
        <div><span>AI分级</span><strong>${escapeHtml(adviceLevelLabel(ai.decision_level))}</strong></div>
      </div>
      <article>
        <h3>纪律提醒</h3>
        ${renderAlertList(alerts)}
      </article>
      <article>
        <h3>DeepSeek 摘要</h3>
        <p>${escapeHtml(ai.summary || ai.operation_plan || ai.message || '暂无 DeepSeek 分析')}</p>
        ${adviceList(ai.analysis || [])}
        ${ai.operation_plan ? `<h3>操作计划</h3><p>${escapeHtml(ai.operation_plan)}</p>` : ''}
      </article>
      <article>
        <h3>建议复盘</h3>
        ${renderOutcomeTable(ai.outcomes || [])}
      </article>
      <article>
        ${levelList('关键支撑', supportLevels)}
        ${levelList('关键压力', resistanceLevels)}
        <h3>止损 / 止盈</h3>
        <ul>
          <li>止损 ${price(plan.stop_loss?.price)}：${escapeHtml(plan.stop_loss?.reason || '')}</li>
          ${tp.map((level) => `<li>止盈${escapeHtml(level.level)} ${price(level.price)}：${escapeHtml(level.suggestion || '')}</li>`).join('')}
        </ul>
      </article>
      <article>
        <h3>本地规则依据</h3>
        ${adviceList(plan.reason || [])}
        ${(plan.risk_flags || []).length ? `<h3>风险提示</h3>${adviceList(plan.risk_flags)}` : ''}
        ${(ai.risks || []).length ? `<h3>DeepSeek 风险</h3>${adviceList(ai.risks)}` : ''}
        <h3>失效条件</h3>
        ${adviceList(plan.invalid_conditions || [])}
      </article>
    </section>
  `;
  document.body.appendChild(shell);
}

function selectPosition(id) {
  portfolioState.selectedId = id;
  const item = portfolioState.positions.find((position) => Number(position.id) === Number(id)) || portfolioState.positions[0] || null;
  if (item) portfolioState.selectedId = item.id;
  renderTable(portfolioState.positions);
  renderDetail(item);
}

async function loadPortfolio() {
  const body = qs('#portfolio-table-body');
  if (body) body.innerHTML = renderEmptyRow(10, '加载中...');
  try {
    const data = await fetchJson('/api/portfolio');
    portfolioState.positions = data.positions || [];
    renderSummary(data.summary || {});
    if (!portfolioState.selectedId && portfolioState.positions.length) {
      portfolioState.selectedId = portfolioState.positions[0].id;
    }
    renderTable(portfolioState.positions);
    selectPosition(portfolioState.selectedId);
  } catch (error) {
    renderError(body, `持仓加载失败：${error.message}`);
  }
}

async function refreshAdvice(id) {
  const item = portfolioState.positions.find((position) => Number(position.id) === Number(id));
  const label = item?.name || item?.code || id;
  try {
    await fetchJson(`/api/portfolio/${id}/advice/refresh?force=true`, { method: 'POST' });
    const selected = portfolioState.positions.find((position) => Number(position.id) === Number(id));
    if (selected) {
      selected.ai_review = {
        ...(selected.ai_review || {}),
        status: 'queued',
        message: 'AI 持仓建议已提交后台生成，稍后刷新查看。',
      };
      renderDetail(selected);
    }
    setTimeout(loadPortfolio, 2500);
  } catch (error) {
    alert(`${label} 刷新分析失败：${error.message}`);
  }
}

async function savePosition(event) {
  event.preventDefault();
  const message = qs('#portfolio-form-message');
  const isEditing = Boolean(portfolioState.editingId);
  const payload = {
    strategy_id: qs('#portfolio-strategy')?.value || 'short_term',
    cost_price: Number(qs('#portfolio-cost')?.value || 0),
    quantity: Number(qs('#portfolio-quantity')?.value || 0),
    buy_datetime: qs('#portfolio-buy-time')?.value || null,
    max_loss_pct: Number(qs('#portfolio-max-loss')?.value || 5),
  };
  if (!isEditing) payload.code = qs('#portfolio-code')?.value.trim();
  if (message) message.textContent = isEditing ? '正在保存调整...' : '正在保存...';
  try {
    const data = await fetchJson(isEditing ? `/api/portfolio/${portfolioState.editingId}` : '/api/portfolio', {
      method: isEditing ? 'PATCH' : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    setFormMode('add');
    if (message) message.textContent = isEditing ? `已更新 ${data.position?.name || data.position?.code || ''}` : `已保存 ${data.position?.name || payload.code}`;
    portfolioState.selectedId = data.position?.id || null;
    await loadPortfolio();
  } catch (error) {
    if (message) message.textContent = `${isEditing ? '调整' : '保存'}失败：${error.message}`;
  }
}

function editPosition(id) {
  const item = portfolioState.positions.find((position) => Number(position.id) === Number(id));
  if (!item) return;
  selectPosition(id);
  setFormMode('edit', item);
}

async function deletePosition(id) {
  if (!window.confirm('确认删除这条持仓记录吗？')) return;
  await fetchJson(`/api/portfolio/${id}`, { method: 'DELETE' });
  if (Number(portfolioState.selectedId) === Number(id)) portfolioState.selectedId = null;
  if (Number(portfolioState.editingId) === Number(id)) setFormMode('add');
  await loadPortfolio();
}

function bindPortfolioEvents() {
  qs('#portfolio-form')?.addEventListener('submit', savePosition);
  qs('#portfolio-form-reset')?.addEventListener('click', () => {
    setFormMode('add');
  });
  qs('#refresh-portfolio')?.addEventListener('click', loadPortfolio);
  qs('#portfolio-refresh-table')?.addEventListener('click', loadPortfolio);
  document.addEventListener('click', async (event) => {
    const viewButton = event.target.closest('[data-view-position]');
    if (viewButton) {
      selectPosition(viewButton.dataset.viewPosition);
      return;
    }
    const editButton = event.target.closest('[data-edit-position]');
    if (editButton) {
      editPosition(editButton.dataset.editPosition);
      return;
    }
    const adviceButton = event.target.closest('[data-advice-detail]');
    if (adviceButton) {
      showAdviceModal(adviceButton.dataset.adviceDetail);
      return;
    }
    const refreshAdviceButton = event.target.closest('[data-refresh-advice]');
    if (refreshAdviceButton) {
      await refreshAdvice(refreshAdviceButton.dataset.refreshAdvice);
      return;
    }
    const closeAdviceButton = event.target.closest('[data-close-advice]');
    if (closeAdviceButton) {
      closeAdviceModal();
      return;
    }
    const deleteButton = event.target.closest('[data-delete-position]');
    if (deleteButton) {
      await deletePosition(deleteButton.dataset.deletePosition);
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeAdviceModal();
  });
}

document.addEventListener('DOMContentLoaded', async () => {
  setDefaultBuyTime();
  bindPortfolioEvents();
  await loadStrategies();
  await loadPortfolio();
});
