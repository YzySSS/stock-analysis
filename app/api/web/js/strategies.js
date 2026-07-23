let currentStrategyId = null;
let strategiesCache = [];
let strategyLoadToken = 0;


function strategyDisplayNameById(strategyId) {
  const item = strategiesCache.find((strategy) => strategy.id === strategyId);
  return item?.display_name || strategyId || '-';
}

function strategyStatusClass(item = {}) {
  if (item.evidence_status === 'historical_diagnostic_fail') return 'status-error';
  if (item.availability === 'runtime_ready' || item.runtime_ready) return 'status-ok';
  if (item.availability === 'prototype' || item.availability === 'data_not_ready' || item.availability === 'research') return 'status-warn';
  return 'status-muted';
}

function capabilityBadge(ok, yesLabel, noLabel) {
  return `<span class="badge ${ok ? 'status-ok' : 'status-muted'}">${escapeHtml(ok ? yesLabel : noLabel)}</span>`;
}

function renderStrategyCards(data) {
  const list = qs('#strategies-list');
  const items = data.strategies || [];
  strategiesCache = items;

  qs('#strategies-default').textContent = data.default_strategy
    ? strategyDisplayNameById(data.default_strategy)
    : '未设置（需主动选择）';
  qs('#strategies-count').textContent = String(data.summary?.count ?? items.length ?? 0);
  qs('#strategies-loadable-count').textContent = String(data.summary?.loadable_count ?? items.filter((item) => item.loadable).length);
  qs('#strategies-data-ready-count').textContent = String(data.summary?.data_ready_count ?? items.filter((item) => item.data_ready).length);
  qs('#strategies-current-count').textContent = String(data.summary?.runtime_ready_count ?? data.summary?.current_count ?? 0);
  qs('#strategies-backtest-count').textContent = String(data.summary?.backtest_ready_count ?? items.filter((item) => item.backtest_ready).length);
  qs('#strategies-validated-count').textContent = String(data.summary?.validated_count ?? items.filter((item) => item.validated).length);

  if (!items.length) {
    list.innerHTML = '<div class="empty-state">暂无策略数据</div>';
    return;
  }

  list.classList.remove('empty-state');
  list.innerHTML = items.map((item) => {
    const availabilityClass = strategyStatusClass(item);
    return `
      <article class="strategy-hero-card ${item.id === currentStrategyId ? 'selected' : ''}" data-strategy-card="${escapeHtml(item.id)}" data-strategy-pick="${escapeHtml(item.id)}">
        <div class="strategy-hero-status-row">
          <span class="strategy-id-chip">${escapeHtml(item.display_name || item.id)}</span>
          ${item.is_default ? '<span class="badge status-ok">默认</span>' : `<span class="badge ${availabilityClass}">${escapeHtml(item.availability_label || '-')}</span>`}
          ${item.evidence_status === 'historical_diagnostic_fail' ? '<span class="badge status-error">历史诊断未通过</span>' : ''}
        </div>
        <h3>${escapeHtml(item.display_name || item.id)}</h3>
        <div class="strategy-hero-state"><i class="strategy-status-dot ${escapeHtml(item.availability || 'unknown')}"></i>${escapeHtml(item.availability_label || '-')}</div>
        <div class="strategy-hero-status-row">
          ${capabilityBadge(item.loadable, '可加载', '不可加载')}
          ${capabilityBadge(item.data_ready, '数据就绪', '数据未就绪')}
          ${capabilityBadge(item.backtest_ready, '研究回测', '回测关闭')}
          ${capabilityBadge(item.validated, '已验证', '未验证')}
        </div>
        <dl>
          <div><dt>版本</dt><dd>v${escapeHtml(item.version || '-')}</dd></div>
          <div><dt>模式</dt><dd>${escapeHtml(item.mode || 'current')}</dd></div>
          <div><dt>状态</dt><dd>${escapeHtml(item.status || '-')}</dd></div>
        </dl>
        <div class="strategy-card-note">${escapeHtml(item.availability_note || item.description || '策略已注册')}</div>
      </article>
    `;
  }).join('');
}

function renderStrategyDetail(strategy = null) {
  const container = qs('#strategy-detail');
  const name = qs('#strategy-detail-name');
  const status = qs('#strategy-detail-status');
  const readiness = qs('#strategy-readiness-list');

  if (!strategy) {
    if (name) name.textContent = '请选择策略';
    if (status) {
      status.className = 'badge status-muted';
      status.textContent = '未选择';
    }
    container.innerHTML = '<div class="empty-state">暂无策略详情</div>';
    if (readiness) readiness.innerHTML = '<div class="empty-state">请选择策略</div>';
    return;
  }

  const factors = strategy.factors || [];
  const factorNames = factors.map((item) => item.name || item.key).filter(Boolean).join(' / ') || '暂无';
  const avgCi = factors.length ? factors.reduce((sum, item) => sum + (Number(item.ci) || 0), 0) / factors.length : null;
  const avgCoverage = factors.length ? factors.reduce((sum, item) => sum + (Number(item.coverage) || 0), 0) / factors.length : null;
  const note = strategy.availability_note || (strategy.executable === false
    ? '当前仅纳入页面展示，尚未接入现有执行链路。'
    : '当前已接入现有策略执行 / 因子统计链路。');

  if (name) name.textContent = strategy.display_name || strategy.id || '-';
  if (status) {
    status.className = `badge ${strategy.evidence_status === 'historical_diagnostic_fail' ? 'status-error' : strategy.runtime_ready ? 'status-ok' : 'status-warn'}`;
    status.textContent = strategy.availability_label || '-';
  }

  container.innerHTML = `
    <div class="strategy-param-list">
      <div><span>得分阈值</span><b>${strategy.score_threshold ?? '-'}</b></div>
      <div><span>最大持仓数</span><b>${strategy.max_picks ?? '-'}</b></div>
      <div><span>调仓频率</span><b>每日</b></div>
      <div><span>实时状态</span><b>${escapeHtml(strategy.runtime_status || '-')}</b></div>
      <div><span>回测状态</span><b>${escapeHtml(strategy.backtest_status || '-')}</b></div>
      <div><span>验证状态</span><b>${escapeHtml(strategy.validation_status || '-')}</b></div>
      <div><span>证据状态</span><b>${escapeHtml(strategy.evidence_status || '-')}</b></div>
    </div>
    <div class="strategy-note-box">
      <strong>策略说明</strong>
      <p>${escapeHtml(strategy.description || note || '暂无描述')}</p>
      ${strategy.evidence_note ? `<p>${escapeHtml(strategy.evidence_note)}</p>` : ''}
      <small>${escapeHtml(factorNames)}</small>
    </div>
    <div class="strategy-detail-metrics compact">
      <span>因子数 <b>${factors.length}</b></span>
      <span>平均 CI <b>${formatNumber(avgCi, 4)}</b></span>
      <span>平均覆盖 <b>${avgCoverage == null ? '-' : formatPercent(avgCoverage)}</b></span>
      <span>样本 <b>${strategy.factor_sample_size ?? '-'}</b></span>
      <span>统计日 <b>${escapeHtml(strategy.factor_ci_date || '-')}</b></span>
    </div>
  `;

  renderStrategyReadiness(strategy);
}

function renderStrategyReadiness(strategy = null) {
  const readiness = qs('#strategy-readiness-list');
  if (!readiness) return;
  if (!strategy) {
    readiness.innerHTML = '<div class="empty-state">请选择策略</div>';
    return;
  }
  const datasetChecks = (strategy.dataset_statuses || []).map((item) => ({
    label: item.name,
    status: Boolean(item.ready),
    detail: `${item.coverage == null ? (item.row_count == null ? '-' : `${item.row_count} 条`) : formatPercent(item.coverage)} · ${item.latest_at || '-'}`,
  }));
  const historicalDiagnosticFailed = strategy.evidence_status === 'historical_diagnostic_fail';
  const checks = [
    { label: '策略代码可加载', status: Boolean(strategy.loadable), detail: strategy.load_error || '' },
    { label: '标的类型兼容', status: Boolean(strategy.instrument_compatible), detail: (strategy.supported_instrument_types || []).join(' / ') || '-' },
    ...datasetChecks,
    { label: '实时选股可执行', status: Boolean(strategy.runtime_ready), detail: (strategy.runtime_reasons || [])[0] || strategy.runtime_status || '-' },
    { label: '研究回测可执行', status: Boolean(strategy.backtest_ready), detail: (strategy.backtest_reasons || [])[0] || strategy.backtest_status || '-' },
    {
      label: '冻结历史诊断',
      status: strategy.evidence_status === 'historical_diagnostic_pass',
      detail: strategy.evidence_note || strategy.evidence_status || '-',
      resultLabel: historicalDiagnosticFailed
        ? '● 未通过'
        : strategy.evidence_status === 'historical_diagnostic_pass'
          ? '● 通过'
          : '○ 未执行',
      resultClass: historicalDiagnosticFailed ? 'down' : '',
    },
    { label: '交易有效性验证', status: Boolean(strategy.validated), detail: strategy.validation_status || '-' },
  ];
  const reasons = strategy.readiness_reasons || strategy.runtime_reasons || [];
  readiness.innerHTML = `
    ${checks.map((item) => `
      <div class="strategy-readiness-item ${item.status ? '' : 'warn'}">
        <span>${escapeHtml(item.label)}</span>
        <b class="${escapeHtml(item.resultClass || '')}">${escapeHtml(item.resultLabel || (item.status ? '● 就绪' : '○ 未就绪'))}</b>
        <em>${escapeHtml(item.time || item.detail || '-')}</em>
      </div>
    `).join('')}
    <div class="strategy-warning-note">${escapeHtml(historicalDiagnosticFailed ? strategy.evidence_note : reasons[0] || strategy.evidence_note || '各能力状态均来自注册表声明与当前数据快照。')}</div>
  `;
}

function renderStrategyFactors(strategy = null) {
  const body = qs('#strategy-factors-body');
  const summary = qs('#strategy-factor-summary');
  const items = strategy?.factors || [];

  if (!strategy) {
    if (summary) summary.textContent = '请选择策略后查看因子分析';
    renderStrategyFactorCards([]);
    renderStrategyTable([]);
    return;
  }

  if (summary) {
    summary.textContent = strategy.evidence_status === 'historical_diagnostic_fail'
      ? '冻结历史诊断未通过；因子数据仅用于复盘和研究，不代表交易有效性。'
      : strategy.runtime_ready === true
      ? `CI 基于 ${strategy.factor_ci_date || '-'} 的全量候选样本与 T+${strategy.factor_ci_horizon_days || 1} 收盘收益计算。`
      : '当前未接通 V1 执行链路，因子分析以配置说明为主。';
  }

  renderStrategyFactorCards(items);
  renderStrategyTable(strategiesCache);
}

function renderStrategyFactorCards(items = []) {
  const container = qs('#strategy-factor-cards');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无因子配置';
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.map((item) => {
    const weight = item.weight == null ? null : Number(item.weight) <= 1 ? Number(item.weight) * 100 : Number(item.weight);
    const strength = weight == null ? 0 : Math.max(0, Math.min(100, Number(weight)));
    const ci = item.ci == null ? null : Number(item.ci);
    const coverage = item.coverage == null ? null : Number(item.coverage);
    const missing = item.missing_rate == null ? null : Number(item.missing_rate);
    const ciClass = ci == null ? 'neutral' : ci >= 0.03 ? 'strong' : ci < 0 ? 'weak' : 'neutral';
    return `
      <div class="factor-metric-row ${ciClass}">
        <div class="factor-metric-main">
          <span>${escapeHtml(item.name || item.key || '-')}</span>
          <div><i style="width:${strength}%"></i></div>
          <b>${weight == null || Number.isNaN(weight) ? '-' : `${formatNumber(weight, 0)}%`}</b>
        </div>
        <div class="factor-metric-detail">
          <span>CI <b>${ci == null || Number.isNaN(ci) ? '-' : formatNumber(ci, 4)}</b></span>
          <span>覆盖 <b>${coverage == null || Number.isNaN(coverage) ? '-' : formatPercent(coverage)}</b></span>
          <span>缺失 <b>${missing == null || Number.isNaN(missing) ? '-' : formatPercent(missing)}</b></span>
          <span>有效样本 <b>${item.valid_sample_size ?? item.sample_size ?? '-'}</b></span>
        </div>
      </div>
    `;
  }).join('');
}

function renderStrategyTable(items = []) {
  const body = qs('#strategy-factors-body');
  if (!body) return;
  if (!items.length) {
    body.innerHTML = '<tr><td colspan="8" class="empty-state">暂无策略数据</td></tr>';
    return;
  }
  body.innerHTML = items.map((item) => {
    const selected = item.id === currentStrategyId;
    const availabilityClass = strategyStatusClass(item);
    return `
      <tr class="${selected ? 'selected-row' : ''}">
        <td>${escapeHtml(item.id || '-')}</td>
        <td>${escapeHtml(item.display_name || '-')}</td>
        <td><span class="badge ${availabilityClass}">${escapeHtml(item.availability_label || '-')}</span></td>
        <td>v${escapeHtml(item.version || '-')}</td>
        <td><span class="badge ${item.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${escapeHtml(item.mode || '-')}</span></td>
        <td>${item.id === currentStrategyId ? (qs('#strategy-factor-cards')?.querySelectorAll('.factor-metric-row').length || '-') : '-'}</td>
        <td>${item.score_threshold ?? '-'}</td>
        <td><button class="btn btn-secondary" type="button" data-strategy-pick="${escapeHtml(item.id)}">查看</button></td>
      </tr>
    `;
  }).join('');
}

function forwardActionLabel(actionType) {
  return ({ viewed: '看过', saved: '保存', bought: '买入', skipped: '跳过', sold: '卖出' })[actionType] || actionType || '-';
}

function forwardAiLabel(mode) {
  return ({
    progressive_ai: '完整 AI',
    partial_ai: '部分 AI',
    local_fallback: '本地降级',
    disabled: 'AI 关闭',
  })[mode] || mode || '-';
}

function forwardReturnCell(value) {
  const cssClass = getPctClass(value) || '';
  return `<span class="${cssClass}">${formatPercent(value)}</span>`;
}

function renderForwardEvidence(evidence = null) {
  const status = qs('#strategy-forward-status');
  const note = qs('#strategy-forward-note');
  const protocol = qs('#strategy-forward-protocol');
  const summary = qs('#strategy-forward-summary');
  const metricsBody = qs('#strategy-forward-metrics');
  const discipline = qs('#strategy-forward-discipline');
  const picksBody = qs('#strategy-forward-picks-body');
  if (!status || !protocol || !summary || !metricsBody || !discipline || !picksBody) return;

  if (!evidence) {
    status.className = 'badge status-muted';
    status.textContent = '未选择';
    note.textContent = '请选择策略后查看固定协议下的真实前瞻样本。';
    protocol.className = 'strategy-forward-protocol empty-state';
    protocol.textContent = '请选择策略';
    summary.innerHTML = '';
    metricsBody.innerHTML = '<tr><td colspan="5" class="empty-state">暂无前瞻样本</td></tr>';
    discipline.className = 'strategy-forward-discipline empty-state';
    discipline.textContent = '暂无操作记录';
    picksBody.innerHTML = '<tr><td colspan="8" class="empty-state">暂无前瞻候选</td></tr>';
    return;
  }

  if (evidence.status === 'not_configured') {
    status.className = 'badge status-muted';
    status.textContent = '未配置前瞻协议';
    note.textContent = '该策略尚未进入固定版本的前瞻观察，当前仍是未验证状态。';
    protocol.className = 'strategy-forward-protocol empty-state';
    protocol.textContent = '暂无冻结协议；不会用历史回测结果冒充前瞻证据。';
    summary.innerHTML = '';
    metricsBody.innerHTML = '<tr><td colspan="5" class="empty-state">暂无前瞻样本</td></tr>';
    discipline.className = 'strategy-forward-discipline empty-state';
    discipline.textContent = '暂无操作记录';
    picksBody.innerHTML = '<tr><td colspan="8" class="empty-state">暂无前瞻候选</td></tr>';
    return;
  }

  const protocolInfo = evidence.protocol || {};
  const sample = evidence.sample || {};
  const aiModes = evidence.ai_modes || {};
  const actionCounts = evidence.actions || {};
  const userDiscipline = evidence.user_discipline || {};
  const isPreliminary = evidence.status === 'preliminary_ready';
  const isAutomaticObservation = protocolInfo.observation_source === 'automatic_observation';
  const entryRuleLabel = protocolInfo.entry_rule === 'same_day_open'
    ? '当日开盘（09:25 信号）'
    : '次日可交易开盘';
  const sourceLabel = isAutomaticObservation ? '自动 5 日配对观察' : '固定前瞻观察';
  status.className = 'badge status-warn';
  status.textContent = isPreliminary ? '初步样本达标 · 未验证' : '采集中 · 未验证';
  note.textContent = isPreliminary
    ? '已达到预设的初步观察门槛，但这不是交易有效性认证；继续积累样本并观察稳定性。'
    : isAutomaticObservation
      ? '新策略按冻结版本自动收集 5 个交易日的 09:25 配对样本；与用户手动选股及其 14 天统计完全分开。'
      : '按冻结版本持续收集真实盘后信号；成熟收益、零候选日和 AI 降级日都会保留。';

  protocol.className = 'strategy-forward-protocol';
  protocol.innerHTML = `
    <span><b>${escapeHtml(protocolInfo.protocol_id || '-')}</b><small>协议</small></span>
    <span><b>v${escapeHtml(protocolInfo.strategy_version || '-')}</b><small>策略版本</small></span>
    <span><b>${escapeHtml(sourceLabel)}</b><small>样本来源</small></span>
    <span><b>${escapeHtml(protocolInfo.execution_time || '-')}</b><small>每日执行</small></span>
    <span><b>${escapeHtml(entryRuleLabel)}</b><small>入场口径</small></span>
    <span><b>${escapeHtml((protocolInfo.horizons || []).join(' / ') || '-')} 日</b><small>观察窗口</small></span>
    <span><b>${escapeHtml(protocolInfo.started_on || '-')}</b><small>开始日期</small></span>
  `;

  const aiSummary = Object.entries(aiModes)
    .map(([mode, count]) => `${forwardAiLabel(mode)} ${count}`)
    .join(' / ') || '尚无成功运行';
  summary.innerHTML = `
    <article class="stat-card"><div class="stat-label">成功观察日</div><div class="stat-value">${sample.observation_days ?? 0} / ${protocolInfo.minimum_observation_days ?? '-'}</div></article>
    <article class="stat-card"><div class="stat-label">累计候选</div><div class="stat-value">${sample.candidate_count ?? 0} / ${protocolInfo.minimum_candidate_count ?? '-'}</div></article>
    <article class="stat-card"><div class="stat-label">零候选 / 失败日</div><div class="stat-value">${sample.zero_pick_days ?? 0} / ${sample.failed_days ?? 0}</div></article>
    <article class="stat-card"><div class="stat-label">AI 执行口径</div><div class="stat-value strategy-forward-ai-value">${escapeHtml(aiSummary)}</div></article>
  `;

  const metrics = evidence.metrics || {};
  metricsBody.innerHTML = [1, 3, 5, 20].map((horizon) => {
    const item = metrics[String(horizon)] || {};
    const excess = (item.average_excess_return_pct || {})['000300.SH'];
    return `
      <tr>
        <td>T+${horizon}</td>
        <td>${item.sample_size ?? 0}</td>
        <td>${forwardReturnCell(item.average_return_pct)}</td>
        <td>${formatPercent(item.win_rate_pct)}</td>
        <td>${forwardReturnCell(excess)}</td>
      </tr>
    `;
  }).join('');

  const boughtReturns = userDiscipline.bought_forward_returns || {};
  discipline.className = 'strategy-forward-discipline';
  discipline.innerHTML = `
    <div><span>已形成决策</span><b>${userDiscipline.decided_pick_count ?? 0} / ${sample.candidate_count ?? 0}</b><small>决策覆盖 ${formatPercent(userDiscipline.decision_rate_pct)}</small></div>
    <div><span>买入 / 卖出候选</span><b>${userDiscipline.bought_pick_count ?? 0} / ${userDiscipline.sold_pick_count ?? 0}</b><small>仅记录真实操作，不等于策略收益</small></div>
    <div><span>动作流水</span><b>${Object.values(actionCounts).reduce((sum, value) => sum + Number(value || 0), 0)}</b><small>${escapeHtml(Object.entries(actionCounts).map(([key, value]) => `${forwardActionLabel(key)} ${value}`).join(' / ') || '暂无')}</small></div>
    <div><span>已买候选 T+5</span><b>${forwardReturnCell(boughtReturns['5']?.average_return_pct)}</b><small>${boughtReturns['5']?.sample_size ?? 0} 个成熟样本 · 胜率 ${formatPercent(boughtReturns['5']?.win_rate_pct)}</small></div>
  `;

  const picks = evidence.recent_picks || [];
  picksBody.innerHTML = picks.length ? picks.map((item) => {
    const actionBadges = (item.actions || []).map((action) => `<span class="badge status-muted">${escapeHtml(forwardActionLabel(action))}</span>`).join('');
    const buttons = ['viewed', 'saved', 'bought', 'skipped', 'sold'].map((action) => `
      <button class="btn btn-secondary forward-action-btn" type="button"
        data-forward-action="${action}"
        data-observation-id="${escapeHtml(item.observation_id || '')}"
        data-code="${escapeHtml(item.code || '')}"
        data-entry-price="${escapeHtml(item.entry_price ?? '')}">${escapeHtml(forwardActionLabel(action))}</button>
    `).join('');
    return `
      <tr>
        <td>${escapeHtml(item.signal_trade_date || '-')}</td>
        <td><a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || item.code || '-')}</a><small>${escapeHtml(item.code || '')}</small></td>
        <td>${formatNumber(item.score, 2)}<small>${escapeHtml(item.theme_name || '-')}</small></td>
        <td>${forwardReturnCell(item.return_1d_pct)}</td>
        <td>${forwardReturnCell(item.return_3d_pct)}</td>
        <td>${forwardReturnCell(item.return_5d_pct)}</td>
        <td>${forwardReturnCell(item.return_20d_pct)}</td>
        <td><div class="strategy-forward-action-log">${actionBadges || '<span class="muted">未记录</span>'}</div><div class="strategy-forward-action-buttons">${buttons}</div></td>
      </tr>
    `;
  }).join('') : '<tr><td colspan="8" class="empty-state">协议已冻结，等待首个交易日样本</td></tr>';
}

function renderForwardEvidenceError(message) {
  renderForwardEvidence(null);
  const status = qs('#strategy-forward-status');
  const protocol = qs('#strategy-forward-protocol');
  if (status) {
    status.className = 'badge status-error';
    status.textContent = '加载失败';
  }
  if (protocol) renderError(protocol, `前瞻证据加载失败: ${message}`);
}

async function loadForwardEvidence(strategyId, token) {
  const data = await fetchJson(`/api/strategies/forward-evidence?strategy_id=${encodeURIComponent(strategyId)}`);
  if (token !== strategyLoadToken) return;
  renderForwardEvidence(data.forward_evidence || null);
}

async function recordForwardAction(button) {
  const actionType = button.getAttribute('data-forward-action');
  const observationId = button.getAttribute('data-observation-id');
  const code = button.getAttribute('data-code');
  let actionPrice = null;
  if (actionType === 'bought' || actionType === 'sold') {
    const suggested = button.getAttribute('data-entry-price') || '';
    const raw = window.prompt(`记录${forwardActionLabel(actionType)}价格（可留空）`, suggested);
    if (raw === null) return;
    if (raw.trim()) {
      actionPrice = Number(raw);
      if (!Number.isFinite(actionPrice) || actionPrice <= 0) {
        window.alert('价格必须是大于 0 的数字');
        return;
      }
    }
  }
  button.disabled = true;
  const originalText = button.textContent;
  button.textContent = '记录中';
  try {
    await fetchJson('/api/strategies/forward-actions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        observation_id: observationId,
        code,
        action_type: actionType,
        action_price: actionPrice,
      }),
    });
    const token = strategyLoadToken;
    await loadForwardEvidence(currentStrategyId, token);
  } catch (error) {
    window.alert(`记录失败：${error.message}`);
    button.disabled = false;
    button.textContent = originalText;
  }
}

async function loadStrategyDetail(strategyId) {
  const token = ++strategyLoadToken;
  const data = await fetchJson(`/api/strategies/detail?strategy_id=${encodeURIComponent(strategyId)}`);
  if (token !== strategyLoadToken) return;
  currentStrategyId = strategyId;
  qsa('[data-strategy-card]').forEach((card) => {
    card.classList.toggle('selected', card.getAttribute('data-strategy-card') === strategyId);
  });
  renderStrategyDetail(data.strategy || null);
  renderStrategyFactors(data.strategy || null);
  try {
    await loadForwardEvidence(strategyId, token);
  } catch (error) {
    if (token === strategyLoadToken) renderForwardEvidenceError(error.message);
  }
}

async function loadStrategiesPage() {
  const data = await fetchJson('/api/strategies');
  renderStrategyCards(data);
  renderStrategyTable(data.strategies || []);
  const defaultStrategy = currentStrategyId || data.default_strategy || data.strategies?.[0]?.id;
  if (defaultStrategy) {
    await loadStrategyDetail(defaultStrategy);
  } else {
    renderStrategyDetail(null);
    renderStrategyFactors(null);
    renderForwardEvidence(null);
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-strategies-page')?.addEventListener('click', loadStrategiesPage);
  document.addEventListener('click', async (event) => {
    const target = event.target.closest('[data-strategy-pick]');
    if (target) {
      await loadStrategyDetail(target.getAttribute('data-strategy-pick'));
      return;
    }
    const forwardAction = event.target.closest('[data-forward-action]');
    if (forwardAction) await recordForwardAction(forwardAction);
  });

  try {
    await loadStrategiesPage();
  } catch (error) {
    renderError(qs('#strategies-list'), `加载策略失败: ${error.message}`);
    qs('#strategies-default').textContent = '加载失败';
    qs('#strategies-count').textContent = '-';
    qs('#strategies-loadable-count').textContent = '-';
    qs('#strategies-data-ready-count').textContent = '-';
    qs('#strategies-current-count').textContent = '-';
    qs('#strategies-backtest-count').textContent = '-';
    qs('#strategies-validated-count').textContent = '-';
    renderStrategyDetail(null);
    renderStrategyFactors(null);
    renderForwardEvidence(null);
  }
});
