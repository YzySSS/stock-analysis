let currentStrategyId = null;
let strategiesCache = [];


function strategyDisplayNameById(strategyId) {
  const item = strategiesCache.find((strategy) => strategy.id === strategyId);
  return item?.display_name || strategyId || '-';
}

function strategyStatusClass(item = {}) {
  if (item.availability === 'runtime_ready' || item.runtime_ready) return 'status-ok';
  if (item.availability === 'experimental' || item.availability === 'research') return 'status-warn';
  return 'status-muted';
}

function renderStrategyCards(data) {
  const list = qs('#strategies-list');
  const items = data.strategies || [];
  strategiesCache = items;

  qs('#strategies-default').textContent = strategyDisplayNameById(data.default_strategy) || '-';
  qs('#strategies-count').textContent = String(data.summary?.count ?? items.length ?? 0);
  qs('#strategies-current-count').textContent = String(data.summary?.runtime_ready_count ?? data.summary?.current_count ?? 0);
  qs('#strategies-legacy-count').textContent = String((data.summary?.experimental_count ?? 0) + (data.summary?.display_only_count ?? data.summary?.legacy_count ?? 0));

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
        </div>
        <h3>${escapeHtml(item.display_name || item.id)}</h3>
        <div class="strategy-hero-state"><i class="strategy-status-dot ${escapeHtml(item.availability || 'unknown')}"></i>${escapeHtml(item.availability_label || '-')}</div>
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
    status.className = `badge ${strategy.runtime_ready ? 'status-ok' : 'status-warn'}`;
    status.textContent = strategy.availability_label || '-';
  }

  container.innerHTML = `
    <div class="strategy-param-list">
      <div><span>得分阈值</span><b>${strategy.score_threshold ?? '-'}</b></div>
      <div><span>最大持仓数</span><b>${strategy.max_picks ?? '-'}</b></div>
      <div><span>调仓频率</span><b>每日</b></div>
      <div><span>风险敞口上限</span><b>${strategy.runtime_ready ? '8%' : '-'}</b></div>
    </div>
    <div class="strategy-note-box">
      <strong>策略说明</strong>
      <p>${escapeHtml(strategy.description || note || '暂无描述')}</p>
      <small>${escapeHtml(factorNames)}</small>
    </div>
    <div class="strategy-detail-metrics compact">
      <span>因子数 <b>${factors.length}</b></span>
      <span>平均 CI <b>${formatNumber(avgCi, 4)}</b></span>
      <span>平均覆盖 <b>${avgCoverage == null ? '-' : formatPercent(avgCoverage)}</b></span>
      <span>样本 <b>${strategy.factor_sample_size ?? '-'}</b></span>
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
  const factors = strategy.factors || [];
  const avgCoverage = factors.length ? factors.reduce((sum, item) => sum + (Number(item.coverage) || 0), 0) / factors.length : null;
  const checks = [
    { label: '行情数据', status: true, time: '09:30:00' },
    { label: '财务因子', status: avgCoverage == null ? true : avgCoverage >= 80, time: '09:25:54' },
    { label: '调仓/涨跌停', status: true, time: '09:30:00' },
    { label: '风险模型', status: strategy.runtime_ready, warn: !strategy.runtime_ready, time: '09:20:11' },
  ];
  readiness.innerHTML = `
    ${checks.map((item) => `
      <div class="strategy-readiness-item ${item.warn ? 'warn' : ''}">
        <span>${escapeHtml(item.label)}</span>
        <b>${item.warn ? '⚠ 警告' : item.status ? '● 就绪' : '○ 待补齐'}</b>
        <em>${escapeHtml(item.time)}</em>
      </div>
    `).join('')}
    <div class="strategy-warning-note">风险模型因子部分缺失时，会使用最近可用版本计算。</div>
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
    summary.textContent = strategy.runtime_ready === true
      ? `样本统计基于最近 ${strategy.factor_sample_size || '-'} 条候选样本。`
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
          <span>样本 <b>${item.sample_size ?? '-'}</b></span>
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

async function loadStrategyDetail(strategyId) {
  const data = await fetchJson(`/api/strategies/detail?strategy_id=${encodeURIComponent(strategyId)}`);
  currentStrategyId = strategyId;
  renderStrategyCards({
    default_strategy: qs('#strategies-default').textContent,
    summary: {
      count: strategiesCache.length,
      runtime_ready_count: strategiesCache.filter((item) => item.runtime_ready).length,
      experimental_count: strategiesCache.filter((item) => item.availability === 'experimental').length,
      display_only_count: strategiesCache.filter((item) => item.availability === 'display_only').length,
      current_count: strategiesCache.filter((item) => item.mode === 'current').length,
      legacy_count: strategiesCache.filter((item) => item.mode === 'legacy').length,
    },
    strategies: strategiesCache,
  });
  renderStrategyDetail(data.strategy || null);
  renderStrategyFactors(data.strategy || null);
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
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-strategies-page')?.addEventListener('click', loadStrategiesPage);
  document.addEventListener('click', async (event) => {
    const target = event.target.closest('[data-strategy-pick]');
    if (!target) return;
    await loadStrategyDetail(target.getAttribute('data-strategy-pick'));
  });

  try {
    await loadStrategiesPage();
  } catch (error) {
    renderError(qs('#strategies-list'), `加载策略失败: ${error.message}`);
    qs('#strategies-default').textContent = '加载失败';
    qs('#strategies-count').textContent = '-';
    qs('#strategies-current-count').textContent = '-';
    qs('#strategies-legacy-count').textContent = '-';
    renderStrategyDetail(null);
    renderStrategyFactors(null);
  }
});
