let currentStrategyId = null;
let strategiesCache = [];

function renderStrategyCards(data) {
  const list = qs('#strategies-list');
  const items = data.strategies || [];
  strategiesCache = items;

  qs('#strategies-default').textContent = data.default_strategy || '-';
  qs('#strategies-count').textContent = String(data.summary?.count ?? items.length ?? 0);
  qs('#strategies-current-count').textContent = String(data.summary?.runtime_ready_count ?? data.summary?.current_count ?? 0);
  qs('#strategies-legacy-count').textContent = String((data.summary?.experimental_count ?? 0) + (data.summary?.display_only_count ?? data.summary?.legacy_count ?? 0));

  if (!items.length) {
    list.innerHTML = '<div class="empty-state">暂无策略数据</div>';
    return;
  }

  list.innerHTML = items.map((item) => {
    const availabilityClass = item.availability === 'runtime_ready'
      ? 'status-ok'
      : item.availability === 'experimental'
        ? 'status-warn'
        : 'status-muted';

    return `
      <article class="strategy-card ${item.id === currentStrategyId ? 'selected' : ''}" data-strategy-card="${escapeHtml(item.id)}">
        <div class="strategy-item-head">
          <h3>${escapeHtml(item.display_name || item.id)}</h3>
          <div>
            ${item.is_default ? '<span class="badge status-ok">默认策略</span>' : ''}
            <span class="badge ${item.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${item.mode === 'legacy' ? 'legacy' : 'current'}</span>
            <span class="badge ${availabilityClass}">${escapeHtml(item.availability_label || '-')}</span>
          </div>
        </div>
        <div class="muted">ID: ${escapeHtml(item.id)} · 版本: ${escapeHtml(item.version || '-')} · 状态: ${escapeHtml(item.status || '-')}</div>
        <p>${escapeHtml(item.description || '暂无描述')}</p>
        <div class="muted">标签：${(item.tags || []).map((tag) => escapeHtml(tag)).join(' / ') || '暂无'} · ${escapeHtml(item.availability_note || '暂无状态说明')}</div>
        <div style="margin-top:12px;"><button class="btn btn-secondary" type="button" data-strategy-pick="${escapeHtml(item.id)}">查看因子分析</button></div>
      </article>
    `;
  }).join('');
}

function renderStrategyDetail(strategy = null) {
  const container = qs('#strategy-detail');
  if (!strategy) {
    container.innerHTML = '<div class="empty-state">暂无策略详情</div>';
    return;
  }

  const factorNames = (strategy.factors || []).map((item) => item.name || item.key).filter(Boolean).join(' / ') || '暂无';
  const note = strategy.availability_note || (strategy.executable === false
    ? '当前仅纳入页面展示，尚未接入现有执行链路。'
    : '当前已接入现有策略执行 / 因子统计链路。');

  container.innerHTML = `
    <div class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategy.display_name || strategy.id || '-')}</strong>
        <div>
          <span class="badge ${strategy.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${escapeHtml(strategy.mode || 'current')}</span>
          <span class="badge ${strategy.status === 'active' ? 'status-ok' : 'status-warn'}">${escapeHtml(strategy.status || '-')}</span>
        </div>
      </div>
      <div class="muted">ID: ${escapeHtml(strategy.id || '-')} · 版本: ${escapeHtml(strategy.version || '-')}</div>
      <p>${escapeHtml(strategy.description || '暂无描述')}</p>
      <div class="muted">阈值：${strategy.score_threshold ?? '-'} · 最多入选：${strategy.max_picks ?? '-'} · 可用状态：${escapeHtml(strategy.availability_label || '-')}</div>
      <div class="muted">核心因子：${escapeHtml(factorNames)}</div>
      <div class="muted">说明：${escapeHtml(note)}</div>
    </div>
  `;
}

function renderStrategyFactors(strategy = null) {
  const body = qs('#strategy-factors-body');
  const summary = qs('#strategy-factor-summary');
  const items = strategy?.factors || [];

  if (!strategy) {
    summary.textContent = '请选择策略后查看因子分析';
    body.innerHTML = '<tr><td colspan="9" class="empty-state">请选择策略</td></tr>';
    return;
  }

  summary.textContent = strategy.runtime_ready === true
    ? `当前策略为 ${strategy.display_name || strategy.id}，样本统计基于最近 ${strategy.factor_sample_size || '-'} 条候选样本。`
    : `当前策略为 ${strategy.display_name || strategy.id}，当前未接通 V1 执行链路，因子分析以配置说明为主。`;

  if (!items.length) {
    body.innerHTML = renderEmptyRow(9, '暂无因子配置');
    return;
  }

  body.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.name || item.key || '-')}</td>
      <td>${escapeHtml(item.category || 'general')}</td>
      <td>${escapeHtml(item.direction || 'positive')}</td>
      <td>${formatNumber(item.weight, 2)}</td>
      <td>${formatNumber(item.ci, 4)}</td>
      <td>${item.coverage == null ? '-' : formatPercent(item.coverage)}</td>
      <td>${item.missing_rate == null ? '-' : formatPercent(item.missing_rate)}</td>
      <td><span class="badge ${item.enabled === false ? 'status-warn' : 'status-ok'}">${item.enabled === false ? '关闭' : '启用'}</span></td>
      <td>${escapeHtml(item.description || '')}${item.is_placeholder ? '<div class="muted">当前为配置级占位说明</div>' : ''}</td>
    </tr>
  `).join('');
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
  const defaultStrategy = currentStrategyId || data.default_strategy || data.strategies?.[0]?.id;
  if (defaultStrategy) {
    await loadStrategyDetail(defaultStrategy);
  } else {
    renderStrategyDetail(null);
    renderStrategyFactors(null);
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-strategies-page').addEventListener('click', loadStrategiesPage);
  qs('#strategies-list').addEventListener('click', async (event) => {
    const button = event.target.closest('[data-strategy-pick]');
    if (!button) return;
    await loadStrategyDetail(button.getAttribute('data-strategy-pick'));
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
