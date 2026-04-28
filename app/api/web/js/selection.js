let currentDefaultStrategy = null;
let lastSelectionResponse = null;

function compareSelectionItems(sortBy, a, b) {
  if (sortBy === 'score_desc') return (Number(b.score ?? -999) - Number(a.score ?? -999));
  if (sortBy === 'change_desc') return (Number(b.price_change_pct ?? -999) - Number(a.price_change_pct ?? -999));
  if (sortBy === 'change_asc') return (Number(a.price_change_pct ?? 999) - Number(b.price_change_pct ?? 999));
  if (sortBy === 'name_asc') return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN');
  return Number(a.rank_no ?? 9999) - Number(b.rank_no ?? 9999);
}

function fillIndustryOptions(items = []) {
  const select = qs('#selection-industry');
  if (!select) return;
  const current = select.value;
  const industries = [...new Set(items.map((item) => (item.industry || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  select.innerHTML = '<option value="">全部行业</option>' + industries.map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`).join('');
  if (industries.includes(current)) select.value = current;
}

function renderStrategySummary(strategy) {
  const container = qs('#strategy-summary');
  if (!strategy) {
    container.innerHTML = '<div class="empty-state">暂无策略信息</div>';
    return;
  }

  const factors = strategy.factors || [];
  const helpText = [
    strategy.description || '暂无策略说明',
    `阈值：${strategy.score_threshold ?? '-'}，最多入选：${strategy.max_picks ?? '-'}`,
    `核心因子：${factors.map((item) => item.name || item.key || '-').join(' / ') || '暂无'}`,
  ].join('｜');

  container.innerHTML = `
    <div class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategy.display_name || strategy.id || '-')}</strong>
        <div>
          <span class="badge ${strategy.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${escapeHtml(strategy.mode || 'current')}</span>
          <span class="badge ${strategy.status === 'active' ? 'status-ok' : 'status-warn'}">${escapeHtml(strategy.version || '-')}</span>
        </div>
      </div>
      <div class="muted">ID: ${escapeHtml(strategy.id || '-')} · 状态: ${escapeHtml(strategy.status || '-')} · ${strategy.executable === false ? '仅展示' : '可执行'}</div>
      <div>${escapeHtml(strategy.description || '')}</div>
      <div class="muted">阈值: ${strategy.score_threshold ?? '-'} 分 · 最多入选: ${strategy.max_picks ?? '-'}</div>
      <div class="muted">核心因子: ${factors.map((item) => escapeHtml(item.name || item.key || '-')).join(' / ') || '暂无'}</div>
      <div class="muted">完整因子分析请前往 <a href="/strategies">策略管理</a> · <button class="icon-help" type="button" data-tooltip="${escapeHtml(helpText)}">ⓘ</button></div>
    </div>
  `;
}

function renderSelectionResults(data) {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const topSummary = qs('#selection-top-summary');
  const minScore = Number(qs('#selection-min-score')?.value || 60);
  const searchText = (qs('#selection-search')?.value || '').trim().toLowerCase();
  const industryValue = (qs('#selection-industry')?.value || '').trim();
  const sortBy = qs('#selection-sort')?.value || 'rank_asc';
  const summary = data.summary || {};
  let items = (data.items || []).filter((item) => Number(item.score ?? 0) >= minScore);

  fillIndustryOptions(data.items || []);

  if (industryValue) {
    items = items.filter((item) => (item.industry || '').trim() === industryValue);
  }

  if (searchText) {
    items = items.filter((item) => [item.code, item.name, item.industry, item.strategy_display_name, item.strategy_id].some((value) => String(value || '').toLowerCase().includes(searchText)));
  }

  items = [...items].sort((a, b) => compareSelectionItems(sortBy, a, b));

  summaryLine.textContent = `run_id：${data.run_id || '最新'} · 选股交易日：${summary.selected_trade_date || '-'} · 入库时间：${summary.run_created_at || '-'} · 最新交易日：${summary.latest_trade_date || '-'} · 当前展示：${items.length} / ${summary.total_count || 0} 条`;
  topSummary.textContent = `样本池：${summary.sample_size || '-'} · 入选数：${summary.total_count || 0} · 数据更新时间：${summary.updated_at || '-'} · 当前策略：${data.strategy?.display_name || data.strategy?.id || '-'} · 策略版本：${data.strategy?.version || '-'} · 阈值：${data.strategy?.score_threshold ?? '-'} 分`;

  if (!items.length) {
    body.innerHTML = renderEmptyRow(13, '当前筛选条件下暂无选股结果');
    return;
  }

  body.innerHTML = items.map((item, index) => {
    const reasons = (item.reason_summary || []).slice(0, 2).join('；') || '-';
    const risks = (item.risk_summary || []).slice(0, 2).join('；') || '-';
    const detailId = `selection-detail-${index}`;
    const factorScores = item.factor_scores || {};
    const detailText = [
      `策略：${item.strategy_display_name || item.strategy_id || '-'}`,
      `策略版本：${item.strategy_version || '-'}`,
      `最新交易日：${item.latest_trade_date || '-'}`,
      `跟踪状态：${item.review_status || '-'}`,
      `开盘入选价：${item.selected_open_price ?? '-'}`,
      `收盘入选价：${item.selected_close_price ?? '-'}`,
      `最新价：${item.current_price ?? '-'}`,
      `区间涨跌幅：${item.price_change_pct ?? '-'}%`,
      `最大浮盈：${item.max_gain_pct ?? '-'}%`,
      `最大回撤：${item.max_drawdown_pct ?? '-'}%`,
      `因子摘要：turnover=${factorScores.turnover ?? '-'}, lowvol=${factorScores.lowvol ?? '-'}, reversal=${factorScores.reversal ?? '-'}`,
      `分项得分：value=${factorScores.value_score ?? '-'}, quality=${factorScores.quality_score ?? '-'}, stability=${factorScores.stability_score ?? '-'}, completeness=${factorScores.completeness_score ?? '-'}`,
      `详细原因：${(item.reason_summary || []).join('；') || '-'}`,
      `详细风险：${(item.risk_summary || []).join('；') || '-'}`,
    ].join('\n');
    return `
      <tr>
        <td>
          <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '')}</a>
          <div class="muted">${escapeHtml(item.code || '')}</div>
        </td>
        <td>
          <div>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</div>
          <div class="muted">${escapeHtml(item.strategy_version || '-')}</div>
        </td>
        <td>${escapeHtml(item.industry || '未分类')}</td>
        <td>${escapeHtml(item.selection_date || '-')}</td>
        <td>${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</td>
        <td>${formatNumber(item.score, 2)}</td>
        <td>${item.rank_no ?? '-'}</td>
        <td>${escapeHtml(item.review_status || '-')}</td>
        <td>${escapeHtml(reasons)}</td>
        <td>${escapeHtml(risks)}</td>
        <td><button class="btn btn-secondary" type="button" data-selection-detail="${detailId}" data-tooltip="${escapeHtml(detailText)}">查看</button></td>
      </tr>
      <tr id="${detailId}" class="selection-detail-row" hidden>
        <td colspan="13">
          <div class="muted">策略：${escapeHtml(item.strategy_display_name || item.strategy_id || '-')} · 版本：${escapeHtml(item.strategy_version || '-')} · 最新交易日：${escapeHtml(item.latest_trade_date || '-')} · 跟踪状态：${escapeHtml(item.review_status || '-')}</div>
          <div class="muted">价格跟踪：最新价 ${formatNumber(item.current_price, 2)} · 涨跌幅 <span class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</span> · 最大浮盈 <span class="up">${formatPercent(item.max_gain_pct)}</span> · 最大回撤 <span class="down">${formatPercent(item.max_drawdown_pct)}</span></div>
          <div class="muted">因子摘要：turnover=${escapeHtml(String(factorScores.turnover ?? '-'))} / lowvol=${escapeHtml(String(factorScores.lowvol ?? '-'))} / reversal=${escapeHtml(String(factorScores.reversal ?? '-'))}</div>
          <div class="score-chip-list">
            <span class="score-chip">value ${escapeHtml(String(factorScores.value_score ?? '-'))}</span>
            <span class="score-chip">quality ${escapeHtml(String(factorScores.quality_score ?? '-'))}</span>
            <span class="score-chip">stability ${escapeHtml(String(factorScores.stability_score ?? '-'))}</span>
            <span class="score-chip">data ${escapeHtml(String(factorScores.data_quality_score ?? '-'))}</span>
            <span class="score-chip">complete ${escapeHtml(String(factorScores.completeness_score ?? '-'))}</span>
          </div>
          <div class="muted">详细原因：${escapeHtml((item.reason_summary || []).join('；') || '-')}</div>
          <div class="muted">详细风险：${escapeHtml((item.risk_summary || []).join('；') || '-')}</div>
        </td>
      </tr>
    `;
  }).join('');

  body.querySelectorAll('[data-selection-detail]').forEach((button) => {
    button.addEventListener('click', () => {
      const detailRow = qs(`#${button.getAttribute('data-selection-detail')}`);
      if (detailRow) detailRow.hidden = !detailRow.hidden;
    });
  });
}

async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const select = qs('#strategy-id');
  select.innerHTML = '';

  const strategies = (data.strategies || []).filter((item) => item.executable !== false);
  currentDefaultStrategy = strategies.find((item) => item.id === data.default_strategy)?.id || strategies[0]?.id || null;

  strategies.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.id;
    option.textContent = `${item.display_name || item.id} (${item.id})`;
    if (item.id === currentDefaultStrategy) option.selected = true;
    select.appendChild(option);
  });

  if (select.value) {
    await loadStrategyDetail(select.value);
  } else {
    renderStrategySummary(null);
  }
}

async function loadStrategyDetail(strategyId) {
  const instrumentType = qs('#instrument-type')?.value || 'stock';
  const data = await fetchJson(`/api/strategies/detail?strategy_id=${encodeURIComponent(strategyId || currentDefaultStrategy || '')}&instrument_type=${encodeURIComponent(instrumentType)}&sample_limit=200`);
  renderStrategySummary(data.strategy);
}

async function loadSelectionResults(runIdOverride = null) {
  const instrumentType = qs('#instrument-type').value || 'stock';
  const limit = Number(qs('#limit').value || 3);
  const runIdInput = (qs('#selection-run-id')?.value || '').trim();
  const runId = runIdOverride || runIdInput;
  const query = new URLSearchParams({ instrument_type: instrumentType, limit: String(limit) });
  if (runId) query.set('run_id', runId);
  const data = await fetchJson(`/api/selection/results?${query.toString()}`);
  lastSelectionResponse = data;
  if (data.run_id && !runIdOverride) {
    qs('#selection-run-id').value = data.run_id;
  }
  renderSelectionResults(data);
  if (data.strategy) {
    renderStrategySummary(data.strategy);
  }
}

async function runSelection(event) {
  event.preventDefault();
  const button = event.submitter || qs('#selection-form button[type="submit"]');
  if (button) button.disabled = true;

  try {
    const result = await fetchJson('/api/selection/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: qs('#strategy-id').value || null,
        instrument_type: qs('#instrument-type').value,
        limit: Number(qs('#limit').value || 3),
        save: true,
      }),
    });
    if (result.run_id) {
      qs('#selection-run-id').value = result.run_id;
    }
    await loadSelectionResults(result.run_id || null);
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
  qs('#refresh-results').addEventListener('click', () => loadSelectionResults());
  qs('#refresh-selection-page').addEventListener('click', refreshSelectionPage);
  qs('#selection-min-score').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : loadSelectionResults());
  qs('#selection-search').addEventListener('input', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-sort').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-industry').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-filter-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    await loadSelectionResults();
  });
  qs('#selection-run-id').addEventListener('change', () => loadSelectionResults());
  qs('#strategy-id').addEventListener('change', async (event) => {
    await loadStrategyDetail(event.target.value);
  });

  try {
    await refreshSelectionPage();
    bindTooltips();
  } catch (error) {
    qs('#selection-summary-line').textContent = `页面初始化失败: ${error.message}`;
  }
});
