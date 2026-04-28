let currentDefaultStrategy = null;
let lastSelectionResponse = null;
let hasExecutedSelection = false;
const savedSelectionKeys = new Set();

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

function strategyBadgeClass(strategy) {
  if (strategy?.availability === 'runtime_ready') return 'status-ok';
  if (strategy?.availability === 'experimental') return 'status-warn';
  return 'status-muted';
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
    `可用状态：${strategy.availability_label || '-'}`,
    `核心因子：${factors.map((item) => item.name || item.key || '-').join(' / ') || '暂无'}`,
  ].join('｜');

  container.innerHTML = `
    <div class="strategy-item">
      <div class="strategy-item-head">
        <strong>${escapeHtml(strategy.display_name || strategy.id || '-')}</strong>
        <div>
          <span class="badge ${strategy.mode === 'legacy' ? 'status-warn' : 'status-ok'}">${escapeHtml(strategy.mode || 'current')}</span>
          <span class="badge ${strategyBadgeClass(strategy)}">${escapeHtml(strategy.availability_label || '-')}</span>
        </div>
      </div>
      <div class="muted">ID: ${escapeHtml(strategy.id || '-')} · 状态: ${escapeHtml(strategy.status || '-')} · 版本: ${escapeHtml(strategy.version || '-')}</div>
      <div>${escapeHtml(strategy.description || '')}</div>
      <div class="muted">当前运行阈值: ${strategy.score_threshold ?? '-'} 分 · 最大入选: ${strategy.max_picks ?? '-'}</div>
      <div class="muted">核心因子: ${factors.map((item) => escapeHtml(item.name || item.key || '-')).join(' / ') || '暂无'}</div>
      <div class="muted">${escapeHtml(strategy.availability_note || '暂无状态说明')} · 完整因子分析请前往 <a href="/strategies">策略管理</a> · <button class="icon-help" type="button" data-tooltip="${escapeHtml(helpText)}">ⓘ</button></div>
    </div>
  `;
}

function renderSelectionPlaceholder(message = '请先设置条件并点击“运行”，再查看本次选股结果') {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  body.innerHTML = renderEmptyRow(14, message);
  summaryLine.textContent = message;
}

function normalizeRunResponse(result) {
  const items = (result.results || []).map((item) => {
    const explain = item.explain || {};
    const factorScores = {
      ...(explain.raw_metrics || {}),
      ...(item.factors || {}),
      ...(explain.summary || {}),
    };
    return {
      run_id: item.run_id || result.run_id || null,
      rank_no: item.rank_no ?? null,
      code: item.code,
      name: item.name,
      selection_date: item.trade_date || '',
      strategy_id: item.strategy_id,
      score: item.score,
      strategy_display_name: item.strategy_display_name,
      strategy_version: item.strategy_version,
      industry: item.industry || null,
      industry_display: item.industry || '暂无行业',
      factor_scores: factorScores,
      selected_open_price: item.open ?? null,
      selected_close_price: item.close ?? null,
      current_price: item.close ?? null,
      latest_trade_date: item.trade_date || null,
      price_change_pct: 0,
      reason_summary: explain.reasons || item.candidate_reasons || [],
      risk_summary: explain.risks || item.candidate_risks || [],
      tracking_days: 0,
      review_status: 'preview',
      max_gain_pct: 0,
      max_drawdown_pct: 0,
    };
  });

  return {
    run_id: result.run_id || null,
    requested_strategy_id: result.strategy?.id || result.strategy_id || null,
    strategy: result.strategy || null,
    summary: {
      selected_trade_date: items[0]?.selection_date || null,
      run_created_at: null,
      latest_trade_date: items[0]?.latest_trade_date || null,
      total_count: items.length,
      sample_size: null,
      instrument_type: items[0]?.instrument_type || qs('#instrument-type')?.value || 'stock',
      updated_at: items[0]?.latest_trade_date || null,
      result_strategy_id: result.strategy?.id || result.strategy_id || null,
    },
    items,
  };
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
  const originalItems = data.items || [];
  let items = originalItems.filter((item) => Number(item.score ?? 0) >= minScore);

  fillIndustryOptions(originalItems);

  if (industryValue) {
    items = items.filter((item) => (item.industry || '').trim() === industryValue);
  }

  if (searchText) {
    items = items.filter((item) => [item.code, item.name, item.industry, item.strategy_display_name, item.strategy_id].some((value) => String(value || '').toLowerCase().includes(searchText)));
  }

  items = [...items].sort((a, b) => compareSelectionItems(sortBy, a, b));

  summaryLine.textContent = `run_id：${data.run_id || '最新'} · 选股交易日：${summary.selected_trade_date || '-'} · 入库时间：${summary.run_created_at || '-'} · 最新交易日：${summary.latest_trade_date || '-'} · 达标展示：${items.length} / 原始入选 ${summary.total_count || 0} 条`;
  topSummary.textContent = `样本池：${summary.sample_size || '-'} · 原始入选上限：${data.strategy?.max_picks ?? '-'} · 数据更新时间：${summary.updated_at || '-'} · 当前策略：${data.strategy?.display_name || data.strategy?.id || '-'} · 策略版本：${data.strategy?.version || '-'} · 当前运行阈值：${data.strategy?.score_threshold ?? '-'} 分`;

  if (!originalItems.length) {
    body.innerHTML = renderEmptyRow(14, '本次运行未产生任何入选结果');
    return;
  }

  if (!items.length) {
    body.innerHTML = renderEmptyRow(14, '无达标股：当前入选结果中没有股票达到设定分数底线');
    return;
  }

  body.innerHTML = items.map((item, index) => {
    const reasonsList = item.reason_summary || [];
    const risksList = item.risk_summary || [];
    const reasons = reasonsList.slice(0, 2).join('；') || '-';
    const risks = risksList.slice(0, 2).join('；') || '-';
    const detailId = `selection-detail-${index}`;
    const factorScores = item.factor_scores || {};
    const saveKey = `${data.run_id || item.run_id || 'preview'}::${item.code || ''}`;
    const isSaved = savedSelectionKeys.has(saveKey);
    const turnover = factorScores.turnover ?? '-';
    const lowvol = factorScores.lowvol ?? '-';
    const reversal = factorScores.reversal ?? '-';
    const factorSummary = `换手 ${turnover} / 低波 ${lowvol} / 反转 ${reversal}`;
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
      `三因子：${factorSummary}`,
      `基础打分：value=${factorScores.value_score ?? '-'}, quality=${factorScores.quality_score ?? '-'}, stability=${factorScores.stability_score ?? '-'}, data=${factorScores.data_quality_score ?? '-'}, completeness=${factorScores.completeness_score ?? '-'}`,
      `详细原因：${reasonsList.join('；') || '-'}`,
      `详细风险：${risksList.join('；') || '-'}`,
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
        <td>${escapeHtml(item.industry_display || '暂无行业')}</td>
        <td>${escapeHtml(item.selection_date || '-')}</td>
        <td>${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</td>
        <td>${formatNumber(item.current_price, 2)}</td>
        <td class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</td>
        <td>
          <div>${formatNumber(item.score, 2)}</div>
          <div class="muted">${escapeHtml(factorSummary)}</div>
        </td>
        <td>
          <div>${item.rank_no ?? '-'}</div>
          <div class="muted">第 ${item.rank_no ?? '-'} 名</div>
        </td>
        <td>${escapeHtml(item.review_status || '-')}</td>
        <td>
          <div>${escapeHtml(reasons)}</div>
          <div class="muted">共 ${reasonsList.length} 条</div>
        </td>
        <td>
          <div>${escapeHtml(risks)}</div>
          <div class="muted">共 ${risksList.length} 条</div>
        </td>
        <td>
          <button class="btn ${isSaved ? 'btn-secondary' : 'btn-primary'}" type="button" data-selection-save="${escapeHtml(saveKey)}" ${isSaved ? 'disabled' : ''}>${isSaved ? '已保存' : '保存'}</button>
        </td>
        <td><button class="btn btn-secondary" type="button" data-selection-detail="${detailId}" data-tooltip="${escapeHtml(detailText)}">查看</button></td>
      </tr>
      <tr id="${detailId}" class="selection-detail-row" hidden>
        <td colspan="14">
          <div class="muted">策略：${escapeHtml(item.strategy_display_name || item.strategy_id || '-')} · 版本：${escapeHtml(item.strategy_version || '-')} · 最新交易日：${escapeHtml(item.latest_trade_date || '-')} · 跟踪状态：${escapeHtml(item.review_status || '-')}</div>
          <div class="muted">行业：${escapeHtml(item.industry_display || '暂无行业')} · 排名：第 ${escapeHtml(String(item.rank_no ?? '-'))} 名 · 总分：${escapeHtml(String(formatNumber(item.score, 2)))}</div>
          <div class="muted">价格跟踪：最新价 ${formatNumber(item.current_price, 2)} · 涨跌幅 <span class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</span> · 最大浮盈 <span class="up">${formatPercent(item.max_gain_pct)}</span> · 最大回撤 <span class="down">${formatPercent(item.max_drawdown_pct)}</span></div>
          <div class="muted">三因子得分：换手=${escapeHtml(String(turnover))} / 低波=${escapeHtml(String(lowvol))} / 反转=${escapeHtml(String(reversal))}</div>
          <div class="score-chip-list">
            <span class="score-chip">value ${escapeHtml(String(factorScores.value_score ?? '-'))}</span>
            <span class="score-chip">quality ${escapeHtml(String(factorScores.quality_score ?? '-'))}</span>
            <span class="score-chip">stability ${escapeHtml(String(factorScores.stability_score ?? '-'))}</span>
            <span class="score-chip">data ${escapeHtml(String(factorScores.data_quality_score ?? '-'))}</span>
            <span class="score-chip">complete ${escapeHtml(String(factorScores.completeness_score ?? '-'))}</span>
          </div>
          <div class="muted">详细原因：${escapeHtml(reasonsList.join('；') || '-')}</div>
          <div class="muted">详细风险：${escapeHtml(risksList.join('；') || '-')}</div>
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

  body.querySelectorAll('[data-selection-save]').forEach((button, index) => {
    button.addEventListener('click', async () => {
      const item = items[index];
      if (!item) return;
      button.disabled = true;
      try {
        const response = await fetchJson('/api/selection/save-item', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            run_id: data.run_id || item.run_id,
            strategy_id: item.strategy_id || data.strategy?.id,
            score_threshold: data.strategy?.score_threshold ?? Number(qs('#selection-min-score')?.value || 60),
            item,
          }),
        });
        savedSelectionKeys.add(`${response.run_id}::${response.code}`);
        savedSelectionKeys.add(button.getAttribute('data-selection-save'));
        button.textContent = '已保存';
        button.classList.remove('btn-primary');
        button.classList.add('btn-secondary');
      } catch (error) {
        button.disabled = false;
        throw error;
      }
    });
  });
}

async function loadStrategies() {
  const data = await fetchJson('/api/strategies');
  const select = qs('#strategy-id');
  select.innerHTML = '';

  const strategies = (data.strategies || []).filter((item) => item.runtime_ready === true);
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
  const strategyId = qs('#strategy-id')?.value || currentDefaultStrategy || '';
  const runId = runIdOverride || runIdInput;

  if (!runId && !hasExecutedSelection) {
    lastSelectionResponse = null;
    renderSelectionPlaceholder();
    return;
  }

  const query = new URLSearchParams({ instrument_type: instrumentType, limit: String(limit) });
  if (runId) {
    query.set('run_id', runId);
  } else if (strategyId) {
    query.set('strategy_id', strategyId);
  }
  const data = await fetchJson(`/api/selection/results?${query.toString()}`);
  lastSelectionResponse = data;
  if (data.run_id && !runIdOverride) {
    qs('#selection-run-id').value = data.run_id;
  }
  if (!runId && data.requested_strategy_id && qs('#strategy-id')) {
    qs('#strategy-id').value = data.requested_strategy_id;
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
        score_threshold: Number(qs('#selection-min-score').value || 60),
        save: false,
      }),
    });
    hasExecutedSelection = true;
    if (result.run_id) {
      qs('#selection-run-id').value = result.run_id;
    }
    const normalized = normalizeRunResponse(result);
    lastSelectionResponse = normalized;
    renderSelectionResults(normalized);
    if (normalized.strategy) {
      renderStrategySummary(normalized.strategy);
    }
  } finally {
    if (button) button.disabled = false;
  }
}

async function refreshSelectionPage() {
  await loadStrategies();
  if (hasExecutedSelection || (qs('#selection-run-id')?.value || '').trim()) {
    await loadSelectionResults();
  } else {
    renderSelectionPlaceholder();
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#selection-form').addEventListener('submit', runSelection);
  qs('#refresh-strategies').addEventListener('click', async () => {
    await loadStrategies();
  });
  qs('#refresh-results').addEventListener('click', () => loadSelectionResults());
  qs('#refresh-selection-page').addEventListener('click', refreshSelectionPage);
  qs('#selection-min-score').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : renderSelectionPlaceholder());
  qs('#selection-search').addEventListener('input', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-sort').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-industry').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-filter-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    await loadSelectionResults();
  });
  qs('#selection-run-id').addEventListener('change', async () => {
    hasExecutedSelection = Boolean((qs('#selection-run-id')?.value || '').trim());
    await loadSelectionResults();
  });
  qs('#strategy-id').addEventListener('change', async (event) => {
    hasExecutedSelection = false;
    lastSelectionResponse = null;
    qs('#selection-run-id').value = '';
    await loadStrategyDetail(event.target.value);
    renderSelectionPlaceholder();
  });

  try {
    await loadStrategies();
    renderSelectionPlaceholder();
    bindTooltips();
  } catch (error) {
    qs('#selection-summary-line').textContent = `页面初始化失败: ${error.message}`;
  }
});
