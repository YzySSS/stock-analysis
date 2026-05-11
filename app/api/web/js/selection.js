let currentDefaultStrategy = null;
let lastSelectionResponse = null;
let hasExecutedSelection = false;
const savedSelectionKeys = new Set();

function buildSelectionPersistKey(item) {
  return [
    item.selection_date || item.trade_date || '',
    item.strategy_id || '',
    item.code || '',
  ].join('::');
}

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
  if (strategy?.availability === 'experimental' || strategy?.availability === 'research') return 'status-warn';
  return 'status-muted';
}

function renderStrategySummary(strategy) {
  const container = qs('#strategy-summary');
  if (!strategy) {
    container.innerHTML = '<div class="empty-state">暂无策略信息</div>';
    renderSelectionFactorBars(null);
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
  renderSelectionFactorBars(strategy);
}

function renderSelectionFactorBars(strategy) {
  const container = qs('#selection-factor-bars');
  if (!container) return;
  const factors = strategy?.factors || [];
  if (!factors.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无因子配置';
    return;
  }
  container.classList.remove('empty-state');
  const weightFallback = Math.round(100 / Math.max(factors.length, 1));
  container.innerHTML = factors.slice(0, 6).map((factor, index) => {
    const rawWeight = factor.weight ?? factor.weight_pct ?? factor.ratio ?? null;
    const weight = rawWeight == null ? weightFallback : Number(rawWeight) <= 1 ? Number(rawWeight) * 100 : Number(rawWeight);
    const safeWeight = Math.max(6, Math.min(100, Number.isNaN(weight) ? weightFallback : weight));
    const label = factor.name || factor.label || factor.key || `因子${index + 1}`;
    return `
      <div class="selection-factor-bar">
        <div><span>${escapeHtml(label)}</span><b>${formatNumber(safeWeight, 0)}%</b></div>
        <i style="width:${safeWeight}%"></i>
      </div>
    `;
  }).join('');
}

function renderSelectionRunStatus(data = null, visibleItems = []) {
  const state = qs('#selection-run-state');
  if (!state) return;
  const summary = data?.summary || {};
  const originalCount = Number(summary.total_count ?? data?.items?.length ?? 0);
  const qualifiedCount = visibleItems.length;
  const successRate = originalCount ? (qualifiedCount / originalCount) * 100 : null;
  const scores = visibleItems.map((item) => Number(item.score)).filter((value) => !Number.isNaN(value));
  const avgScore = scores.length ? scores.reduce((sum, value) => sum + value, 0) / scores.length : null;
  qs('#selection-success-rate').textContent = successRate == null ? '-' : `${formatNumber(successRate, 0)}%`;
  state.textContent = data ? (qualifiedCount ? '运行成功' : '无达标股') : '待运行';
  state.classList.remove('up', 'down');
  if (data && qualifiedCount) state.classList.add('up');
  if (data && !qualifiedCount) state.classList.add('down');
  qs('#selection-run-time').textContent = summary.run_created_at || summary.updated_at || summary.latest_trade_date || '-';
  qs('#selection-qualified-count').textContent = data ? `${qualifiedCount} / ${originalCount || 0}` : '-';
  qs('#selection-avg-score').textContent = formatNumber(avgScore, 1);
  renderSelectionScoreSparkline(scores);
}

function renderSelectionScoreSparkline(scores = []) {
  const svg = qs('#selection-score-sparkline');
  if (!svg) return;
  if (!scores.length) {
    svg.innerHTML = '<text x="16" y="44" fill="#64748b" font-size="12">暂无评分分布</text>';
    return;
  }
  const width = 320;
  const height = 88;
  const padding = { left: 14, right: 14, top: 12, bottom: 16 };
  const values = scores.slice(0, 12);
  const max = Math.max(100, ...values);
  const min = Math.min(0, ...values);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const step = plotWidth / Math.max(values.length - 1, 1);
  const points = values.map((value, index) => {
    const x = padding.left + index * step;
    const y = padding.top + ((max - value) / (max - min || 1)) * plotHeight;
    return `${x},${y}`;
  }).join(' ');
  svg.innerHTML = `
    <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" stroke="rgba(148,163,184,.22)" />
    <polyline points="${points}" fill="none" stroke="#38bdf8" stroke-width="2.5" />
    ${values.map((value, index) => {
      const x = padding.left + index * step;
      const y = padding.top + ((max - value) / (max - min || 1)) * plotHeight;
      return `<circle cx="${x}" cy="${y}" r="3" fill="${value >= 80 ? '#fb7185' : '#38bdf8'}" />`;
    }).join('')}
  `;
}

function syncInstrumentSegments() {
  const select = qs('#instrument-type');
  qsa('[data-instrument-value]').forEach((button) => {
    button.classList.toggle('active', button.dataset.instrumentValue === select.value);
  });
}

function syncScoreInputs(source) {
  const numberInput = qs('#selection-min-score');
  const rangeInput = qs('#selection-min-score-range');
  if (!numberInput || !rangeInput) return;
  const value = Math.max(0, Math.min(100, Number(source.value || 0)));
  numberInput.value = String(value);
  rangeInput.value = String(value);
}

function formatLocalDateTimeValue(date = new Date()) {
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function syncRunTimeDisplay() {
  const input = qs('#selection-run-time-input');
  const shell = qs('#selection-run-time-shell');
  const target = qs('#selection-run-time-display');
  if (!input || !shell || !target) return;
  const hasValue = Boolean(input.value);
  shell.classList.toggle('has-value', hasValue);
  target.textContent = hasValue ? input.value.replace('T', ' ').replaceAll('-', '/') : '自动使用最新交易日';
}

function updateRunTimeDisplay() {
  const input = qs('#selection-run-time-input');
  if (!input) return;
  input.value = formatLocalDateTimeValue(new Date());
  syncRunTimeDisplay();
}

function renderSelectionPlaceholder(message = '请先设置条件并点击“运行”，再查看本次选股结果') {
  const body = qs('#selection-results-body');
  const summaryLine = qs('#selection-summary-line');
  const cards = qs('#selection-result-cards');
  body.innerHTML = renderEmptyRow(14, message);
  summaryLine.textContent = message;
  if (cards) {
    cards.classList.add('empty-state');
    cards.innerHTML = message;
  }
  renderSelectionRunStatus(null, []);
}

const FACTOR_LABELS = {
  turnover: '换手',
  lowvol: '低波',
  reversal: '反转',
  trend: '趋势',
  momentum: '动量',
  quality: '质量',
  sentiment: '情绪',
  value: '估值',
};

function formatFactorSummary(factors = {}, maxItems = 5) {
  const entries = Object.entries(factors || {}).filter(([, value]) => value !== null && value !== undefined && value !== '');
  if (!entries.length) return '-';
  return entries.slice(0, maxItems).map(([key, value]) => `${FACTOR_LABELS[key] || key} ${formatNumber(value, 2)}`).join(' / ');
}

function renderSelectionResultCards(items = []) {
  const container = qs('#selection-result-cards');
  if (!container) return;
  if (!items.length) {
    container.classList.add('empty-state');
    container.innerHTML = '暂无达标标的';
    return;
  }
  container.classList.remove('empty-state');
  container.innerHTML = items.slice(0, 6).map((item) => {
    const factorSummary = formatFactorSummary(item.factors || item.factor_scores || {}, 4);
    const reasons = (item.reason_summary || []).slice(0, 2).join('；') || '暂无原因摘要';
    const risks = (item.risk_summary || []).slice(0, 1).join('；') || '暂无明显风险提示';
    const pctClass = getPctClass(item.price_change_pct) || '';
    return `
      <article class="selection-stock-card">
        <div class="selection-stock-head">
          <div>
            <a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.name || '-')}</a>
            <span>${escapeHtml(item.code || '-')}</span>
          </div>
          <em>#${escapeHtml(item.rank_no ?? '-')}</em>
        </div>
        <div class="selection-stock-score-row">
          <strong>${formatNumber(item.score, 2)}</strong>
          <span class="${pctClass}">${formatPercent(item.price_change_pct)}</span>
        </div>
        <div class="selection-stock-meta">
          <span>${escapeHtml(item.industry_display || item.industry || '暂无行业')}</span>
          <span>${escapeHtml(item.selection_date || '-')}</span>
        </div>
        <div class="selection-stock-prices">
          <span>入选 ${formatNumber(item.selected_close_price ?? item.selected_open_price, 2)}</span>
          <span>最新 ${formatNumber(item.current_price, 2)}</span>
        </div>
        <div class="selection-factor-mini">${escapeHtml(factorSummary)}</div>
        <div class="selection-card-note">${escapeHtml(reasons)}</div>
        <div class="selection-card-risk">${escapeHtml(risks)}</div>
      </article>
    `;
  }).join('');
}

function normalizeRunResponse(result) {
  const runDate = result.run_id?.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/) ? `${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[1]}-${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[2]}-${result.run_id.match(/^selection(?:_preview)?_(\d{4})(\d{2})(\d{2})_/)[3]}` : null;
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
      selection_date: runDate || item.trade_date || '',
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
      instrument_type: item.instrument_type || qs('#instrument-type')?.value || 'stock',
      explain: explain,
      factors: item.factors || {},
      candidate_reasons: item.candidate_reasons || explain.reasons || [],
      candidate_risks: item.candidate_risks || explain.risks || [],
      missing_fields: item.missing_fields || explain.missing_fields || [],
      close: item.close ?? null,
      pe_tushare: item.pe_tushare ?? null,
      pb_tushare: item.pb_tushare ?? null,
      roe: item.roe ?? null,
      roa: item.roa ?? null,
      grossprofit_margin: item.grossprofit_margin ?? null,
      netprofit_margin: item.netprofit_margin ?? null,
      revenue_yoy: item.revenue_yoy ?? null,
      profit_yoy: item.profit_yoy ?? null,
      value_score: item.value_score ?? explain.summary?.value_score ?? null,
      quality_score: item.quality_score ?? explain.summary?.quality_score ?? null,
      stability_score: item.stability_score ?? explain.summary?.stability_score ?? null,
      data_quality_score: item.data_quality_score ?? explain.summary?.data_quality_score ?? null,
      completeness_score: item.completeness_score ?? explain.summary?.completeness_score ?? null,
      run_diagnostics: item.run_diagnostics || result.diagnostics || null,
    };
  });

  return {
    run_id: result.run_id || null,
    requested_strategy_id: result.strategy?.id || result.strategy_id || null,
    strategy: result.strategy || null,
    diagnostics: result.diagnostics || items[0]?.run_diagnostics || null,
    sentiment_prefetch: result.sentiment_prefetch || null,
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
  const diagnostics = data.diagnostics || {};
  const v13Filter = diagnostics.v13_filter_summary || null;
  const v12Filter = diagnostics.v12_filter_summary || null;
  const filterSummary = v13Filter
    ? ` · V13硬过滤：${v13Filter.before ?? '-'} → ${v13Filter.after ?? '-'}，剔除 ${v13Filter.removed ?? '-'}`
    : v12Filter
      ? ` · V12硬过滤：${v12Filter.before ?? '-'} → ${v12Filter.after ?? '-'}，剔除 ${v12Filter.removed ?? '-'}`
      : '';
  const sentimentSummary = data.sentiment_prefetch
    ? ` · 舆情精排：Tavily ${data.sentiment_prefetch.tavily_runs ?? 0}/${data.sentiment_prefetch.requested ?? '-'}`
    : '';
  topSummary.textContent = `样本池：${summary.sample_size || '-'} · 原始入选上限：${data.strategy?.max_picks ?? '-'} · 数据更新时间：${summary.updated_at || '-'} · 当前策略：${data.strategy?.display_name || data.strategy?.id || '-'} · 策略版本：${data.strategy?.version || '-'} · 当前运行阈值：${data.strategy?.score_threshold ?? '-'} 分${filterSummary}${sentimentSummary}`;

  renderSelectionRunStatus(data, items);

  if (!originalItems.length) {
    renderSelectionResultCards([]);
    body.innerHTML = renderEmptyRow(14, '本次运行未产生任何入选结果');
    return;
  }

  if (!items.length) {
    renderSelectionResultCards([]);
    body.innerHTML = renderEmptyRow(14, '无达标股：当前入选结果中没有股票达到设定分数底线');
    return;
  }

  renderSelectionResultCards(items);

  body.innerHTML = items.map((item, index) => {
    const reasonsList = item.reason_summary || [];
    const risksList = item.risk_summary || [];
    const reasons = reasonsList.slice(0, 2).join('；') || '-';
    const risks = risksList.slice(0, 2).join('；') || '-';
    const detailId = `selection-detail-${index}`;
    const factorScores = item.factor_scores || {};
    const saveKey = buildSelectionPersistKey(item);
    const isSaved = savedSelectionKeys.has(saveKey);
    const factorSummary = formatFactorSummary(item.factors || factorScores);
    const fundamentalMissingFields = Array.isArray(factorScores.fundamental_missing_fields) ? factorScores.fundamental_missing_fields : [];
    const fundamentalCompleteness = factorScores.fundamental_completeness == null ? null : Number(factorScores.fundamental_completeness) * 100;
    const fundamentalHint = fundamentalMissingFields.length
      ? `基本面完整度 ${formatNumber(fundamentalCompleteness, 0)}% · 缺失 ${fundamentalMissingFields.join(', ')}`
      : `基本面完整度 ${formatNumber(fundamentalCompleteness, 0)}% · 关键字段齐全`;
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
      `因子得分：${factorSummary}`,
      `基础打分：value=${factorScores.value_score ?? '-'}, quality=${factorScores.quality_score ?? '-'}, stability=${factorScores.stability_score ?? '-'}, data=${factorScores.data_quality_score ?? '-'}, completeness=${factorScores.completeness_score ?? '-'}`,
      `基本面：${fundamentalHint}`,
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
        <td class="${getPctClass(item.price_change_pct) || ''}">${formatPercent(item.price_change_pct)}</td>
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
          <div class="muted">${escapeHtml(fundamentalHint)}</div>
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
          <div class="muted">价格跟踪：最新价 ${formatNumber(item.current_price, 2)} · 涨跌幅 <span class="${getPctClass(item.price_change_pct) || ''}">${formatPercent(item.price_change_pct)}</span> · 最大浮盈 <span class="up">${formatPercent(item.max_gain_pct)}</span> · 最大回撤 <span class="down">${formatPercent(item.max_drawdown_pct)}</span></div>
          <div class="muted">因子得分：${escapeHtml(factorSummary)}</div>
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
        const persistedKey = buildSelectionPersistKey(item);
        savedSelectionKeys.add(persistedKey);
        savedSelectionKeys.add(button.getAttribute('data-selection-save'));
        button.textContent = '已保存';
        button.classList.remove('btn-primary');
        if (!button.classList.contains('btn-secondary')) {
          button.classList.add('btn-secondary');
        }
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
  (data.items || []).forEach((item) => {
    if (item.persisted_key) savedSelectionKeys.add(item.persisted_key);
  });
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
    (normalized.items || []).forEach((item) => {
      if (item.persisted_key) savedSelectionKeys.add(item.persisted_key);
    });
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
  qs('#selection-min-score').addEventListener('change', (event) => {
    syncScoreInputs(event.target);
    lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : renderSelectionPlaceholder();
  });
  qs('#selection-min-score-range')?.addEventListener('input', (event) => {
    syncScoreInputs(event.target);
    lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : renderSelectionPlaceholder();
  });
  qs('#selection-search').addEventListener('input', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-sort').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-industry').addEventListener('change', () => lastSelectionResponse ? renderSelectionResults(lastSelectionResponse) : null);
  qs('#selection-run-time-input')?.addEventListener('change', syncRunTimeDisplay);
  qs('#selection-filter-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    await loadSelectionResults();
  });
  qs('#selection-run-id').addEventListener('change', async () => {
    hasExecutedSelection = Boolean((qs('#selection-run-id')?.value || '').trim());
    await loadSelectionResults();
  });
  qsa('[data-instrument-value]').forEach((button) => {
    button.addEventListener('click', async () => {
      qs('#instrument-type').value = button.dataset.instrumentValue;
      syncInstrumentSegments();
      hasExecutedSelection = false;
      lastSelectionResponse = null;
      await loadStrategyDetail(qs('#strategy-id')?.value || currentDefaultStrategy || '');
      renderSelectionPlaceholder();
    });
  });
  qs('#instrument-type').addEventListener('change', () => syncInstrumentSegments());
  qs('#strategy-id').addEventListener('change', async (event) => {
    hasExecutedSelection = false;
    lastSelectionResponse = null;
    qs('#selection-run-id').value = '';
    await loadStrategyDetail(event.target.value);
    renderSelectionPlaceholder();
  });

  try {
    updateRunTimeDisplay();
    syncScoreInputs(qs('#selection-min-score'));
    syncInstrumentSegments();
    await loadStrategies();
    renderSelectionPlaceholder();
    bindTooltips();
  } catch (error) {
    qs('#selection-summary-line').textContent = `页面初始化失败: ${error.message}`;
  }
});
