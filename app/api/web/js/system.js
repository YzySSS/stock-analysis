async function loadSystemPage() {
  const started = performance.now();
  const panels = {
    api: qs('#system-api-panel'),
    db: qs('#system-db-panel'),
    latestDate: qs('#system-latest-date-panel'),
    lastRun: qs('#system-last-run-panel'),
    baseline: qs('#system-baseline-panel'),
    baselineOverall: qs('#system-baseline-overall'),
    coverage: qs('#system-coverage-panel'),
    gap: qs('#system-gap-panel'),
    fieldMissing: qs('#system-field-missing-panel'),
    shortfall: qs('#system-shortfall-panel'),
    taskRun: qs('#system-task-run-panel'),
    sentiment: qs('#system-sentiment-quality-panel'),
  };

  try {
    const data = await fetchJson('/api/system/status');
    const elapsedMs = Math.round(performance.now() - started);
    const latest = data.latest || {};
    const sentimentQuality = data.sentiment_quality || {};
    const taskRuns = data.task_runs || [];
    const schedules = data.scheduled_tasks || [];
    const marketOpinion = data.market_opinion_update || null;
    const taskMap = Object.fromEntries(taskRuns.map((item) => [item.task_name, item]));

    panels.api.innerHTML = renderTopMetricCard({
      icon: '⌘',
      title: 'API状态',
      state: data.status === 'ok' ? '正常' : '异常',
      stateClass: data.status === 'ok' ? 'ok' : 'error',
      lines: [`响应时间 ${elapsedMs} ms`, `缓存 ${data.cache?.hit ? '命中' : '刷新'}`],
    });
    panels.db.innerHTML = renderTopMetricCard({
      icon: '▣',
      title: '数据库状态',
      state: data.health?.database ? '正常' : '异常',
      stateClass: data.health?.database ? 'ok' : 'error',
      lines: [`连接池 18/50`, `缓存 ${data.cache?.ttl_seconds ?? '-'}s`],
    });
    panels.latestDate.innerHTML = renderTopMetricCard({
      icon: '▦',
      title: '最新交易日',
      state: latest.daily_kline_latest_trade_date || '-',
      stateClass: 'neutral',
      lines: [
        `选股交易日 ${latest.selection_result_latest_trade_date || '-'}`,
        latest.daily_kline_latest_is_partial
          ? `最新可用 ${latest.daily_kline_latest_available_trade_date || '-'} 仅 ${latest.daily_kline_latest_available_count || 0} 条`
          : 'A股正常交易',
      ],
    });
    panels.lastRun.innerHTML = renderTopMetricCard({
      icon: '◷',
      title: '最后更新时间',
      state: findLastUpdate(latest, taskRuns),
      stateClass: 'neutral',
      lines: [`本次耗时 ${elapsedMs}ms`, `任务数 ${taskRuns.length}`],
    });

    renderBaseline(data.data_baseline || {}, panels.baseline, panels.baselineOverall);
    panels.coverage.innerHTML = renderScheduleGrid(schedules, taskMap, taskRuns, marketOpinion);
    if (panels.taskRun) panels.taskRun.innerHTML = '';
    panels.gap.innerHTML = renderGapNote();
    panels.sentiment.innerHTML = renderSentimentQuality(sentimentQuality);
    if (panels.fieldMissing) panels.fieldMissing.innerHTML = '';
    if (panels.shortfall) panels.shortfall.innerHTML = '';
  } catch (error) {
    Object.values(panels).forEach((panel) => {
      if (panel && panel !== panels.baselineOverall) panel.innerHTML = `<div class="error-box">加载系统状态失败: ${escapeHtml(error.message)}</div>`;
    });
  }
}

function renderTopMetricCard({ icon, title, state, stateClass = 'neutral', lines = [] }) {
  return `
    <div class="top-metric-head">
      <div class="top-metric-icon">${escapeHtml(icon)}</div>
      <div>
        <span>${escapeHtml(title)}</span>
        <b class="${stateClass}">${escapeHtml(state)}</b>
      </div>
    </div>
    <div class="top-metric-body no-spark">
      <div>${lines.map((line) => `<span>${escapeHtml(line)}</span>`).join('')}</div>
    </div>
  `;
}

function findLastUpdate(latest = {}, taskRuns = []) {
  const candidates = [
    latest.stock_basic_latest_updated_at,
    latest.fundamental_latest_updated_at,
    latest.valuation_latest_updated_at,
    latest.selection_result_latest_created_at,
    ...taskRuns.map((item) => item.finished_at),
  ].filter(Boolean).sort();
  return candidates[candidates.length - 1] || '-';
}

function renderBaseline(baseline = {}, container, overallEl) {
  const items = baseline.items || [];
  if (!items.length) {
    container.innerHTML = '<div class="empty-state">暂无数据基准</div>';
    if (overallEl) overallEl.textContent = '覆盖率 -';
    return;
  }
  const validValues = items.map((item) => Number(item.value)).filter((value) => Number.isFinite(value));
  const overall = validValues.length ? validValues.reduce((sum, value) => sum + value, 0) / validValues.length : null;
  if (overallEl) overallEl.textContent = `覆盖率均值 ${overall == null ? '-' : `${overall.toFixed(2)}%`}`;
  container.innerHTML = items.map((item) => {
    const value = Number(item.value);
    const pct = Number.isFinite(value) ? Math.max(0, Math.min(100, value)) : 0;
    return `
      <article class="baseline-card ${item.key || ''}">
        <div class="baseline-card-head">
          <span>${baselineIcon(item.key)}</span>
          <b>${escapeHtml(item.label || '-')}</b>
        </div>
        <strong>${Number.isFinite(value) ? `${value.toFixed(2)}%` : '-'}</strong>
        <div class="baseline-progress"><i style="width:${pct}%"></i></div>
        <p>${escapeHtml(item.done ?? '-')} / ${escapeHtml(item.total ?? '-')}</p>
        <small>${escapeHtml(item.unit || '记录数')}</small>
      </article>
    `;
  }).join('');
}

function baselineIcon(key) {
  const map = { kline: '↕', fundamental: '▥', valuation: '◔', factor: 'Σ', realtime: 'ϟ', adjfactor: '⇄', moneyflow: '⇢', chip: '◍', fundflow: '≈', sentiment: '☻' };
  return map[key] || '•';
}

function renderScheduleGrid(schedules = [], taskMap = {}, taskRuns = [], marketOpinion = null) {
  const scheduledNames = new Set(schedules.map((task) => task.task_name));
  const extraTasks = taskRuns
    .filter((run) => run.task_name && !scheduledNames.has(run.task_name))
    .map((run) => ({ task_name: run.task_name, task_label: run.task_label || run.task_name, schedule: '按需 / 后台任务' }));
  const merged = [...schedules, ...extraTasks];
  if (!merged.length) return '<div class="empty-state">暂无任务配置</div>';
  return merged.map((task) => {
    const run = taskMap[task.task_name] || {};
    const statusClass = getStatusClass(run.status);
    return `
      <article class="system-task-card">
        <div class="system-task-card-head">
          <strong>${escapeHtml(task.task_label || task.task_name)}</strong>
          <span class="badge ${statusClass}">${escapeHtml(run.status || '未执行')}</span>
        </div>
        <div class="system-task-schedule">${escapeHtml(task.schedule || '-')}</div>
        <div class="system-task-times">
          <span>开始 ${escapeHtml(run.started_at || '-')}</span>
          <span>结束 ${escapeHtml(run.finished_at || '-')}</span>
        </div>
        <div class="system-task-result">${renderTaskRunMetrics(run, task.task_name === 'market_opinion_update' ? marketOpinion : null)}</div>
      </article>
    `;
  }).join('');
}

function renderTaskRunMetrics(run = {}, marketOpinion = null) {
  const meta = run.metadata || {};
  const metrics = [
    meta.success_codes != null ? `成功 ${escapeHtml(meta.success_codes)} / ${escapeHtml(meta.requested_codes ?? meta.limit ?? '-')}` : '',
    meta.updated != null ? `更新 ${escapeHtml(meta.updated)} / 扫描 ${escapeHtml(meta.scanned ?? '-')}` : '',
    meta.rows_synced != null ? `写入 ${escapeHtml(meta.rows_synced)} 行` : '',
    meta.failed != null ? `失败 ${escapeHtml(meta.failed)}` : '',
    meta.no_data != null ? `无数据 ${escapeHtml(meta.no_data)}` : '',
    marketOpinion?.source_count != null ? `源 ${escapeHtml(marketOpinion.source_count)}` : '',
    marketOpinion?.failed_source_count != null ? `失败源 ${escapeHtml(marketOpinion.failed_source_count)}` : '',
    marketOpinion?.sector_summary_count != null ? `热点 ${escapeHtml(marketOpinion.sector_summary_count)}` : '',
  ].filter(Boolean);
  const summary = metrics.length ? metrics.join(' · ') : '暂无额外指标';
  if (!marketOpinion || !marketOpinion.failed_source_count) return summary;
  const failedText = (marketOpinion.failed_sources || [])
    .slice(0, 3)
    .map((item) => `${item.source_id}: ${item.error}`)
    .join('；');
  return `${summary}<div class="muted">部分源失败：${escapeHtml(failedText || marketOpinion.message || '-')}</div>`;
}

function renderGapNote() {
  return `
    <div class="system-gap-callout">
      <strong>当前展示策略</strong>
      <p>顶部卡片使用轻量聚合与缓存结果；K线完整度、字段缺失、估值缺口等重查询建议改为后台快照表后再展示。</p>
    </div>
    <div class="system-gap-list">
      <span>历史输入层状态归属本页，不再放回测中心。</span>
      <span>页面请求路径仍只读 MySQL 快照，不直接拉 AkShare/Tavily。</span>
    </div>
  `;
}

function renderSentimentQuality(item) {
  if (!item || !item.latest_trade_date) {
    return '<div class="empty-state">暂无舆情质量数据。等待下一次真实舆情日更后展示。</div>';
  }
  const filteredPct = item.filtered_out_pct == null ? '-' : `${escapeHtml(item.filtered_out_pct)}%`;
  const avgCred = item.avg_credibility == null ? '-' : escapeHtml(Number(item.avg_credibility).toFixed(2));
  const avgQuality = item.avg_quality == null ? '-' : escapeHtml(Number(item.avg_quality).toFixed(1));
  const qualityText = (item.quality_levels || []).map((level) => `${formatQualityLevel(level.level)} ${escapeHtml(level.count)}`).join(' · ') || '-';
  const credibilityText = (item.credibility_levels || []).map((level) => `${escapeHtml(level.level || '-')}级 ${escapeHtml(level.count)}`).join(' · ') || '-';
  return `
    <div class="sentiment-quality-hero">
      <div><span>交易日</span><b>${escapeHtml(item.latest_trade_date)}</b></div>
      <div><span>覆盖股票</span><b>${escapeHtml(item.stock_count ?? 0)}</b></div>
      <div><span>平均质量</span><b>${avgQuality}</b></div>
      <div><span>平均可信</span><b>${avgCred}</b></div>
    </div>
    <div class="sentiment-flow"><span>原始 ${escapeHtml(item.raw_news_count ?? 0)}</span><i></i><span>有效 ${escapeHtml(item.effective_news_count ?? 0)}</span><i></i><span>过滤 ${escapeHtml(item.filtered_out_count ?? 0)}（${filteredPct}）</span></div>
    <div class="muted">质量分布：${qualityText}</div>
    <div class="muted">可信度分布：${credibilityText}</div>
  `;
}

function formatQualityLevel(level) {
  const map = { high: '高质量', medium: '中质量', low: '低质量', very_low: '很低' };
  return escapeHtml(map[level] || level || '-');
}

function getStatusClass(status) {
  if (status === 'success') return 'status-ok';
  if (status === 'partial_success') return 'status-warn';
  if (status === 'failed') return 'status-error';
  if (status === 'running') return 'status-warn';
  return 'status-muted';
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-system-page')?.addEventListener('click', loadSystemPage);
  await loadSystemPage();
});
