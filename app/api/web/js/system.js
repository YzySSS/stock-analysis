async function loadSystemPage() {
  const started = performance.now();
  const panels = {
    api: qs('#system-api-panel'),
    db: qs('#system-db-panel'),
    latestDate: qs('#system-latest-date-panel'),
    lastRun: qs('#system-last-run-panel'),
    baseline: qs('#system-baseline-panel'),
    baselineOverall: qs('#system-baseline-overall'),
    readiness: qs('#system-readiness-panel'),
    readinessOverall: qs('#system-readiness-overall'),
    coverage: qs('#system-coverage-panel'),
    gap: qs('#system-gap-panel'),
    fieldMissing: qs('#system-field-missing-panel'),
    shortfall: qs('#system-shortfall-panel'),
    taskRun: qs('#system-task-run-panel'),
    sentiment: qs('#system-sentiment-quality-panel'),
    jobErrors: qs('#system-job-error-panel'),
    realtimeLifecycle: qs('#system-realtime-lifecycle-panel'),
    marketOpinionStorage: qs('#system-market-opinion-storage-panel'),
  };

  try {
    const data = await fetchJson('/api/system/status');
    const elapsedMs = Math.round(performance.now() - started);
    const latest = data.latest || {};
    const sentimentQuality = data.sentiment_quality || {};
    const dataQuality = data.data_quality || {};
    const taskRuns = data.task_runs || [];
    const schedules = data.scheduled_tasks || [];
    const marketOpinion = data.market_opinion_update || null;
    const readiness = data.readiness || {};
    const taskMap = Object.fromEntries(taskRuns.map((item) => [item.task_name, item]));

    panels.api.innerHTML = renderTopMetricCard({
      icon: '⌘',
      title: 'API状态',
      state: data.status === 'ok' ? '正常' : '异常',
      stateClass: data.status === 'ok' ? 'ok' : 'error',
      lines: [`响应时间 ${elapsedMs} ms`, `Readiness ${formatReadinessStatus(readiness.status)}`],
    });
    panels.db.innerHTML = renderTopMetricCard({
      icon: '▣',
      title: '数据库状态',
      state: data.health?.database ? '正常' : '异常',
      stateClass: data.health?.database ? 'ok' : 'error',
      lines: [`MySQL ${data.health?.version || '-'}`, `状态缓存 ${data.cache?.ttl_seconds ?? '-'}s`],
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
      lines: [`本次耗时 ${elapsedMs}ms`, `有状态 ${taskRuns.length} / 已登记 ${schedules.length}`],
    });

    renderBaseline(data.data_baseline || {}, panels.baseline, panels.baselineOverall);
    renderReadiness(readiness, panels.readiness, panels.readinessOverall);
    panels.coverage.innerHTML = renderScheduleGrid(schedules, taskMap, taskRuns, marketOpinion);
    if (panels.taskRun) panels.taskRun.innerHTML = '';
    panels.gap.innerHTML = renderDataQuality(dataQuality);
    panels.sentiment.innerHTML = renderSentimentQuality(sentimentQuality);
    panels.jobErrors.innerHTML = renderJobErrors(data.job_error_summary || [], data.retention_policy || {});
    panels.realtimeLifecycle.innerHTML = renderRealtimeLifecycle(data.realtime_lifecycle || {});
    panels.marketOpinionStorage.innerHTML = renderMarketOpinionStorage(data.market_opinion_storage || {});
    if (panels.fieldMissing) panels.fieldMissing.innerHTML = '';
    if (panels.shortfall) panels.shortfall.innerHTML = '';
  } catch (error) {
    Object.values(panels).forEach((panel) => {
      if (panel && panel !== panels.baselineOverall) panel.innerHTML = `<div class="error-box">加载系统状态失败: ${escapeHtml(error.message)}</div>`;
    });
  }
}

function formatReadinessStatus(status) {
  return ({ ready: '就绪', degraded: '降级', not_ready: '未就绪' })[status] || '未知';
}

function renderReadiness(readiness = {}, container, overallEl) {
  const workers = readiness.workers || [];
  const queues = Object.fromEntries((readiness.queues || []).map((item) => [item.job_type, item]));
  const migrations = readiness.schema_migrations || {};
  const status = readiness.status || 'not_ready';
  if (overallEl) {
    overallEl.textContent = `${formatReadinessStatus(status)}${readiness.reasons?.length ? ` · ${readiness.reasons.length} 项提示` : ''}`;
    overallEl.className = `system-section-note ${status === 'ready' ? 'ok' : status === 'degraded' ? 'warn' : 'error'}`;
  }
  const migrationCard = `
    <article class="system-task-card">
      <div class="system-task-card-head">
        <strong>Schema Migration</strong>
        <span class="badge ${migrations.health === 'healthy' ? 'status-ok' : 'status-error'}">${migrations.health === 'healthy' ? '已就绪' : '待迁移'}</span>
      </div>
      <div class="system-task-schedule">目标版本 ${escapeHtml(migrations.target || '-')}</div>
      <div class="system-task-times">
        <span>已应用 ${escapeHtml(migrations.applied ?? '-')}</span>
        <span>待执行 ${escapeHtml(migrations.pending ?? '-')}</span>
      </div>
      <div class="system-task-result">${migrations.pending_versions?.length ? `待执行 ${escapeHtml(migrations.pending_versions.join(', '))}` : '数据库结构已通过启动前检查'}</div>
    </article>
  `;
  const workerCards = workers.map((worker) => {
    const queue = queues[worker.worker_type] || {};
    const healthClass = worker.health === 'healthy' && queue.health === 'healthy'
      ? 'status-ok'
      : worker.health === 'stale' || worker.health === 'missing' || queue.health === 'error'
        ? 'status-error'
        : 'status-warn';
    const statusLabel = worker.health === 'healthy' ? (worker.process_status === 'running' ? '执行中' : '在线') : worker.health;
    return `
      <article class="system-task-card">
        <div class="system-task-card-head">
          <strong>${escapeHtml(worker.label || worker.worker_type)}</strong>
          <span class="badge ${healthClass}">${escapeHtml(statusLabel || '-')}</span>
        </div>
        <div class="system-task-schedule">心跳 ${escapeHtml(worker.heartbeat_age_seconds ?? '-')} 秒前 · 阈值 ${escapeHtml(worker.stale_after_seconds ?? '-')} 秒</div>
        <div class="system-task-times">
          <span>排队 ${escapeHtml(queue.queued_count ?? 0)}</span>
          <span>运行 ${escapeHtml(queue.running_count ?? 0)}</span>
        </div>
        <div class="system-task-result">
          当前任务 ${escapeHtml(worker.current_job_id || '空闲')}
          <div class="muted">失联 ${escapeHtml(queue.stale_running_count ?? 0)} · 24h失败 ${escapeHtml(queue.failed_24h_count ?? 0)}</div>
        </div>
      </article>
    `;
  }).join('');
  const emptyWorkers = workers.length ? '' : '<div class="empty-state">暂无 worker 心跳；请检查对应 systemd 服务。</div>';
  container.innerHTML = migrationCard + workerCards + emptyWorkers;
}

function renderJobErrors(items = [], policy = {}) {
  const policyText = [
    `任务明细 ${policy.task_run_log_detail_days ?? '-'} 天`,
    `选股任务 ${policy.selection_task_days ?? '-'} 天`,
    `系统回测 ${policy.backtest_system_test_days ?? '-'} 天`,
    `AI原文 ${policy.portfolio_raw_response_days ?? '-'} 天`,
    `错误汇总 ${policy.structured_error_summary_days ?? '-'} 天`,
  ].join(' · ');
  const errorRows = items.length
    ? items.slice(0, 8).map((item) => `
        <div class="system-error-summary-row">
          <b>${escapeHtml(item.job_type || '-')} · ${escapeHtml(item.error_code || '-')}</b>
          <span>${escapeHtml(item.occurrence_count ?? 0)} 次 · 最近 ${escapeHtml(item.last_seen_at || '-')}</span>
          <small>${escapeHtml(item.last_message || '-')}</small>
        </div>
      `).join('')
    : '<div class="empty-state">近 7 天暂无已聚合错误。</div>';
  return `
    <div class="system-gap-callout"><strong>保留口径</strong><p>${escapeHtml(policyText)}</p></div>
    <div class="system-error-summary-list">${errorRows}</div>
  `;
}

function renderRealtimeLifecycle(item = {}) {
  const policy = item.policy || {};
  const raw = item.raw || {};
  const rollup = item.rollup || {};
  const tracked = item.tracked || {};
  const manifests = item.latest_manifests || [];
  const manifestRows = manifests.length
    ? manifests.map((manifest) => {
        const statusClass = manifest.status === 'success' ? 'status-ok' : manifest.status === 'partial' ? 'status-warn' : 'status-error';
        return `
          <div class="system-error-summary-row">
            <b>${escapeHtml(manifest.interval_minutes || '-')}m 汇总 · ${escapeHtml(manifest.trade_date || '-')}</b>
            <span class="badge ${statusClass}">${escapeHtml(manifest.status || '-')}</span>
            <small>${escapeHtml(manifest.rollup_rows ?? 0)} 条 · ${escapeHtml(manifest.rollup_codes ?? 0)} 只 · 行情截至 ${escapeHtml(manifest.last_quote_minute || '-')}</small>
          </div>
        `;
      }).join('')
    : '<div class="empty-state">暂无分钟汇总 manifest。</div>';
  const partitionLabel = raw.partitioned ? `${raw.daily_partitions ?? 0} 个日分区` : '尚未分区';
  return `
    <div class="system-gap-callout">
      <strong>原始 1m：${escapeHtml(raw.trade_days ?? 0)} 个交易日</strong>
      <p>全市场保留 ${escapeHtml(policy.full_market_raw_trade_days ?? '-')} 日 · ${escapeHtml(partitionLabel)} · 约 ${escapeHtml(raw.allocated_mb ?? '-')} MiB</p>
    </div>
    <div class="system-gap-list">
      <span>5m/15m 汇总保留 ${escapeHtml(policy.rollup_trade_days ?? '-')} 个交易日，当前约 ${escapeHtml(rollup.approx_rows ?? 0)} 行。</span>
      <span>持仓/跟踪股 1m 保留 ${escapeHtml(policy.tracked_raw_trade_days ?? '-')} 个交易日，当前约 ${escapeHtml(tracked.approx_rows ?? 0)} 行。</span>
    </div>
    <div class="system-error-summary-list">${manifestRows}</div>
  `;
}

function renderMarketOpinionStorage(item = {}) {
  const policy = item.policy || {};
  const tables = item.tables || {};
  const parent = tables.sector_opinion_daily || {};
  const stocks = tables.sector_opinion_stock || {};
  const news = tables.sector_opinion_news_ref || {};
  const versions = item.latest_payload_versions || [];
  const latestV2 = versions.find((entry) => Number(entry.payload_version) === 2)?.rows || 0;
  const versionText = versions.length
    ? versions.map((entry) => `v${entry.payload_version}: ${entry.rows}`).join(' · ')
    : '暂无快照';
  return `
    <div class="system-gap-callout">
      <strong>最新快照 ${escapeHtml(item.latest_as_of || '-')}</strong>
      <p>${escapeHtml(versionText)} · v2 关系化 ${escapeHtml(latestV2)} 行</p>
    </div>
    <div class="system-gap-list">
      <span>盘中全量保留 ${escapeHtml(policy.intraday_trade_days ?? '-')} 个交易日，较老日期仅留日末快照至 ${escapeHtml(policy.daily_trade_days ?? '-')} 个交易日。</span>
      <span>父快照约 ${escapeHtml(parent.approx_rows ?? 0)} 行 / ${escapeHtml(parent.allocated_mb ?? '-')} MiB。</span>
      <span>股票关系约 ${escapeHtml(stocks.approx_rows ?? 0)} 行，新闻引用约 ${escapeHtml(news.approx_rows ?? 0)} 行。</span>
    </div>
  `;
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
    meta.rows != null ? `数据 ${escapeHtml(meta.rows)} 行` : '',
    meta.success_codes != null ? `成功 ${escapeHtml(meta.success_codes)} / ${escapeHtml(meta.requested_codes ?? meta.limit ?? '-')}` : '',
    meta.updated != null ? `更新 ${escapeHtml(meta.updated)} / 扫描 ${escapeHtml(meta.scanned ?? '-')}` : '',
    meta.updated_count != null ? `更新 ${escapeHtml(meta.updated_count)}` : '',
    meta.failed_count != null ? `失败 ${escapeHtml(meta.failed_count)}` : '',
    meta.rows_synced != null ? `写入 ${escapeHtml(meta.rows_synced)} 行` : '',
    meta.failed != null ? `失败 ${escapeHtml(meta.failed)}` : '',
    meta.no_data != null ? `无数据 ${escapeHtml(meta.no_data)}` : '',
    marketOpinion?.source_count != null ? `源 ${escapeHtml(marketOpinion.source_count)}` : '',
    marketOpinion?.failed_source_count != null ? `失败源 ${escapeHtml(marketOpinion.failed_source_count)}` : '',
    marketOpinion?.sector_summary_count != null ? `热点 ${escapeHtml(marketOpinion.sector_summary_count)}` : '',
    meta.source_used ? `来源 ${escapeHtml(meta.source_used)}` : '',
    meta.counts ? `质量 通过${escapeHtml(meta.counts.pass ?? 0)} / 提示${escapeHtml(meta.counts.warn ?? 0)} / 失败${escapeHtml(meta.counts.fail ?? 0)}` : '',
  ].filter(Boolean);
  const summary = metrics.length ? metrics.join(' · ') : '暂无额外指标';
  const sourceErrors = meta.source_errors && typeof meta.source_errors === 'object'
    ? Object.entries(meta.source_errors).slice(0, 3).map(([source, error]) => `${source}: ${error}`).join('；')
    : '';
  const opinionErrors = marketOpinion?.failed_source_count
    ? (marketOpinion.failed_sources || []).slice(0, 3).map((item) => `${item.source_id}: ${item.error}`).join('；')
    : '';
  const failureText = run.status === 'failed' || run.status === 'stale'
    ? (run.message || meta.error || (run.status === 'stale' ? '运行时间超过 1 小时，需检查或回收' : '任务失败'))
    : '';
  const detail = sourceErrors || opinionErrors || failureText;
  if (!detail) return summary;
  const prefix = run.status === 'failed' || run.status === 'stale' ? '异常' : '降级';
  return `${summary}<div class="muted">${prefix}：${escapeHtml(detail)}</div>`;
}

function formatDataQualityGapSample(sample = {}) {
  const code = sample.code || '-';
  const details = [];
  if (sample.classification === 'historical_universe_missing') {
    const missing = [];
    if (sample.missing_kline) missing.push('日线');
    if (sample.missing_factor_input) missing.push('因子');
    if (missing.length) details.push(`缺 ${missing.join('/')}`);
    if (sample.delisting_date) details.push(`退市 ${sample.delisting_date}`);
    return details.length ? `${code}：${details.join('；')}` : code;
  }
  if (sample.classification === 'fundamental_asof_missing') {
    if (sample.trade_date) details.push(`截至 ${sample.trade_date} 无已公告财务版本`);
    if (sample.listing_date) details.push(`上市 ${sample.listing_date}`);
    return details.length ? `${code}：${details.join('；')}` : code;
  }
  if (sample.consecutive_missing_trade_days != null) {
    const capped = sample.persistence_capped ? '+' : '';
    details.push(`连续 ${sample.consecutive_missing_trade_days}${capped} 个交易日`);
  }
  if (sample.last_success_trade_date || sample.last_success_source) {
    details.push(`上次成功 ${sample.last_success_trade_date || '-'} / ${sample.last_success_source || '来源未知'}`);
  } else {
    details.push('回溯窗口内无成功记录');
  }
  if (sample.last_attempt_at) {
    details.push(`最近尝试 ${sample.last_attempt_at}${sample.last_attempt_status ? `（${sample.last_attempt_status}）` : ''}`);
  }
  return details.length ? `${code}：${details.join('；')}` : code;
}

function renderDataQuality(item = {}) {
  if (!item.generated_at) {
    return '<div class="empty-state">尚无离线质量快照，等待 04:55 或 18:45 自动审计。</div>';
  }
  const counts = item.counts || {};
  const checks = item.checks || [];
  const noteworthy = checks.filter((check) => check.status !== 'pass');
  const rows = noteworthy.length
    ? noteworthy.map((check) => {
        const badgeClass = check.status === 'fail' ? 'status-error' : 'status-warn';
        const statusLabel = check.status === 'fail' ? '失败' : '提示';
        const rawSamples = check.samples || [];
        const actionableSamples = rawSamples.filter((sample) => sample.classification === 'actionable_missing');
        const samples = (actionableSamples.length ? actionableSamples : rawSamples)
          .slice(0, 5)
          .map(formatDataQualityGapSample);
        const sampleText = samples.length ? `<div class="muted">追溯：${escapeHtml(samples.join('｜'))}</div>` : '';
        return `
          <div class="system-error-summary-row">
            <b>${escapeHtml(check.label || check.check_id || '-')}</b>
            <span class="badge ${badgeClass}">${statusLabel}</span>
            <small>${escapeHtml(check.message || '-')}</small>
            ${sampleText}
          </div>
        `;
      }).join('')
    : '<div class="empty-state">本批次全部质量规则通过。</div>';
  const healthLabel = item.status === 'fail' ? '存在硬失败' : item.status === 'warn' ? '有可解释告警' : '全部通过';
  return `
    <div class="system-gap-callout">
      <strong>${escapeHtml(healthLabel)} · ${escapeHtml(item.reference_trade_date || '-')}</strong>
      <p>${escapeHtml((item.audit_version || 'dq1').toUpperCase())} 于 ${escapeHtml(item.generated_at)} 完成：通过 ${escapeHtml(counts.pass ?? 0)}，提示 ${escapeHtml(counts.warn ?? 0)}，失败 ${escapeHtml(counts.fail ?? 0)}。</p>
    </div>
    <div class="system-error-summary-list">${rows}</div>
    <div class="muted">待处理样本最多回溯 ${escapeHtml(item.history_lookback_trade_days ?? '-')} 个交易日，并记录上次成功来源与最近上游尝试；PIT 上市/ST/退市与停复牌覆盖也由离线任务核验。PE 缺失仍不作为硬故障，页面不扫描行情大表。</div>
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
  if (status === 'stale') return 'status-error';
  if (status === 'running') return 'status-warn';
  return 'status-muted';
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-system-page')?.addEventListener('click', loadSystemPage);
  await loadSystemPage();
});
