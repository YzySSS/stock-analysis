async function loadSystemPage() {
  const panel = qs('#system-health-panel');
  const coveragePanel = qs('#system-coverage-panel');
  const latestPanel = qs('#system-latest-panel');
  const gapPanel = qs('#system-gap-panel');
  const fieldMissingPanel = qs('#system-field-missing-panel');
  const shortfallPanel = qs('#system-shortfall-panel');
  const taskRunPanel = qs('#system-task-run-panel');
  try {
    const data = await fetchJson('/api/system/status');
    const counts = data.table_counts || {};
    const latest = data.latest || {};
    const coverage = data.coverage || {};
    const fieldMissing = data.field_missing || {};
    const taskRuns = data.task_runs || [];
    const totalStockCodes = Number(coverage.total_stock_codes || 0);
    const klineCovered = Number(coverage.daily_kline_covered_codes || 0);
    const fundamentalCovered = Number(coverage.fundamental_filled_codes || 0);
    const klineMissing = Math.max(totalStockCodes - klineCovered, 0);
    const fundamentalMissing = Math.max(totalStockCodes - fundamentalCovered, 0);
    const worstFields = fieldMissing.worst_fields || [];

    panel.innerHTML = `
      <div class="status-row">
        <span class="badge ${data.status === 'ok' ? 'status-ok' : 'status-error'}">${escapeHtml(data.status || 'unknown')}</span>
        <span class="muted">DB: ${escapeHtml(data.health?.database || '-')}</span>
      </div>
      <div class="status-detail">
        <div><strong>MySQL 版本:</strong> ${escapeHtml(data.health?.version || '-')}</div>
        <div><strong>stock_basic:</strong> ${escapeHtml(counts.stock_basic ?? '-')}</div>
        <div><strong>daily_kline:</strong> ${escapeHtml(counts.daily_kline ?? '-')}</div>
        <div><strong>selection_result:</strong> ${escapeHtml(counts.selection_result ?? '-')}</div>
      </div>
    `;

    coveragePanel.innerHTML = `
      <div><strong>股票总数:</strong> ${escapeHtml(coverage.total_stock_codes ?? '-')}</div>
      <div><strong>K 线已覆盖股票:</strong> ${escapeHtml(coverage.daily_kline_covered_codes ?? '-')} (${escapeHtml(coverage.daily_kline_coverage_pct ?? '-')}%)</div>
      <div><strong>基本面已覆盖股票:</strong> ${escapeHtml(coverage.fundamental_filled_codes ?? '-')} (${escapeHtml(coverage.fundamental_coverage_pct ?? '-')}%)</div>
    `;

    latestPanel.innerHTML = `
      <div><strong>最新 K 线日期:</strong> ${escapeHtml(latest.daily_kline_latest_trade_date || '-')}</div>
      <div><strong>最近选股写入:</strong> ${escapeHtml(latest.selection_result_latest_created_at || '-')}</div>
      <div><strong>最近选股交易日:</strong> ${escapeHtml(latest.selection_result_latest_trade_date || '-')}</div>
      <div><strong>最近基础信息更新时间:</strong> ${escapeHtml(latest.stock_basic_latest_updated_at || '-')}</div>
      <div><strong>最近基本面更新时间:</strong> ${escapeHtml(latest.fundamental_latest_updated_at || '-')}</div>
    `;

    if (!taskRuns.length) {
      taskRunPanel.innerHTML = '<div class="empty-state">暂无同步任务记录</div>';
    } else {
      taskRunPanel.innerHTML = taskRuns.map((item) => {
        const meta = item.metadata || {};
        const statusClass = item.status === 'success' ? 'status-ok' : item.status === 'failed' ? 'status-error' : item.status === 'running' ? 'status-warn' : '';
        const dateRange = meta.trade_date ? `交易日 ${escapeHtml(meta.trade_date)}` : `${escapeHtml(meta.start_date || '-')} → ${escapeHtml(meta.end_date || '-')}`;
        const progress = meta.success_codes != null
          ? `成功 ${escapeHtml(meta.success_codes)} / ${escapeHtml(meta.requested_codes ?? meta.limit ?? '-')}`
          : (meta.updated != null ? `更新 ${escapeHtml(meta.updated)} / 扫描 ${escapeHtml(meta.scanned ?? '-')}` : '-');
        const extraMetrics = [
          meta.rows_synced != null ? `写入 ${escapeHtml(meta.rows_synced)} 行` : '',
          meta.failed != null ? `失败 ${escapeHtml(meta.failed)}` : '',
          meta.no_data != null ? `无数据 ${escapeHtml(meta.no_data)}` : '',
        ].filter(Boolean).join(' · ');
        return `
          <div class="preview-item task-run-item">
            <div class="preview-main">
              <div class="status-row">
                <strong>${escapeHtml(item.task_label || item.task_name)}</strong>
                <span class="badge ${statusClass}">${escapeHtml(item.status || '-')}</span>
              </div>
              <div class="muted">${dateRange}</div>
              <div class="muted">开始 ${escapeHtml(item.started_at || '-')} · 结束 ${escapeHtml(item.finished_at || '-')}</div>
              <div class="muted">${escapeHtml(item.message || '')}</div>
            </div>
            <div class="preview-side task-run-side">
              <strong>${progress}</strong>
              <span class="muted">${extraMetrics || '暂无额外指标'}</span>
            </div>
          </div>
        `;
      }).join('');
    }

    gapPanel.innerHTML = `
      <div><strong>K 线待补股票:</strong> ${klineMissing}</div>
      <div><strong>基本面待补股票:</strong> ${fundamentalMissing}</div>
      <div class="muted">当前最大缺口仍是基本面覆盖，V1 展示可信度主要受它影响。</div>
    `;

    if (!fieldMissing.items?.length) {
      fieldMissingPanel.innerHTML = '<div class="empty-state">暂无字段缺失统计</div>';
    } else {
      fieldMissingPanel.innerHTML = fieldMissing.items.map((item) => `
        <div class="preview-item">
          <div class="preview-main">
            <strong>${escapeHtml(item.field)}</strong>
            <span class="muted">覆盖率 ${formatPercent(item.coverage_pct)} · 缺失率 ${formatPercent(item.missing_rate_pct)}</span>
          </div>
          <div class="preview-side">
            <strong>${escapeHtml(item.missing_count)}</strong>
            <span class="muted">缺失数</span>
          </div>
        </div>
      `).join('');
    }

    if (!worstFields.length) {
      shortfallPanel.innerHTML = '<div class="empty-state">暂无主要短板分析</div>';
    } else {
      shortfallPanel.innerHTML = `
        <div><strong>最缺字段 Top ${worstFields.length}</strong></div>
        ${worstFields.map((item, index) => `
          <div>${index + 1}. <strong>${escapeHtml(item.field)}</strong> · 缺失 ${escapeHtml(item.missing_count)} 条 · 覆盖率 ${formatPercent(item.coverage_pct)}</div>
        `).join('')}
        <div class="muted">这几个字段会直接影响选股解释、因子分析和单票详情的可信度。</div>
      `;
    }
  } catch (error) {
    panel.innerHTML = `<div class="error-box">加载系统状态失败: ${escapeHtml(error.message)}</div>`;
    coveragePanel.innerHTML = `<div class="error-box">加载覆盖率失败: ${escapeHtml(error.message)}</div>`;
    latestPanel.innerHTML = `<div class="error-box">加载最近同步结果失败: ${escapeHtml(error.message)}</div>`;
    gapPanel.innerHTML = `<div class="error-box">加载缺口提示失败: ${escapeHtml(error.message)}</div>`;
    fieldMissingPanel.innerHTML = `<div class="error-box">加载字段缺失统计失败: ${escapeHtml(error.message)}</div>`;
    shortfallPanel.innerHTML = `<div class="error-box">加载主要短板分析失败: ${escapeHtml(error.message)}</div>`;
    taskRunPanel.innerHTML = `<div class="error-box">加载同步任务状态失败: ${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-system-page').addEventListener('click', loadSystemPage);
  await loadSystemPage();
});
