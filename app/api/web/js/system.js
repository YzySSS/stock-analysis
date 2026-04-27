async function loadSystemPage() {
  const panel = qs('#system-health-panel');
  const coveragePanel = qs('#system-coverage-panel');
  try {
    const data = await fetchJson('/api/system/status');
    const counts = data.table_counts || {};
    const latest = data.latest || {};
    const coverage = data.coverage || {};

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
        <div><strong>最新 K 线日期:</strong> ${escapeHtml(latest.daily_kline_latest_trade_date || '-')}</div>
        <div><strong>最近选股写入:</strong> ${escapeHtml(latest.selection_result_latest_created_at || '-')}</div>
        <div><strong>最近选股交易日:</strong> ${escapeHtml(latest.selection_result_latest_trade_date || '-')}</div>
        <div><strong>最近基础信息更新时间:</strong> ${escapeHtml(latest.stock_basic_latest_updated_at || '-')}</div>
      </div>
    `;

    coveragePanel.innerHTML = `
      <div><strong>股票总数:</strong> ${escapeHtml(coverage.total_stock_codes ?? '-')}</div>
      <div><strong>K 线已覆盖股票:</strong> ${escapeHtml(coverage.daily_kline_covered_codes ?? '-')} (${escapeHtml(coverage.daily_kline_coverage_pct ?? '-')}%)</div>
      <div><strong>基本面已覆盖股票:</strong> ${escapeHtml(coverage.fundamental_filled_codes ?? '-')} (${escapeHtml(coverage.fundamental_coverage_pct ?? '-')}%)</div>
    `;
  } catch (error) {
    panel.innerHTML = `<div class="error-box">加载系统状态失败: ${escapeHtml(error.message)}</div>`;
    coveragePanel.innerHTML = `<div class="error-box">加载覆盖率失败: ${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-system-page').addEventListener('click', loadSystemPage);
  await loadSystemPage();
});
