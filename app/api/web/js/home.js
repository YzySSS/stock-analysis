async function loadHomePage() {
  const healthStatus = qs('#home-health-status');
  const healthDetail = qs('#home-health-detail');
  const defaultStrategy = qs('#home-default-strategy');
  const latestTradeDate = qs('#home-latest-trade-date');
  const trackingCount = qs('#home-tracking-count');
  const avgChange = qs('#home-avg-change');
  const trackingPreview = qs('#home-tracking-preview');
  const selectionSummary = qs('#home-selection-summary');
  const dataStatus = qs('#home-data-status');

  try {
    const data = await fetchJson('/api/dashboard/summary?limit=5');
    const health = data.health || {};
    const items = data.latest_tracking_preview || [];
    const latestSelectionSummary = data.latest_selection_summary || null;
    const dataStats = data.data_stats || {};

    healthStatus.textContent = health.status || 'ok';
    healthStatus.classList.remove('up', 'down');
    healthStatus.classList.add(health.status === 'ok' ? 'up' : 'down');
    healthDetail.textContent = health.database
      ? `MySQL: ${health.database} · ${health.version || '版本未知'}`
      : '健康检查正常';

    defaultStrategy.textContent = data.default_strategy || '-';
    latestTradeDate.textContent = data.latest_trade_date || '-';
    trackingCount.textContent = String(data.latest_tracking_count ?? items.length ?? 0);
    avgChange.textContent = formatPercent(data.latest_tracking_avg_price_change_pct);
    avgChange.classList.remove('up', 'down');
    const avgChangeClass = getPctClass(data.latest_tracking_avg_price_change_pct);
    if (avgChangeClass) {
      avgChange.classList.add(avgChangeClass);
    }

    if (!items.length) {
      trackingPreview.innerHTML = '<div class="empty-state">暂无跟踪数据</div>';
    } else {
      trackingPreview.innerHTML = items.map((item) => {
        const pct = item.price_change_pct;
        return `
          <a class="preview-item" href="/stocks/${encodeURIComponent(item.code || '')}">
            <div class="preview-main">
              <strong>${escapeHtml(item.code || '')}</strong>
              <span>${escapeHtml(item.name || '')}</span>
            </div>
            <div class="preview-side">
              <span class="muted">${escapeHtml(item.strategy_display_name || item.strategy_id || '')}</span>
              <strong class="${getPctClass(pct)}">${formatPercent(pct)}</strong>
            </div>
          </a>
        `;
      }).join('');
    }

    if (!latestSelectionSummary) {
      selectionSummary.innerHTML = '<div class="empty-state">暂无最近一次选股摘要</div>';
    } else {
      selectionSummary.innerHTML = `
        <article class="strategy-item">
          <div class="strategy-item-head">
            <strong>${escapeHtml(latestSelectionSummary.strategy_display_name || '-')}</strong>
            <span class="badge status-ok">${escapeHtml(latestSelectionSummary.pick_count ?? 0)} 只</span>
          </div>
          <div class="muted">run_id: ${escapeHtml(latestSelectionSummary.run_id || '-')}</div>
          <div class="muted">选股日期：${escapeHtml(latestSelectionSummary.selected_trade_date || '-')}</div>
          <div class="muted">Top 3：</div>
          <div class="preview-list">
            ${(latestSelectionSummary.top_items || []).map((item) => `
              <div class="preview-item">
                <div class="preview-main">
                  <strong>${escapeHtml(item.code || '')}</strong>
                  <span>${escapeHtml(item.name || '')}</span>
                </div>
                <div class="preview-side">
                  <span class="muted">分数 ${formatNumber(item.score, 4)}</span>
                  <strong class="${getPctClass(item.price_change_pct)}">${formatPercent(item.price_change_pct)}</strong>
                </div>
              </div>
            `).join('')}
          </div>
        </article>
      `;
    }

    dataStatus.innerHTML = `
      <article class="strategy-item">
        <div><strong>历史 K 线覆盖：</strong>${escapeHtml(dataStats.daily_kline_covered_codes ?? '-')} / ${escapeHtml(dataStats.total_stock_codes ?? '-')} (${escapeHtml(dataStats.daily_kline_coverage_pct ?? '-')}%)</div>
        <div><strong>历史 K 线总行数：</strong>${escapeHtml(dataStats.daily_kline_rows ?? '-')}</div>
        <div><strong>基本面覆盖：</strong>${escapeHtml(dataStats.fundamental_filled_codes ?? '-')} / ${escapeHtml(dataStats.total_stock_codes ?? '-')} (${escapeHtml(dataStats.fundamental_coverage_pct ?? '-')}%)</div>
        <div><strong>最新 K 线日期：</strong>${escapeHtml(dataStats.daily_kline_latest_trade_date || '-')}</div>
        <div><strong>最近基本面更新时间：</strong>${escapeHtml(dataStats.fundamental_latest_updated_at || '-')}</div>
        <div class="muted">当前首页先重点回答：数据够不够、最近一次选了谁、链路是不是活着。</div>
      </article>
    `;
  } catch (error) {
    healthStatus.textContent = '失败';
    healthStatus.classList.remove('up');
    healthStatus.classList.add('down');
    healthDetail.textContent = error.message;
    defaultStrategy.textContent = '加载失败';
    latestTradeDate.textContent = '加载失败';
    trackingCount.textContent = '加载失败';
    avgChange.textContent = '加载失败';
    trackingPreview.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    selectionSummary.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    dataStatus.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadHomePage();
});
