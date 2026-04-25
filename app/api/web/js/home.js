async function loadHomePage() {
  const healthStatus = qs('#home-health-status');
  const healthDetail = qs('#home-health-detail');
  const defaultStrategy = qs('#home-default-strategy');
  const trackingCount = qs('#home-tracking-count');
  const trackingPreview = qs('#home-tracking-preview');

  try {
    const data = await fetchJson('/api/dashboard/summary?limit=5');
    const health = data.health || {};
    const items = data.latest_tracking_preview || [];

    healthStatus.textContent = health.status || 'ok';
    healthStatus.classList.remove('up', 'down');
    healthStatus.classList.add(health.status === 'ok' ? 'up' : 'down');
    healthDetail.textContent = health.database
      ? `MySQL: ${health.database} · ${health.version || 'unknown version'}`
      : '健康检查正常';

    defaultStrategy.textContent = data.default_strategy || '-';
    trackingCount.textContent = String(data.latest_tracking_count ?? items.length ?? 0);

    if (!items.length) {
      trackingPreview.innerHTML = renderEmptyRow(4, '暂无跟踪数据');
      return;
    }

    trackingPreview.innerHTML = items.map((item) => {
      const pct = item.price_change_pct;
      return `
        <tr>
          <td><a href="/stocks/${encodeURIComponent(item.code || '')}">${escapeHtml(item.code || '')}</a></td>
          <td>${escapeHtml(item.name || '')}</td>
          <td>${escapeHtml(item.strategy_display_name || item.strategy_id || '')}</td>
          <td class="${getPctClass(pct)}">${formatPercent(pct)}</td>
        </tr>
      `;
    }).join('');
  } catch (error) {
    healthStatus.textContent = '失败';
    healthStatus.classList.remove('up');
    healthStatus.classList.add('down');
    healthDetail.textContent = error.message;
    defaultStrategy.textContent = '加载失败';
    trackingCount.textContent = '加载失败';
    trackingPreview.innerHTML = renderEmptyRow(4, error.message);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadHomePage();
});
