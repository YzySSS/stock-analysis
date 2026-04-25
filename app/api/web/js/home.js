async function loadHomePage() {
  const [healthResult, strategiesResult, trackingResult] = await Promise.allSettled([
    fetchJson('/api/health'),
    fetchJson('/api/strategies'),
    fetchJson('/api/tracking/latest?limit=5&instrument_type=stock'),
  ]);

  const healthStatus = qs('#home-health-status');
  const healthDetail = qs('#home-health-detail');
  if (healthResult.status === 'fulfilled') {
    const health = healthResult.value;
    healthStatus.textContent = health.status || 'ok';
    healthStatus.classList.add(health.status === 'ok' ? 'up' : 'down');
    healthDetail.textContent = health.message || '健康检查正常';
  } else {
    healthStatus.textContent = '失败';
    healthStatus.classList.add('down');
    healthDetail.textContent = healthResult.reason.message;
  }

  const defaultStrategy = qs('#home-default-strategy');
  if (strategiesResult.status === 'fulfilled') {
    defaultStrategy.textContent = strategiesResult.value.default_strategy || '-';
  } else {
    defaultStrategy.textContent = '加载失败';
  }

  const trackingCount = qs('#home-tracking-count');
  const trackingPreview = qs('#home-tracking-preview');
  if (trackingResult.status === 'fulfilled') {
    const items = trackingResult.value.items || [];
    trackingCount.textContent = String(items.length);
    if (!items.length) {
      trackingPreview.innerHTML = renderEmptyRow(4, '暂无跟踪数据');
    } else {
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
    }
  } else {
    trackingCount.textContent = '加载失败';
    trackingPreview.innerHTML = renderEmptyRow(4, trackingResult.reason.message);
  }
}

document.addEventListener('DOMContentLoaded', loadHomePage);
