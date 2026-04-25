async function loadStrategiesPage() {
  const data = await fetchJson('/api/strategies');
  const list = qs('#strategies-list');
  const items = data.strategies || [];

  qs('#strategies-default').textContent = data.default_strategy || '-';
  qs('#strategies-count').textContent = String(items.length);

  if (!items.length) {
    list.innerHTML = '<div class="empty-state">暂无策略数据</div>';
    return;
  }

  list.innerHTML = items.map((item) => `
    <article class="strategy-card">
      <div class="strategy-item-head">
        <h3>${escapeHtml(item.display_name || item.id)}</h3>
        ${item.id === data.default_strategy ? '<span class="badge status-ok">默认策略</span>' : ''}
      </div>
      <div class="muted">ID: ${escapeHtml(item.id)} · 版本: ${escapeHtml(item.version || '-')} · 状态: ${escapeHtml(item.status || '-')}</div>
      <p>${escapeHtml(item.description || '暂无描述')}</p>
    </article>
  `).join('');
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-strategies-page').addEventListener('click', loadStrategiesPage);

  try {
    await loadStrategiesPage();
  } catch (error) {
    renderError(qs('#strategies-list'), `加载策略失败: ${error.message}`);
    qs('#strategies-default').textContent = '加载失败';
    qs('#strategies-count').textContent = '-';
  }
});
