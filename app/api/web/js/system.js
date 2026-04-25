async function loadSystemPage() {
  const panel = qs('#system-health-panel');
  try {
    const data = await fetchJson('/api/health');
    panel.innerHTML = `
      <div class="status-row">
        <span class="badge ${data.status === 'ok' ? 'status-ok' : 'status-error'}">${escapeHtml(data.status || 'unknown')}</span>
      </div>
      <div class="status-detail">
        <div><strong>message:</strong> ${escapeHtml(data.message || '-')}</div>
        <div><strong>raw:</strong></div>
        <pre class="code-block small-code">${escapeHtml(JSON.stringify(data, null, 2))}</pre>
      </div>
    `;
  } catch (error) {
    panel.innerHTML = `<div class="error-box">加载健康状态失败: ${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  qs('#refresh-system-page').addEventListener('click', loadSystemPage);
  await loadSystemPage();
});
