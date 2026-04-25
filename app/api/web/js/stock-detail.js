function loadStockDetailPlaceholder() {
  const code = decodeURIComponent(window.location.pathname.split('/').pop() || 'UNKNOWN');
  qs('#stock-detail-title').textContent = `个股详情: ${code}`;
  qs('#stock-detail-subtitle').textContent = `当前为 ${code} 的详情占位页，后面可接股票基础信息、因子数据和历史入选记录。`;
  qs('#stock-detail-basic').innerHTML = `
    <div><strong>股票代码</strong></div>
    <div>${escapeHtml(code)}</div>
    <div><strong>状态</strong></div>
    <div>待接接口</div>
  `;
  qs('#stock-detail-factors').innerHTML = `
    <div><strong>综合评分</strong></div>
    <div>-</div>
    <div><strong>Turnover / LowVol / Reversal</strong></div>
    <div>待接接口</div>
  `;
}

document.addEventListener('DOMContentLoaded', loadStockDetailPlaceholder);
