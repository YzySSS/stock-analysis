function renderSelectionHistory(items) {
  const container = qs('#stock-detail-history');
  if (!items || !items.length) {
    container.innerHTML = '<div class="empty-state">暂无历史入选记录</div>';
    return;
  }

  container.innerHTML = `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>交易日</th>
            <th>策略</th>
            <th>分数</th>
            <th>排名</th>
            <th>run_id</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(item.trade_date || '-')}</td>
              <td>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')}</td>
              <td>${formatNumber(item.score, 4)}</td>
              <td>${escapeHtml(item.rank_no ?? '-')}</td>
              <td>${escapeHtml(item.run_id || '-')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

async function loadStockDetail() {
  const code = decodeURIComponent(window.location.pathname.split('/').pop() || 'UNKNOWN');
  qs('#stock-detail-title').textContent = `个股详情: ${code}`;

  try {
    const data = await fetchJson(`/api/stocks/${encodeURIComponent(code)}`);
    qs('#stock-detail-title').textContent = `${escapeHtml(data.name || code)} (${escapeHtml(data.code || code)})`;
    qs('#stock-detail-subtitle').textContent = `${escapeHtml(data.industry || '未分类行业')} · ${escapeHtml(data.market || '-')} · ${escapeHtml(data.instrument_type || '-')}`;

    qs('#stock-detail-basic').innerHTML = `
      <div><strong>股票代码</strong></div>
      <div>${escapeHtml(data.code || '-')}</div>
      <div><strong>上市日期</strong></div>
      <div>${escapeHtml(data.listing_date || '-')}</div>
      <div><strong>行业</strong></div>
      <div>${escapeHtml(data.industry || '-')}</div>
      <div><strong>ST / 退市</strong></div>
      <div>${data.flags?.is_st ? 'ST' : '正常'} / ${data.flags?.is_delisted ? '已退市' : '未退市'}</div>
      <div><strong>最新收盘</strong></div>
      <div>${formatNumber(data.latest_kline?.close, 2)}</div>
      <div><strong>日内涨跌幅</strong></div>
      <div class="${getPctClass(data.latest_kline?.intraday_change_pct)}">${formatPercent(data.latest_kline?.intraday_change_pct)}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(data.latest_kline?.trade_date || '-')}</div>
    `;

    const latestSelection = data.latest_selection || {};
    const factorScores = latestSelection.factor_scores || {};
    qs('#stock-detail-factors').innerHTML = `
      <div><strong>最近选股分数</strong></div>
      <div>${formatNumber(latestSelection.score, 4)}</div>
      <div><strong>最近入选策略</strong></div>
      <div>${escapeHtml(latestSelection.strategy_display_name || latestSelection.strategy_id || '-')}</div>
      <div><strong>Turnover</strong></div>
      <div>${formatNumber(factorScores.turnover, 4)}</div>
      <div><strong>LowVol</strong></div>
      <div>${formatNumber(factorScores.lowvol, 4)}</div>
      <div><strong>Reversal</strong></div>
      <div>${formatNumber(factorScores.reversal, 4)}</div>
      <div><strong>PE / PB</strong></div>
      <div>${formatNumber(data.valuation?.pe_tushare, 2)} / ${formatNumber(data.valuation?.pb_tushare, 2)}</div>
      <div><strong>ROE / ROA</strong></div>
      <div>${formatNumber(data.fundamentals?.roe, 2)} / ${formatNumber(data.fundamentals?.roa, 2)}</div>
    `;

    renderSelectionHistory(data.selection_history || []);
  } catch (error) {
    qs('#stock-detail-subtitle').textContent = '加载详情失败';
    qs('#stock-detail-basic').innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    qs('#stock-detail-factors').innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    qs('#stock-detail-history').innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', loadStockDetail);
