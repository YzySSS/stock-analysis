function renderPriceChart(history) {
  const svg = qs('#stock-price-chart');
  if (!svg) return;
  if (!history || history.length < 2) {
    svg.innerHTML = '';
    return;
  }

  const points = history.filter((item) => item.close != null);
  if (points.length < 2) {
    svg.innerHTML = '';
    return;
  }

  const width = 640;
  const height = 220;
  const padding = 18;
  const values = points.map((item) => Number(item.close));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const polyline = points.map((item, index) => {
    const x = padding + (index * (width - padding * 2)) / (points.length - 1);
    const y = height - padding - ((Number(item.close) - min) / range) * (height - padding * 2);
    return `${x},${y}`;
  }).join(' ');

  const last = values[values.length - 1];
  const first = values[0];
  const positive = last >= first;
  const stroke = positive ? '#22c55e' : '#ef4444';

  svg.innerHTML = `
    <polyline fill="none" stroke="${stroke}" stroke-width="3" points="${polyline}" />
    <line x1="18" y1="202" x2="622" y2="202" stroke="#334155" stroke-width="1" />
    <text x="18" y="18" fill="#94a3b8" font-size="12">最高 ${max.toFixed(2)}</text>
    <text x="18" y="198" fill="#94a3b8" font-size="12">最低 ${min.toFixed(2)}</text>
    <text x="540" y="18" fill="#94a3b8" font-size="12">最近 ${last.toFixed(2)}</text>
  `;
}

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
    const latestSelection = data.latest_selection || {};
    const factorScores = latestSelection.factor_scores || {};

    qs('#stock-detail-title').textContent = `${escapeHtml(data.name || code)} (${escapeHtml(data.code || code)})`;
    qs('#stock-detail-subtitle').textContent = `${escapeHtml(data.industry || '未分类行业')} · ${escapeHtml(data.market || '-')} · ${escapeHtml(data.instrument_type || '-')}`;

    qs('#stock-stat-close').textContent = formatNumber(data.latest_kline?.close, 2);
    qs('#stock-stat-change').textContent = formatPercent(data.latest_kline?.intraday_change_pct);
    qs('#stock-stat-change').classList.remove('up', 'down');
    qs('#stock-stat-change').classList.add(getPctClass(data.latest_kline?.intraday_change_pct));
    qs('#stock-stat-score').textContent = formatNumber(latestSelection.score, 4);
    qs('#stock-stat-date').textContent = escapeHtml(data.latest_kline?.trade_date || '-');

    qs('#stock-detail-basic').innerHTML = `
      <div><strong>股票代码</strong></div>
      <div>${escapeHtml(data.code || '-')}</div>
      <div><strong>市场</strong></div>
      <div>${escapeHtml(data.market || '-')}</div>
      <div><strong>行业</strong></div>
      <div>${escapeHtml(data.industry || '-')}</div>
      <div><strong>类型</strong></div>
      <div>${escapeHtml(data.instrument_type || '-')}</div>
      <div><strong>上市日期</strong></div>
      <div>${escapeHtml(data.listing_date || '-')}</div>
      <div><strong>ST / 退市</strong></div>
      <div>${data.flags?.is_st ? 'ST' : '正常'} / ${data.flags?.is_delisted ? '已退市' : '未退市'}</div>
      <div><strong>更新时间</strong></div>
      <div>${escapeHtml(data.updated_at || '-')}</div>
    `;

    qs('#stock-detail-factors').innerHTML = `
      <div><strong>PE</strong></div>
      <div>${formatNumber(data.valuation?.pe_tushare, 2)}</div>
      <div><strong>PB</strong></div>
      <div>${formatNumber(data.valuation?.pb_tushare, 2)}</div>
      <div><strong>Turnover</strong></div>
      <div>${formatNumber(factorScores.turnover, 4)}</div>
      <div><strong>LowVol</strong></div>
      <div>${formatNumber(factorScores.lowvol, 4)}</div>
      <div><strong>Reversal</strong></div>
      <div>${formatNumber(factorScores.reversal, 4)}</div>
      <div><strong>估值更新时间</strong></div>
      <div>${escapeHtml(data.valuation?.valuation_updated_at || '-')}</div>
    `;

    qs('#stock-detail-fundamentals').innerHTML = `
      <div><strong>ROE</strong></div>
      <div>${formatNumber(data.fundamentals?.roe, 2)}</div>
      <div><strong>ROA</strong></div>
      <div>${formatNumber(data.fundamentals?.roa, 2)}</div>
      <div><strong>毛利率</strong></div>
      <div>${formatNumber(data.fundamentals?.grossprofit_margin, 2)}</div>
      <div><strong>净利率</strong></div>
      <div>${formatNumber(data.fundamentals?.netprofit_margin, 2)}</div>
      <div><strong>营收同比</strong></div>
      <div>${formatNumber(data.fundamentals?.revenue_yoy, 2)}</div>
      <div><strong>利润同比</strong></div>
      <div>${formatNumber(data.fundamentals?.profit_yoy, 2)}</div>
      <div><strong>报告期</strong></div>
      <div>${escapeHtml(data.fundamentals?.fundamental_period || '-')}</div>
    `;

    qs('#stock-detail-selection').innerHTML = `
      <div><strong>最近策略</strong></div>
      <div>${escapeHtml(latestSelection.strategy_display_name || latestSelection.strategy_id || '-')}</div>
      <div><strong>最近分数</strong></div>
      <div>${formatNumber(latestSelection.score, 4)}</div>
      <div><strong>最近排名</strong></div>
      <div>${escapeHtml(latestSelection.rank_no ?? '-')}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(latestSelection.trade_date || '-')}</div>
      <div><strong>最近 run_id</strong></div>
      <div>${escapeHtml(latestSelection.run_id || '-')}</div>
      <div><strong>记录创建时间</strong></div>
      <div>${escapeHtml(latestSelection.created_at || '-')}</div>
    `;

    const riskTexts = [];
    if (data.fundamentals?.roe == null) riskTexts.push('缺少 ROE 数据');
    if (data.fundamentals?.revenue_yoy == null) riskTexts.push('缺少营收同比数据');
    if (data.valuation?.pe_tushare == null) riskTexts.push('缺少 PE 数据');
    if (data.flags?.is_st) riskTexts.push('股票处于 ST 状态');
    const positiveReasons = [];
    if (data.fundamentals?.roe != null && Number(data.fundamentals.roe) >= 10) positiveReasons.push(`ROE 良好 (${formatNumber(data.fundamentals.roe, 2)})`);
    if (data.valuation?.pb_tushare != null && Number(data.valuation.pb_tushare) <= 2) positiveReasons.push(`PB 偏低 (${formatNumber(data.valuation.pb_tushare, 2)})`);
    if (latestSelection.score != null) positiveReasons.push(`最近入选分数 ${formatNumber(latestSelection.score, 4)}`);

    qs('#stock-detail-reasons').innerHTML = `
      <div><strong>当前可见正向信号：</strong>${positiveReasons.length ? escapeHtml(positiveReasons.join('；')) : '暂无明显正向信号'}</div>
      <div class="muted"><strong>当前主要风险：</strong>${riskTexts.length ? escapeHtml(riskTexts.join('；')) : '暂无明显风险提示'}</div>
    `;

    qs('#stock-detail-tracking-summary').innerHTML = `
      <div><strong>最近复盘入口</strong></div>
      <div>${latestSelection.run_id ? `run_id ${escapeHtml(latestSelection.run_id)}` : '暂无 run_id'}</div>
      <div><strong>建议动作</strong></div>
      <div>${latestSelection.run_id ? '可直接跳转到跟踪复盘页查看整轮表现' : '当前暂无可关联复盘 run_id'}</div>
      <div><strong>当前价格</strong></div>
      <div>${formatNumber(data.latest_kline?.close, 2)}</div>
      <div><strong>最近交易日</strong></div>
      <div>${escapeHtml(data.latest_kline?.trade_date || '-')}</div>
    `;

    const trackingLink = qs('#stock-detail-tracking-link');
    if (trackingLink && latestSelection.run_id) {
      trackingLink.href = `/tracking?run_id=${encodeURIComponent(latestSelection.run_id)}`;
    }

    renderPriceChart(data.price_history || []);
    renderSelectionHistory(data.selection_history || []);
  } catch (error) {
    qs('#stock-detail-subtitle').textContent = '加载详情失败';
    ['#stock-detail-basic', '#stock-detail-factors', '#stock-detail-fundamentals', '#stock-detail-selection', '#stock-detail-reasons', '#stock-detail-tracking-summary', '#stock-detail-history'].forEach((selector) => {
      qs(selector).innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    });
  }
}

document.addEventListener('DOMContentLoaded', loadStockDetail);
