function formatAmount(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  const n = Number(value);
  if (Math.abs(n) >= 100000000) return `${(n / 100000000).toFixed(1)}亿`;
  if (Math.abs(n) >= 10000) return `${(n / 10000).toFixed(1)}万`;
  return n.toFixed(0);
}

function formatSectorAmount(value, unit) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  if (unit === '亿元') return `${Number(value).toFixed(1)}亿`;
  return formatAmount(value);
}

function setText(selector, value) {
  const element = qs(selector);
  if (element) element.textContent = value;
}

function setWidth(selector, value) {
  const element = qs(selector);
  if (element) element.style.width = `${Math.max(0, Math.min(100, Number(value) || 0))}%`;
}

function setLeftPercent(selector, value) {
  const element = qs(selector);
  if (element) element.style.left = `${Math.max(0, Math.min(100, Number(value) || 0))}%`;
}

function renderSectorList(selector, sectors, emptyText) {
  const container = qs(selector);
  if (!container) return;
  if (!sectors || !sectors.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }
  container.innerHTML = sectors.slice(0, 5).map((item, index) => {
    const typeLabel = item.sector_type_label || '行业';
    const isFundFlow = item.net_amount != null;
    const mainValue = isFundFlow ? formatSectorAmount(item.net_amount, item.source_unit) : formatPercent(item.amount_weighted_pct_chg ?? item.avg_pct_chg);
    const extraLine = isFundFlow
      ? `领涨 ${escapeHtml(item.leading_stock || '-')} ${formatPercent(item.leading_stock_pct_chg)} · 涨幅 ${formatPercent(item.avg_pct_chg)}`
      : `上涨 ${escapeHtml(item.up_count ?? 0)} · 下跌 ${escapeHtml(item.down_count ?? 0)}`;
    return `
      <a class="home-sector-row" href="/selection">
        <span class="sector-rank">${index + 1}</span>
        <span class="sector-name">
          <strong>${escapeHtml(item.name || '-')}</strong>
          <small>${escapeHtml(typeLabel)} · ${escapeHtml(item.stock_count ?? '-')} 只</small>
        </span>
        <span class="sector-flow">
          <b>${mainValue}</b>
          <small>${extraLine}</small>
        </span>
      </a>
    `;
  }).join('');
}

function splitDateTime(value) {
  if (!value) return { date: '-', time: '--:--:--' };
  const parts = String(value).split(' ');
  return { date: parts[0] || '-', time: parts[1] || parts[0] || '--:--:--' };
}

function setClass(selector, className) {
  const element = qs(selector);
  if (element) element.className = className;
}

function renderMarketOverview(overview) {
  if (!overview) return;
  const quote = splitDateTime(overview.latest_quote_time);
  const strength = Number(overview.market_strength || 0);
  const total = Number(overview.total || 0);
  const upPct = total ? (Number(overview.up_count || 0) / total) * 100 : 0;
  const downPct = total ? (Number(overview.down_count || 0) / total) * 100 : 0;
  const flatPct = total ? (Number(overview.flat_count || 0) / total) * 100 : 0;
  setText('#home-market-strength', overview.market_strength == null ? '-' : `${strength.toFixed(1)}`);
  setClass('.market-strength-thermo', `market-strength-thermo ${strength > 60 ? 'hot' : 'cool'}`);
  setLeftPercent('#home-strength-pointer', strength);
  const change = overview.market_strength_change;
  const changeText = change == null ? '较昨日 -' : `较昨日 ${Number(change) >= 0 ? '+' : ''}${Number(change).toFixed(1)}`;
  setText('#home-market-strength-change', changeText);
  setText('#home-market-state', `${overview.market_state_label || '-'} · 额加权 ${formatPercent(overview.amount_weighted_pct_chg ?? overview.avg_pct_chg)}`);
  setText('#home-market-breadth', `${overview.up_count ?? '-'} / ${overview.down_count ?? '-'} / ${overview.flat_count ?? '-'}`);
  setText('#home-market-up-ratio', overview.up_ratio == null ? '-' : `上涨占比 ${(Number(overview.up_ratio) * 100).toFixed(1)}%`);
  setText('#home-market-active', `涨 ${overview.up_count ?? '-'} · 平 ${overview.flat_count ?? '-'} · 跌 ${overview.down_count ?? '-'} · 样本 ${overview.total ?? '-'} 只`);
  setWidth('#home-breadth-up', upPct);
  setWidth('#home-breadth-flat', flatPct);
  setWidth('#home-breadth-down', downPct);
  setText('#home-market-limit', `${overview.limit_up_like ?? '-'} / ${overview.limit_down_like ?? '-'}`);
  const bjLimit = overview.limit_breakdown?.board30;
  const bjLimitText = bjLimit ? ` · 北证 ${bjLimit.up ?? 0}/${bjLimit.down ?? 0}` : '';
  setText('#home-market-big-move', `大涨 ${overview.strong_up_count ?? '-'} · 大跌 ${overview.strong_down_count ?? '-'}${bjLimitText}`);
  const activeTotal = (overview.up_count ?? 0) + (overview.down_count ?? 0);
  const sectorTime = overview.sector_fund_flow_time ? ` · 板块资金 ${overview.sector_fund_flow_time}` : '';
  setText('#home-market-clock', quote.time);
  setText('#home-market-session', overview.market_session_label || '已收盘');
  setText('#home-market-time', `数据更新时间：${overview.latest_quote_time || '-'}`);
  setText('#home-market-total', `${overview.total ?? '-'} 只`);
  setText('#home-market-source', `样本 ${overview.total ?? '-'} 只 · ${overview.trade_date || '-'}${sectorTime}`);
  renderSectorList('#home-strong-sectors', overview.strong_sectors, '暂无强势板块统计');
  renderSectorList('#home-weak-sectors', overview.weak_sectors, '暂无弱势板块统计');
}

function renderTrackingCards(items = []) {
  if (!items.length) return '<div class="empty-state">暂无跟踪数据。可以先去选股中心运行一次策略。</div>';
  return items.map((item) => {
    const pct = item.price_change_pct;
    const pctClass = getPctClass(pct);
    const detailUrl = `/stocks/${encodeURIComponent(item.code || '')}`;
    return `
      <article class="home-tracking-card">
        <a href="${detailUrl}" class="home-tracking-card-main">
          <div class="home-tracking-card-head">
            <div>
              <strong>${escapeHtml(item.name || item.code || '-')}</strong>
              <span>${escapeHtml(item.code || '-')}</span>
            </div>
            ${item.rank_no != null ? `<em>#${escapeHtml(item.rank_no)}</em>` : ''}
          </div>
          <div class="home-tracking-return ${pctClass}">${formatPercent(pct)}</div>
          <div class="home-tracking-meta">
            <span>入选 ${escapeHtml(item.selection_date || '-')}</span>
            <span>跟踪 ${escapeHtml(item.tracking_days ?? '-')} 日</span>
          </div>
          <div class="home-tracking-price-row">
            <span>入选价 <b>${formatNumber(item.selected_open_price ?? item.selected_close_price, 2)}</b></span>
            <span>实时价 <b>${formatNumber(item.current_price, 2)}</b></span>
            <span>分数 <b>${formatNumber(item.score, 2)}</b></span>
          </div>
          <small>${escapeHtml(item.strategy_display_name || item.strategy_id || '-')} · ${escapeHtml(item.realtime_quote_time || '暂无实时快照')}</small>
        </a>
      </article>
    `;
  }).join('');
}

async function loadHomePage() {
  const trackingSummary = qs('#home-tracking-summary');
  const trackingPreview = qs('#home-tracking-preview');

  try {
    const data = await fetchJson('/api/dashboard/summary?limit=8');
    const items = data.latest_tracking_preview || [];
    renderMarketOverview(data.market_overview);

    const avgText = data.latest_tracking_avg_price_change_pct == null
      ? '平均涨跌幅 -'
      : `平均涨跌幅 ${formatPercent(data.latest_tracking_avg_price_change_pct)}`;
    trackingSummary.textContent = `共 ${data.latest_tracking_count ?? items.length ?? 0} 条 · ${avgText}`;
    trackingPreview.innerHTML = renderTrackingCards(items);
  } catch (error) {
    trackingSummary.textContent = '加载失败';
    trackingPreview.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadHomePage();
});
