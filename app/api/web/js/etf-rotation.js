const ETF_SIGNAL_STATUS = {
  ready: '有影子候选',
  cash: '无达标项 / 空仓',
  blocked: '数据阻断',
};

const ETF_TIMING_STATE = {
  cash: '现金防守',
  defensive: '低仓防守',
  cautious: '谨慎',
  neutral: '中性',
  risk_on: '风险偏好',
  strong_risk_on: '强风险偏好',
  missing: '择时缺失',
};

function moneyText(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  const number = Number(value);
  if (Math.abs(number) >= 100000000) return `${(number / 100000000).toFixed(2)} 亿`;
  if (Math.abs(number) >= 10000) return `${(number / 10000).toFixed(2)} 万`;
  return number.toFixed(0);
}

function dateTimeText(value) {
  if (!value) return '-';
  return String(value).replace('T', ' ').slice(0, 16);
}

function failedGateText(gates = {}) {
  const labels = {
    timing_aligned: '择时未对齐',
    sector_latest_aligned: '行业数据未对齐',
    sector_history_sufficient: '行业历史不足',
    sector_alias_coverage_complete: '行业映射覆盖不足',
    opinion_available: '舆情评分缺失',
    opinion_aligned: '舆情日期未对齐',
    opinion_alias_coverage_complete: '舆情映射覆盖不足',
    etf_latest_aligned: 'ETF 行情未对齐',
    etf_history_sufficient: 'ETF 历史不足',
    listing_age_sufficient: '上市时间不足',
    liquidity_sufficient: '流动性不足',
    share_history_available: '份额历史缺失',
    nav_available: '净值缺失',
    nav_fresh: '净值过期',
    premium_discount_within_limit: '折溢价超限',
  };
  const failed = Object.entries(gates)
    .filter(([, passed]) => !passed)
    .map(([key]) => labels[key] || key);
  return failed.join('；') || '分数未达冻结门槛';
}

function renderCandidates(items = []) {
  const body = qs('#etf-candidate-body');
  if (!body) return;
  if (!items.length) {
    body.innerHTML = renderEmptyRow(9, '暂无候选');
    return;
  }
  body.innerHTML = items.map((item) => {
    const conclusion = item.is_selected
      ? '<span class="badge status-ok">影子入选</span>'
      : item.is_eligible
        ? '<span class="badge status-warning">达标未入选</span>'
        : `<span class="badge status-muted" title="${escapeHtml(failedGateText(item.gates))}">未达标</span>`;
    return `
      <tr class="${item.is_selected ? 'is-selected' : ''}">
        <td>${escapeHtml(item.rank_no)}</td>
        <td>
          <strong>${escapeHtml(item.sector_name)}</strong>
          <small>${escapeHtml(item.ts_code)} · ${escapeHtml(item.fund_name)}</small>
        </td>
        <td><b>${formatNumber(item.combined_score, 1)}</b></td>
        <td>${formatNumber(item.sector_score, 1)}</td>
        <td>${formatNumber(item.etf_score, 1)}</td>
        <td>${moneyText(item.average_amount_20d_yuan)}</td>
        <td class="${getPctClass(item.share_change_20d_pct) || ''}">${formatPercent(item.share_change_20d_pct)}</td>
        <td class="${Math.abs(Number(item.premium_discount_pct)) > 1 ? 'down' : ''}">${formatPercent(item.premium_discount_pct)}</td>
        <td>${conclusion}</td>
      </tr>
    `;
  }).join('');
}

function renderOutcomes(items = []) {
  const container = qs('#etf-outcome-list');
  if (!container) return;
  if (!items.length) {
    container.className = 'etf-outcome-list muted';
    container.textContent = '当前信号尚未产生下一交易日开盘观察；这不会被填成 0 或模拟收益。';
    return;
  }
  container.className = 'etf-outcome-list';
  container.innerHTML = items.map((item) => `
    <article>
      <strong>${escapeHtml(item.ts_code)} · ${escapeHtml(item.horizon_days)} 日</strong>
      <span>${escapeHtml(item.outcome_status || 'pending')}</span>
      <small>入场 ${escapeHtml(item.entry_trade_date || '-')} ${formatPrice(item.entry_price)}</small>
      <small>退出 ${escapeHtml(item.exit_trade_date || '-')} ${formatPrice(item.exit_price)}</small>
      <b class="${getPctClass(item.gross_return_pct) || ''}">${formatPercent(item.gross_return_pct)}</b>
    </article>
  `).join('');
}

function renderDiagnostics(item) {
  const diagnostics = item.diagnostics || {};
  const rejected = diagnostics.rejected_gate_counts || {};
  const rejectedText = Object.entries(rejected)
    .map(([key, count]) => `${key}: ${count}`)
    .join('；');
  qs('#etf-diagnostics').innerHTML = `
    <p><strong>完整池：</strong>${escapeHtml(diagnostics.complete_candidate_count || 0)} / ${escapeHtml(item.candidate_count || 0)}</p>
    <p><strong>择时上限：</strong>${escapeHtml(item.selection_cap || 0)} 只</p>
    <p><strong>阻断项：</strong>${escapeHtml(rejectedText || '无')}</p>
    <p><strong>运行 ID：</strong>${escapeHtml(item.run_id || '-')}</p>
  `;
}

function renderContract(spec = {}) {
  const list = qs('#etf-contract-list');
  if (!list) return;
  const data = spec.data_contract || {};
  list.innerHTML = [
    `固定 ${spec.sectors?.length || 0} 个行业篮子，每个篮子只映射 1 只 ETF`,
    `行业至少 ${data.minimum_sector_history_days || '-'} 个交易日，ETF 至少 ${data.minimum_etf_history_days || '-'} 个交易日`,
    `20 日平均成交额不低于 ${moneyText(data.minimum_average_amount_20d_yuan)}`,
    `收盘决策，下一交易日开盘开始观察；最多 ${spec.maximum_selections || 0} 只`,
    '零候选是合法结果；缺数据不补值、不自动调参、不自动交易',
  ].map((text) => `<li>${escapeHtml(text)}</li>`).join('');
}

function renderSignal(item, spec) {
  qs('#etf-rotation-content').hidden = false;
  qs('#etf-signal-date').textContent = String(item.trade_date || '-');
  qs('#etf-signal-status').textContent = ETF_SIGNAL_STATUS[item.status] || item.status || '-';
  qs('#etf-timing-state').textContent = ETF_TIMING_STATE[item.timing_state] || item.timing_state || '-';
  const complete = item.diagnostics?.complete_candidate_count || 0;
  qs('#etf-universe-coverage').textContent = `${complete} / ${item.candidate_count || 0}`;
  qs('#etf-selection-count').textContent = `${item.eligible_count || 0} / ${item.selected_count || 0}`;
  qs('#etf-earliest-execution').textContent = dateTimeText(item.earliest_execution_at);
  renderCandidates(item.candidates || []);
  renderOutcomes(item.outcomes || []);
  renderDiagnostics(item);
  renderContract(spec);
}

async function loadEtfRotation() {
  try {
    const data = await fetchJson('/api/etf-rotation/latest');
    const spec = data.spec || {};
    const badge = qs('#etf-rotation-model-badge');
    badge.textContent = `${spec.model_name || 'ETF 轮动'} · ${spec.version || '-'}`;
    badge.className = 'badge status-warning';
    if (!data.item) {
      qs('#etf-rotation-empty').hidden = false;
      renderContract(spec);
      return;
    }
    renderSignal(data.item, spec);
  } catch (error) {
    const empty = qs('#etf-rotation-empty');
    empty.hidden = false;
    renderError(empty, `ETF 轮动加载失败：${error.message}`);
  }
}

document.addEventListener('DOMContentLoaded', loadEtfRotation);
