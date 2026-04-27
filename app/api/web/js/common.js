async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}${text ? `: ${text}` : ''}`);
  }
  return response.json();
}

function qs(selector) {
  return document.querySelector(selector);
}

function qsa(selector) {
  return Array.from(document.querySelectorAll(selector));
}

function setActiveNav() {
  const page = document.body.dataset.page;
  qsa('[data-nav]').forEach((link) => {
    if (link.dataset.nav === page) {
      link.classList.add('active');
    }
  });
}

function formatPercent(value, digits = 2) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return `${Number(value).toFixed(digits)}%`;
}

function formatNumber(value, digits = 2) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return Number(value).toFixed(digits);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function renderEmptyRow(colspan, text = '暂无数据') {
  return `<tr><td colspan="${colspan}" class="muted">${escapeHtml(text)}</td></tr>`;
}

function renderError(container, message) {
  container.innerHTML = `<div class="error-box">${escapeHtml(message)}</div>`;
}

function getPctClass(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return Number(value) >= 0 ? 'up' : 'down';
}

function bindStockQuickSearch(inputSelector, buttonSelector) {
  const input = qs(inputSelector);
  const button = qs(buttonSelector);
  if (!input || !button) return;

  const go = () => {
    const code = (input.value || '').trim();
    if (!code) return;
    window.location.href = `/stocks/${encodeURIComponent(code)}`;
  };

  button.addEventListener('click', go);
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      go();
    }
  });
}

function bindGlobalStockSearch() {
  bindStockQuickSearch('[data-global-stock-search-input]', '[data-global-stock-search-btn]');
}

function ensureTooltip() {
  let tooltip = document.querySelector('[data-shared-tooltip]');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.className = 'tooltip-popover';
    tooltip.setAttribute('data-shared-tooltip', 'true');
    tooltip.hidden = true;
    document.body.appendChild(tooltip);
  }
  return tooltip;
}

function bindTooltips() {
  const tooltip = ensureTooltip();

  qsa('[data-tooltip]').forEach((element) => {
    if (element.dataset.tooltipBound === 'true') return;
    element.dataset.tooltipBound = 'true';

    const show = () => {
      tooltip.textContent = element.getAttribute('data-tooltip') || '';
      const rect = element.getBoundingClientRect();
      tooltip.hidden = false;
      tooltip.style.top = `${window.scrollY + rect.bottom + 8}px`;
      tooltip.style.left = `${Math.min(window.scrollX + rect.left, window.scrollX + window.innerWidth - 340)}px`;
    };

    const hide = () => {
      tooltip.hidden = true;
    };

    element.addEventListener('mouseenter', show);
    element.addEventListener('focus', show);
    element.addEventListener('mouseleave', hide);
    element.addEventListener('blur', hide);
  });
}

document.addEventListener('DOMContentLoaded', () => {
  setActiveNav();
  bindGlobalStockSearch();
  bindTooltips();
});
