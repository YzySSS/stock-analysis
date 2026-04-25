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

document.addEventListener('DOMContentLoaded', () => {
  setActiveNav();
});
