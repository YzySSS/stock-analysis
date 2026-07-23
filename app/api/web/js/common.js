function getCookieValue(name) {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie
    .split(';')
    .map((part) => part.trim())
    .find((part) => part.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : '';
}

function currentLoginUrl() {
  const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  return `/login?next=${encodeURIComponent(currentPath)}`;
}

function installAuthenticatedFetch() {
  if (window.__stockAnalysisAuthFetchInstalled) return;
  window.__stockAnalysisAuthFetchInstalled = true;
  const nativeFetch = window.fetch.bind(window);

  window.fetch = async (input, options = {}) => {
    const requestUrl = new URL(
      typeof input === 'string' ? input : input.url,
      window.location.href,
    );
    const requestMethod = String(
      options.method || (typeof input === 'string' ? 'GET' : input.method) || 'GET',
    ).toUpperCase();
    let requestOptions = options;

    if (
      requestUrl.origin === window.location.origin
      && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(requestMethod)
    ) {
      const csrfToken = getCookieValue('stock_analysis_csrf');
      const headers = new Headers(typeof input === 'string' ? undefined : input.headers);
      new Headers(options.headers || {}).forEach((value, key) => headers.set(key, value));
      if (csrfToken) headers.set('X-CSRF-Token', csrfToken);
      requestOptions = { ...options, headers };
    }

    const response = await nativeFetch(input, requestOptions);
    if (response.status === 401 && requestUrl.origin === window.location.origin) {
      window.location.replace(currentLoginUrl());
    }
    return response;
  };
}

installAuthenticatedFetch();

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

function formatPrice(value) {
  return formatNumber(value, 3);
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
  if (value == null || Number.isNaN(Number(value))) return null;
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

function bindLogoutControl() {
  const sidebar = qs('.sidebar');
  if (!sidebar || sidebar.querySelector('[data-logout-control]')) return;

  const wrapper = document.createElement('div');
  wrapper.className = 'sidebar-auth';
  wrapper.dataset.logoutControl = 'true';
  wrapper.innerHTML = `
    <span class="sidebar-auth-label">已安全登录</span>
    <button class="sidebar-logout" type="button">退出登录</button>
  `;
  sidebar.appendChild(wrapper);

  wrapper.querySelector('.sidebar-logout').addEventListener('click', async (event) => {
    const button = event.currentTarget;
    button.disabled = true;
    button.textContent = '正在退出…';
    try {
      const response = await fetch('/logout', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      window.location.replace('/login');
    } catch (error) {
      button.disabled = false;
      button.textContent = '重试退出';
    }
  });
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

  const show = (element) => {
    tooltip.textContent = element.getAttribute('data-tooltip') || '';
    const rect = element.getBoundingClientRect();
    tooltip.hidden = false;
    tooltip.style.top = `${window.scrollY + rect.bottom + 8}px`;
    tooltip.style.left = `${Math.min(window.scrollX + rect.left, window.scrollX + window.innerWidth - 340)}px`;
    tooltip.dataset.owner = element.dataset.tooltipId || '';
  };

  const hide = () => {
    tooltip.hidden = true;
    tooltip.dataset.owner = '';
  };

  qsa('[data-tooltip]').forEach((element, index) => {
    if (element.dataset.tooltipBound === 'true') return;
    element.dataset.tooltipBound = 'true';
    if (!element.dataset.tooltipId) element.dataset.tooltipId = `tooltip-${index}`;

    element.addEventListener('mouseenter', () => show(element));
    element.addEventListener('focus', () => show(element));
    element.addEventListener('mouseleave', hide);
    element.addEventListener('blur', hide);
    element.addEventListener('click', (event) => {
      event.preventDefault();
      if (!tooltip.hidden && tooltip.dataset.owner === element.dataset.tooltipId) {
        hide();
      } else {
        show(element);
      }
    });
  });

  if (!document.body.dataset.tooltipGlobalBound) {
    document.body.dataset.tooltipGlobalBound = 'true';
    document.addEventListener('click', (event) => {
      if (!event.target.closest('[data-tooltip]') && !event.target.closest('[data-shared-tooltip]')) {
        hide();
      }
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  setActiveNav();
  bindGlobalStockSearch();
  bindLogoutControl();
  bindTooltips();
});
